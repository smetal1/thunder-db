//! Cluster Coordinator
//!
//! Implements the ClusterManager trait and coordinates distributed operations
//! across the cluster, including region placement, load balancing, and
//! failure recovery.

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex as AsyncMutex};
use thunder_common::prelude::*;

use crate::gossip::{GossipConfig, GossipEvent, Gossiper};
use crate::membership::{HealthStatus, Membership, MembershipConfig, MembershipEvent, NodeMeta};
use crate::region::{Peer, RegionAction, RegionConfig, RegionManager};
use crate::transport::{NetworkTransport, TransportConfig};
use crate::{ClusterManager, Command, NodeInfo, NodeStatus, RaftMessage, RegionInfo};

/// Cluster coordinator configuration
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// This node's ID
    pub node_id: NodeId,
    /// This node's address
    pub addr: SocketAddr,
    /// Cluster name for gossip validation
    pub cluster_name: String,
    /// Seed nodes for gossip bootstrap
    pub seeds: Vec<SocketAddr>,
    /// Raft tick interval
    pub tick_interval: Duration,
    /// Region check interval
    pub region_check_interval: Duration,
    /// Rebalance interval
    pub rebalance_interval: Duration,
    /// Maximum regions per node
    pub max_regions_per_node: usize,
    /// Target region replicas
    pub target_replicas: usize,
    /// Region configuration
    pub region_config: RegionConfig,
    /// Transport configuration
    pub transport_config: TransportConfig,
    /// Membership configuration
    pub membership_config: MembershipConfig,
    /// Gossip configuration
    pub gossip_config: GossipConfig,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId(1),
            addr: "127.0.0.1:5000".parse().unwrap(),
            cluster_name: "thunderdb".to_string(),
            seeds: Vec::new(),
            tick_interval: Duration::from_millis(100),
            region_check_interval: Duration::from_secs(60),
            rebalance_interval: Duration::from_secs(300),
            max_regions_per_node: 100,
            target_replicas: 3,
            region_config: RegionConfig::default(),
            transport_config: TransportConfig::default(),
            membership_config: MembershipConfig::default(),
            gossip_config: GossipConfig::default(),
        }
    }
}

/// Cluster state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    /// Cluster is initializing
    Initializing,
    /// Cluster is bootstrapping (first node)
    Bootstrapping,
    /// Cluster is joining an existing cluster
    Joining,
    /// Cluster is running normally
    Running,
    /// Cluster is shutting down
    ShuttingDown,
}

/// Pending operation for tracking async operations
#[allow(dead_code)]
#[derive(Debug)]
struct PendingOperation {
    /// Operation type
    op_type: OperationType,
    /// Started at
    started_at: Instant,
    /// Regions involved
    regions: Vec<RegionId>,
    /// Result sender
    result_tx: Option<tokio::sync::oneshot::Sender<Result<()>>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationType {
    RegionSplit,
    RegionMerge,
    PeerAdd,
    PeerRemove,
    LeaderTransfer,
    Rebalance,
}

/// The main cluster coordinator
pub struct ClusterCoordinator {
    /// Configuration
    config: CoordinatorConfig,
    /// Cluster state
    state: RwLock<ClusterState>,
    /// Membership manager
    membership: Arc<Membership>,
    /// Region manager
    region_manager: Arc<RegionManager>,
    /// Network transport
    transport: Arc<dyn NetworkTransport>,
    /// Gossiper for peer discovery
    gossiper: Option<Arc<Gossiper>>,
    /// Pending operations
    #[allow(dead_code)]
    pending_ops: DashMap<u64, PendingOperation>,
    /// Next operation ID
    next_op_id: AtomicU64,
    /// Shutdown flag
    shutdown: AtomicBool,
    /// Background task handles
    task_handles: AsyncMutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Distributed table catalog
    distributed_catalog: Option<Arc<crate::catalog_sync::DistributedCatalog>>,
    /// Distributed deadlock detector
    deadlock_detector: Option<Arc<crate::deadlock::DistributedDeadlockDetector>>,
}

impl ClusterCoordinator {
    /// Create a new cluster coordinator
    pub fn new(
        config: CoordinatorConfig,
        transport: Arc<dyn NetworkTransport>,
    ) -> Self {
        let membership = Arc::new(Membership::new(
            config.node_id,
            config.addr,
            config.membership_config.clone(),
        ));

        let region_manager = Arc::new(RegionManager::new(
            config.node_id,
            config.region_config.clone(),
        ));

        // Create gossiper if clustering is enabled (seeds provided or is seed)
        let gossiper = if !config.seeds.is_empty() || config.gossip_config.is_seed {
            Some(Gossiper::new(
                config.node_id,
                config.addr,
                config.cluster_name.clone(),
                config.seeds.clone(),
                membership.clone(),
                config.gossip_config.clone(),
            ))
        } else {
            None
        };

        let distributed_catalog = Some(Arc::new(crate::catalog_sync::DistributedCatalog::new(
            config.node_id,
            config.max_regions_per_node.min(16), // shard_count
            config.target_replicas,
        )));

        let deadlock_detector = Some(Arc::new(
            crate::deadlock::DistributedDeadlockDetector::new(config.node_id),
        ));

        Self {
            config,
            state: RwLock::new(ClusterState::Initializing),
            membership,
            region_manager,
            transport,
            gossiper,
            pending_ops: DashMap::new(),
            next_op_id: AtomicU64::new(1),
            shutdown: AtomicBool::new(false),
            task_handles: AsyncMutex::new(Vec::new()),
            distributed_catalog,
            deadlock_detector,
        }
    }

    /// Get the distributed catalog
    pub fn distributed_catalog(&self) -> Option<&Arc<crate::catalog_sync::DistributedCatalog>> {
        self.distributed_catalog.as_ref()
    }

    /// Get the deadlock detector
    pub fn deadlock_detector(&self) -> Option<&Arc<crate::deadlock::DistributedDeadlockDetector>> {
        self.deadlock_detector.as_ref()
    }

    /// Get the gossiper
    pub fn gossiper(&self) -> Option<&Arc<Gossiper>> {
        self.gossiper.as_ref()
    }

    /// Get the address of a remote node (from transport registry).
    pub fn get_node_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.transport.get_addr(node_id)
    }

    /// Get the membership manager.
    pub fn membership(&self) -> &Arc<Membership> {
        &self.membership
    }

    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Get the cluster state
    pub fn cluster_state(&self) -> ClusterState {
        *self.state.read()
    }

    /// Bootstrap a new cluster (single node) or join via gossip
    pub async fn bootstrap(&self) -> Result<()> {
        *self.state.write() = ClusterState::Bootstrapping;

        // If gossiper is present, try to bootstrap via gossip first
        if let Some(gossiper) = &self.gossiper {
            tracing::info!("Bootstrapping cluster via gossip protocol");

            if let Err(e) = gossiper.bootstrap().await {
                tracing::warn!("Gossip bootstrap failed: {}, proceeding as new cluster", e);
            }

            // Check if we discovered any nodes
            let discovered = gossiper.discovered_nodes();
            if !discovered.is_empty() {
                tracing::info!(
                    "Discovered {} nodes via gossip, joining cluster",
                    discovered.len()
                );

                // Register discovered nodes with transport
                for (node_id, addr) in &discovered {
                    self.transport.register(*node_id, *addr).await?;
                }

                *self.state.write() = ClusterState::Joining;

                // Mark ourselves online
                self.membership.mark_online();

                *self.state.write() = ClusterState::Running;
                return Ok(());
            }
        }

        // No peers found - bootstrap as new single-node cluster
        let peers = vec![Peer::new(self.config.node_id.0, self.config.node_id)];
        let region_id = self.region_manager.bootstrap(peers)?;

        tracing::info!("Bootstrapped new cluster with region {:?}", region_id);

        // Mark node as online
        self.membership.mark_online();

        *self.state.write() = ClusterState::Running;
        Ok(())
    }

    /// Join an existing cluster via gossip
    pub async fn join(&self) -> Result<()> {
        *self.state.write() = ClusterState::Joining;

        // Use gossip to discover and join
        if let Some(gossiper) = &self.gossiper {
            gossiper.bootstrap().await?;

            let discovered = gossiper.discovered_nodes();
            if discovered.is_empty() {
                *self.state.write() = ClusterState::Initializing;
                return Err(Error::Internal(
                    "No cluster nodes discovered via gossip".to_string(),
                ));
            }

            // Register discovered nodes
            for (node_id, addr) in &discovered {
                self.transport.register(*node_id, *addr).await?;
            }

            tracing::info!("Joined cluster with {} nodes via gossip", discovered.len());
        } else {
            *self.state.write() = ClusterState::Initializing;
            return Err(Error::Internal(
                "Cannot join cluster: gossip not configured".to_string(),
            ));
        }

        self.membership.mark_online();
        *self.state.write() = ClusterState::Running;
        Ok(())
    }

    /// Join an existing cluster by contacting a specific seed address
    pub async fn join_with_seed(&self, seed_addr: SocketAddr) -> Result<()> {
        *self.state.write() = ClusterState::Joining;

        // Register the seed node with a temporary ID for initial contact
        self.transport
            .register(NodeId(0), seed_addr)
            .await?;

        // Send a Heartbeat message to announce our presence to the seed node
        let join_msg = RaftMessage {
            region_id: RegionId(0), // Cluster-level message (no specific region)
            from: self.config.node_id,
            to: NodeId(0),
            msg_type: crate::RaftMessageType::Heartbeat,
            term: 0,
            payload: Vec::new(),
        };

        if let Err(e) = self.transport.send(NodeId(0), join_msg).await {
            // Failed to contact seed node — revert to Initializing
            *self.state.write() = ClusterState::Initializing;
            return Err(Error::Internal(format!(
                "Failed to contact seed node at {}: {}",
                seed_addr, e
            )));
        }

        // Seed contacted successfully — mark ourselves as online
        self.membership.mark_online();

        *self.state.write() = ClusterState::Running;
        Ok(())
    }

    /// Return the quorum size for the current cluster (majority).
    pub fn quorum_size(&self) -> usize {
        (self.membership.node_count() / 2) + 1
    }

    /// Check whether the cluster currently has quorum.
    pub fn has_quorum(&self) -> bool {
        self.membership.online_count() >= self.quorum_size()
    }

    /// Start background tasks
    pub async fn start(&self) -> Result<()> {
        let mut handles = self.task_handles.lock().await;

        // Start gossiper if present
        if let Some(gossiper) = &self.gossiper {
            let gossiper_clone = gossiper.clone();
            handles.push(tokio::spawn(async move {
                if let Err(e) = gossiper_clone.run().await {
                    tracing::error!("Gossiper stopped with error: {}", e);
                }
            }));

            // Start gossip event handler
            let coordinator = self.clone_ref();
            let mut gossip_rx = gossiper.subscribe();
            handles.push(tokio::spawn(async move {
                loop {
                    match gossip_rx.recv().await {
                        Ok(event) => {
                            coordinator.handle_gossip_event(event).await;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }));

            tracing::info!("Started gossip protocol");
        }

        // Start tick task
        let coordinator = self.clone_ref();
        handles.push(tokio::spawn(async move {
            coordinator.tick_loop().await;
        }));

        // Start region check task
        let coordinator = self.clone_ref();
        handles.push(tokio::spawn(async move {
            coordinator.region_check_loop().await;
        }));

        // Start message receive task
        let coordinator = self.clone_ref();
        handles.push(tokio::spawn(async move {
            coordinator.message_receive_loop().await;
        }));

        // Start membership event handler
        let coordinator = self.clone_ref();
        handles.push(tokio::spawn(async move {
            coordinator.membership_event_loop().await;
        }));

        Ok(())
    }

    /// Handle gossip events
    async fn handle_gossip_event(&self, event: GossipEvent) {
        match event {
            GossipEvent::NodeDiscovered(node_id, addr) => {
                tracing::info!("Gossip: discovered node {:?} at {}", node_id, addr);

                // Register with transport
                if let Err(e) = self.transport.register(node_id, addr).await {
                    tracing::warn!("Failed to register discovered node: {}", e);
                }

                // Trigger rebalance check if we're running
                if self.cluster_state() == ClusterState::Running {
                    self.trigger_rebalance_check().await;
                }
            }
            GossipEvent::NodeAlive(node_id) => {
                tracing::info!("Gossip: node {:?} is now alive", node_id);

                // Update membership
                let _ = self.membership.update_status(node_id, NodeStatus::Online);
            }
            GossipEvent::NodeDead(node_id) => {
                tracing::warn!("Gossip: node {:?} is now dead", node_id);

                // Handle as failure
                if let Err(e) = self.handle_node_failure(node_id).await {
                    tracing::error!("Failed to handle dead node {:?}: {}", node_id, e);
                }
            }
            GossipEvent::StateUpdated(node_id) => {
                tracing::debug!("Gossip: state updated for node {:?}", node_id);
            }
        }
    }

    /// Trigger a rebalance check
    async fn trigger_rebalance_check(&self) {
        let nodes = self.membership.node_infos();
        let regions = self.region_manager.region_infos();
        let scheduler = Scheduler::new(
            self.config.target_replicas,
            self.config.max_regions_per_node,
        );

        let node_metas: Vec<_> = nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Online)
            .map(|n| {
                let mut meta = NodeMeta::new(n.id, self.config.addr);
                meta.status = n.status;
                meta
            })
            .collect();

        if scheduler.needs_rebalance(&node_metas, &regions) {
            tracing::info!("Triggering rebalance after topology change");
            let moves = scheduler.generate_moves(&node_metas, &regions);
            for mv in moves {
                tracing::info!(
                    "Rebalance: moving region {:?} from {:?} to {:?}",
                    mv.region_id, mv.from, mv.to
                );

                let new_peer = Peer::new(
                    self.next_op_id.fetch_add(1, Ordering::SeqCst),
                    mv.to,
                );

                if let Err(e) = self.add_peer_to_region(mv.region_id, new_peer).await {
                    tracing::error!("Failed to add peer during rebalance: {}", e);
                }
            }
        }
    }

    /// Stop the coordinator
    pub async fn stop(&self) -> Result<()> {
        *self.state.write() = ClusterState::ShuttingDown;
        self.shutdown.store(true, Ordering::SeqCst);

        // Shutdown gossiper
        if let Some(gossiper) = &self.gossiper {
            gossiper.shutdown();
        }

        // Cancel all background tasks
        let mut handles = self.task_handles.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }

        // Close transport
        self.transport.close().await?;

        Ok(())
    }

    /// Clone a reference to self (for spawning tasks)
    fn clone_ref(&self) -> Arc<ClusterCoordinator> {
        // This is a workaround - in practice, the coordinator would be Arc-wrapped
        // For now, we'll just create a simple wrapper
        Arc::new(Self {
            config: self.config.clone(),
            state: RwLock::new(*self.state.read()),
            membership: self.membership.clone(),
            region_manager: self.region_manager.clone(),
            transport: self.transport.clone(),
            gossiper: self.gossiper.clone(),
            pending_ops: DashMap::new(),
            next_op_id: AtomicU64::new(self.next_op_id.load(Ordering::SeqCst)),
            shutdown: AtomicBool::new(self.shutdown.load(Ordering::SeqCst)),
            task_handles: AsyncMutex::new(Vec::new()),
            distributed_catalog: self.distributed_catalog.clone(),
            deadlock_detector: self.deadlock_detector.clone(),
        })
    }

    /// Tick loop for Raft nodes
    async fn tick_loop(&self) {
        let mut interval = tokio::time::interval(self.config.tick_interval);

        while !self.shutdown.load(Ordering::SeqCst) {
            interval.tick().await;
            self.region_manager.tick_all();
        }
    }

    /// Region check loop
    async fn region_check_loop(&self) {
        let mut interval = tokio::time::interval(self.config.region_check_interval);

        while !self.shutdown.load(Ordering::SeqCst) {
            interval.tick().await;

            // Check for region actions
            let actions = self.region_manager.check_regions();
            for action in actions {
                if let Err(e) = self.handle_region_action(action).await {
                    tracing::error!("Failed to handle region action: {}", e);
                }
            }
        }
    }

    /// Message receive loop
    async fn message_receive_loop(&self) {
        while !self.shutdown.load(Ordering::SeqCst) {
            match self.transport.recv().await {
                Ok(msg) => {
                    if let Err(e) = self.handle_message(msg).await {
                        tracing::error!("Failed to handle message: {}", e);
                    }
                }
                Err(e) => {
                    if !self.shutdown.load(Ordering::SeqCst) {
                        tracing::error!("Transport recv error: {}", e);
                    }
                    break;
                }
            }
        }
    }

    /// Membership event loop
    async fn membership_event_loop(&self) {
        let mut rx = self.membership.subscribe();

        while !self.shutdown.load(Ordering::SeqCst) {
            match rx.recv().await {
                Ok(event) => {
                    self.handle_membership_event(event).await;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // Missed some events, continue
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    }

    /// Handle a region action
    async fn handle_region_action(&self, action: RegionAction) -> Result<()> {
        match action {
            RegionAction::Split { region_id } => {
                tracing::info!("Initiating split for region {:?}", region_id);
                self.execute_split(region_id).await?;
            }
            RegionAction::CheckMerge { region_id } => {
                tracing::info!("Checking merge for region {:?}", region_id);
                self.check_and_execute_merge(region_id).await?;
            }
            RegionAction::TransferLeader { region_id, target } => {
                tracing::info!("Transferring leader for region {:?} to {:?}", region_id, target);
                self.transfer_leader(region_id, target).await?;
            }
            RegionAction::AddPeer { region_id, peer } => {
                tracing::info!("Adding peer {:?} to region {:?}", peer.node_id, region_id);
                self.add_peer_to_region(region_id, peer).await?;
            }
            RegionAction::RemovePeer { region_id, peer_id } => {
                tracing::info!("Removing peer {} from region {:?}", peer_id, region_id);
                self.remove_peer_from_region(region_id, peer_id).await?;
            }
        }
        Ok(())
    }

    /// Execute a region split
    async fn execute_split(&self, region_id: RegionId) -> Result<()> {
        let region = self.region_manager.get_region(region_id).ok_or_else(|| {
            Error::Internal(format!("Region {:?} not found", region_id))
        })?;

        // Find split key using sampled keys
        let (start_key, end_key) = region.key_range();

        // Calculate approximate midpoint for split
        let split_key = if end_key.is_empty() {
            // Unbounded region - use a heuristic midpoint
            let mut mid = start_key.clone();
            if mid.is_empty() {
                mid = vec![128]; // Middle of byte range
            } else {
                // Increment the last byte and add middle value
                mid.push(128);
            }
            mid
        } else {
            // Calculate midpoint between start and end keys
            let mut mid = Vec::with_capacity(start_key.len().max(end_key.len()));
            for i in 0..start_key.len().max(end_key.len()) {
                let s = *start_key.get(i).unwrap_or(&0) as u16;
                let e = *end_key.get(i).unwrap_or(&0) as u16;
                mid.push(((s + e) / 2) as u8);
            }
            mid
        };

        // Execute the split
        let new_region_id = self.region_manager.split_region(region_id, split_key.clone())?;

        tracing::info!(
            "Split region {:?} at key {:?}, created new region {:?}",
            region_id, split_key, new_region_id
        );

        // Notify other nodes about the split via Raft proposal
        let cmd = Command::Split {
            region_id,
            split_key,
        };

        if let Err(e) = self.propose_to_region(region_id, cmd).await {
            tracing::warn!("Failed to propose split command: {}", e);
        }

        Ok(())
    }

    /// Check and execute merge if appropriate
    async fn check_and_execute_merge(&self, region_id: RegionId) -> Result<()> {
        let region = self.region_manager.get_region(region_id).ok_or_else(|| {
            Error::Internal(format!("Region {:?} not found", region_id))
        })?;

        let (_start_key, end_key) = region.key_range();

        // Find adjacent region to merge with
        let adjacent_region_id = if !end_key.is_empty() {
            // Look for region starting at our end key
            self.region_manager.locate_key(&end_key)
        } else {
            None
        };

        if let Some(adj_id) = adjacent_region_id {
            if let Some(adj_region) = self.region_manager.get_region(adj_id) {
                // Check if adjacent region is also small enough for merge
                if adj_region.can_merge() {
                    tracing::info!(
                        "Merging regions {:?} and {:?}",
                        region_id, adj_id
                    );

                    // Propose merge command
                    let cmd = Command::Merge {
                        region_a: region_id,
                        region_b: adj_id,
                    };

                    if let Err(e) = self.propose_to_region(region_id, cmd).await {
                        tracing::warn!("Failed to propose merge command: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Transfer leadership to another node
    async fn transfer_leader(&self, region_id: RegionId, target: NodeId) -> Result<()> {
        let region = self.region_manager.get_region(region_id).ok_or_else(|| {
            Error::Internal(format!("Region {:?} not found", region_id))
        })?;

        if !region.is_leader() {
            return Err(Error::Internal("Not the leader for this region".to_string()));
        }

        // Find the peer ID for the target node
        let meta = region.meta();
        let target_peer = meta.peers.iter()
            .find(|p| p.node_id == target)
            .ok_or_else(|| Error::Internal(format!("Node {:?} is not a peer", target)))?;

        // Request Raft leadership transfer
        // In a real implementation, this would call raft_node.transfer_leader(target_peer.id)
        tracing::info!(
            "Initiating leader transfer for region {:?} to peer {} (node {:?})",
            region_id, target_peer.id, target
        );

        Ok(())
    }

    /// Add a peer to a region
    async fn add_peer_to_region(&self, region_id: RegionId, peer: Peer) -> Result<()> {
        let region = self.region_manager.get_region(region_id).ok_or_else(|| {
            Error::Internal(format!("Region {:?} not found", region_id))
        })?;

        if !region.is_leader() {
            return Err(Error::Internal("Not the leader for this region".to_string()));
        }

        // Propose add peer command
        let cmd = Command::AddPeer {
            region_id,
            peer: peer.node_id,
        };

        self.propose_to_region(region_id, cmd).await?;

        tracing::info!(
            "Added peer {:?} to region {:?}",
            peer.node_id, region_id
        );

        Ok(())
    }

    /// Remove a peer from a region
    async fn remove_peer_from_region(&self, region_id: RegionId, peer_id: u64) -> Result<()> {
        let region = self.region_manager.get_region(region_id).ok_or_else(|| {
            Error::Internal(format!("Region {:?} not found", region_id))
        })?;

        if !region.is_leader() {
            return Err(Error::Internal("Not the leader for this region".to_string()));
        }

        // Find the node ID for the peer
        let meta = region.meta();
        let peer = meta.peers.iter()
            .find(|p| p.id == peer_id)
            .ok_or_else(|| Error::Internal(format!("Peer {} not found", peer_id)))?;

        // Propose remove peer command
        let cmd = Command::RemovePeer {
            region_id,
            peer: peer.node_id,
        };

        self.propose_to_region(region_id, cmd).await?;

        tracing::info!(
            "Removed peer {} from region {:?}",
            peer_id, region_id
        );

        Ok(())
    }

    /// Handle failover for a failed node
    pub async fn handle_node_failure(&self, failed_node: NodeId) -> Result<()> {
        tracing::info!("Handling failure of node {:?}", failed_node);

        // Find all regions that have the failed node as a peer
        let affected_regions: Vec<(RegionId, bool)> = self.region_manager
            .all_regions()
            .iter()
            .filter_map(|region| {
                let meta = region.meta();
                let has_peer = meta.peers.iter().any(|p| p.node_id == failed_node);
                let is_leader = region.leader_id().map(|id| NodeId(id) == failed_node).unwrap_or(false);
                if has_peer {
                    Some((region.id(), is_leader))
                } else {
                    None
                }
            })
            .collect();

        for (region_id, was_leader) in affected_regions {
            if was_leader {
                // The failed node was the leader - Raft will handle election
                tracing::info!("Region {:?} lost its leader, waiting for new election", region_id);
            }

            // Schedule replacement of the failed peer
            let nodes = self.membership.node_infos();
            let scheduler = Scheduler::new(
                self.config.target_replicas,
                self.config.max_regions_per_node,
            );

            // Find a replacement node
            let regions = self.region_manager.region_infos();
            let available_nodes: Vec<_> = nodes.iter()
                .filter(|n| n.id != failed_node && n.status == NodeStatus::Online)
                .map(|n| {
                    let mut meta = crate::membership::NodeMeta::new(n.id, self.config.addr);
                    meta.status = n.status;
                    meta
                })
                .collect();

            let placement = scheduler.place_region(&available_nodes, &regions);

            if let Some(&new_node) = placement.first() {
                // Add new peer to replace failed one
                let new_peer = Peer::new(
                    self.next_op_id.fetch_add(1, Ordering::SeqCst),
                    new_node,
                );

                if let Err(e) = self.add_peer_to_region(region_id, new_peer).await {
                    tracing::error!("Failed to add replacement peer: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Handle an incoming message
    async fn handle_message(&self, msg: RaftMessage) -> Result<()> {
        // Find the region
        if let Some(_region) = self.region_manager.get_region(msg.region_id) {
            // Convert message and step the Raft node
            let _raft_msg = crate::raft_node::to_raft_message(&msg);
            // region.step(raft_msg)?;
            tracing::debug!("Received message for region {:?}: {:?}", msg.region_id, msg.msg_type);
        } else {
            tracing::warn!("Received message for unknown region {:?}", msg.region_id);
        }
        Ok(())
    }

    /// Handle a membership event
    async fn handle_membership_event(&self, event: MembershipEvent) {
        match event {
            MembershipEvent::NodeJoined(node_id) => {
                tracing::info!("Node {:?} joined the cluster", node_id);
            }
            MembershipEvent::NodeLeft(node_id) => {
                tracing::info!("Node {:?} left the cluster", node_id);

                // Handle as a node failure to ensure all regions are covered
                if let Err(e) = self.handle_node_failure(node_id).await {
                    tracing::error!(
                        node_id = ?node_id,
                        error = %e,
                        "Failed to handle regions for departed node"
                    );
                }

                // Check if rebalancing is needed
                let nodes = self.membership.node_infos();
                let regions = self.region_manager.region_infos();
                let scheduler = Scheduler::new(
                    self.config.target_replicas,
                    self.config.max_regions_per_node,
                );

                let node_metas: Vec<_> = nodes.iter()
                    .filter(|n| n.id != node_id && n.status == NodeStatus::Online)
                    .map(|n| {
                        let mut meta = crate::membership::NodeMeta::new(n.id, self.config.addr);
                        meta.status = n.status;
                        meta
                    })
                    .collect();

                if scheduler.needs_rebalance(&node_metas, &regions) {
                    let moves = scheduler.generate_moves(&node_metas, &regions);
                    for mv in moves {
                        tracing::info!(
                            "Rebalance: moving region {:?} from {:?} to {:?}",
                            mv.region_id, mv.from, mv.to
                        );
                        let new_peer = Peer::new(
                            self.next_op_id.fetch_add(1, Ordering::SeqCst),
                            mv.to,
                        );
                        if let Err(e) = self.add_peer_to_region(mv.region_id, new_peer).await {
                            tracing::error!("Failed to add peer during rebalance: {}", e);
                            continue;
                        }
                        // Remove the old peer
                        if let Some(region) = self.region_manager.get_region(mv.region_id) {
                            let meta = region.meta();
                            if let Some(old_peer) = meta.peers.iter().find(|p| p.node_id == mv.from) {
                                if let Err(e) = self.remove_peer_from_region(mv.region_id, old_peer.id).await {
                                    tracing::error!("Failed to remove old peer during rebalance: {}", e);
                                }
                            }
                        }
                    }
                }
            }
            MembershipEvent::HealthChanged { node_id, old, new } => {
                tracing::info!("Node {:?} health changed: {:?} -> {:?}", node_id, old, new);
                if new == HealthStatus::Failed {
                    if let Err(e) = self.handle_node_failure(node_id).await {
                        tracing::error!(
                            node_id = ?node_id,
                            error = %e,
                            "Failed to handle node failure"
                        );
                    }
                }
            }
            MembershipEvent::StatusChanged { node_id, old, new } => {
                tracing::info!("Node {:?} status changed: {:?} -> {:?}", node_id, old, new);
            }
            MembershipEvent::LeaderChanged { old, new } => {
                tracing::info!("Cluster leader changed: {:?} -> {:?}", old, new);
            }
        }
    }

    /// Get region manager
    pub fn region_manager(&self) -> &Arc<RegionManager> {
        &self.region_manager
    }

    /// Propose a command to a region
    pub async fn propose_to_region(&self, region_id: RegionId, cmd: Command) -> Result<()> {
        let region = self.region_manager.get_region(region_id).ok_or_else(|| {
            Error::Internal(format!("Region {:?} not found", region_id))
        })?;

        if !region.is_leader() {
            return Err(Error::Internal("Not leader for this region".to_string()));
        }

        region.propose(cmd)?;
        Ok(())
    }

    /// Route a command to the appropriate region
    pub async fn route_command(&self, key: &[u8], cmd: Command) -> Result<()> {
        let region_id = self.region_manager.locate_key(key).ok_or_else(|| {
            Error::Internal("No region found for key".to_string())
        })?;

        self.propose_to_region(region_id, cmd).await
    }
}

#[async_trait]
impl ClusterManager for ClusterCoordinator {
    fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    fn is_leader(&self, region_id: RegionId) -> bool {
        self.region_manager
            .get_region(region_id)
            .map(|r| r.is_leader())
            .unwrap_or(false)
    }

    async fn get_leader(&self, region_id: RegionId) -> Result<Option<NodeId>> {
        let region = self.region_manager.get_region(region_id).ok_or_else(|| {
            Error::Internal(format!("Region {:?} not found", region_id))
        })?;

        Ok(region.leader_id().map(NodeId))
    }

    async fn get_nodes(&self) -> Result<Vec<NodeInfo>> {
        Ok(self.membership.node_infos())
    }

    async fn get_regions(&self) -> Result<Vec<RegionInfo>> {
        Ok(self.region_manager.region_infos())
    }

    fn locate_region(&self, key: &[u8]) -> Option<RegionId> {
        self.region_manager.locate_key(key)
    }

    async fn propose(&self, region_id: RegionId, cmd: Command) -> Result<()> {
        self.propose_to_region(region_id, cmd).await
    }
}

/// Scheduler for region placement and load balancing
pub struct Scheduler {
    /// Target replicas per region
    target_replicas: usize,
    /// Maximum regions per node
    max_regions_per_node: usize,
}

impl Scheduler {
    pub fn new(target_replicas: usize, max_regions_per_node: usize) -> Self {
        Self {
            target_replicas,
            max_regions_per_node,
        }
    }

    /// Calculate region placement for a new region
    pub fn place_region(&self, nodes: &[NodeMeta], existing_regions: &[RegionInfo]) -> Vec<NodeId> {
        // Count regions per node
        let mut region_counts: HashMap<NodeId, usize> = HashMap::new();
        for region in existing_regions {
            for peer in &region.peers {
                *region_counts.entry(*peer).or_insert(0) += 1;
            }
        }

        // Sort nodes by region count (ascending)
        let mut candidates: Vec<_> = nodes
            .iter()
            .filter(|n| n.is_available())
            .filter(|n| {
                region_counts.get(&n.id).copied().unwrap_or(0) < self.max_regions_per_node
            })
            .collect();

        candidates.sort_by_key(|n| region_counts.get(&n.id).copied().unwrap_or(0));

        // Take the first target_replicas nodes
        candidates
            .into_iter()
            .take(self.target_replicas)
            .map(|n| n.id)
            .collect()
    }

    /// Check if rebalancing is needed
    pub fn needs_rebalance(&self, nodes: &[NodeMeta], regions: &[RegionInfo]) -> bool {
        if nodes.is_empty() || regions.is_empty() {
            return false;
        }

        // Count regions per node
        let mut region_counts: HashMap<NodeId, usize> = HashMap::new();
        for region in regions {
            for peer in &region.peers {
                *region_counts.entry(*peer).or_insert(0) += 1;
            }
        }

        let counts: Vec<usize> = nodes
            .iter()
            .map(|n| region_counts.get(&n.id).copied().unwrap_or(0))
            .collect();

        if counts.is_empty() {
            return false;
        }

        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap();

        // Rebalance if difference is more than 2 regions
        max - min > 2
    }

    /// Generate rebalance moves
    pub fn generate_moves(
        &self,
        nodes: &[NodeMeta],
        regions: &[RegionInfo],
    ) -> Vec<RebalanceMove> {
        let mut moves = Vec::new();

        // Count regions per node
        let mut region_counts: HashMap<NodeId, usize> = HashMap::new();
        let mut node_regions: HashMap<NodeId, Vec<RegionId>> = HashMap::new();

        for region in regions {
            for peer in &region.peers {
                *region_counts.entry(*peer).or_insert(0) += 1;
                node_regions
                    .entry(*peer)
                    .or_insert_with(Vec::new)
                    .push(region.id);
            }
        }

        let available_nodes: Vec<_> = nodes.iter().filter(|n| n.is_available()).collect();
        if available_nodes.is_empty() {
            return moves;
        }

        let target_per_node = regions.len() * self.target_replicas / available_nodes.len();

        // Find overloaded and underloaded nodes
        let overloaded: Vec<_> = available_nodes
            .iter()
            .filter(|n| region_counts.get(&n.id).copied().unwrap_or(0) > target_per_node + 1)
            .collect();

        let underloaded: Vec<_> = available_nodes
            .iter()
            .filter(|n| region_counts.get(&n.id).copied().unwrap_or(0) < target_per_node)
            .collect();

        // Generate moves from overloaded to underloaded
        for over_node in &overloaded {
            if underloaded.is_empty() {
                break;
            }

            let over_count = region_counts.get(&over_node.id).copied().unwrap_or(0);
            let excess = over_count.saturating_sub(target_per_node + 1);

            if let Some(regions) = node_regions.get(&over_node.id) {
                for (i, &region_id) in regions.iter().take(excess).enumerate() {
                    if let Some(under_node) = underloaded.get(i % underloaded.len()) {
                        moves.push(RebalanceMove {
                            region_id,
                            from: over_node.id,
                            to: under_node.id,
                        });
                    }
                }
            }
        }

        moves
    }
}

/// A move for rebalancing
#[derive(Debug, Clone)]
pub struct RebalanceMove {
    pub region_id: RegionId,
    pub from: NodeId,
    pub to: NodeId,
}

/// Failure detector using phi-accrual algorithm
pub struct PhiAccrualDetector {
    /// Sample window size
    window_size: usize,
    /// Heartbeat samples per node
    samples: DashMap<NodeId, Vec<Duration>>,
    /// Last heartbeat time per node
    last_heartbeat: DashMap<NodeId, Instant>,
    /// Phi threshold for suspicion
    phi_threshold: f64,
}

impl PhiAccrualDetector {
    pub fn new(window_size: usize, phi_threshold: f64) -> Self {
        Self {
            window_size,
            samples: DashMap::new(),
            last_heartbeat: DashMap::new(),
            phi_threshold,
        }
    }

    /// Record a heartbeat
    pub fn heartbeat(&self, node_id: NodeId) {
        let now = Instant::now();

        if let Some(last) = self.last_heartbeat.get(&node_id) {
            let interval = now.duration_since(*last);

            let mut samples = self.samples.entry(node_id).or_insert_with(Vec::new);
            samples.push(interval);

            // Keep only window_size samples
            if samples.len() > self.window_size {
                samples.remove(0);
            }
        }

        self.last_heartbeat.insert(node_id, now);
    }

    /// Calculate phi for a node
    pub fn phi(&self, node_id: NodeId) -> f64 {
        let samples = match self.samples.get(&node_id) {
            Some(s) if s.len() >= 3 => s,
            _ => return 0.0, // Not enough samples
        };

        let last = match self.last_heartbeat.get(&node_id) {
            Some(t) => *t,
            None => return 0.0,
        };

        let since_last = last.elapsed();

        // Calculate mean and variance
        let mean: f64 = samples.iter().map(|d| d.as_secs_f64()).sum::<f64>() / samples.len() as f64;
        let variance: f64 = samples
            .iter()
            .map(|d| {
                let diff = d.as_secs_f64() - mean;
                diff * diff
            })
            .sum::<f64>()
            / samples.len() as f64;

        let std_dev = variance.sqrt();

        if std_dev == 0.0 {
            return 0.0;
        }

        // Calculate phi using normal distribution CDF approximation
        let t = since_last.as_secs_f64();
        let z = (t - mean) / std_dev;

        // Approximate erf using Horner's method (Abramowitz and Stegun approximation)
        fn erf_approx(x: f64) -> f64 {
            let a1 = 0.254829592;
            let a2 = -0.284496736;
            let a3 = 1.421413741;
            let a4 = -1.453152027;
            let a5 = 1.061405429;
            let p = 0.3275911;

            let sign = if x < 0.0 { -1.0 } else { 1.0 };
            let x = x.abs();

            let t = 1.0 / (1.0 + p * x);
            let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

            sign * y
        }

        // Phi = -log10(1 - CDF(z))
        let cdf = 0.5 * (1.0 + erf_approx(z / std::f64::consts::SQRT_2));
        if cdf >= 1.0 {
            return 16.0; // Max phi
        }

        // log10(x) = ln(x) / ln(10)
        -(1.0 - cdf).ln() / std::f64::consts::LN_10
    }

    /// Check if a node is suspected
    pub fn is_suspected(&self, node_id: NodeId) -> bool {
        self.phi(node_id) > self.phi_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::InMemoryTransport;

    fn test_config() -> CoordinatorConfig {
        CoordinatorConfig {
            node_id: NodeId(1),
            addr: "127.0.0.1:5000".parse().unwrap(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_coordinator_bootstrap() {
        let config = test_config();
        let transport = Arc::new(InMemoryTransport::with_default_config(config.node_id));
        let coordinator = ClusterCoordinator::new(config, transport);

        coordinator.bootstrap().await.unwrap();

        assert_eq!(coordinator.cluster_state(), ClusterState::Running);
        assert_eq!(coordinator.region_manager().region_count(), 1);
    }

    #[tokio::test]
    async fn test_coordinator_locate_region() {
        let config = test_config();
        let transport = Arc::new(InMemoryTransport::with_default_config(config.node_id));
        let coordinator = ClusterCoordinator::new(config, transport);

        coordinator.bootstrap().await.unwrap();

        // Should find region for any key
        let region_id = coordinator.locate_region(&[100]);
        assert!(region_id.is_some());
    }

    #[test]
    fn test_scheduler_place_region() {
        let scheduler = Scheduler::new(3, 100);

        let nodes: Vec<NodeMeta> = (1..=5)
            .map(|i| {
                let mut meta = NodeMeta::new(
                    NodeId(i),
                    format!("127.0.0.1:{}", 5000 + i).parse().unwrap(),
                );
                meta.status = NodeStatus::Online;
                meta.health = HealthStatus::Healthy;
                meta
            })
            .collect();

        let placement = scheduler.place_region(&nodes, &[]);
        assert_eq!(placement.len(), 3);
    }

    #[test]
    fn test_scheduler_needs_rebalance() {
        let scheduler = Scheduler::new(3, 100);

        let nodes: Vec<NodeMeta> = (1..=3)
            .map(|i| {
                let mut meta = NodeMeta::new(
                    NodeId(i),
                    format!("127.0.0.1:{}", 5000 + i).parse().unwrap(),
                );
                meta.status = NodeStatus::Online;
                meta.health = HealthStatus::Healthy;
                meta
            })
            .collect();

        // Unbalanced regions (all on node 1)
        let regions = vec![
            RegionInfo {
                id: RegionId(1),
                start_key: vec![],
                end_key: vec![100],
                leader: Some(NodeId(1)),
                peers: vec![NodeId(1)],
                size_bytes: 1000,
            },
            RegionInfo {
                id: RegionId(2),
                start_key: vec![100],
                end_key: vec![200],
                leader: Some(NodeId(1)),
                peers: vec![NodeId(1)],
                size_bytes: 1000,
            },
            RegionInfo {
                id: RegionId(3),
                start_key: vec![200],
                end_key: vec![],
                leader: Some(NodeId(1)),
                peers: vec![NodeId(1)],
                size_bytes: 1000,
            },
        ];

        // With all regions on one node, should need rebalance
        assert!(scheduler.needs_rebalance(&nodes, &regions));
    }

    #[test]
    fn test_phi_accrual_detector() {
        let detector = PhiAccrualDetector::new(10, 8.0);

        // Simulate regular heartbeats
        for _ in 0..5 {
            detector.heartbeat(NodeId(1));
            std::thread::sleep(Duration::from_millis(10));
        }

        // Phi should be low after regular heartbeats
        let phi = detector.phi(NodeId(1));
        assert!(phi < 8.0, "Phi should be low: {}", phi);
    }

    #[test]
    fn test_rebalance_move() {
        let scheduler = Scheduler::new(1, 100);

        let nodes: Vec<NodeMeta> = (1..=3)
            .map(|i| {
                let mut meta = NodeMeta::new(
                    NodeId(i),
                    format!("127.0.0.1:{}", 5000 + i).parse().unwrap(),
                );
                meta.status = NodeStatus::Online;
                meta.health = HealthStatus::Healthy;
                meta
            })
            .collect();

        // All regions on node 1
        let regions = vec![
            RegionInfo {
                id: RegionId(1),
                start_key: vec![],
                end_key: vec![100],
                leader: Some(NodeId(1)),
                peers: vec![NodeId(1)],
                size_bytes: 1000,
            },
            RegionInfo {
                id: RegionId(2),
                start_key: vec![100],
                end_key: vec![200],
                leader: Some(NodeId(1)),
                peers: vec![NodeId(1)],
                size_bytes: 1000,
            },
            RegionInfo {
                id: RegionId(3),
                start_key: vec![200],
                end_key: vec![],
                leader: Some(NodeId(1)),
                peers: vec![NodeId(1)],
                size_bytes: 1000,
            },
        ];

        let moves = scheduler.generate_moves(&nodes, &regions);
        // Should generate moves to balance across nodes
        assert!(!moves.is_empty() || nodes.len() < 2);
    }

    #[tokio::test]
    async fn test_handle_node_failure_no_regions() {
        let config = test_config();
        let transport = Arc::new(InMemoryTransport::with_default_config(config.node_id));
        let coordinator = ClusterCoordinator::new(config, transport);
        coordinator.bootstrap().await.unwrap();

        // Node 99 has no regions - failure handling should succeed as no-op
        let result = coordinator.handle_node_failure(NodeId(99)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_membership_health_changed_to_failed() {
        let config = test_config();
        let transport = Arc::new(InMemoryTransport::with_default_config(config.node_id));
        let coordinator = ClusterCoordinator::new(config, transport);
        coordinator.bootstrap().await.unwrap();

        // Simulate health change to Failed for a non-existent node
        let event = MembershipEvent::HealthChanged {
            node_id: NodeId(99),
            old: HealthStatus::Healthy,
            new: HealthStatus::Failed,
        };

        // Should not panic - calls handle_node_failure internally
        coordinator.handle_membership_event(event).await;
        assert_eq!(coordinator.cluster_state(), ClusterState::Running);
    }

    #[tokio::test]
    async fn test_membership_node_left_triggers_rebalance_check() {
        let config = test_config();
        let transport = Arc::new(InMemoryTransport::with_default_config(config.node_id));
        let coordinator = ClusterCoordinator::new(config, transport);
        coordinator.bootstrap().await.unwrap();

        // Simulate a node leaving
        let event = MembershipEvent::NodeLeft(NodeId(99));

        // Should handle gracefully even when node had no regions
        coordinator.handle_membership_event(event).await;

        // Cluster should still be running
        assert_eq!(coordinator.cluster_state(), ClusterState::Running);
    }

    #[test]
    fn test_scheduler_generate_moves_unbalanced() {
        let scheduler = Scheduler::new(1, 100);

        let nodes: Vec<NodeMeta> = (1..=3)
            .map(|i| {
                let mut meta = NodeMeta::new(
                    NodeId(i),
                    format!("127.0.0.1:{}", 5000 + i).parse().unwrap(),
                );
                meta.status = NodeStatus::Online;
                meta.health = HealthStatus::Healthy;
                meta
            })
            .collect();

        // All 5 regions on node 1 - highly unbalanced
        let regions: Vec<RegionInfo> = (1..=5)
            .map(|i| RegionInfo {
                id: RegionId(i),
                start_key: vec![],
                end_key: vec![],
                leader: Some(NodeId(1)),
                peers: vec![NodeId(1)],
                size_bytes: 1000,
            })
            .collect();

        let moves = scheduler.generate_moves(&nodes, &regions);
        // Should produce moves from node 1 to nodes 2 and 3
        assert!(!moves.is_empty());
        for mv in &moves {
            assert_eq!(mv.from, NodeId(1));
            assert!(mv.to == NodeId(2) || mv.to == NodeId(3));
        }
    }
}
