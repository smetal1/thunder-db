//! Gossip Protocol Implementation
//!
//! Implements Cassandra-style Scuttlebutt anti-entropy gossip for cluster discovery
//! and failure detection. Uses a 3-way handshake (SYN/ACK/ACK2) for state exchange.
//!
//! # Protocol Overview
//! ```text
//! Node A                          Node B
//!   │                                │
//!   │──── SYN(digest_a) ────────────►│
//!   │                                │
//!   │◄─── ACK(digest_b, deltas) ─────│
//!   │                                │
//!   │──── ACK2(deltas) ─────────────►│
//!   │                                │
//! ```

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use thunder_common::prelude::*;
use tracing::{debug, error, info, warn};

use crate::membership::{HealthStatus, Membership, MembershipEvent, NodeMeta};
use crate::transport::NetworkTransport;
use crate::NodeStatus;

/// Gossip configuration
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// Interval between gossip rounds
    pub gossip_interval: Duration,
    /// Number of nodes to gossip with per round
    pub gossip_fanout: usize,
    /// Phi threshold for failure detection (typically 8.0)
    pub phi_threshold: f64,
    /// Minimum samples for phi calculation
    pub phi_min_samples: usize,
    /// Bootstrap timeout when contacting seeds
    pub bootstrap_timeout: Duration,
    /// Quarantine time for failed nodes before removal
    pub quarantine_duration: Duration,
    /// Whether this node is a seed node
    pub is_seed: bool,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: Duration::from_secs(1),
            gossip_fanout: 3,
            phi_threshold: 8.0,
            phi_min_samples: 5,
            bootstrap_timeout: Duration::from_secs(30),
            quarantine_duration: Duration::from_secs(300),
            is_seed: false,
        }
    }
}

/// Application state keys for versioned values
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum AppStateKey {
    /// Node status (online, leaving, etc.)
    Status,
    /// Node load/capacity info
    Load,
    /// Region assignments
    Regions,
    /// Schema version
    Schema,
    /// Custom application state
    Custom(String),
}

/// Versioned value for conflict resolution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedValue {
    /// The actual value (serialized)
    pub value: Vec<u8>,
    /// Version number (monotonically increasing)
    pub version: u64,
}

impl VersionedValue {
    pub fn new(value: Vec<u8>, version: u64) -> Self {
        Self { value, version }
    }
}

/// Heartbeat state for a node
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatState {
    /// Generation: changes when node restarts (typically startup timestamp)
    pub generation: u64,
    /// Version: monotonically increasing counter
    pub version: AtomicU64,
}

impl Clone for HeartbeatState {
    fn clone(&self) -> Self {
        Self {
            generation: self.generation,
            version: AtomicU64::new(self.version.load(Ordering::SeqCst)),
        }
    }
}

impl HeartbeatState {
    pub fn new() -> Self {
        let generation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            generation,
            version: AtomicU64::new(0),
        }
    }

    pub fn with_generation(generation: u64) -> Self {
        Self {
            generation,
            version: AtomicU64::new(0),
        }
    }

    pub fn increment(&self) -> u64 {
        self.version.fetch_add(1, Ordering::SeqCst)
    }

    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }
}

impl Default for HeartbeatState {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete state for a remote endpoint (other node)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointState {
    /// Node ID
    pub node_id: NodeId,
    /// Node address
    pub addr: SocketAddr,
    /// Heartbeat state
    pub heartbeat: HeartbeatState,
    /// Application states with versions
    pub app_states: HashMap<AppStateKey, VersionedValue>,
    /// When we last heard from this node
    #[serde(skip)]
    pub updated_at: Option<Instant>,
    /// Is this endpoint alive?
    pub is_alive: bool,
}

impl EndpointState {
    pub fn new(node_id: NodeId, addr: SocketAddr) -> Self {
        Self {
            node_id,
            addr,
            heartbeat: HeartbeatState::new(),
            app_states: HashMap::new(),
            updated_at: Some(Instant::now()),
            is_alive: true,
        }
    }

    pub fn max_version(&self) -> u64 {
        let heartbeat_ver = self.heartbeat.current_version();
        let app_max = self
            .app_states
            .values()
            .map(|v| v.version)
            .max()
            .unwrap_or(0);
        std::cmp::max(heartbeat_ver, app_max)
    }

    pub fn touch(&mut self) {
        self.updated_at = Some(Instant::now());
    }
}

/// Digest entry for a single endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipDigestEntry {
    /// Node ID
    pub node_id: NodeId,
    /// Generation (changes on restart)
    pub generation: u64,
    /// Maximum version we know about
    pub max_version: u64,
}

/// Gossip digest containing known state versions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipDigest {
    /// Entries for all known nodes
    pub entries: Vec<GossipDigestEntry>,
}

impl GossipDigest {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add(&mut self, node_id: NodeId, generation: u64, max_version: u64) {
        self.entries.push(GossipDigestEntry {
            node_id,
            generation,
            max_version,
        });
    }
}

impl Default for GossipDigest {
    fn default() -> Self {
        Self::new()
    }
}

/// State update for a single endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointStateUpdate {
    /// The endpoint being updated
    pub endpoint: EndpointState,
}

/// Gossip message types for the 3-way handshake
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    /// SYN: "Here's what I know, what do you have?"
    Syn {
        /// Sender's node ID
        from: NodeId,
        /// Sender's address
        from_addr: SocketAddr,
        /// Cluster name for validation
        cluster_name: String,
        /// Our knowledge digest
        digest: GossipDigest,
    },

    /// ACK: "Here's what you're missing + my digest"
    Ack {
        /// Sender's node ID
        from: NodeId,
        /// Our digest for comparison
        digest: GossipDigest,
        /// Updates the other node is missing
        updates: Vec<EndpointStateUpdate>,
    },

    /// ACK2: "Here's what you're missing"
    Ack2 {
        /// Sender's node ID
        from: NodeId,
        /// Updates the other node is missing
        updates: Vec<EndpointStateUpdate>,
    },
}

impl GossipMessage {
    pub fn sender(&self) -> NodeId {
        match self {
            GossipMessage::Syn { from, .. } => *from,
            GossipMessage::Ack { from, .. } => *from,
            GossipMessage::Ack2 { from, .. } => *from,
        }
    }
}

/// Phi-accrual failure detector
/// Based on "The Phi Accrual Failure Detector" by Hayashibara et al.
pub struct PhiAccrualDetector {
    /// Heartbeat arrival intervals per node
    intervals: DashMap<NodeId, VecDeque<f64>>,
    /// Last heartbeat time per node
    last_heartbeat: DashMap<NodeId, Instant>,
    /// Minimum samples before phi calculation
    min_samples: usize,
    /// Maximum samples to keep
    max_samples: usize,
}

impl PhiAccrualDetector {
    pub fn new(min_samples: usize) -> Self {
        Self {
            intervals: DashMap::new(),
            last_heartbeat: DashMap::new(),
            min_samples,
            max_samples: 1000,
        }
    }

    /// Report a heartbeat from a node
    pub fn report_heartbeat(&self, node_id: NodeId) {
        let now = Instant::now();

        if let Some(last) = self.last_heartbeat.get(&node_id) {
            let interval = now.duration_since(*last).as_secs_f64() * 1000.0; // ms

            let mut intervals = self.intervals.entry(node_id).or_insert_with(VecDeque::new);
            intervals.push_back(interval);

            // Keep only recent samples
            while intervals.len() > self.max_samples {
                intervals.pop_front();
            }
        }

        self.last_heartbeat.insert(node_id, now);
    }

    /// Calculate phi value for a node
    /// Higher phi = more likely the node has failed
    pub fn phi(&self, node_id: NodeId) -> f64 {
        let last = match self.last_heartbeat.get(&node_id) {
            Some(t) => *t,
            None => return 0.0, // No data yet
        };

        let intervals = match self.intervals.get(&node_id) {
            Some(i) => i,
            None => return 0.0,
        };

        if intervals.len() < self.min_samples {
            return 0.0; // Not enough samples
        }

        // Calculate mean and standard deviation
        let sum: f64 = intervals.iter().sum();
        let mean = sum / intervals.len() as f64;

        let variance: f64 = intervals
            .iter()
            .map(|x| {
                let diff = x - mean;
                diff * diff
            })
            .sum::<f64>()
            / intervals.len() as f64;

        let std_dev = variance.sqrt();

        // Time since last heartbeat (ms)
        let t = last.elapsed().as_secs_f64() * 1000.0;

        // Calculate phi using exponential distribution approximation
        // phi = -log10(1 - F(t)) where F is the CDF
        if std_dev < 0.001 {
            // Avoid division by zero
            if t > mean * 2.0 {
                return 16.0; // Very high phi
            }
            return 0.0;
        }

        let y = (t - mean) / std_dev;
        let e = (-y * (std::f64::consts::PI / (8.0_f64.sqrt()))).exp();
        let p = 1.0 / (1.0 + e);

        if p >= 1.0 {
            return 16.0; // Cap at high value
        }

        -p.log10()
    }

    /// Check if a node is considered failed based on phi threshold
    pub fn is_failed(&self, node_id: NodeId, threshold: f64) -> bool {
        self.phi(node_id) > threshold
    }

    /// Remove tracking for a node
    pub fn remove(&self, node_id: NodeId) {
        self.intervals.remove(&node_id);
        self.last_heartbeat.remove(&node_id);
    }
}

/// Gossip event for internal communication
#[derive(Debug, Clone)]
pub enum GossipEvent {
    /// A new node was discovered
    NodeDiscovered(NodeId, SocketAddr),
    /// A node is now considered alive
    NodeAlive(NodeId),
    /// A node is now considered dead
    NodeDead(NodeId),
    /// State was updated for a node
    StateUpdated(NodeId),
}

/// The main Gossiper implementation
pub struct Gossiper {
    /// This node's ID
    node_id: NodeId,
    /// This node's address
    addr: SocketAddr,
    /// Cluster name for validation
    cluster_name: String,
    /// Configuration
    config: GossipConfig,
    /// Local endpoint state
    local_state: RwLock<EndpointState>,
    /// Known endpoints (including self)
    endpoints: DashMap<NodeId, EndpointState>,
    /// Seed nodes for bootstrap
    seeds: Vec<SocketAddr>,
    /// Phi-accrual failure detector
    phi_detector: PhiAccrualDetector,
    /// Membership manager reference
    membership: Arc<Membership>,
    /// Shutdown token
    shutdown: CancellationToken,
    /// Event sender for gossip events
    event_tx: broadcast::Sender<GossipEvent>,
    /// Incoming gossip message channel
    incoming_tx: mpsc::Sender<(SocketAddr, GossipMessage)>,
    incoming_rx: tokio::sync::Mutex<mpsc::Receiver<(SocketAddr, GossipMessage)>>,
    /// Is bootstrapped
    bootstrapped: AtomicBool,
    /// Dead nodes in quarantine (node_id -> time marked dead)
    quarantine: DashMap<NodeId, Instant>,
}

impl Gossiper {
    /// Create a new Gossiper
    pub fn new(
        node_id: NodeId,
        addr: SocketAddr,
        cluster_name: String,
        seeds: Vec<SocketAddr>,
        membership: Arc<Membership>,
        config: GossipConfig,
    ) -> Arc<Self> {
        let (event_tx, _) = broadcast::channel(1000);
        let (incoming_tx, incoming_rx) = mpsc::channel(1000);

        let local_state = EndpointState::new(node_id, addr);

        let gossiper = Arc::new(Self {
            node_id,
            addr,
            cluster_name,
            config: config.clone(),
            local_state: RwLock::new(local_state.clone()),
            endpoints: DashMap::new(),
            seeds,
            phi_detector: PhiAccrualDetector::new(config.phi_min_samples),
            membership,
            shutdown: CancellationToken::new(),
            event_tx,
            incoming_tx,
            incoming_rx: tokio::sync::Mutex::new(incoming_rx),
            bootstrapped: AtomicBool::new(false),
            quarantine: DashMap::new(),
        });

        // Add self to endpoints
        gossiper.endpoints.insert(node_id, local_state);

        gossiper
    }

    /// Get the incoming message sender for the transport layer
    pub fn incoming_sender(&self) -> mpsc::Sender<(SocketAddr, GossipMessage)> {
        self.incoming_tx.clone()
    }

    /// Subscribe to gossip events
    pub fn subscribe(&self) -> broadcast::Receiver<GossipEvent> {
        self.event_tx.subscribe()
    }

    /// Get all discovered nodes
    pub fn discovered_nodes(&self) -> Vec<(NodeId, SocketAddr)> {
        self.endpoints
            .iter()
            .filter(|e| e.node_id != self.node_id && e.is_alive)
            .map(|e| (e.node_id, e.addr))
            .collect()
    }

    /// Check if gossiper has bootstrapped
    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrapped.load(Ordering::SeqCst)
    }

    /// Bootstrap the gossiper by contacting seed nodes
    pub async fn bootstrap(&self) -> Result<()> {
        info!(
            node_id = ?self.node_id,
            seeds = ?self.seeds,
            "Starting gossip bootstrap"
        );

        // If we have no seeds or we are the only seed, bootstrap alone
        if self.seeds.is_empty() {
            info!("No seeds configured, bootstrapping as first node");
            self.bootstrapped.store(true, Ordering::SeqCst);
            return Ok(());
        }

        // Filter out our own address from seeds
        let other_seeds: Vec<_> = self
            .seeds
            .iter()
            .filter(|s| **s != self.addr)
            .cloned()
            .collect();

        if other_seeds.is_empty() && self.config.is_seed {
            info!("This is the only seed node, bootstrapping alone");
            self.bootstrapped.store(true, Ordering::SeqCst);
            return Ok(());
        }

        // Try to contact each seed
        let mut contacted = false;
        for seed in &other_seeds {
            info!(seed = %seed, "Attempting to contact seed");

            match self.send_syn_to_addr(*seed).await {
                Ok(response) => {
                    info!(seed = %seed, "Successfully contacted seed");
                    self.handle_gossip_message(*seed, response).await?;
                    contacted = true;
                    break;
                }
                Err(e) => {
                    warn!(seed = %seed, error = %e, "Failed to contact seed");
                    continue;
                }
            }
        }

        if contacted {
            self.bootstrapped.store(true, Ordering::SeqCst);
            info!(
                discovered = self.endpoints.len() - 1,
                "Bootstrap complete"
            );
            Ok(())
        } else if self.config.is_seed {
            // We're a seed but couldn't contact others - bootstrap alone
            info!("Could not contact other seeds, bootstrapping as seed node");
            self.bootstrapped.store(true, Ordering::SeqCst);
            Ok(())
        } else {
            Err(Error::Internal(
                "Failed to contact any seed nodes".to_string(),
            ))
        }
    }

    /// Run the gossip loop
    pub async fn run(self: &Arc<Self>) -> Result<()> {
        let mut interval = tokio::time::interval(self.config.gossip_interval);

        info!(
            node_id = ?self.node_id,
            interval = ?self.config.gossip_interval,
            "Starting gossip loop"
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.do_gossip_round().await {
                        warn!(error = %e, "Gossip round failed");
                    }
                }

                msg = async {
                    let mut rx = self.incoming_rx.lock().await;
                    rx.recv().await
                } => {
                    if let Some((from_addr, message)) = msg {
                        if let Err(e) = self.handle_gossip_message(from_addr, message).await {
                            warn!(error = %e, "Failed to handle gossip message");
                        }
                    }
                }

                _ = self.shutdown.cancelled() => {
                    info!("Gossiper shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Perform one round of gossip
    async fn do_gossip_round(&self) -> Result<()> {
        // 1. Increment our heartbeat version
        {
            let mut local = self.local_state.write();
            local.heartbeat.increment();
            local.touch();
            // Update in endpoints map too
            if let Some(mut ep) = self.endpoints.get_mut(&self.node_id) {
                ep.heartbeat = HeartbeatState::with_generation(local.heartbeat.generation);
                ep.heartbeat
                    .version
                    .store(local.heartbeat.current_version(), Ordering::SeqCst);
            }
        }

        // 2. Select random peers to gossip with
        let targets = self.select_gossip_targets();

        if targets.is_empty() {
            // No peers yet, try seeds
            if !self.seeds.is_empty() {
                let seed_idx = rand::random::<usize>() % self.seeds.len();
                if self.seeds[seed_idx] != self.addr {
                    debug!(seed = %self.seeds[seed_idx], "Gossiping with seed");
                    let _ = self.send_syn_to_addr(self.seeds[seed_idx]).await;
                }
            }
            return Ok(());
        }

        // 3. Send SYN to selected targets
        for target in targets {
            if let Some(endpoint) = self.endpoints.get(&target) {
                let addr = endpoint.addr;
                drop(endpoint);

                debug!(target = ?target, addr = %addr, "Sending gossip SYN");
                match self.send_syn_to_addr(addr).await {
                    Ok(response) => {
                        self.phi_detector.report_heartbeat(target);
                        if let Err(e) = self.handle_gossip_message(addr, response).await {
                            warn!(target = ?target, error = %e, "Failed to handle ACK");
                        }
                    }
                    Err(e) => {
                        debug!(target = ?target, error = %e, "Failed to send SYN");
                    }
                }
            }
        }

        // 4. Check for failed nodes
        self.check_failures().await;

        // 5. Clean up quarantined nodes
        self.cleanup_quarantine();

        Ok(())
    }

    /// Select random nodes for gossip
    fn select_gossip_targets(&self) -> Vec<NodeId> {
        let alive_nodes: Vec<NodeId> = self
            .endpoints
            .iter()
            .filter(|e| e.node_id != self.node_id && e.is_alive)
            .map(|e| e.node_id)
            .collect();

        if alive_nodes.is_empty() {
            return vec![];
        }

        // Randomly select up to gossip_fanout nodes
        let count = std::cmp::min(self.config.gossip_fanout, alive_nodes.len());
        let mut targets = Vec::with_capacity(count);
        let mut available = alive_nodes;

        for _ in 0..count {
            if available.is_empty() {
                break;
            }
            let idx = rand::random::<usize>() % available.len();
            targets.push(available.remove(idx));
        }

        targets
    }

    /// Create a digest of our known state
    fn create_digest(&self) -> GossipDigest {
        let mut digest = GossipDigest::new();

        for entry in self.endpoints.iter() {
            digest.add(
                entry.node_id,
                entry.heartbeat.generation,
                entry.max_version(),
            );
        }

        digest
    }

    /// Send SYN message to an address
    async fn send_syn_to_addr(&self, addr: SocketAddr) -> Result<GossipMessage> {
        let syn = GossipMessage::Syn {
            from: self.node_id,
            from_addr: self.addr,
            cluster_name: self.cluster_name.clone(),
            digest: self.create_digest(),
        };

        // In a real implementation, this would use gRPC
        // For now, we'll use a placeholder that will be implemented in transport.rs
        self.send_gossip_message(addr, syn).await
    }

    /// Send a gossip message (placeholder - will be implemented via transport)
    async fn send_gossip_message(
        &self,
        _addr: SocketAddr,
        msg: GossipMessage,
    ) -> Result<GossipMessage> {
        // This will be replaced with actual gRPC transport
        // For now, return a mock ACK
        debug!(msg_type = ?std::mem::discriminant(&msg), "Would send gossip message");

        // Return empty ACK for now
        Ok(GossipMessage::Ack {
            from: self.node_id,
            digest: GossipDigest::new(),
            updates: vec![],
        })
    }

    /// Handle an incoming gossip message
    async fn handle_gossip_message(
        &self,
        from_addr: SocketAddr,
        message: GossipMessage,
    ) -> Result<()> {
        match message {
            GossipMessage::Syn {
                from,
                from_addr: sender_addr,
                cluster_name,
                digest,
            } => {
                // Validate cluster name
                if cluster_name != self.cluster_name {
                    warn!(
                        expected = %self.cluster_name,
                        received = %cluster_name,
                        "Received gossip from different cluster"
                    );
                    return Err(Error::Internal("Cluster name mismatch".to_string()));
                }

                // Record the sender if new
                self.maybe_add_endpoint(from, sender_addr);

                // Report heartbeat
                self.phi_detector.report_heartbeat(from);

                // Calculate what they're missing and what we're missing
                let (their_missing, our_missing) = self.compare_digest(&digest);

                // Create updates for what they're missing
                let updates = self.create_updates(&their_missing);

                // Send ACK with our digest and their missing updates
                let ack = GossipMessage::Ack {
                    from: self.node_id,
                    digest: self.create_digest(),
                    updates,
                };

                // Would send ack back via transport
                debug!(target = ?from, "Prepared ACK response");
                let _ = self.send_gossip_message(from_addr, ack).await;

                // Apply updates for what we're missing
                self.request_and_apply_updates(&our_missing);
            }

            GossipMessage::Ack {
                from,
                digest,
                updates,
            } => {
                // Report heartbeat
                self.phi_detector.report_heartbeat(from);

                // Apply the updates they sent
                for update in updates {
                    self.apply_endpoint_update(update);
                }

                // Calculate what they're still missing
                let (their_missing, _) = self.compare_digest(&digest);

                if !their_missing.is_empty() {
                    // Send ACK2 with remaining updates
                    let updates = self.create_updates(&their_missing);
                    let ack2 = GossipMessage::Ack2 {
                        from: self.node_id,
                        updates,
                    };

                    let _ = self.send_gossip_message(from_addr, ack2).await;
                }
            }

            GossipMessage::Ack2 { from, updates } => {
                // Report heartbeat
                self.phi_detector.report_heartbeat(from);

                // Apply the updates
                for update in updates {
                    self.apply_endpoint_update(update);
                }
            }
        }

        Ok(())
    }

    /// Compare a digest to find differences
    fn compare_digest(&self, remote: &GossipDigest) -> (Vec<NodeId>, Vec<(NodeId, u64)>) {
        let mut they_need = Vec::new();
        let mut we_need = Vec::new();

        // Build a map of remote's knowledge
        let remote_map: HashMap<NodeId, (u64, u64)> = remote
            .entries
            .iter()
            .map(|e| (e.node_id, (e.generation, e.max_version)))
            .collect();

        // Check what we have that they don't (or is newer)
        for entry in self.endpoints.iter() {
            match remote_map.get(&entry.node_id) {
                None => {
                    // They don't know about this node at all
                    they_need.push(entry.node_id);
                }
                Some((their_gen, their_ver)) => {
                    let our_gen = entry.heartbeat.generation;
                    let our_ver = entry.max_version();

                    if our_gen > *their_gen || (our_gen == *their_gen && our_ver > *their_ver) {
                        // We have newer info
                        they_need.push(entry.node_id);
                    }
                }
            }
        }

        // Check what they have that we don't
        for entry in &remote.entries {
            match self.endpoints.get(&entry.node_id) {
                None => {
                    // We don't know about this node
                    we_need.push((entry.node_id, 0));
                }
                Some(our_entry) => {
                    let our_gen = our_entry.heartbeat.generation;
                    let our_ver = our_entry.max_version();

                    if entry.generation > our_gen
                        || (entry.generation == our_gen && entry.max_version > our_ver)
                    {
                        // They have newer info
                        we_need.push((entry.node_id, our_ver));
                    }
                }
            }
        }

        (they_need, we_need)
    }

    /// Create updates for nodes the remote is missing
    fn create_updates(&self, node_ids: &[NodeId]) -> Vec<EndpointStateUpdate> {
        node_ids
            .iter()
            .filter_map(|id| {
                self.endpoints.get(id).map(|e| EndpointStateUpdate {
                    endpoint: e.clone(),
                })
            })
            .collect()
    }

    /// Request and apply updates for nodes we're missing
    fn request_and_apply_updates(&self, _needed: &[(NodeId, u64)]) {
        // In the 3-way handshake, we'll receive these in ACK2
        // For now, just log
        debug!(count = _needed.len(), "Need updates for nodes");
    }

    /// Apply an endpoint update from gossip
    fn apply_endpoint_update(&self, update: EndpointStateUpdate) {
        let node_id = update.endpoint.node_id;
        let new_gen = update.endpoint.heartbeat.generation;
        let new_ver = update.endpoint.max_version();

        match self.endpoints.get(&node_id) {
            Some(existing) => {
                let existing_gen = existing.heartbeat.generation;
                let existing_ver = existing.max_version();

                // Only apply if newer
                if new_gen > existing_gen || (new_gen == existing_gen && new_ver > existing_ver) {
                    drop(existing);
                    let mut ep = update.endpoint;
                    ep.touch();
                    self.endpoints.insert(node_id, ep);

                    // Notify about state update
                    let _ = self.event_tx.send(GossipEvent::StateUpdated(node_id));

                    // Update membership
                    if let Some(mut node_meta) = self.membership.get_node(node_id) {
                        node_meta.heartbeat();
                        let _ = self.membership.add_node(node_meta);
                    }

                    debug!(node = ?node_id, "Applied endpoint update");
                }
            }
            None => {
                // New node discovered
                let addr = update.endpoint.addr;
                let mut ep = update.endpoint;
                ep.touch();
                self.endpoints.insert(node_id, ep);

                info!(node = ?node_id, addr = %addr, "Discovered new node via gossip");

                // Add to membership
                let node_meta = NodeMeta::new(node_id, addr);
                let _ = self.membership.add_node(node_meta);

                // Notify about discovery
                let _ = self.event_tx.send(GossipEvent::NodeDiscovered(node_id, addr));
            }
        }
    }

    /// Maybe add a new endpoint
    fn maybe_add_endpoint(&self, node_id: NodeId, addr: SocketAddr) {
        if !self.endpoints.contains_key(&node_id) {
            let endpoint = EndpointState::new(node_id, addr);
            self.endpoints.insert(node_id, endpoint);

            info!(node = ?node_id, addr = %addr, "Added new endpoint from gossip");

            // Add to membership
            let node_meta = NodeMeta::new(node_id, addr);
            let _ = self.membership.add_node(node_meta);

            // Notify
            let _ = self.event_tx.send(GossipEvent::NodeDiscovered(node_id, addr));
        }
    }

    /// Check for failed nodes using phi-accrual detector
    async fn check_failures(&self) {
        // Collect nodes that need to be marked dead first
        let mut dead_nodes = Vec::new();

        for entry in self.endpoints.iter() {
            if entry.node_id == self.node_id {
                continue;
            }

            let node_id = entry.node_id;
            let is_alive = entry.is_alive;
            let phi = self.phi_detector.phi(node_id);

            if phi > self.config.phi_threshold {
                if is_alive {
                    dead_nodes.push(node_id);
                }
            } else if phi > self.config.phi_threshold / 2.0 {
                // Suspected but not dead yet
                debug!(
                    node = ?node_id,
                    phi = phi,
                    threshold = self.config.phi_threshold,
                    "Node suspected"
                );
            }
        }

        // Mark nodes as dead outside the iterator
        for node_id in dead_nodes {
            self.mark_dead(node_id).await;
        }
    }

    /// Mark a node as dead
    async fn mark_dead(&self, node_id: NodeId) {
        if let Some(mut entry) = self.endpoints.get_mut(&node_id) {
            if entry.is_alive {
                entry.is_alive = false;
                warn!(node = ?node_id, "Marking node as dead");

                // Add to quarantine
                self.quarantine.insert(node_id, Instant::now());

                // Update membership
                let _ = self.membership.update_status(node_id, NodeStatus::Offline);

                // Notify
                let _ = self.event_tx.send(GossipEvent::NodeDead(node_id));
            }
        }
    }

    /// Mark a node as alive (when it comes back)
    fn mark_alive(&self, node_id: NodeId) {
        if let Some(mut entry) = self.endpoints.get_mut(&node_id) {
            if !entry.is_alive {
                entry.is_alive = true;
                entry.touch();
                info!(node = ?node_id, "Marking node as alive");

                // Remove from quarantine
                self.quarantine.remove(&node_id);

                // Update membership
                let _ = self.membership.update_status(node_id, NodeStatus::Online);

                // Notify
                let _ = self.event_tx.send(GossipEvent::NodeAlive(node_id));
            }
        }
    }

    /// Clean up quarantined nodes that have been dead too long
    fn cleanup_quarantine(&self) {
        let mut to_remove = Vec::new();

        for entry in self.quarantine.iter() {
            if entry.value().elapsed() > self.config.quarantine_duration {
                to_remove.push(*entry.key());
            }
        }

        for node_id in to_remove {
            info!(node = ?node_id, "Removing node from cluster after quarantine");
            self.quarantine.remove(&node_id);
            self.endpoints.remove(&node_id);
            self.phi_detector.remove(node_id);
            let _ = self.membership.remove_node(node_id);
        }
    }

    /// Set an application state value
    pub fn set_app_state(&self, key: AppStateKey, value: Vec<u8>) {
        let mut local = self.local_state.write();
        let new_version = local.heartbeat.increment();
        local
            .app_states
            .insert(key.clone(), VersionedValue::new(value, new_version));

        // Update in endpoints map
        if let Some(mut ep) = self.endpoints.get_mut(&self.node_id) {
            ep.app_states = local.app_states.clone();
        }
    }

    /// Get an application state value
    pub fn get_app_state(&self, node_id: NodeId, key: &AppStateKey) -> Option<Vec<u8>> {
        self.endpoints
            .get(&node_id)
            .and_then(|e| e.app_states.get(key).map(|v| v.value.clone()))
    }

    /// Shutdown the gossiper
    pub fn shutdown(&self) {
        info!(node = ?self.node_id, "Shutting down gossiper");
        self.shutdown.cancel();
    }

    /// Get node count
    pub fn node_count(&self) -> usize {
        self.endpoints.iter().filter(|e| e.is_alive).count()
    }

    /// Get all alive endpoints
    pub fn alive_endpoints(&self) -> Vec<EndpointState> {
        self.endpoints
            .iter()
            .filter(|e| e.is_alive)
            .map(|e| e.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    fn create_test_membership(node_id: NodeId) -> Arc<Membership> {
        Arc::new(Membership::with_default_config(
            node_id,
            test_addr(5000 + node_id.0 as u16),
        ))
    }

    #[test]
    fn test_heartbeat_state() {
        let hb = HeartbeatState::new();
        assert_eq!(hb.current_version(), 0);

        hb.increment();
        assert_eq!(hb.current_version(), 1);

        hb.increment();
        assert_eq!(hb.current_version(), 2);
    }

    #[test]
    fn test_endpoint_state() {
        let addr = test_addr(5000);
        let mut ep = EndpointState::new(NodeId(1), addr);

        assert!(ep.is_alive);
        assert_eq!(ep.max_version(), 0);

        ep.heartbeat.increment();
        assert_eq!(ep.max_version(), 1);

        ep.app_states.insert(
            AppStateKey::Status,
            VersionedValue::new(vec![1, 2, 3], 5),
        );
        assert_eq!(ep.max_version(), 5);
    }

    #[test]
    fn test_gossip_digest() {
        let mut digest = GossipDigest::new();
        digest.add(NodeId(1), 100, 5);
        digest.add(NodeId(2), 101, 3);

        assert_eq!(digest.entries.len(), 2);
        assert_eq!(digest.entries[0].node_id, NodeId(1));
        assert_eq!(digest.entries[1].generation, 101);
    }

    #[test]
    fn test_phi_detector() {
        let detector = PhiAccrualDetector::new(3);

        // Report some heartbeats
        for _ in 0..5 {
            detector.report_heartbeat(NodeId(1));
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Should have low phi right after heartbeat
        let phi = detector.phi(NodeId(1));
        assert!(phi < 8.0, "Phi should be low: {}", phi);
    }

    #[tokio::test]
    async fn test_gossiper_creation() {
        let membership = create_test_membership(NodeId(1));
        let gossiper = Gossiper::new(
            NodeId(1),
            test_addr(5001),
            "test-cluster".to_string(),
            vec![],
            membership,
            GossipConfig::default(),
        );

        assert_eq!(gossiper.node_count(), 1);
        assert!(!gossiper.is_bootstrapped());
    }

    #[tokio::test]
    async fn test_gossiper_bootstrap_no_seeds() {
        let membership = create_test_membership(NodeId(1));
        let gossiper = Gossiper::new(
            NodeId(1),
            test_addr(5001),
            "test-cluster".to_string(),
            vec![],
            membership,
            GossipConfig::default(),
        );

        gossiper.bootstrap().await.unwrap();
        assert!(gossiper.is_bootstrapped());
    }

    #[test]
    fn test_gossiper_select_targets() {
        let membership = create_test_membership(NodeId(1));
        let gossiper = Gossiper::new(
            NodeId(1),
            test_addr(5001),
            "test-cluster".to_string(),
            vec![],
            membership,
            GossipConfig::default(),
        );

        // Add some endpoints
        for i in 2..=5 {
            let ep = EndpointState::new(NodeId(i), test_addr(5000 + i as u16));
            gossiper.endpoints.insert(NodeId(i), ep);
        }

        let targets = gossiper.select_gossip_targets();
        assert!(targets.len() <= 3); // gossip_fanout is 3
        assert!(!targets.contains(&NodeId(1))); // Should not include self
    }

    #[test]
    fn test_gossiper_create_digest() {
        let membership = create_test_membership(NodeId(1));
        let gossiper = Gossiper::new(
            NodeId(1),
            test_addr(5001),
            "test-cluster".to_string(),
            vec![],
            membership,
            GossipConfig::default(),
        );

        // Add another endpoint
        let mut ep = EndpointState::new(NodeId(2), test_addr(5002));
        ep.heartbeat.increment();
        ep.heartbeat.increment();
        gossiper.endpoints.insert(NodeId(2), ep);

        let digest = gossiper.create_digest();
        assert_eq!(digest.entries.len(), 2);
    }

    #[test]
    fn test_compare_digest() {
        let membership = create_test_membership(NodeId(1));
        let gossiper = Gossiper::new(
            NodeId(1),
            test_addr(5001),
            "test-cluster".to_string(),
            vec![],
            membership,
            GossipConfig::default(),
        );

        // Add node 2 with version 5
        let mut ep2 = EndpointState::new(NodeId(2), test_addr(5002));
        for _ in 0..5 {
            ep2.heartbeat.increment();
        }
        gossiper.endpoints.insert(NodeId(2), ep2);

        // Create a remote digest that has node 2 at version 3 and node 3 we don't know
        let mut remote = GossipDigest::new();
        remote.add(NodeId(2), gossiper.endpoints.get(&NodeId(2)).unwrap().heartbeat.generation, 3);
        remote.add(NodeId(3), 100, 1);

        let (they_need, we_need) = gossiper.compare_digest(&remote);

        // They need node 1 (we have, they don't) and node 2 (we have newer)
        assert!(they_need.contains(&NodeId(1)));
        assert!(they_need.contains(&NodeId(2)));

        // We need node 3 (they have, we don't)
        assert_eq!(we_need.len(), 1);
        assert_eq!(we_need[0].0, NodeId(3));
    }

    #[test]
    fn test_app_state() {
        let membership = create_test_membership(NodeId(1));
        let gossiper = Gossiper::new(
            NodeId(1),
            test_addr(5001),
            "test-cluster".to_string(),
            vec![],
            membership,
            GossipConfig::default(),
        );

        // Set app state
        gossiper.set_app_state(AppStateKey::Status, vec![1, 2, 3]);

        // Get it back
        let value = gossiper.get_app_state(NodeId(1), &AppStateKey::Status);
        assert_eq!(value, Some(vec![1, 2, 3]));

        // Non-existent
        let none = gossiper.get_app_state(NodeId(1), &AppStateKey::Load);
        assert!(none.is_none());
    }
}
