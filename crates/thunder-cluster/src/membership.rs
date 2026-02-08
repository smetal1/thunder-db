//! Cluster Membership
//!
//! Provides cluster membership management, node discovery, and health monitoring.
//! Handles node join/leave operations and maintains cluster topology.

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use thunder_common::prelude::*;

use crate::{NodeInfo, NodeStatus};

/// Membership configuration
#[derive(Debug, Clone)]
pub struct MembershipConfig {
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Node timeout before marking as suspected
    pub suspect_timeout: Duration,
    /// Node timeout before marking as failed
    pub failure_timeout: Duration,
    /// Gossip interval for membership propagation
    pub gossip_interval: Duration,
    /// Maximum gossip fanout
    pub gossip_fanout: usize,
    /// Enable automatic failure detection
    pub auto_failure_detection: bool,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(1),
            suspect_timeout: Duration::from_secs(5),
            failure_timeout: Duration::from_secs(30),
            gossip_interval: Duration::from_millis(500),
            gossip_fanout: 3,
            auto_failure_detection: true,
        }
    }
}

/// Node health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Node is healthy and responding
    Healthy,
    /// Node is suspected of being unhealthy
    Suspected,
    /// Node has failed and is unresponsive
    Failed,
    /// Health status is unknown
    Unknown,
}

/// Extended node metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMeta {
    /// Node ID
    pub id: NodeId,
    /// Node address
    pub addr: SocketAddr,
    /// Node status
    pub status: NodeStatus,
    /// Health status
    pub health: HealthStatus,
    /// Number of regions hosted
    pub region_count: usize,
    /// Available storage capacity (bytes)
    pub storage_capacity: u64,
    /// Used storage (bytes)
    pub storage_used: u64,
    /// CPU utilization (0-100)
    pub cpu_utilization: f32,
    /// Memory utilization (0-100)
    pub memory_utilization: f32,
    /// Labels for placement rules
    pub labels: HashMap<String, String>,
    /// Last heartbeat time (not serialized)
    #[serde(skip)]
    pub last_heartbeat: Option<Instant>,
    /// Incarnation number for conflict resolution
    pub incarnation: u64,
}

impl NodeMeta {
    pub fn new(id: NodeId, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            status: NodeStatus::Joining,
            health: HealthStatus::Unknown,
            region_count: 0,
            storage_capacity: 0,
            storage_used: 0,
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
            labels: HashMap::new(),
            last_heartbeat: None,
            incarnation: 1,
        }
    }

    /// Convert to NodeInfo
    pub fn to_info(&self) -> NodeInfo {
        NodeInfo {
            id: self.id,
            addr: self.addr.to_string(),
            status: self.status,
            region_count: self.region_count,
        }
    }

    /// Check if node is available for work
    pub fn is_available(&self) -> bool {
        self.status == NodeStatus::Online && self.health == HealthStatus::Healthy
    }

    /// Update heartbeat
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
        self.health = HealthStatus::Healthy;
    }

    /// Check health based on timeout
    pub fn check_health(&mut self, suspect_timeout: Duration, failure_timeout: Duration) {
        if let Some(last) = self.last_heartbeat {
            let elapsed = last.elapsed();
            if elapsed > failure_timeout {
                self.health = HealthStatus::Failed;
                self.status = NodeStatus::Offline;
            } else if elapsed > suspect_timeout {
                self.health = HealthStatus::Suspected;
            }
        }
    }

    /// Get storage utilization percentage
    pub fn storage_utilization(&self) -> f32 {
        if self.storage_capacity == 0 {
            0.0
        } else {
            (self.storage_used as f32 / self.storage_capacity as f32) * 100.0
        }
    }
}

/// Membership change event
#[derive(Debug, Clone)]
pub enum MembershipEvent {
    /// Node joined the cluster
    NodeJoined(NodeId),
    /// Node left the cluster
    NodeLeft(NodeId),
    /// Node status changed
    StatusChanged { node_id: NodeId, old: NodeStatus, new: NodeStatus },
    /// Node health changed
    HealthChanged { node_id: NodeId, old: HealthStatus, new: HealthStatus },
    /// Leader changed
    LeaderChanged { old: Option<NodeId>, new: Option<NodeId> },
}

/// Cluster membership manager
pub struct Membership {
    /// This node's ID
    node_id: NodeId,
    /// This node's metadata
    self_meta: RwLock<NodeMeta>,
    /// All known nodes
    nodes: DashMap<NodeId, NodeMeta>,
    /// Configuration
    config: MembershipConfig,
    /// Event sender
    event_tx: broadcast::Sender<MembershipEvent>,
    /// Next incarnation number
    #[allow(dead_code)]
    next_incarnation: AtomicU64,
    /// Current leader (if known)
    leader_id: RwLock<Option<NodeId>>,
    /// Seed nodes for discovery
    seed_nodes: RwLock<Vec<SocketAddr>>,
}

impl Membership {
    /// Create a new membership manager
    pub fn new(node_id: NodeId, addr: SocketAddr, config: MembershipConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1000);
        let self_meta = NodeMeta::new(node_id, addr);

        let membership = Self {
            node_id,
            self_meta: RwLock::new(self_meta.clone()),
            nodes: DashMap::new(),
            config,
            event_tx,
            next_incarnation: AtomicU64::new(2),
            leader_id: RwLock::new(None),
            seed_nodes: RwLock::new(Vec::new()),
        };

        // Add self to nodes
        membership.nodes.insert(node_id, self_meta);

        membership
    }

    /// Create with default config
    pub fn with_default_config(node_id: NodeId, addr: SocketAddr) -> Self {
        Self::new(node_id, addr, MembershipConfig::default())
    }

    /// Get this node's ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get this node's metadata
    pub fn self_meta(&self) -> NodeMeta {
        self.self_meta.read().clone()
    }

    /// Update this node's metadata
    pub fn update_self<F>(&self, f: F)
    where
        F: FnOnce(&mut NodeMeta),
    {
        let mut meta = self.self_meta.write();
        f(&mut meta);
        self.nodes.insert(self.node_id, meta.clone());
    }

    /// Set seed nodes for discovery
    pub fn set_seed_nodes(&self, seeds: Vec<SocketAddr>) {
        *self.seed_nodes.write() = seeds;
    }

    /// Get seed nodes
    pub fn seed_nodes(&self) -> Vec<SocketAddr> {
        self.seed_nodes.read().clone()
    }

    /// Subscribe to membership events
    pub fn subscribe(&self) -> broadcast::Receiver<MembershipEvent> {
        self.event_tx.subscribe()
    }

    /// Add a node to the cluster
    pub fn add_node(&self, meta: NodeMeta) -> Result<()> {
        let node_id = meta.id;

        if self.nodes.contains_key(&node_id) {
            // Update existing node
            if let Some(mut existing) = self.nodes.get_mut(&node_id) {
                if meta.incarnation > existing.incarnation {
                    *existing = meta;
                }
            }
        } else {
            // New node
            self.nodes.insert(node_id, meta);
            let _ = self.event_tx.send(MembershipEvent::NodeJoined(node_id));
        }

        Ok(())
    }

    /// Remove a node from the cluster
    pub fn remove_node(&self, node_id: NodeId) -> Result<()> {
        if node_id == self.node_id {
            return Err(Error::Internal("Cannot remove self".to_string()));
        }

        if self.nodes.remove(&node_id).is_some() {
            let _ = self.event_tx.send(MembershipEvent::NodeLeft(node_id));
        }

        Ok(())
    }

    /// Get a node's metadata
    pub fn get_node(&self, node_id: NodeId) -> Option<NodeMeta> {
        self.nodes.get(&node_id).map(|r| r.clone())
    }

    /// Get all nodes
    pub fn all_nodes(&self) -> Vec<NodeMeta> {
        self.nodes.iter().map(|r| r.clone()).collect()
    }

    /// Get all online nodes
    pub fn online_nodes(&self) -> Vec<NodeMeta> {
        self.nodes
            .iter()
            .filter(|r| r.status == NodeStatus::Online)
            .map(|r| r.clone())
            .collect()
    }

    /// Get all healthy nodes
    pub fn healthy_nodes(&self) -> Vec<NodeMeta> {
        self.nodes
            .iter()
            .filter(|r| r.is_available())
            .map(|r| r.clone())
            .collect()
    }

    /// Get node count
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get online node count
    pub fn online_count(&self) -> usize {
        self.nodes.iter().filter(|r| r.status == NodeStatus::Online).count()
    }

    /// Record a heartbeat from a node
    pub fn heartbeat(&self, node_id: NodeId) -> Result<()> {
        if let Some(mut node) = self.nodes.get_mut(&node_id) {
            let old_health = node.health;
            node.heartbeat();

            if old_health != HealthStatus::Healthy {
                let _ = self.event_tx.send(MembershipEvent::HealthChanged {
                    node_id,
                    old: old_health,
                    new: HealthStatus::Healthy,
                });
            }
        }
        Ok(())
    }

    /// Update node status
    pub fn update_status(&self, node_id: NodeId, status: NodeStatus) -> Result<()> {
        if let Some(mut node) = self.nodes.get_mut(&node_id) {
            if node.status != status {
                let old = node.status;
                node.status = status;
                let _ = self.event_tx.send(MembershipEvent::StatusChanged {
                    node_id,
                    old,
                    new: status,
                });
            }
        }
        Ok(())
    }

    /// Check all nodes' health
    pub fn check_health(&self) {
        for mut node in self.nodes.iter_mut() {
            if node.id == self.node_id {
                continue;
            }

            let old_health = node.health;
            node.check_health(self.config.suspect_timeout, self.config.failure_timeout);

            if old_health != node.health {
                let _ = self.event_tx.send(MembershipEvent::HealthChanged {
                    node_id: node.id,
                    old: old_health,
                    new: node.health,
                });
            }
        }
    }

    /// Set the cluster leader
    pub fn set_leader(&self, leader_id: Option<NodeId>) {
        let mut current = self.leader_id.write();
        if *current != leader_id {
            let old = *current;
            *current = leader_id;
            let _ = self.event_tx.send(MembershipEvent::LeaderChanged {
                old,
                new: leader_id,
            });
        }
    }

    /// Get the cluster leader
    pub fn leader(&self) -> Option<NodeId> {
        *self.leader_id.read()
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        *self.leader_id.read() == Some(self.node_id)
    }

    /// Get nodes for gossip (random subset)
    pub fn gossip_targets(&self) -> Vec<NodeId> {
        let nodes: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|r| r.id != self.node_id && r.is_available())
            .map(|r| r.id)
            .collect();

        // Take random subset
        let count = std::cmp::min(self.config.gossip_fanout, nodes.len());
        nodes.into_iter().take(count).collect()
    }

    /// Get the best nodes for placing a new region (based on utilization)
    pub fn select_placement_nodes(&self, count: usize) -> Vec<NodeId> {
        let mut nodes: Vec<_> = self
            .healthy_nodes()
            .into_iter()
            .filter(|n| n.id != self.node_id)
            .collect();

        // Sort by storage utilization (lowest first)
        nodes.sort_by(|a, b| {
            a.storage_utilization()
                .partial_cmp(&b.storage_utilization())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        nodes.into_iter().take(count).map(|n| n.id).collect()
    }

    /// Mark this node as online
    pub fn mark_online(&self) {
        self.update_self(|meta| {
            meta.status = NodeStatus::Online;
            meta.health = HealthStatus::Healthy;
            meta.heartbeat();
        });
    }

    /// Mark this node as leaving
    pub fn mark_leaving(&self) {
        self.update_self(|meta| {
            meta.status = NodeStatus::Leaving;
        });
    }

    /// Generate a membership digest for gossip
    pub fn digest(&self) -> MembershipDigest {
        let nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|r| NodeDigest {
                id: r.id,
                incarnation: r.incarnation,
                status: r.status,
            })
            .collect();

        MembershipDigest {
            from: self.node_id,
            nodes,
        }
    }

    /// Merge a membership digest from gossip
    pub fn merge_digest(&self, digest: MembershipDigest) {
        for node_digest in digest.nodes {
            if let Some(mut existing) = self.nodes.get_mut(&node_digest.id) {
                // Update if incarnation is newer
                if node_digest.incarnation > existing.incarnation {
                    existing.incarnation = node_digest.incarnation;
                    existing.status = node_digest.status;
                }
            }
            // Note: We don't add unknown nodes from digest - they must join properly
        }
    }

    /// Get all node infos
    pub fn node_infos(&self) -> Vec<NodeInfo> {
        self.nodes.iter().map(|r| r.to_info()).collect()
    }
}

/// Compact node info for gossip
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDigest {
    pub id: NodeId,
    pub incarnation: u64,
    pub status: NodeStatus,
}

/// Membership digest for gossip protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipDigest {
    pub from: NodeId,
    pub nodes: Vec<NodeDigest>,
}

/// Node selector for choosing nodes based on criteria
pub struct NodeSelector<'a> {
    membership: &'a Membership,
    filters: Vec<Box<dyn Fn(&NodeMeta) -> bool + 'a>>,
}

impl<'a> NodeSelector<'a> {
    pub fn new(membership: &'a Membership) -> Self {
        Self {
            membership,
            filters: Vec::new(),
        }
    }

    /// Filter to online nodes only
    pub fn online(mut self) -> Self {
        self.filters.push(Box::new(|n| n.status == NodeStatus::Online));
        self
    }

    /// Filter to healthy nodes only
    pub fn healthy(mut self) -> Self {
        self.filters.push(Box::new(|n| n.health == HealthStatus::Healthy));
        self
    }

    /// Exclude specific nodes
    pub fn exclude(mut self, nodes: HashSet<NodeId>) -> Self {
        self.filters.push(Box::new(move |n| !nodes.contains(&n.id)));
        self
    }

    /// Filter by label
    pub fn with_label(mut self, key: String, value: String) -> Self {
        self.filters.push(Box::new(move |n| {
            n.labels.get(&key).map(|v| v == &value).unwrap_or(false)
        }));
        self
    }

    /// Filter by minimum storage capacity
    pub fn min_storage(mut self, min_bytes: u64) -> Self {
        self.filters.push(Box::new(move |n| {
            n.storage_capacity - n.storage_used >= min_bytes
        }));
        self
    }

    /// Select nodes matching all filters
    pub fn select(self) -> Vec<NodeMeta> {
        self.membership
            .nodes
            .iter()
            .filter(|n| self.filters.iter().all(|f| f(&n)))
            .map(|n| n.clone())
            .collect()
    }

    /// Select a single best node (lowest utilization)
    pub fn select_best(self) -> Option<NodeMeta> {
        self.select()
            .into_iter()
            .min_by(|a, b| {
                a.storage_utilization()
                    .partial_cmp(&b.storage_utilization())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }
}

/// Health checker for periodic health monitoring
pub struct HealthChecker {
    membership: Arc<Membership>,
    interval: Duration,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl HealthChecker {
    pub fn new(membership: Arc<Membership>, interval: Duration) -> Self {
        Self {
            membership,
            interval,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Run the health checker loop
    pub async fn run(&self) {
        while !self.shutdown.load(Ordering::SeqCst) {
            self.membership.check_health();
            tokio::time::sleep(self.interval).await;
        }
    }

    /// Stop the health checker
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_addr() -> SocketAddr {
        "127.0.0.1:5000".parse().unwrap()
    }

    #[test]
    fn test_node_meta() {
        let mut meta = NodeMeta::new(NodeId(1), test_addr());

        assert_eq!(meta.status, NodeStatus::Joining);
        assert_eq!(meta.health, HealthStatus::Unknown);
        assert!(!meta.is_available());

        meta.status = NodeStatus::Online;
        meta.heartbeat();

        assert_eq!(meta.health, HealthStatus::Healthy);
        assert!(meta.is_available());
    }

    #[test]
    fn test_node_meta_storage_utilization() {
        let mut meta = NodeMeta::new(NodeId(1), test_addr());

        meta.storage_capacity = 1000;
        meta.storage_used = 250;

        assert!((meta.storage_utilization() - 25.0).abs() < 0.01);
    }

    #[test]
    fn test_membership_add_node() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());

        let node2 = NodeMeta::new(NodeId(2), "127.0.0.1:5001".parse().unwrap());
        membership.add_node(node2).unwrap();

        assert_eq!(membership.node_count(), 2);
        assert!(membership.get_node(NodeId(2)).is_some());
    }

    #[test]
    fn test_membership_remove_node() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());

        let node2 = NodeMeta::new(NodeId(2), "127.0.0.1:5001".parse().unwrap());
        membership.add_node(node2).unwrap();

        membership.remove_node(NodeId(2)).unwrap();
        assert_eq!(membership.node_count(), 1);
        assert!(membership.get_node(NodeId(2)).is_none());
    }

    #[test]
    fn test_membership_cannot_remove_self() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());
        assert!(membership.remove_node(NodeId(1)).is_err());
    }

    #[test]
    fn test_membership_heartbeat() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());

        let mut node2 = NodeMeta::new(NodeId(2), "127.0.0.1:5001".parse().unwrap());
        node2.status = NodeStatus::Online;
        membership.add_node(node2).unwrap();

        membership.heartbeat(NodeId(2)).unwrap();

        let node = membership.get_node(NodeId(2)).unwrap();
        assert_eq!(node.health, HealthStatus::Healthy);
    }

    #[test]
    fn test_membership_leader() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());

        assert!(membership.leader().is_none());
        assert!(!membership.is_leader());

        membership.set_leader(Some(NodeId(1)));
        assert_eq!(membership.leader(), Some(NodeId(1)));
        assert!(membership.is_leader());
    }

    #[test]
    fn test_membership_online_nodes() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());
        membership.mark_online();

        let mut node2 = NodeMeta::new(NodeId(2), "127.0.0.1:5001".parse().unwrap());
        node2.status = NodeStatus::Online;
        membership.add_node(node2).unwrap();

        let mut node3 = NodeMeta::new(NodeId(3), "127.0.0.1:5002".parse().unwrap());
        node3.status = NodeStatus::Offline;
        membership.add_node(node3).unwrap();

        assert_eq!(membership.online_count(), 2);
    }

    #[test]
    fn test_membership_digest() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());
        membership.mark_online();

        let digest = membership.digest();
        assert_eq!(digest.from, NodeId(1));
        assert_eq!(digest.nodes.len(), 1);
    }

    #[test]
    fn test_node_selector() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());
        membership.mark_online();

        let mut node2 = NodeMeta::new(NodeId(2), "127.0.0.1:5001".parse().unwrap());
        node2.status = NodeStatus::Online;
        node2.health = HealthStatus::Healthy;
        membership.add_node(node2).unwrap();

        let mut node3 = NodeMeta::new(NodeId(3), "127.0.0.1:5002".parse().unwrap());
        node3.status = NodeStatus::Offline;
        membership.add_node(node3).unwrap();

        let selected = NodeSelector::new(&membership)
            .online()
            .healthy()
            .select();

        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_node_selector_with_label() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());

        let mut node2 = NodeMeta::new(NodeId(2), "127.0.0.1:5001".parse().unwrap());
        node2.status = NodeStatus::Online;
        node2.health = HealthStatus::Healthy;
        node2.labels.insert("zone".to_string(), "us-east-1".to_string());
        membership.add_node(node2).unwrap();

        let mut node3 = NodeMeta::new(NodeId(3), "127.0.0.1:5002".parse().unwrap());
        node3.status = NodeStatus::Online;
        node3.health = HealthStatus::Healthy;
        node3.labels.insert("zone".to_string(), "us-west-2".to_string());
        membership.add_node(node3).unwrap();

        let selected = NodeSelector::new(&membership)
            .online()
            .with_label("zone".to_string(), "us-east-1".to_string())
            .select();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, NodeId(2));
    }

    #[test]
    fn test_select_placement_nodes() {
        let membership = Membership::with_default_config(NodeId(1), test_addr());

        for i in 2..=5 {
            let mut node = NodeMeta::new(NodeId(i), format!("127.0.0.1:{}", 5000 + i).parse().unwrap());
            node.status = NodeStatus::Online;
            node.health = HealthStatus::Healthy;
            node.storage_capacity = 1000;
            node.storage_used = (i as u64) * 100; // Different utilization
            membership.add_node(node).unwrap();
        }

        let selected = membership.select_placement_nodes(2);
        assert_eq!(selected.len(), 2);
        // Should select nodes with lowest utilization
        assert_eq!(selected[0], NodeId(2)); // 20% utilization
        assert_eq!(selected[1], NodeId(3)); // 30% utilization
    }
}
