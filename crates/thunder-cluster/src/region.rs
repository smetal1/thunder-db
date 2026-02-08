//! Region Management
//!
//! Provides region-based sharding for distributed data management.
//! Each region manages a contiguous key range and is replicated across nodes
//! using Raft consensus.

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use thunder_common::prelude::*;

use crate::raft_node::{MemoryStorage, RaftNode, RaftNodeConfig, RaftStatus};
use crate::{Command, RegionInfo};

/// Region configuration
#[derive(Debug, Clone)]
pub struct RegionConfig {
    /// Maximum region size in bytes before splitting
    pub max_region_size: u64,
    /// Minimum region size in bytes before merging
    pub min_region_size: u64,
    /// Target number of replicas per region
    pub replica_count: usize,
    /// Split check interval
    pub split_check_interval: std::time::Duration,
    /// Whether to enable auto-split
    pub auto_split: bool,
    /// Whether to enable auto-merge
    pub auto_merge: bool,
}

impl Default for RegionConfig {
    fn default() -> Self {
        Self {
            max_region_size: 256 * 1024 * 1024, // 256MB
            min_region_size: 32 * 1024 * 1024,  // 32MB
            replica_count: 3,
            split_check_interval: std::time::Duration::from_secs(60),
            auto_split: true,
            auto_merge: true,
        }
    }
}

/// Region state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionState {
    /// Region is active and serving requests
    Active,
    /// Region is being created
    Creating,
    /// Region is being split
    Splitting,
    /// Region is being merged
    Merging,
    /// Region is being transferred to another node
    Transferring,
    /// Region is tombstoned (deleted)
    Tombstone,
}

/// Region epoch for detecting stale messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RegionEpoch {
    /// Configuration version (incremented on peer changes)
    pub conf_ver: u64,
    /// Version (incremented on split/merge)
    pub version: u64,
}

impl RegionEpoch {
    pub fn new(conf_ver: u64, version: u64) -> Self {
        Self { conf_ver, version }
    }

    pub fn initial() -> Self {
        Self {
            conf_ver: 1,
            version: 1,
        }
    }

    pub fn bump_conf_ver(&mut self) {
        self.conf_ver += 1;
    }

    pub fn bump_version(&mut self) {
        self.version += 1;
    }
}

impl Default for RegionEpoch {
    fn default() -> Self {
        Self::initial()
    }
}

/// Peer information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    /// Peer ID (unique within region)
    pub id: u64,
    /// Node ID hosting this peer
    pub node_id: NodeId,
    /// Whether this is a learner (non-voting)
    pub is_learner: bool,
}

impl Peer {
    pub fn new(id: u64, node_id: NodeId) -> Self {
        Self {
            id,
            node_id,
            is_learner: false,
        }
    }

    pub fn learner(id: u64, node_id: NodeId) -> Self {
        Self {
            id,
            node_id,
            is_learner: true,
        }
    }
}

/// Region metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionMeta {
    /// Region ID
    pub id: RegionId,
    /// Start key (inclusive)
    pub start_key: Vec<u8>,
    /// End key (exclusive)
    pub end_key: Vec<u8>,
    /// Region epoch
    pub epoch: RegionEpoch,
    /// Peers in this region
    pub peers: Vec<Peer>,
    /// Current state
    pub state: RegionState,
}

impl RegionMeta {
    pub fn new(id: RegionId, start_key: Vec<u8>, end_key: Vec<u8>) -> Self {
        Self {
            id,
            start_key,
            end_key,
            epoch: RegionEpoch::initial(),
            peers: Vec::new(),
            state: RegionState::Creating,
        }
    }

    /// Check if a key belongs to this region
    pub fn contains_key(&self, key: &[u8]) -> bool {
        key >= &self.start_key[..] && (self.end_key.is_empty() || key < &self.end_key[..])
    }

    /// Get the leader peer
    pub fn leader(&self) -> Option<&Peer> {
        // In a real implementation, this would track the actual leader
        self.peers.first()
    }

    /// Add a peer
    pub fn add_peer(&mut self, peer: Peer) {
        if !self.peers.iter().any(|p| p.id == peer.id) {
            self.peers.push(peer);
            self.epoch.bump_conf_ver();
        }
    }

    /// Remove a peer
    pub fn remove_peer(&mut self, peer_id: u64) {
        if let Some(pos) = self.peers.iter().position(|p| p.id == peer_id) {
            self.peers.remove(pos);
            self.epoch.bump_conf_ver();
        }
    }

    /// Convert to RegionInfo
    pub fn to_info(&self, leader_id: Option<NodeId>, size_bytes: u64) -> RegionInfo {
        RegionInfo {
            id: self.id,
            start_key: self.start_key.clone(),
            end_key: self.end_key.clone(),
            leader: leader_id,
            peers: self.peers.iter().map(|p| p.node_id).collect(),
            size_bytes,
        }
    }
}

/// Region statistics
#[derive(Debug, Clone, Default)]
pub struct RegionStats {
    /// Approximate size in bytes
    pub size_bytes: u64,
    /// Number of keys
    pub key_count: u64,
    /// Read bytes per second
    pub read_bytes_rate: f64,
    /// Write bytes per second
    pub write_bytes_rate: f64,
    /// Read keys per second
    pub read_keys_rate: f64,
    /// Write keys per second
    pub write_keys_rate: f64,
    /// Last update time
    pub last_update: Option<Instant>,
}

impl RegionStats {
    pub fn update(&mut self, size_bytes: u64, key_count: u64) {
        self.size_bytes = size_bytes;
        self.key_count = key_count;
        self.last_update = Some(Instant::now());
    }

    pub fn needs_split(&self, max_size: u64) -> bool {
        self.size_bytes > max_size
    }

    pub fn needs_merge(&self, min_size: u64) -> bool {
        self.size_bytes < min_size
    }
}

/// A region with its Raft group
pub struct Region {
    /// Region metadata
    meta: RwLock<RegionMeta>,
    /// Raft node for this region
    raft_node: Option<RaftNode<MemoryStorage>>,
    /// Region statistics
    stats: RwLock<RegionStats>,
    /// Configuration
    config: RegionConfig,
    /// Applied index
    #[allow(dead_code)]
    applied_index: AtomicU64,
    /// Pending split key (if splitting)
    pending_split: RwLock<Option<Vec<u8>>>,
}

impl Region {
    /// Create a new region
    pub fn new(meta: RegionMeta, config: RegionConfig) -> Self {
        Self {
            meta: RwLock::new(meta),
            raft_node: None,
            stats: RwLock::new(RegionStats::default()),
            config,
            applied_index: AtomicU64::new(0),
            pending_split: RwLock::new(None),
        }
    }

    /// Create a region with a Raft node
    pub fn with_raft(
        meta: RegionMeta,
        config: RegionConfig,
        raft_config: RaftNodeConfig,
    ) -> Result<Self> {
        let region_id = meta.id;
        let voters: Vec<u64> = meta.peers.iter().filter(|p| !p.is_learner).map(|p| p.id).collect();

        let storage = MemoryStorage::new_with_conf_state(voters);
        let raft_node = RaftNode::new(region_id, raft_config, storage)?;

        Ok(Self {
            meta: RwLock::new(meta),
            raft_node: Some(raft_node),
            stats: RwLock::new(RegionStats::default()),
            config,
            applied_index: AtomicU64::new(0),
            pending_split: RwLock::new(None),
        })
    }

    /// Get region ID
    pub fn id(&self) -> RegionId {
        self.meta.read().id
    }

    /// Get region metadata
    pub fn meta(&self) -> RegionMeta {
        self.meta.read().clone()
    }

    /// Get region state
    pub fn state(&self) -> RegionState {
        self.meta.read().state
    }

    /// Set region state
    pub fn set_state(&self, state: RegionState) {
        self.meta.write().state = state;
    }

    /// Check if a key belongs to this region
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.meta.read().contains_key(key)
    }

    /// Get the key range
    pub fn key_range(&self) -> (Vec<u8>, Vec<u8>) {
        let meta = self.meta.read();
        (meta.start_key.clone(), meta.end_key.clone())
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        self.raft_node.as_ref().map(|r| r.is_leader()).unwrap_or(false)
    }

    /// Get the leader ID
    pub fn leader_id(&self) -> Option<u64> {
        self.raft_node.as_ref().and_then(|r| r.leader_id())
    }

    /// Get Raft status
    pub fn raft_status(&self) -> Option<RaftStatus> {
        self.raft_node.as_ref().map(|r| r.status())
    }

    /// Propose a command
    pub fn propose(&self, cmd: Command) -> Result<u64> {
        let raft_node = self.raft_node.as_ref().ok_or_else(|| {
            Error::Internal("No Raft node".to_string())
        })?;

        if !raft_node.is_leader() {
            return Err(Error::Internal("Not leader".to_string()));
        }

        raft_node.propose(cmd)
    }

    /// Tick the Raft node
    pub fn tick(&self) -> bool {
        self.raft_node.as_ref().map(|r| r.tick()).unwrap_or(false)
    }

    /// Get region statistics
    pub fn stats(&self) -> RegionStats {
        self.stats.read().clone()
    }

    /// Update region statistics
    pub fn update_stats(&self, size_bytes: u64, key_count: u64) {
        self.stats.write().update(size_bytes, key_count);
    }

    /// Check if region needs to be split
    pub fn needs_split(&self) -> bool {
        if !self.config.auto_split {
            return false;
        }
        self.stats.read().needs_split(self.config.max_region_size)
    }

    /// Check if region can be merged
    pub fn can_merge(&self) -> bool {
        if !self.config.auto_merge {
            return false;
        }
        self.stats.read().needs_merge(self.config.min_region_size)
    }

    /// Prepare for split at the given key
    pub fn prepare_split(&self, split_key: Vec<u8>) -> Result<()> {
        let meta = self.meta.read();
        if !meta.contains_key(&split_key) {
            return Err(Error::Internal("Split key not in region".to_string()));
        }

        *self.pending_split.write() = Some(split_key);
        Ok(())
    }

    /// Execute the split
    pub fn execute_split(&self, new_region_id: RegionId) -> Result<RegionMeta> {
        let split_key = self.pending_split.write().take().ok_or_else(|| {
            Error::Internal("No pending split".to_string())
        })?;

        let mut meta = self.meta.write();
        let old_end_key = std::mem::replace(&mut meta.end_key, split_key.clone());
        meta.epoch.bump_version();

        // Create new region meta
        let mut new_meta = RegionMeta::new(new_region_id, split_key, old_end_key);
        new_meta.peers = meta.peers.clone();
        new_meta.state = RegionState::Active;

        Ok(new_meta)
    }

    /// Convert to RegionInfo
    pub fn to_info(&self) -> RegionInfo {
        let meta = self.meta.read();
        let stats = self.stats.read();
        let leader_id = self.leader_id().map(|id| NodeId(id));

        meta.to_info(leader_id, stats.size_bytes)
    }
}

/// Region manager for managing multiple regions on a node
pub struct RegionManager {
    /// This node's ID
    node_id: NodeId,
    /// Regions managed by this node
    regions: DashMap<RegionId, Arc<Region>>,
    /// Key to region mapping (B-tree for range lookups)
    key_map: RwLock<BTreeMap<Vec<u8>, RegionId>>,
    /// Configuration
    config: RegionConfig,
    /// Next region ID for new regions
    next_region_id: AtomicU64,
}

impl RegionManager {
    /// Create a new region manager
    pub fn new(node_id: NodeId, config: RegionConfig) -> Self {
        Self {
            node_id,
            regions: DashMap::new(),
            key_map: RwLock::new(BTreeMap::new()),
            config,
            next_region_id: AtomicU64::new(1),
        }
    }

    /// Create with default config
    pub fn with_default_config(node_id: NodeId) -> Self {
        Self::new(node_id, RegionConfig::default())
    }

    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Bootstrap with an initial region covering all keys
    pub fn bootstrap(&self, peers: Vec<Peer>) -> Result<RegionId> {
        let region_id = RegionId(self.next_region_id.fetch_add(1, Ordering::SeqCst));

        let mut meta = RegionMeta::new(region_id, vec![], vec![]);
        meta.peers = peers;
        meta.state = RegionState::Active;

        let raft_config = RaftNodeConfig {
            id: self.node_id.0,
            ..Default::default()
        };

        let region = Region::with_raft(meta, self.config.clone(), raft_config)?;
        let region = Arc::new(region);

        self.regions.insert(region_id, region);
        self.key_map.write().insert(vec![], region_id);

        Ok(region_id)
    }

    /// Add a region
    pub fn add_region(&self, region: Region) -> Result<()> {
        let region_id = region.id();
        let (start_key, _) = region.key_range();

        let region = Arc::new(region);
        self.regions.insert(region_id, region);
        self.key_map.write().insert(start_key, region_id);

        Ok(())
    }

    /// Remove a region
    pub fn remove_region(&self, region_id: RegionId) -> Result<()> {
        if let Some((_, region)) = self.regions.remove(&region_id) {
            let (start_key, _) = region.key_range();
            self.key_map.write().remove(&start_key);
        }
        Ok(())
    }

    /// Get a region by ID
    pub fn get_region(&self, region_id: RegionId) -> Option<Arc<Region>> {
        self.regions.get(&region_id).map(|r| r.clone())
    }

    /// Locate the region containing a key
    pub fn locate_key(&self, key: &[u8]) -> Option<RegionId> {
        let key_map = self.key_map.read();

        // Find the region whose start_key <= key
        key_map
            .range(..=key.to_vec())
            .next_back()
            .and_then(|(_, &region_id)| {
                // Verify the key is within the region
                self.regions.get(&region_id).and_then(|region| {
                    if region.contains_key(key) {
                        Some(region_id)
                    } else {
                        None
                    }
                })
            })
    }

    /// Get all regions
    pub fn all_regions(&self) -> Vec<Arc<Region>> {
        self.regions.iter().map(|r| r.clone()).collect()
    }

    /// Get regions where this node is the leader
    pub fn leader_regions(&self) -> Vec<Arc<Region>> {
        self.regions
            .iter()
            .filter(|r| r.is_leader())
            .map(|r| r.clone())
            .collect()
    }

    /// Get region count
    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    /// Tick all regions
    pub fn tick_all(&self) {
        for region in self.regions.iter() {
            region.tick();
        }
    }

    /// Check regions for split/merge
    pub fn check_regions(&self) -> Vec<RegionAction> {
        let mut actions = Vec::new();

        for region in self.regions.iter() {
            if region.is_leader() {
                if region.needs_split() {
                    actions.push(RegionAction::Split {
                        region_id: region.id(),
                    });
                } else if region.can_merge() {
                    actions.push(RegionAction::CheckMerge {
                        region_id: region.id(),
                    });
                }
            }
        }

        actions
    }

    /// Split a region
    pub fn split_region(&self, region_id: RegionId, split_key: Vec<u8>) -> Result<RegionId> {
        let region = self.regions.get(&region_id).ok_or_else(|| {
            Error::Internal("Region not found".to_string())
        })?;

        region.prepare_split(split_key)?;

        let new_region_id = RegionId(self.next_region_id.fetch_add(1, Ordering::SeqCst));
        let new_meta = region.execute_split(new_region_id)?;

        // Create new region
        let raft_config = RaftNodeConfig {
            id: self.node_id.0,
            ..Default::default()
        };

        let new_region = Region::with_raft(new_meta, self.config.clone(), raft_config)?;
        self.add_region(new_region)?;

        Ok(new_region_id)
    }

    /// Get all region infos
    pub fn region_infos(&self) -> Vec<RegionInfo> {
        self.regions.iter().map(|r| r.to_info()).collect()
    }
}

/// Action to take on a region
#[derive(Debug, Clone)]
pub enum RegionAction {
    /// Split the region
    Split { region_id: RegionId },
    /// Check if region can merge with neighbor
    CheckMerge { region_id: RegionId },
    /// Transfer leader to another node
    TransferLeader { region_id: RegionId, target: NodeId },
    /// Add a peer
    AddPeer { region_id: RegionId, peer: Peer },
    /// Remove a peer
    RemovePeer { region_id: RegionId, peer_id: u64 },
}

/// Split finder for determining optimal split points
pub struct SplitFinder {
    /// Sample keys for finding split point
    samples: Vec<Vec<u8>>,
    /// Sample size threshold
    sample_threshold: usize,
}

impl SplitFinder {
    pub fn new(sample_threshold: usize) -> Self {
        Self {
            samples: Vec::new(),
            sample_threshold,
        }
    }

    /// Add a key sample
    pub fn add_sample(&mut self, key: Vec<u8>) {
        if self.samples.len() < self.sample_threshold {
            self.samples.push(key);
        }
    }

    /// Find the approximate middle key for splitting
    pub fn find_split_key(&mut self) -> Option<Vec<u8>> {
        if self.samples.len() < 10 {
            return None;
        }

        self.samples.sort();
        let mid = self.samples.len() / 2;
        Some(self.samples[mid].clone())
    }

    /// Clear samples
    pub fn clear(&mut self) {
        self.samples.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_epoch() {
        let mut epoch = RegionEpoch::initial();
        assert_eq!(epoch.conf_ver, 1);
        assert_eq!(epoch.version, 1);

        epoch.bump_conf_ver();
        assert_eq!(epoch.conf_ver, 2);

        epoch.bump_version();
        assert_eq!(epoch.version, 2);
    }

    #[test]
    fn test_region_meta_contains_key() {
        let meta = RegionMeta::new(RegionId(1), vec![10], vec![20]);

        assert!(!meta.contains_key(&[5]));
        assert!(meta.contains_key(&[10]));
        assert!(meta.contains_key(&[15]));
        assert!(!meta.contains_key(&[20]));
        assert!(!meta.contains_key(&[25]));
    }

    #[test]
    fn test_region_meta_unbounded() {
        let meta = RegionMeta::new(RegionId(1), vec![], vec![]);

        assert!(meta.contains_key(&[0]));
        assert!(meta.contains_key(&[128]));
        assert!(meta.contains_key(&[255]));
    }

    #[test]
    fn test_region_meta_add_remove_peer() {
        let mut meta = RegionMeta::new(RegionId(1), vec![], vec![]);
        let initial_conf_ver = meta.epoch.conf_ver;

        meta.add_peer(Peer::new(1, NodeId(1)));
        assert_eq!(meta.peers.len(), 1);
        assert_eq!(meta.epoch.conf_ver, initial_conf_ver + 1);

        meta.add_peer(Peer::new(2, NodeId(2)));
        assert_eq!(meta.peers.len(), 2);

        meta.remove_peer(1);
        assert_eq!(meta.peers.len(), 1);
        assert_eq!(meta.peers[0].id, 2);
    }

    #[test]
    fn test_region_stats() {
        let mut stats = RegionStats::default();

        stats.update(100 * 1024 * 1024, 1000);
        assert_eq!(stats.size_bytes, 100 * 1024 * 1024);
        assert_eq!(stats.key_count, 1000);

        assert!(!stats.needs_split(256 * 1024 * 1024));
        assert!(stats.needs_split(50 * 1024 * 1024));

        assert!(stats.needs_merge(200 * 1024 * 1024));
        assert!(!stats.needs_merge(50 * 1024 * 1024));
    }

    #[test]
    fn test_region_manager_bootstrap() {
        let manager = RegionManager::with_default_config(NodeId(1));

        let peers = vec![
            Peer::new(1, NodeId(1)),
            Peer::new(2, NodeId(2)),
            Peer::new(3, NodeId(3)),
        ];

        let region_id = manager.bootstrap(peers).unwrap();
        assert_eq!(manager.region_count(), 1);

        let region = manager.get_region(region_id).unwrap();
        assert!(region.contains_key(&[0]));
        assert!(region.contains_key(&[255]));
    }

    #[test]
    fn test_region_manager_locate_key() {
        let manager = RegionManager::with_default_config(NodeId(1));

        let peers = vec![Peer::new(1, NodeId(1))];
        let region_id = manager.bootstrap(peers).unwrap();

        let located = manager.locate_key(&[100]);
        assert_eq!(located, Some(region_id));
    }

    #[test]
    fn test_split_finder() {
        let mut finder = SplitFinder::new(100);

        for i in 0..50u8 {
            finder.add_sample(vec![i * 2]);
        }

        let split_key = finder.find_split_key().unwrap();
        // Should be around the middle
        assert!(split_key[0] >= 40 && split_key[0] <= 60);
    }

    #[test]
    fn test_peer() {
        let voter = Peer::new(1, NodeId(1));
        assert!(!voter.is_learner);

        let learner = Peer::learner(2, NodeId(2));
        assert!(learner.is_learner);
    }

    #[test]
    fn test_region_creation() {
        let meta = RegionMeta::new(RegionId(1), vec![0], vec![100]);
        let config = RegionConfig::default();
        let region = Region::new(meta, config);

        assert_eq!(region.id(), RegionId(1));
        assert_eq!(region.state(), RegionState::Creating);
        assert!(!region.is_leader()); // No Raft node
    }

    #[test]
    fn test_region_to_info() {
        let mut meta = RegionMeta::new(RegionId(1), vec![0], vec![100]);
        meta.peers = vec![Peer::new(1, NodeId(1)), Peer::new(2, NodeId(2))];
        meta.state = RegionState::Active;

        let config = RegionConfig::default();
        let region = Region::new(meta, config);
        region.update_stats(1024, 10);

        let info = region.to_info();
        assert_eq!(info.id, RegionId(1));
        assert_eq!(info.peers.len(), 2);
        assert_eq!(info.size_bytes, 1024);
    }
}
