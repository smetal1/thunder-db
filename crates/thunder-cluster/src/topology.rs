//! Ring + Grid Topology for Distributed Query Routing
//!
//! Provides consistent hash ring placement for table-level routing
//! and a 2D grid view (shard x replica) for replication tracking.
//! Supports auto-sharding for large tables.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thunder_common::prelude::*;

/// Consistent hash ring for mapping keys to nodes.
/// Uses virtual nodes (default 128 per physical node) for even distribution.
#[derive(Debug, Clone)]
pub struct HashRing {
    /// Ring positions: hash position -> node ID
    ring: BTreeMap<u64, NodeId>,
    /// Reverse map: node ID -> list of positions on the ring
    node_positions: HashMap<NodeId, Vec<u64>>,
    /// Number of virtual nodes per physical node
    virtual_nodes: usize,
}

impl HashRing {
    pub fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            node_positions: HashMap::new(),
            virtual_nodes,
        }
    }

    /// Add a node to the ring with `virtual_nodes` positions.
    pub fn add_node(&mut self, node_id: NodeId) {
        let mut positions = Vec::with_capacity(self.virtual_nodes);
        for i in 0..self.virtual_nodes {
            let key = format!("{}:{}", node_id.0, i);
            let hash = Self::hash_key(&key);
            self.ring.insert(hash, node_id);
            positions.push(hash);
        }
        self.node_positions.insert(node_id, positions);
    }

    /// Remove a node and all its virtual positions from the ring.
    pub fn remove_node(&mut self, node_id: NodeId) {
        if let Some(positions) = self.node_positions.remove(&node_id) {
            for pos in positions {
                self.ring.remove(&pos);
            }
        }
    }

    /// Locate the primary owner node for a given key.
    pub fn locate(&self, key: &str) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = Self::hash_key(key);
        // Walk clockwise from hash to find the first node
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, &node_id)| node_id)
    }

    /// Locate up to `n` distinct replica nodes for a key (walking clockwise).
    pub fn locate_n(&self, key: &str, n: usize) -> Vec<NodeId> {
        if self.ring.is_empty() {
            return Vec::new();
        }
        let hash = Self::hash_key(key);
        let mut result = Vec::with_capacity(n);
        let mut seen = std::collections::HashSet::new();

        // Walk clockwise from hash position
        for (_, &node_id) in self.ring.range(hash..).chain(self.ring.iter()) {
            if seen.insert(node_id) {
                result.push(node_id);
                if result.len() >= n {
                    break;
                }
            }
        }

        result
    }

    /// Number of physical nodes on the ring.
    pub fn node_count(&self) -> usize {
        self.node_positions.len()
    }

    /// Check if a node is on the ring.
    pub fn contains_node(&self, node_id: NodeId) -> bool {
        self.node_positions.contains_key(&node_id)
    }

    /// FNV-1a hash for consistent distribution.
    fn hash_key(key: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in key.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

/// 2D Grid topology: rows = shards, columns = replicas.
///
/// ```text
///        Replica 0 (Primary)   Replica 1   Replica 2
/// Shard 0    Node A              Node B      Node C
/// Shard 1    Node B              Node C      Node A
/// ```
#[derive(Debug, Clone)]
pub struct GridTopology {
    ring: HashRing,
    shard_count: usize,
    replica_count: usize,
    /// grid[shard_idx][replica_idx] = Some(NodeId)
    grid: Vec<Vec<Option<NodeId>>>,
}

impl GridTopology {
    pub fn new(shard_count: usize, replica_count: usize, vnodes: usize) -> Self {
        let grid = vec![vec![None; replica_count]; shard_count];
        Self {
            ring: HashRing::new(vnodes),
            shard_count,
            replica_count,
            grid,
        }
    }

    /// Get the primary (shard 0, replica 0 slot) node for a table.
    pub fn primary_node(&self, table: &str) -> Option<NodeId> {
        self.ring.locate(table)
    }

    /// Get all replica nodes for a table (via ring locate_n).
    pub fn replica_nodes(&self, table: &str) -> Vec<NodeId> {
        self.ring.locate_n(table, self.replica_count)
    }

    /// Rebalance the grid after node membership changes.
    /// Rebuilds the ring and reassigns grid cells.
    pub fn rebalance(&mut self, nodes: &[crate::NodeInfo]) {
        // Rebuild ring
        self.ring = HashRing::new(self.ring.virtual_nodes);
        for node in nodes {
            if node.status == crate::NodeStatus::Online {
                self.ring.add_node(node.id);
            }
        }

        // Reassign grid cells
        for shard_idx in 0..self.shard_count {
            let shard_key = format!("__shard_{}", shard_idx);
            let replicas = self.ring.locate_n(&shard_key, self.replica_count);
            for (replica_idx, &node_id) in replicas.iter().enumerate() {
                if replica_idx < self.replica_count {
                    self.grid[shard_idx][replica_idx] = Some(node_id);
                }
            }
            // Clear remaining replicas if fewer nodes than replica_count
            for replica_idx in replicas.len()..self.replica_count {
                self.grid[shard_idx][replica_idx] = None;
            }
        }
    }

    /// Get the grid cell (shard, replica) -> node mapping.
    pub fn grid_cell(&self, shard: usize, replica: usize) -> Option<NodeId> {
        self.grid.get(shard).and_then(|row| row.get(replica).copied().flatten())
    }

    /// Get shard count.
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Get replica count.
    pub fn replica_count(&self) -> usize {
        self.replica_count
    }

    /// Get a reference to the underlying hash ring.
    pub fn ring(&self) -> &HashRing {
        &self.ring
    }

    /// Add a node to the underlying ring.
    pub fn add_node(&mut self, node_id: NodeId) {
        self.ring.add_node(node_id);
    }

    /// Remove a node from the underlying ring.
    pub fn remove_node(&mut self, node_id: NodeId) {
        self.ring.remove_node(node_id);
    }

    /// Serialize topology state to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let state = TopologySerde {
            shard_count: self.shard_count,
            replica_count: self.replica_count,
            vnodes: self.ring.virtual_nodes,
            node_ids: self.ring.node_positions.keys().map(|n| n.0).collect(),
            grid: self.grid.iter().map(|row| {
                row.iter().map(|cell| cell.map(|n| n.0)).collect()
            }).collect(),
        };
        bincode::serialize(&state).unwrap_or_default()
    }

    /// Deserialize topology state from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let state: TopologySerde = bincode::deserialize(data)
            .map_err(|e| Error::Internal(format!("Failed to deserialize topology: {}", e)))?;

        let mut ring = HashRing::new(state.vnodes);
        for node_id in &state.node_ids {
            ring.add_node(NodeId(*node_id));
        }

        let grid = state.grid.into_iter().map(|row| {
            row.into_iter().map(|cell| cell.map(NodeId)).collect()
        }).collect();

        Ok(Self {
            ring,
            shard_count: state.shard_count,
            replica_count: state.replica_count,
            grid,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct TopologySerde {
    shard_count: usize,
    replica_count: usize,
    vnodes: usize,
    node_ids: Vec<u64>,
    grid: Vec<Vec<Option<u64>>>,
}

/// Auto-shard configuration for large tables.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Size threshold (in bytes) above which a table is automatically sharded.
    /// Default: 256MB.
    pub auto_shard_threshold: u64,
    /// Maximum number of shards per table. Default: 64.
    pub max_shards_per_table: usize,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            auto_shard_threshold: 256 * 1024 * 1024, // 256MB
            max_shards_per_table: 64,
        }
    }
}

/// A single shard of an auto-sharded table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableShard {
    pub shard_id: u32,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub owner_node: NodeId,
    pub size_bytes: u64,
}

/// Tracks shard state for auto-sharded tables.
pub struct TableShardMap {
    /// table_name -> list of TableShard entries
    shards: DashMap<String, Vec<TableShard>>,
    version: AtomicU64,
}

impl TableShardMap {
    pub fn new() -> Self {
        Self {
            shards: DashMap::new(),
            version: AtomicU64::new(0),
        }
    }

    /// Check if a table is auto-sharded.
    pub fn is_sharded(&self, table: &str) -> bool {
        self.shards.get(table).map(|v| v.len() > 1).unwrap_or(false)
    }

    /// Locate the shard owning a specific key within a table.
    pub fn locate_shard(&self, table: &str, key: &[u8]) -> Option<TableShard> {
        self.shards.get(table).and_then(|shards| {
            shards.iter().find(|s| {
                (s.start_key.is_empty() || key >= s.start_key.as_slice())
                    && (s.end_key.is_empty() || key < s.end_key.as_slice())
            }).cloned()
        })
    }

    /// Get all shards for a table.
    pub fn all_shards(&self, table: &str) -> Vec<TableShard> {
        self.shards.get(table).map(|v| v.clone()).unwrap_or_default()
    }

    /// Register a single-shard (non-sharded) table.
    pub fn register_table(&self, table: &str, owner_node: NodeId) {
        self.shards.insert(table.to_string(), vec![TableShard {
            shard_id: 0,
            start_key: Vec::new(),
            end_key: Vec::new(),
            owner_node,
            size_bytes: 0,
        }]);
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a table from the shard map.
    pub fn unregister_table(&self, table: &str) {
        self.shards.remove(table);
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    /// Split a shard at the given key, creating two shards.
    pub fn split_shard(&self, table: &str, shard_id: u32, split_key: Vec<u8>) {
        if let Some(mut shards) = self.shards.get_mut(table) {
            if let Some(idx) = shards.iter().position(|s| s.shard_id == shard_id) {
                let old = shards[idx].clone();
                let new_shard_id = shards.iter().map(|s| s.shard_id).max().unwrap_or(0) + 1;

                // Replace old shard with [start_key, split_key)
                shards[idx] = TableShard {
                    shard_id: old.shard_id,
                    start_key: old.start_key,
                    end_key: split_key.clone(),
                    owner_node: old.owner_node,
                    size_bytes: old.size_bytes / 2,
                };

                // Add new shard [split_key, end_key)
                shards.push(TableShard {
                    shard_id: new_shard_id,
                    start_key: split_key,
                    end_key: old.end_key,
                    owner_node: old.owner_node,
                    size_bytes: old.size_bytes / 2,
                });
            }
        }
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if a table should be auto-sharded based on its size.
    pub fn check_auto_shard(&self, table: &str, size: u64, config: &ShardConfig) -> bool {
        if size < config.auto_shard_threshold {
            return false;
        }
        // Don't exceed max shards
        let current_shards = self.shards.get(table).map(|v| v.len()).unwrap_or(0);
        current_shards < config.max_shards_per_table
    }

    /// Update the size of a shard.
    pub fn update_shard_size(&self, table: &str, shard_id: u32, size: u64) {
        if let Some(mut shards) = self.shards.get_mut(table) {
            if let Some(shard) = shards.iter_mut().find(|s| s.shard_id == shard_id) {
                shard.size_bytes = size;
            }
        }
    }

    /// Get the current version (incremented on mutations).
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// Get all shard-owning nodes for a table.
    pub fn owner_nodes(&self, table: &str) -> Vec<NodeId> {
        self.shards.get(table)
            .map(|shards| {
                let mut nodes: Vec<NodeId> = shards.iter().map(|s| s.owner_node).collect();
                nodes.dedup();
                nodes
            })
            .unwrap_or_default()
    }
}

impl Default for TableShardMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_ring_basic() {
        let mut ring = HashRing::new(128);
        ring.add_node(NodeId(1));
        ring.add_node(NodeId(2));
        ring.add_node(NodeId(3));

        assert_eq!(ring.node_count(), 3);

        // Every key should map to some node
        for i in 0..100 {
            let key = format!("table_{}", i);
            assert!(ring.locate(&key).is_some());
        }
    }

    #[test]
    fn test_hash_ring_locate_n() {
        let mut ring = HashRing::new(128);
        ring.add_node(NodeId(1));
        ring.add_node(NodeId(2));
        ring.add_node(NodeId(3));

        let replicas = ring.locate_n("test_table", 3);
        assert_eq!(replicas.len(), 3);
        // All replicas should be distinct
        let mut unique = replicas.clone();
        unique.sort_by_key(|n| n.0);
        unique.dedup();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn test_hash_ring_remove_node() {
        let mut ring = HashRing::new(128);
        ring.add_node(NodeId(1));
        ring.add_node(NodeId(2));

        assert_eq!(ring.node_count(), 2);

        ring.remove_node(NodeId(1));
        assert_eq!(ring.node_count(), 1);

        // All keys should now map to node 2
        for i in 0..100 {
            let key = format!("key_{}", i);
            assert_eq!(ring.locate(&key), Some(NodeId(2)));
        }
    }

    #[test]
    fn test_hash_ring_distribution() {
        let mut ring = HashRing::new(128);
        for i in 1..=5 {
            ring.add_node(NodeId(i));
        }

        let mut counts = HashMap::new();
        for i in 0..10000 {
            let key = format!("table_{}", i);
            let node = ring.locate(&key).unwrap();
            *counts.entry(node).or_insert(0) += 1;
        }

        // Each node should get roughly 2000 +/- 500 (20% deviation)
        for (_, count) in &counts {
            assert!(*count > 1000, "Node got too few keys: {}", count);
            assert!(*count < 3500, "Node got too many keys: {}", count);
        }
    }

    #[test]
    fn test_grid_topology() {
        let mut topo = GridTopology::new(4, 3, 128);
        let nodes = vec![
            crate::NodeInfo { id: NodeId(1), addr: "127.0.0.1:5001".into(), status: crate::NodeStatus::Online, region_count: 0 },
            crate::NodeInfo { id: NodeId(2), addr: "127.0.0.1:5002".into(), status: crate::NodeStatus::Online, region_count: 0 },
            crate::NodeInfo { id: NodeId(3), addr: "127.0.0.1:5003".into(), status: crate::NodeStatus::Online, region_count: 0 },
        ];
        topo.rebalance(&nodes);

        // Primary should always resolve
        assert!(topo.primary_node("test_table").is_some());
        // Replicas should return up to 3 distinct nodes
        let replicas = topo.replica_nodes("test_table");
        assert_eq!(replicas.len(), 3);
    }

    #[test]
    fn test_grid_topology_serialize_roundtrip() {
        let mut topo = GridTopology::new(4, 3, 128);
        let nodes = vec![
            crate::NodeInfo { id: NodeId(1), addr: "127.0.0.1:5001".into(), status: crate::NodeStatus::Online, region_count: 0 },
            crate::NodeInfo { id: NodeId(2), addr: "127.0.0.1:5002".into(), status: crate::NodeStatus::Online, region_count: 0 },
        ];
        topo.rebalance(&nodes);

        let data = topo.serialize();
        let restored = GridTopology::deserialize(&data).unwrap();

        assert_eq!(restored.shard_count(), topo.shard_count());
        assert_eq!(restored.replica_count(), topo.replica_count());
    }

    #[test]
    fn test_table_shard_map() {
        let map = TableShardMap::new();
        map.register_table("users", NodeId(1));

        assert!(!map.is_sharded("users"));
        assert_eq!(map.all_shards("users").len(), 1);

        // Split the single shard
        map.split_shard("users", 0, vec![128]);
        assert!(map.is_sharded("users"));
        assert_eq!(map.all_shards("users").len(), 2);
    }

    #[test]
    fn test_table_shard_map_locate() {
        let map = TableShardMap::new();
        map.register_table("orders", NodeId(1));
        map.split_shard("orders", 0, vec![128]);

        // Key below split point
        let shard = map.locate_shard("orders", &[64]).unwrap();
        assert_eq!(shard.shard_id, 0);

        // Key at or above split point
        let shard = map.locate_shard("orders", &[200]).unwrap();
        assert!(shard.shard_id > 0);
    }

    #[test]
    fn test_auto_shard_check() {
        let map = TableShardMap::new();
        let config = ShardConfig::default();

        map.register_table("small", NodeId(1));
        assert!(!map.check_auto_shard("small", 100 * 1024 * 1024, &config)); // 100MB < 256MB

        assert!(map.check_auto_shard("small", 300 * 1024 * 1024, &config)); // 300MB > 256MB
    }
}
