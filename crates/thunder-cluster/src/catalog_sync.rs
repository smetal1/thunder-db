//! Distributed Table Catalog
//!
//! Tracks table locations across cluster nodes via gossip-propagated metadata.
//! Provides the QueryExecutor trait to avoid circular dependencies between
//! the cluster and server crates.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thunder_common::prelude::*;

use crate::topology::{GridTopology, ShardConfig, TableShard, TableShardMap};

/// Entry describing a table's location in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedTableEntry {
    pub table_name: String,
    pub owner_node: NodeId,
    pub replica_nodes: Vec<NodeId>,
    /// Column definitions: (name, type_name)
    pub columns: Vec<(String, String)>,
    pub schema_version: u64,
    pub is_sharded: bool,
    pub shard_count: u32,
}

/// Result of a forwarded query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardedQueryResult {
    pub columns: Vec<(String, String)>,
    /// bincode-serialized `Vec<Row>`
    pub rows_data: Vec<u8>,
    pub rows_affected: u64,
    pub execution_time_us: u64,
}

/// Trait for executing forwarded queries.
/// Implemented by `DatabaseEngine` in thunder-server.
/// Using a trait here avoids circular dependencies (cluster -> server).
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn execute_forwarded_query(
        &self,
        sql: &str,
        query_id: &str,
    ) -> Result<ForwardedQueryResult>;
}

/// Distributed catalog tracking table locations across the cluster.
pub struct DistributedCatalog {
    /// table_name -> entry
    entries: DashMap<String, DistributedTableEntry>,
    /// Current grid topology
    topology: RwLock<GridTopology>,
    /// Shard map for auto-sharded tables
    shard_map: TableShardMap,
    /// Monotonically increasing version
    version: AtomicU64,
    /// This node's ID
    local_node: NodeId,
    /// Shard config
    shard_config: ShardConfig,
}

impl DistributedCatalog {
    pub fn new(local_node: NodeId, shard_count: usize, replica_count: usize) -> Self {
        Self {
            entries: DashMap::new(),
            topology: RwLock::new(GridTopology::new(shard_count, replica_count, 128)),
            shard_map: TableShardMap::new(),
            version: AtomicU64::new(0),
            local_node,
            shard_config: ShardConfig::default(),
        }
    }

    /// Register a table owned by a specific node.
    pub fn register_table(
        &self,
        name: &str,
        columns: Vec<(String, String)>,
        node: NodeId,
    ) {
        let topo = self.topology.read();
        let replicas = topo.replica_nodes(name);

        let entry = DistributedTableEntry {
            table_name: name.to_string(),
            owner_node: node,
            replica_nodes: replicas,
            columns,
            schema_version: self.version.load(Ordering::Relaxed),
            is_sharded: false,
            shard_count: 1,
        };

        self.entries.insert(name.to_string(), entry);
        self.shard_map.register_table(name, node);
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    /// Unregister a table.
    pub fn unregister_table(&self, name: &str) {
        self.entries.remove(name);
        self.shard_map.unregister_table(name);
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    /// Look up a table's location in the catalog.
    pub fn locate_table(&self, name: &str) -> Option<DistributedTableEntry> {
        self.entries.get(name).map(|e| e.clone())
    }

    /// Check if a table exists anywhere in the cluster.
    pub fn table_exists(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Check if a table is owned by the local node.
    pub fn is_local(&self, name: &str) -> bool {
        self.entries
            .get(name)
            .map(|e| e.owner_node == self.local_node)
            .unwrap_or(false)
    }

    /// Get the local node ID.
    pub fn local_node(&self) -> NodeId {
        self.local_node
    }

    /// Apply a gossip update from another node.
    /// Deserializes catalog entries and merges them with local state.
    pub fn apply_gossip_update(&self, _node_id: NodeId, data: &[u8]) {
        if let Ok(entries) = bincode::deserialize::<Vec<DistributedTableEntry>>(data) {
            for entry in entries {
                // Only update if version is newer or we don't have this table
                let should_update = self.entries.get(&entry.table_name)
                    .map(|existing| entry.schema_version > existing.schema_version)
                    .unwrap_or(true);

                if should_update {
                    if entry.is_sharded {
                        // Don't overwrite local shard state here — full sync is separate
                    }
                    self.entries.insert(entry.table_name.clone(), entry);
                }
            }
            self.version.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Serialize catalog for gossip propagation.
    pub fn serialize_for_gossip(&self) -> Vec<u8> {
        let entries: Vec<DistributedTableEntry> = self.entries.iter()
            .map(|e| e.value().clone())
            .collect();
        bincode::serialize(&entries).unwrap_or_default()
    }

    /// Check if a table should be auto-sharded and return new shard info if so.
    pub fn check_and_auto_shard(&self, name: &str, size: u64) -> Option<Vec<TableShard>> {
        if !self.shard_map.check_auto_shard(name, size, &self.shard_config) {
            return None;
        }

        // Find the largest shard and split it at its midpoint
        let shards = self.shard_map.all_shards(name);
        let largest = shards.iter().max_by_key(|s| s.size_bytes)?;

        // Compute midpoint key
        let mid = if largest.start_key.is_empty() && largest.end_key.is_empty() {
            vec![128] // Split an unbounded shard at 128
        } else if largest.end_key.is_empty() {
            let mut mid = largest.start_key.clone();
            if let Some(last) = mid.last_mut() {
                *last = last.wrapping_add(128);
            }
            mid
        } else {
            // Midpoint of start and end keys
            let len = largest.start_key.len().max(largest.end_key.len());
            let mut mid = Vec::with_capacity(len);
            for i in 0..len {
                let s = *largest.start_key.get(i).unwrap_or(&0);
                let e = *largest.end_key.get(i).unwrap_or(&0);
                mid.push(s.wrapping_add(e.wrapping_sub(s) / 2));
            }
            mid
        };

        self.shard_map.split_shard(name, largest.shard_id, mid);

        // Update the catalog entry
        if let Some(mut entry) = self.entries.get_mut(name) {
            entry.is_sharded = true;
            entry.shard_count = self.shard_map.all_shards(name).len() as u32;
        }

        Some(self.shard_map.all_shards(name))
    }

    /// Get all tables in the catalog.
    pub fn all_tables(&self) -> Vec<DistributedTableEntry> {
        self.entries.iter().map(|e| e.value().clone()).collect()
    }

    /// Get current version.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// Get a reference to the shard map.
    pub fn shard_map(&self) -> &TableShardMap {
        &self.shard_map
    }

    /// Get a read lock on the topology.
    pub fn topology(&self) -> parking_lot::RwLockReadGuard<'_, GridTopology> {
        self.topology.read()
    }

    /// Update the topology (e.g., after rebalance).
    pub fn update_topology(&self, topo: GridTopology) {
        *self.topology.write() = topo;
    }

    /// Rebalance topology with current node set.
    pub fn rebalance(&self, nodes: &[crate::NodeInfo]) {
        self.topology.write().rebalance(nodes);
    }
}

/// Distribution classification for a set of tables referenced by a query.
#[derive(Debug)]
pub enum Distribution {
    /// All referenced tables are on the local node.
    AllLocal,
    /// All referenced tables are on a single remote node.
    SingleRemote(NodeId),
    /// Tables span multiple remote nodes — scatter-gather needed.
    MultiNode(Vec<(String, NodeId)>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_register_locate() {
        let catalog = DistributedCatalog::new(NodeId(1), 4, 3);

        catalog.register_table(
            "users",
            vec![
                ("id".to_string(), "INTEGER".to_string()),
                ("name".to_string(), "VARCHAR".to_string()),
            ],
            NodeId(1),
        );

        let entry = catalog.locate_table("users").unwrap();
        assert_eq!(entry.table_name, "users");
        assert_eq!(entry.owner_node, NodeId(1));
        assert_eq!(entry.columns.len(), 2);
    }

    #[test]
    fn test_catalog_is_local() {
        let catalog = DistributedCatalog::new(NodeId(1), 4, 3);
        catalog.register_table("local_table", vec![], NodeId(1));
        catalog.register_table("remote_table", vec![], NodeId(2));

        assert!(catalog.is_local("local_table"));
        assert!(!catalog.is_local("remote_table"));
    }

    #[test]
    fn test_catalog_gossip_roundtrip() {
        let catalog1 = DistributedCatalog::new(NodeId(1), 4, 3);
        catalog1.register_table("t1", vec![("id".into(), "INT".into())], NodeId(1));

        let data = catalog1.serialize_for_gossip();

        let catalog2 = DistributedCatalog::new(NodeId(2), 4, 3);
        catalog2.apply_gossip_update(NodeId(1), &data);

        assert!(catalog2.locate_table("t1").is_some());
    }

    #[test]
    fn test_catalog_unregister() {
        let catalog = DistributedCatalog::new(NodeId(1), 4, 3);
        catalog.register_table("tmp", vec![], NodeId(1));
        assert!(catalog.table_exists("tmp"));

        catalog.unregister_table("tmp");
        assert!(!catalog.table_exists("tmp"));
    }
}
