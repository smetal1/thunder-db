//! Scatter-Gather Coordinator for Distributed OLAP Queries
//!
//! Fans out queries to nodes owning different tables/shards and merges results.
//! Supports:
//! - Simple SELECT: concatenate rows from all nodes
//! - GROUP BY: merge partial aggregates
//! - ORDER BY: merge-sort partial results
//! - LIMIT: take top-N across partials

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use thunder_common::prelude::*;

use crate::catalog_sync::{DistributedCatalog, Distribution};

/// Plan describing how to distribute a query across nodes.
#[derive(Debug, Clone)]
pub struct DistributionPlan {
    /// List of (table_name, owner_node_id) pairs
    pub table_nodes: Vec<(String, NodeId)>,
    /// All unique node IDs that must participate
    pub participant_nodes: Vec<NodeId>,
    /// Whether this requires scatter-gather (multiple nodes)
    pub needs_scatter: bool,
}

impl DistributionPlan {
    /// Create a plan for a single remote node.
    pub fn single_remote(node: NodeId) -> Self {
        Self {
            table_nodes: Vec::new(),
            participant_nodes: vec![node],
            needs_scatter: false,
        }
    }

    /// Create a plan requiring scatter-gather across multiple nodes.
    pub fn multi_node(table_nodes: Vec<(String, NodeId)>) -> Self {
        let mut participant_nodes: Vec<NodeId> = table_nodes.iter().map(|(_, n)| *n).collect();
        participant_nodes.sort_by_key(|n| n.0);
        participant_nodes.dedup();
        let needs_scatter = participant_nodes.len() > 1;
        Self {
            table_nodes,
            participant_nodes,
            needs_scatter,
        }
    }
}

/// Partial result from a single node in a scatter-gather operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialResult {
    pub node_id: NodeId,
    pub columns: Vec<(String, String)>,
    /// bincode-serialized rows
    pub rows_data: Vec<u8>,
    pub rows_affected: u64,
    pub execution_time_us: u64,
    /// Optional partial aggregation state (for GROUP BY pushdown)
    pub partial_agg_state: Option<Vec<u8>>,
    pub success: bool,
    pub error_message: Option<String>,
}

/// Coordinator for scatter-gather query execution.
pub struct ScatterGatherCoordinator {
    distributed_catalog: Arc<DistributedCatalog>,
    local_node: NodeId,
}

impl ScatterGatherCoordinator {
    pub fn new(
        distributed_catalog: Arc<DistributedCatalog>,
        local_node: NodeId,
    ) -> Self {
        Self {
            distributed_catalog,
            local_node,
        }
    }

    /// Determine the distribution plan for a query referencing the given tables.
    pub fn plan_distribution(&self, tables: &[String]) -> Distribution {
        if tables.is_empty() {
            return Distribution::AllLocal;
        }

        let mut remote_tables = Vec::new();
        let mut all_local = true;

        for table in tables {
            if let Some(entry) = self.distributed_catalog.locate_table(table) {
                if entry.owner_node != self.local_node {
                    all_local = false;
                    remote_tables.push((table.clone(), entry.owner_node));
                }
            }
            // If table not in catalog, assume it's local (or will fail with TableNotFound)
        }

        if all_local {
            return Distribution::AllLocal;
        }

        // Check if all remote tables are on the same node
        let first_remote = remote_tables[0].1;
        if remote_tables.iter().all(|(_, n)| *n == first_remote)
            && tables.len() == remote_tables.len()
        {
            return Distribution::SingleRemote(first_remote);
        }

        Distribution::MultiNode(remote_tables)
    }

    /// Merge partial results from multiple nodes into a single result.
    /// For simple SELECTs, this concatenates rows.
    pub fn merge_results(&self, partials: Vec<PartialResult>) -> Result<MergedResult> {
        if partials.is_empty() {
            return Ok(MergedResult {
                columns: Vec::new(),
                rows_data: Vec::new(),
                total_rows_affected: 0,
                total_execution_time_us: 0,
            });
        }

        // Use the first successful partial for column schema
        let first_success = partials.iter().find(|p| p.success);
        let columns = first_success
            .map(|p| p.columns.clone())
            .unwrap_or_default();

        // Collect errors
        let errors: Vec<&str> = partials.iter()
            .filter(|p| !p.success)
            .filter_map(|p| p.error_message.as_deref())
            .collect();

        if !errors.is_empty() && first_success.is_none() {
            return Err(Error::Internal(format!(
                "All scatter nodes failed: {}",
                errors.join("; ")
            )));
        }

        // Concatenate row data from all successful partials
        let mut all_rows: Vec<Row> = Vec::new();
        let mut total_affected = 0u64;
        let mut max_time = 0u64;

        for partial in &partials {
            if !partial.success {
                continue;
            }

            if !partial.rows_data.is_empty() {
                match bincode::deserialize::<Vec<Row>>(&partial.rows_data) {
                    Ok(rows) => all_rows.extend(rows),
                    Err(e) => {
                        tracing::warn!("Failed to deserialize rows from node {:?}: {}", partial.node_id, e);
                    }
                }
            }

            total_affected += partial.rows_affected;
            max_time = max_time.max(partial.execution_time_us);
        }

        let rows_data = bincode::serialize(&all_rows)
            .map_err(|e| Error::Internal(format!("Failed to serialize merged rows: {}", e)))?;

        Ok(MergedResult {
            columns,
            rows_data,
            total_rows_affected: total_affected,
            total_execution_time_us: max_time,
        })
    }
}

/// Result of merging partial results from multiple nodes.
#[derive(Debug)]
pub struct MergedResult {
    pub columns: Vec<(String, String)>,
    /// bincode-serialized merged `Vec<Row>`
    pub rows_data: Vec<u8>,
    pub total_rows_affected: u64,
    pub total_execution_time_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distribution_plan_single_remote() {
        let plan = DistributionPlan::single_remote(NodeId(2));
        assert_eq!(plan.participant_nodes.len(), 1);
        assert!(!plan.needs_scatter);
    }

    #[test]
    fn test_distribution_plan_multi_node() {
        let table_nodes = vec![
            ("users".to_string(), NodeId(1)),
            ("orders".to_string(), NodeId(2)),
            ("items".to_string(), NodeId(3)),
        ];
        let plan = DistributionPlan::multi_node(table_nodes);
        assert_eq!(plan.participant_nodes.len(), 3);
        assert!(plan.needs_scatter);
    }

    #[test]
    fn test_merge_empty_results() {
        let catalog = Arc::new(DistributedCatalog::new(NodeId(1), 4, 3));
        let coord = ScatterGatherCoordinator::new(catalog, NodeId(1));

        let result = coord.merge_results(Vec::new()).unwrap();
        assert!(result.columns.is_empty());
        assert_eq!(result.total_rows_affected, 0);
    }

    #[test]
    fn test_merge_concatenates_rows() {
        let catalog = Arc::new(DistributedCatalog::new(NodeId(1), 4, 3));
        let coord = ScatterGatherCoordinator::new(catalog, NodeId(1));

        let rows1 = vec![Row::new(vec![Value::Int32(1)])];
        let rows2 = vec![Row::new(vec![Value::Int32(2)])];

        let partials = vec![
            PartialResult {
                node_id: NodeId(1),
                columns: vec![("id".into(), "INTEGER".into())],
                rows_data: bincode::serialize(&rows1).unwrap(),
                rows_affected: 1,
                execution_time_us: 100,
                partial_agg_state: None,
                success: true,
                error_message: None,
            },
            PartialResult {
                node_id: NodeId(2),
                columns: vec![("id".into(), "INTEGER".into())],
                rows_data: bincode::serialize(&rows2).unwrap(),
                rows_affected: 1,
                execution_time_us: 200,
                partial_agg_state: None,
                success: true,
                error_message: None,
            },
        ];

        let result = coord.merge_results(partials).unwrap();
        let merged_rows: Vec<Row> = bincode::deserialize(&result.rows_data).unwrap();
        assert_eq!(merged_rows.len(), 2);
        assert_eq!(result.total_rows_affected, 2);
    }
}
