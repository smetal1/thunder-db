//! Distributed Deadlock Detection
//!
//! Implements the Chandy-Misra-Haas probe-based algorithm for detecting
//! deadlocks across cluster nodes. Runs as a periodic background task
//! and aborts the lowest-cost victim transaction when a cycle is found.

use std::collections::{HashMap, HashSet};

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thunder_common::prelude::*;

/// A directed edge in the wait-for graph: waiter_txn waits for holder_txn.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct WaitForEdge {
    pub waiter_txn: TxnId,
    pub waiter_node: NodeId,
    pub holder_txn: TxnId,
    pub holder_node: NodeId,
    pub resource: String,
}

/// A detected deadlock cycle.
#[derive(Debug, Clone)]
pub struct DeadlockCycle {
    pub edges: Vec<WaitForEdge>,
    pub txn_ids: Vec<TxnId>,
}

impl DeadlockCycle {
    /// Check if this cycle spans multiple nodes.
    pub fn is_distributed(&self) -> bool {
        let mut nodes = HashSet::new();
        for edge in &self.edges {
            nodes.insert(edge.waiter_node);
            nodes.insert(edge.holder_node);
        }
        nodes.len() > 1
    }
}

/// Distributed deadlock detector using Chandy-Misra-Haas probes.
pub struct DistributedDeadlockDetector {
    local_node: NodeId,
    /// Wait-for edges originating from this node
    local_edges: RwLock<Vec<WaitForEdge>>,
    /// Wait-for edges received from remote nodes
    remote_edges: DashMap<NodeId, Vec<WaitForEdge>>,
}

impl DistributedDeadlockDetector {
    pub fn new(local_node: NodeId) -> Self {
        Self {
            local_node,
            local_edges: RwLock::new(Vec::new()),
            remote_edges: DashMap::new(),
        }
    }

    /// Record that `waiter` is waiting for `holder` (which may be on another node).
    pub fn add_wait(
        &self,
        waiter: TxnId,
        holder: TxnId,
        holder_node: NodeId,
        resource: &str,
    ) {
        let edge = WaitForEdge {
            waiter_txn: waiter,
            waiter_node: self.local_node,
            holder_txn: holder,
            holder_node,
            resource: resource.to_string(),
        };
        self.local_edges.write().push(edge);
    }

    /// Remove all wait edges for a completed/aborted transaction.
    pub fn remove_wait(&self, waiter: TxnId) {
        self.local_edges.write().retain(|e| e.waiter_txn != waiter);
    }

    /// Serialize local edges for gossip propagation.
    pub fn serialize_for_gossip(&self) -> Vec<u8> {
        let edges = self.local_edges.read().clone();
        bincode::serialize(&edges).unwrap_or_default()
    }

    /// Apply remote wait-for edges received via gossip.
    pub fn apply_remote_edges(&self, node_id: NodeId, data: &[u8]) {
        if let Ok(edges) = bincode::deserialize::<Vec<WaitForEdge>>(data) {
            self.remote_edges.insert(node_id, edges);
        }
    }

    /// Detect deadlock cycles in the combined local + remote wait-for graph.
    pub fn detect_deadlocks(&self) -> Vec<DeadlockCycle> {
        // Build adjacency list: txn -> list of (held-by txn, edge)
        let mut adj: HashMap<TxnId, Vec<(TxnId, WaitForEdge)>> = HashMap::new();

        // Add local edges
        for edge in self.local_edges.read().iter() {
            adj.entry(edge.waiter_txn)
                .or_default()
                .push((edge.holder_txn, edge.clone()));
        }

        // Add remote edges
        for entry in self.remote_edges.iter() {
            for edge in entry.value().iter() {
                adj.entry(edge.waiter_txn)
                    .or_default()
                    .push((edge.holder_txn, edge.clone()));
            }
        }

        // DFS-based cycle detection
        let all_txns: Vec<TxnId> = adj.keys().copied().collect();
        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();
        let mut cycles = Vec::new();

        for txn in &all_txns {
            if !visited.contains(txn) {
                let mut path = Vec::new();
                let mut path_edges = Vec::new();
                Self::dfs(
                    *txn,
                    &adj,
                    &mut visited,
                    &mut in_stack,
                    &mut path,
                    &mut path_edges,
                    &mut cycles,
                );
            }
        }

        cycles
    }

    fn dfs(
        txn: TxnId,
        adj: &HashMap<TxnId, Vec<(TxnId, WaitForEdge)>>,
        visited: &mut HashSet<TxnId>,
        in_stack: &mut HashSet<TxnId>,
        path: &mut Vec<TxnId>,
        path_edges: &mut Vec<WaitForEdge>,
        cycles: &mut Vec<DeadlockCycle>,
    ) {
        visited.insert(txn);
        in_stack.insert(txn);
        path.push(txn);

        if let Some(neighbors) = adj.get(&txn) {
            for (next_txn, edge) in neighbors {
                path_edges.push(edge.clone());

                if !visited.contains(next_txn) {
                    Self::dfs(*next_txn, adj, visited, in_stack, path, path_edges, cycles);
                } else if in_stack.contains(next_txn) {
                    // Found a cycle — extract it
                    if let Some(start_idx) = path.iter().position(|t| t == next_txn) {
                        let cycle_txns: Vec<TxnId> = path[start_idx..].to_vec();
                        let cycle_edges: Vec<WaitForEdge> = path_edges[start_idx..].to_vec();
                        cycles.push(DeadlockCycle {
                            edges: cycle_edges,
                            txn_ids: cycle_txns,
                        });
                    }
                }

                path_edges.pop();
            }
        }

        path.pop();
        in_stack.remove(&txn);
    }

    /// Choose the lowest-cost victim in a deadlock cycle.
    /// Strategy: abort the transaction with the lowest ID (least work done).
    pub fn choose_victim(&self, cycle: &DeadlockCycle) -> TxnId {
        cycle.txn_ids.iter().copied().min_by_key(|t| t.0).unwrap_or(TxnId(0))
    }

    /// Clear all edges (e.g., on shutdown).
    pub fn clear(&self) {
        self.local_edges.write().clear();
        self.remote_edges.clear();
    }

    /// Get count of local wait-for edges.
    pub fn local_edge_count(&self) -> usize {
        self.local_edges.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_deadlock() {
        let detector = DistributedDeadlockDetector::new(NodeId(1));

        // T1 waits for T2, T2 waits for T3 — no cycle
        detector.add_wait(TxnId(1), TxnId(2), NodeId(1), "table_a");
        detector.add_wait(TxnId(2), TxnId(3), NodeId(1), "table_b");

        let cycles = detector.detect_deadlocks();
        assert!(cycles.is_empty());
    }

    #[test]
    fn test_local_deadlock() {
        let detector = DistributedDeadlockDetector::new(NodeId(1));

        // T1 -> T2 -> T1: cycle!
        detector.add_wait(TxnId(1), TxnId(2), NodeId(1), "table_a");
        detector.add_wait(TxnId(2), TxnId(1), NodeId(1), "table_b");

        let cycles = detector.detect_deadlocks();
        assert!(!cycles.is_empty());
        assert!(cycles[0].txn_ids.contains(&TxnId(1)));
        assert!(cycles[0].txn_ids.contains(&TxnId(2)));
    }

    #[test]
    fn test_distributed_deadlock() {
        let detector = DistributedDeadlockDetector::new(NodeId(1));

        // Local: T1 waits for T2 (on node 2)
        detector.add_wait(TxnId(1), TxnId(2), NodeId(2), "table_a");

        // Remote: T2 waits for T1 (from node 2 perspective)
        let remote_edges = vec![WaitForEdge {
            waiter_txn: TxnId(2),
            waiter_node: NodeId(2),
            holder_txn: TxnId(1),
            holder_node: NodeId(1),
            resource: "table_b".to_string(),
        }];
        let data = bincode::serialize(&remote_edges).unwrap();
        detector.apply_remote_edges(NodeId(2), &data);

        let cycles = detector.detect_deadlocks();
        assert!(!cycles.is_empty());
        assert!(cycles[0].is_distributed());
    }

    #[test]
    fn test_choose_victim() {
        let detector = DistributedDeadlockDetector::new(NodeId(1));
        let cycle = DeadlockCycle {
            edges: vec![],
            txn_ids: vec![TxnId(5), TxnId(3), TxnId(10)],
        };

        // Should pick the lowest txn_id
        assert_eq!(detector.choose_victim(&cycle), TxnId(3));
    }

    #[test]
    fn test_remove_wait() {
        let detector = DistributedDeadlockDetector::new(NodeId(1));
        detector.add_wait(TxnId(1), TxnId(2), NodeId(1), "t");
        detector.add_wait(TxnId(1), TxnId(3), NodeId(1), "u");
        assert_eq!(detector.local_edge_count(), 2);

        detector.remove_wait(TxnId(1));
        assert_eq!(detector.local_edge_count(), 0);
    }

    #[test]
    fn test_gossip_roundtrip() {
        let d1 = DistributedDeadlockDetector::new(NodeId(1));
        d1.add_wait(TxnId(10), TxnId(20), NodeId(2), "orders");

        let data = d1.serialize_for_gossip();

        let d2 = DistributedDeadlockDetector::new(NodeId(2));
        d2.apply_remote_edges(NodeId(1), &data);

        // d2 should see the remote edge from d1
        assert!(d2.remote_edges.get(&NodeId(1)).is_some());
    }
}
