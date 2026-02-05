//! Deadlock detection for ThunderDB.
//!
//! Implements wait-for graph based deadlock detection:
//! - Wait-for graph maintenance
//! - Cycle detection using DFS
//! - Victim selection strategies
//! - Background detection thread

use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thunder_common::prelude::*;

use crate::DeadlockDetector;

/// Strategy for selecting deadlock victim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VictimSelection {
    /// Abort the youngest transaction (highest TxnId)
    Youngest,
    /// Abort the oldest transaction (lowest TxnId)
    Oldest,
    /// Abort the transaction with least work done (fewest locks)
    LeastWork,
    /// Abort the transaction that would break the most cycles
    MostCycles,
}

/// Edge in the wait-for graph.
#[derive(Debug, Clone)]
pub struct WaitEdge {
    /// Transaction that is waiting
    pub waiter: TxnId,
    /// Transaction being waited on
    pub holder: TxnId,
    /// Resource being waited for
    pub resource: String,
    /// When the wait started
    pub wait_start: Instant,
}

impl WaitEdge {
    pub fn new(waiter: TxnId, holder: TxnId, resource: impl Into<String>) -> Self {
        Self {
            waiter,
            holder,
            resource: resource.into(),
            wait_start: Instant::now(),
        }
    }

    /// Get wait duration.
    pub fn wait_duration(&self) -> Duration {
        self.wait_start.elapsed()
    }
}

/// Detected deadlock cycle.
#[derive(Debug, Clone)]
pub struct DeadlockCycle {
    /// Transactions involved in the cycle
    pub transactions: Vec<TxnId>,
    /// Edges in the cycle
    pub edges: Vec<WaitEdge>,
    /// Selected victim
    pub victim: TxnId,
}

/// Wait-for graph for deadlock detection.
pub struct WaitForGraph {
    /// Adjacency list: waiter -> list of (holder, edge)
    edges: DashMap<TxnId, Vec<WaitEdge>>,
    /// Reverse adjacency list: holder -> list of waiters
    reverse_edges: DashMap<TxnId, HashSet<TxnId>>,
    /// Transaction metadata: txn_id -> (start_time, lock_count)
    txn_metadata: DashMap<TxnId, (Instant, usize)>,
    /// Victim selection strategy
    victim_strategy: VictimSelection,
    /// Statistics
    stats: RwLock<DeadlockStats>,
}

/// Deadlock detection statistics.
#[derive(Debug, Clone, Default)]
pub struct DeadlockStats {
    pub edges_added: u64,
    pub edges_removed: u64,
    pub cycles_detected: u64,
    pub victims_selected: u64,
    pub detection_runs: u64,
    pub total_detection_time_us: u64,
}

impl WaitForGraph {
    /// Create a new wait-for graph.
    pub fn new() -> Self {
        Self::with_strategy(VictimSelection::Youngest)
    }

    /// Create with specific victim selection strategy.
    pub fn with_strategy(strategy: VictimSelection) -> Self {
        Self {
            edges: DashMap::new(),
            reverse_edges: DashMap::new(),
            txn_metadata: DashMap::new(),
            victim_strategy: strategy,
            stats: RwLock::new(DeadlockStats::default()),
        }
    }

    /// Register a transaction.
    pub fn register_txn(&self, txn_id: TxnId) {
        self.txn_metadata.insert(txn_id, (Instant::now(), 0));
    }

    /// Update lock count for a transaction.
    pub fn update_lock_count(&self, txn_id: TxnId, count: usize) {
        if let Some(mut entry) = self.txn_metadata.get_mut(&txn_id) {
            entry.1 = count;
        }
    }

    /// Add a wait edge: waiter is waiting for holder.
    pub fn add_edge(&self, waiter: TxnId, holder: TxnId, resource: impl Into<String>) {
        if waiter == holder {
            return; // Don't add self-loops
        }

        let edge = WaitEdge::new(waiter, holder, resource);

        self.edges
            .entry(waiter)
            .or_insert_with(Vec::new)
            .push(edge);

        self.reverse_edges
            .entry(holder)
            .or_insert_with(HashSet::new)
            .insert(waiter);

        self.stats.write().edges_added += 1;
    }

    /// Remove all wait edges for a transaction (when it commits/aborts).
    pub fn remove_txn(&self, txn_id: TxnId) {
        // Remove outgoing edges
        if let Some((_, edges)) = self.edges.remove(&txn_id) {
            for edge in &edges {
                if let Some(mut waiters) = self.reverse_edges.get_mut(&edge.holder) {
                    waiters.remove(&txn_id);
                }
            }
            self.stats.write().edges_removed += edges.len() as u64;
        }

        // Remove incoming edges (others waiting for this txn)
        if let Some((_, waiters)) = self.reverse_edges.remove(&txn_id) {
            for waiter in waiters {
                if let Some(mut edges) = self.edges.get_mut(&waiter) {
                    let before = edges.len();
                    edges.retain(|e| e.holder != txn_id);
                    self.stats.write().edges_removed += (before - edges.len()) as u64;
                }
            }
        }

        // Remove metadata
        self.txn_metadata.remove(&txn_id);
    }

    /// Remove specific wait edge.
    pub fn remove_edge(&self, waiter: TxnId, holder: TxnId) {
        if let Some(mut edges) = self.edges.get_mut(&waiter) {
            let before = edges.len();
            edges.retain(|e| e.holder != holder);
            if before > edges.len() {
                self.stats.write().edges_removed += 1;
            }
        }

        if let Some(mut waiters) = self.reverse_edges.get_mut(&holder) {
            waiters.remove(&waiter);
        }
    }

    /// Detect deadlock and return cycle if found.
    pub fn detect(&self) -> Option<DeadlockCycle> {
        let start = Instant::now();
        self.stats.write().detection_runs += 1;

        // Get all transactions that are waiting
        let waiters: Vec<TxnId> = self.edges.iter().map(|e| *e.key()).collect();

        for start_txn in waiters {
            if let Some(cycle) = self.find_cycle(start_txn) {
                self.stats.write().cycles_detected += 1;
                self.stats.write().total_detection_time_us += start.elapsed().as_micros() as u64;

                let victim = self.select_victim(&cycle);
                self.stats.write().victims_selected += 1;

                return Some(DeadlockCycle {
                    transactions: cycle.iter().map(|(t, _)| *t).collect(),
                    edges: cycle.into_iter().map(|(_, e)| e).collect(),
                    victim,
                });
            }
        }

        self.stats.write().total_detection_time_us += start.elapsed().as_micros() as u64;
        None
    }

    /// Find a cycle starting from the given transaction using DFS.
    fn find_cycle(&self, start: TxnId) -> Option<Vec<(TxnId, WaitEdge)>> {
        let mut visited = HashSet::new();
        let mut path = Vec::new();
        let mut path_set = HashSet::new();

        self.dfs_cycle(start, &mut visited, &mut path, &mut path_set)
    }

    /// DFS helper for cycle detection.
    fn dfs_cycle(
        &self,
        current: TxnId,
        visited: &mut HashSet<TxnId>,
        path: &mut Vec<(TxnId, WaitEdge)>,
        path_set: &mut HashSet<TxnId>,
    ) -> Option<Vec<(TxnId, WaitEdge)>> {
        if path_set.contains(&current) {
            // Found a cycle - extract it
            let cycle_start = path.iter().position(|(t, _)| *t == current)?;
            return Some(path[cycle_start..].to_vec());
        }

        if visited.contains(&current) {
            return None;
        }

        visited.insert(current);
        path_set.insert(current);

        if let Some(edges) = self.edges.get(&current) {
            for edge in edges.iter() {
                path.push((current, edge.clone()));

                if let Some(cycle) = self.dfs_cycle(edge.holder, visited, path, path_set) {
                    return Some(cycle);
                }

                path.pop();
            }
        }

        path_set.remove(&current);
        None
    }

    /// Select victim transaction from a cycle.
    fn select_victim(&self, cycle: &[(TxnId, WaitEdge)]) -> TxnId {
        let txns: Vec<TxnId> = cycle.iter().map(|(t, _)| *t).collect();

        match self.victim_strategy {
            VictimSelection::Youngest => {
                // Highest TxnId (most recently started)
                *txns.iter().max().unwrap_or(&txns[0])
            }
            VictimSelection::Oldest => {
                // Lowest TxnId (oldest)
                *txns.iter().min().unwrap_or(&txns[0])
            }
            VictimSelection::LeastWork => {
                // Transaction with fewest locks
                txns.iter()
                    .min_by_key(|t| {
                        self.txn_metadata
                            .get(t)
                            .map(|m| m.1)
                            .unwrap_or(0)
                    })
                    .copied()
                    .unwrap_or(txns[0])
            }
            VictimSelection::MostCycles => {
                // Transaction that appears in most edges (simplification)
                let mut counts: HashMap<TxnId, usize> = HashMap::new();
                for txn in &txns {
                    if let Some(edges) = self.edges.get(txn) {
                        *counts.entry(*txn).or_default() += edges.len();
                    }
                    if let Some(waiters) = self.reverse_edges.get(txn) {
                        *counts.entry(*txn).or_default() += waiters.len();
                    }
                }
                counts
                    .into_iter()
                    .max_by_key(|(_, c)| *c)
                    .map(|(t, _)| t)
                    .unwrap_or(txns[0])
            }
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> DeadlockStats {
        self.stats.read().clone()
    }

    /// Get number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.edges.iter().map(|e| e.value().len()).sum()
    }

    /// Get number of transactions in the graph.
    pub fn txn_count(&self) -> usize {
        self.edges.len()
    }

    /// Check if a transaction is waiting.
    pub fn is_waiting(&self, txn_id: TxnId) -> bool {
        self.edges.get(&txn_id).map(|e| !e.is_empty()).unwrap_or(false)
    }

    /// Get transactions that are waiting on a specific transaction.
    pub fn get_waiters(&self, holder: TxnId) -> Vec<TxnId> {
        self.reverse_edges
            .get(&holder)
            .map(|w| w.iter().copied().collect())
            .unwrap_or_default()
    }
}

impl Default for WaitForGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl DeadlockDetector for WaitForGraph {
    fn add_wait(&self, waiter: TxnId, holder: TxnId) {
        self.add_edge(waiter, holder, "unknown");
    }

    fn remove_wait(&self, txn_id: TxnId) {
        self.remove_txn(txn_id);
    }

    fn detect(&self) -> Option<TxnId> {
        WaitForGraph::detect(self).map(|cycle| cycle.victim)
    }
}

/// Background deadlock detector that periodically checks for deadlocks.
pub struct BackgroundDeadlockDetector {
    graph: Arc<WaitForGraph>,
    interval: Duration,
    running: Arc<AtomicBool>,
    detected_count: Arc<AtomicU64>,
}

impl BackgroundDeadlockDetector {
    /// Create a new background detector.
    pub fn new(graph: Arc<WaitForGraph>, interval: Duration) -> Self {
        Self {
            graph,
            interval,
            running: Arc::new(AtomicBool::new(false)),
            detected_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Start the background detector.
    pub fn start(&self) -> Option<std::thread::JoinHandle<()>> {
        if self.running.swap(true, Ordering::SeqCst) {
            return None; // Already running
        }

        let graph = self.graph.clone();
        let interval = self.interval;
        let running = self.running.clone();
        let detected_count = self.detected_count.clone();

        Some(std::thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                std::thread::sleep(interval);

                if let Some(cycle) = graph.detect() {
                    detected_count.fetch_add(1, Ordering::SeqCst);
                    tracing::warn!(
                        "Deadlock detected! Victim: {:?}, Cycle: {:?}",
                        cycle.victim,
                        cycle.transactions
                    );
                }
            }
        }))
    }

    /// Stop the background detector.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get number of deadlocks detected.
    pub fn detected_count(&self) -> u64 {
        self.detected_count.load(Ordering::SeqCst)
    }
}

/// Timeout-based deadlock prevention.
/// Aborts transactions that have been waiting too long.
pub struct TimeoutDetector {
    /// Maximum wait time before considering it a deadlock
    timeout: Duration,
    /// Transactions and their wait start times
    wait_times: DashMap<TxnId, Instant>,
}

impl TimeoutDetector {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            wait_times: DashMap::new(),
        }
    }

    /// Record that a transaction started waiting.
    pub fn start_wait(&self, txn_id: TxnId) {
        self.wait_times.insert(txn_id, Instant::now());
    }

    /// Record that a transaction stopped waiting.
    pub fn end_wait(&self, txn_id: TxnId) {
        self.wait_times.remove(&txn_id);
    }

    /// Check for transactions that have timed out.
    pub fn check_timeouts(&self) -> Vec<TxnId> {
        let now = Instant::now();
        self.wait_times
            .iter()
            .filter(|e| now.duration_since(*e.value()) > self.timeout)
            .map(|e| *e.key())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wait_for_graph_basic() {
        let graph = WaitForGraph::new();

        graph.add_edge(TxnId(1), TxnId(2), "row1");
        graph.add_edge(TxnId(2), TxnId(3), "row2");

        assert_eq!(graph.edge_count(), 2);
        assert!(graph.is_waiting(TxnId(1)));
        assert!(graph.is_waiting(TxnId(2)));
        assert!(!graph.is_waiting(TxnId(3)));
    }

    #[test]
    fn test_no_cycle() {
        let graph = WaitForGraph::new();

        // Linear chain: 1 -> 2 -> 3
        graph.add_edge(TxnId(1), TxnId(2), "row1");
        graph.add_edge(TxnId(2), TxnId(3), "row2");

        assert!(graph.detect().is_none());
    }

    #[test]
    fn test_simple_cycle() {
        let graph = WaitForGraph::new();

        // Cycle: 1 -> 2 -> 3 -> 1
        graph.add_edge(TxnId(1), TxnId(2), "row1");
        graph.add_edge(TxnId(2), TxnId(3), "row2");
        graph.add_edge(TxnId(3), TxnId(1), "row3");

        let cycle = graph.detect();
        assert!(cycle.is_some());

        let cycle = cycle.unwrap();
        assert_eq!(cycle.transactions.len(), 3);
        assert!(cycle.transactions.contains(&TxnId(1)));
        assert!(cycle.transactions.contains(&TxnId(2)));
        assert!(cycle.transactions.contains(&TxnId(3)));
    }

    #[test]
    fn test_two_node_cycle() {
        let graph = WaitForGraph::new();

        // Cycle: 1 <-> 2
        graph.add_edge(TxnId(1), TxnId(2), "row1");
        graph.add_edge(TxnId(2), TxnId(1), "row2");

        let cycle = graph.detect();
        assert!(cycle.is_some());
        assert_eq!(cycle.unwrap().transactions.len(), 2);
    }

    #[test]
    fn test_victim_selection_youngest() {
        let graph = WaitForGraph::with_strategy(VictimSelection::Youngest);

        graph.add_edge(TxnId(1), TxnId(2), "row1");
        graph.add_edge(TxnId(2), TxnId(1), "row2");

        let cycle = graph.detect().unwrap();
        assert_eq!(cycle.victim, TxnId(2)); // Highest TxnId
    }

    #[test]
    fn test_victim_selection_oldest() {
        let graph = WaitForGraph::with_strategy(VictimSelection::Oldest);

        graph.add_edge(TxnId(1), TxnId(2), "row1");
        graph.add_edge(TxnId(2), TxnId(1), "row2");

        let cycle = graph.detect().unwrap();
        assert_eq!(cycle.victim, TxnId(1)); // Lowest TxnId
    }

    #[test]
    fn test_remove_txn() {
        let graph = WaitForGraph::new();

        graph.add_edge(TxnId(1), TxnId(2), "row1");
        graph.add_edge(TxnId(2), TxnId(3), "row2");
        graph.add_edge(TxnId(3), TxnId(1), "row3");

        // Removing txn 2 should break the cycle
        graph.remove_txn(TxnId(2));

        assert!(graph.detect().is_none());
    }

    #[test]
    fn test_get_waiters() {
        let graph = WaitForGraph::new();

        graph.add_edge(TxnId(1), TxnId(3), "row1");
        graph.add_edge(TxnId(2), TxnId(3), "row2");

        let waiters = graph.get_waiters(TxnId(3));
        assert_eq!(waiters.len(), 2);
        assert!(waiters.contains(&TxnId(1)));
        assert!(waiters.contains(&TxnId(2)));
    }

    #[test]
    fn test_self_loop_ignored() {
        let graph = WaitForGraph::new();

        // Self-loop should be ignored
        graph.add_edge(TxnId(1), TxnId(1), "row1");

        assert_eq!(graph.edge_count(), 0);
        assert!(graph.detect().is_none());
    }

    #[test]
    fn test_stats() {
        let graph = WaitForGraph::new();

        graph.add_edge(TxnId(1), TxnId(2), "row1");
        graph.add_edge(TxnId(2), TxnId(1), "row2");

        graph.detect();

        let stats = graph.stats();
        assert_eq!(stats.edges_added, 2);
        assert_eq!(stats.detection_runs, 1);
        assert_eq!(stats.cycles_detected, 1);
    }

    #[test]
    fn test_timeout_detector() {
        let detector = TimeoutDetector::new(Duration::from_millis(10));

        detector.start_wait(TxnId(1));

        // Immediately check - should be empty
        assert!(detector.check_timeouts().is_empty());

        // Wait and check again
        std::thread::sleep(Duration::from_millis(20));
        let timed_out = detector.check_timeouts();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], TxnId(1));

        // End wait
        detector.end_wait(TxnId(1));
        assert!(detector.check_timeouts().is_empty());
    }

    #[test]
    fn test_deadlock_detector_trait() {
        let graph = WaitForGraph::new();

        // Use trait methods
        <WaitForGraph as DeadlockDetector>::add_wait(&graph, TxnId(1), TxnId(2));
        <WaitForGraph as DeadlockDetector>::add_wait(&graph, TxnId(2), TxnId(1));

        let victim = <WaitForGraph as DeadlockDetector>::detect(&graph);
        assert!(victim.is_some());

        <WaitForGraph as DeadlockDetector>::remove_wait(&graph, TxnId(1));
        let victim = <WaitForGraph as DeadlockDetector>::detect(&graph);
        assert!(victim.is_none());
    }

    #[test]
    fn test_complex_graph() {
        let graph = WaitForGraph::new();

        // Create a more complex graph with multiple paths
        //     1 -> 2 -> 3
        //     |         |
        //     v         v
        //     4 -> 5 -> 6 -> 1 (cycle through 1-2-3-6-1)

        graph.add_edge(TxnId(1), TxnId(2), "r1");
        graph.add_edge(TxnId(2), TxnId(3), "r2");
        graph.add_edge(TxnId(1), TxnId(4), "r3");
        graph.add_edge(TxnId(4), TxnId(5), "r4");
        graph.add_edge(TxnId(5), TxnId(6), "r5");
        graph.add_edge(TxnId(3), TxnId(6), "r6");
        graph.add_edge(TxnId(6), TxnId(1), "r7"); // Creates cycle

        let cycle = graph.detect();
        assert!(cycle.is_some());
    }
}
