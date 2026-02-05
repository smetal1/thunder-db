//! HNSW (Hierarchical Navigable Small World) Index
//!
//! A graph-based index for approximate nearest neighbor search with logarithmic
//! search complexity. Based on the paper "Efficient and robust approximate nearest
//! neighbor search using Hierarchical Navigable Small World graphs" by Malkov & Yashunin.
//!
//! Key features:
//! - O(log N) search complexity
//! - High recall (>95% typical)
//! - Incremental insertions
//! - Configurable memory/speed tradeoff via M and ef parameters

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use async_trait::async_trait;
use parking_lot::RwLock;
use rand::prelude::*;

use thunder_common::prelude::*;
use crate::{DistanceFunction, HnswConfig, IndexStats, SearchResult, VectorFilter, VectorIndex};

// ============================================================================
// Data Structures
// ============================================================================

/// A node in the HNSW graph
#[derive(Debug, Clone)]
struct HnswNode {
    /// Unique identifier
    id: u64,
    /// The vector data
    vector: Vec<f32>,
    /// Connections at each level (level -> neighbor ids)
    connections: Vec<Vec<u64>>,
    /// Maximum level for this node
    level: usize,
}

impl HnswNode {
    fn new(id: u64, vector: Vec<f32>, level: usize, m: usize) -> Self {
        // Level 0 has 2*M connections, higher levels have M connections
        let connections = (0..=level)
            .map(|l| Vec::with_capacity(if l == 0 { 2 * m } else { m }))
            .collect();

        Self {
            id,
            vector,
            connections,
            level,
        }
    }
}

/// Candidate for nearest neighbor search
#[derive(Clone)]
struct Candidate {
    id: u64,
    distance: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior with BinaryHeap
        other.distance.partial_cmp(&self.distance).unwrap_or(Ordering::Equal)
    }
}

/// Max-heap candidate (for pruning)
#[derive(Clone)]
struct MaxCandidate {
    id: u64,
    distance: f32,
}

impl PartialEq for MaxCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for MaxCandidate {}

impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance.partial_cmp(&other.distance).unwrap_or(Ordering::Equal)
    }
}

// ============================================================================
// HNSW Index Implementation
// ============================================================================

/// HNSW (Hierarchical Navigable Small World) index
pub struct HnswIndex {
    /// Index name
    name: String,
    /// Vector dimensions
    dimensions: usize,
    /// Configuration
    config: HnswConfig,
    /// Nodes in the graph (id -> node)
    nodes: RwLock<HashMap<u64, HnswNode>>,
    /// Entry point (node with highest level)
    entry_point: RwLock<Option<u64>>,
    /// Maximum level in the graph
    max_level: RwLock<usize>,
    /// Level multiplier (1/ln(M))
    level_mult: f64,
    /// Random number generator
    rng: RwLock<StdRng>,
    /// Statistics
    insert_count: AtomicU64,
}

impl HnswIndex {
    /// Create a new HNSW index
    pub fn new(name: impl Into<String>, dimensions: usize, config: HnswConfig) -> Self {
        let level_mult = 1.0 / (config.m as f64).ln();
        let rng = match config.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };

        Self {
            name: name.into(),
            dimensions,
            config,
            nodes: RwLock::new(HashMap::new()),
            entry_point: RwLock::new(None),
            max_level: RwLock::new(0),
            level_mult,
            rng: RwLock::new(rng),
            insert_count: AtomicU64::new(0),
        }
    }

    /// Generate a random level for a new node
    fn random_level(&self) -> usize {
        let mut rng = self.rng.write();
        let r: f64 = rng.gen();
        (-r.ln() * self.level_mult).floor() as usize
    }

    /// Compute distance between two vectors
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        self.config.distance_fn.compute(a, b)
    }

    /// Search for nearest neighbors at a specific layer
    fn search_layer(
        &self,
        query: &[f32],
        entry_points: Vec<u64>,
        ef: usize,
        level: usize,
        nodes: &HashMap<u64, HnswNode>,
    ) -> Vec<Candidate> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new(); // Min-heap
        let mut results = BinaryHeap::new();    // Max-heap for tracking worst result

        // Initialize with entry points
        for ep in entry_points {
            if let Some(node) = nodes.get(&ep) {
                let dist = self.distance(query, &node.vector);
                visited.insert(ep);
                candidates.push(Candidate { id: ep, distance: dist });
                results.push(MaxCandidate { id: ep, distance: dist });
            }
        }

        // Greedy search
        while let Some(candidate) = candidates.pop() {
            // Get the worst result so far
            let worst_dist = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);

            // If the best candidate is worse than our worst result, we're done
            if candidate.distance > worst_dist {
                break;
            }

            // Explore neighbors of this candidate
            if let Some(node) = nodes.get(&candidate.id) {
                if level < node.connections.len() {
                    for &neighbor_id in &node.connections[level] {
                        if visited.contains(&neighbor_id) {
                            continue;
                        }
                        visited.insert(neighbor_id);

                        if let Some(neighbor) = nodes.get(&neighbor_id) {
                            let dist = self.distance(query, &neighbor.vector);
                            let worst_dist = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);

                            // Add to candidates if better than worst result or we need more results
                            if dist < worst_dist || results.len() < ef {
                                candidates.push(Candidate { id: neighbor_id, distance: dist });
                                results.push(MaxCandidate { id: neighbor_id, distance: dist });

                                // Keep only ef best results
                                if results.len() > ef {
                                    results.pop();
                                }
                            }
                        }
                    }
                }
            }
        }

        // Convert results to sorted vector
        let mut result_vec: Vec<Candidate> = results
            .into_iter()
            .map(|c| Candidate { id: c.id, distance: c.distance })
            .collect();
        result_vec.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal));
        result_vec
    }

    /// Select neighbors using simple heuristic
    fn select_neighbors(
        &self,
        candidates: Vec<Candidate>,
        m: usize,
    ) -> Vec<u64> {
        candidates.into_iter()
            .take(m)
            .map(|c| c.id)
            .collect()
    }

    /// Select neighbors using diversity heuristic (better for high dimensions)
    fn select_neighbors_heuristic(
        &self,
        query: &[f32],
        candidates: Vec<Candidate>,
        m: usize,
        nodes: &HashMap<u64, HnswNode>,
    ) -> Vec<u64> {
        if candidates.len() <= m {
            return candidates.into_iter().map(|c| c.id).collect();
        }

        let mut selected = Vec::with_capacity(m);
        let mut remaining: Vec<_> = candidates;

        while selected.len() < m && !remaining.is_empty() {
            // Find the closest remaining candidate
            let (best_idx, _) = remaining
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal)
                })
                .unwrap();

            let best = remaining.remove(best_idx);
            let best_vec = nodes.get(&best.id).map(|n| &n.vector);

            // Only add if it's closer to query than to any selected neighbor
            // (diversity heuristic)
            let should_add = if let Some(bv) = best_vec {
                selected.iter().all(|&s_id| {
                    if let Some(sn) = nodes.get(&s_id) {
                        let dist_to_query = self.distance(query, bv);
                        let dist_to_selected = self.distance(bv, &sn.vector);
                        dist_to_query <= dist_to_selected
                    } else {
                        true
                    }
                })
            } else {
                false
            };

            if should_add || selected.is_empty() {
                selected.push(best.id);
            }
        }

        // If we didn't get enough with heuristic, fill with closest
        if selected.len() < m {
            for c in remaining {
                if selected.len() >= m {
                    break;
                }
                if !selected.contains(&c.id) {
                    selected.push(c.id);
                }
            }
        }

        selected
    }

    /// Insert a single vector (internal implementation)
    fn insert_internal(&self, id: u64, vector: Vec<f32>) -> Result<()> {
        if vector.len() != self.dimensions {
            return Err(Error::Internal(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )));
        }

        let level = self.random_level();
        let mut node = HnswNode::new(id, vector.clone(), level, self.config.m);

        let mut nodes = self.nodes.write();
        let entry_point = *self.entry_point.read();
        let max_level = *self.max_level.read();

        // If this is the first node
        if entry_point.is_none() {
            nodes.insert(id, node);
            drop(nodes);
            *self.entry_point.write() = Some(id);
            *self.max_level.write() = level;
            self.insert_count.fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(());
        }

        let entry_id = entry_point.unwrap();
        let mut current_entry = vec![entry_id];

        // Traverse from top to node's level, finding closest entry point
        for l in (level + 1..=max_level).rev() {
            let nearest = self.search_layer(&vector, current_entry.clone(), 1, l, &nodes);
            if !nearest.is_empty() {
                current_entry = vec![nearest[0].id];
            }
        }

        // Insert at each level from node's level down to 0
        for l in (0..=level.min(max_level)).rev() {
            let ef = self.config.ef_construction;
            let candidates = self.search_layer(&vector, current_entry.clone(), ef, l, &nodes);

            // Select neighbors
            let m = if l == 0 { 2 * self.config.m } else { self.config.m };
            let neighbors = self.select_neighbors_heuristic(&vector, candidates.clone(), m, &nodes);

            // Connect node to neighbors
            node.connections[l] = neighbors.clone();

            // Connect neighbors back to node (bidirectional)
            for &neighbor_id in &neighbors {
                // First, check if pruning is needed and collect data
                let needs_pruning = if let Some(neighbor) = nodes.get(&neighbor_id) {
                    if l < neighbor.connections.len() {
                        let max_conn = if l == 0 { 2 * self.config.m } else { self.config.m };
                        neighbor.connections[l].len() >= max_conn
                    } else {
                        false
                    }
                } else {
                    false
                };

                // Collect pruning data if needed (immutable borrow)
                let pruning_data = if needs_pruning {
                    if let Some(neighbor) = nodes.get(&neighbor_id) {
                        if l < neighbor.connections.len() {
                            let neighbor_vec = neighbor.vector.clone();
                            let conn_ids: Vec<u64> = neighbor.connections[l].clone();
                            let max_conn = if l == 0 { 2 * self.config.m } else { self.config.m };

                            // Build candidates from connections
                            let neighbor_candidates: Vec<Candidate> = conn_ids
                                .iter()
                                .chain(std::iter::once(&id)) // Include the new connection
                                .filter_map(|&nid| {
                                    nodes.get(&nid).map(|n| Candidate {
                                        id: nid,
                                        distance: self.distance(&neighbor_vec, &n.vector),
                                    })
                                })
                                .collect();

                            let new_conns = self.select_neighbors(neighbor_candidates, max_conn);
                            Some(new_conns)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Now apply the changes (mutable borrow)
                if let Some(neighbor) = nodes.get_mut(&neighbor_id) {
                    if l < neighbor.connections.len() {
                        if let Some(new_conns) = pruning_data {
                            neighbor.connections[l] = new_conns;
                        } else {
                            neighbor.connections[l].push(id);
                        }
                    }
                }
            }

            // Update entry for next level
            if !candidates.is_empty() {
                current_entry = candidates.into_iter().take(1).map(|c| c.id).collect();
            }
        }

        nodes.insert(id, node);

        // Update entry point if this node has higher level
        drop(nodes);
        if level > max_level {
            *self.entry_point.write() = Some(id);
            *self.max_level.write() = level;
        }

        self.insert_count.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    /// Search with a specific ef parameter
    pub fn search_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: Option<&dyn Fn(u64) -> bool>,
    ) -> Result<Vec<SearchResult>> {
        if query.len() != self.dimensions {
            return Err(Error::Internal(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimensions,
                query.len()
            )));
        }

        let nodes = self.nodes.read();
        let entry_point = *self.entry_point.read();
        let max_level = *self.max_level.read();

        if entry_point.is_none() {
            return Ok(vec![]);
        }

        let mut current_entry = vec![entry_point.unwrap()];

        // Traverse from top level to level 1, finding closest entry point
        for l in (1..=max_level).rev() {
            let nearest = self.search_layer(query, current_entry.clone(), 1, l, &nodes);
            if !nearest.is_empty() {
                current_entry = vec![nearest[0].id];
            }
        }

        // Search at level 0 with ef candidates
        let candidates = self.search_layer(query, current_entry, ef.max(k), 0, &nodes);

        // Apply filter and return top k
        let results: Vec<SearchResult> = candidates
            .into_iter()
            .filter(|c| filter.map_or(true, |f| f(c.id)))
            .take(k)
            .map(|c| SearchResult {
                id: c.id,
                distance: c.distance,
            })
            .collect();

        Ok(results)
    }

    /// Get a vector by id
    pub fn get(&self, id: u64) -> Option<Vec<f32>> {
        self.nodes.read().get(&id).map(|n| n.vector.clone())
    }

    /// Check if a vector exists
    pub fn contains(&self, id: u64) -> bool {
        self.nodes.read().contains_key(&id)
    }

    /// Get the number of vectors
    pub fn len(&self) -> usize {
        self.nodes.read().len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.read().is_empty()
    }
}

#[async_trait]
impl VectorIndex for HnswIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    async fn insert(&self, id: u64, vector: &[f32]) -> Result<()> {
        self.insert_internal(id, vector.to_vec())
    }

    async fn delete(&self, id: u64) -> Result<bool> {
        let mut nodes = self.nodes.write();

        // Remove the node
        let removed = nodes.remove(&id);
        if removed.is_none() {
            return Ok(false);
        }

        let removed_node = removed.unwrap();

        // Remove references to this node from all neighbors
        for level in 0..removed_node.connections.len() {
            for &neighbor_id in &removed_node.connections[level] {
                if let Some(neighbor) = nodes.get_mut(&neighbor_id) {
                    if level < neighbor.connections.len() {
                        neighbor.connections[level].retain(|&x| x != id);
                    }
                }
            }
        }

        // Update entry point if necessary
        drop(nodes);
        let entry_point = *self.entry_point.read();
        if entry_point == Some(id) {
            let nodes = self.nodes.read();
            // Find a new entry point (node with highest level)
            let new_entry = nodes
                .iter()
                .max_by_key(|(_, n)| n.level)
                .map(|(&id, n)| (id, n.level));

            drop(nodes);
            if let Some((new_id, new_level)) = new_entry {
                *self.entry_point.write() = Some(new_id);
                *self.max_level.write() = new_level;
            } else {
                *self.entry_point.write() = None;
                *self.max_level.write() = 0;
            }
        }

        Ok(true)
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        // Use ef = max(k, ef_construction / 2) for search
        let ef = k.max(self.config.ef_construction / 2);
        self.search_with_ef(query, k, ef, None)
    }

    async fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<SearchResult>> {
        let ef = k.max(self.config.ef_construction / 2);
        // Convert VectorFilter to a reference that works with our internal method
        match filter {
            Some(f) => self.search_with_ef(query, k, ef, Some(f.as_ref())),
            None => self.search_with_ef(query, k, ef, None),
        }
    }

    async fn bulk_insert(&self, vectors: &[(u64, Vec<f32>)]) -> Result<()> {
        for (id, vector) in vectors {
            self.insert_internal(*id, vector.clone())?;
        }
        Ok(())
    }

    fn stats(&self) -> IndexStats {
        let nodes = self.nodes.read();
        let vector_count = nodes.len() as u64;

        // Estimate memory: vectors + connections
        let vector_mem = vector_count * self.dimensions as u64 * 4; // 4 bytes per f32
        let conn_mem: u64 = nodes
            .values()
            .map(|n| {
                n.connections
                    .iter()
                    .map(|c| c.len() as u64 * 8) // 8 bytes per u64
                    .sum::<u64>()
            })
            .sum();

        IndexStats {
            vector_count,
            dimensions: self.dimensions,
            memory_bytes: vector_mem + conn_mem,
            index_type: "HNSW".to_string(),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_index() -> HnswIndex {
        let config = HnswConfig {
            m: 8,
            ef_construction: 100,
            distance_fn: DistanceFunction::Euclidean,
            seed: Some(42),
        };
        HnswIndex::new("test", 3, config)
    }

    #[tokio::test]
    async fn test_insert_and_search() {
        let index = create_test_index();

        // Insert some vectors
        index.insert(1, &[1.0, 0.0, 0.0]).await.unwrap();
        index.insert(2, &[0.0, 1.0, 0.0]).await.unwrap();
        index.insert(3, &[0.0, 0.0, 1.0]).await.unwrap();
        index.insert(4, &[1.0, 1.0, 0.0]).await.unwrap();
        index.insert(5, &[1.0, 1.0, 1.0]).await.unwrap();

        assert_eq!(index.len(), 5);

        // Search for nearest to [1, 0, 0]
        let results = index.search(&[1.0, 0.0, 0.0], 3).await.unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].id, 1); // Exact match should be first
        assert!(results[0].distance < 0.001);
    }

    #[tokio::test]
    async fn test_delete() {
        let index = create_test_index();

        index.insert(1, &[1.0, 0.0, 0.0]).await.unwrap();
        index.insert(2, &[0.0, 1.0, 0.0]).await.unwrap();

        assert!(index.contains(1));
        assert!(index.delete(1).await.unwrap());
        assert!(!index.contains(1));
        assert_eq!(index.len(), 1);
    }

    #[tokio::test]
    async fn test_bulk_insert() {
        let index = create_test_index();

        let vectors: Vec<(u64, Vec<f32>)> = (0..100)
            .map(|i| {
                let v = vec![i as f32, (i * 2) as f32, (i * 3) as f32];
                (i as u64, v)
            })
            .collect();

        index.bulk_insert(&vectors).await.unwrap();
        assert_eq!(index.len(), 100);

        // Search should find the exact vector
        let results = index.search(&[50.0, 100.0, 150.0], 1).await.unwrap();
        assert_eq!(results[0].id, 50);
    }

    #[tokio::test]
    async fn test_filtered_search() {
        let index = create_test_index();

        for i in 0..10 {
            index.insert(i, &[i as f32, 0.0, 0.0]).await.unwrap();
        }

        // Search with filter that only accepts even ids
        let filter: VectorFilter = Box::new(|id: u64| id % 2 == 0);
        let results = index.search_filtered(&[5.0, 0.0, 0.0], 3, Some(&filter)).await.unwrap();

        for r in &results {
            assert!(r.id % 2 == 0);
        }
    }

    #[tokio::test]
    async fn test_cosine_distance() {
        let config = HnswConfig {
            m: 8,
            ef_construction: 100,
            distance_fn: DistanceFunction::Cosine,
            seed: Some(42),
        };
        let index = HnswIndex::new("cosine_test", 3, config);

        // Insert vectors with different magnitudes but same direction
        index.insert(1, &[1.0, 0.0, 0.0]).await.unwrap();
        index.insert(2, &[10.0, 0.0, 0.0]).await.unwrap(); // Same direction, different magnitude
        index.insert(3, &[0.0, 1.0, 0.0]).await.unwrap();

        // Search should find both vectors in same direction
        let results = index.search(&[5.0, 0.0, 0.0], 2).await.unwrap();
        assert!(results.iter().any(|r| r.id == 1));
        assert!(results.iter().any(|r| r.id == 2));
    }

    #[test]
    fn test_stats() {
        let index = create_test_index();
        let stats = index.stats();
        assert_eq!(stats.vector_count, 0);
        assert_eq!(stats.dimensions, 3);
        assert_eq!(stats.index_type, "HNSW");
    }
}
