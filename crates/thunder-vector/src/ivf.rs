//! IVF (Inverted File) Index
//!
//! A partition-based index that uses k-means clustering to organize vectors
//! into buckets for efficient approximate nearest neighbor search.
//!
//! Key features:
//! - Fast search via cluster pruning
//! - Linear scan within clusters
//! - Configurable accuracy/speed tradeoff via n_probe
//! - Supports training on sample data

use std::collections::HashMap;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use async_trait::async_trait;
use parking_lot::RwLock;
use rand::prelude::*;

use thunder_common::prelude::*;
use crate::{IvfConfig, IndexStats, SearchResult, VectorFilter, VectorIndex};

// ============================================================================
// Data Structures
// ============================================================================

/// A cluster centroid
#[derive(Debug, Clone)]
struct Centroid {
    /// Centroid vector
    vector: Vec<f32>,
    /// Vectors assigned to this cluster
    vectors: Vec<(u64, Vec<f32>)>,
}

impl Centroid {
    fn new(vector: Vec<f32>) -> Self {
        Self {
            vector,
            vectors: Vec::new(),
        }
    }
}

/// Candidate for search results
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
        self.distance.partial_cmp(&other.distance).unwrap_or(Ordering::Equal)
    }
}

// ============================================================================
// IVF Index Implementation
// ============================================================================

/// IVF (Inverted File) index
pub struct IvfIndex {
    /// Index name
    name: String,
    /// Vector dimensions
    dimensions: usize,
    /// Configuration
    config: IvfConfig,
    /// Cluster centroids
    centroids: RwLock<Vec<Centroid>>,
    /// Whether the index has been trained
    trained: RwLock<bool>,
    /// All vectors (for flat fallback when not trained)
    flat_vectors: RwLock<HashMap<u64, Vec<f32>>>,
    /// Random number generator
    rng: RwLock<StdRng>,
    /// Statistics
    vector_count: AtomicU64,
}

impl IvfIndex {
    /// Create a new IVF index
    pub fn new(name: impl Into<String>, dimensions: usize, config: IvfConfig) -> Self {
        Self {
            name: name.into(),
            dimensions,
            config,
            centroids: RwLock::new(Vec::new()),
            trained: RwLock::new(false),
            flat_vectors: RwLock::new(HashMap::new()),
            rng: RwLock::new(StdRng::from_entropy()),
            vector_count: AtomicU64::new(0),
        }
    }

    /// Compute distance between two vectors
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        self.config.distance_fn.compute(a, b)
    }

    /// Train the index using k-means clustering
    pub fn train(&self, vectors: &[Vec<f32>]) -> Result<()> {
        if vectors.is_empty() {
            return Err(Error::Internal("Cannot train on empty dataset".into()));
        }

        let n_clusters = self.config.n_clusters.min(vectors.len());

        // Initialize centroids using k-means++ algorithm
        let mut centroids = self.kmeans_plusplus_init(vectors, n_clusters);

        // Run k-means iterations
        const MAX_ITERATIONS: usize = 100;
        const TOLERANCE: f32 = 1e-4;

        for _ in 0..MAX_ITERATIONS {
            // Assign vectors to nearest centroid
            let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); n_clusters];

            for (vec_idx, vec) in vectors.iter().enumerate() {
                let nearest = self.find_nearest_centroid(vec, &centroids);
                assignments[nearest].push(vec_idx);
            }

            // Update centroids
            let mut max_shift = 0.0f32;

            for (cluster_idx, assigned) in assignments.iter().enumerate() {
                if assigned.is_empty() {
                    continue;
                }

                // Compute new centroid as mean of assigned vectors
                let mut new_centroid = vec![0.0f32; self.dimensions];
                for &vec_idx in assigned {
                    for (d, v) in new_centroid.iter_mut().zip(&vectors[vec_idx]) {
                        *d += v;
                    }
                }
                for d in &mut new_centroid {
                    *d /= assigned.len() as f32;
                }

                // Track centroid shift
                let shift = self.distance(&centroids[cluster_idx], &new_centroid);
                max_shift = max_shift.max(shift);

                centroids[cluster_idx] = new_centroid;
            }

            // Check convergence
            if max_shift < TOLERANCE {
                break;
            }
        }

        // Store trained centroids
        let mut trained_centroids: Vec<Centroid> = centroids
            .into_iter()
            .map(Centroid::new)
            .collect();

        // Reassign all existing flat vectors to clusters
        let flat_vectors = self.flat_vectors.read();
        for (&id, vec) in flat_vectors.iter() {
            let nearest = self.find_nearest_centroid_idx(vec, &trained_centroids);
            trained_centroids[nearest].vectors.push((id, vec.clone()));
        }

        *self.centroids.write() = trained_centroids;
        *self.trained.write() = true;

        Ok(())
    }

    /// Initialize centroids using k-means++ algorithm
    fn kmeans_plusplus_init(&self, vectors: &[Vec<f32>], k: usize) -> Vec<Vec<f32>> {
        let mut rng = self.rng.write();
        let mut centroids = Vec::with_capacity(k);

        // Choose first centroid randomly
        let first_idx = rng.gen_range(0..vectors.len());
        centroids.push(vectors[first_idx].clone());

        // Choose remaining centroids with probability proportional to distance squared
        while centroids.len() < k {
            let mut distances: Vec<f32> = vectors
                .iter()
                .map(|v| {
                    centroids
                        .iter()
                        .map(|c| self.distance(v, c))
                        .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                        .unwrap_or(f32::MAX)
                })
                .map(|d| d * d) // Square the distances
                .collect();

            let total: f32 = distances.iter().sum();
            if total <= 0.0 {
                break;
            }

            // Normalize to probabilities
            for d in &mut distances {
                *d /= total;
            }

            // Sample according to probabilities
            let mut cumsum = 0.0;
            let r: f32 = rng.gen();
            for (idx, &prob) in distances.iter().enumerate() {
                cumsum += prob;
                if cumsum >= r {
                    centroids.push(vectors[idx].clone());
                    break;
                }
            }
        }

        centroids
    }

    /// Find the nearest centroid index
    fn find_nearest_centroid(&self, vector: &[f32], centroids: &[Vec<f32>]) -> usize {
        centroids
            .iter()
            .enumerate()
            .map(|(idx, c)| (idx, self.distance(vector, c)))
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .map(|(idx, _)| idx)
            .unwrap_or(0)
    }

    /// Find the nearest centroid index from Centroid structs
    fn find_nearest_centroid_idx(&self, vector: &[f32], centroids: &[Centroid]) -> usize {
        centroids
            .iter()
            .enumerate()
            .map(|(idx, c)| (idx, self.distance(vector, &c.vector)))
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .map(|(idx, _)| idx)
            .unwrap_or(0)
    }

    /// Find the n_probe nearest centroids
    fn find_nearest_centroids(
        &self,
        vector: &[f32],
        centroids: &[Centroid],
        n_probe: usize,
    ) -> Vec<usize> {
        let mut distances: Vec<(usize, f32)> = centroids
            .iter()
            .enumerate()
            .map(|(idx, c)| (idx, self.distance(vector, &c.vector)))
            .collect();

        distances.sort_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        distances.into_iter().take(n_probe).map(|(idx, _)| idx).collect()
    }

    /// Insert a vector (internal)
    fn insert_internal(&self, id: u64, vector: Vec<f32>) -> Result<()> {
        if vector.len() != self.dimensions {
            return Err(Error::Internal(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )));
        }

        // Always store in flat index
        self.flat_vectors.write().insert(id, vector.clone());

        // If trained, also add to appropriate cluster
        if *self.trained.read() {
            let mut centroids = self.centroids.write();
            let nearest = self.find_nearest_centroid_idx(&vector, &centroids);
            centroids[nearest].vectors.push((id, vector));
        }

        self.vector_count.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    /// Search using the index
    fn search_internal(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&dyn Fn(u64) -> bool>,
    ) -> Result<Vec<SearchResult>> {
        if query.len() != self.dimensions {
            return Err(Error::Internal(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimensions,
                query.len()
            )));
        }

        let trained = *self.trained.read();

        if !trained {
            // Fall back to flat (brute-force) search
            return self.flat_search(query, k, filter);
        }

        let centroids = self.centroids.read();

        // Find nearest centroids to probe
        let n_probe = self.config.n_probe.min(centroids.len());
        let probe_clusters = self.find_nearest_centroids(query, &centroids, n_probe);

        // Search within probed clusters
        let mut candidates: Vec<Candidate> = Vec::new();

        for &cluster_idx in &probe_clusters {
            for (id, vec) in &centroids[cluster_idx].vectors {
                if filter.map_or(true, |f| f(*id)) {
                    let dist = self.distance(query, vec);
                    candidates.push(Candidate { id: *id, distance: dist });
                }
            }
        }

        // Sort and return top k
        candidates.sort();
        let results = candidates
            .into_iter()
            .take(k)
            .map(|c| SearchResult {
                id: c.id,
                distance: c.distance,
            })
            .collect();

        Ok(results)
    }

    /// Flat (brute-force) search
    fn flat_search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&dyn Fn(u64) -> bool>,
    ) -> Result<Vec<SearchResult>> {
        let flat_vectors = self.flat_vectors.read();

        let mut candidates: Vec<Candidate> = flat_vectors
            .iter()
            .filter(|(id, _)| filter.map_or(true, |f| f(**id)))
            .map(|(&id, vec)| Candidate {
                id,
                distance: self.distance(query, vec),
            })
            .collect();

        candidates.sort();

        let results = candidates
            .into_iter()
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
        self.flat_vectors.read().get(&id).cloned()
    }

    /// Check if a vector exists
    pub fn contains(&self, id: u64) -> bool {
        self.flat_vectors.read().contains_key(&id)
    }

    /// Get the number of vectors
    pub fn len(&self) -> usize {
        self.flat_vectors.read().len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.flat_vectors.read().is_empty()
    }

    /// Check if index is trained
    pub fn is_trained(&self) -> bool {
        *self.trained.read()
    }

    /// Auto-train if we have enough vectors
    fn maybe_auto_train(&self) {
        let trained = *self.trained.read();
        if trained {
            return;
        }

        let count = self.vector_count.load(AtomicOrdering::Relaxed) as usize;
        if count >= self.config.training_size {
            let flat_vectors = self.flat_vectors.read();
            let vectors: Vec<Vec<f32>> = flat_vectors.values().cloned().collect();
            drop(flat_vectors);

            // Train in background (ignore errors)
            let _ = self.train(&vectors);
        }
    }
}

#[async_trait]
impl VectorIndex for IvfIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    async fn insert(&self, id: u64, vector: &[f32]) -> Result<()> {
        self.insert_internal(id, vector.to_vec())?;
        self.maybe_auto_train();
        Ok(())
    }

    async fn delete(&self, id: u64) -> Result<bool> {
        // Remove from flat index
        let removed = self.flat_vectors.write().remove(&id).is_some();

        if removed {
            // Remove from clusters if trained
            if *self.trained.read() {
                let mut centroids = self.centroids.write();
                for centroid in centroids.iter_mut() {
                    centroid.vectors.retain(|(vid, _)| *vid != id);
                }
            }
            self.vector_count.fetch_sub(1, AtomicOrdering::Relaxed);
        }

        Ok(removed)
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.search_internal(query, k, None)
    }

    async fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<SearchResult>> {
        match filter {
            Some(f) => self.search_internal(query, k, Some(f.as_ref())),
            None => self.search_internal(query, k, None),
        }
    }

    async fn bulk_insert(&self, vectors: &[(u64, Vec<f32>)]) -> Result<()> {
        for (id, vector) in vectors {
            self.insert_internal(*id, vector.clone())?;
        }
        self.maybe_auto_train();
        Ok(())
    }

    fn stats(&self) -> IndexStats {
        let flat_vectors = self.flat_vectors.read();
        let vector_count = flat_vectors.len() as u64;
        let trained = *self.trained.read();
        let centroids = self.centroids.read();

        // Estimate memory
        let vector_mem = vector_count * self.dimensions as u64 * 4;
        let centroid_mem = if trained {
            centroids.len() as u64 * self.dimensions as u64 * 4
        } else {
            0
        };

        IndexStats {
            vector_count,
            dimensions: self.dimensions,
            memory_bytes: vector_mem + centroid_mem,
            index_type: format!(
                "IVF{} ({})",
                self.config.n_clusters,
                if trained { "trained" } else { "untrained" }
            ),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_index() -> IvfIndex {
        let config = IvfConfig {
            n_clusters: 4,
            n_probe: 2,
            distance_fn: DistanceFunction::Euclidean,
            training_size: 10,
        };
        IvfIndex::new("test", 3, config)
    }

    #[tokio::test]
    async fn test_insert_and_flat_search() {
        let index = create_test_index();

        // Insert vectors (not enough to trigger auto-training)
        index.insert(1, &[1.0, 0.0, 0.0]).await.unwrap();
        index.insert(2, &[0.0, 1.0, 0.0]).await.unwrap();
        index.insert(3, &[0.0, 0.0, 1.0]).await.unwrap();

        assert_eq!(index.len(), 3);
        assert!(!index.is_trained());

        // Search (flat search)
        let results = index.search(&[1.0, 0.0, 0.0], 2).await.unwrap();
        assert_eq!(results[0].id, 1);
        assert!(results[0].distance < 0.001);
    }

    #[tokio::test]
    async fn test_training() {
        let index = create_test_index();

        // Create training data
        let training_data: Vec<Vec<f32>> = (0..20)
            .map(|i| vec![i as f32, (i % 5) as f32, (i % 3) as f32])
            .collect();

        index.train(&training_data).unwrap();
        assert!(index.is_trained());

        // Insert vectors
        for (i, v) in training_data.iter().enumerate() {
            index.insert(i as u64, v).await.unwrap();
        }

        // Search should use IVF
        let results = index.search(&[10.0, 0.0, 1.0], 3).await.unwrap();
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_auto_training() {
        let config = IvfConfig {
            n_clusters: 3,
            n_probe: 2,
            distance_fn: DistanceFunction::Euclidean,
            training_size: 5, // Low threshold for testing
        };
        let index = IvfIndex::new("auto_test", 2, config);

        // Insert vectors to trigger auto-training
        for i in 0..10 {
            index.insert(i, &[i as f32, (i * 2) as f32]).await.unwrap();
        }

        // Should be trained now
        assert!(index.is_trained());
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
    async fn test_filtered_search() {
        let index = create_test_index();

        for i in 0..10 {
            index.insert(i, &[i as f32, 0.0, 0.0]).await.unwrap();
        }

        // Only even ids
        let filter: VectorFilter = Box::new(|id: u64| id % 2 == 0);
        let results = index.search_filtered(&[5.0, 0.0, 0.0], 3, Some(&filter)).await.unwrap();

        for r in &results {
            assert!(r.id % 2 == 0);
        }
    }

    #[test]
    fn test_stats() {
        let index = create_test_index();
        let stats = index.stats();
        assert_eq!(stats.vector_count, 0);
        assert_eq!(stats.dimensions, 3);
        assert!(stats.index_type.contains("IVF"));
    }
}
