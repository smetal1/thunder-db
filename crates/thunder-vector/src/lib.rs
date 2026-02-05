//! # Thunder Vector
//!
//! Vector indexing for ThunderDB providing:
//! - HNSW (Hierarchical Navigable Small World) index
//! - IVF (Inverted File) index
//! - Distance functions (cosine, euclidean, inner product)
//! - Approximate and exact nearest neighbor search

pub mod hnsw;
pub mod ivf;
pub mod distance;
pub mod quantization;

use async_trait::async_trait;
use thunder_common::prelude::*;

/// Filter function for vector search
pub type VectorFilter = Box<dyn Fn(u64) -> bool + Send + Sync>;

/// Vector index trait
#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Get index name
    fn name(&self) -> &str;

    /// Get vector dimensions
    fn dimensions(&self) -> usize;

    /// Insert a vector
    async fn insert(&self, id: u64, vector: &[f32]) -> Result<()>;

    /// Delete a vector
    async fn delete(&self, id: u64) -> Result<bool>;

    /// Search for k nearest neighbors (without filter)
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>>;

    /// Search for k nearest neighbors with optional filter
    async fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&VectorFilter>,
    ) -> Result<Vec<SearchResult>> {
        // Default implementation: call unfiltered search
        // Implementations can override for more efficient filtered search
        let results = self.search(query, k * 2).await?; // Over-fetch to account for filtering
        let filtered: Vec<_> = results
            .into_iter()
            .filter(|r| filter.as_ref().map_or(true, |f| f(r.id)))
            .take(k)
            .collect();
        Ok(filtered)
    }

    /// Bulk insert vectors
    async fn bulk_insert(&self, vectors: &[(u64, Vec<f32>)]) -> Result<()>;

    /// Get index statistics
    fn stats(&self) -> IndexStats;
}

/// Search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: u64,
    pub distance: f32,
}

/// Index statistics
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    pub vector_count: u64,
    pub dimensions: usize,
    pub memory_bytes: u64,
    pub index_type: String,
}

/// Distance function types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceFunction {
    #[default]
    Euclidean,
    Cosine,
    InnerProduct,
    Manhattan,
}

impl DistanceFunction {
    /// Compute distance between two vectors
    pub fn compute(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

        match self {
            DistanceFunction::Euclidean => {
                a.iter()
                    .zip(b)
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
            DistanceFunction::Cosine => {
                let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
                1.0 - (dot / (norm_a * norm_b + f32::EPSILON))
            }
            DistanceFunction::InnerProduct => {
                // Negative because we want to maximize inner product
                -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>()
            }
            DistanceFunction::Manhattan => {
                a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum()
            }
        }
    }
}

/// HNSW index configuration
#[derive(Debug, Clone)]
pub struct HnswConfig {
    /// Maximum number of connections per element (M)
    pub m: usize,
    /// Size of dynamic candidate list during construction (ef_construction)
    pub ef_construction: usize,
    /// Distance function
    pub distance_fn: DistanceFunction,
    /// Random seed for reproducibility
    pub seed: Option<u64>,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
            distance_fn: DistanceFunction::Cosine,
            seed: None,
        }
    }
}

/// IVF index configuration
#[derive(Debug, Clone)]
pub struct IvfConfig {
    /// Number of clusters (nlist)
    pub n_clusters: usize,
    /// Number of clusters to probe during search (nprobe)
    pub n_probe: usize,
    /// Distance function
    pub distance_fn: DistanceFunction,
    /// Training sample size
    pub training_size: usize,
}

impl Default for IvfConfig {
    fn default() -> Self {
        Self {
            n_clusters: 100,
            n_probe: 10,
            distance_fn: DistanceFunction::Euclidean,
            training_size: 10000,
        }
    }
}

/// Normalize a vector to unit length
pub fn normalize(vector: &mut [f32]) {
    let norm: f32 = vector.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
    if norm > f32::EPSILON {
        vector.iter_mut().for_each(|x| *x /= norm);
    }
}

/// Compute dot product
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Compute L2 norm
pub fn l2_norm(vector: &[f32]) -> f32 {
    vector.iter().map(|x| x.powi(2)).sum::<f32>().sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        let dist = DistanceFunction::Euclidean.compute(&a, &b);
        assert!((dist - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let dist = DistanceFunction::Cosine.compute(&a, &b);
        assert!((dist - 1.0).abs() < 1e-6); // Orthogonal vectors = distance 1
    }

    #[test]
    fn test_inner_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let dist = DistanceFunction::InnerProduct.compute(&a, &b);
        assert!((dist - (-32.0)).abs() < 1e-6); // 1*4 + 2*5 + 3*6 = 32
    }

    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0];
        normalize(&mut v);
        assert!((v[0] - 0.6).abs() < 1e-6);
        assert!((v[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_l2_norm() {
        let v = vec![3.0, 4.0];
        assert!((l2_norm(&v) - 5.0).abs() < 1e-6);
    }
}
