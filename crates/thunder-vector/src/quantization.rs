//! Vector Quantization
//!
//! Provides compression techniques for reducing memory usage of vector indexes:
//! - Scalar Quantization (SQ): Maps floats to uint8
//! - Product Quantization (PQ): Divides vectors into subspaces
//! - Binary Quantization (BQ): Single-bit per dimension

use std::cmp::Ordering;

use parking_lot::RwLock;
use rand::prelude::*;

use thunder_common::prelude::*;
use crate::DistanceFunction;

// ============================================================================
// Scalar Quantization
// ============================================================================

/// Scalar Quantization - maps f32 values to u8 (256 levels)
#[derive(Debug, Clone)]
pub struct ScalarQuantizer {
    /// Minimum values per dimension
    min_vals: Vec<f32>,
    /// Maximum values per dimension
    max_vals: Vec<f32>,
    /// Scale factors per dimension
    scales: Vec<f32>,
    /// Number of dimensions
    dimensions: usize,
    /// Whether the quantizer is trained
    trained: bool,
}

impl ScalarQuantizer {
    /// Create a new scalar quantizer
    pub fn new(dimensions: usize) -> Self {
        Self {
            min_vals: vec![f32::MAX; dimensions],
            max_vals: vec![f32::MIN; dimensions],
            scales: vec![1.0; dimensions],
            dimensions,
            trained: false,
        }
    }

    /// Train the quantizer on sample vectors
    pub fn train(&mut self, vectors: &[&[f32]]) -> Result<()> {
        if vectors.is_empty() {
            return Err(Error::Internal("Cannot train on empty dataset".into()));
        }

        // Find min/max for each dimension
        for vec in vectors {
            if vec.len() != self.dimensions {
                return Err(Error::Internal("Dimension mismatch".into()));
            }
            for (d, &v) in vec.iter().enumerate() {
                self.min_vals[d] = self.min_vals[d].min(v);
                self.max_vals[d] = self.max_vals[d].max(v);
            }
        }

        // Compute scales
        for d in 0..self.dimensions {
            let range = self.max_vals[d] - self.min_vals[d];
            self.scales[d] = if range > f32::EPSILON {
                255.0 / range
            } else {
                1.0
            };
        }

        self.trained = true;
        Ok(())
    }

    /// Encode a vector to quantized form
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        debug_assert_eq!(vector.len(), self.dimensions);

        vector
            .iter()
            .enumerate()
            .map(|(d, &v)| {
                let normalized = (v - self.min_vals[d]) * self.scales[d];
                normalized.clamp(0.0, 255.0) as u8
            })
            .collect()
    }

    /// Decode a quantized vector back to f32
    pub fn decode(&self, quantized: &[u8]) -> Vec<f32> {
        debug_assert_eq!(quantized.len(), self.dimensions);

        quantized
            .iter()
            .enumerate()
            .map(|(d, &q)| {
                self.min_vals[d] + (q as f32) / self.scales[d]
            })
            .collect()
    }

    /// Compute approximate distance between quantized vectors
    pub fn distance(&self, a: &[u8], b: &[u8], distance_fn: DistanceFunction) -> f32 {
        // Decode and compute exact distance
        let a_decoded = self.decode(a);
        let b_decoded = self.decode(b);
        distance_fn.compute(&a_decoded, &b_decoded)
    }

    /// Compute approximate distance between a raw query and quantized vector
    pub fn asymmetric_distance(
        &self,
        query: &[f32],
        quantized: &[u8],
        distance_fn: DistanceFunction,
    ) -> f32 {
        let decoded = self.decode(quantized);
        distance_fn.compute(query, &decoded)
    }

    /// Memory per vector in bytes
    pub fn bytes_per_vector(&self) -> usize {
        self.dimensions
    }

    /// Compression ratio (original / compressed)
    pub fn compression_ratio(&self) -> f32 {
        (self.dimensions * 4) as f32 / self.dimensions as f32 // f32 vs u8
    }

    /// Check if trained
    pub fn is_trained(&self) -> bool {
        self.trained
    }
}

// ============================================================================
// Product Quantization
// ============================================================================

/// Product Quantization configuration
#[derive(Debug, Clone)]
pub struct PqConfig {
    /// Number of subspaces
    pub n_subspaces: usize,
    /// Number of centroids per subspace (typically 256 for u8 codes)
    pub n_centroids: usize,
    /// Number of k-means iterations
    pub n_iterations: usize,
}

impl Default for PqConfig {
    fn default() -> Self {
        Self {
            n_subspaces: 8,
            n_centroids: 256,
            n_iterations: 25,
        }
    }
}

/// Product Quantization - divides vectors into subspaces
#[derive(Debug)]
pub struct ProductQuantizer {
    /// Configuration
    config: PqConfig,
    /// Dimensions per subspace
    dims_per_subspace: usize,
    /// Total dimensions
    dimensions: usize,
    /// Centroids for each subspace [subspace][centroid][dim]
    centroids: Vec<Vec<Vec<f32>>>,
    /// Distance lookup tables (precomputed for search)
    #[allow(dead_code)]
    distance_tables: RwLock<Option<Vec<Vec<f32>>>>,
    /// Whether trained
    trained: bool,
    /// Distance function
    distance_fn: DistanceFunction,
}

impl ProductQuantizer {
    /// Create a new product quantizer
    pub fn new(dimensions: usize, config: PqConfig, distance_fn: DistanceFunction) -> Result<Self> {
        if dimensions % config.n_subspaces != 0 {
            return Err(Error::Internal(format!(
                "Dimensions {} must be divisible by n_subspaces {}",
                dimensions, config.n_subspaces
            )));
        }

        let dims_per_subspace = dimensions / config.n_subspaces;

        Ok(Self {
            config,
            dims_per_subspace,
            dimensions,
            centroids: Vec::new(),
            distance_tables: RwLock::new(None),
            trained: false,
            distance_fn,
        })
    }

    /// Train the quantizer using k-means on each subspace
    pub fn train(&mut self, vectors: &[&[f32]]) -> Result<()> {
        if vectors.is_empty() {
            return Err(Error::Internal("Cannot train on empty dataset".into()));
        }

        let mut rng = StdRng::from_entropy();
        self.centroids = Vec::with_capacity(self.config.n_subspaces);

        for m in 0..self.config.n_subspaces {
            let start = m * self.dims_per_subspace;
            let end = start + self.dims_per_subspace;

            // Extract subvectors for this subspace
            let subvectors: Vec<Vec<f32>> = vectors
                .iter()
                .map(|v| v[start..end].to_vec())
                .collect();

            // Run k-means
            let centroids = self.kmeans(
                &subvectors,
                self.config.n_centroids,
                self.config.n_iterations,
                &mut rng,
            );

            self.centroids.push(centroids);
        }

        self.trained = true;
        Ok(())
    }

    /// K-means clustering for a subspace
    fn kmeans(
        &self,
        vectors: &[Vec<f32>],
        k: usize,
        iterations: usize,
        rng: &mut StdRng,
    ) -> Vec<Vec<f32>> {
        let n = vectors.len();
        let d = vectors[0].len();
        let k = k.min(n);

        // Initialize centroids randomly
        let mut indices: Vec<usize> = (0..n).collect();
        indices.shuffle(rng);
        let mut centroids: Vec<Vec<f32>> = indices
            .iter()
            .take(k)
            .map(|&i| vectors[i].clone())
            .collect();

        // K-means iterations
        for _ in 0..iterations {
            // Assign vectors to nearest centroid
            let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); k];
            for (vec_idx, vec) in vectors.iter().enumerate() {
                let nearest = self.find_nearest_centroid(vec, &centroids);
                assignments[nearest].push(vec_idx);
            }

            // Update centroids
            for (c_idx, assigned) in assignments.iter().enumerate() {
                if assigned.is_empty() {
                    continue;
                }

                let mut new_centroid = vec![0.0f32; d];
                for &vec_idx in assigned {
                    for (i, &v) in vectors[vec_idx].iter().enumerate() {
                        new_centroid[i] += v;
                    }
                }
                for v in &mut new_centroid {
                    *v /= assigned.len() as f32;
                }
                centroids[c_idx] = new_centroid;
            }
        }

        centroids
    }

    fn find_nearest_centroid(&self, vector: &[f32], centroids: &[Vec<f32>]) -> usize {
        centroids
            .iter()
            .enumerate()
            .map(|(idx, c)| (idx, self.subspace_distance(vector, c)))
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .map(|(idx, _)| idx)
            .unwrap_or(0)
    }

    fn subspace_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.distance_fn {
            DistanceFunction::Euclidean => {
                a.iter()
                    .zip(b)
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
            DistanceFunction::InnerProduct => {
                -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>()
            }
            _ => {
                a.iter()
                    .zip(b)
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
        }
    }

    /// Encode a vector to PQ codes
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        debug_assert_eq!(vector.len(), self.dimensions);

        (0..self.config.n_subspaces)
            .map(|m| {
                let start = m * self.dims_per_subspace;
                let end = start + self.dims_per_subspace;
                let subvector = &vector[start..end];
                self.find_nearest_centroid(subvector, &self.centroids[m]) as u8
            })
            .collect()
    }

    /// Decode PQ codes back to approximate vector
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        debug_assert_eq!(codes.len(), self.config.n_subspaces);

        let mut result = Vec::with_capacity(self.dimensions);
        for (m, &code) in codes.iter().enumerate() {
            result.extend_from_slice(&self.centroids[m][code as usize]);
        }
        result
    }

    /// Precompute distance table for a query
    pub fn compute_distance_table(&self, query: &[f32]) -> Vec<Vec<f32>> {
        (0..self.config.n_subspaces)
            .map(|m| {
                let start = m * self.dims_per_subspace;
                let end = start + self.dims_per_subspace;
                let subquery = &query[start..end];

                self.centroids[m]
                    .iter()
                    .map(|c| self.subspace_distance(subquery, c))
                    .collect()
            })
            .collect()
    }

    /// Compute distance using precomputed table (ADC - Asymmetric Distance Computation)
    pub fn distance_with_table(&self, codes: &[u8], table: &[Vec<f32>]) -> f32 {
        codes
            .iter()
            .enumerate()
            .map(|(m, &code)| table[m][code as usize])
            .sum::<f32>()
    }

    /// Compute distance without precomputed table
    pub fn distance(&self, query: &[f32], codes: &[u8]) -> f32 {
        let table = self.compute_distance_table(query);
        self.distance_with_table(codes, &table)
    }

    /// Memory per vector in bytes
    pub fn bytes_per_vector(&self) -> usize {
        self.config.n_subspaces
    }

    /// Compression ratio
    pub fn compression_ratio(&self) -> f32 {
        (self.dimensions * 4) as f32 / self.config.n_subspaces as f32
    }

    /// Check if trained
    pub fn is_trained(&self) -> bool {
        self.trained
    }
}

// ============================================================================
// Binary Quantization
// ============================================================================

/// Binary Quantization - single bit per dimension
#[derive(Debug, Clone)]
pub struct BinaryQuantizer {
    /// Thresholds per dimension (values above threshold = 1)
    thresholds: Vec<f32>,
    /// Number of dimensions
    dimensions: usize,
    /// Whether trained
    trained: bool,
}

impl BinaryQuantizer {
    /// Create a new binary quantizer
    pub fn new(dimensions: usize) -> Self {
        Self {
            thresholds: vec![0.0; dimensions],
            dimensions,
            trained: false,
        }
    }

    /// Train using median values as thresholds
    pub fn train(&mut self, vectors: &[&[f32]]) -> Result<()> {
        if vectors.is_empty() {
            return Err(Error::Internal("Cannot train on empty dataset".into()));
        }

        // Compute median for each dimension
        for d in 0..self.dimensions {
            let mut values: Vec<f32> = vectors.iter().map(|v| v[d]).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            self.thresholds[d] = values[values.len() / 2];
        }

        self.trained = true;
        Ok(())
    }

    /// Encode a vector to binary form (packed in u64s)
    pub fn encode(&self, vector: &[f32]) -> Vec<u64> {
        debug_assert_eq!(vector.len(), self.dimensions);

        let n_words = (self.dimensions + 63) / 64;
        let mut result = vec![0u64; n_words];

        for (d, &v) in vector.iter().enumerate() {
            if v > self.thresholds[d] {
                let word_idx = d / 64;
                let bit_idx = d % 64;
                result[word_idx] |= 1u64 << bit_idx;
            }
        }

        result
    }

    /// Compute Hamming distance between two binary vectors
    pub fn distance(&self, a: &[u64], b: &[u64]) -> u32 {
        a.iter()
            .zip(b)
            .map(|(x, y)| (x ^ y).count_ones())
            .sum()
    }

    /// Compute asymmetric distance (float query vs binary vector)
    pub fn asymmetric_distance(&self, query: &[f32], binary: &[u64]) -> f32 {
        // Count matching bits weighted by query values
        let mut distance = 0.0f32;

        for (d, &v) in query.iter().enumerate() {
            let word_idx = d / 64;
            let bit_idx = d % 64;
            let bit = (binary[word_idx] >> bit_idx) & 1;

            // Penalty for mismatch
            let binary_val = if bit == 1 { 1.0 } else { -1.0 };
            let diff = v - binary_val;
            distance += diff * diff;
        }

        distance.sqrt()
    }

    /// Memory per vector in bytes
    pub fn bytes_per_vector(&self) -> usize {
        (self.dimensions + 63) / 64 * 8
    }

    /// Compression ratio
    pub fn compression_ratio(&self) -> f32 {
        (self.dimensions * 32) as f32 / self.dimensions as f32 // 32:1 compression
    }

    /// Check if trained
    pub fn is_trained(&self) -> bool {
        self.trained
    }
}

// ============================================================================
// Quantized Vector Store
// ============================================================================

/// Storage for quantized vectors with different compression methods
pub enum QuantizedStore {
    /// Scalar quantization (u8 per dimension)
    Scalar {
        quantizer: ScalarQuantizer,
        vectors: Vec<(u64, Vec<u8>)>,
    },
    /// Product quantization (subspace codes)
    Product {
        quantizer: ProductQuantizer,
        vectors: Vec<(u64, Vec<u8>)>,
    },
    /// Binary quantization (1 bit per dimension)
    Binary {
        quantizer: BinaryQuantizer,
        vectors: Vec<(u64, Vec<u64>)>,
    },
}

impl QuantizedStore {
    /// Create a scalar quantized store
    pub fn scalar(dimensions: usize) -> Self {
        Self::Scalar {
            quantizer: ScalarQuantizer::new(dimensions),
            vectors: Vec::new(),
        }
    }

    /// Create a product quantized store
    pub fn product(dimensions: usize, config: PqConfig) -> Result<Self> {
        Ok(Self::Product {
            quantizer: ProductQuantizer::new(dimensions, config, DistanceFunction::Euclidean)?,
            vectors: Vec::new(),
        })
    }

    /// Create a binary quantized store
    pub fn binary(dimensions: usize) -> Self {
        Self::Binary {
            quantizer: BinaryQuantizer::new(dimensions),
            vectors: Vec::new(),
        }
    }

    /// Get memory usage in bytes
    pub fn memory_bytes(&self) -> usize {
        match self {
            Self::Scalar { vectors, quantizer } => {
                vectors.len() * quantizer.bytes_per_vector()
            }
            Self::Product { vectors, quantizer } => {
                vectors.len() * quantizer.bytes_per_vector()
            }
            Self::Binary { vectors, quantizer } => {
                vectors.len() * quantizer.bytes_per_vector()
            }
        }
    }

    /// Get number of vectors
    pub fn len(&self) -> usize {
        match self {
            Self::Scalar { vectors, .. } => vectors.len(),
            Self::Product { vectors, .. } => vectors.len(),
            Self::Binary { vectors, .. } => vectors.len(),
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_quantizer() {
        let mut sq = ScalarQuantizer::new(3);

        let vectors: Vec<&[f32]> = vec![
            &[0.0, 0.0, 0.0],
            &[1.0, 1.0, 1.0],
            &[0.5, 0.5, 0.5],
        ];

        sq.train(&vectors).unwrap();
        assert!(sq.is_trained());

        let encoded = sq.encode(&[0.5, 0.5, 0.5]);
        assert_eq!(encoded.len(), 3);

        let decoded = sq.decode(&encoded);
        assert_eq!(decoded.len(), 3);

        // Check approximate reconstruction
        for (i, &v) in decoded.iter().enumerate() {
            assert!((v - 0.5).abs() < 0.1);
        }
    }

    #[test]
    fn test_product_quantizer() {
        let config = PqConfig {
            n_subspaces: 2,
            n_centroids: 4,
            n_iterations: 10,
        };
        let mut pq = ProductQuantizer::new(4, config, DistanceFunction::Euclidean).unwrap();

        let vectors: Vec<&[f32]> = vec![
            &[0.0, 0.0, 0.0, 0.0],
            &[1.0, 1.0, 1.0, 1.0],
            &[0.5, 0.5, 0.5, 0.5],
            &[0.2, 0.8, 0.3, 0.7],
        ];

        pq.train(&vectors).unwrap();
        assert!(pq.is_trained());

        let encoded = pq.encode(&[0.5, 0.5, 0.5, 0.5]);
        assert_eq!(encoded.len(), 2);

        let decoded = pq.decode(&encoded);
        assert_eq!(decoded.len(), 4);
    }

    #[test]
    fn test_binary_quantizer() {
        let mut bq = BinaryQuantizer::new(8);

        let vectors: Vec<&[f32]> = vec![
            &[0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0],
            &[1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0],
            &[0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
        ];

        bq.train(&vectors).unwrap();
        assert!(bq.is_trained());

        let a = bq.encode(&[0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0]);
        let b = bq.encode(&[1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0]);

        let dist = bq.distance(&a, &b);
        assert!(dist > 0);
    }

    #[test]
    fn test_compression_ratios() {
        let sq = ScalarQuantizer::new(128);
        assert_eq!(sq.compression_ratio(), 4.0); // 4 bytes -> 1 byte

        let config = PqConfig {
            n_subspaces: 16,
            n_centroids: 256,
            n_iterations: 10,
        };
        let pq = ProductQuantizer::new(128, config, DistanceFunction::Euclidean).unwrap();
        assert_eq!(pq.compression_ratio(), 32.0); // 512 bytes -> 16 bytes

        let bq = BinaryQuantizer::new(128);
        assert_eq!(bq.compression_ratio(), 32.0); // 512 bytes -> 16 bytes
    }
}
