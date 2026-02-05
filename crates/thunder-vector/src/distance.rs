//! Distance Functions for Vector Similarity
//!
//! Provides optimized implementations of common distance metrics:
//! - Euclidean (L2)
//! - Cosine similarity
//! - Inner product (dot product)
//! - Manhattan (L1)
//! - Hamming (for binary vectors)

#[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
use std::arch::x86_64::*;

use crate::DistanceFunction;

// ============================================================================
// SIMD-Optimized Distance Functions
// ============================================================================

/// Compute Euclidean distance between two vectors
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(target_feature = "avx2")]
    {
        if a.len() >= 8 {
            return euclidean_avx2(a, b);
        }
    }

    #[cfg(target_feature = "sse")]
    {
        if a.len() >= 4 {
            return euclidean_sse(a, b);
        }
    }

    euclidean_scalar(a, b)
}

/// Scalar Euclidean distance
#[inline]
fn euclidean_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// SSE-optimized Euclidean distance
#[cfg(target_feature = "sse")]
#[inline]
fn euclidean_sse(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let mut sum = _mm_setzero_ps();
        let chunks = a.len() / 4;

        for i in 0..chunks {
            let va = _mm_loadu_ps(a.as_ptr().add(i * 4));
            let vb = _mm_loadu_ps(b.as_ptr().add(i * 4));
            let diff = _mm_sub_ps(va, vb);
            sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
        }

        // Horizontal sum
        let mut result = [0.0f32; 4];
        _mm_storeu_ps(result.as_mut_ptr(), sum);
        let mut total: f32 = result.iter().sum();

        // Handle remainder
        for i in (chunks * 4)..a.len() {
            let diff = a[i] - b[i];
            total += diff * diff;
        }

        total.sqrt()
    }
}

/// AVX2-optimized Euclidean distance
#[cfg(target_feature = "avx2")]
#[inline]
fn euclidean_avx2(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let mut sum = _mm256_setzero_ps();
        let chunks = a.len() / 8;

        for i in 0..chunks {
            let va = _mm256_loadu_ps(a.as_ptr().add(i * 8));
            let vb = _mm256_loadu_ps(b.as_ptr().add(i * 8));
            let diff = _mm256_sub_ps(va, vb);
            sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
        }

        // Horizontal sum
        let mut result = [0.0f32; 8];
        _mm256_storeu_ps(result.as_mut_ptr(), sum);
        let mut total: f32 = result.iter().sum();

        // Handle remainder
        for i in (chunks * 8)..a.len() {
            let diff = a[i] - b[i];
            total += diff * diff;
        }

        total.sqrt()
    }
}

/// Compute squared Euclidean distance (faster, no sqrt)
#[inline]
pub fn euclidean_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y).powi(2))
        .sum()
}

/// Compute cosine distance (1 - cosine similarity)
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    let dot = dot_product(a, b);
    let norm_a = l2_norm(a);
    let norm_b = l2_norm(b);

    let similarity = dot / (norm_a * norm_b + f32::EPSILON);
    1.0 - similarity.clamp(-1.0, 1.0)
}

/// Compute cosine similarity
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    let dot = dot_product(a, b);
    let norm_a = l2_norm(a);
    let norm_b = l2_norm(b);

    (dot / (norm_a * norm_b + f32::EPSILON)).clamp(-1.0, 1.0)
}

/// Compute inner product (dot product)
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(target_feature = "avx2")]
    {
        if a.len() >= 8 {
            return dot_product_avx2(a, b);
        }
    }

    #[cfg(target_feature = "sse")]
    {
        if a.len() >= 4 {
            return dot_product_sse(a, b);
        }
    }

    dot_product_scalar(a, b)
}

/// Scalar dot product
#[inline]
fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// SSE-optimized dot product
#[cfg(target_feature = "sse")]
#[inline]
fn dot_product_sse(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let mut sum = _mm_setzero_ps();
        let chunks = a.len() / 4;

        for i in 0..chunks {
            let va = _mm_loadu_ps(a.as_ptr().add(i * 4));
            let vb = _mm_loadu_ps(b.as_ptr().add(i * 4));
            sum = _mm_add_ps(sum, _mm_mul_ps(va, vb));
        }

        let mut result = [0.0f32; 4];
        _mm_storeu_ps(result.as_mut_ptr(), sum);
        let mut total: f32 = result.iter().sum();

        for i in (chunks * 4)..a.len() {
            total += a[i] * b[i];
        }

        total
    }
}

/// AVX2-optimized dot product
#[cfg(target_feature = "avx2")]
#[inline]
fn dot_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let mut sum = _mm256_setzero_ps();
        let chunks = a.len() / 8;

        for i in 0..chunks {
            let va = _mm256_loadu_ps(a.as_ptr().add(i * 8));
            let vb = _mm256_loadu_ps(b.as_ptr().add(i * 8));
            sum = _mm256_add_ps(sum, _mm256_mul_ps(va, vb));
        }

        let mut result = [0.0f32; 8];
        _mm256_storeu_ps(result.as_mut_ptr(), sum);
        let mut total: f32 = result.iter().sum();

        for i in (chunks * 8)..a.len() {
            total += a[i] * b[i];
        }

        total
    }
}

/// Compute inner product distance (negative for max similarity search)
#[inline]
pub fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    -dot_product(a, b)
}

/// Compute L2 norm
#[inline]
pub fn l2_norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x.powi(2)).sum::<f32>().sqrt()
}

/// Compute L1 norm (Manhattan)
#[inline]
pub fn l1_norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x.abs()).sum()
}

/// Compute Manhattan distance (L1)
#[inline]
pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum()
}

/// Compute Hamming distance for binary vectors (packed as u64)
#[inline]
pub fn hamming_distance(a: &[u64], b: &[u64]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b)
        .map(|(x, y)| (x ^ y).count_ones())
        .sum()
}

/// Compute Hamming distance for byte vectors
#[inline]
pub fn hamming_distance_bytes(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b)
        .map(|(x, y)| (x ^ y).count_ones())
        .sum()
}

// ============================================================================
// Batch Distance Computation
// ============================================================================

/// Compute distances from a query to multiple vectors
pub fn batch_distances(
    query: &[f32],
    vectors: &[&[f32]],
    distance_fn: DistanceFunction,
) -> Vec<f32> {
    vectors
        .iter()
        .map(|v| distance_fn.compute(query, v))
        .collect()
}

/// Find k nearest neighbors from a set of vectors
pub fn find_k_nearest(
    query: &[f32],
    vectors: &[(u64, &[f32])],
    k: usize,
    distance_fn: DistanceFunction,
) -> Vec<(u64, f32)> {
    let mut distances: Vec<(u64, f32)> = vectors
        .iter()
        .map(|(id, v)| (*id, distance_fn.compute(query, v)))
        .collect();

    // Partial sort for top k
    if k < distances.len() {
        distances.select_nth_unstable_by(k, |a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        distances.truncate(k);
    }

    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    distances
}

// ============================================================================
// Vector Normalization
// ============================================================================

/// Normalize a vector to unit length (in-place)
#[inline]
pub fn normalize_inplace(v: &mut [f32]) {
    let norm = l2_norm(v);
    if norm > f32::EPSILON {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

/// Normalize a vector to unit length (returns new vector)
#[inline]
pub fn normalize(v: &[f32]) -> Vec<f32> {
    let norm = l2_norm(v);
    if norm > f32::EPSILON {
        v.iter().map(|x| x / norm).collect()
    } else {
        v.to_vec()
    }
}

/// Batch normalize vectors
pub fn batch_normalize(vectors: &mut [Vec<f32>]) {
    for v in vectors.iter_mut() {
        normalize_inplace(v);
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        let dist = euclidean_distance(&a, &b);
        assert!((dist - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_distance_squared() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        let dist = euclidean_distance_squared(&a, &b);
        assert!((dist - 25.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let dist = cosine_distance(&a, &b);
        assert!((dist - 1.0).abs() < 1e-6); // Orthogonal

        let c = vec![1.0, 0.0];
        let d = vec![1.0, 0.0];
        let dist2 = cosine_distance(&c, &d);
        assert!(dist2.abs() < 1e-6); // Same direction
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim - 1.0).abs() < 1e-6);

        let c = vec![1.0, 0.0];
        let d = vec![-1.0, 0.0];
        let sim2 = cosine_similarity(&c, &d);
        assert!((sim2 - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let dot = dot_product(&a, &b);
        assert!((dot - 32.0).abs() < 1e-6);
    }

    #[test]
    fn test_manhattan_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        let dist = manhattan_distance(&a, &b);
        assert!((dist - 7.0).abs() < 1e-6);
    }

    #[test]
    fn test_hamming_distance() {
        let a = vec![0b1010_1010_u64];
        let b = vec![0b0101_0101_u64];
        let dist = hamming_distance(&a, &b);
        assert_eq!(dist, 8);
    }

    #[test]
    fn test_l2_norm() {
        let v = vec![3.0, 4.0];
        let norm = l2_norm(&v);
        assert!((norm - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_normalize() {
        let v = vec![3.0, 4.0];
        let n = normalize(&v);
        assert!((n[0] - 0.6).abs() < 1e-6);
        assert!((n[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_find_k_nearest() {
        let query = vec![0.0, 0.0];
        let vectors: Vec<(u64, &[f32])> = vec![
            (1, &[1.0, 0.0][..]),
            (2, &[2.0, 0.0][..]),
            (3, &[3.0, 0.0][..]),
        ];

        let nearest = find_k_nearest(&query, &vectors, 2, DistanceFunction::Euclidean);
        assert_eq!(nearest.len(), 2);
        assert_eq!(nearest[0].0, 1);
        assert_eq!(nearest[1].0, 2);
    }
}
