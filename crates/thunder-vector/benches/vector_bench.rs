//! Vector index benchmarks
//!
//! Benchmarks HNSW index operations: insert, search, and distance functions
//! across different dimensionalities and dataset sizes.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use thunder_vector::{DistanceFunction, HnswConfig, VectorIndex};
use thunder_vector::hnsw::HnswIndex;

/// Generate random vectors for benchmarking
fn generate_vectors(count: usize, dimensions: usize) -> Vec<(u64, Vec<f32>)> {
    // Use deterministic pseudo-random generation for reproducibility
    let mut seed: u64 = 42;
    (0..count)
        .map(|i| {
            let vec: Vec<f32> = (0..dimensions)
                .map(|_| {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (seed >> 33) as f32 / (u32::MAX >> 1) as f32
                })
                .collect();
            (i as u64, vec)
        })
        .collect()
}

fn hnsw_search(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dimensions = 128;
    let dataset_size = 10_000;

    let config = HnswConfig {
        m: 16,
        ef_construction: 200,
        distance_fn: DistanceFunction::Cosine,
        seed: Some(42),
    };

    let index = HnswIndex::new("bench_index", dimensions, config);
    let vectors = generate_vectors(dataset_size, dimensions);

    // Pre-populate index
    rt.block_on(async {
        index.bulk_insert(&vectors).await.unwrap();
    });

    let query: Vec<f32> = (0..dimensions).map(|i| i as f32 / dimensions as f32).collect();

    let mut group = c.benchmark_group("hnsw_search");
    for k in [1, 10, 50, 100] {
        group.bench_with_input(BenchmarkId::new("top_k", k), &k, |b, &k| {
            b.to_async(&rt).iter(|| {
                let q = query.clone();
                let idx = &index;
                async move {
                    idx.search(&q, k).await.unwrap()
                }
            });
        });
    }
    group.finish();
}

fn hnsw_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dimensions = 128;

    let config = HnswConfig {
        m: 16,
        ef_construction: 200,
        distance_fn: DistanceFunction::Cosine,
        seed: Some(42),
    };

    c.bench_function("hnsw_insert_single", |b| {
        let index = HnswIndex::new("bench_insert", dimensions, config.clone());
        let mut id = 0u64;

        b.to_async(&rt).iter(|| {
            id += 1;
            let vec: Vec<f32> = (0..dimensions).map(|i| (id as f32 + i as f32) / dimensions as f32).collect();
            let idx = &index;
            async move {
                idx.insert(id, &vec).await.unwrap()
            }
        });
    });
}

fn distance_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance_functions");

    for dim in [32, 128, 512] {
        let a: Vec<f32> = (0..dim).map(|i| i as f32 / dim as f32).collect();
        let b_vec: Vec<f32> = (0..dim).map(|i| (dim - 1 - i) as f32 / dim as f32).collect();

        group.bench_with_input(BenchmarkId::new("cosine", dim), &dim, |b, _| {
            b.iter(|| DistanceFunction::Cosine.compute(&a, &b_vec))
        });

        group.bench_with_input(BenchmarkId::new("euclidean", dim), &dim, |b, _| {
            b.iter(|| DistanceFunction::Euclidean.compute(&a, &b_vec))
        });

        group.bench_with_input(BenchmarkId::new("inner_product", dim), &dim, |b, _| {
            b.iter(|| DistanceFunction::InnerProduct.compute(&a, &b_vec))
        });

        group.bench_with_input(BenchmarkId::new("manhattan", dim), &dim, |b, _| {
            b.iter(|| DistanceFunction::Manhattan.compute(&a, &b_vec))
        });
    }
    group.finish();
}

criterion_group!(benches, hnsw_search, hnsw_insert, distance_functions);
criterion_main!(benches);
