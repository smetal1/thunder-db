//! Vector index benchmarks

use criterion::{criterion_group, criterion_main, Criterion};

fn vector_search(c: &mut Criterion) {
    c.bench_function("hnsw_search", |b| {
        b.iter(|| {
            // TODO: Benchmark HNSW search
        })
    });
}

fn distance_functions(c: &mut Criterion) {
    c.bench_function("cosine_distance", |b| {
        let a: Vec<f32> = (0..128).map(|i| i as f32 / 128.0).collect();
        let b: Vec<f32> = (0..128).map(|i| (127 - i) as f32 / 128.0).collect();

        b.iter(|| {
            thunder_vector::DistanceFunction::Cosine.compute(&a, &b)
        })
    });
}

criterion_group!(benches, vector_search, distance_functions);
criterion_main!(benches);
