//! Transaction manager benchmarks

use criterion::{criterion_group, criterion_main, Criterion};

fn transaction_throughput(c: &mut Criterion) {
    c.bench_function("txn_begin_commit", |b| {
        b.iter(|| {
            // TODO: Benchmark transaction begin/commit
        })
    });
}

criterion_group!(benches, transaction_throughput);
criterion_main!(benches);
