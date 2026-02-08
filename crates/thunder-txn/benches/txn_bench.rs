//! Transaction manager benchmarks
//!
//! Benchmarks MVCC transaction lifecycle: begin, read, write, commit/abort
//! across different isolation levels and conflict scenarios.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use thunder_txn::mvcc::{MvccTransactionManager, MvccConfig};
use thunder_common::types::{IsolationLevel, TableId, RowId, Row, Value};

fn transaction_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("transaction_throughput");

    // Benchmark begin + commit cycle at different isolation levels
    for (label, isolation) in [
        ("read_committed", IsolationLevel::ReadCommitted),
        ("repeatable_read", IsolationLevel::RepeatableRead),
        ("serializable", IsolationLevel::Serializable),
    ] {
        let config = MvccConfig {
            max_active_txns: 10000,
            detect_write_conflicts: true,
            default_isolation: isolation,
        };
        let mgr = MvccTransactionManager::new(config);

        group.bench_with_input(
            BenchmarkId::new("begin_commit", label),
            &label,
            |b, _| {
                b.to_async(&rt).iter(|| {
                    let mgr = &mgr;
                    async move {
                        let txn_id = mgr.begin_with_isolation(isolation).unwrap();
                        mgr.commit_txn(txn_id).await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn transaction_read_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let config = MvccConfig {
        max_active_txns: 10000,
        detect_write_conflicts: true,
        default_isolation: IsolationLevel::RepeatableRead,
    };
    let mgr = MvccTransactionManager::new(config);
    let table_id = TableId(1);

    let mut group = c.benchmark_group("transaction_rw");

    // Benchmark single write + commit
    group.bench_function("single_write_commit", |b| {
        let mut row_counter = 0u64;
        b.to_async(&rt).iter(|| {
            row_counter += 1;
            let mgr = &mgr;
            let row = Row::new(vec![Value::Int64(row_counter as i64), Value::String("bench".into())]);
            async move {
                let txn_id = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();
                mgr.write_row(txn_id, table_id, RowId(row_counter), row, true).await.unwrap();
                mgr.commit_txn(txn_id).await.unwrap();
            }
        });
    });

    // Benchmark read after write
    group.bench_function("write_then_read", |b| {
        let mut row_counter = 100_000u64;
        b.to_async(&rt).iter(|| {
            row_counter += 1;
            let mgr = &mgr;
            let row = Row::new(vec![Value::Int64(row_counter as i64)]);
            async move {
                let txn_id = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();
                mgr.write_row(txn_id, table_id, RowId(row_counter), row, true).await.unwrap();
                let _ = mgr.read_row(txn_id, table_id, RowId(row_counter)).await;
                mgr.commit_txn(txn_id).await.unwrap();
            }
        });
    });

    // Benchmark abort path
    group.bench_function("begin_abort", |b| {
        b.to_async(&rt).iter(|| {
            let mgr = &mgr;
            async move {
                let txn_id = mgr.begin_with_isolation(IsolationLevel::ReadCommitted).unwrap();
                mgr.abort_txn(txn_id, thunder_txn::AbortReason::UserRequested).await.unwrap();
            }
        });
    });

    group.finish();
}

fn snapshot_overhead(c: &mut Criterion) {
    let config = MvccConfig {
        max_active_txns: 10000,
        detect_write_conflicts: true,
        default_isolation: IsolationLevel::RepeatableRead,
    };
    let mgr = MvccTransactionManager::new(config);

    c.bench_function("get_snapshot", |b| {
        let txn_id = mgr.begin_with_isolation(IsolationLevel::Serializable).unwrap();
        b.iter(|| {
            let snap = mgr.get_snapshot(txn_id);
            criterion::black_box(snap);
        });
    });
}

criterion_group!(benches, transaction_throughput, transaction_read_write, snapshot_overhead);
criterion_main!(benches);
