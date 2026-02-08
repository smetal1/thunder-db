//! Query executor benchmarks
//!
//! Benchmarks query execution for different plan types using in-memory data.
//! Tests sequential scan, filter, projection, sorting, and aggregation.

use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use thunder_common::types::*;
use thunder_query::executor::MemoryExecutor;
use thunder_query::PhysicalPlan;
use thunder_sql::{Expr, BinaryOperator, AggregateExpr, AggregateFunction, SortExpr};

/// Generate test rows with schema: (id INT64, name STRING, value FLOAT64, active BOOLEAN)
fn generate_test_data(count: usize) -> (Schema, Vec<Row>) {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::String),
        ColumnDef::new("value", DataType::Float64),
        ColumnDef::new("active", DataType::Boolean),
    ]);

    let rows: Vec<Row> = (0..count)
        .map(|i| {
            Row::new(vec![
                Value::Int64(i as i64),
                Value::String(format!("row_{}", i).into()),
                Value::Float64(i as f64 * 1.5),
                Value::Boolean(i % 2 == 0),
            ])
        })
        .collect();

    (schema, rows)
}

fn seq_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_scan");

    for row_count in [100, 1000, 10000] {
        let (schema, rows) = generate_test_data(row_count);
        let table_id = TableId(1);

        let mut executor = MemoryExecutor::new();
        executor.register_table(table_id, schema.clone(), rows);

        let plan = PhysicalPlan::SeqScan {
            table_id,
            schema: schema.clone(),
            filter: None,
        };

        group.bench_with_input(
            BenchmarkId::new("no_filter", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let result = executor.execute(&plan).unwrap();
                    criterion::black_box(result);
                })
            },
        );
    }

    group.finish();
}

fn filter_scan(c: &mut Criterion) {
    let (schema, rows) = generate_test_data(10_000);
    let table_id = TableId(1);

    let mut executor = MemoryExecutor::new();
    executor.register_table(table_id, schema.clone(), rows);

    let mut group = c.benchmark_group("filter_scan");

    // Filter: id > 5000 (50% selectivity)
    let filter_50pct = Expr::BinaryOp {
        left: Box::new(Expr::Column { name: "id".to_string(), index: Some(0) }),
        op: BinaryOperator::Gt,
        right: Box::new(Expr::Literal(Value::Int64(5000))),
    };

    let plan_50 = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::SeqScan {
            table_id,
            schema: schema.clone(),
            filter: None,
        }),
        predicate: filter_50pct,
    };

    group.bench_function("selectivity_50pct", |b| {
        b.iter(|| {
            let result = executor.execute(&plan_50).unwrap();
            criterion::black_box(result);
        })
    });

    // Filter: id > 9900 (1% selectivity)
    let filter_1pct = Expr::BinaryOp {
        left: Box::new(Expr::Column { name: "id".to_string(), index: Some(0) }),
        op: BinaryOperator::Gt,
        right: Box::new(Expr::Literal(Value::Int64(9900))),
    };

    let plan_1 = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::SeqScan {
            table_id,
            schema: schema.clone(),
            filter: None,
        }),
        predicate: filter_1pct,
    };

    group.bench_function("selectivity_1pct", |b| {
        b.iter(|| {
            let result = executor.execute(&plan_1).unwrap();
            criterion::black_box(result);
        })
    });

    group.finish();
}

fn projection(c: &mut Criterion) {
    let (schema, rows) = generate_test_data(10_000);
    let table_id = TableId(1);

    let mut executor = MemoryExecutor::new();
    executor.register_table(table_id, schema.clone(), rows);

    let projected_schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::String),
    ]);

    let plan = PhysicalPlan::Project {
        input: Box::new(PhysicalPlan::SeqScan {
            table_id,
            schema: schema.clone(),
            filter: None,
        }),
        exprs: vec![
            Expr::Column { name: "id".to_string(), index: Some(0) },
            Expr::Column { name: "name".to_string(), index: Some(1) },
        ],
        schema: projected_schema,
    };

    c.bench_function("project_2_of_4_cols_10k", |b| {
        b.iter(|| {
            let result = executor.execute(&plan).unwrap();
            criterion::black_box(result);
        })
    });
}

fn sort_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort");

    for row_count in [100, 1000, 10000] {
        let (schema, rows) = generate_test_data(row_count);
        let table_id = TableId(1);

        let mut executor = MemoryExecutor::new();
        executor.register_table(table_id, schema.clone(), rows);

        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::SeqScan {
                table_id,
                schema: schema.clone(),
                filter: None,
            }),
            order_by: vec![SortExpr {
                expr: Expr::Column { name: "value".to_string(), index: Some(2) },
                asc: false,
                nulls_first: false,
            }],
        };

        group.bench_with_input(
            BenchmarkId::new("sort_desc", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let result = executor.execute(&plan).unwrap();
                    criterion::black_box(result);
                })
            },
        );
    }

    group.finish();
}

fn aggregation(c: &mut Criterion) {
    let (schema, rows) = generate_test_data(10_000);
    let table_id = TableId(1);

    let mut executor = MemoryExecutor::new();
    executor.register_table(table_id, schema.clone(), rows);

    let agg_schema = Schema::new(vec![
        ColumnDef::new("active", DataType::Boolean),
        ColumnDef::new("count", DataType::Int64),
        ColumnDef::new("sum_value", DataType::Float64),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        input: Box::new(PhysicalPlan::SeqScan {
            table_id,
            schema: schema.clone(),
            filter: None,
        }),
        group_by: vec![3], // group by active
        aggregates: vec![
            AggregateExpr {
                func: AggregateFunction::Count,
                args: vec![Expr::Column { name: "id".to_string(), index: Some(0) }],
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunction::Sum,
                args: vec![Expr::Column { name: "value".to_string(), index: Some(2) }],
                distinct: false,
            },
        ],
        schema: agg_schema,
    };

    c.bench_function("hash_aggregate_group_by_10k", |b| {
        b.iter(|| {
            let result = executor.execute(&plan).unwrap();
            criterion::black_box(result);
        })
    });
}

criterion_group!(benches, seq_scan, filter_scan, projection, sort_benchmark, aggregation);
criterion_main!(benches);
