//! Storage engine benchmarks
//!
//! Benchmarks page operations (tuple insert/get/delete) and B+Tree node
//! operations across different workloads and data sizes.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use thunder_storage::page::{Page, PageType, PAGE_SIZE};
use thunder_storage::btree::BTreeNode;
use thunder_common::types::PageId;
use bytes::Bytes;

fn page_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_operations");

    // Benchmark inserting tuples of different sizes
    for tuple_size in [32, 128, 512, 2048] {
        let data: Vec<u8> = (0..tuple_size).map(|i| (i % 256) as u8).collect();

        group.bench_with_input(
            BenchmarkId::new("insert", tuple_size),
            &tuple_size,
            |b, _| {
                b.iter(|| {
                    let mut page = Page::new(PageId(1));
                    let mut count = 0;
                    while page.can_fit(data.len()) {
                        page.insert_tuple(&data).unwrap();
                        count += 1;
                    }
                    count
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("get", tuple_size),
            &tuple_size,
            |b, _| {
                // Setup: fill the page first
                let mut page = Page::new(PageId(1));
                let mut slots = Vec::new();
                while page.can_fit(data.len()) {
                    let slot = page.insert_tuple(&data).unwrap();
                    slots.push(slot);
                }

                b.iter(|| {
                    for &slot in &slots {
                        criterion::black_box(page.get_tuple(slot));
                    }
                })
            },
        );
    }

    // Benchmark mixed insert/delete pattern
    group.bench_function("insert_delete_mixed", |b| {
        let data: Vec<u8> = vec![0xAB; 128];
        b.iter(|| {
            let mut page = Page::new(PageId(1));
            let mut slots = Vec::new();

            // Fill half the page
            for _ in 0..20 {
                if page.can_fit(data.len()) {
                    slots.push(page.insert_tuple(&data).unwrap());
                }
            }

            // Delete every other slot
            for i in (0..slots.len()).step_by(2) {
                let _ = page.delete_tuple(slots[i]);
            }

            // Re-insert
            for _ in 0..10 {
                if page.can_fit(data.len()) {
                    page.insert_tuple(&data).unwrap();
                }
            }
        })
    });

    // Benchmark page free space calculation
    group.bench_function("free_space_check", |b| {
        let mut page = Page::new_with_type(PageId(1), PageType::Data);
        let data: Vec<u8> = vec![0; 128];
        // Insert some tuples first
        for _ in 0..10 {
            if page.can_fit(data.len()) {
                page.insert_tuple(&data).unwrap();
            }
        }
        b.iter(|| {
            criterion::black_box(page.free_space());
            criterion::black_box(page.slot_count());
            criterion::black_box(page.can_fit(128));
        })
    });

    group.finish();
}

fn btree_node_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_node");

    // Benchmark BTreeNode key search (binary search)
    for key_count in [10, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("search_key", key_count),
            &key_count,
            |b, &count| {
                let mut node = BTreeNode::new_leaf(PageId(1));
                for i in 0..count {
                    let key = Bytes::from(format!("key_{:08}", i * 10));
                    node.keys.push(key);
                    node.values.push(i as u64);
                }

                // Search for a key in the middle
                let search_key = format!("key_{:08}", (count / 2) * 10);
                b.iter(|| {
                    criterion::black_box(node.search_key(search_key.as_bytes()));
                })
            },
        );
    }

    // Benchmark node serialization/deserialization
    group.bench_function("serialize_leaf_100", |b| {
        let mut node = BTreeNode::new_leaf(PageId(1));
        for i in 0..100 {
            node.keys.push(Bytes::from(format!("key_{:08}", i)));
            node.values.push(i as u64);
        }
        b.iter(|| {
            let bytes = node.serialize();
            criterion::black_box(bytes);
        })
    });

    group.bench_function("deserialize_leaf_100", |b| {
        let mut node = BTreeNode::new_leaf(PageId(1));
        for i in 0..100 {
            node.keys.push(Bytes::from(format!("key_{:08}", i)));
            node.values.push(i as u64);
        }
        let bytes = node.serialize();
        b.iter(|| {
            let deserialized = BTreeNode::deserialize(PageId(1), &bytes).unwrap();
            criterion::black_box(deserialized);
        })
    });

    // Benchmark node split
    group.bench_function("split_leaf_256", |b| {
        b.iter(|| {
            let mut node = BTreeNode::new_leaf(PageId(1));
            for i in 0..256 {
                node.keys.push(Bytes::from(format!("key_{:08}", i)));
                node.values.push(i as u64);
            }
            let (right, separator) = node.split(PageId(2));
            criterion::black_box((right, separator));
        })
    });

    group.finish();
}

criterion_group!(benches, page_operations, btree_node_operations);
criterion_main!(benches);
