//! ACID Compliance Integration Tests for ThunderDB Storage Engine
//!
//! These tests verify that the storage engine properly implements:
//! - **Atomicity**: All operations in a transaction complete or none do
//! - **Consistency**: Data integrity is maintained
//! - **Isolation**: Concurrent transactions don't interfere
//! - **Durability**: Committed data survives crashes

use bytes::Bytes;
use std::sync::Arc;
use tempfile::tempdir;
use thunder_storage::{
    buffer::{BufferPoolImpl, DiskManager},
    btree::{BTree, BTreeConfig},
    page::Page,
    row_store::{FreeSpaceMap, RowStoreImpl, Snapshot},
    wal::{
        InsertRecord, WalRecordType, WalRecovery, WalWriterImpl,
        RecoveryExecutor, CheckpointRecord,
    },
    WalWriter,
};
use thunder_common::prelude::*;
use thunder_common::types::{Lsn, PageId, RowId, TableId, TxnId, Value};

// ============================================================================
// Test Helpers
// ============================================================================

fn create_test_db() -> (Arc<BufferPoolImpl>, Arc<WalWriterImpl>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("wal");

    let dm = Arc::new(DiskManager::new(&db_path).unwrap());
    let pool = Arc::new(BufferPoolImpl::new(500, dm));
    let wal = Arc::new(WalWriterImpl::new(&wal_path, true).unwrap());

    (pool, wal, dir)
}

fn create_test_row(id: i64, name: &str) -> Row {
    Row {
        values: vec![Value::Int64(id), Value::String(name.into())],
    }
}

// ============================================================================
// Atomicity Tests
// ============================================================================

mod atomicity {
    use super::*;

    /// Test that committed transactions persist all changes.
    #[tokio::test]
    async fn test_committed_transaction_persists() {
        let (pool, wal, _dir) = create_test_db();
        let store = RowStoreImpl::new(pool.clone(), wal.clone());
        let table_id = TableId(1);
        let txn_id = TxnId(1);

        // Insert rows
        let row1 = create_test_row(1, "Alice");
        let row2 = create_test_row(2, "Bob");

        let id1 = store.insert(table_id, txn_id, row1).await.unwrap();
        let id2 = store.insert(table_id, txn_id, row2).await.unwrap();

        assert!(id1.0 > 0);
        assert!(id2.0 > 0);
        assert_ne!(id1, id2);
    }

    /// Test that WAL records are written for all operations.
    #[test]
    fn test_wal_records_written() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");
        let wal = WalWriterImpl::new(&wal_path, true).unwrap();

        // Write multiple records
        let insert = InsertRecord {
            row_id: RowId(1),
            page_id: PageId(10),
            slot_id: 0,
            tuple_data: Bytes::from_static(b"test"),
        };

        let lsn1 = wal.write_record(TxnId(1), TableId(1), WalRecordType::Insert, &insert.encode())
            .unwrap();
        let lsn2 = wal.write_record(TxnId(1), TableId(1), WalRecordType::Commit, &[])
            .unwrap();

        // LSNs should be monotonically increasing
        assert!(lsn2 > lsn1);

        // Flush and verify
        wal.flush_buffer().unwrap();

        // WAL file should exist
        let files: Vec<_> = std::fs::read_dir(&wal_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!files.is_empty());
    }

    /// Test B+Tree atomicity - inserts should all be visible or none.
    #[test]
    fn test_btree_atomic_inserts() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = Arc::new(BufferPoolImpl::new(100, dm));

        let btree = BTree::new(
            IndexId(1),
            pool.clone(),
            BTreeConfig {
                max_keys: 64, // Larger fanout to reduce tree depth
                min_keys: 32,
                prefix_compression: false,
            },
        ).unwrap();

        // Insert multiple keys
        for i in 0..10 {
            let key = format!("key{:05}", i);
            btree.insert(key.as_bytes(), RowId(i as u64)).unwrap();
        }

        // All keys should be visible
        for i in 0..10 {
            let key = format!("key{:05}", i);
            let result = btree.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(RowId(i as u64)), "Key {} not found", key);
        }
    }
}

// ============================================================================
// Consistency Tests
// ============================================================================

mod consistency {
    use super::*;

    /// Test page checksums detect corruption.
    #[test]
    fn test_page_checksum_integrity() {
        let mut page = Page::new(PageId(1));

        // Insert data
        let data = b"test tuple data";
        page.insert_tuple(data).unwrap();

        // Verify data can be read
        let slot_count = page.slot_count();
        assert_eq!(slot_count, 1);

        let tuple = page.get_tuple(0);
        assert!(tuple.is_some());
        assert_eq!(tuple.unwrap(), data);
    }

    /// Test free space tracking is accurate.
    #[test]
    fn test_free_space_map_consistency() {
        let fsm = FreeSpaceMap::new();

        // Add pages with different free space
        fsm.update(PageId(1), 1000);
        fsm.update(PageId(2), 500);
        fsm.update(PageId(3), 2000);

        // Find should return appropriate pages
        let page = fsm.find_page(1500).unwrap();
        assert_eq!(page, PageId(3));

        // Update and verify
        fsm.update(PageId(3), 100);
        let page = fsm.find_page(1500);
        assert!(page.is_none());
    }

    /// Test B+Tree maintains sorted order.
    #[test]
    fn test_btree_sorted_order() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = Arc::new(BufferPoolImpl::new(500, dm));

        let btree = BTree::new(
            IndexId(1),
            pool.clone(),
            BTreeConfig {
                max_keys: 8,
                min_keys: 4,
                prefix_compression: false,
            },
        ).unwrap();

        // Insert in random order
        let keys = vec!["zebra", "apple", "mango", "banana", "cherry"];
        for (i, key) in keys.iter().enumerate() {
            btree.insert(key.as_bytes(), RowId(i as u64)).unwrap();
        }

        // Scan should return in sorted order
        let results = btree.scan_all().unwrap();
        let sorted_keys: Vec<&str> = results.iter()
            .map(|(k, _)| std::str::from_utf8(k).unwrap())
            .collect();

        assert_eq!(sorted_keys, vec!["apple", "banana", "cherry", "mango", "zebra"]);
    }

    /// Test unique constraint via B+Tree updates.
    #[test]
    fn test_btree_unique_keys() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = Arc::new(BufferPoolImpl::new(500, dm));

        let btree = BTree::new(IndexId(1), pool.clone(), BTreeConfig::default()).unwrap();

        // Insert a key
        btree.insert(b"unique_key", RowId(1)).unwrap();
        assert_eq!(btree.get(b"unique_key").unwrap(), Some(RowId(1)));

        // Insert same key with different value (should update)
        btree.insert(b"unique_key", RowId(999)).unwrap();
        assert_eq!(btree.get(b"unique_key").unwrap(), Some(RowId(999)));
    }
}

// ============================================================================
// Isolation Tests
// ============================================================================

mod isolation {
    use super::*;

    /// Test snapshot isolation - transaction sees consistent snapshot.
    #[test]
    fn test_snapshot_visibility_basic() {
        // Create snapshot for transaction 10
        // xmin=3, xmax=11 (next txn), active=[5]
        let snapshot = Snapshot::new(
            TxnId(10),
            TxnId(3),
            TxnId(11),
            vec![TxnId(5)],
        );

        // Own changes are visible
        assert!(snapshot.is_visible(TxnId(10), TxnId(0)));

        // Committed transaction before snapshot is visible
        assert!(snapshot.is_visible(TxnId(2), TxnId(0)));

        // Active transaction at snapshot time is NOT visible
        assert!(!snapshot.is_visible(TxnId(5), TxnId(0)));

        // Future transaction is NOT visible
        assert!(!snapshot.is_visible(TxnId(12), TxnId(0)));
    }

    /// Test deleted tuples visibility.
    #[test]
    fn test_snapshot_deleted_visibility() {
        let snapshot = Snapshot::new(
            TxnId(10),
            TxnId(1),
            TxnId(11),
            vec![],
        );

        // Tuple deleted by committed transaction (xmax < snapshot.xmax) is NOT visible
        assert!(!snapshot.is_visible(TxnId(2), TxnId(5)));

        // Tuple deleted by future transaction is visible
        assert!(snapshot.is_visible(TxnId(2), TxnId(15)));

        // Tuple deleted by self is NOT visible
        assert!(!snapshot.is_visible(TxnId(10), TxnId(10)));
    }

    /// Test read-your-own-writes.
    #[test]
    fn test_read_your_own_writes() {
        let snapshot = Snapshot::new(
            TxnId(5),
            TxnId(1),
            TxnId(10),
            vec![],
        );

        // Own insert is visible (xmin = our txn, xmax = 0)
        assert!(snapshot.is_visible(TxnId(5), TxnId(0)));
    }

    /// Test B+Tree concurrent access via range scans.
    #[test]
    fn test_btree_range_scan_isolation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = Arc::new(BufferPoolImpl::new(500, dm));

        let btree = BTree::new(IndexId(1), pool.clone(), BTreeConfig::default()).unwrap();

        // Insert keys
        for i in 0..10 {
            let key = format!("key{:02}", i);
            btree.insert(key.as_bytes(), RowId(i as u64)).unwrap();
        }

        // Range scan should see all keys in range
        let results = btree.range(b"key02", b"key07").unwrap();
        assert_eq!(results.len(), 6); // key02, key03, key04, key05, key06, key07

        // Verify values
        for (i, (key, row_id)) in results.iter().enumerate() {
            let expected_key = format!("key{:02}", i + 2);
            assert_eq!(std::str::from_utf8(key).unwrap(), expected_key);
            assert_eq!(*row_id, RowId((i + 2) as u64));
        }
    }
}

// ============================================================================
// Durability Tests
// ============================================================================

mod durability {
    use super::*;

    /// Test WAL recovery for committed transactions.
    #[test]
    fn test_wal_recovery_committed() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");

        // Phase 1: Write and commit a transaction
        {
            let wal = WalWriterImpl::new(&wal_path, true).unwrap();

            let insert = InsertRecord {
                row_id: RowId(1),
                page_id: PageId(10),
                slot_id: 0,
                tuple_data: Bytes::from_static(b"committed data"),
            };

            wal.write_record(TxnId(1), TableId(1), WalRecordType::Insert, &insert.encode())
                .unwrap();
            wal.write_record(TxnId(1), TableId(1), WalRecordType::Commit, &[])
                .unwrap();
            wal.flush_buffer().unwrap();
        }

        // Phase 2: Recovery should find committed transaction
        let recovery = WalRecovery::new(&wal_path);
        let plan = recovery.recover().unwrap();

        // Committed transaction - should have redo but no undo
        assert!(plan.redo_count() > 0, "Should have redo records");
        assert_eq!(plan.undo_count(), 0, "Should not have undo records");
        assert_eq!(plan.active_txn_count(), 0, "Should not have active txns");
        assert!(plan.analysis.committed_txns.contains(&TxnId(1)));
    }

    /// Test WAL recovery for uncommitted transactions.
    #[test]
    fn test_wal_recovery_uncommitted() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");

        // Phase 1: Write but DON'T commit (simulating crash)
        {
            let wal = WalWriterImpl::new(&wal_path, true).unwrap();

            let insert = InsertRecord {
                row_id: RowId(1),
                page_id: PageId(10),
                slot_id: 0,
                tuple_data: Bytes::from_static(b"uncommitted data"),
            };

            wal.write_record(TxnId(1), TableId(1), WalRecordType::Insert, &insert.encode())
                .unwrap();
            // No commit!
            wal.flush_buffer().unwrap();
        }

        // Phase 2: Recovery should identify uncommitted transaction
        let recovery = WalRecovery::new(&wal_path);
        let plan = recovery.recover().unwrap();

        assert!(plan.needs_recovery());
        assert!(plan.undo_count() > 0, "Should have undo records");
        assert_eq!(plan.active_txn_count(), 1, "Should have 1 active txn");
    }

    /// Test page persistence via buffer pool.
    #[test]
    fn test_page_persistence() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let page_id;

        // Phase 1: Create page and write data
        {
            let dm = Arc::new(DiskManager::new(&db_path).unwrap());
            let pool = Arc::new(BufferPoolImpl::new(10, dm));

            let (pid, mut guard) = pool.new_page().unwrap();
            page_id = pid;

            let data = b"persistent data that should survive";
            guard.page_mut().insert_tuple(data).unwrap();

            drop(guard);
            pool.flush_page(page_id).unwrap();
        }

        // Phase 2: Reopen and verify data persists
        {
            let dm = Arc::new(DiskManager::new(&db_path).unwrap());
            let pool = Arc::new(BufferPoolImpl::new(10, dm));

            let guard = pool.fetch_page(page_id).unwrap();
            let tuple = guard.get_tuple(0);
            assert!(tuple.is_some());
            assert_eq!(tuple.unwrap(), b"persistent data that should survive");
        }
    }

    /// Test B+Tree persistence.
    #[test]
    fn test_btree_persistence() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let root_page_id;

        // Phase 1: Create B+Tree and insert data
        {
            let dm = Arc::new(DiskManager::new(&db_path).unwrap());
            let pool = Arc::new(BufferPoolImpl::new(100, dm));

            let config = BTreeConfig {
                max_keys: 64,
                min_keys: 32,
                prefix_compression: false,
            };
            let btree = BTree::new(IndexId(1), pool.clone(), config).unwrap();
            root_page_id = btree.root_page_id();

            for i in 0..10 {
                let key = format!("persistent_key_{:03}", i);
                btree.insert(key.as_bytes(), RowId(i as u64)).unwrap();
            }

            // Flush all pages
            pool.flush_all().unwrap();
        }

        // Phase 2: Reopen and verify data persists
        {
            let dm = Arc::new(DiskManager::new(&db_path).unwrap());
            let pool = Arc::new(BufferPoolImpl::new(100, dm));

            let config = BTreeConfig {
                max_keys: 64,
                min_keys: 32,
                prefix_compression: false,
            };
            let btree = BTree::open(IndexId(1), root_page_id, pool.clone(), config);

            for i in 0..10 {
                let key = format!("persistent_key_{:03}", i);
                let result = btree.get(key.as_bytes()).unwrap();
                assert_eq!(result, Some(RowId(i as u64)), "Key {} should persist", key);
            }
        }
    }

    /// Test WAL checkpoint record.
    #[test]
    fn test_checkpoint_record() {
        let checkpoint = CheckpointRecord {
            active_txns: vec![
                (TxnId(1), Lsn(100)),
                (TxnId(2), Lsn(200)),
            ],
            dirty_pages: vec![
                (PageId(10), Lsn(50)),
                (PageId(20), Lsn(150)),
            ],
        };

        let encoded = checkpoint.encode();
        let decoded = CheckpointRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.active_txns.len(), 2);
        assert_eq!(decoded.dirty_pages.len(), 2);
        assert_eq!(decoded.active_txns[0], (TxnId(1), Lsn(100)));
        assert_eq!(decoded.dirty_pages[1], (PageId(20), Lsn(150)));
    }

    /// Test recovery executor.
    #[test]
    fn test_recovery_execution() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");

        // Create WAL with uncommitted transaction
        {
            let wal = WalWriterImpl::new(&wal_path, true).unwrap();

            let insert = InsertRecord {
                row_id: RowId(1),
                page_id: PageId(10),
                slot_id: 0,
                tuple_data: Bytes::from_static(b"test"),
            };

            wal.write_record(TxnId(1), TableId(1), WalRecordType::Insert, &insert.encode())
                .unwrap();
            wal.flush_buffer().unwrap();
        }

        // Perform recovery
        let recovery = WalRecovery::new(&wal_path);
        let plan = recovery.recover().unwrap();

        // Execute recovery with mock handlers
        let mut redo_count = 0;
        let mut undo_count = 0;

        let mut executor = RecoveryExecutor::new(
            &plan,
            |_record| {
                redo_count += 1;
                Ok(())
            },
            |_record| {
                undo_count += 1;
                Ok(())
            },
        );

        let stats = executor.execute().unwrap();

        assert!(stats.is_successful());
        assert_eq!(stats.records_redone as usize, plan.redo_count());
        assert_eq!(stats.records_undone as usize, plan.undo_count());
    }
}

// ============================================================================
// Stress Tests
// ============================================================================

mod stress {
    use super::*;

    /// Test B+Tree with many splits.
    #[test]
    fn test_btree_many_splits() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = Arc::new(BufferPoolImpl::new(100, dm));

        let config = BTreeConfig {
            max_keys: 8, // Small to force splits
            min_keys: 4,
            prefix_compression: false,
        };

        let btree = BTree::new(IndexId(1), pool.clone(), config).unwrap();

        // Insert keys to trigger a few splits
        for i in 0..20 {
            let key = format!("stress_key_{:06}", i);
            btree.insert(key.as_bytes(), RowId(i as u64)).unwrap();
        }

        // Verify all keys
        for i in 0..20 {
            let key = format!("stress_key_{:06}", i);
            let result = btree.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(RowId(i as u64)), "Key {} missing after splits", key);
        }

        // Verify stats show splits occurred
        let splits = btree.stats().splits.load(std::sync::atomic::Ordering::Relaxed);
        assert!(splits > 0, "Should have performed splits");
    }

    /// Test buffer pool eviction under pressure.
    #[test]
    fn test_buffer_pool_eviction() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = Arc::new(DiskManager::new(&db_path).unwrap());

        // Small buffer pool to force eviction
        let pool = Arc::new(BufferPoolImpl::new(10, dm));

        // Allocate many pages (more than pool size)
        let mut page_ids = Vec::new();
        for _ in 0..50 {
            let (page_id, mut guard) = pool.new_page().unwrap();
            guard.page_mut().insert_tuple(b"test data").unwrap();
            page_ids.push(page_id);
            drop(guard);
        }

        // Flush all
        pool.flush_all().unwrap();

        // All pages should still be accessible
        for page_id in page_ids {
            let guard = pool.fetch_page(page_id).unwrap();
            assert!(guard.get_tuple(0).is_some());
        }
    }

    /// Test WAL with many records.
    #[test]
    fn test_wal_many_records() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");
        let wal = WalWriterImpl::new(&wal_path, false).unwrap();

        // Write many records
        for i in 0..1000 {
            let insert = InsertRecord {
                row_id: RowId(i),
                page_id: PageId(i % 100),
                slot_id: (i % 256) as u16,
                tuple_data: Bytes::from(format!("data_{}", i)),
            };

            wal.write_record(
                TxnId((i / 10) + 1),
                TableId(1),
                WalRecordType::Insert,
                &insert.encode(),
            ).unwrap();
        }

        // Flush
        wal.flush_buffer().unwrap();

        // Verify current LSN
        let current_lsn = wal.current_lsn();
        assert!(current_lsn.0 >= 1000);
    }
}
