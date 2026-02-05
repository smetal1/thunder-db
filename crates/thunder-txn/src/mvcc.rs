//! MVCC (Multi-Version Concurrency Control) transaction manager.
//!
//! Implements snapshot isolation with:
//! - Transaction lifecycle management
//! - Snapshot-based visibility
//! - Write-write conflict detection
//! - Integration with lock manager

use crate::deadlock::WaitForGraph;
use crate::lock_manager::{LockManager, LockMode, LockResource, LockResult};
use crate::{
    AbortReason, CommitResult, Transaction, TransactionManager, TransactionStatus, WriteType,
};
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thunder_common::error::TransactionError;
use thunder_common::prelude::*;

/// MVCC transaction manager configuration.
#[derive(Debug, Clone)]
pub struct MvccConfig {
    /// Maximum number of active transactions
    pub max_active_txns: usize,
    /// Enable write-write conflict detection
    pub detect_write_conflicts: bool,
    /// Snapshot isolation level
    pub default_isolation: IsolationLevel,
}

impl Default for MvccConfig {
    fn default() -> Self {
        Self {
            max_active_txns: 10000,
            detect_write_conflicts: true,
            default_isolation: IsolationLevel::RepeatableRead,
        }
    }
}

/// Snapshot for transaction visibility.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction ID that created this snapshot
    pub txn_id: TxnId,
    /// Minimum active transaction ID when snapshot was created
    pub xmin: TxnId,
    /// Maximum transaction ID when snapshot was created (exclusive)
    pub xmax: TxnId,
    /// Active transaction IDs when snapshot was created
    pub active_txns: HashSet<TxnId>,
}

impl Snapshot {
    /// Create a new snapshot.
    pub fn new(txn_id: TxnId, xmin: TxnId, xmax: TxnId, active_txns: HashSet<TxnId>) -> Self {
        Self {
            txn_id,
            xmin,
            xmax,
            active_txns,
        }
    }

    /// Check if a transaction's changes are visible to this snapshot.
    pub fn is_visible(&self, other_txn_id: TxnId) -> bool {
        // Own changes are visible
        if other_txn_id == self.txn_id {
            return true;
        }

        // Transactions started after this snapshot are not visible
        if other_txn_id >= self.xmax {
            return false;
        }

        // Transactions that were active when snapshot was taken are not visible
        if self.active_txns.contains(&other_txn_id) {
            return false;
        }

        // All committed transactions before xmax are visible
        true
    }
}

/// MVCC transaction manager statistics.
#[derive(Debug, Clone, Default)]
pub struct MvccStats {
    pub txns_started: u64,
    pub txns_committed: u64,
    pub txns_aborted: u64,
    pub write_conflicts: u64,
    pub reads_performed: u64,
    pub writes_performed: u64,
}

/// MVCC transaction state.
struct MvccTransaction {
    /// Core transaction data
    inner: Transaction,
    /// Snapshot for visibility
    snapshot: Snapshot,
    /// Undo log for abort
    undo_log: Vec<UndoRecord>,
}

/// Undo record for transaction rollback.
#[derive(Debug, Clone)]
struct UndoRecord {
    table_id: TableId,
    row_id: RowId,
    operation: UndoOperation,
}

#[derive(Debug, Clone)]
enum UndoOperation {
    /// Delete the inserted row
    DeleteInsert,
    /// Restore the old version
    RestoreUpdate(Row),
    /// Undelete the row
    RestoreDelete(Row),
}

/// MVCC transaction manager implementation.
pub struct MvccTransactionManager {
    /// Configuration
    config: MvccConfig,
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Active transactions
    active_txns: DashMap<TxnId, RwLock<MvccTransaction>>,
    /// Committed transactions (for GC)
    committed_txns: DashMap<TxnId, TxnId>, // txn_id -> commit_ts
    /// Lock manager
    lock_manager: Arc<LockManager>,
    /// Deadlock detector
    deadlock_detector: Arc<WaitForGraph>,
    /// Statistics
    stats: Mutex<MvccStats>,
    /// Write sets for conflict detection: (table_id, row_id) -> txn_id
    write_intents: DashMap<(TableId, RowId), TxnId>,
}

impl MvccTransactionManager {
    /// Create a new MVCC transaction manager.
    pub fn new(config: MvccConfig) -> Self {
        Self {
            config,
            next_txn_id: AtomicU64::new(1),
            active_txns: DashMap::new(),
            committed_txns: DashMap::new(),
            lock_manager: Arc::new(LockManager::new()),
            deadlock_detector: Arc::new(WaitForGraph::new()),
            stats: Mutex::new(MvccStats::default()),
            write_intents: DashMap::new(),
        }
    }

    /// Create with custom lock manager.
    pub fn with_lock_manager(config: MvccConfig, lock_manager: Arc<LockManager>) -> Self {
        Self {
            config,
            next_txn_id: AtomicU64::new(1),
            active_txns: DashMap::new(),
            committed_txns: DashMap::new(),
            lock_manager,
            deadlock_detector: Arc::new(WaitForGraph::new()),
            stats: Mutex::new(MvccStats::default()),
            write_intents: DashMap::new(),
        }
    }

    /// Set the starting transaction ID after recovery.
    /// This ensures we don't reuse transaction IDs that were used before crash.
    pub fn set_next_txn_id(&self, txn_id: u64) {
        self.next_txn_id.store(txn_id, Ordering::SeqCst);
    }

    /// Get current snapshot for a new transaction.
    fn create_snapshot(&self, txn_id: TxnId) -> Snapshot {
        let active: HashSet<TxnId> = self.active_txns.iter().map(|e| *e.key()).collect();

        let xmin = active.iter().min().copied().unwrap_or(txn_id);
        let xmax = TxnId(self.next_txn_id.load(Ordering::SeqCst));

        Snapshot::new(txn_id, xmin, xmax, active)
    }

    /// Check for write-write conflict.
    fn check_write_conflict(&self, txn_id: TxnId, table_id: TableId, row_id: RowId) -> Option<TxnId> {
        if !self.config.detect_write_conflicts {
            return None;
        }

        let key = (table_id, row_id);
        if let Some(existing) = self.write_intents.get(&key) {
            let existing_txn = *existing;
            if existing_txn != txn_id && self.active_txns.contains_key(&existing_txn) {
                return Some(existing_txn);
            }
        }
        None
    }

    /// Record write intent.
    fn record_write_intent(&self, txn_id: TxnId, table_id: TableId, row_id: RowId) {
        self.write_intents.insert((table_id, row_id), txn_id);
    }

    /// Clear write intents for a transaction.
    fn clear_write_intents(&self, txn_id: TxnId) {
        self.write_intents.retain(|_, v| *v != txn_id);
    }

    /// Get transaction if active.
    fn get_txn(&self, txn_id: TxnId) -> Result<dashmap::mapref::one::Ref<TxnId, RwLock<MvccTransaction>>> {
        self.active_txns
            .get(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))
    }

    /// Get transaction mutably if active.
    fn get_txn_mut(
        &self,
        txn_id: TxnId,
    ) -> Result<dashmap::mapref::one::RefMut<TxnId, RwLock<MvccTransaction>>> {
        self.active_txns
            .get_mut(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))
    }

    /// Get lock manager.
    pub fn lock_manager(&self) -> &Arc<LockManager> {
        &self.lock_manager
    }

    /// Get deadlock detector.
    pub fn deadlock_detector(&self) -> &Arc<WaitForGraph> {
        &self.deadlock_detector
    }

    /// Get statistics.
    pub fn stats(&self) -> MvccStats {
        self.stats.lock().clone()
    }

    /// Get active transaction count.
    pub fn active_txn_count(&self) -> usize {
        self.active_txns.len()
    }

    /// Get snapshot for a transaction.
    pub fn get_snapshot(&self, txn_id: TxnId) -> Result<Snapshot> {
        let txn_ref = self.get_txn(txn_id)?;
        let txn = txn_ref.read();
        Ok(txn.snapshot.clone())
    }

    /// Begin a new transaction with specified isolation level.
    pub fn begin_with_isolation(&self, isolation: IsolationLevel) -> Result<TxnId> {
        if self.active_txns.len() >= self.config.max_active_txns {
            return Err(Error::ResourceExhausted("Too many active transactions".into()));
        }

        let txn_id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let snapshot = self.create_snapshot(txn_id);

        let txn = MvccTransaction {
            inner: Transaction::new(txn_id, isolation),
            snapshot,
            undo_log: Vec::new(),
        };

        self.active_txns.insert(txn_id, RwLock::new(txn));
        self.deadlock_detector.register_txn(txn_id);
        self.stats.lock().txns_started += 1;

        Ok(txn_id)
    }

    /// Perform read within transaction.
    pub async fn read_row(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
    ) -> Result<Option<Row>> {
        let txn_ref = self.get_txn(txn_id)?;

        // Acquire shared lock on row
        let resource = LockResource::Row(table_id, row_id);
        match self.lock_manager.acquire(txn_id, resource, LockMode::Shared)? {
            LockResult::Granted | LockResult::AlreadyHeld(_) => {}
            LockResult::Denied(reason) => {
                return Err(Error::Transaction(TransactionError::WriteConflict(reason)));
            }
            LockResult::Timeout => {
                return Err(Error::Timeout("Lock acquisition timed out".into()));
            }
            LockResult::Waiting => {
                // Shouldn't happen with blocking acquire
                return Err(Error::Internal("Unexpected waiting state".into()));
            }
        }

        // Record read in transaction
        {
            let mut txn = txn_ref.write();
            txn.inner.add_read(table_id, row_id);
        }

        self.stats.lock().reads_performed += 1;

        // Note: Actual row fetch would be done by storage layer using the snapshot
        // For now, return None as placeholder
        Ok(None)
    }

    /// Perform write within transaction.
    pub async fn write_row(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
        row: Row,
        is_insert: bool,
    ) -> Result<()> {
        let txn_ref = self.get_txn_mut(txn_id)?;

        // Check for write-write conflict
        if let Some(conflicting_txn) = self.check_write_conflict(txn_id, table_id, row_id) {
            self.stats.lock().write_conflicts += 1;
            return Err(Error::Transaction(TransactionError::WriteConflict(format!(
                "Write conflict with transaction {:?}",
                conflicting_txn
            ))));
        }

        // Acquire exclusive lock on row
        let resource = LockResource::Row(table_id, row_id);
        match self.lock_manager.acquire(txn_id, resource, LockMode::Exclusive)? {
            LockResult::Granted | LockResult::AlreadyHeld(_) => {}
            LockResult::Denied(reason) => {
                return Err(Error::Transaction(TransactionError::WriteConflict(reason)));
            }
            LockResult::Timeout => {
                return Err(Error::Timeout("Lock acquisition timed out".into()));
            }
            LockResult::Waiting => {
                return Err(Error::Internal("Unexpected waiting state".into()));
            }
        }

        // Record write intent
        self.record_write_intent(txn_id, table_id, row_id);

        // Record write in transaction
        {
            let mut txn = txn_ref.write();
            let write_type = if is_insert {
                WriteType::Insert(row.clone())
            } else {
                WriteType::Update(row.clone())
            };
            txn.inner.add_write(table_id, row_id, write_type);

            // Add undo record
            let undo_op = if is_insert {
                UndoOperation::DeleteInsert
            } else {
                // For updates, we'd need the old value from storage
                UndoOperation::RestoreUpdate(row)
            };
            txn.undo_log.push(UndoRecord {
                table_id,
                row_id,
                operation: undo_op,
            });
        }

        self.stats.lock().writes_performed += 1;

        Ok(())
    }

    /// Delete a row within transaction.
    pub async fn delete_row(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
    ) -> Result<()> {
        let txn_ref = self.get_txn_mut(txn_id)?;

        // Check for write-write conflict
        if let Some(conflicting_txn) = self.check_write_conflict(txn_id, table_id, row_id) {
            self.stats.lock().write_conflicts += 1;
            return Err(Error::Transaction(TransactionError::WriteConflict(format!(
                "Write conflict with transaction {:?}",
                conflicting_txn
            ))));
        }

        // Acquire exclusive lock
        let resource = LockResource::Row(table_id, row_id);
        match self.lock_manager.acquire(txn_id, resource, LockMode::Exclusive)? {
            LockResult::Granted | LockResult::AlreadyHeld(_) => {}
            LockResult::Denied(reason) => {
                return Err(Error::Transaction(TransactionError::WriteConflict(reason)));
            }
            LockResult::Timeout => {
                return Err(Error::Timeout("Lock acquisition timed out".into()));
            }
            LockResult::Waiting => {
                return Err(Error::Internal("Unexpected waiting state".into()));
            }
        }

        // Record write intent
        self.record_write_intent(txn_id, table_id, row_id);

        // Record delete
        {
            let mut txn = txn_ref.write();
            txn.inner.add_write(table_id, row_id, WriteType::Delete);

            // Add undo record (would need old value from storage)
            txn.undo_log.push(UndoRecord {
                table_id,
                row_id,
                operation: UndoOperation::RestoreDelete(Row::empty()),
            });
        }

        self.stats.lock().writes_performed += 1;

        Ok(())
    }

    /// Commit a transaction.
    pub async fn commit_txn(&self, txn_id: TxnId) -> Result<CommitResult> {
        // Remove from active set
        let txn_entry = self
            .active_txns
            .remove(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))?;

        let txn = txn_entry.1.into_inner();

        // Assign commit timestamp
        let commit_ts = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));

        // Record commit
        self.committed_txns.insert(txn_id, commit_ts);

        // Clear write intents
        self.clear_write_intents(txn_id);

        // Release all locks
        self.lock_manager.release_all(txn_id)?;

        // Remove from deadlock detector
        self.deadlock_detector.remove_txn(txn_id);

        self.stats.lock().txns_committed += 1;

        Ok(CommitResult::Committed { commit_ts })
    }

    /// Abort a transaction.
    pub async fn abort_txn(&self, txn_id: TxnId, reason: AbortReason) -> Result<()> {
        // Remove from active set
        let txn_entry = self
            .active_txns
            .remove(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))?;

        let _txn = txn_entry.1.into_inner();

        // Execute undo log in reverse order
        // Note: In a real implementation, this would apply undo operations to storage
        // for _undo in txn.undo_log.into_iter().rev() { ... }

        // Clear write intents
        self.clear_write_intents(txn_id);

        // Release all locks
        self.lock_manager.release_all(txn_id)?;

        // Remove from deadlock detector
        self.deadlock_detector.remove_txn(txn_id);

        self.stats.lock().txns_aborted += 1;

        Ok(())
    }

    /// Check if a transaction is committed.
    pub fn is_committed(&self, txn_id: TxnId) -> bool {
        self.committed_txns.contains_key(&txn_id)
    }

    /// Get commit timestamp for a transaction.
    pub fn get_commit_ts(&self, txn_id: TxnId) -> Option<TxnId> {
        self.committed_txns.get(&txn_id).map(|e| *e)
    }

    /// Garbage collect old transaction metadata.
    pub fn gc(&self, before_txn_id: TxnId) {
        self.committed_txns.retain(|txn_id, _| *txn_id >= before_txn_id);
    }

    /// Get the oldest active transaction ID.
    ///
    /// This is used by vacuum to determine which dead tuples are safe to remove.
    /// Tuples with xmax < oldest_active_txn are invisible to all active transactions
    /// and can be safely removed.
    ///
    /// Returns TxnId(u64::MAX) if there are no active transactions (all dead tuples are safe).
    pub fn oldest_active_txn(&self) -> TxnId {
        self.active_txns
            .iter()
            .map(|entry| *entry.key())
            .min()
            .unwrap_or(TxnId(u64::MAX))
    }

    /// Get list of active transaction IDs.
    pub fn active_transaction_ids(&self) -> Vec<TxnId> {
        self.active_txns
            .iter()
            .map(|entry| *entry.key())
            .collect()
    }
}

#[async_trait]
impl TransactionManager for MvccTransactionManager {
    async fn begin(&self, isolation: IsolationLevel) -> Result<TxnId> {
        self.begin_with_isolation(isolation)
    }

    async fn read(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
    ) -> Result<Option<Row>> {
        self.read_row(txn_id, table_id, row_id).await
    }

    async fn write(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
        row: Row,
    ) -> Result<()> {
        self.write_row(txn_id, table_id, row_id, row, false).await
    }

    async fn delete(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
    ) -> Result<()> {
        self.delete_row(txn_id, table_id, row_id).await
    }

    async fn commit(&self, txn_id: TxnId) -> Result<CommitResult> {
        self.commit_txn(txn_id).await
    }

    async fn abort(&self, txn_id: TxnId) -> Result<()> {
        self.abort_txn(txn_id, AbortReason::UserRequested).await
    }

    fn status(&self, txn_id: TxnId) -> Option<TransactionStatus> {
        if self.active_txns.contains_key(&txn_id) {
            Some(TransactionStatus::Active)
        } else if self.committed_txns.contains_key(&txn_id) {
            Some(TransactionStatus::Committed)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_visibility() {
        let mut active = HashSet::new();
        active.insert(TxnId(3));
        active.insert(TxnId(5));

        // Snapshot for txn 10, with xmin=3, xmax=11 (next txn to be assigned)
        let snapshot = Snapshot::new(TxnId(10), TxnId(3), TxnId(11), active);

        // Own changes are visible
        assert!(snapshot.is_visible(TxnId(10)));

        // Committed transactions before snapshot are visible
        assert!(snapshot.is_visible(TxnId(1)));
        assert!(snapshot.is_visible(TxnId(2)));

        // Active transactions when snapshot was taken are NOT visible
        assert!(!snapshot.is_visible(TxnId(3)));
        assert!(!snapshot.is_visible(TxnId(5)));

        // Committed transactions not in active set are visible
        assert!(snapshot.is_visible(TxnId(4)));

        // Transactions started after snapshot are NOT visible
        assert!(!snapshot.is_visible(TxnId(11)));
        assert!(!snapshot.is_visible(TxnId(12)));
    }

    #[tokio::test]
    async fn test_begin_commit() {
        let mgr = MvccTransactionManager::new(MvccConfig::default());

        let txn_id = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();
        assert!(mgr.active_txns.contains_key(&txn_id));
        assert_eq!(mgr.active_txn_count(), 1);

        let result = mgr.commit_txn(txn_id).await.unwrap();
        assert!(matches!(result, CommitResult::Committed { .. }));
        assert!(!mgr.active_txns.contains_key(&txn_id));
        assert!(mgr.is_committed(txn_id));
    }

    #[tokio::test]
    async fn test_begin_abort() {
        let mgr = MvccTransactionManager::new(MvccConfig::default());

        let txn_id = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();
        assert_eq!(mgr.active_txn_count(), 1);

        mgr.abort_txn(txn_id, AbortReason::UserRequested).await.unwrap();
        assert!(!mgr.active_txns.contains_key(&txn_id));
        assert!(!mgr.is_committed(txn_id));
    }

    #[tokio::test]
    async fn test_write_conflict() {
        let config = MvccConfig {
            detect_write_conflicts: true,
            ..Default::default()
        };
        let mgr = MvccTransactionManager::new(config);

        let txn1 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();
        let txn2 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();

        let table_id = TableId(1);
        let row_id = RowId(1);
        let row = Row::empty();

        // First write succeeds
        mgr.write_row(txn1, table_id, row_id, row.clone(), false).await.unwrap();

        // Second write to same row should fail with conflict
        let result = mgr.write_row(txn2, table_id, row_id, row, false).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_snapshot_isolation() {
        let mgr = MvccTransactionManager::new(MvccConfig::default());

        let txn1 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();
        let txn2 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();

        // Get snapshots
        let snapshot1 = mgr.get_snapshot(txn1).unwrap();
        let snapshot2 = mgr.get_snapshot(txn2).unwrap();

        // Each transaction sees its own changes
        assert!(snapshot1.is_visible(txn1));
        assert!(snapshot2.is_visible(txn2));

        // But not each other's uncommitted changes
        assert!(!snapshot1.is_visible(txn2));
        assert!(!snapshot2.is_visible(txn1));
    }

    #[tokio::test]
    async fn test_transaction_manager_trait() {
        let mgr = MvccTransactionManager::new(MvccConfig::default());

        // Use trait methods
        let txn_id = mgr.begin(IsolationLevel::RepeatableRead).await.unwrap();

        assert_eq!(mgr.status(txn_id), Some(TransactionStatus::Active));

        mgr.commit(txn_id).await.unwrap();

        assert_eq!(mgr.status(txn_id), Some(TransactionStatus::Committed));
    }

    #[tokio::test]
    async fn test_max_active_txns() {
        let config = MvccConfig {
            max_active_txns: 2,
            ..Default::default()
        };
        let mgr = MvccTransactionManager::new(config);

        let _txn1 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();
        let _txn2 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();

        // Third transaction should fail
        let result = mgr.begin_with_isolation(IsolationLevel::RepeatableRead);
        assert!(result.is_err());
    }

    #[test]
    fn test_stats() {
        let mgr = MvccTransactionManager::new(MvccConfig::default());

        let _txn1 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();
        let _txn2 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();

        let stats = mgr.stats();
        assert_eq!(stats.txns_started, 2);
    }

    #[tokio::test]
    async fn test_gc() {
        let mgr = MvccTransactionManager::new(MvccConfig::default());

        let txn1 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();
        let txn2 = mgr.begin_with_isolation(IsolationLevel::RepeatableRead).unwrap();

        mgr.commit_txn(txn1).await.unwrap();
        mgr.commit_txn(txn2).await.unwrap();

        assert!(mgr.is_committed(txn1));
        assert!(mgr.is_committed(txn2));

        // GC transactions before txn2
        mgr.gc(txn2);

        assert!(!mgr.is_committed(txn1));
        assert!(mgr.is_committed(txn2));
    }
}
