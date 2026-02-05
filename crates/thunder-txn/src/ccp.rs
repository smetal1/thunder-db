//! Optimistic Concurrency Control (OCC) implementation.
//!
//! Implements validation-based concurrency control:
//! - Read phase: Execute operations without locks
//! - Validation phase: Check for conflicts before commit
//! - Write phase: Apply changes if validation passes
//!
//! Also supports Serializable Snapshot Isolation (SSI).

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

/// OCC configuration.
#[derive(Debug, Clone)]
pub struct OccConfig {
    /// Maximum number of concurrent transactions
    pub max_concurrent_txns: usize,
    /// Enable backward validation (check against committed txns)
    pub backward_validation: bool,
    /// Enable forward validation (check against active txns)
    pub forward_validation: bool,
    /// Validation retry count before abort
    pub max_validation_retries: usize,
}

impl Default for OccConfig {
    fn default() -> Self {
        Self {
            max_concurrent_txns: 10000,
            backward_validation: true,
            forward_validation: true,
            max_validation_retries: 3,
        }
    }
}

/// OCC transaction state.
#[derive(Debug)]
struct OccTransaction {
    /// Transaction ID
    txn_id: TxnId,
    /// Start timestamp
    start_ts: u64,
    /// Status
    status: TransactionStatus,
    /// Read set: (table_id, row_id) -> version read
    read_set: HashMap<(TableId, RowId), u64>,
    /// Write set: (table_id, row_id) -> new value
    write_set: HashMap<(TableId, RowId), WriteType>,
    /// Validation timestamp (assigned at validation time)
    validation_ts: Option<u64>,
    /// Commit timestamp (assigned at commit time)
    commit_ts: Option<u64>,
}

impl OccTransaction {
    fn new(txn_id: TxnId, start_ts: u64) -> Self {
        Self {
            txn_id,
            start_ts,
            status: TransactionStatus::Active,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            validation_ts: None,
            commit_ts: None,
        }
    }

    fn add_read(&mut self, table_id: TableId, row_id: RowId, version: u64) {
        self.read_set.insert((table_id, row_id), version);
    }

    fn add_write(&mut self, table_id: TableId, row_id: RowId, write_type: WriteType) {
        self.write_set.insert((table_id, row_id), write_type);
    }
}

/// Version info for a data item.
#[derive(Debug, Clone)]
struct VersionInfo {
    /// Current version number
    version: u64,
    /// Transaction that last wrote this item
    writer_txn: TxnId,
    /// Commit timestamp of last write
    commit_ts: u64,
}

/// Committed transaction info for validation.
#[derive(Debug, Clone)]
struct CommittedTxnInfo {
    /// Commit timestamp
    commit_ts: u64,
    /// Write set of committed transaction
    write_set: HashSet<(TableId, RowId)>,
}

/// OCC transaction manager statistics.
#[derive(Debug, Clone, Default)]
pub struct OccStats {
    pub txns_started: u64,
    pub txns_committed: u64,
    pub txns_aborted: u64,
    pub validation_failures: u64,
    pub validation_retries: u64,
    pub reads_performed: u64,
    pub writes_performed: u64,
}

/// OCC transaction manager.
pub struct OccTransactionManager {
    /// Configuration
    config: OccConfig,
    /// Global timestamp counter
    timestamp: AtomicU64,
    /// Active transactions
    active_txns: DashMap<TxnId, RwLock<OccTransaction>>,
    /// Recently committed transactions (for backward validation)
    committed_txns: DashMap<TxnId, CommittedTxnInfo>,
    /// Version table: (table_id, row_id) -> version info
    versions: DashMap<(TableId, RowId), VersionInfo>,
    /// Statistics
    stats: Mutex<OccStats>,
}

impl OccTransactionManager {
    /// Create a new OCC transaction manager.
    pub fn new(config: OccConfig) -> Self {
        Self {
            config,
            timestamp: AtomicU64::new(1),
            active_txns: DashMap::new(),
            committed_txns: DashMap::new(),
            versions: DashMap::new(),
            stats: Mutex::new(OccStats::default()),
        }
    }

    /// Get next timestamp.
    fn next_timestamp(&self) -> u64 {
        self.timestamp.fetch_add(1, Ordering::SeqCst)
    }

    /// Begin a new OCC transaction.
    pub fn begin_txn(&self) -> Result<TxnId> {
        if self.active_txns.len() >= self.config.max_concurrent_txns {
            return Err(Error::ResourceExhausted("Too many concurrent transactions".into()));
        }

        let start_ts = self.next_timestamp();
        let txn_id = TxnId(start_ts);

        let txn = OccTransaction::new(txn_id, start_ts);
        self.active_txns.insert(txn_id, RwLock::new(txn));
        self.stats.lock().txns_started += 1;

        Ok(txn_id)
    }

    /// Read a data item.
    pub fn read(&self, txn_id: TxnId, table_id: TableId, row_id: RowId) -> Result<Option<u64>> {
        let txn_ref = self
            .active_txns
            .get(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))?;

        // Check write set first (read-your-writes)
        {
            let txn = txn_ref.read();
            if txn.write_set.contains_key(&(table_id, row_id)) {
                return Ok(Some(txn.start_ts));
            }
        }

        // Get current version
        let version = self
            .versions
            .get(&(table_id, row_id))
            .map(|v| v.version)
            .unwrap_or(0);

        // Record read
        {
            let mut txn = txn_ref.write();
            txn.add_read(table_id, row_id, version);
        }

        self.stats.lock().reads_performed += 1;

        Ok(Some(version))
    }

    /// Write a data item (buffered until commit).
    pub fn write(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
        row: Row,
    ) -> Result<()> {
        let txn_ref = self
            .active_txns
            .get(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))?;

        let mut txn = txn_ref.write();
        txn.add_write(table_id, row_id, WriteType::Update(row));

        self.stats.lock().writes_performed += 1;

        Ok(())
    }

    /// Insert a data item.
    pub fn insert(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
        row: Row,
    ) -> Result<()> {
        let txn_ref = self
            .active_txns
            .get(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))?;

        let mut txn = txn_ref.write();
        txn.add_write(table_id, row_id, WriteType::Insert(row));

        self.stats.lock().writes_performed += 1;

        Ok(())
    }

    /// Delete a data item.
    pub fn delete(&self, txn_id: TxnId, table_id: TableId, row_id: RowId) -> Result<()> {
        let txn_ref = self
            .active_txns
            .get(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))?;

        let mut txn = txn_ref.write();
        txn.add_write(table_id, row_id, WriteType::Delete);

        self.stats.lock().writes_performed += 1;

        Ok(())
    }

    /// Validate a transaction.
    fn validate(&self, txn: &OccTransaction, validation_ts: u64) -> Result<bool> {
        // Backward validation: check if any item in read set was modified
        // by a transaction that committed after we started
        if self.config.backward_validation {
            for ((table_id, row_id), read_version) in &txn.read_set {
                if let Some(version_info) = self.versions.get(&(*table_id, *row_id)) {
                    // If version changed and was committed after our start
                    if version_info.version > *read_version
                        && version_info.commit_ts > txn.start_ts
                    {
                        return Ok(false);
                    }
                }
            }
        }

        // Forward validation: check if any concurrent transaction's write set
        // overlaps with our read set
        if self.config.forward_validation {
            for entry in self.active_txns.iter() {
                let other_txn = entry.value().read();

                // Skip ourselves and transactions that started after us
                if other_txn.txn_id == txn.txn_id || other_txn.start_ts > txn.start_ts {
                    continue;
                }

                // Check if other transaction's write set overlaps our read set
                for (key, _) in &other_txn.write_set {
                    if txn.read_set.contains_key(key) {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    /// Commit a transaction.
    pub async fn commit_txn(&self, txn_id: TxnId) -> Result<CommitResult> {
        // Remove from active set
        let txn_entry = self
            .active_txns
            .remove(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))?;

        let mut txn = txn_entry.1.into_inner();

        // Validation phase
        let validation_ts = self.next_timestamp();
        txn.validation_ts = Some(validation_ts);

        // Try validation with retries
        let mut validated = false;
        for attempt in 0..=self.config.max_validation_retries {
            if self.validate(&txn, validation_ts)? {
                validated = true;
                break;
            }

            if attempt < self.config.max_validation_retries {
                self.stats.lock().validation_retries += 1;
                // Brief pause before retry
                tokio::time::sleep(std::time::Duration::from_micros(100)).await;
            }
        }

        if !validated {
            self.stats.lock().validation_failures += 1;
            self.stats.lock().txns_aborted += 1;
            return Ok(CommitResult::Aborted {
                reason: AbortReason::SerializationFailure,
            });
        }

        // Write phase - apply all writes
        let commit_ts = self.next_timestamp();
        txn.commit_ts = Some(commit_ts);

        for ((table_id, row_id), _write_type) in &txn.write_set {
            self.versions.insert(
                (*table_id, *row_id),
                VersionInfo {
                    version: commit_ts,
                    writer_txn: txn_id,
                    commit_ts,
                },
            );
        }

        // Record committed transaction for future backward validation
        let write_set: HashSet<_> = txn.write_set.keys().copied().collect();
        self.committed_txns.insert(
            txn_id,
            CommittedTxnInfo {
                commit_ts,
                write_set,
            },
        );

        self.stats.lock().txns_committed += 1;

        Ok(CommitResult::Committed {
            commit_ts: TxnId(commit_ts),
        })
    }

    /// Abort a transaction.
    pub fn abort_txn(&self, txn_id: TxnId) -> Result<()> {
        self.active_txns
            .remove(&txn_id)
            .ok_or_else(|| Error::NotFound("Transaction".into(), txn_id.0.to_string()))?;

        self.stats.lock().txns_aborted += 1;

        Ok(())
    }

    /// Get transaction status.
    pub fn get_status(&self, txn_id: TxnId) -> Option<TransactionStatus> {
        if self.active_txns.contains_key(&txn_id) {
            Some(TransactionStatus::Active)
        } else if self.committed_txns.contains_key(&txn_id) {
            Some(TransactionStatus::Committed)
        } else {
            None
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> OccStats {
        self.stats.lock().clone()
    }

    /// Get active transaction count.
    pub fn active_txn_count(&self) -> usize {
        self.active_txns.len()
    }

    /// Garbage collect old committed transaction info.
    pub fn gc(&self, before_ts: u64) {
        self.committed_txns
            .retain(|_, info| info.commit_ts >= before_ts);
    }
}

#[async_trait]
impl TransactionManager for OccTransactionManager {
    async fn begin(&self, _isolation: IsolationLevel) -> Result<TxnId> {
        self.begin_txn()
    }

    async fn read(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
    ) -> Result<Option<Row>> {
        // OCC read returns version, not actual data
        // In a full implementation, would fetch from storage
        self.read(txn_id, table_id, row_id)?;
        Ok(None)
    }

    async fn write(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
        row: Row,
    ) -> Result<()> {
        OccTransactionManager::write(self, txn_id, table_id, row_id, row)
    }

    async fn delete(&self, txn_id: TxnId, table_id: TableId, row_id: RowId) -> Result<()> {
        OccTransactionManager::delete(self, txn_id, table_id, row_id)
    }

    async fn commit(&self, txn_id: TxnId) -> Result<CommitResult> {
        self.commit_txn(txn_id).await
    }

    async fn abort(&self, txn_id: TxnId) -> Result<()> {
        self.abort_txn(txn_id)
    }

    fn status(&self, txn_id: TxnId) -> Option<TransactionStatus> {
        self.get_status(txn_id)
    }
}

/// Serializable Snapshot Isolation (SSI) manager.
///
/// Extends OCC with anti-dependency tracking for full serializability.
pub struct SsiManager {
    /// Base OCC manager
    occ: OccTransactionManager,
    /// Read-write anti-dependencies: txn that read -> txn that wrote
    rw_dependencies: DashMap<TxnId, HashSet<TxnId>>,
    /// Write-read anti-dependencies: txn that wrote -> txn that read
    wr_dependencies: DashMap<TxnId, HashSet<TxnId>>,
}

impl SsiManager {
    /// Create a new SSI manager.
    pub fn new(config: OccConfig) -> Self {
        Self {
            occ: OccTransactionManager::new(config),
            rw_dependencies: DashMap::new(),
            wr_dependencies: DashMap::new(),
        }
    }

    /// Begin a transaction.
    pub fn begin_txn(&self) -> Result<TxnId> {
        let txn_id = self.occ.begin_txn()?;
        self.rw_dependencies.insert(txn_id, HashSet::new());
        self.wr_dependencies.insert(txn_id, HashSet::new());
        Ok(txn_id)
    }

    /// Read with dependency tracking.
    pub fn read(&self, txn_id: TxnId, table_id: TableId, row_id: RowId) -> Result<Option<u64>> {
        let result = self.occ.read(txn_id, table_id, row_id)?;

        // Track WR anti-dependency
        if let Some(version_info) = self.occ.versions.get(&(table_id, row_id)) {
            let writer = version_info.writer_txn;
            if writer != txn_id {
                // writer wrote something we're reading
                self.wr_dependencies
                    .entry(writer)
                    .or_insert_with(HashSet::new)
                    .insert(txn_id);
            }
        }

        Ok(result)
    }

    /// Write with dependency tracking.
    pub fn write(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
        row: Row,
    ) -> Result<()> {
        // Track RW anti-dependency
        for entry in self.occ.active_txns.iter() {
            let other_txn = entry.value().read();
            if other_txn.txn_id != txn_id && other_txn.read_set.contains_key(&(table_id, row_id)) {
                // other_txn read something we're writing
                self.rw_dependencies
                    .entry(other_txn.txn_id)
                    .or_insert_with(HashSet::new)
                    .insert(txn_id);
            }
        }

        self.occ.write(txn_id, table_id, row_id, row)
    }

    /// Check for dangerous structure (cycle in dependency graph).
    fn has_dangerous_structure(&self, txn_id: TxnId) -> bool {
        // Look for T1 --rw--> T2 --rw--> T1 pattern
        if let Some(rw_out) = self.rw_dependencies.get(&txn_id) {
            for &other_txn in rw_out.value() {
                if let Some(other_rw_out) = self.rw_dependencies.get(&other_txn) {
                    if other_rw_out.contains(&txn_id) {
                        return true;
                    }
                }
            }
        }

        // Also check WR + RW pattern
        if let Some(wr_out) = self.wr_dependencies.get(&txn_id) {
            for &other_txn in wr_out.value() {
                if let Some(other_rw_out) = self.rw_dependencies.get(&other_txn) {
                    if other_rw_out.contains(&txn_id) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Commit with SSI validation.
    pub async fn commit_txn(&self, txn_id: TxnId) -> Result<CommitResult> {
        // Check for dangerous structure
        if self.has_dangerous_structure(txn_id) {
            self.abort_txn(txn_id)?;
            return Ok(CommitResult::Aborted {
                reason: AbortReason::SerializationFailure,
            });
        }

        // Proceed with OCC commit
        let result = self.occ.commit_txn(txn_id).await?;

        // Clean up dependency tracking
        self.rw_dependencies.remove(&txn_id);
        self.wr_dependencies.remove(&txn_id);

        Ok(result)
    }

    /// Abort a transaction.
    pub fn abort_txn(&self, txn_id: TxnId) -> Result<()> {
        self.rw_dependencies.remove(&txn_id);
        self.wr_dependencies.remove(&txn_id);
        self.occ.abort_txn(txn_id)
    }

    /// Get statistics.
    pub fn stats(&self) -> OccStats {
        self.occ.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_occ_begin_commit() {
        let mgr = OccTransactionManager::new(OccConfig::default());

        let txn_id = mgr.begin_txn().unwrap();
        assert!(mgr.active_txns.contains_key(&txn_id));

        let result = mgr.commit_txn(txn_id).await.unwrap();
        assert!(matches!(result, CommitResult::Committed { .. }));
        assert!(!mgr.active_txns.contains_key(&txn_id));
    }

    #[tokio::test]
    async fn test_occ_begin_abort() {
        let mgr = OccTransactionManager::new(OccConfig::default());

        let txn_id = mgr.begin_txn().unwrap();
        mgr.abort_txn(txn_id).unwrap();

        assert!(!mgr.active_txns.contains_key(&txn_id));
    }

    #[tokio::test]
    async fn test_occ_read_write() {
        let mgr = OccTransactionManager::new(OccConfig::default());

        let txn_id = mgr.begin_txn().unwrap();
        let table_id = TableId(1);
        let row_id = RowId(1);
        let row = Row::empty();

        // Read
        let version = mgr.read(txn_id, table_id, row_id).unwrap();
        assert_eq!(version, Some(0)); // Initial version

        // Write
        mgr.write(txn_id, table_id, row_id, row).unwrap();

        // Commit
        let result = mgr.commit_txn(txn_id).await.unwrap();
        assert!(matches!(result, CommitResult::Committed { .. }));

        // Version should be updated
        assert!(mgr.versions.contains_key(&(table_id, row_id)));
    }

    #[tokio::test]
    async fn test_occ_read_your_writes() {
        let mgr = OccTransactionManager::new(OccConfig::default());

        let txn_id = mgr.begin_txn().unwrap();
        let table_id = TableId(1);
        let row_id = RowId(1);
        let row = Row::empty();

        // Write first
        mgr.write(txn_id, table_id, row_id, row).unwrap();

        // Read should see our write
        let version = mgr.read(txn_id, table_id, row_id).unwrap();
        assert!(version.is_some());
    }

    #[tokio::test]
    async fn test_occ_validation_conflict() {
        let config = OccConfig {
            backward_validation: true,
            forward_validation: false,
            max_validation_retries: 0, // Fail immediately
            ..Default::default()
        };
        let mgr = OccTransactionManager::new(config);

        let table_id = TableId(1);
        let row_id = RowId(1);

        // Transaction 1 reads
        let txn1 = mgr.begin_txn().unwrap();
        mgr.read(txn1, table_id, row_id).unwrap();

        // Transaction 2 writes and commits
        let txn2 = mgr.begin_txn().unwrap();
        mgr.write(txn2, table_id, row_id, Row::empty()).unwrap();
        let result2 = mgr.commit_txn(txn2).await.unwrap();
        assert!(matches!(result2, CommitResult::Committed { .. }));

        // Transaction 1 tries to commit - should fail validation
        let result1 = mgr.commit_txn(txn1).await.unwrap();
        assert!(matches!(result1, CommitResult::Aborted { .. }));
    }

    #[tokio::test]
    async fn test_occ_no_conflict() {
        let mgr = OccTransactionManager::new(OccConfig::default());

        let table_id1 = TableId(1);
        let table_id2 = TableId(2);
        let row_id = RowId(1);

        // Transaction 1 reads table 1
        let txn1 = mgr.begin_txn().unwrap();
        mgr.read(txn1, table_id1, row_id).unwrap();

        // Transaction 2 writes to table 2 (no conflict)
        let txn2 = mgr.begin_txn().unwrap();
        mgr.write(txn2, table_id2, row_id, Row::empty()).unwrap();
        let result2 = mgr.commit_txn(txn2).await.unwrap();
        assert!(matches!(result2, CommitResult::Committed { .. }));

        // Transaction 1 commits successfully
        let result1 = mgr.commit_txn(txn1).await.unwrap();
        assert!(matches!(result1, CommitResult::Committed { .. }));
    }

    #[test]
    fn test_occ_stats() {
        let mgr = OccTransactionManager::new(OccConfig::default());

        let _txn1 = mgr.begin_txn().unwrap();
        let _txn2 = mgr.begin_txn().unwrap();

        let stats = mgr.stats();
        assert_eq!(stats.txns_started, 2);
    }

    #[test]
    fn test_occ_transaction_manager_trait() {
        // Just verify it compiles - trait is implemented
        let _mgr: Box<dyn TransactionManager> =
            Box::new(OccTransactionManager::new(OccConfig::default()));
    }

    #[tokio::test]
    async fn test_ssi_basic() {
        let mgr = SsiManager::new(OccConfig::default());

        let txn_id = mgr.begin_txn().unwrap();

        let table_id = TableId(1);
        let row_id = RowId(1);

        mgr.read(txn_id, table_id, row_id).unwrap();
        mgr.write(txn_id, table_id, row_id, Row::empty()).unwrap();

        let result = mgr.commit_txn(txn_id).await.unwrap();
        assert!(matches!(result, CommitResult::Committed { .. }));
    }

    #[tokio::test]
    async fn test_max_concurrent_txns() {
        let config = OccConfig {
            max_concurrent_txns: 2,
            ..Default::default()
        };
        let mgr = OccTransactionManager::new(config);

        let _txn1 = mgr.begin_txn().unwrap();
        let _txn2 = mgr.begin_txn().unwrap();

        // Third should fail
        let result = mgr.begin_txn();
        assert!(result.is_err());
    }
}
