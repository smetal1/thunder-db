//! # Thunder Transaction
//!
//! Transaction management for ThunderDB providing:
//! - MVCC (Multi-Version Concurrency Control)
//! - Optimistic Concurrency Control Protocol (CCP)
//! - Deadlock detection
//! - Distributed transaction coordination

pub mod ccp;
pub mod coordinator;
pub mod deadlock;
pub mod lock_manager;
pub mod mvcc;

// Re-export key types for convenience
pub use ccp::{OccConfig, OccStats, OccTransactionManager, SsiManager};
pub use coordinator::{
    CoordinatorConfig, CoordinatorStats, DistributedTxnState, Participant, ParticipantId,
    PrepareVote, TwoPhaseCoordinator, TwoPhaseMessage,
};
pub use deadlock::{
    BackgroundDeadlockDetector, DeadlockCycle, DeadlockStats, TimeoutDetector, VictimSelection,
    WaitEdge, WaitForGraph,
};
pub use lock_manager::{LockManager, LockManagerStats, LockMode, LockRequest, LockResource, LockResult};
pub use mvcc::{MvccConfig, MvccStats, MvccTransactionManager, Snapshot};

use async_trait::async_trait;
use thunder_common::prelude::*;

/// Transaction manager trait
#[async_trait]
pub trait TransactionManager: Send + Sync {
    /// Begin a new transaction
    async fn begin(&self, isolation: IsolationLevel) -> Result<TxnId>;

    /// Read a row within a transaction
    async fn read(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
    ) -> Result<Option<Row>>;

    /// Write a row within a transaction
    async fn write(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
        row: Row,
    ) -> Result<()>;

    /// Delete a row within a transaction
    async fn delete(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        row_id: RowId,
    ) -> Result<()>;

    /// Commit a transaction
    async fn commit(&self, txn_id: TxnId) -> Result<CommitResult>;

    /// Abort a transaction
    async fn abort(&self, txn_id: TxnId) -> Result<()>;

    /// Get transaction status
    fn status(&self, txn_id: TxnId) -> Option<TransactionStatus>;
}

/// Result of a commit operation
#[derive(Debug, Clone)]
pub enum CommitResult {
    Committed { commit_ts: TxnId },
    Aborted { reason: AbortReason },
}

/// Reason for transaction abort
#[derive(Debug, Clone)]
pub enum AbortReason {
    Deadlock,
    WriteConflict,
    SerializationFailure,
    UserRequested,
    Timeout,
}

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Active,
    Preparing,
    Committed,
    Aborted,
}

/// Transaction state
#[derive(Debug)]
pub struct Transaction {
    pub id: TxnId,
    pub isolation: IsolationLevel,
    pub status: TransactionStatus,
    pub start_ts: TxnId,
    pub commit_ts: Option<TxnId>,
    pub read_set: Vec<(TableId, RowId)>,
    pub write_set: Vec<(TableId, RowId, WriteType)>,
}

/// Type of write operation
#[derive(Debug, Clone)]
pub enum WriteType {
    Insert(Row),
    Update(Row),
    Delete,
}

impl Transaction {
    pub fn new(id: TxnId, isolation: IsolationLevel) -> Self {
        Self {
            id,
            isolation,
            status: TransactionStatus::Active,
            start_ts: id,
            commit_ts: None,
            read_set: Vec::new(),
            write_set: Vec::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        self.status == TransactionStatus::Active
    }

    pub fn add_read(&mut self, table_id: TableId, row_id: RowId) {
        self.read_set.push((table_id, row_id));
    }

    pub fn add_write(&mut self, table_id: TableId, row_id: RowId, write_type: WriteType) {
        self.write_set.push((table_id, row_id, write_type));
    }
}

/// Deadlock detector trait
pub trait DeadlockDetector: Send + Sync {
    /// Add a wait edge: waiter is waiting for holder
    fn add_wait(&self, waiter: TxnId, holder: TxnId);

    /// Remove wait edges for a transaction
    fn remove_wait(&self, txn_id: TxnId);

    /// Detect deadlock and return victim transaction if found
    fn detect(&self) -> Option<TxnId>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_new() {
        let txn = Transaction::new(TxnId(1), IsolationLevel::Serializable);
        assert_eq!(txn.id, TxnId(1));
        assert!(txn.is_active());
        assert!(txn.read_set.is_empty());
        assert!(txn.write_set.is_empty());
    }
}
