//! Transaction Support
//!
//! Provides transaction management with isolation levels and savepoints.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use tracing::{debug, warn};

use thunder_common::error::TransactionError;
use thunder_common::prelude::*;

use crate::pool::PooledConnection;
use crate::statement::PreparedStatement;
use crate::QueryResult;

// ============================================================================
// Isolation Level
// ============================================================================

/// Transaction isolation level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

// ============================================================================
// Transaction Options
// ============================================================================

/// Transaction configuration options
#[derive(Debug, Clone, Default)]
pub struct TransactionOptions {
    pub isolation_level: IsolationLevel,
    pub read_only: bool,
    pub deferrable: bool,
}

impl TransactionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    pub fn deferrable(mut self, deferrable: bool) -> Self {
        self.deferrable = deferrable;
        self
    }
}

// ============================================================================
// Transaction
// ============================================================================

/// A database transaction
pub struct Transaction<'a> {
    conn: &'a mut PooledConnection,
    id: String,
    finished: AtomicBool,
    savepoint_counter: AtomicU32,
}

impl<'a> Transaction<'a> {
    pub async fn begin(conn: &'a mut PooledConnection) -> Result<Transaction<'a>> {
        Self::begin_with_options(conn, TransactionOptions::default()).await
    }

    pub async fn begin_with_options(
        conn: &'a mut PooledConnection,
        options: TransactionOptions,
    ) -> Result<Transaction<'a>> {
        let id = uuid::Uuid::new_v4().to_string();
        let sql = build_begin_sql(&options);

        debug!(txn_id = %id, sql = %sql, "Beginning transaction");
        conn.execute(&sql).await?;

        Ok(Transaction {
            conn,
            id,
            finished: AtomicBool::new(false),
            savepoint_counter: AtomicU32::new(0),
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        if self.finished.load(Ordering::Relaxed) {
            return Err(Error::Transaction(TransactionError::NotFound(0)));
        }
        self.conn.query(sql).await
    }

    pub async fn execute(&mut self, sql: &str) -> Result<u64> {
        if self.finished.load(Ordering::Relaxed) {
            return Err(Error::Transaction(TransactionError::NotFound(0)));
        }
        self.conn.execute(sql).await
    }

    pub async fn execute_prepared(&mut self, stmt: &PreparedStatement, params: &[Value]) -> Result<u64> {
        if self.finished.load(Ordering::Relaxed) {
            return Err(Error::Transaction(TransactionError::NotFound(0)));
        }
        stmt.execute(self.conn, params).await
    }

    pub async fn query_prepared(&mut self, stmt: &PreparedStatement, params: &[Value]) -> Result<QueryResult> {
        if self.finished.load(Ordering::Relaxed) {
            return Err(Error::Transaction(TransactionError::NotFound(0)));
        }
        stmt.query(self.conn, params).await
    }

    pub async fn savepoint(&mut self, name: &str) -> Result<Savepoint<'_, 'a>> {
        if self.finished.load(Ordering::Relaxed) {
            return Err(Error::Transaction(TransactionError::NotFound(0)));
        }

        let sql = format!("SAVEPOINT {}", name);
        self.conn.execute(&sql).await?;
        self.savepoint_counter.fetch_add(1, Ordering::Relaxed);

        debug!(txn_id = %self.id, savepoint = %name, "Savepoint created");

        Ok(Savepoint {
            transaction: self,
            name: name.to_string(),
            released: AtomicBool::new(false),
        })
    }

    pub async fn commit(self) -> Result<()> {
        if self.finished.swap(true, Ordering::Relaxed) {
            return Err(Error::Transaction(TransactionError::AlreadyCommitted(0)));
        }

        debug!(txn_id = %self.id, "Committing transaction");
        self.conn.execute("COMMIT").await?;
        std::mem::forget(self);
        Ok(())
    }

    pub async fn rollback(self) -> Result<()> {
        if self.finished.swap(true, Ordering::Relaxed) {
            return Err(Error::Transaction(TransactionError::AlreadyAborted(0)));
        }

        debug!(txn_id = %self.id, "Rolling back transaction");
        self.conn.execute("ROLLBACK").await?;
        std::mem::forget(self);
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        !self.finished.load(Ordering::Relaxed)
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            warn!(txn_id = %self.id, "Transaction dropped without commit/rollback");
            self.finished.store(true, Ordering::Relaxed);
        }
    }
}

// ============================================================================
// Savepoint
// ============================================================================

pub struct Savepoint<'t, 'c> {
    transaction: &'t mut Transaction<'c>,
    name: String,
    released: AtomicBool,
}

impl<'t, 'c> Savepoint<'t, 'c> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.transaction.query(sql).await
    }

    pub async fn execute(&mut self, sql: &str) -> Result<u64> {
        self.transaction.execute(sql).await
    }

    pub async fn release(self) -> Result<()> {
        if self.released.swap(true, Ordering::Relaxed) {
            return Err(Error::Transaction(TransactionError::AlreadyCommitted(0)));
        }

        let sql = format!("RELEASE SAVEPOINT {}", self.name);
        self.transaction.conn.execute(&sql).await?;
        debug!(savepoint = %self.name, "Savepoint released");
        std::mem::forget(self);
        Ok(())
    }

    pub async fn rollback(self) -> Result<()> {
        if self.released.swap(true, Ordering::Relaxed) {
            return Err(Error::Transaction(TransactionError::AlreadyAborted(0)));
        }

        let sql = format!("ROLLBACK TO SAVEPOINT {}", self.name);
        self.transaction.conn.execute(&sql).await?;
        debug!(savepoint = %self.name, "Savepoint rolled back");
        std::mem::forget(self);
        Ok(())
    }
}

impl<'t, 'c> Drop for Savepoint<'t, 'c> {
    fn drop(&mut self) {
        if !self.released.load(Ordering::Relaxed) {
            warn!(savepoint = %self.name, "Savepoint dropped without release/rollback");
            self.released.store(true, Ordering::Relaxed);
        }
    }
}

// ============================================================================
// Transaction Builder
// ============================================================================

pub struct TransactionBuilder {
    options: TransactionOptions,
}

impl TransactionBuilder {
    pub fn new() -> Self {
        Self {
            options: TransactionOptions::default(),
        }
    }

    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.options.isolation_level = level;
        self
    }

    pub fn read_only(mut self) -> Self {
        self.options.read_only = true;
        self
    }

    pub fn deferrable(mut self) -> Self {
        self.options.deferrable = true;
        self
    }

    pub async fn begin<'a>(self, conn: &'a mut PooledConnection) -> Result<Transaction<'a>> {
        Transaction::begin_with_options(conn, self.options).await
    }
}

impl Default for TransactionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn build_begin_sql(options: &TransactionOptions) -> String {
    let isolation = match options.isolation_level {
        IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
        IsolationLevel::ReadCommitted => "READ COMMITTED",
        IsolationLevel::RepeatableRead => "REPEATABLE READ",
        IsolationLevel::Serializable => "SERIALIZABLE",
    };

    let mut parts = vec!["BEGIN", "ISOLATION LEVEL", isolation];

    if options.read_only {
        parts.push("READ ONLY");
    } else {
        parts.push("READ WRITE");
    }

    if options.deferrable && options.read_only && options.isolation_level == IsolationLevel::Serializable {
        parts.push("DEFERRABLE");
    }

    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_begin_sql() {
        let opts = TransactionOptions::default();
        assert_eq!(build_begin_sql(&opts), "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE");

        let opts = TransactionOptions::new()
            .isolation_level(IsolationLevel::Serializable)
            .read_only(true);
        assert_eq!(build_begin_sql(&opts), "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY");
    }

    #[test]
    fn test_transaction_builder() {
        let builder = TransactionBuilder::new()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only();

        assert_eq!(builder.options.isolation_level, IsolationLevel::RepeatableRead);
        assert!(builder.options.read_only);
    }
}
