//! # Thunder Storage
//!
//! Storage engine for ThunderDB providing:
//! - Page-based storage with buffer pool management
//! - Write-ahead logging (WAL) for durability
//! - B+Tree indexes optimized for flash storage
//! - Row store for OLTP workloads
//! - Column store for OLAP workloads

pub mod buffer;
pub mod btree;
pub mod catalog;
pub mod column_store;
pub mod compression;
pub mod page;
pub mod row_store;
pub mod wal;

// Re-export key types for convenience
pub use buffer::{BufferPoolImpl, DiskManager, PageEncryptor, PageReadGuard, PageWriteGuard};
pub use btree::{BTree, BTreeConfig, BTreeNode, BTreeStats};
pub use catalog::{Catalog, ColumnDef, ForeignKeyAction, ForeignKeyConstraint, IndexDescriptor, TableDescriptor};
pub use column_store::{ColumnStoreImpl, ColumnStatistics, SegmentMetadata, WriteBuffer};
pub use compression::{create_codec, CompressionCodec, CompressionStats};
pub use page::{Page, PageHeader, PageType, SlotEntry, TupleHeader, TUPLE_HEADER_SIZE};
pub use row_store::{FreeSpaceMap, RowStoreImpl, Snapshot, VacuumStats};
pub use wal::{
    WalReader, WalRecordHeader, WalRecordType, WalWriterImpl,
    WalRecovery, RecoveryPlan, RecoveryExecutor, RecoveryStats,
    RedoRecord, UndoRecord, AnalysisResult, RecoveryTxnState,
    CreateTableRecord, DropTableRecord,
};

use async_trait::async_trait;
use thunder_common::prelude::*;

/// Page size constant (16KB default)
pub const PAGE_SIZE: usize = 16 * 1024;

/// Buffer pool trait for managing pages in memory
#[async_trait]
pub trait BufferPool: Send + Sync {
    /// Fetch a page, loading from disk if not cached
    async fn fetch_page(&self, page_id: PageId) -> Result<PageGuard>;

    /// Allocate a new page
    async fn new_page(&self) -> Result<(PageId, PageGuard)>;

    /// Flush all dirty pages to disk
    async fn flush(&self) -> Result<()>;

    /// Flush a specific page to disk
    async fn flush_page(&self, page_id: PageId) -> Result<()>;

    /// Get buffer pool statistics
    fn stats(&self) -> BufferPoolStats;
}

/// Guard for page access with automatic release
pub struct PageGuard {
    // Will hold Arc<Page> and drop logic
    _private: (),
}

/// Buffer pool statistics
#[derive(Debug, Clone, Default)]
pub struct BufferPoolStats {
    pub total_pages: usize,
    pub used_pages: usize,
    pub dirty_pages: usize,
    pub hits: u64,
    pub misses: u64,
}

/// Write-ahead log trait
#[async_trait]
pub trait WalWriter: Send + Sync {
    /// Append an entry to the WAL
    async fn append(&self, entry: WalEntry) -> Result<Lsn>;

    /// Append multiple entries to the WAL with a single sync (group commit)
    /// Returns the LSN of the last entry
    async fn append_batch(&self, entries: Vec<WalEntry>) -> Result<Vec<Lsn>> {
        // Default implementation: append each entry individually
        let mut lsns = Vec::with_capacity(entries.len());
        for entry in entries {
            lsns.push(self.append(entry).await?);
        }
        Ok(lsns)
    }

    /// Sync WAL to durable storage
    async fn sync(&self) -> Result<()>;

    /// Truncate WAL entries before the given LSN
    async fn truncate_before(&self, lsn: Lsn) -> Result<()>;

    /// Get the current LSN
    fn current_lsn(&self) -> Lsn;
}

/// WAL entry types
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub lsn: Lsn,
    pub txn_id: TxnId,
    pub entry_type: WalEntryType,
    pub table_id: TableId,
    pub data: Vec<u8>,
}

/// Types of WAL entries
#[derive(Debug, Clone)]
pub enum WalEntryType {
    Insert { row_id: RowId },
    Update { row_id: RowId },
    Delete { row_id: RowId },
    Commit,
    Abort,
    Checkpoint { active_txns: Vec<TxnId> },
    /// Create a new table
    CreateTable { table_name: String, schema_name: String },
    /// Drop an existing table
    DropTable { table_name: String },
}

/// Row store trait for OLTP operations
#[async_trait]
pub trait RowStore: Send + Sync {
    /// Insert a row and return its ID
    async fn insert(&self, table_id: TableId, txn_id: TxnId, row: Row) -> Result<RowId>;

    /// Get a row by ID
    async fn get(&self, table_id: TableId, txn_id: TxnId, row_id: RowId) -> Result<Option<Row>>;

    /// Update a row
    async fn update(&self, table_id: TableId, txn_id: TxnId, row_id: RowId, row: Row) -> Result<()>;

    /// Delete a row
    async fn delete(&self, table_id: TableId, txn_id: TxnId, row_id: RowId) -> Result<()>;

    /// Scan all rows in a table
    async fn scan(&self, table_id: TableId, txn_id: TxnId) -> Result<Box<dyn RowIterator>>;
}

/// Iterator over rows
pub trait RowIterator: Send {
    fn next(&mut self) -> Option<Result<(RowId, Row)>>;
}

/// Column store trait for OLAP operations
#[async_trait]
pub trait ColumnStore: Send + Sync {
    /// Append a batch of rows
    async fn append_batch(&self, table_id: TableId, batch: arrow::record_batch::RecordBatch) -> Result<()>;

    /// Scan columns with optional predicate
    async fn scan(
        &self,
        table_id: TableId,
        columns: &[ColumnId],
        predicate: Option<&str>,
    ) -> Result<arrow::record_batch::RecordBatch>;

    /// Compact and merge segments
    async fn compact(&self) -> Result<()>;
}

/// B+Tree index trait
#[async_trait]
pub trait BTreeIndex: Send + Sync {
    /// Insert a key-value pair
    async fn insert(&self, key: &[u8], value: RowId) -> Result<()>;

    /// Get value by key
    async fn get(&self, key: &[u8]) -> Result<Option<RowId>>;

    /// Delete a key
    async fn delete(&self, key: &[u8]) -> Result<bool>;

    /// Range scan
    async fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, RowId)>>;

    /// Scan all entries
    async fn scan(&self) -> Result<Box<dyn Iterator<Item = (Vec<u8>, RowId)> + Send>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_size() {
        assert_eq!(PAGE_SIZE, 16384);
    }
}
