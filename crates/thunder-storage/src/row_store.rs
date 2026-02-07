//! Row store implementation for ThunderDB.
//!
//! Provides MVCC-aware row storage for OLTP workloads:
//! - Tuple-level visibility using xmin/xmax
//! - Snapshot isolation
//! - Free space management

use crate::buffer::BufferPoolImpl;
use crate::page::{TupleHeader, TUPLE_HEADER_SIZE};
use crate::wal::{WalWriterImpl, InsertRecord, DeleteRecord};
use crate::{RowIterator, WalEntry, WalEntryType, WalWriter};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thunder_common::prelude::*;

/// Snapshot for MVCC visibility checks.
/// Uses HashSet internally for O(1) visibility lookups instead of O(n) Vec::contains.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction ID of the snapshot creator
    pub txn_id: TxnId,
    /// Minimum active transaction ID at snapshot time
    pub xmin: TxnId,
    /// Maximum committed transaction ID at snapshot time
    pub xmax: TxnId,
    /// Set of active (uncommitted) transaction IDs at snapshot time (Vec for compatibility)
    pub active_txns: Vec<TxnId>,
    /// HashSet for O(1) visibility checks (lazily built from active_txns)
    #[doc(hidden)]
    active_txns_set: HashSet<TxnId>,
}

impl Snapshot {
    /// Create a new snapshot.
    pub fn new(txn_id: TxnId, xmin: TxnId, xmax: TxnId, active_txns: Vec<TxnId>) -> Self {
        // Build HashSet for O(1) lookups
        let active_txns_set: HashSet<TxnId> = active_txns.iter().copied().collect();
        Self {
            txn_id,
            xmin,
            xmax,
            active_txns,
            active_txns_set,
        }
    }

    /// Check if a transaction ID is in the active set (O(1) lookup).
    #[inline]
    fn is_active(&self, txn_id: TxnId) -> bool {
        self.active_txns_set.contains(&txn_id)
    }

    /// Check if a transaction is visible to this snapshot.
    /// Optimized with O(1) HashSet lookups for active transaction checks.
    #[inline]
    pub fn is_visible(&self, tuple_xmin: TxnId, tuple_xmax: TxnId) -> bool {
        // Tuple is visible if:
        // 1. xmin is committed and < our xmax
        // 2. xmin is not in our active set
        // 3. xmax is 0 (not deleted) OR xmax > our xmax OR xmax is in our active set

        // Check if xmin is visible
        if tuple_xmin.0 == 0 {
            return false; // Never inserted
        }

        // Check if inserting transaction is visible
        if tuple_xmin == self.txn_id {
            // Our own insert - visible only if not deleted by us
            if tuple_xmax.0 == 0 {
                return true; // Not deleted
            }
            return false; // Deleted (by us or someone else)
        }

        if tuple_xmin >= self.xmax {
            return false; // Inserted by future transaction
        }

        // O(1) lookup instead of O(n) Vec::contains
        if self.is_active(tuple_xmin) {
            return false; // Inserted by still-active transaction
        }

        // Check if tuple is deleted
        if tuple_xmax.0 == 0 {
            return true; // Not deleted
        }

        if tuple_xmax == self.txn_id {
            return false; // We deleted it
        }

        if tuple_xmax >= self.xmax {
            return true; // Deleted by future transaction
        }

        // O(1) lookup instead of O(n) Vec::contains
        if self.is_active(tuple_xmax) {
            return true; // Deleted by still-active transaction
        }

        false // Deleted by committed transaction
    }
}

/// Free space map for tracking page free space.
#[derive(Debug, Default)]
pub struct FreeSpaceMap {
    /// Page ID to free space bytes mapping
    entries: DashMap<PageId, u16>,
}

impl FreeSpaceMap {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Update free space for a page.
    pub fn update(&self, page_id: PageId, free_space: u16) {
        self.entries.insert(page_id, free_space);
    }

    /// Find a page with at least `needed` bytes of free space.
    pub fn find_page(&self, needed: u16) -> Option<PageId> {
        self.entries
            .iter()
            .find(|entry| *entry.value() >= needed)
            .map(|entry| *entry.key())
    }

    /// Remove a page from the FSM.
    pub fn remove(&self, page_id: PageId) {
        self.entries.remove(&page_id);
    }
}

/// Row store implementation.
pub struct RowStoreImpl {
    /// Buffer pool for page access
    buffer_pool: Arc<BufferPoolImpl>,
    /// WAL writer for durability
    wal: Arc<WalWriterImpl>,
    /// Free space map per table
    table_fsm: DashMap<TableId, FreeSpaceMap>,
    /// First page ID per table
    table_first_page: DashMap<TableId, PageId>,
    /// Next row ID per table
    next_row_id: DashMap<TableId, AtomicU64>,
}

impl RowStoreImpl {
    /// Create a new row store.
    pub fn new(buffer_pool: Arc<BufferPoolImpl>, wal: Arc<WalWriterImpl>) -> Self {
        Self {
            buffer_pool,
            wal,
            table_fsm: DashMap::new(),
            table_first_page: DashMap::new(),
            next_row_id: DashMap::new(),
        }
    }

    /// Get or create the first page for a table.
    fn get_or_create_first_page(&self, table_id: TableId) -> Result<PageId> {
        if let Some(page_id) = self.table_first_page.get(&table_id) {
            return Ok(*page_id);
        }

        // Create first page for table
        let (page_id, _guard) = self.buffer_pool.new_page()?;
        self.table_first_page.insert(table_id, page_id);
        self.table_fsm
            .entry(table_id)
            .or_insert_with(FreeSpaceMap::new);
        self.next_row_id
            .entry(table_id)
            .or_insert_with(|| AtomicU64::new(1));

        Ok(page_id)
    }

    /// Allocate the next row ID for a table.
    fn allocate_row_id(&self, table_id: TableId) -> RowId {
        let counter = self
            .next_row_id
            .entry(table_id)
            .or_insert_with(|| AtomicU64::new(1));
        RowId(counter.fetch_add(1, Ordering::SeqCst))
    }

    /// Encode a row to bytes with tuple header.
    fn encode_row(&self, txn_id: TxnId, row: &Row) -> Bytes {
        let row_data = bincode::serialize(row).unwrap_or_default();
        let mut buf = BytesMut::with_capacity(TUPLE_HEADER_SIZE + row_data.len());

        // Write tuple header
        let header = TupleHeader {
            xmin: txn_id.0,
            xmax: 0, // Not deleted
            null_bitmap: 0,
        };

        // Pre-allocate header buffer
        let mut header_buf = [0u8; TUPLE_HEADER_SIZE];
        header.write_to(&mut header_buf);
        buf.extend_from_slice(&header_buf);

        // Write row data
        buf.extend_from_slice(&row_data);

        buf.freeze()
    }

    /// Decode a row from bytes.
    fn decode_row(&self, data: &[u8]) -> Result<(TupleHeader, Row)> {
        if data.len() < TUPLE_HEADER_SIZE {
            return Err(Error::Internal("Tuple too short".into()));
        }

        let header = TupleHeader::read_from(data);
        let row_data = &data[TUPLE_HEADER_SIZE..];

        let row: Row = bincode::deserialize(row_data).map_err(|e| {
            Error::Internal(format!("Failed to deserialize row: {}", e))
        })?;

        Ok((header, row))
    }

    /// Insert a row.
    pub async fn insert(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        row: Row,
    ) -> Result<RowId> {
        let row_id = self.allocate_row_id(table_id);
        let tuple_data = self.encode_row(txn_id, &row);
        let needed_space = tuple_data.len() as u16;

        // Find or create a page with enough space
        let page_id = if let Some(fsm) = self.table_fsm.get(&table_id) {
            if let Some(page_id) = fsm.find_page(needed_space) {
                page_id
            } else {
                // Need a new page
                let (page_id, _) = self.buffer_pool.new_page()?;
                fsm.update(page_id, crate::PAGE_SIZE as u16);
                page_id
            }
        } else {
            self.get_or_create_first_page(table_id)?
        };

        // Insert tuple into page
        let mut guard = self.buffer_pool.fetch_page_mut(page_id)?;
        let slot_id = guard.page_mut().insert_tuple(&tuple_data)?;

        // Update free space map
        if let Some(fsm) = self.table_fsm.get(&table_id) {
            fsm.update(page_id, guard.free_space() as u16);
        }

        drop(guard);

        // Write WAL record with full InsertRecord format for recovery
        let insert_record = InsertRecord {
            row_id,
            page_id,
            slot_id,
            tuple_data: tuple_data.clone(),
        };

        let wal_entry = WalEntry {
            lsn: Lsn(0), // Will be assigned by WAL
            txn_id,
            entry_type: WalEntryType::Insert { row_id },
            table_id,
            data: insert_record.encode().to_vec(),
        };
        self.wal.append(wal_entry).await?;

        Ok(row_id)
    }

    /// Insert multiple rows efficiently with group commit.
    /// This is significantly faster than inserting rows one at a time because:
    /// 1. All WAL entries are written to buffer first
    /// 2. Single fsync for all entries (group commit)
    /// 3. Reduced lock contention on pages
    pub async fn insert_batch(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        rows: Vec<Row>,
    ) -> Result<Vec<RowId>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let batch_size = rows.len();
        let mut row_ids = Vec::with_capacity(batch_size);
        let mut wal_entries = Vec::with_capacity(batch_size);

        // Pre-allocate pages for the batch to reduce allocation overhead
        let avg_row_size = 100; // Estimate, will be refined
        let rows_per_page = (crate::PAGE_SIZE - 100) / avg_row_size;
        let pages_needed = (batch_size + rows_per_page - 1) / rows_per_page;

        // Ensure we have enough pages pre-allocated
        let mut available_pages = Vec::with_capacity(pages_needed);
        for _ in 0..pages_needed {
            let fsm = self.table_fsm
                .entry(table_id)
                .or_insert_with(FreeSpaceMap::new);

            let page_id = if let Some(pid) = fsm.find_page(avg_row_size as u16) {
                pid
            } else {
                let (page_id, _) = self.buffer_pool.new_page()?;
                fsm.update(page_id, crate::PAGE_SIZE as u16);
                page_id
            };
            available_pages.push(page_id);
        }

        // Insert all rows, collecting WAL entries
        let mut page_idx = 0;
        for row in rows {
            let row_id = self.allocate_row_id(table_id);
            let tuple_data = self.encode_row(txn_id, &row);
            let needed_space = tuple_data.len() as u16;

            // Find a page with enough space from our pre-allocated pool
            let page_id = loop {
                if page_idx >= available_pages.len() {
                    // Need more pages
                    let (new_page_id, _) = self.buffer_pool.new_page()?;
                    if let Some(fsm) = self.table_fsm.get(&table_id) {
                        fsm.update(new_page_id, crate::PAGE_SIZE as u16);
                    }
                    available_pages.push(new_page_id);
                }

                let pid = available_pages[page_idx];
                let guard = self.buffer_pool.fetch_page(pid)?;
                if guard.free_space() as u16 >= needed_space {
                    break pid;
                }
                page_idx += 1;
            };

            // Insert tuple into page
            let mut guard = self.buffer_pool.fetch_page_mut(page_id)?;
            let slot_id = guard.page_mut().insert_tuple(&tuple_data)?;

            // Update free space map
            if let Some(fsm) = self.table_fsm.get(&table_id) {
                fsm.update(page_id, guard.free_space() as u16);
            }

            drop(guard);

            // Create WAL entry (don't append yet)
            let insert_record = InsertRecord {
                row_id,
                page_id,
                slot_id,
                tuple_data: tuple_data.clone(),
            };

            wal_entries.push(WalEntry {
                lsn: Lsn(0), // Will be assigned by WAL
                txn_id,
                entry_type: WalEntryType::Insert { row_id },
                table_id,
                data: insert_record.encode().to_vec(),
            });

            row_ids.push(row_id);
        }

        // Group commit: write all WAL entries with single sync
        self.wal.append_batch(wal_entries).await?;

        Ok(row_ids)
    }

    /// Get a row by ID with visibility check.
    pub async fn get(
        &self,
        table_id: TableId,
        snapshot: &Snapshot,
        row_id: RowId,
    ) -> Result<Option<Row>> {
        // For simplicity, scan all pages to find the row
        // In a real implementation, we'd use an index or row ID map
        let first_page_id = match self.table_first_page.get(&table_id) {
            Some(p) => *p,
            None => return Ok(None),
        };

        // Scan pages (simplified - in reality we'd use a row locator)
        let mut current_page_id = first_page_id;
        loop {
            let guard = self.buffer_pool.fetch_page(current_page_id)?;

            // Scan tuples in page
            for slot_id in 0..guard.slot_count() {
                if let Some(tuple_data) = guard.get_tuple(slot_id) {
                    let (header, row) = self.decode_row(tuple_data)?;

                    // Check visibility
                    if snapshot.is_visible(TxnId(header.xmin), TxnId(header.xmax)) {
                        // Check if this is the row we're looking for
                        // (In a real impl, row_id would be stored in the tuple)
                        return Ok(Some(row));
                    }
                }
            }

            // Move to next page (simplified - no linked list yet)
            break;
        }

        Ok(None)
    }

    /// Get multiple rows by IDs with visibility check - batch optimized.
    /// This is significantly faster than calling get() for each row because:
    /// 1. Single page scan covers multiple row lookups
    /// 2. Reduced lock acquisition overhead
    /// 3. Better cache utilization
    pub async fn get_batch(
        &self,
        table_id: TableId,
        snapshot: &Snapshot,
        row_ids: &[RowId],
    ) -> Result<Vec<(RowId, Row)>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Build a set of requested row IDs for O(1) lookup
        let requested: HashSet<RowId> = row_ids.iter().copied().collect();
        let mut results = Vec::with_capacity(row_ids.len());

        let first_page_id = match self.table_first_page.get(&table_id) {
            Some(p) => *p,
            None => return Ok(results),
        };

        // Scan pages once, collecting all matching rows
        let mut current_page_id = first_page_id;
        loop {
            let guard = self.buffer_pool.fetch_page(current_page_id)?;

            // Scan tuples in page
            for slot_id in 0..guard.slot_count() {
                if let Some(tuple_data) = guard.get_tuple(slot_id) {
                    let (header, row) = self.decode_row(tuple_data)?;

                    // Create row_id from slot (simplified)
                    let row_id = RowId(slot_id as u64);

                    // Check if this row is requested and visible
                    if requested.contains(&row_id)
                        && snapshot.is_visible(TxnId(header.xmin), TxnId(header.xmax))
                    {
                        results.push((row_id, row));

                        // Early exit if we found all requested rows
                        if results.len() == row_ids.len() {
                            return Ok(results);
                        }
                    }
                }
            }

            // Move to next page (simplified - no linked list yet)
            break;
        }

        Ok(results)
    }

    /// Update a row.
    pub async fn update(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        row_id: RowId,
        new_row: Row,
    ) -> Result<()> {
        // In MVCC, update = delete old + insert new
        // First, find and mark the old row as deleted
        self.delete(table_id, txn_id, row_id).await?;

        // Insert new version
        let new_row_id = self.insert(table_id, txn_id, new_row).await?;

        // Write WAL record
        let wal_entry = WalEntry {
            lsn: Lsn(0),
            txn_id,
            entry_type: WalEntryType::Update { row_id: new_row_id },
            table_id,
            data: vec![],
        };
        self.wal.append(wal_entry).await?;

        Ok(())
    }

    /// Delete a row by setting xmax.
    pub async fn delete(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        row_id: RowId,
    ) -> Result<()> {
        // Find the row and set xmax
        let first_page_id = match self.table_first_page.get(&table_id) {
            Some(p) => *p,
            None => return Err(Error::NotFound("Table".into(), table_id.0.to_string())),
        };

        let mut guard = self.buffer_pool.fetch_page_mut(first_page_id)?;

        // Find the tuple (simplified)
        for slot_id in 0..guard.slot_count() {
            if let Some(_) = guard.get_tuple(slot_id) {
                // Set xmax to mark as deleted
                guard.page_mut().set_xmax(slot_id, txn_id)?;
                break;
            }
        }

        drop(guard);

        // Write WAL record
        let wal_entry = WalEntry {
            lsn: Lsn(0),
            txn_id,
            entry_type: WalEntryType::Delete { row_id },
            table_id,
            data: vec![],
        };
        self.wal.append(wal_entry).await?;

        Ok(())
    }

    /// Scan all visible rows in a table.
    pub fn scan(
        &self,
        table_id: TableId,
        snapshot: &Snapshot,
    ) -> Result<RowStoreIterator> {
        let first_page_id = self.table_first_page.get(&table_id).map(|p| *p);

        Ok(RowStoreIterator {
            buffer_pool: Arc::clone(&self.buffer_pool),
            current_page_id: first_page_id,
            current_slot: 0,
            snapshot: snapshot.clone(),
        })
    }

    /// Vacuum a table, removing dead tuples and reclaiming space.
    ///
    /// A tuple is "dead" and safe to remove if:
    /// 1. It has xmax set (was deleted)
    /// 2. The deleting transaction has committed (xmax is not in active transactions)
    /// 3. No active transaction can see the deleted version (xmax < oldest_active_txn)
    ///
    /// # Arguments
    /// * `table_id` - The table to vacuum
    /// * `oldest_active_txn` - The oldest active transaction ID (from transaction manager)
    ///
    /// # Returns
    /// Statistics about the vacuum operation
    pub fn vacuum(&self, table_id: TableId, oldest_active_txn: TxnId) -> Result<VacuumStats> {
        let mut stats = VacuumStats::default();

        let first_page_id = match self.table_first_page.get(&table_id) {
            Some(p) => *p,
            None => return Ok(stats), // No pages for this table
        };

        // Track pages to process (for multi-page tables, we'd iterate through linked list)
        let mut pages_to_process = vec![first_page_id];

        while let Some(page_id) = pages_to_process.pop() {
            stats.pages_scanned += 1;

            let mut guard = self.buffer_pool.fetch_page_mut(page_id)?;
            let page = guard.page_mut();

            let mut tuples_to_delete = Vec::new();

            // First pass: identify dead tuples
            for slot_id in 0..page.slot_count() {
                if let Some(tuple_data) = page.get_tuple(slot_id) {
                    if tuple_data.len() < TUPLE_HEADER_SIZE {
                        continue;
                    }

                    let header = TupleHeader::read_from(tuple_data);

                    // A tuple is dead if:
                    // 1. xmax != 0 (it was deleted)
                    // 2. xmax < oldest_active_txn (no active transaction can see it)
                    if header.xmax != 0 && TxnId(header.xmax) < oldest_active_txn {
                        tuples_to_delete.push(slot_id);
                        stats.tuples_removed += 1;
                        stats.bytes_freed += tuple_data.len() as u64;
                    }
                }
            }

            // Second pass: mark dead tuples as deleted in slot directory
            for slot_id in tuples_to_delete {
                let _ = page.delete_tuple(slot_id);
            }

            // Compact page if it has significant dead space
            if page.needs_compaction() {
                page.compact();
                stats.pages_compacted += 1;
            }

            // Update free space map
            if let Some(fsm) = self.table_fsm.get(&table_id) {
                fsm.update(page_id, page.free_space() as u16);
            }

            // Note: In a full implementation, we'd follow page links to process all pages
            // For now, we only process the first page
        }

        Ok(stats)
    }

    /// Get list of all table IDs that have data.
    pub fn table_ids(&self) -> Vec<TableId> {
        self.table_first_page.iter().map(|e| *e.key()).collect()
    }

    /// Get the buffer pool.
    pub fn buffer_pool(&self) -> &Arc<BufferPoolImpl> {
        &self.buffer_pool
    }

    /// Register a page for a table during recovery.
    /// This is used to rebuild the table_first_page mapping after WAL recovery.
    pub fn register_page_for_table(&self, table_id: TableId, page_id: PageId) {
        // Only register if this table doesn't already have a first page
        self.table_first_page.entry(table_id).or_insert(page_id);
        // Initialize FSM for the table
        self.table_fsm.entry(table_id).or_insert_with(FreeSpaceMap::new);
        // Initialize next_row_id counter
        self.next_row_id.entry(table_id).or_insert_with(|| std::sync::atomic::AtomicU64::new(1));
    }

    /// Update the next row ID for a table during recovery.
    /// This ensures we don't reuse row IDs after recovery.
    pub fn update_next_row_id(&self, table_id: TableId, row_id: RowId) {
        if let Some(counter) = self.next_row_id.get(&table_id) {
            // Ensure next_row_id is at least row_id + 1
            let current = counter.load(std::sync::atomic::Ordering::SeqCst);
            if row_id.0 >= current {
                counter.store(row_id.0 + 1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
}

/// Statistics from a vacuum operation.
#[derive(Debug, Clone, Default)]
pub struct VacuumStats {
    /// Number of pages scanned
    pub pages_scanned: u64,
    /// Number of pages that were compacted
    pub pages_compacted: u64,
    /// Number of dead tuples removed
    pub tuples_removed: u64,
    /// Total bytes freed
    pub bytes_freed: u64,
}

/// Iterator over rows in the row store.
pub struct RowStoreIterator {
    buffer_pool: Arc<BufferPoolImpl>,
    current_page_id: Option<PageId>,
    current_slot: u16,
    snapshot: Snapshot,
}

impl RowIterator for RowStoreIterator {
    fn next(&mut self) -> Option<Result<(RowId, Row)>> {
        let page_id = self.current_page_id?;

        let guard = match self.buffer_pool.fetch_page(page_id) {
            Ok(g) => g,
            Err(e) => return Some(Err(e)),
        };

        while self.current_slot < guard.slot_count() {
            let slot_id = self.current_slot;
            self.current_slot += 1;

            if let Some(tuple_data) = guard.get_tuple(slot_id) {
                if tuple_data.len() < TUPLE_HEADER_SIZE {
                    continue;
                }

                let header = TupleHeader::read_from(tuple_data);
                if !self
                    .snapshot
                    .is_visible(TxnId(header.xmin), TxnId(header.xmax))
                {
                    continue;
                }

                // Decode row
                let row_data = &tuple_data[TUPLE_HEADER_SIZE..];
                match bincode::deserialize::<Row>(row_data) {
                    Ok(row) => {
                        // Generate a row ID (simplified)
                        let row_id = RowId(slot_id as u64);
                        return Some(Ok((row_id, row)));
                    }
                    Err(e) => {
                        return Some(Err(Error::Internal(format!(
                            "Failed to deserialize row: {}",
                            e
                        ))))
                    }
                }
            }
        }

        // No more tuples in current page
        self.current_page_id = None;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::DiskManager;
    use tempfile::tempdir;
    use thunder_common::types::Value;

    fn create_test_row_store() -> (RowStoreImpl, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("wal");

        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = Arc::new(BufferPoolImpl::new(100, dm));
        let wal = Arc::new(
            WalWriterImpl::new(&wal_path, true).unwrap()
        );

        let store = RowStoreImpl::new(pool, wal);
        (store, dir)
    }

    fn create_test_snapshot() -> Snapshot {
        Snapshot::new(TxnId(1), TxnId(0), TxnId(100), vec![])
    }

    #[test]
    fn test_snapshot_visibility() {
        let snapshot = Snapshot::new(TxnId(10), TxnId(1), TxnId(20), vec![TxnId(5)]);

        // Visible: committed transaction before our xmax
        assert!(snapshot.is_visible(TxnId(2), TxnId(0)));

        // Not visible: future transaction
        assert!(!snapshot.is_visible(TxnId(25), TxnId(0)));

        // Not visible: active transaction
        assert!(!snapshot.is_visible(TxnId(5), TxnId(0)));

        // Visible: our own insert
        assert!(snapshot.is_visible(TxnId(10), TxnId(0)));

        // Not visible: deleted by committed transaction
        assert!(!snapshot.is_visible(TxnId(2), TxnId(3)));

        // Visible: deleted by future transaction
        assert!(snapshot.is_visible(TxnId(2), TxnId(25)));
    }

    #[test]
    fn test_free_space_map() {
        let fsm = FreeSpaceMap::new();

        fsm.update(PageId(1), 1000);
        fsm.update(PageId(2), 500);
        fsm.update(PageId(3), 2000);

        // Find page with at least 1500 bytes
        let page = fsm.find_page(1500);
        assert!(page.is_some());
        assert_eq!(page.unwrap(), PageId(3));

        // Find page with at least 100 bytes
        let page = fsm.find_page(100);
        assert!(page.is_some());

        // Find page with 5000 bytes (none available)
        let page = fsm.find_page(5000);
        assert!(page.is_none());
    }

    #[tokio::test]
    async fn test_row_store_insert() {
        let (store, _dir) = create_test_row_store();
        let table_id = TableId(1);
        let txn_id = TxnId(1);

        let row = Row {
            values: vec![
                Value::Int64(1),
                Value::String("test".into()),
            ],
        };

        let row_id = store.insert(table_id, txn_id, row).await.unwrap();
        assert!(row_id.0 > 0);
    }

    #[tokio::test]
    async fn test_row_store_scan() {
        let (store, _dir) = create_test_row_store();
        let table_id = TableId(1);
        let txn_id = TxnId(1);

        // Insert some rows
        for i in 0..5 {
            let row = Row {
                values: vec![Value::Int64(i as i64)],
            };
            store.insert(table_id, txn_id, row).await.unwrap();
        }

        // Scan
        let snapshot = Snapshot::new(txn_id, TxnId(0), TxnId(100), vec![]);
        let mut iter = store.scan(table_id, &snapshot).unwrap();

        let mut count = 0;
        while let Some(result) = iter.next() {
            let (_row_id, _row) = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_row_store_delete() {
        let (store, _dir) = create_test_row_store();
        let table_id = TableId(1);
        let txn_id = TxnId(1);

        let row = Row {
            values: vec![Value::Int64(42)],
        };

        let row_id = store.insert(table_id, txn_id, row).await.unwrap();

        // Delete
        store.delete(table_id, txn_id, row_id).await.unwrap();

        // Scan should not find the deleted row (in our own transaction)
        let snapshot = Snapshot::new(txn_id, TxnId(0), TxnId(100), vec![]);
        let mut iter = store.scan(table_id, &snapshot).unwrap();

        let count = std::iter::from_fn(|| iter.next()).count();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_row_encoding() {
        let (store, _dir) = create_test_row_store();
        let txn_id = TxnId(42);

        let row = Row {
            values: vec![
                Value::Int64(123),
                Value::String("hello".into()),
                Value::Boolean(true),
            ],
        };

        let encoded = store.encode_row(txn_id, &row);
        let (header, decoded) = store.decode_row(&encoded).unwrap();

        assert_eq!(header.xmin, 42);
        assert_eq!(header.xmax, 0);
        assert_eq!(decoded.values.len(), 3);
    }
}
