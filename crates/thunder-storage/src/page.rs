//! Slotted page implementation for ThunderDB.
//!
//! This module provides the core page format used throughout the storage engine:
//! - Row data pages
//! - B+Tree index nodes
//! - Overflow pages for large tuples
//!
//! Page Layout (16KB default):
//! ```text
//! +------------------+  0x0000
//! | PageHeader (24B) |
//! +------------------+  0x0018
//! | SlotEntry[0] 6B  |
//! | SlotEntry[1] 6B  |
//! | ...              |
//! +------------------+  (header + slots)
//! |    FREE SPACE    |
//! +------------------+  free_space_offset
//! | Tuple N          |  (grows upward)
//! | ...              |
//! | Tuple 1          |
//! | Tuple 0          |
//! +------------------+  PAGE_SIZE (0x4000)
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thunder_common::prelude::*;
use thunder_common::types::{Lsn, PageId, TxnId};

/// Default page size: 16KB (optimized for modern SSDs)
pub const PAGE_SIZE: usize = 16 * 1024;

/// Page header size: 24 bytes
pub const PAGE_HEADER_SIZE: usize = 24;

/// Slot entry size: 6 bytes
pub const SLOT_ENTRY_SIZE: usize = 6;

/// Minimum free space to keep in a page for future updates
pub const MIN_FREE_SPACE: usize = 64;

/// Slot flags
pub mod slot_flags {
    pub const NORMAL: u16 = 0x00;
    pub const DELETED: u16 = 0x01;
    pub const REDIRECTED: u16 = 0x02;
    pub const COMPRESSED: u16 = 0x04;
    pub const OVERFLOW: u16 = 0x08;
}

/// Page types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Data page for row storage
    Data = 0,
    /// B+Tree leaf node
    BTreeLeaf = 1,
    /// B+Tree internal node
    BTreeInternal = 2,
    /// Free page (in free list)
    Free = 3,
    /// Overflow page for large tuples
    Overflow = 4,
}

impl TryFrom<u8> for PageType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(PageType::Data),
            1 => Ok(PageType::BTreeLeaf),
            2 => Ok(PageType::BTreeInternal),
            3 => Ok(PageType::Free),
            4 => Ok(PageType::Overflow),
            _ => Err(Error::Storage(
                thunder_common::error::StorageError::PageCorrupted(0),
            )),
        }
    }
}

/// Page header (24 bytes, fixed layout)
///
/// Layout:
/// - page_id: u64 (8 bytes) - Unique page identifier
/// - lsn: u64 (8 bytes) - Log sequence number for crash recovery
/// - checksum: u32 (4 bytes) - CRC32 checksum of page content
/// - free_space_offset: u16 (2 bytes) - Offset where free space ends (tuple data starts)
/// - slot_count: u16 (2 bytes) - Number of slots in the slot array
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PageHeader {
    pub page_id: u64,
    pub lsn: u64,
    pub checksum: u32,
    pub free_space_offset: u16,
    pub slot_count: u16,
}

impl PageHeader {
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id: page_id.0,
            lsn: 0,
            checksum: 0,
            free_space_offset: PAGE_SIZE as u16,
            slot_count: 0,
        }
    }

    pub fn read_from(data: &[u8]) -> Self {
        let mut buf = &data[..PAGE_HEADER_SIZE];
        Self {
            page_id: buf.get_u64_le(),
            lsn: buf.get_u64_le(),
            checksum: buf.get_u32_le(),
            free_space_offset: buf.get_u16_le(),
            slot_count: buf.get_u16_le(),
        }
    }

    pub fn write_to(&self, data: &mut [u8]) {
        let mut buf = &mut data[..PAGE_HEADER_SIZE];
        buf.put_u64_le(self.page_id);
        buf.put_u64_le(self.lsn);
        buf.put_u32_le(self.checksum);
        buf.put_u16_le(self.free_space_offset);
        buf.put_u16_le(self.slot_count);
    }
}

/// Slot entry (6 bytes, fixed layout)
///
/// Each slot points to a tuple on the page.
/// Layout:
/// - offset: u16 (2 bytes) - Offset from page start to tuple
/// - length: u16 (2 bytes) - Length of tuple data
/// - flags: u16 (2 bytes) - Slot flags (deleted, redirected, etc.)
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct SlotEntry {
    pub offset: u16,
    pub length: u16,
    pub flags: u16,
}

impl SlotEntry {
    pub fn new(offset: u16, length: u16) -> Self {
        Self {
            offset,
            length,
            flags: slot_flags::NORMAL,
        }
    }

    pub fn empty() -> Self {
        Self {
            offset: 0,
            length: 0,
            flags: slot_flags::DELETED,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & slot_flags::DELETED != 0
    }

    pub fn is_overflow(&self) -> bool {
        self.flags & slot_flags::OVERFLOW != 0
    }

    pub fn read_from(data: &[u8]) -> Self {
        let mut buf = data;
        Self {
            offset: buf.get_u16_le(),
            length: buf.get_u16_le(),
            flags: buf.get_u16_le(),
        }
    }

    pub fn write_to(&self, data: &mut [u8]) {
        let mut buf = data;
        buf.put_u16_le(self.offset);
        buf.put_u16_le(self.length);
        buf.put_u16_le(self.flags);
    }
}

/// MVCC tuple header (24 bytes)
///
/// Embedded at the start of each tuple for visibility checking.
/// Layout:
/// - xmin: u64 (8 bytes) - Transaction ID that created this tuple
/// - xmax: u64 (8 bytes) - Transaction ID that deleted this tuple (0 if live)
/// - null_bitmap: u64 (8 bytes) - Bitmap for NULL columns (up to 64 columns)
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct TupleHeader {
    pub xmin: u64,
    pub xmax: u64,
    pub null_bitmap: u64,
}

/// Size of tuple header
pub const TUPLE_HEADER_SIZE: usize = 24;

impl TupleHeader {
    pub fn new(xmin: TxnId) -> Self {
        Self {
            xmin: xmin.0,
            xmax: 0,
            null_bitmap: 0,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.xmax != 0
    }

    pub fn is_null(&self, column_idx: usize) -> bool {
        if column_idx >= 64 {
            return false;
        }
        (self.null_bitmap >> column_idx) & 1 == 1
    }

    pub fn set_null(&mut self, column_idx: usize, is_null: bool) {
        if column_idx >= 64 {
            return;
        }
        if is_null {
            self.null_bitmap |= 1 << column_idx;
        } else {
            self.null_bitmap &= !(1 << column_idx);
        }
    }

    pub fn read_from(data: &[u8]) -> Self {
        let mut buf = data;
        Self {
            xmin: buf.get_u64_le(),
            xmax: buf.get_u64_le(),
            null_bitmap: buf.get_u64_le(),
        }
    }

    pub fn write_to(&self, data: &mut [u8]) {
        let mut buf = data;
        buf.put_u64_le(self.xmin);
        buf.put_u64_le(self.xmax);
        buf.put_u64_le(self.null_bitmap);
    }
}

/// A slotted page implementation.
///
/// The page stores tuples using a slot directory approach:
/// - Header at the start (fixed 24 bytes)
/// - Slot array grows downward after header
/// - Tuple data grows upward from the end of the page
/// - Free space is in the middle
pub struct Page {
    data: BytesMut,
    page_type: PageType,
    dirty: bool,
}

impl Page {
    /// Create a new empty page with the given page ID.
    pub fn new(page_id: PageId) -> Self {
        let mut data = BytesMut::zeroed(PAGE_SIZE);
        let header = PageHeader::new(page_id);
        header.write_to(&mut data);

        Self {
            data,
            page_type: PageType::Data,
            dirty: true,
        }
    }

    /// Create a new page with the specified type.
    pub fn new_with_type(page_id: PageId, page_type: PageType) -> Self {
        let mut page = Self::new(page_id);
        page.page_type = page_type;
        page
    }

    /// Load a page from raw bytes.
    pub fn from_bytes(data: Bytes) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(Error::Storage(
                thunder_common::error::StorageError::PageCorrupted(0),
            ));
        }

        let page = Self {
            data: BytesMut::from(data.as_ref()),
            page_type: PageType::Data,
            dirty: false,
        };

        // Verify checksum
        if !page.verify_checksum() {
            return Err(Error::Storage(
                thunder_common::error::StorageError::PageCorrupted(page.page_id().0),
            ));
        }

        Ok(page)
    }

    /// Convert page to bytes for persistence.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.data)
    }

    /// Convert page to bytes with checksum update.
    pub fn to_bytes_with_checksum(&mut self) -> Bytes {
        self.update_checksum();
        self.to_bytes()
    }

    /// Get raw data reference.
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable raw data reference.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.dirty = true;
        &mut self.data
    }

    // =========================================================================
    // Header operations
    // =========================================================================

    /// Get the page header.
    pub fn header(&self) -> PageHeader {
        PageHeader::read_from(&self.data)
    }

    /// Get the page ID.
    pub fn page_id(&self) -> PageId {
        PageId(self.header().page_id)
    }

    /// Get the LSN.
    pub fn lsn(&self) -> Lsn {
        Lsn(self.header().lsn)
    }

    /// Set the LSN.
    pub fn set_lsn(&mut self, lsn: Lsn) {
        let mut header = self.header();
        header.lsn = lsn.0;
        header.write_to(&mut self.data);
        self.dirty = true;
    }

    /// Get the page type.
    pub fn page_type(&self) -> PageType {
        self.page_type
    }

    /// Set the page type.
    pub fn set_page_type(&mut self, page_type: PageType) {
        self.page_type = page_type;
        self.dirty = true;
    }

    /// Check if the page is dirty.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Mark the page as clean.
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }

    /// Mark the page as dirty.
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    // =========================================================================
    // Checksum operations
    // =========================================================================

    /// Compute CRC32 checksum of page content (excluding checksum field).
    pub fn compute_checksum(&self) -> u32 {
        // Checksum covers everything except the checksum field itself (bytes 16-20)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.data[0..16]); // page_id + lsn
        hasher.update(&self.data[20..]); // free_space_offset + slot_count + rest
        hasher.finalize()
    }

    /// Update the checksum in the header.
    pub fn update_checksum(&mut self) {
        let checksum = self.compute_checksum();
        let mut header = self.header();
        header.checksum = checksum;
        header.write_to(&mut self.data);
    }

    /// Verify the page checksum.
    pub fn verify_checksum(&self) -> bool {
        let header = self.header();
        if header.checksum == 0 {
            // New page without checksum
            return true;
        }
        header.checksum == self.compute_checksum()
    }

    // =========================================================================
    // Slot operations
    // =========================================================================

    /// Get the number of slots.
    pub fn slot_count(&self) -> u16 {
        self.header().slot_count
    }

    /// Get a slot entry by index.
    pub fn slot(&self, slot_id: u16) -> Option<SlotEntry> {
        if slot_id >= self.slot_count() {
            return None;
        }
        let offset = PAGE_HEADER_SIZE + (slot_id as usize) * SLOT_ENTRY_SIZE;
        Some(SlotEntry::read_from(&self.data[offset..]))
    }

    /// Set a slot entry.
    fn set_slot(&mut self, slot_id: u16, entry: SlotEntry) {
        let offset = PAGE_HEADER_SIZE + (slot_id as usize) * SLOT_ENTRY_SIZE;
        entry.write_to(&mut self.data[offset..]);
        self.dirty = true;
    }

    /// Get the end of slot array (start of free space).
    fn slot_array_end(&self) -> usize {
        PAGE_HEADER_SIZE + (self.slot_count() as usize) * SLOT_ENTRY_SIZE
    }

    // =========================================================================
    // Free space management
    // =========================================================================

    /// Calculate available free space for new tuples.
    pub fn free_space(&self) -> usize {
        let header = self.header();
        let slot_end = self.slot_array_end();
        let tuple_start = header.free_space_offset as usize;

        if tuple_start > slot_end {
            tuple_start - slot_end
        } else {
            0
        }
    }

    /// Check if we can fit a tuple of given size (including new slot).
    pub fn can_fit(&self, tuple_size: usize) -> bool {
        let required = tuple_size + SLOT_ENTRY_SIZE;
        self.free_space() >= required + MIN_FREE_SPACE
    }

    /// Check if the page needs compaction.
    pub fn needs_compaction(&self) -> bool {
        let total_deleted: usize = (0..self.slot_count())
            .filter_map(|i| self.slot(i))
            .filter(|s| s.is_deleted())
            .map(|s| s.length as usize)
            .sum();

        // Compact if more than 25% of used space is from deleted tuples
        let header = self.header();
        let used_space = PAGE_SIZE - header.free_space_offset as usize;
        total_deleted > used_space / 4
    }

    // =========================================================================
    // Tuple operations
    // =========================================================================

    /// Insert a tuple into the page.
    ///
    /// Returns the slot ID of the inserted tuple.
    pub fn insert_tuple(&mut self, data: &[u8]) -> Result<u16> {
        if !self.can_fit(data.len()) {
            return Err(Error::Storage(
                thunder_common::error::StorageError::PageFull,
            ));
        }

        let mut header = self.header();

        // Find the offset for the new tuple (grows from end)
        let tuple_offset = header.free_space_offset as usize - data.len();

        // Copy tuple data
        self.data[tuple_offset..tuple_offset + data.len()].copy_from_slice(data);

        // Find or allocate a slot
        let slot_id = self.find_free_slot().unwrap_or_else(|| {
            let new_slot = header.slot_count;
            header.slot_count += 1;
            new_slot
        });

        // Update slot entry
        let slot = SlotEntry::new(tuple_offset as u16, data.len() as u16);
        self.set_slot(slot_id, slot);

        // Update header
        header.free_space_offset = tuple_offset as u16;
        header.write_to(&mut self.data);
        self.dirty = true;

        Ok(slot_id)
    }

    /// Find a free (deleted) slot for reuse.
    fn find_free_slot(&self) -> Option<u16> {
        for i in 0..self.slot_count() {
            if let Some(slot) = self.slot(i) {
                if slot.is_deleted() {
                    return Some(i);
                }
            }
        }
        None
    }

    /// Get tuple data by slot ID.
    pub fn get_tuple(&self, slot_id: u16) -> Option<&[u8]> {
        let slot = self.slot(slot_id)?;
        if slot.is_deleted() {
            return None;
        }
        let start = slot.offset as usize;
        let end = start + slot.length as usize;
        if end > PAGE_SIZE {
            return None;
        }
        Some(&self.data[start..end])
    }

    /// Update tuple data at the given slot.
    ///
    /// If the new data fits in the existing space, update in place.
    /// Otherwise, delete the old tuple and insert the new one.
    pub fn update_tuple(&mut self, slot_id: u16, data: &[u8]) -> Result<()> {
        let slot = self.slot(slot_id).ok_or_else(|| {
            Error::Storage(thunder_common::error::StorageError::PageCorrupted(
                self.page_id().0,
            ))
        })?;

        if slot.is_deleted() {
            return Err(Error::Storage(
                thunder_common::error::StorageError::PageCorrupted(self.page_id().0),
            ));
        }

        // If new data fits in existing space, update in place
        if data.len() <= slot.length as usize {
            let start = slot.offset as usize;
            self.data[start..start + data.len()].copy_from_slice(data);

            // Update slot length if smaller
            if data.len() < slot.length as usize {
                let mut new_slot = slot;
                new_slot.length = data.len() as u16;
                self.set_slot(slot_id, new_slot);
            }
            self.dirty = true;
            return Ok(());
        }

        // New data is larger - delete old and insert new
        self.delete_tuple(slot_id)?;

        // Check if we have space
        if !self.can_fit(data.len()) {
            // Need to compact first
            self.compact();
            if !self.can_fit(data.len()) {
                return Err(Error::Storage(
                    thunder_common::error::StorageError::PageFull,
                ));
            }
        }

        // Insert at the same slot ID
        let mut header = self.header();
        let tuple_offset = header.free_space_offset as usize - data.len();
        self.data[tuple_offset..tuple_offset + data.len()].copy_from_slice(data);

        let new_slot = SlotEntry::new(tuple_offset as u16, data.len() as u16);
        self.set_slot(slot_id, new_slot);

        header.free_space_offset = tuple_offset as u16;
        header.write_to(&mut self.data);
        self.dirty = true;

        Ok(())
    }

    /// Delete a tuple by marking its slot as deleted.
    pub fn delete_tuple(&mut self, slot_id: u16) -> Result<()> {
        let mut slot = self.slot(slot_id).ok_or_else(|| {
            Error::Storage(thunder_common::error::StorageError::PageCorrupted(
                self.page_id().0,
            ))
        })?;

        slot.flags |= slot_flags::DELETED;
        self.set_slot(slot_id, slot);
        self.dirty = true;

        Ok(())
    }

    // =========================================================================
    // MVCC operations
    // =========================================================================

    /// Get the tuple header for MVCC visibility checking.
    pub fn get_tuple_header(&self, slot_id: u16) -> Option<TupleHeader> {
        let tuple_data = self.get_tuple(slot_id)?;
        if tuple_data.len() < TUPLE_HEADER_SIZE {
            return None;
        }
        Some(TupleHeader::read_from(tuple_data))
    }

    /// Set xmax on a tuple (for deletion marking in MVCC).
    pub fn set_xmax(&mut self, slot_id: u16, xmax: TxnId) -> Result<()> {
        let slot = self.slot(slot_id).ok_or_else(|| {
            Error::Storage(thunder_common::error::StorageError::PageCorrupted(
                self.page_id().0,
            ))
        })?;

        if slot.is_deleted() {
            return Err(Error::Storage(
                thunder_common::error::StorageError::PageCorrupted(self.page_id().0),
            ));
        }

        // Update xmax in the tuple header
        let offset = slot.offset as usize + 8; // Skip xmin
        let mut buf = &mut self.data[offset..offset + 8];
        buf.put_u64_le(xmax.0);
        self.dirty = true;

        Ok(())
    }

    // =========================================================================
    // Compaction
    // =========================================================================

    /// Compact the page by removing deleted tuples and defragmenting.
    pub fn compact(&mut self) {
        let mut header = self.header();

        // Collect live tuples with their slot IDs
        let mut live_tuples: Vec<(u16, Vec<u8>)> = Vec::new();
        for slot_id in 0..header.slot_count {
            if let Some(slot) = self.slot(slot_id) {
                if !slot.is_deleted() {
                    let start = slot.offset as usize;
                    let end = start + slot.length as usize;
                    let data = self.data[start..end].to_vec();
                    live_tuples.push((slot_id, data));
                }
            }
        }

        // Clear the page data (keep header)
        self.data[PAGE_HEADER_SIZE..].fill(0);

        // Reset free space offset to end of page
        header.free_space_offset = PAGE_SIZE as u16;

        // Re-insert tuples in order
        for (slot_id, data) in live_tuples {
            let tuple_offset = header.free_space_offset as usize - data.len();
            self.data[tuple_offset..tuple_offset + data.len()].copy_from_slice(&data);

            let slot = SlotEntry::new(tuple_offset as u16, data.len() as u16);
            self.set_slot(slot_id, slot);

            header.free_space_offset = tuple_offset as u16;
        }

        // Mark all unused slots as deleted
        for slot_id in 0..header.slot_count {
            if let Some(slot) = self.slot(slot_id) {
                if slot.offset == 0 && slot.length == 0 && slot.flags == slot_flags::NORMAL {
                    self.set_slot(slot_id, SlotEntry::empty());
                }
            }
        }

        header.write_to(&mut self.data);
        self.dirty = true;
    }

    // =========================================================================
    // Iteration
    // =========================================================================

    /// Iterate over all non-deleted tuples in the page.
    pub fn iter(&self) -> PageIterator<'_> {
        PageIterator {
            page: self,
            current_slot: 0,
        }
    }
}

/// Iterator over tuples in a page.
pub struct PageIterator<'a> {
    page: &'a Page,
    current_slot: u16,
}

impl<'a> Iterator for PageIterator<'a> {
    type Item = (u16, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_slot < self.page.slot_count() {
            let slot_id = self.current_slot;
            self.current_slot += 1;

            if let Some(data) = self.page.get_tuple(slot_id) {
                return Some((slot_id, data));
            }
        }
        None
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            page_type: self.page_type,
            dirty: self.dirty,
        }
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let header = self.header();
        f.debug_struct("Page")
            .field("page_id", &header.page_id)
            .field("lsn", &header.lsn)
            .field("slot_count", &header.slot_count)
            .field("free_space", &self.free_space())
            .field("page_type", &self.page_type)
            .field("dirty", &self.dirty)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(PageId(42));
        assert_eq!(page.page_id(), PageId(42));
        assert_eq!(page.slot_count(), 0);
        assert!(page.free_space() > 0);
        assert!(page.is_dirty());
    }

    #[test]
    fn test_page_header() {
        let mut page = Page::new(PageId(1));
        page.set_lsn(Lsn(100));

        let header = page.header();
        assert_eq!(header.page_id, 1);
        assert_eq!(header.lsn, 100);
        assert_eq!(header.slot_count, 0);
    }

    #[test]
    fn test_insert_and_get_tuple() {
        let mut page = Page::new(PageId(1));

        let data = b"Hello, World!";
        let slot_id = page.insert_tuple(data).unwrap();
        assert_eq!(slot_id, 0);
        assert_eq!(page.slot_count(), 1);

        let retrieved = page.get_tuple(slot_id).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_multiple_inserts() {
        let mut page = Page::new(PageId(1));

        let data1 = b"First tuple";
        let data2 = b"Second tuple";
        let data3 = b"Third tuple";

        let slot1 = page.insert_tuple(data1).unwrap();
        let slot2 = page.insert_tuple(data2).unwrap();
        let slot3 = page.insert_tuple(data3).unwrap();

        assert_eq!(slot1, 0);
        assert_eq!(slot2, 1);
        assert_eq!(slot3, 2);
        assert_eq!(page.slot_count(), 3);

        assert_eq!(page.get_tuple(slot1).unwrap(), data1);
        assert_eq!(page.get_tuple(slot2).unwrap(), data2);
        assert_eq!(page.get_tuple(slot3).unwrap(), data3);
    }

    #[test]
    fn test_delete_tuple() {
        let mut page = Page::new(PageId(1));

        let data = b"Test tuple";
        let slot_id = page.insert_tuple(data).unwrap();

        assert!(page.get_tuple(slot_id).is_some());

        page.delete_tuple(slot_id).unwrap();
        assert!(page.get_tuple(slot_id).is_none());

        let slot = page.slot(slot_id).unwrap();
        assert!(slot.is_deleted());
    }

    #[test]
    fn test_slot_reuse() {
        let mut page = Page::new(PageId(1));

        let slot1 = page.insert_tuple(b"First").unwrap();
        let slot2 = page.insert_tuple(b"Second").unwrap();

        page.delete_tuple(slot1).unwrap();

        // New insert should reuse slot1
        let slot3 = page.insert_tuple(b"Third").unwrap();
        assert_eq!(slot3, slot1);
        assert_eq!(page.slot_count(), 2); // No new slot created
    }

    #[test]
    fn test_update_tuple_in_place() {
        let mut page = Page::new(PageId(1));

        let data = b"Original data here!!";
        let slot_id = page.insert_tuple(data).unwrap();

        let new_data = b"Updated!"; // Shorter
        page.update_tuple(slot_id, new_data).unwrap();

        let retrieved = page.get_tuple(slot_id).unwrap();
        assert_eq!(retrieved, new_data);
    }

    #[test]
    fn test_update_tuple_larger() {
        let mut page = Page::new(PageId(1));

        let data = b"Short";
        let slot_id = page.insert_tuple(data).unwrap();

        let new_data = b"Much longer data that requires reallocation";
        page.update_tuple(slot_id, new_data).unwrap();

        let retrieved = page.get_tuple(slot_id).unwrap();
        assert_eq!(retrieved, new_data);
    }

    #[test]
    fn test_page_compaction() {
        let mut page = Page::new(PageId(1));

        // Insert multiple tuples
        let slot1 = page.insert_tuple(b"AAAAAAAAAA").unwrap();
        let slot2 = page.insert_tuple(b"BBBBBBBBBB").unwrap();
        let slot3 = page.insert_tuple(b"CCCCCCCCCC").unwrap();

        let free_before = page.free_space();

        // Delete middle tuple
        page.delete_tuple(slot2).unwrap();

        // Compact
        page.compact();

        // Free space should increase (deleted tuple reclaimed)
        assert!(page.free_space() > free_before);

        // Other tuples should still be accessible
        assert_eq!(page.get_tuple(slot1).unwrap(), b"AAAAAAAAAA");
        assert_eq!(page.get_tuple(slot3).unwrap(), b"CCCCCCCCCC");
        assert!(page.get_tuple(slot2).is_none());
    }

    #[test]
    fn test_checksum() {
        let mut page = Page::new(PageId(1));
        page.insert_tuple(b"Test data").unwrap();

        page.update_checksum();
        assert!(page.verify_checksum());

        // Corrupt the data
        page.data[100] ^= 0xFF;
        assert!(!page.verify_checksum());
    }

    #[test]
    fn test_page_serialization() {
        let mut page = Page::new(PageId(42));
        page.insert_tuple(b"Test data").unwrap();
        page.set_lsn(Lsn(1000));

        let bytes = page.to_bytes_with_checksum();
        assert_eq!(bytes.len(), PAGE_SIZE);

        let loaded = Page::from_bytes(bytes).unwrap();
        assert_eq!(loaded.page_id(), PageId(42));
        assert_eq!(loaded.lsn(), Lsn(1000));
        assert_eq!(loaded.get_tuple(0).unwrap(), b"Test data");
    }

    #[test]
    fn test_page_iterator() {
        let mut page = Page::new(PageId(1));

        page.insert_tuple(b"First").unwrap();
        page.insert_tuple(b"Second").unwrap();
        let slot_to_delete = page.insert_tuple(b"Third").unwrap();
        page.insert_tuple(b"Fourth").unwrap();

        page.delete_tuple(slot_to_delete).unwrap();

        let tuples: Vec<_> = page.iter().collect();
        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0].1, b"First");
        assert_eq!(tuples[1].1, b"Second");
        assert_eq!(tuples[2].1, b"Fourth");
    }

    #[test]
    fn test_tuple_header() {
        let mut header = TupleHeader::new(TxnId(100));
        assert_eq!(header.xmin, 100);
        assert_eq!(header.xmax, 0);
        assert!(!header.is_deleted());

        header.set_null(0, true);
        header.set_null(5, true);
        assert!(header.is_null(0));
        assert!(header.is_null(5));
        assert!(!header.is_null(1));

        header.set_null(0, false);
        assert!(!header.is_null(0));
    }

    #[test]
    fn test_mvcc_operations() {
        let mut page = Page::new(PageId(1));

        // Create tuple with MVCC header
        let mut tuple_data = vec![0u8; TUPLE_HEADER_SIZE + 10];
        let header = TupleHeader::new(TxnId(100));
        header.write_to(&mut tuple_data);
        tuple_data[TUPLE_HEADER_SIZE..].copy_from_slice(b"TestData!!");

        let slot_id = page.insert_tuple(&tuple_data).unwrap();

        // Check tuple header
        let retrieved_header = page.get_tuple_header(slot_id).unwrap();
        assert_eq!(retrieved_header.xmin, 100);
        assert_eq!(retrieved_header.xmax, 0);

        // Set xmax (delete marking)
        page.set_xmax(slot_id, TxnId(200)).unwrap();

        let updated_header = page.get_tuple_header(slot_id).unwrap();
        assert_eq!(updated_header.xmax, 200);
        assert!(updated_header.is_deleted());
    }

    #[test]
    fn test_free_space_calculation() {
        let mut page = Page::new(PageId(1));
        let initial_free = page.free_space();

        // Insert a 100-byte tuple
        let data = vec![0u8; 100];
        page.insert_tuple(&data).unwrap();

        // Free space should decrease by tuple size + slot entry size
        let expected_decrease = 100 + SLOT_ENTRY_SIZE;
        assert_eq!(page.free_space(), initial_free - expected_decrease);
    }

    #[test]
    fn test_can_fit() {
        let mut page = Page::new(PageId(1));

        // Should be able to fit reasonable sized tuples
        assert!(page.can_fit(100));
        assert!(page.can_fit(1000));

        // Fill the page
        while page.can_fit(1000) {
            let data = vec![0u8; 1000];
            page.insert_tuple(&data).unwrap();
        }

        // Now should not be able to fit large tuples
        assert!(!page.can_fit(1000));
    }

    #[test]
    fn test_page_types() {
        let mut page = Page::new_with_type(PageId(1), PageType::BTreeLeaf);
        assert_eq!(page.page_type(), PageType::BTreeLeaf);

        page.set_page_type(PageType::BTreeInternal);
        assert_eq!(page.page_type(), PageType::BTreeInternal);
    }
}
