//! Write-Ahead Logging (WAL) for ThunderDB.
//!
//! Provides durability through write-ahead logging:
//! - All modifications are logged before being applied
//! - Group commit for high throughput
//! - Segment-based log files for efficient management
//! - ARIES-style crash recovery
//!
//! WAL Record Format:
//! ```text
//! +--------------------+
//! | WalRecordHeader    |  32 bytes
//! |  - lsn (8)         |
//! |  - prev_lsn (8)    |
//! |  - txn_id (8)      |
//! |  - record_type (1) |
//! |  - flags (1)       |
//! |  - table_id (6)    |
//! |  - length (4)      |
//! |  - checksum (4)    |
//! +--------------------+
//! | Payload (variable) |
//! +--------------------+
//! ```

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thunder_common::prelude::*;
use thunder_common::types::{Lsn, PageId, RowId, TableId, TxnId};
use tokio::sync::Notify;

/// WAL record header size: 40 bytes
pub const WAL_HEADER_SIZE: usize = 40;

/// Default WAL segment size: 64MB
pub const DEFAULT_SEGMENT_SIZE: usize = 64 * 1024 * 1024;

/// WAL buffer size for batching writes
pub const DEFAULT_WAL_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

/// WAL record types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    /// Insert a new tuple
    Insert = 1,
    /// Update an existing tuple
    Update = 2,
    /// Delete a tuple
    Delete = 3,
    /// Commit a transaction
    Commit = 4,
    /// Abort a transaction
    Abort = 5,
    /// Checkpoint record
    Checkpoint = 6,
    /// Begin transaction
    BeginTxn = 7,
    /// Compensation log record (for undo)
    Clr = 8,
    /// B+Tree page split
    PageSplit = 9,
    /// B+Tree page merge
    PageMerge = 10,
    /// Create table DDL
    CreateTable = 11,
    /// Drop table DDL
    DropTable = 12,
}

impl TryFrom<u8> for WalRecordType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(WalRecordType::Insert),
            2 => Ok(WalRecordType::Update),
            3 => Ok(WalRecordType::Delete),
            4 => Ok(WalRecordType::Commit),
            5 => Ok(WalRecordType::Abort),
            6 => Ok(WalRecordType::Checkpoint),
            7 => Ok(WalRecordType::BeginTxn),
            8 => Ok(WalRecordType::Clr),
            9 => Ok(WalRecordType::PageSplit),
            10 => Ok(WalRecordType::PageMerge),
            11 => Ok(WalRecordType::CreateTable),
            12 => Ok(WalRecordType::DropTable),
            _ => Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("Invalid record type".into()),
            )),
        }
    }
}

/// WAL record flags
pub mod wal_flags {
    pub const NONE: u8 = 0x00;
    pub const REDO_ONLY: u8 = 0x01;
    pub const UNDO_ONLY: u8 = 0x02;
}

/// WAL record header (32 bytes)
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WalRecordHeader {
    pub lsn: u64,
    pub prev_lsn: u64,
    pub txn_id: u64,
    pub record_type: u8,
    pub flags: u8,
    pub table_id: u64,
    pub length: u32,
    pub checksum: u32,
}

impl WalRecordHeader {
    pub fn new(
        lsn: Lsn,
        prev_lsn: Lsn,
        txn_id: TxnId,
        record_type: WalRecordType,
        table_id: TableId,
        payload_len: usize,
    ) -> Self {
        Self {
            lsn: lsn.0,
            prev_lsn: prev_lsn.0,
            txn_id: txn_id.0,
            record_type: record_type as u8,
            flags: wal_flags::NONE,
            table_id: table_id.0,
            length: payload_len as u32,
            checksum: 0,
        }
    }

    pub fn read_from(data: &[u8]) -> Result<Self> {
        if data.len() < WAL_HEADER_SIZE {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("Header too short".into()),
            ));
        }

        let mut buf = data;
        Ok(Self {
            lsn: buf.get_u64_le(),
            prev_lsn: buf.get_u64_le(),
            txn_id: buf.get_u64_le(),
            record_type: buf.get_u8(),
            flags: buf.get_u8(),
            table_id: {
                // table_id is stored as 6 bytes but we read as u64
                let hi = buf.get_u16_le() as u64;
                let lo = buf.get_u32_le() as u64;
                (hi << 32) | lo
            },
            length: buf.get_u32_le(),
            checksum: buf.get_u32_le(),
        })
    }

    pub fn write_to(&self, data: &mut [u8]) {
        let mut buf = data;
        buf.put_u64_le(self.lsn);
        buf.put_u64_le(self.prev_lsn);
        buf.put_u64_le(self.txn_id);
        buf.put_u8(self.record_type);
        buf.put_u8(self.flags);
        buf.put_u16_le((self.table_id >> 32) as u16);
        buf.put_u32_le(self.table_id as u32);
        buf.put_u32_le(self.length);
        buf.put_u32_le(self.checksum);
    }

    pub fn compute_checksum(&self, payload: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        let mut header_bytes = [0u8; WAL_HEADER_SIZE];
        self.write_to(&mut header_bytes);
        // Don't include checksum field in calculation (last 4 bytes)
        hasher.update(&header_bytes[..36]);
        hasher.update(payload);
        hasher.finalize()
    }

    pub fn record_size(&self) -> usize {
        WAL_HEADER_SIZE + self.length as usize
    }
}

/// Insert record payload
#[derive(Debug, Clone)]
pub struct InsertRecord {
    pub row_id: RowId,
    pub page_id: PageId,
    pub slot_id: u16,
    pub tuple_data: Bytes,
}

impl InsertRecord {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(18 + self.tuple_data.len());
        buf.put_u64_le(self.row_id.0);
        buf.put_u64_le(self.page_id.0);
        buf.put_u16_le(self.slot_id);
        buf.put_slice(&self.tuple_data);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 18 {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("Insert record too short".into()),
            ));
        }
        let mut buf = data;
        Ok(Self {
            row_id: RowId(buf.get_u64_le()),
            page_id: PageId(buf.get_u64_le()),
            slot_id: buf.get_u16_le(),
            tuple_data: Bytes::copy_from_slice(&data[18..]),
        })
    }
}

/// Update record payload (before + after images)
#[derive(Debug, Clone)]
pub struct UpdateRecord {
    pub row_id: RowId,
    pub page_id: PageId,
    pub slot_id: u16,
    pub before_image: Bytes,
    pub after_image: Bytes,
}

impl UpdateRecord {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(22 + self.before_image.len() + self.after_image.len());
        buf.put_u64_le(self.row_id.0);
        buf.put_u64_le(self.page_id.0);
        buf.put_u16_le(self.slot_id);
        buf.put_u16_le(self.before_image.len() as u16);
        buf.put_u16_le(self.after_image.len() as u16);
        buf.put_slice(&self.before_image);
        buf.put_slice(&self.after_image);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 22 {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("Update record too short".into()),
            ));
        }
        let mut buf = &data[..];
        let row_id = RowId(buf.get_u64_le());
        let page_id = PageId(buf.get_u64_le());
        let slot_id = buf.get_u16_le();
        let before_len = buf.get_u16_le() as usize;
        let after_len = buf.get_u16_le() as usize;

        if data.len() < 22 + before_len + after_len {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("Update record truncated".into()),
            ));
        }

        Ok(Self {
            row_id,
            page_id,
            slot_id,
            before_image: Bytes::copy_from_slice(&data[22..22 + before_len]),
            after_image: Bytes::copy_from_slice(&data[22 + before_len..22 + before_len + after_len]),
        })
    }
}

/// Delete record payload
#[derive(Debug, Clone)]
pub struct DeleteRecord {
    pub row_id: RowId,
    pub page_id: PageId,
    pub slot_id: u16,
    pub before_image: Bytes,
}

impl DeleteRecord {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(18 + self.before_image.len());
        buf.put_u64_le(self.row_id.0);
        buf.put_u64_le(self.page_id.0);
        buf.put_u16_le(self.slot_id);
        buf.put_slice(&self.before_image);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 18 {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("Delete record too short".into()),
            ));
        }
        let mut buf = data;
        Ok(Self {
            row_id: RowId(buf.get_u64_le()),
            page_id: PageId(buf.get_u64_le()),
            slot_id: buf.get_u16_le(),
            before_image: Bytes::copy_from_slice(&data[18..]),
        })
    }
}

/// Checkpoint record
#[derive(Debug, Clone)]
pub struct CheckpointRecord {
    pub active_txns: Vec<(TxnId, Lsn)>,
    pub dirty_pages: Vec<(PageId, Lsn)>,
}

impl CheckpointRecord {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32_le(self.active_txns.len() as u32);
        for (txn_id, lsn) in &self.active_txns {
            buf.put_u64_le(txn_id.0);
            buf.put_u64_le(lsn.0);
        }
        buf.put_u32_le(self.dirty_pages.len() as u32);
        for (page_id, lsn) in &self.dirty_pages {
            buf.put_u64_le(page_id.0);
            buf.put_u64_le(lsn.0);
        }
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("Checkpoint record too short".into()),
            ));
        }

        let mut buf = data;
        let txn_count = buf.get_u32_le() as usize;

        if data.len() < 4 + txn_count * 16 + 4 {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("Checkpoint record truncated".into()),
            ));
        }

        let mut active_txns = Vec::with_capacity(txn_count);
        for _ in 0..txn_count {
            let txn_id = TxnId(buf.get_u64_le());
            let lsn = Lsn(buf.get_u64_le());
            active_txns.push((txn_id, lsn));
        }

        let page_count = buf.get_u32_le() as usize;
        let mut dirty_pages = Vec::with_capacity(page_count);
        for _ in 0..page_count {
            let page_id = PageId(buf.get_u64_le());
            let lsn = Lsn(buf.get_u64_le());
            dirty_pages.push((page_id, lsn));
        }

        Ok(Self {
            active_txns,
            dirty_pages,
        })
    }
}

/// Create table record - stores table metadata for catalog persistence
#[derive(Debug, Clone)]
pub struct CreateTableRecord {
    /// Table ID
    pub table_id: TableId,
    /// Table name
    pub table_name: String,
    /// Schema name (e.g., "public")
    pub schema_name: String,
    /// Serialized schema (column definitions as JSON)
    pub schema_json: String,
}

impl CreateTableRecord {
    pub fn encode(&self) -> Bytes {
        let table_name_bytes = self.table_name.as_bytes();
        let schema_name_bytes = self.schema_name.as_bytes();
        let schema_json_bytes = self.schema_json.as_bytes();

        let mut buf = BytesMut::with_capacity(
            8 + 4 + table_name_bytes.len() + 4 + schema_name_bytes.len() + 4 + schema_json_bytes.len()
        );

        buf.put_u64_le(self.table_id.0);
        buf.put_u32_le(table_name_bytes.len() as u32);
        buf.put_slice(table_name_bytes);
        buf.put_u32_le(schema_name_bytes.len() as u32);
        buf.put_slice(schema_name_bytes);
        buf.put_u32_le(schema_json_bytes.len() as u32);
        buf.put_slice(schema_json_bytes);

        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 20 {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("CreateTable record too short".into()),
            ));
        }

        let mut buf = data;
        let table_id = TableId(buf.get_u64_le());

        let table_name_len = buf.get_u32_le() as usize;
        if buf.len() < table_name_len {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("CreateTable record truncated (table_name)".into()),
            ));
        }
        let table_name = String::from_utf8_lossy(&buf[..table_name_len]).to_string();
        buf.advance(table_name_len);

        let schema_name_len = buf.get_u32_le() as usize;
        if buf.len() < schema_name_len {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("CreateTable record truncated (schema_name)".into()),
            ));
        }
        let schema_name = String::from_utf8_lossy(&buf[..schema_name_len]).to_string();
        buf.advance(schema_name_len);

        let schema_json_len = buf.get_u32_le() as usize;
        if buf.len() < schema_json_len {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("CreateTable record truncated (schema_json)".into()),
            ));
        }
        let schema_json = String::from_utf8_lossy(&buf[..schema_json_len]).to_string();

        Ok(Self {
            table_id,
            table_name,
            schema_name,
            schema_json,
        })
    }
}

/// Drop table record - stores table identifier for catalog persistence
#[derive(Debug, Clone)]
pub struct DropTableRecord {
    /// Table ID
    pub table_id: TableId,
    /// Table name (for logging/debugging)
    pub table_name: String,
}

impl DropTableRecord {
    pub fn encode(&self) -> Bytes {
        let table_name_bytes = self.table_name.as_bytes();
        let mut buf = BytesMut::with_capacity(8 + 4 + table_name_bytes.len());

        buf.put_u64_le(self.table_id.0);
        buf.put_u32_le(table_name_bytes.len() as u32);
        buf.put_slice(table_name_bytes);

        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 12 {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("DropTable record too short".into()),
            ));
        }

        let mut buf = data;
        let table_id = TableId(buf.get_u64_le());

        let table_name_len = buf.get_u32_le() as usize;
        if buf.len() < table_name_len {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("DropTable record truncated".into()),
            ));
        }
        let table_name = String::from_utf8_lossy(&buf[..table_name_len]).to_string();

        Ok(Self {
            table_id,
            table_name,
        })
    }
}

/// WAL entry (as used by the WalWriter trait in lib.rs)
pub use crate::WalEntry;
pub use crate::WalEntryType;

/// WAL segment file
struct WalSegment {
    file: File,
    segment_id: u64,
    start_lsn: Lsn,
    current_offset: u64,
    path: PathBuf,
}

impl WalSegment {
    fn create(dir: &Path, segment_id: u64, start_lsn: Lsn) -> Result<Self> {
        let filename = format!("wal_{:016x}.log", segment_id);
        let path = dir.join(filename);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::WalWriteFailed(
                    e.to_string(),
                ))
            })?;

        Ok(Self {
            file,
            segment_id,
            start_lsn,
            current_offset: 0,
            path,
        })
    }

    fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::WalWriteFailed(
                    e.to_string(),
                ))
            })?;

        // Parse segment ID from filename
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed("Invalid WAL filename".into())
            ))?;
        let segment_id = u64::from_str_radix(&filename[4..20], 16).unwrap_or(0);

        Ok(Self {
            file,
            segment_id,
            start_lsn: Lsn(0), // Will be set when reading first record
            current_offset: 0,
            path: path.to_path_buf(),
        })
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::WalWriteFailed(
                e.to_string(),
            ))
        })?;
        self.current_offset += data.len() as u64;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.sync_all().map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::WalWriteFailed(
                e.to_string(),
            ))
        })
    }

    fn size(&self) -> u64 {
        self.current_offset
    }
}

/// WAL buffer for batching writes
struct WalBuffer {
    data: BytesMut,
    records: Vec<Lsn>,
}

impl WalBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            data: BytesMut::with_capacity(capacity),
            records: Vec::new(),
        }
    }

    fn append(&mut self, lsn: Lsn, record: &[u8]) {
        self.data.extend_from_slice(record);
        self.records.push(lsn);
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn take(&mut self) -> (Bytes, Vec<Lsn>) {
        let data = self.data.split().freeze();
        let records = std::mem::take(&mut self.records);
        (data, records)
    }
}

/// WAL writer implementation
pub struct WalWriterImpl {
    /// Current LSN (atomically incremented)
    current_lsn: AtomicU64,
    /// Last flushed LSN
    flushed_lsn: AtomicU64,
    /// Write buffer for batching
    buffer: Mutex<WalBuffer>,
    /// Current segment
    current_segment: Mutex<WalSegment>,
    /// Segment directory
    segment_dir: PathBuf,
    /// Segment size threshold
    segment_size: usize,
    /// Sync on every commit
    sync_commit: bool,
    /// Notification for flush completion
    flush_notify: Arc<Notify>,
    /// Per-transaction previous LSN
    txn_prev_lsn: Mutex<HashMap<TxnId, Lsn>>,
}

impl WalWriterImpl {
    /// Create a new WAL writer.
    pub fn new(dir: &Path, sync_commit: bool) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::WalWriteFailed(
                e.to_string(),
            ))
        })?;

        // Create initial segment
        let segment = WalSegment::create(dir, 0, Lsn(1))?;

        Ok(Self {
            current_lsn: AtomicU64::new(1),
            flushed_lsn: AtomicU64::new(0),
            buffer: Mutex::new(WalBuffer::new(DEFAULT_WAL_BUFFER_SIZE)),
            current_segment: Mutex::new(segment),
            segment_dir: dir.to_path_buf(),
            segment_size: DEFAULT_SEGMENT_SIZE,
            sync_commit,
            flush_notify: Arc::new(Notify::new()),
            txn_prev_lsn: Mutex::new(HashMap::new()),
        })
    }

    /// Open an existing WAL directory.
    pub fn open(dir: &Path, sync_commit: bool) -> Result<Self> {
        // Find the latest segment
        let mut segments: Vec<_> = std::fs::read_dir(dir)
            .map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::WalWriteFailed(
                    e.to_string(),
                ))
            })?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("wal_") && n.ends_with(".log"))
                    .unwrap_or(false)
            })
            .collect();

        segments.sort_by_key(|e| e.path());

        let (segment, last_lsn) = if let Some(entry) = segments.last() {
            let mut seg = WalSegment::open(&entry.path())?;
            let last_lsn = Self::find_last_lsn(&mut seg)?;
            seg.current_offset = seg.file.seek(SeekFrom::End(0)).unwrap_or(0);
            (seg, last_lsn)
        } else {
            (WalSegment::create(dir, 0, Lsn(1))?, Lsn(0))
        };

        Ok(Self {
            current_lsn: AtomicU64::new(last_lsn.0 + 1),
            flushed_lsn: AtomicU64::new(last_lsn.0),
            buffer: Mutex::new(WalBuffer::new(DEFAULT_WAL_BUFFER_SIZE)),
            current_segment: Mutex::new(segment),
            segment_dir: dir.to_path_buf(),
            segment_size: DEFAULT_SEGMENT_SIZE,
            sync_commit,
            flush_notify: Arc::new(Notify::new()),
            txn_prev_lsn: Mutex::new(HashMap::new()),
        })
    }

    fn find_last_lsn(segment: &mut WalSegment) -> Result<Lsn> {
        segment.file.seek(SeekFrom::Start(0)).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::WalWriteFailed(
                e.to_string(),
            ))
        })?;

        let mut last_lsn = Lsn(0);
        let mut header_buf = [0u8; WAL_HEADER_SIZE];

        loop {
            match segment.file.read_exact(&mut header_buf) {
                Ok(()) => {
                    let header = WalRecordHeader::read_from(&header_buf)?;
                    last_lsn = Lsn(header.lsn);
                    // Skip payload
                    segment
                        .file
                        .seek(SeekFrom::Current(header.length as i64))
                        .map_err(|e| {
                            Error::Storage(thunder_common::error::StorageError::WalWriteFailed(
                                e.to_string(),
                            ))
                        })?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(Error::Storage(
                        thunder_common::error::StorageError::WalWriteFailed(e.to_string()),
                    ))
                }
            }
        }

        Ok(last_lsn)
    }

    /// Allocate a new LSN.
    fn allocate_lsn(&self) -> Lsn {
        Lsn(self.current_lsn.fetch_add(1, Ordering::SeqCst))
    }

    /// Get the previous LSN for a transaction.
    fn get_prev_lsn(&self, txn_id: TxnId) -> Lsn {
        self.txn_prev_lsn
            .lock()
            .get(&txn_id)
            .copied()
            .unwrap_or(Lsn::INVALID)
    }

    /// Update the previous LSN for a transaction.
    fn set_prev_lsn(&self, txn_id: TxnId, lsn: Lsn) {
        self.txn_prev_lsn.lock().insert(txn_id, lsn);
    }

    /// Remove transaction from prev_lsn tracking (on commit/abort).
    fn remove_txn(&self, txn_id: TxnId) {
        self.txn_prev_lsn.lock().remove(&txn_id);
    }

    /// Write a WAL record.
    pub fn write_record(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        record_type: WalRecordType,
        payload: &[u8],
    ) -> Result<Lsn> {
        let lsn = self.allocate_lsn();
        let prev_lsn = self.get_prev_lsn(txn_id);

        let mut header = WalRecordHeader::new(lsn, prev_lsn, txn_id, record_type, table_id, payload.len());
        header.checksum = header.compute_checksum(payload);

        // Serialize record
        let mut record = BytesMut::with_capacity(WAL_HEADER_SIZE + payload.len());
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        header.write_to(&mut header_buf);
        record.extend_from_slice(&header_buf);
        record.extend_from_slice(payload);

        // Add to buffer
        {
            let mut buffer = self.buffer.lock();
            buffer.append(lsn, &record);
        }

        // Update prev_lsn for transaction
        self.set_prev_lsn(txn_id, lsn);

        // Maybe rotate segment
        self.maybe_rotate_segment()?;

        Ok(lsn)
    }

    /// Flush the buffer to disk.
    pub fn flush_buffer(&self) -> Result<()> {
        let (data, _lsns) = {
            let mut buffer = self.buffer.lock();
            if buffer.is_empty() {
                return Ok(());
            }
            buffer.take()
        };

        {
            let mut segment = self.current_segment.lock();
            segment.write(&data)?;
            segment.sync()?;
        }

        // Update flushed LSN
        let current = self.current_lsn.load(Ordering::SeqCst);
        self.flushed_lsn.store(current - 1, Ordering::SeqCst);

        // Notify waiters
        self.flush_notify.notify_waiters();

        Ok(())
    }

    /// Maybe rotate to a new segment if current is full.
    fn maybe_rotate_segment(&self) -> Result<()> {
        let needs_rotation = {
            let segment = self.current_segment.lock();
            segment.size() as usize >= self.segment_size
        };

        if needs_rotation {
            self.rotate_segment()?;
        }

        Ok(())
    }

    /// Rotate to a new segment.
    fn rotate_segment(&self) -> Result<()> {
        // Flush current buffer first
        self.flush_buffer()?;

        let mut current = self.current_segment.lock();
        let new_segment_id = current.segment_id + 1;
        let new_start_lsn = Lsn(self.current_lsn.load(Ordering::SeqCst));

        let new_segment = WalSegment::create(&self.segment_dir, new_segment_id, new_start_lsn)?;
        *current = new_segment;

        Ok(())
    }

    /// Get the flushed LSN.
    pub fn flushed_lsn(&self) -> Lsn {
        Lsn(self.flushed_lsn.load(Ordering::SeqCst))
    }
}

#[async_trait]
impl super::WalWriter for WalWriterImpl {
    async fn append(&self, entry: WalEntry) -> Result<Lsn> {
        let record_type = match &entry.entry_type {
            WalEntryType::Insert { .. } => WalRecordType::Insert,
            WalEntryType::Update { .. } => WalRecordType::Update,
            WalEntryType::Delete { .. } => WalRecordType::Delete,
            WalEntryType::Commit => WalRecordType::Commit,
            WalEntryType::Abort => WalRecordType::Abort,
            WalEntryType::Checkpoint { .. } => WalRecordType::Checkpoint,
            WalEntryType::CreateTable { .. } => WalRecordType::CreateTable,
            WalEntryType::DropTable { .. } => WalRecordType::DropTable,
        };

        let lsn = self.write_record(entry.txn_id, entry.table_id, record_type, &entry.data)?;

        // For commit/abort, clean up transaction tracking
        if matches!(entry.entry_type, WalEntryType::Commit | WalEntryType::Abort) {
            self.remove_txn(entry.txn_id);
        }

        // Sync if configured
        if self.sync_commit {
            self.flush_buffer()?;
        }

        Ok(lsn)
    }

    async fn sync(&self) -> Result<()> {
        self.flush_buffer()
    }

    async fn truncate_before(&self, lsn: Lsn) -> Result<()> {
        // Find and delete segments whose records are entirely before the given LSN.
        // We read the first record header from each segment to get its starting LSN,
        // then delete segments where the NEXT segment's start LSN <= truncation LSN
        // (meaning all records in the candidate segment are before the checkpoint).
        let segment_dir = &self.segment_dir;

        // List all segment files
        let mut segments: Vec<(u64, PathBuf)> = std::fs::read_dir(segment_dir)
            .map_err(|e| Error::Internal(format!("Failed to read WAL directory: {}", e)))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("wal_") && n.ends_with(".log"))
                    .unwrap_or(false)
            })
            .filter_map(|entry| {
                let path = entry.path();
                let filename = path.file_name()?.to_str()?;
                let segment_id = u64::from_str_radix(&filename[4..20], 16).ok()?;
                Some((segment_id, path))
            })
            .collect();

        // Sort by segment ID
        segments.sort_by_key(|(id, _)| *id);

        if segments.len() <= 2 {
            // Never delete if only 1-2 segments exist (safety margin)
            return Ok(());
        }

        // Keep track of the current segment to avoid deleting it
        let current_segment_id = self.current_segment.lock().segment_id;

        // Read the first record LSN from each segment for LSN-aware truncation
        let segment_start_lsns: Vec<(u64, PathBuf, u64)> = segments
            .into_iter()
            .map(|(seg_id, path)| {
                let first_lsn = read_segment_first_lsn(&path).unwrap_or(0);
                (seg_id, path, first_lsn)
            })
            .collect();

        // Delete segments where ALL records are before the truncation LSN.
        // A segment is safe to delete when:
        // 1. It is NOT the current segment or the one immediately before it
        // 2. The NEXT segment's first LSN <= truncation LSN (meaning this segment
        //    contains only records older than the checkpoint)
        for i in 0..segment_start_lsns.len() {
            let (segment_id, ref path, _first_lsn) = segment_start_lsns[i];

            // Never delete the current segment or the one immediately before it
            if segment_id >= current_segment_id {
                break;
            }
            if segment_id + 1 >= current_segment_id {
                continue;
            }

            // Check if the next segment's first LSN is <= truncation LSN
            // If so, all records in THIS segment are before the checkpoint
            if let Some((_next_seg_id, _next_path, next_first_lsn)) = segment_start_lsns.get(i + 1) {
                if *next_first_lsn > 0 && *next_first_lsn <= lsn.0 {
                    tracing::info!(
                        segment_id = segment_id,
                        first_lsn = _first_lsn,
                        truncation_lsn = lsn.0,
                        "Deleting old WAL segment: {:?}", path
                    );
                    if let Err(e) = std::fs::remove_file(path) {
                        tracing::warn!("Failed to delete WAL segment {:?}: {}", path, e);
                    }
                }
            }
        }

        Ok(())
    }

    fn current_lsn(&self) -> Lsn {
        Lsn(self.current_lsn.load(Ordering::SeqCst))
    }
}

/// Read the first record's LSN from a WAL segment file.
/// Returns 0 if the file is empty or unreadable.
fn read_segment_first_lsn(path: &Path) -> Option<u64> {
    use std::io::Read;
    let mut file = File::open(path).ok()?;
    let mut buf = [0u8; 8]; // LSN is the first 8 bytes (u64 LE)
    file.read_exact(&mut buf).ok()?;
    Some(u64::from_le_bytes(buf))
}

/// WAL reader for recovery
pub struct WalReader {
    segment_dir: PathBuf,
    current_segment: Option<WalSegment>,
    current_position: u64,
}

impl WalReader {
    pub fn new(dir: &Path) -> Self {
        Self {
            segment_dir: dir.to_path_buf(),
            current_segment: None,
            current_position: 0,
        }
    }

    /// Seek to a specific LSN.
    ///
    /// This finds the segment containing the LSN and positions the reader
    /// just before the record at that LSN.
    pub fn seek_to_lsn(&mut self, target_lsn: Lsn) -> Result<()> {
        // List all segment files
        let mut segments: Vec<(u64, PathBuf)> = std::fs::read_dir(&self.segment_dir)
            .map_err(|e| Error::Internal(format!("Failed to read WAL directory: {}", e)))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("wal_") && n.ends_with(".log"))
                    .unwrap_or(false)
            })
            .filter_map(|entry| {
                let path = entry.path();
                let filename = path.file_name()?.to_str()?;
                let segment_id = u64::from_str_radix(&filename[4..20], 16).ok()?;
                Some((segment_id, path))
            })
            .collect();

        // Sort by segment ID (ascending)
        segments.sort_by_key(|(id, _)| *id);

        // Find the segment that might contain our LSN
        // We need to read segments to find the right one
        let mut found_segment: Option<WalSegment> = None;

        for (segment_id, path) in segments {
            // Open the segment and read its first record to get start_lsn
            let mut segment = WalSegment::open(&path)?;

            // Read the first header to get the start_lsn
            let mut header_buf = [0u8; 40];
            use std::io::{Read, Seek, SeekFrom};
            segment.file.seek(SeekFrom::Start(0))
                .map_err(|e| Error::Internal(format!("Failed to seek: {}", e)))?;

            if segment.file.read(&mut header_buf)
                .map_err(|e| Error::Internal(format!("Failed to read: {}", e)))? < 40 {
                // Empty segment, continue to next
                continue;
            }

            segment.file.seek(SeekFrom::Start(0))
                .map_err(|e| Error::Internal(format!("Failed to seek: {}", e)))?;

            // Parse the header to get LSN
            let header = WalRecordHeader::read_from(&header_buf)?;
            segment.start_lsn = Lsn(header.lsn);

            // Check if target LSN could be in this segment
            if target_lsn.0 >= header.lsn {
                found_segment = Some(segment);
            } else {
                // Target LSN is before this segment, use the previous one
                break;
            }
        }

        if let Some(mut segment) = found_segment {
            // Now scan the segment to find the exact position of the target LSN
            use std::io::{Read, Seek, SeekFrom};
            segment.file.seek(SeekFrom::Start(0))
                .map_err(|e| Error::Internal(format!("Failed to seek: {}", e)))?;

            let mut position: u64 = 0;
            let mut header_buf = [0u8; 40];

            loop {
                // Read header
                let bytes_read = segment.file.read(&mut header_buf)
                    .map_err(|e| Error::Internal(format!("Failed to read: {}", e)))?;

                if bytes_read < 40 {
                    // End of segment
                    break;
                }

                let header = WalRecordHeader::read_from(&header_buf)?;

                if header.lsn >= target_lsn.0 {
                    // Found the position, seek back to the beginning of this record
                    segment.file.seek(SeekFrom::Start(position))
                        .map_err(|e| Error::Internal(format!("Failed to seek: {}", e)))?;
                    break;
                }

                // Move past the record payload
                position += 40 + header.length as u64;
                segment.file.seek(SeekFrom::Start(position))
                    .map_err(|e| Error::Internal(format!("Failed to seek: {}", e)))?;
            }

            self.current_segment = Some(segment);
            self.current_position = position;
        }

        Ok(())
    }

    /// Read the next record.
    pub fn read_next(&mut self) -> Result<Option<(WalRecordHeader, Bytes)>> {
        loop {
            let segment = match &mut self.current_segment {
                Some(seg) => seg,
                None => {
                    // Open first segment if we haven't opened any
                    self.open_next_segment()?;
                    match &mut self.current_segment {
                        Some(seg) => seg,
                        None => return Ok(None), // No segments at all
                    }
                }
            };

            // Read header
            let mut header_buf = [0u8; WAL_HEADER_SIZE];
            match segment.file.read_exact(&mut header_buf) {
                Ok(()) => {
                    // Successfully read header, continue to read payload
                    break self.read_record_payload(header_buf);
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Try next segment
                    self.open_next_segment()?;
                    if self.current_segment.is_none() {
                        return Ok(None); // No more segments
                    }
                    // Continue loop to try reading from next segment
                }
                Err(e) => {
                    return Err(Error::Storage(
                        thunder_common::error::StorageError::WalWriteFailed(e.to_string()),
                    ))
                }
            }
        }
    }

    /// Read record payload after successfully reading header
    fn read_record_payload(&mut self, header_buf: [u8; WAL_HEADER_SIZE]) -> Result<Option<(WalRecordHeader, Bytes)>> {
        let segment = self.current_segment.as_mut().ok_or_else(|| {
            Error::Internal("No WAL segment open during read".into())
        })?;

        let header = WalRecordHeader::read_from(&header_buf)?;

        // Read payload
        let mut payload = vec![0u8; header.length as usize];
        segment.file.read_exact(&mut payload).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::WalWriteFailed(
                e.to_string(),
            ))
        })?;

        // Verify checksum
        let computed_checksum = header.compute_checksum(&payload);
        if computed_checksum != header.checksum {
            return Err(Error::Storage(
                thunder_common::error::StorageError::WalWriteFailed(format!(
                    "WAL record checksum mismatch at LSN {}: expected {:08x}, got {:08x}",
                    header.lsn, header.checksum, computed_checksum
                )),
            ));
        }

        self.current_position += WAL_HEADER_SIZE as u64 + header.length as u64;

        Ok(Some((header, Bytes::from(payload))))
    }

    fn open_next_segment(&mut self) -> Result<()> {
        let next_id = self
            .current_segment
            .as_ref()
            .map(|s| s.segment_id + 1)
            .unwrap_or(0);

        let filename = format!("wal_{:016x}.log", next_id);
        let path = self.segment_dir.join(filename);

        if path.exists() {
            self.current_segment = Some(WalSegment::open(&path)?);
            self.current_position = 0;
        } else {
            self.current_segment = None;
        }

        Ok(())
    }
}

// ============================================================================
// ARIES-Style WAL Recovery
// ============================================================================

/// Transaction state during recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryTxnState {
    /// Transaction is active (uncommitted)
    Active,
    /// Transaction committed
    Committed,
    /// Transaction aborted
    Aborted,
}

/// Transaction information tracked during recovery.
#[derive(Debug, Clone)]
pub struct RecoveryTxnInfo {
    /// Current state
    pub state: RecoveryTxnState,
    /// Last LSN for this transaction
    pub last_lsn: Lsn,
    /// Undo next LSN (for rollback)
    pub undo_next_lsn: Lsn,
}

/// Dirty page entry for recovery.
#[derive(Debug, Clone)]
pub struct DirtyPageEntry {
    /// Page ID
    pub page_id: PageId,
    /// Recovery LSN - first LSN that might have dirtied this page
    pub recovery_lsn: Lsn,
}

/// Result of the analysis phase.
#[derive(Debug)]
pub struct AnalysisResult {
    /// Active transactions at time of crash
    pub active_txns: HashMap<TxnId, RecoveryTxnInfo>,
    /// Dirty pages that may need redo
    pub dirty_pages: HashMap<PageId, DirtyPageEntry>,
    /// Starting LSN for redo phase
    pub redo_lsn: Lsn,
    /// Committed transactions
    pub committed_txns: HashSet<TxnId>,
}

/// Record to redo during recovery.
#[derive(Debug, Clone)]
pub struct RedoRecord {
    pub lsn: Lsn,
    pub txn_id: TxnId,
    pub record_type: WalRecordType,
    pub table_id: TableId,
    pub payload: Bytes,
}

/// Record to undo during recovery.
#[derive(Debug, Clone)]
pub struct UndoRecord {
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub txn_id: TxnId,
    pub record_type: WalRecordType,
    pub table_id: TableId,
    pub payload: Bytes,
}

/// ARIES-style WAL recovery manager.
///
/// Recovery proceeds in three phases:
/// 1. **Analysis**: Scan log from last checkpoint to end, determining:
///    - Which transactions were active at crash
///    - Which pages may need redo
///    - Starting point for redo
///
/// 2. **Redo**: Scan from redo_lsn to end, replaying all logged actions
///    for pages that need recovery (history repeating)
///
/// 3. **Undo**: Roll back all uncommitted transactions by following their
///    prev_lsn chains and applying compensating actions
pub struct WalRecovery {
    /// WAL directory
    wal_dir: PathBuf,
    /// Last known checkpoint LSN
    checkpoint_lsn: Option<Lsn>,
}

impl WalRecovery {
    /// Create a new recovery manager.
    pub fn new(wal_dir: &Path) -> Self {
        Self {
            wal_dir: wal_dir.to_path_buf(),
            checkpoint_lsn: None,
        }
    }

    /// Create with a known checkpoint LSN.
    pub fn with_checkpoint(wal_dir: &Path, checkpoint_lsn: Lsn) -> Self {
        Self {
            wal_dir: wal_dir.to_path_buf(),
            checkpoint_lsn: Some(checkpoint_lsn),
        }
    }

    /// Perform full ARIES recovery.
    ///
    /// Returns the redo and undo records that need to be applied.
    pub fn recover(&self) -> Result<RecoveryPlan> {
        tracing::info!("Starting WAL recovery from {:?}", self.wal_dir);

        // Phase 1: Analysis
        let analysis = self.analysis_phase()?;
        tracing::info!(
            "Analysis complete: {} active txns, {} dirty pages, redo from {:?}",
            analysis.active_txns.len(),
            analysis.dirty_pages.len(),
            analysis.redo_lsn
        );

        // Phase 2: Collect redo records
        let redo_records = self.collect_redo_records(&analysis)?;
        tracing::info!("Redo phase: {} records to replay", redo_records.len());

        // Phase 3: Collect undo records
        let undo_records = self.collect_undo_records(&analysis)?;
        tracing::info!("Undo phase: {} records to rollback", undo_records.len());

        Ok(RecoveryPlan {
            analysis,
            redo_records,
            undo_records,
        })
    }

    /// Phase 1: Analysis - scan log to determine recovery state.
    fn analysis_phase(&self) -> Result<AnalysisResult> {
        let mut reader = WalReader::new(&self.wal_dir);
        let mut active_txns: HashMap<TxnId, RecoveryTxnInfo> = HashMap::new();
        let mut dirty_pages: HashMap<PageId, DirtyPageEntry> = HashMap::new();
        let mut committed_txns: HashSet<TxnId> = HashSet::new();
        let mut redo_lsn = Lsn(u64::MAX);

        // If we have a checkpoint, seek to it and load initial state
        if let Some(checkpoint_lsn) = self.checkpoint_lsn {
            reader.seek_to_lsn(checkpoint_lsn)?;
        }

        // Scan all records from checkpoint (or start) to end
        while let Some((header, payload)) = reader.read_next()? {
            let lsn = Lsn(header.lsn);
            let txn_id = TxnId(header.txn_id);
            let record_type = WalRecordType::try_from(header.record_type)?;

            // Track minimum LSN for redo
            if lsn < redo_lsn {
                redo_lsn = lsn;
            }

            match record_type {
                WalRecordType::BeginTxn => {
                    active_txns.insert(
                        txn_id,
                        RecoveryTxnInfo {
                            state: RecoveryTxnState::Active,
                            last_lsn: lsn,
                            undo_next_lsn: lsn,
                        },
                    );
                }

                WalRecordType::Insert | WalRecordType::Update | WalRecordType::Delete => {
                    // Update transaction's last LSN
                    if let Some(info) = active_txns.get_mut(&txn_id) {
                        info.last_lsn = lsn;
                        info.undo_next_lsn = Lsn(header.prev_lsn);
                    } else {
                        // Transaction started before checkpoint
                        active_txns.insert(
                            txn_id,
                            RecoveryTxnInfo {
                                state: RecoveryTxnState::Active,
                                last_lsn: lsn,
                                undo_next_lsn: Lsn(header.prev_lsn),
                            },
                        );
                    }

                    // Track dirty pages for redo
                    if record_type == WalRecordType::Insert
                        || record_type == WalRecordType::Update
                        || record_type == WalRecordType::Delete
                    {
                        // Extract page_id from payload
                        if payload.len() >= 16 {
                            let page_id = PageId((&payload[8..16]).get_u64_le());
                            dirty_pages.entry(page_id).or_insert(DirtyPageEntry {
                                page_id,
                                recovery_lsn: lsn,
                            });
                        }
                    }
                }

                WalRecordType::Commit => {
                    if let Some(info) = active_txns.get_mut(&txn_id) {
                        info.state = RecoveryTxnState::Committed;
                    }
                    committed_txns.insert(txn_id);
                }

                WalRecordType::Abort => {
                    if let Some(info) = active_txns.get_mut(&txn_id) {
                        info.state = RecoveryTxnState::Aborted;
                    }
                }

                WalRecordType::Checkpoint => {
                    // Parse checkpoint record for initial state
                    if let Ok(checkpoint) = CheckpointRecord::decode(&payload) {
                        // Load active transactions from checkpoint
                        for (chk_txn_id, chk_lsn) in checkpoint.active_txns {
                            active_txns.entry(chk_txn_id).or_insert(RecoveryTxnInfo {
                                state: RecoveryTxnState::Active,
                                last_lsn: chk_lsn,
                                undo_next_lsn: chk_lsn,
                            });
                        }
                        // Load dirty pages from checkpoint
                        for (chk_page_id, chk_lsn) in checkpoint.dirty_pages {
                            dirty_pages.entry(chk_page_id).or_insert(DirtyPageEntry {
                                page_id: chk_page_id,
                                recovery_lsn: chk_lsn,
                            });
                            // Update redo_lsn if needed
                            if chk_lsn < redo_lsn {
                                redo_lsn = chk_lsn;
                            }
                        }
                    }
                }

                WalRecordType::Clr => {
                    // Compensation Log Record - update undo_next_lsn
                    if let Some(info) = active_txns.get_mut(&txn_id) {
                        info.last_lsn = lsn;
                        // CLR's prev_lsn points to the next record to undo
                        info.undo_next_lsn = Lsn(header.prev_lsn);
                    }
                }

                WalRecordType::PageSplit | WalRecordType::PageMerge => {
                    // B+Tree operations - track as dirty pages
                    if payload.len() >= 8 {
                        let page_id = PageId((&payload[..8]).get_u64_le());
                        dirty_pages.entry(page_id).or_insert(DirtyPageEntry {
                            page_id,
                            recovery_lsn: lsn,
                        });
                    }
                }

                WalRecordType::CreateTable | WalRecordType::DropTable => {
                    // DDL operations use TxnId(0) and don't need transaction tracking.
                    // They will be replayed during redo phase to recover the catalog.
                    // No dirty page tracking needed as catalog is in-memory.
                }
            }
        }

        // Filter out committed/aborted transactions - we only need to undo active ones
        active_txns.retain(|_, info| info.state == RecoveryTxnState::Active);

        // If we found no records, start from LSN 1
        if redo_lsn == Lsn(u64::MAX) {
            redo_lsn = Lsn(1);
        }

        Ok(AnalysisResult {
            active_txns,
            dirty_pages,
            redo_lsn,
            committed_txns,
        })
    }

    /// Phase 2: Collect records that need to be redone.
    fn collect_redo_records(&self, analysis: &AnalysisResult) -> Result<Vec<RedoRecord>> {
        let mut reader = WalReader::new(&self.wal_dir);
        let mut redo_records = Vec::new();

        // Start from the redo LSN
        reader.seek_to_lsn(analysis.redo_lsn)?;

        while let Some((header, payload)) = reader.read_next()? {
            let lsn = Lsn(header.lsn);
            let record_type = WalRecordType::try_from(header.record_type)?;

            // Only redo data modification and DDL records
            match record_type {
                WalRecordType::Insert
                | WalRecordType::Update
                | WalRecordType::Delete
                | WalRecordType::PageSplit
                | WalRecordType::PageMerge
                | WalRecordType::CreateTable
                | WalRecordType::DropTable => {
                    // Check if page needs redo (page LSN < log LSN)
                    // In practice, we'd check the page header here
                    // For now, we collect all records for the caller to filter
                    redo_records.push(RedoRecord {
                        lsn,
                        txn_id: TxnId(header.txn_id),
                        record_type,
                        table_id: TableId(header.table_id),
                        payload: payload.clone(),
                    });
                }
                _ => {}
            }
        }

        Ok(redo_records)
    }

    /// Phase 3: Collect records that need to be undone.
    fn collect_undo_records(&self, analysis: &AnalysisResult) -> Result<Vec<UndoRecord>> {
        let mut undo_records = Vec::new();
        let mut reader = WalReader::new(&self.wal_dir);

        // For each active (uncommitted) transaction, walk its undo chain
        for (txn_id, info) in &analysis.active_txns {
            let mut current_lsn = info.last_lsn;

            while current_lsn.0 > 0 {
                reader.seek_to_lsn(current_lsn)?;

                if let Some((header, payload)) = reader.read_next()? {
                    let record_type = WalRecordType::try_from(header.record_type)?;

                    // Only undo data modification records
                    match record_type {
                        WalRecordType::Insert
                        | WalRecordType::Update
                        | WalRecordType::Delete => {
                            undo_records.push(UndoRecord {
                                lsn: Lsn(header.lsn),
                                prev_lsn: Lsn(header.prev_lsn),
                                txn_id: *txn_id,
                                record_type,
                                table_id: TableId(header.table_id),
                                payload: payload.clone(),
                            });
                        }
                        _ => {}
                    }

                    // Follow prev_lsn chain
                    current_lsn = Lsn(header.prev_lsn);
                } else {
                    break;
                }
            }
        }

        // Sort undo records by LSN in descending order (undo in reverse)
        undo_records.sort_by(|a, b| b.lsn.cmp(&a.lsn));

        Ok(undo_records)
    }
}

/// Recovery plan containing all records to redo and undo.
#[derive(Debug)]
pub struct RecoveryPlan {
    /// Analysis results
    pub analysis: AnalysisResult,
    /// Records to redo (in LSN order)
    pub redo_records: Vec<RedoRecord>,
    /// Records to undo (in reverse LSN order)
    pub undo_records: Vec<UndoRecord>,
}

impl RecoveryPlan {
    /// Check if recovery is needed.
    pub fn needs_recovery(&self) -> bool {
        !self.redo_records.is_empty() || !self.undo_records.is_empty()
    }

    /// Get number of redo records.
    pub fn redo_count(&self) -> usize {
        self.redo_records.len()
    }

    /// Get number of undo records.
    pub fn undo_count(&self) -> usize {
        self.undo_records.len()
    }

    /// Get active (uncommitted) transaction count.
    pub fn active_txn_count(&self) -> usize {
        self.analysis.active_txns.len()
    }
}

/// Recovery executor that applies the recovery plan.
pub struct RecoveryExecutor<'a, F, G>
where
    F: FnMut(&RedoRecord) -> Result<()>,
    G: FnMut(&UndoRecord) -> Result<()>,
{
    plan: &'a RecoveryPlan,
    redo_fn: F,
    undo_fn: G,
}

impl<'a, F, G> RecoveryExecutor<'a, F, G>
where
    F: FnMut(&RedoRecord) -> Result<()>,
    G: FnMut(&UndoRecord) -> Result<()>,
{
    /// Create a new recovery executor.
    pub fn new(plan: &'a RecoveryPlan, redo_fn: F, undo_fn: G) -> Self {
        Self {
            plan,
            redo_fn,
            undo_fn,
        }
    }

    /// Execute the recovery plan.
    pub fn execute(&mut self) -> Result<RecoveryStats> {
        let mut stats = RecoveryStats::default();

        // Phase 2: Redo
        tracing::info!("Executing redo phase: {} records", self.plan.redo_records.len());
        for record in &self.plan.redo_records {
            match (self.redo_fn)(record) {
                Ok(()) => stats.records_redone += 1,
                Err(e) => {
                    tracing::warn!("Redo failed for LSN {:?}: {}", record.lsn, e);
                    stats.redo_errors += 1;
                }
            }
        }

        // Phase 3: Undo
        tracing::info!("Executing undo phase: {} records", self.plan.undo_records.len());
        for record in &self.plan.undo_records {
            match (self.undo_fn)(record) {
                Ok(()) => stats.records_undone += 1,
                Err(e) => {
                    tracing::warn!("Undo failed for LSN {:?}: {}", record.lsn, e);
                    stats.undo_errors += 1;
                }
            }
        }

        stats.txns_rolled_back = self.plan.analysis.active_txns.len() as u64;

        Ok(stats)
    }
}

/// Statistics from recovery execution.
#[derive(Debug, Default)]
pub struct RecoveryStats {
    /// Number of records successfully redone
    pub records_redone: u64,
    /// Number of redo errors
    pub redo_errors: u64,
    /// Number of records successfully undone
    pub records_undone: u64,
    /// Number of undo errors
    pub undo_errors: u64,
    /// Number of transactions rolled back
    pub txns_rolled_back: u64,
}

impl RecoveryStats {
    /// Check if recovery completed successfully.
    pub fn is_successful(&self) -> bool {
        self.redo_errors == 0 && self.undo_errors == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WalWriter;
    use tempfile::tempdir;

    #[test]
    fn test_wal_record_header() {
        let header = WalRecordHeader::new(
            Lsn(100),
            Lsn(50),
            TxnId(1),
            WalRecordType::Insert,
            TableId(42),
            128,
        );

        let mut buf = [0u8; WAL_HEADER_SIZE];
        header.write_to(&mut buf);

        let decoded = WalRecordHeader::read_from(&buf).unwrap();
        assert_eq!(decoded.lsn, 100);
        assert_eq!(decoded.prev_lsn, 50);
        assert_eq!(decoded.txn_id, 1);
        assert_eq!(decoded.record_type, WalRecordType::Insert as u8);
        assert_eq!(decoded.table_id, 42);
        assert_eq!(decoded.length, 128);
    }

    #[test]
    fn test_insert_record() {
        let record = InsertRecord {
            row_id: RowId(1),
            page_id: PageId(10),
            slot_id: 5,
            tuple_data: Bytes::from_static(b"test data"),
        };

        let encoded = record.encode();
        let decoded = InsertRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.row_id, record.row_id);
        assert_eq!(decoded.page_id, record.page_id);
        assert_eq!(decoded.slot_id, record.slot_id);
        assert_eq!(decoded.tuple_data, record.tuple_data);
    }

    #[test]
    fn test_update_record() {
        let record = UpdateRecord {
            row_id: RowId(1),
            page_id: PageId(10),
            slot_id: 5,
            before_image: Bytes::from_static(b"old"),
            after_image: Bytes::from_static(b"new"),
        };

        let encoded = record.encode();
        let decoded = UpdateRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.row_id, record.row_id);
        assert_eq!(decoded.before_image, record.before_image);
        assert_eq!(decoded.after_image, record.after_image);
    }

    #[test]
    fn test_delete_record() {
        let record = DeleteRecord {
            row_id: RowId(1),
            page_id: PageId(10),
            slot_id: 5,
            before_image: Bytes::from_static(b"deleted data"),
        };

        let encoded = record.encode();
        let decoded = DeleteRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.row_id, record.row_id);
        assert_eq!(decoded.before_image, record.before_image);
    }

    #[test]
    fn test_checkpoint_record() {
        let record = CheckpointRecord {
            active_txns: vec![(TxnId(1), Lsn(100)), (TxnId(2), Lsn(200))],
            dirty_pages: vec![(PageId(10), Lsn(50)), (PageId(20), Lsn(150))],
        };

        let encoded = record.encode();
        let decoded = CheckpointRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.active_txns.len(), 2);
        assert_eq!(decoded.dirty_pages.len(), 2);
        assert_eq!(decoded.active_txns[0], (TxnId(1), Lsn(100)));
    }

    #[test]
    fn test_wal_writer_basic() {
        let dir = tempdir().unwrap();
        let wal = WalWriterImpl::new(dir.path(), false).unwrap();

        let lsn1 = wal
            .write_record(
                TxnId(1),
                TableId(1),
                WalRecordType::Insert,
                b"test data",
            )
            .unwrap();

        let lsn2 = wal
            .write_record(
                TxnId(1),
                TableId(1),
                WalRecordType::Commit,
                &[],
            )
            .unwrap();

        assert!(lsn2 > lsn1);
        assert_eq!(wal.current_lsn().0, lsn2.0 + 1);
    }

    #[test]
    fn test_wal_writer_flush() {
        let dir = tempdir().unwrap();
        let wal = WalWriterImpl::new(dir.path(), false).unwrap();

        wal.write_record(TxnId(1), TableId(1), WalRecordType::Insert, b"test")
            .unwrap();

        wal.flush_buffer().unwrap();

        // Verify file exists and has content
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!files.is_empty());
    }

    #[tokio::test]
    async fn test_wal_writer_trait() {
        let dir = tempdir().unwrap();
        let wal = WalWriterImpl::new(dir.path(), true).unwrap();

        let entry = WalEntry {
            lsn: Lsn(0),
            txn_id: TxnId(1),
            entry_type: WalEntryType::Insert { row_id: RowId(1) },
            table_id: TableId(1),
            data: b"test tuple".to_vec(),
        };

        let lsn = wal.append(entry).await.unwrap();
        assert!(lsn > Lsn(0));

        let commit_entry = WalEntry {
            lsn: Lsn(0),
            txn_id: TxnId(1),
            entry_type: WalEntryType::Commit,
            table_id: TableId(0),
            data: vec![],
        };

        let commit_lsn = wal.append(commit_entry).await.unwrap();
        assert!(commit_lsn > lsn);
    }

    #[test]
    fn test_wal_checksum() {
        let header = WalRecordHeader::new(
            Lsn(1),
            Lsn(0),
            TxnId(1),
            WalRecordType::Insert,
            TableId(1),
            4,
        );

        let payload = b"test";
        let checksum = header.compute_checksum(payload);
        assert_ne!(checksum, 0);

        // Different payload should have different checksum
        let checksum2 = header.compute_checksum(b"diff");
        assert_ne!(checksum, checksum2);
    }

    #[test]
    fn test_recovery_committed_txn() {
        let dir = tempdir().unwrap();
        let wal = WalWriterImpl::new(dir.path(), false).unwrap();

        // Write a complete transaction (insert + commit)
        let insert = InsertRecord {
            row_id: RowId(1),
            page_id: PageId(10),
            slot_id: 0,
            tuple_data: Bytes::from_static(b"test data"),
        };

        wal.write_record(TxnId(1), TableId(1), WalRecordType::Insert, &insert.encode())
            .unwrap();
        wal.write_record(TxnId(1), TableId(1), WalRecordType::Commit, &[])
            .unwrap();
        wal.flush_buffer().unwrap();

        // Perform recovery
        let recovery = WalRecovery::new(dir.path());
        let plan = recovery.recover().unwrap();

        // Committed transaction should have redo records but no undo
        assert!(plan.redo_count() > 0);
        assert_eq!(plan.undo_count(), 0);
        assert_eq!(plan.active_txn_count(), 0);
    }

    #[test]
    fn test_recovery_uncommitted_txn() {
        let dir = tempdir().unwrap();
        let wal = WalWriterImpl::new(dir.path(), false).unwrap();

        // Write an incomplete transaction (insert without commit)
        let insert = InsertRecord {
            row_id: RowId(1),
            page_id: PageId(10),
            slot_id: 0,
            tuple_data: Bytes::from_static(b"uncommitted data"),
        };

        wal.write_record(TxnId(1), TableId(1), WalRecordType::Insert, &insert.encode())
            .unwrap();
        wal.flush_buffer().unwrap();

        // Perform recovery
        let recovery = WalRecovery::new(dir.path());
        let plan = recovery.recover().unwrap();

        // Uncommitted transaction should have both redo and undo records
        assert!(plan.redo_count() > 0);
        assert!(plan.undo_count() > 0);
        assert_eq!(plan.active_txn_count(), 1);
    }

    #[test]
    fn test_recovery_multiple_txns() {
        let dir = tempdir().unwrap();
        let wal = WalWriterImpl::new(dir.path(), false).unwrap();

        // Txn 1: committed
        let insert1 = InsertRecord {
            row_id: RowId(1),
            page_id: PageId(10),
            slot_id: 0,
            tuple_data: Bytes::from_static(b"data1"),
        };
        wal.write_record(TxnId(1), TableId(1), WalRecordType::Insert, &insert1.encode())
            .unwrap();
        wal.write_record(TxnId(1), TableId(1), WalRecordType::Commit, &[])
            .unwrap();

        // Txn 2: uncommitted
        let insert2 = InsertRecord {
            row_id: RowId(2),
            page_id: PageId(20),
            slot_id: 0,
            tuple_data: Bytes::from_static(b"data2"),
        };
        wal.write_record(TxnId(2), TableId(1), WalRecordType::Insert, &insert2.encode())
            .unwrap();

        // Txn 3: aborted
        let insert3 = InsertRecord {
            row_id: RowId(3),
            page_id: PageId(30),
            slot_id: 0,
            tuple_data: Bytes::from_static(b"data3"),
        };
        wal.write_record(TxnId(3), TableId(1), WalRecordType::Insert, &insert3.encode())
            .unwrap();
        wal.write_record(TxnId(3), TableId(1), WalRecordType::Abort, &[])
            .unwrap();

        wal.flush_buffer().unwrap();

        // Perform recovery
        let recovery = WalRecovery::new(dir.path());
        let plan = recovery.recover().unwrap();

        // Only txn 2 is active (uncommitted and not aborted)
        assert_eq!(plan.active_txn_count(), 1);
        assert!(plan.analysis.active_txns.contains_key(&TxnId(2)));
        assert!(plan.analysis.committed_txns.contains(&TxnId(1)));
    }

    #[test]
    fn test_recovery_executor() {
        let dir = tempdir().unwrap();
        let wal = WalWriterImpl::new(dir.path(), false).unwrap();

        // Write some records
        let insert = InsertRecord {
            row_id: RowId(1),
            page_id: PageId(10),
            slot_id: 0,
            tuple_data: Bytes::from_static(b"test"),
        };
        wal.write_record(TxnId(1), TableId(1), WalRecordType::Insert, &insert.encode())
            .unwrap();
        wal.flush_buffer().unwrap();

        // Perform recovery
        let recovery = WalRecovery::new(dir.path());
        let plan = recovery.recover().unwrap();

        // Execute with mock callbacks
        let mut redo_count = 0;
        let mut undo_count = 0;

        let mut executor = RecoveryExecutor::new(
            &plan,
            |_| { redo_count += 1; Ok(()) },
            |_| { undo_count += 1; Ok(()) },
        );

        let stats = executor.execute().unwrap();
        assert!(stats.is_successful());
        assert_eq!(stats.records_redone, redo_count);
        assert_eq!(stats.records_undone, undo_count);
    }

    #[test]
    fn test_recovery_plan_needs_recovery() {
        let dir = tempdir().unwrap();

        // Empty WAL - no recovery needed
        let _ = WalWriterImpl::new(dir.path(), false).unwrap();
        let recovery = WalRecovery::new(dir.path());
        let plan = recovery.recover().unwrap();

        // Empty WAL should not need recovery
        assert!(!plan.needs_recovery());
    }
}
