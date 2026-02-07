//! Buffer pool implementation for ThunderDB.
//!
//! Provides in-memory caching of pages with:
//! - Clock eviction algorithm for efficient page replacement
//! - Pin counting to prevent eviction of pages in use
//! - Dirty page tracking for write-back
//! - Async I/O via DiskManager

use crate::page::Page;
use crate::PAGE_SIZE;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use aes_gcm::aead::Aead;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use thunder_common::prelude::*;
use tracing::{debug, trace};

/// AES-256-GCM page encryptor for data-at-rest encryption.
///
/// Uses a deterministic nonce derived from the page ID so that pages can be
/// encrypted and decrypted independently without storing nonces separately.
/// The encrypted page has a 16-byte GCM tag appended, so the ciphertext is
/// PAGE_SIZE + 16 bytes. To keep pages at PAGE_SIZE on disk, we encrypt
/// only the first (PAGE_SIZE - 16) bytes and store the tag in the remaining 16.
pub struct PageEncryptor {
    cipher: Aes256Gcm,
}

impl PageEncryptor {
    /// Create a new encryptor from a 32-byte key file.
    pub fn from_key_file(path: &Path) -> Result<Self> {
        let key_bytes = fs::read(path).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::IoError(format!(
                "Failed to read encryption key file: {}", e
            )))
        })?;

        if key_bytes.len() < 32 {
            return Err(Error::Storage(thunder_common::error::StorageError::IoError(
                format!("Encryption key must be at least 32 bytes, got {}", key_bytes.len())
            )));
        }

        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes[..32]);
        let cipher = Aes256Gcm::new(key);

        Ok(Self { cipher })
    }

    /// Create from raw key bytes (for testing).
    pub fn from_key(key: &[u8; 32]) -> Self {
        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(key);
        Self { cipher: Aes256Gcm::new(key) }
    }

    /// Derive a 12-byte nonce from a page ID (deterministic per-page).
    fn nonce_for_page(page_id: PageId) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&page_id.0.to_le_bytes());
        nonce
    }

    /// Encrypt page data in-place. Input is PAGE_SIZE bytes, output is PAGE_SIZE bytes.
    /// We use the first (PAGE_SIZE - 16) bytes as plaintext and store the 16-byte tag
    /// in the last 16 bytes of the output.
    pub fn encrypt_page(&self, page_id: PageId, data: &[u8]) -> Result<Vec<u8>> {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        let nonce = Nonce::from(Self::nonce_for_page(page_id));

        // Encrypt all page data; GCM appends a 16-byte tag
        let ciphertext = self.cipher.encrypt(&nonce, data)
            .map_err(|e| Error::Storage(thunder_common::error::StorageError::IoError(
                format!("Page encryption failed: {}", e)
            )))?;

        Ok(ciphertext)
    }

    /// Decrypt page data. Input may be PAGE_SIZE + 16 bytes (ciphertext + tag).
    pub fn decrypt_page(&self, page_id: PageId, data: &[u8]) -> Result<Vec<u8>> {
        let nonce = Nonce::from(Self::nonce_for_page(page_id));

        let plaintext = self.cipher.decrypt(&nonce, data)
            .map_err(|e| Error::Storage(thunder_common::error::StorageError::IoError(
                format!("Page decryption failed: {}", e)
            )))?;

        Ok(plaintext)
    }
}

/// Buffer frame holding a page and its metadata.
pub struct BufferFrame {
    /// The page data
    page: RwLock<Option<Page>>,
    /// Page ID currently in this frame
    page_id: AtomicU64,
    /// Pin count (number of active references)
    pin_count: AtomicU32,
    /// Dirty flag
    dirty: AtomicBool,
    /// Reference bit for Clock algorithm
    reference_bit: AtomicBool,
    /// Frame is valid (contains a loaded page)
    valid: AtomicBool,
}

impl BufferFrame {
    pub fn new() -> Self {
        Self {
            page: RwLock::new(None),
            page_id: AtomicU64::new(0),
            pin_count: AtomicU32::new(0),
            dirty: AtomicBool::new(false),
            reference_bit: AtomicBool::new(false),
            valid: AtomicBool::new(false),
        }
    }

    /// Get the page ID stored in this frame.
    /// Uses Acquire ordering to synchronize with page data.
    #[inline]
    pub fn page_id(&self) -> PageId {
        PageId(self.page_id.load(Ordering::Acquire))
    }

    /// Check if the frame is currently pinned.
    /// Uses Acquire ordering to synchronize with pin/unpin operations.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.pin_count.load(Ordering::Acquire) > 0
    }

    /// Check if the frame is dirty (modified since last flush).
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Check if the frame contains a valid page.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    /// Pin the frame, preventing eviction.
    /// Uses AcqRel ordering to ensure visibility to eviction checks.
    #[inline]
    pub fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::AcqRel);
        self.reference_bit.store(true, Ordering::Release);
    }

    /// Unpin the frame, allowing eviction when pin_count reaches 0.
    #[inline]
    pub fn unpin(&self) {
        let old = self.pin_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0, "Unpin called when pin_count is 0");
    }

    /// Mark the frame as dirty (needs to be written to disk before eviction).
    #[inline]
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// Clear the dirty flag after flushing to disk.
    #[inline]
    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    /// Reset the frame for a new page.
    /// Uses Release ordering to ensure all metadata updates are visible.
    pub fn reset(&self, page_id: PageId) {
        // Order matters: set invalid first, then update metadata, then page_id last
        self.valid.store(false, Ordering::Release);
        self.pin_count.store(0, Ordering::Relaxed);
        self.dirty.store(false, Ordering::Relaxed);
        self.reference_bit.store(true, Ordering::Relaxed);
        // page_id is set last with Release to synchronize with readers
        self.page_id.store(page_id.0, Ordering::Release);
    }

    /// Mark the frame as containing a valid page.
    #[inline]
    pub fn set_valid(&self) {
        self.valid.store(true, Ordering::Release);
    }

    /// Get the reference bit for Clock algorithm.
    #[inline]
    pub fn get_reference_bit(&self) -> bool {
        self.reference_bit.load(Ordering::Relaxed)
    }

    /// Clear the reference bit (second chance given).
    #[inline]
    pub fn clear_reference_bit(&self) {
        self.reference_bit.store(false, Ordering::Relaxed);
    }
}

impl Default for BufferFrame {
    fn default() -> Self {
        Self::new()
    }
}

/// Disk manager for reading and writing pages.
pub struct DiskManager {
    /// Path to the database file
    db_path: PathBuf,
    /// File handle
    file: Mutex<File>,
    /// Next page ID to allocate
    next_page_id: AtomicU64,
    /// Optional page encryptor for data-at-rest encryption
    encryptor: Option<PageEncryptor>,
}

impl DiskManager {
    /// Create a new disk manager, creating the file if it doesn't exist.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&db_path)
            .map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::IoError(format!(
                    "Failed to open database file: {}",
                    e
                )))
            })?;

        // Determine next page ID from file size
        let metadata = file.metadata().map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::IoError(format!(
                "Failed to get file metadata: {}",
                e
            )))
        })?;
        let file_size = metadata.len();
        let next_page_id = if file_size == 0 {
            1 // Page 0 is reserved
        } else {
            (file_size / PAGE_SIZE as u64) + 1
        };

        Ok(Self {
            db_path,
            file: Mutex::new(file),
            next_page_id: AtomicU64::new(next_page_id),
            encryptor: None,
        })
    }

    /// Create a new disk manager with encryption enabled.
    pub fn with_encryption<P: AsRef<Path>>(path: P, key_path: &Path) -> Result<Self> {
        let mut dm = Self::new(path)?;
        dm.encryptor = Some(PageEncryptor::from_key_file(key_path)?);
        tracing::info!("Data-at-rest encryption enabled");
        Ok(dm)
    }

    /// Read a page from disk, decrypting if encryption is enabled.
    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        let mut file = self.file.lock();

        if let Some(ref encryptor) = self.encryptor {
            let encrypted_size = PAGE_SIZE + 16;
            let offset = (page_id.0 as u64) * encrypted_size as u64;
            file.seek(SeekFrom::Start(offset)).map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::IoError(format!(
                    "Failed to seek: {}",
                    e
                )))
            })?;
            let mut buffer = vec![0u8; encrypted_size];
            file.read_exact(&mut buffer).map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::IoError(format!(
                    "Failed to read encrypted page: {}",
                    e
                )))
            })?;
            let plaintext = encryptor.decrypt_page(page_id, &buffer)?;
            Page::from_bytes(Bytes::from(plaintext))
        } else {
            let offset = (page_id.0 as u64) * PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset)).map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::IoError(format!(
                    "Failed to seek: {}",
                    e
                )))
            })?;
            let mut buffer = vec![0u8; PAGE_SIZE];
            file.read_exact(&mut buffer).map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::IoError(format!(
                    "Failed to read page: {}",
                    e
                )))
            })?;
            Page::from_bytes(Bytes::from(buffer))
        }
    }

    /// Write a page to disk, encrypting if encryption is enabled.
    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<()> {
        let offset: u64;
        let write_data: Vec<u8>;

        let data = page.to_bytes();

        if let Some(ref encryptor) = self.encryptor {
            // Encrypted pages are PAGE_SIZE + 16 bytes (GCM tag)
            let encrypted_size = PAGE_SIZE + 16;
            offset = (page_id.0 as u64) * encrypted_size as u64;
            write_data = encryptor.encrypt_page(page_id, &data)?;
        } else {
            offset = (page_id.0 as u64) * PAGE_SIZE as u64;
            write_data = data.to_vec();
        }

        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(offset)).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::IoError(format!(
                "Failed to seek: {}",
                e
            )))
        })?;

        file.write_all(&write_data).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::IoError(format!(
                "Failed to write page: {}",
                e
            )))
        })?;

        Ok(())
    }

    /// Sync file to disk.
    pub fn sync(&self) -> Result<()> {
        let file = self.file.lock();
        file.sync_all().map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::IoError(format!(
                "Failed to sync: {}",
                e
            )))
        })
    }

    /// Allocate a new page ID.
    /// Uses Relaxed ordering since uniqueness only requires atomicity, not ordering.
    #[inline]
    pub fn allocate_page(&self) -> PageId {
        PageId(self.next_page_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Get the database file path.
    pub fn path(&self) -> &Path {
        &self.db_path
    }

    /// Get the total number of allocated pages on disk.
    pub fn num_pages(&self) -> u64 {
        self.next_page_id.load(Ordering::Relaxed)
    }
}

/// Buffer pool implementation with Clock eviction.
pub struct BufferPoolImpl {
    /// Buffer frames
    frames: Vec<Arc<BufferFrame>>,
    /// Page ID to frame index mapping
    page_table: DashMap<PageId, usize>,
    /// Free frame list
    free_list: Mutex<VecDeque<usize>>,
    /// Clock hand for eviction
    clock_hand: AtomicU64,
    /// Disk manager
    disk_manager: Arc<DiskManager>,
    /// Statistics
    stats: BufferPoolStatsInner,
}

/// Internal statistics with atomic counters
struct BufferPoolStatsInner {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    flushes: AtomicU64,
}

impl BufferPoolStatsInner {
    fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
        }
    }
}

impl BufferPoolImpl {
    /// Create a new buffer pool.
    ///
    /// # Arguments
    /// * `num_frames` - Number of buffer frames (pages that can be cached)
    /// * `disk_manager` - Disk manager for I/O
    pub fn new(num_frames: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut frames = Vec::with_capacity(num_frames);
        let mut free_list = VecDeque::with_capacity(num_frames);

        for i in 0..num_frames {
            frames.push(Arc::new(BufferFrame::new()));
            free_list.push_back(i);
        }

        Self {
            frames,
            page_table: DashMap::new(),
            free_list: Mutex::new(free_list),
            clock_hand: AtomicU64::new(0),
            disk_manager,
            stats: BufferPoolStatsInner::new(),
        }
    }

    /// Get a free frame, evicting if necessary.
    fn get_free_frame(&self) -> Result<usize> {
        // Try free list first
        {
            let mut free_list = self.free_list.lock();
            if let Some(frame_id) = free_list.pop_front() {
                return Ok(frame_id);
            }
        }

        // Need to evict - use Clock algorithm
        self.evict_frame()
    }

    /// Evict a frame using Clock algorithm.
    fn evict_frame(&self) -> Result<usize> {
        let num_frames = self.frames.len();
        let mut iterations = 0;
        let max_iterations = num_frames * 2;

        loop {
            if iterations >= max_iterations {
                return Err(Error::Storage(
                    thunder_common::error::StorageError::BufferPoolFull,
                ));
            }

            // Clock hand only needs to advance atomically, no ordering required
            let frame_id =
                (self.clock_hand.fetch_add(1, Ordering::Relaxed) as usize) % num_frames;
            let frame = &self.frames[frame_id];

            // Skip pinned frames
            if frame.is_pinned() {
                iterations += 1;
                continue;
            }

            // Skip invalid frames
            if !frame.is_valid() {
                iterations += 1;
                continue;
            }

            // Check reference bit
            if frame.get_reference_bit() {
                // Give second chance
                frame.clear_reference_bit();
                iterations += 1;
                continue;
            }

            // Found victim - flush if dirty
            if frame.is_dirty() {
                let page_guard = frame.page.read();
                if let Some(ref page) = *page_guard {
                    self.disk_manager.write_page(frame.page_id(), page)?;
                    self.stats.flushes.fetch_add(1, Ordering::Relaxed);
                }
                drop(page_guard);
                frame.clear_dirty();
            }

            // Remove from page table
            let old_page_id = frame.page_id();
            self.page_table.remove(&old_page_id);

            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            trace!("Evicted page {:?} from frame {}", old_page_id, frame_id);

            return Ok(frame_id);
        }
    }

    /// Fetch a page, loading from disk if not cached.
    pub fn fetch_page(&self, page_id: PageId) -> Result<PageReadGuard> {
        // Check if page is already in buffer pool
        if let Some(entry) = self.page_table.get(&page_id) {
            let frame_id = *entry;
            let frame = &self.frames[frame_id];
            frame.pin();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            trace!("Buffer pool hit for page {:?}", page_id);

            return Ok(PageReadGuard {
                frame: Arc::clone(frame),
                guard: frame.page.read(),
            });
        }

        // Cache miss - load from disk
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        debug!("Buffer pool miss for page {:?}, loading from disk", page_id);

        // Get a free frame
        let frame_id = self.get_free_frame()?;
        let frame = &self.frames[frame_id];

        // Reset frame for new page
        frame.reset(page_id);

        // Load page from disk
        let page = self.disk_manager.read_page(page_id)?;

        // Store in frame
        {
            let mut page_guard = frame.page.write();
            *page_guard = Some(page);
        }
        frame.set_valid();
        frame.pin();

        // Add to page table
        self.page_table.insert(page_id, frame_id);

        Ok(PageReadGuard {
            frame: Arc::clone(frame),
            guard: frame.page.read(),
        })
    }

    /// Fetch a page for writing.
    pub fn fetch_page_mut(&self, page_id: PageId) -> Result<PageWriteGuard> {
        // Check if page is already in buffer pool
        if let Some(entry) = self.page_table.get(&page_id) {
            let frame_id = *entry;
            let frame = &self.frames[frame_id];
            frame.pin();
            frame.mark_dirty();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);

            return Ok(PageWriteGuard {
                frame: Arc::clone(frame),
                guard: frame.page.write(),
            });
        }

        // Cache miss - load from disk
        self.stats.misses.fetch_add(1, Ordering::Relaxed);

        let frame_id = self.get_free_frame()?;
        let frame = &self.frames[frame_id];

        frame.reset(page_id);

        let page = self.disk_manager.read_page(page_id)?;

        {
            let mut page_guard = frame.page.write();
            *page_guard = Some(page);
        }
        frame.set_valid();
        frame.pin();
        frame.mark_dirty();

        self.page_table.insert(page_id, frame_id);

        Ok(PageWriteGuard {
            frame: Arc::clone(frame),
            guard: frame.page.write(),
        })
    }

    /// Allocate a new page.
    pub fn new_page(&self) -> Result<(PageId, PageWriteGuard)> {
        let page_id = self.disk_manager.allocate_page();
        let frame_id = self.get_free_frame()?;
        let frame = &self.frames[frame_id];

        frame.reset(page_id);

        // Create new empty page
        let page = Page::new(page_id);

        // Write to disk to ensure file is extended
        self.disk_manager.write_page(page_id, &page)?;

        {
            let mut page_guard = frame.page.write();
            *page_guard = Some(page);
        }
        frame.set_valid();
        frame.pin();
        frame.mark_dirty();

        self.page_table.insert(page_id, frame_id);

        debug!("Allocated new page {:?}", page_id);

        Ok((
            page_id,
            PageWriteGuard {
                frame: Arc::clone(frame),
                guard: frame.page.write(),
            },
        ))
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&self) -> Result<()> {
        for frame in &self.frames {
            if frame.is_valid() && frame.is_dirty() {
                let page_guard = frame.page.read();
                if let Some(ref page) = *page_guard {
                    self.disk_manager.write_page(frame.page_id(), page)?;
                    self.stats.flushes.fetch_add(1, Ordering::Relaxed);
                }
                drop(page_guard);
                frame.clear_dirty();
            }
        }

        self.disk_manager.sync()?;
        debug!("Flushed all dirty pages");
        Ok(())
    }

    /// Flush a specific page to disk.
    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        if let Some(entry) = self.page_table.get(&page_id) {
            let frame_id = *entry;
            let frame = &self.frames[frame_id];

            if frame.is_dirty() {
                let page_guard = frame.page.read();
                if let Some(ref page) = *page_guard {
                    self.disk_manager.write_page(page_id, page)?;
                    self.stats.flushes.fetch_add(1, Ordering::Relaxed);
                }
                drop(page_guard);
                frame.clear_dirty();
            }
        }

        Ok(())
    }

    /// Get buffer pool statistics.
    pub fn stats(&self) -> crate::BufferPoolStats {
        let mut used = 0;
        let mut dirty = 0;

        for frame in &self.frames {
            if frame.is_valid() {
                used += 1;
                if frame.is_dirty() {
                    dirty += 1;
                }
            }
        }

        crate::BufferPoolStats {
            total_pages: self.frames.len(),
            used_pages: used,
            dirty_pages: dirty,
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
        }
    }

    /// Get the disk manager.
    pub fn disk_manager(&self) -> &Arc<DiskManager> {
        &self.disk_manager
    }

    /// Get number of frames.
    pub fn num_frames(&self) -> usize {
        self.frames.len()
    }
}

/// Read guard for a page.
pub struct PageReadGuard<'a> {
    frame: Arc<BufferFrame>,
    guard: RwLockReadGuard<'a, Option<Page>>,
}

impl<'a> PageReadGuard<'a> {
    pub fn page(&self) -> &Page {
        self.guard.as_ref().expect("PageReadGuard should always have a valid page")
    }
}

impl<'a> Deref for PageReadGuard<'a> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().expect("PageReadGuard should always have a valid page")
    }
}

impl<'a> Drop for PageReadGuard<'a> {
    fn drop(&mut self) {
        self.frame.unpin();
    }
}

/// Write guard for a page.
pub struct PageWriteGuard<'a> {
    frame: Arc<BufferFrame>,
    guard: RwLockWriteGuard<'a, Option<Page>>,
}

impl<'a> PageWriteGuard<'a> {
    pub fn page(&self) -> &Page {
        self.guard.as_ref().expect("PageWriteGuard should always have a valid page")
    }

    pub fn page_mut(&mut self) -> &mut Page {
        self.frame.mark_dirty();
        self.guard.as_mut().expect("PageWriteGuard should always have a valid page")
    }
}

impl<'a> Deref for PageWriteGuard<'a> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().expect("PageWriteGuard should always have a valid page")
    }
}

impl<'a> DerefMut for PageWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.frame.mark_dirty();
        self.guard.as_mut().expect("PageWriteGuard should always have a valid page")
    }
}

impl<'a> Drop for PageWriteGuard<'a> {
    fn drop(&mut self) {
        self.frame.unpin();
    }
}

/// LRU-K eviction tracker for scan resistance.
///
/// Tracks the K most recent access times for each page to prevent
/// sequential scans from evicting frequently accessed pages.
pub struct LruKTracker {
    /// K value (typically 2)
    k: usize,
    /// Access history: page_id -> list of access timestamps
    history: DashMap<PageId, VecDeque<u64>>,
    /// Current timestamp counter
    timestamp: AtomicU64,
}

impl LruKTracker {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            history: DashMap::new(),
            timestamp: AtomicU64::new(0),
        }
    }

    /// Record an access to a page.
    pub fn record_access(&self, page_id: PageId) {
        // Timestamp only needs atomicity, not ordering
        let ts = self.timestamp.fetch_add(1, Ordering::Relaxed);

        self.history
            .entry(page_id)
            .and_modify(|history| {
                history.push_back(ts);
                while history.len() > self.k {
                    history.pop_front();
                }
            })
            .or_insert_with(|| {
                let mut dq = VecDeque::with_capacity(self.k);
                dq.push_back(ts);
                dq
            });
    }

    /// Get the backward K-distance for a page.
    /// Returns None if the page has been accessed fewer than K times.
    pub fn backward_k_distance(&self, page_id: PageId) -> Option<u64> {
        self.history.get(&page_id).and_then(|history| {
            if history.len() >= self.k {
                let current_ts = self.timestamp.load(Ordering::Relaxed);
                let kth_access = history.front().copied()?;
                Some(current_ts - kth_access)
            } else {
                None
            }
        })
    }

    /// Remove a page from tracking.
    pub fn remove(&self, page_id: PageId) {
        self.history.remove(&page_id);
    }

    /// Clear all history.
    pub fn clear(&self) {
        self.history.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_disk_manager_new() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = DiskManager::new(&db_path).unwrap();
        assert!(dm.path().exists());
    }

    #[test]
    fn test_disk_manager_page_allocation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = DiskManager::new(&db_path).unwrap();

        let page1 = dm.allocate_page();
        let page2 = dm.allocate_page();

        assert_ne!(page1, page2);
        assert!(page2.0 > page1.0);
    }

    #[test]
    fn test_disk_manager_read_write() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = DiskManager::new(&db_path).unwrap();

        let page_id = dm.allocate_page();
        let mut page = Page::new(page_id);

        // Insert some data
        page.insert_tuple(b"Hello, World!").unwrap();

        // Write page
        dm.write_page(page_id, &page).unwrap();

        // Read page back
        let read_page = dm.read_page(page_id).unwrap();

        // Verify data
        let tuple = read_page.get_tuple(0).unwrap();
        assert_eq!(tuple, b"Hello, World!");
    }

    #[test]
    fn test_buffer_pool_new_page() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = BufferPoolImpl::new(10, dm);

        let (page_id, mut guard) = pool.new_page().unwrap();
        assert!(page_id.0 > 0);

        guard.page_mut().insert_tuple(b"test data").unwrap();
        drop(guard);

        // Verify page is in pool
        let stats = pool.stats();
        assert!(stats.used_pages >= 1);
    }

    #[test]
    fn test_buffer_pool_fetch() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = BufferPoolImpl::new(10, dm);

        // Create a page
        let (page_id, mut guard) = pool.new_page().unwrap();
        guard.page_mut().insert_tuple(b"test data").unwrap();
        drop(guard);

        // Flush to disk
        pool.flush_all().unwrap();

        // Fetch the page
        let guard = pool.fetch_page(page_id).unwrap();
        let tuple = guard.get_tuple(0).unwrap();
        assert_eq!(tuple, b"test data");
    }

    #[test]
    fn test_buffer_pool_stats() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = BufferPoolImpl::new(10, dm);

        // Create some pages
        for _ in 0..5 {
            let (_, guard) = pool.new_page().unwrap();
            drop(guard);
        }

        let stats = pool.stats();
        assert_eq!(stats.total_pages, 10);
        assert!(stats.used_pages >= 5);
    }

    #[test]
    fn test_buffer_pool_eviction() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = BufferPoolImpl::new(5, dm);

        // Create more pages than can fit in pool
        let mut page_ids = Vec::new();
        for i in 0..10 {
            let (page_id, mut guard) = pool.new_page().unwrap();
            guard
                .page_mut()
                .insert_tuple(format!("data {}", i).as_bytes())
                .unwrap();
            page_ids.push(page_id);
            drop(guard); // Must drop to allow eviction
        }

        // All pages should still be accessible
        for (i, &page_id) in page_ids.iter().enumerate() {
            let guard = pool.fetch_page(page_id).unwrap();
            let tuple = guard.get_tuple(0).unwrap();
            assert_eq!(tuple, format!("data {}", i).as_bytes());
        }
    }

    #[test]
    fn test_buffer_pool_dirty_tracking() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = BufferPoolImpl::new(10, dm);

        let (page_id, mut guard) = pool.new_page().unwrap();
        guard.page_mut().insert_tuple(b"initial").unwrap();
        drop(guard);

        let stats = pool.stats();
        assert!(stats.dirty_pages >= 1);

        pool.flush_page(page_id).unwrap();

        let stats = pool.stats();
        // After flush, dirty count should decrease
        assert_eq!(stats.dirty_pages, 0);
    }

    #[test]
    fn test_lru_k_tracker() {
        let tracker = LruKTracker::new(2);

        let page1 = PageId(1);
        let page2 = PageId(2);

        // Access page1 twice
        tracker.record_access(page1);
        tracker.record_access(page1);

        // Access page2 once
        tracker.record_access(page2);

        // page1 should have a K-distance, page2 should not
        assert!(tracker.backward_k_distance(page1).is_some());
        assert!(tracker.backward_k_distance(page2).is_none());

        // Access page2 again
        tracker.record_access(page2);
        assert!(tracker.backward_k_distance(page2).is_some());
    }

    #[test]
    fn test_buffer_frame() {
        let frame = BufferFrame::new();

        assert!(!frame.is_pinned());
        assert!(!frame.is_dirty());
        assert!(!frame.is_valid());

        frame.pin();
        assert!(frame.is_pinned());

        frame.mark_dirty();
        assert!(frame.is_dirty());

        frame.set_valid();
        assert!(frame.is_valid());

        frame.unpin();
        assert!(!frame.is_pinned());

        frame.clear_dirty();
        assert!(!frame.is_dirty());
    }

    #[test]
    fn test_page_guards() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = BufferPoolImpl::new(10, dm);

        let (page_id, mut write_guard) = pool.new_page().unwrap();

        // Write through guard
        write_guard.insert_tuple(b"test").unwrap();
        drop(write_guard);

        // Read through guard
        let read_guard = pool.fetch_page(page_id).unwrap();
        let tuple = read_guard.get_tuple(0).unwrap();
        assert_eq!(tuple, b"test");
    }
}
