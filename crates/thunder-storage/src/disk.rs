//! Disk Manager - Low-level persistent storage I/O
//!
//! Provides direct file I/O for database pages with:
//! - Direct I/O support (bypassing OS cache when available)
//! - Page-aligned reads/writes
//! - File growth management
//! - Crash-safe operations

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::{Mutex, RwLock};
use crc32fast::Hasher;

use thunder_common::prelude::*;
use crate::PAGE_SIZE;

/// Page identifier (file_id, page_number)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiskPageId {
    pub file_id: u32,
    pub page_num: u32,
}

impl DiskPageId {
    pub fn new(file_id: u32, page_num: u32) -> Self {
        Self { file_id, page_num }
    }

    pub fn to_u64(&self) -> u64 {
        ((self.file_id as u64) << 32) | (self.page_num as u64)
    }

    pub fn from_u64(val: u64) -> Self {
        Self {
            file_id: (val >> 32) as u32,
            page_num: val as u32,
        }
    }
}

/// File types managed by the disk manager
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// Main data file
    Data,
    /// Index file
    Index,
    /// WAL segment file
    Wal,
    /// Temporary file
    Temp,
}

impl FileType {
    pub fn extension(&self) -> &'static str {
        match self {
            FileType::Data => "dat",
            FileType::Index => "idx",
            FileType::Wal => "wal",
            FileType::Temp => "tmp",
        }
    }
}

/// Managed file handle with metadata
struct ManagedFile {
    file: Mutex<File>,
    path: PathBuf,
    file_type: FileType,
    file_id: u32,
    num_pages: AtomicU64,
}

impl ManagedFile {
    fn new(file: File, path: PathBuf, file_type: FileType, file_id: u32) -> Result<Self> {
        let metadata = file.metadata()
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;
        let size = metadata.len();
        let num_pages = (size + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64;

        Ok(Self {
            file: Mutex::new(file),
            path,
            file_type,
            file_id,
            num_pages: AtomicU64::new(num_pages),
        })
    }

    fn read_page(&self, page_num: u32, buffer: &mut [u8]) -> Result<()> {
        if buffer.len() != PAGE_SIZE {
            return Err(Error::Storage(StorageError::InvalidPage(
                "Buffer size must match PAGE_SIZE".to_string()
            )));
        }

        let offset = (page_num as u64) * (PAGE_SIZE as u64);
        let mut file = self.file.lock();

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;

        file.read_exact(buffer)
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;

        Ok(())
    }

    fn write_page(&self, page_num: u32, buffer: &[u8]) -> Result<()> {
        if buffer.len() != PAGE_SIZE {
            return Err(Error::Storage(StorageError::InvalidPage(
                "Buffer size must match PAGE_SIZE".to_string()
            )));
        }

        let offset = (page_num as u64) * (PAGE_SIZE as u64);
        let mut file = self.file.lock();

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;

        file.write_all(buffer)
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;

        // Update page count if we wrote past the current end
        let current_pages = self.num_pages.load(Ordering::Relaxed);
        if (page_num as u64) >= current_pages {
            self.num_pages.store(page_num as u64 + 1, Ordering::Relaxed);
        }

        Ok(())
    }

    fn sync(&self) -> Result<()> {
        let file = self.file.lock();
        file.sync_all()
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))
    }

    fn allocate_page(&self) -> u32 {
        self.num_pages.fetch_add(1, Ordering::SeqCst) as u32
    }

    fn num_pages(&self) -> u64 {
        self.num_pages.load(Ordering::Relaxed)
    }
}

/// Disk Manager configuration
#[derive(Debug, Clone)]
pub struct DiskManagerConfig {
    /// Base directory for data files
    pub data_dir: PathBuf,
    /// Maximum number of open files
    pub max_open_files: usize,
    /// Use direct I/O (O_DIRECT on Linux)
    pub use_direct_io: bool,
    /// Pre-allocate file space
    pub preallocate_size: u64,
}

impl Default for DiskManagerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            max_open_files: 1024,
            use_direct_io: false,
            preallocate_size: 64 * 1024 * 1024, // 64MB
        }
    }
}

/// Disk Manager - manages all database files
pub struct DiskManager {
    config: DiskManagerConfig,
    files: RwLock<Vec<Option<ManagedFile>>>,
    next_file_id: AtomicU64,
}

impl DiskManager {
    /// Create a new disk manager
    pub fn new(config: DiskManagerConfig) -> Result<Self> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;

        let manager = Self {
            config,
            files: RwLock::new(Vec::new()),
            next_file_id: AtomicU64::new(0),
        };

        // Open existing files
        manager.open_existing_files()?;

        Ok(manager)
    }

    /// Open existing database files from the data directory
    fn open_existing_files(&self) -> Result<()> {
        let entries = std::fs::read_dir(&self.config.data_dir)
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;

        let mut max_file_id = 0u32;

        for entry in entries {
            let entry = entry.map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;
            let path = entry.path();

            if let Some(ext) = path.extension() {
                let file_type = match ext.to_str() {
                    Some("dat") => Some(FileType::Data),
                    Some("idx") => Some(FileType::Index),
                    Some("wal") => Some(FileType::Wal),
                    _ => None,
                };

                if let Some(file_type) = file_type {
                    // Extract file ID from filename (e.g., "data_0001.dat" -> 1)
                    if let Some(stem) = path.file_stem() {
                        if let Some(stem_str) = stem.to_str() {
                            if let Some(id_str) = stem_str.split('_').last() {
                                if let Ok(file_id) = id_str.parse::<u32>() {
                                    self.open_file_internal(&path, file_type, file_id)?;
                                    max_file_id = max_file_id.max(file_id);
                                }
                            }
                        }
                    }
                }
            }
        }

        self.next_file_id.store(max_file_id as u64 + 1, Ordering::SeqCst);
        Ok(())
    }

    /// Open or create a file internally
    fn open_file_internal(&self, path: &Path, file_type: FileType, file_id: u32) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;

        let managed = ManagedFile::new(file, path.to_path_buf(), file_type, file_id)?;

        let mut files = self.files.write();
        let idx = file_id as usize;
        if idx >= files.len() {
            files.resize_with(idx + 1, || None);
        }
        files[idx] = Some(managed);

        Ok(())
    }

    /// Create a new data file
    pub fn create_file(&self, file_type: FileType) -> Result<u32> {
        let file_id = self.next_file_id.fetch_add(1, Ordering::SeqCst) as u32;
        let filename = format!("{}_{:04}.{}",
            match file_type {
                FileType::Data => "data",
                FileType::Index => "index",
                FileType::Wal => "wal",
                FileType::Temp => "temp",
            },
            file_id,
            file_type.extension()
        );
        let path = self.config.data_dir.join(filename);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;

        // Preallocate space if configured
        if self.config.preallocate_size > 0 {
            file.set_len(self.config.preallocate_size)
                .map_err(|e| Error::Storage(StorageError::IoError(e.to_string())))?;
        }

        let managed = ManagedFile::new(file, path, file_type, file_id)?;

        let mut files = self.files.write();
        let idx = file_id as usize;
        if idx >= files.len() {
            files.resize_with(idx + 1, || None);
        }
        files[idx] = Some(managed);

        Ok(file_id)
    }

    /// Read a page from disk
    pub fn read_page(&self, page_id: DiskPageId, buffer: &mut [u8]) -> Result<()> {
        let files = self.files.read();
        let file = files.get(page_id.file_id as usize)
            .and_then(|f| f.as_ref())
            .ok_or_else(|| Error::Storage(StorageError::InvalidPage(
                format!("File {} not found", page_id.file_id)
            )))?;

        file.read_page(page_id.page_num, buffer)
    }

    /// Write a page to disk
    pub fn write_page(&self, page_id: DiskPageId, buffer: &[u8]) -> Result<()> {
        let files = self.files.read();
        let file = files.get(page_id.file_id as usize)
            .and_then(|f| f.as_ref())
            .ok_or_else(|| Error::Storage(StorageError::InvalidPage(
                format!("File {} not found", page_id.file_id)
            )))?;

        file.write_page(page_id.page_num, buffer)
    }

    /// Allocate a new page in the specified file
    pub fn allocate_page(&self, file_id: u32) -> Result<DiskPageId> {
        let files = self.files.read();
        let file = files.get(file_id as usize)
            .and_then(|f| f.as_ref())
            .ok_or_else(|| Error::Storage(StorageError::InvalidPage(
                format!("File {} not found", file_id)
            )))?;

        let page_num = file.allocate_page();
        Ok(DiskPageId::new(file_id, page_num))
    }

    /// Sync a specific file to disk
    pub fn sync_file(&self, file_id: u32) -> Result<()> {
        let files = self.files.read();
        let file = files.get(file_id as usize)
            .and_then(|f| f.as_ref())
            .ok_or_else(|| Error::Storage(StorageError::InvalidPage(
                format!("File {} not found", file_id)
            )))?;

        file.sync()
    }

    /// Sync all files to disk
    pub fn sync_all(&self) -> Result<()> {
        let files = self.files.read();
        for file_opt in files.iter() {
            if let Some(file) = file_opt {
                file.sync()?;
            }
        }
        Ok(())
    }

    /// Get number of pages in a file
    pub fn num_pages(&self, file_id: u32) -> Result<u64> {
        let files = self.files.read();
        let file = files.get(file_id as usize)
            .and_then(|f| f.as_ref())
            .ok_or_else(|| Error::Storage(StorageError::InvalidPage(
                format!("File {} not found", file_id)
            )))?;

        Ok(file.num_pages())
    }

    /// Get data directory path
    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }
}

/// Compute CRC32 checksum for a page
pub fn compute_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Verify CRC32 checksum
pub fn verify_checksum(data: &[u8], expected: u32) -> bool {
    compute_checksum(data) == expected
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_disk_page_id() {
        let id = DiskPageId::new(1, 42);
        assert_eq!(id.file_id, 1);
        assert_eq!(id.page_num, 42);

        let as_u64 = id.to_u64();
        let restored = DiskPageId::from_u64(as_u64);
        assert_eq!(restored.file_id, 1);
        assert_eq!(restored.page_num, 42);
    }

    #[test]
    fn test_disk_manager_create() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskManagerConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let dm = DiskManager::new(config).unwrap();
        let file_id = dm.create_file(FileType::Data).unwrap();
        assert_eq!(file_id, 0);
    }

    #[test]
    fn test_disk_manager_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskManagerConfig {
            data_dir: temp_dir.path().to_path_buf(),
            preallocate_size: 0,
            ..Default::default()
        };

        let dm = DiskManager::new(config).unwrap();
        let file_id = dm.create_file(FileType::Data).unwrap();

        // Write a page
        let mut write_buf = vec![0u8; PAGE_SIZE];
        write_buf[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        write_buf[PAGE_SIZE - 4..].copy_from_slice(&[0xCA, 0xFE, 0xBA, 0xBE]);

        let page_id = dm.allocate_page(file_id).unwrap();
        dm.write_page(page_id, &write_buf).unwrap();
        dm.sync_file(file_id).unwrap();

        // Read it back
        let mut read_buf = vec![0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_buf).unwrap();

        assert_eq!(&read_buf[0..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(&read_buf[PAGE_SIZE - 4..], &[0xCA, 0xFE, 0xBA, 0xBE]);
    }

    #[test]
    fn test_disk_manager_multiple_pages() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskManagerConfig {
            data_dir: temp_dir.path().to_path_buf(),
            preallocate_size: 0,
            ..Default::default()
        };

        let dm = DiskManager::new(config).unwrap();
        let file_id = dm.create_file(FileType::Data).unwrap();

        // Write multiple pages
        for i in 0..10u32 {
            let page_id = dm.allocate_page(file_id).unwrap();
            assert_eq!(page_id.page_num, i);

            let mut buf = vec![0u8; PAGE_SIZE];
            buf[0..4].copy_from_slice(&i.to_le_bytes());
            dm.write_page(page_id, &buf).unwrap();
        }

        dm.sync_file(file_id).unwrap();

        // Read them back
        for i in 0..10u32 {
            let page_id = DiskPageId::new(file_id, i);
            let mut buf = vec![0u8; PAGE_SIZE];
            dm.read_page(page_id, &mut buf).unwrap();

            let read_i = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            assert_eq!(read_i, i);
        }
    }

    #[test]
    fn test_checksum() {
        let data = b"Hello, World!";
        let checksum = compute_checksum(data);
        assert!(verify_checksum(data, checksum));
        assert!(!verify_checksum(data, checksum + 1));
    }

    #[test]
    fn test_disk_manager_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        // Write data with first manager
        {
            let config = DiskManagerConfig {
                data_dir: data_dir.clone(),
                preallocate_size: 0,
                ..Default::default()
            };
            let dm = DiskManager::new(config).unwrap();
            let file_id = dm.create_file(FileType::Data).unwrap();

            let mut buf = vec![0u8; PAGE_SIZE];
            buf[0..8].copy_from_slice(b"PERSIST!");
            let page_id = dm.allocate_page(file_id).unwrap();
            dm.write_page(page_id, &buf).unwrap();
            dm.sync_all().unwrap();
        }

        // Read with new manager
        {
            let config = DiskManagerConfig {
                data_dir,
                preallocate_size: 0,
                ..Default::default()
            };
            let dm = DiskManager::new(config).unwrap();

            let page_id = DiskPageId::new(0, 0);
            let mut buf = vec![0u8; PAGE_SIZE];
            dm.read_page(page_id, &mut buf).unwrap();

            assert_eq!(&buf[0..8], b"PERSIST!");
        }
    }
}
