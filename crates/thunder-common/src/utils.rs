//! Utility functions and helpers

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Get current timestamp in microseconds since epoch
pub fn timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_micros() as u64
}

/// Get current timestamp in milliseconds since epoch
pub fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

/// Get current timestamp in nanoseconds since epoch
pub fn timestamp_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos() as u64
}

/// Thread-safe ID generator
pub struct IdGenerator {
    counter: AtomicU64,
    node_id: u8,
}

impl IdGenerator {
    pub fn new(node_id: u8) -> Self {
        Self {
            counter: AtomicU64::new(0),
            node_id,
        }
    }

    /// Generate a unique ID
    pub fn next_id(&self) -> u64 {
        let seq = self.counter.fetch_add(1, Ordering::Relaxed) & 0xFF;
        let ts = timestamp_us();
        (ts << 16) | ((self.node_id as u64) << 8) | seq
    }
}

/// Compute CRC32 checksum
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Verify CRC32 checksum
pub fn verify_crc32(data: &[u8], expected: u32) -> bool {
    crc32(data) == expected
}

/// Align a value up to the given alignment
#[inline]
pub const fn align_up(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

/// Align a value down to the given alignment
#[inline]
pub const fn align_down(value: usize, alignment: usize) -> usize {
    value & !(alignment - 1)
}

/// Check if a value is aligned to the given alignment
#[inline]
pub const fn is_aligned(value: usize, alignment: usize) -> bool {
    value & (alignment - 1) == 0
}

/// Human-readable byte size formatting
pub fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;
    const TB: usize = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Parse human-readable byte size
pub fn parse_bytes(s: &str) -> Option<usize> {
    let s = s.trim().to_uppercase();
    let (num_str, unit) = if s.ends_with("TB") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024 * 1024usize)
    } else if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024)
    } else if s.ends_with("B") {
        (&s[..s.len() - 1], 1)
    } else {
        (s.as_str(), 1)
    };

    num_str.trim().parse::<f64>().ok().map(|n| (n * unit as f64) as usize)
}

/// Escape a string for SQL
pub fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Escape a string for LIKE pattern
pub fn escape_like_pattern(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align() {
        assert_eq!(align_up(0, 8), 0);
        assert_eq!(align_up(1, 8), 8);
        assert_eq!(align_up(8, 8), 8);
        assert_eq!(align_up(9, 8), 16);

        assert_eq!(align_down(0, 8), 0);
        assert_eq!(align_down(7, 8), 0);
        assert_eq!(align_down(8, 8), 8);
        assert_eq!(align_down(15, 8), 8);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_parse_bytes() {
        assert_eq!(parse_bytes("1024"), Some(1024));
        assert_eq!(parse_bytes("1KB"), Some(1024));
        assert_eq!(parse_bytes("1.5 MB"), Some(1572864));
        assert_eq!(parse_bytes("1GB"), Some(1073741824));
    }

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let checksum = crc32(data);
        assert!(verify_crc32(data, checksum));
        assert!(!verify_crc32(data, checksum + 1));
    }

    #[test]
    fn test_id_generator() {
        let gen = IdGenerator::new(1);
        let id1 = gen.next_id();
        let id2 = gen.next_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_escape_sql() {
        assert_eq!(escape_sql_string("hello"), "hello");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("O'Brien"), "O''Brien");
    }
}
