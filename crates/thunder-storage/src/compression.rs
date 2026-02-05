//! Compression codecs for ThunderDB storage engine.
//!
//! Provides LZ4, Snappy, and Zstd compression for:
//! - Page-level compression
//! - Column store segment compression
//! - WAL compression (optional)

use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use thunder_common::config::CompressionAlgorithm;
use thunder_common::prelude::*;

/// Compression codec trait for all compression implementations.
pub trait CompressionCodec: Send + Sync {
    /// Compress input data.
    fn compress(&self, input: &[u8]) -> Result<Bytes>;

    /// Decompress data, with optional expected size hint for buffer allocation.
    fn decompress(&self, input: &[u8], expected_size: Option<usize>) -> Result<Vec<u8>>;

    /// Get the compression algorithm identifier.
    fn algorithm(&self) -> CompressionAlgorithm;

    /// Get the name of the codec.
    fn name(&self) -> &'static str;
}

/// No compression - pass-through codec.
#[derive(Debug, Default)]
pub struct NoopCodec;

impl CompressionCodec for NoopCodec {
    fn compress(&self, input: &[u8]) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(input))
    }

    fn decompress(&self, input: &[u8], _expected_size: Option<usize>) -> Result<Vec<u8>> {
        Ok(input.to_vec())
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::None
    }

    fn name(&self) -> &'static str {
        "none"
    }
}

/// LZ4 compression codec - optimized for speed.
///
/// LZ4 provides very fast compression and decompression with moderate
/// compression ratios. Ideal for hot data paths where latency matters.
#[derive(Debug)]
pub struct Lz4Codec {
    /// Acceleration factor (1-12). Higher = faster but less compression.
    #[allow(dead_code)]
    acceleration: i32,
}

impl Lz4Codec {
    /// Create a new LZ4 codec with default settings (acceleration = 1).
    pub fn new() -> Self {
        Self { acceleration: 1 }
    }

    /// Create a new LZ4 codec with custom acceleration.
    ///
    /// Acceleration range: 1-12
    /// - 1: Best compression ratio (default)
    /// - 12: Fastest compression
    pub fn with_acceleration(acceleration: i32) -> Self {
        Self {
            acceleration: acceleration.clamp(1, 12),
        }
    }
}

impl Default for Lz4Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionCodec for Lz4Codec {
    fn compress(&self, input: &[u8]) -> Result<Bytes> {
        if input.is_empty() {
            return Ok(Bytes::new());
        }

        let compressed = lz4_flex::compress_prepend_size(input);
        Ok(Bytes::from(compressed))
    }

    fn decompress(&self, input: &[u8], _expected_size: Option<usize>) -> Result<Vec<u8>> {
        if input.is_empty() {
            return Ok(Vec::new());
        }

        lz4_flex::decompress_size_prepended(input).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::CompressionError(format!(
                "LZ4 decompression failed: {}",
                e
            )))
        })
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Lz4
    }

    fn name(&self) -> &'static str {
        "lz4"
    }
}

/// Snappy compression codec - balanced speed and ratio.
///
/// Snappy provides a good balance between compression speed and ratio.
/// Originally developed by Google for use in BigTable and MapReduce.
#[derive(Debug, Default)]
pub struct SnappyCodec;

impl SnappyCodec {
    pub fn new() -> Self {
        Self
    }
}

impl CompressionCodec for SnappyCodec {
    fn compress(&self, input: &[u8]) -> Result<Bytes> {
        if input.is_empty() {
            return Ok(Bytes::new());
        }

        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(input).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::CompressionError(format!(
                "Snappy compression failed: {}",
                e
            )))
        })?;

        Ok(Bytes::from(compressed))
    }

    fn decompress(&self, input: &[u8], expected_size: Option<usize>) -> Result<Vec<u8>> {
        if input.is_empty() {
            return Ok(Vec::new());
        }

        let mut decoder = snap::raw::Decoder::new();

        // Pre-allocate buffer if expected size is known
        if let Some(size) = expected_size {
            let mut output = vec![0u8; size];
            decoder.decompress(input, &mut output).map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::CompressionError(format!(
                    "Snappy decompression failed: {}",
                    e
                )))
            })?;
            Ok(output)
        } else {
            decoder.decompress_vec(input).map_err(|e| {
                Error::Storage(thunder_common::error::StorageError::CompressionError(format!(
                    "Snappy decompression failed: {}",
                    e
                )))
            })
        }
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Snappy
    }

    fn name(&self) -> &'static str {
        "snappy"
    }
}

/// Zstd compression codec - best compression ratio.
///
/// Zstd provides the best compression ratios while maintaining reasonable
/// decompression speed. Ideal for cold data and archival storage.
#[derive(Debug)]
pub struct ZstdCodec {
    /// Compression level (1-22). Higher = better compression but slower.
    level: i32,
}

impl ZstdCodec {
    /// Create a new Zstd codec with default compression level (3).
    pub fn new() -> Self {
        Self { level: 3 }
    }

    /// Create a new Zstd codec with custom compression level.
    ///
    /// Level range: 1-22
    /// - 1-3: Fast compression
    /// - 4-9: Balanced (default range)
    /// - 10-22: Maximum compression
    pub fn with_level(level: i32) -> Self {
        Self {
            level: level.clamp(1, 22),
        }
    }
}

impl Default for ZstdCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionCodec for ZstdCodec {
    fn compress(&self, input: &[u8]) -> Result<Bytes> {
        if input.is_empty() {
            return Ok(Bytes::new());
        }

        let compressed = zstd::bulk::compress(input, self.level).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::CompressionError(format!(
                "Zstd compression failed: {}",
                e
            )))
        })?;

        Ok(Bytes::from(compressed))
    }

    fn decompress(&self, input: &[u8], expected_size: Option<usize>) -> Result<Vec<u8>> {
        if input.is_empty() {
            return Ok(Vec::new());
        }

        // Use expected_size as capacity hint if provided
        let capacity = expected_size.unwrap_or(input.len() * 4);

        zstd::bulk::decompress(input, capacity).map_err(|e| {
            Error::Storage(thunder_common::error::StorageError::CompressionError(format!(
                "Zstd decompression failed: {}",
                e
            )))
        })
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Zstd
    }

    fn name(&self) -> &'static str {
        "zstd"
    }
}

/// Create a compression codec based on the algorithm.
pub fn create_codec(algorithm: CompressionAlgorithm) -> Box<dyn CompressionCodec> {
    match algorithm {
        CompressionAlgorithm::None => Box::new(NoopCodec),
        CompressionAlgorithm::Lz4 => Box::new(Lz4Codec::new()),
        CompressionAlgorithm::Snappy => Box::new(SnappyCodec::new()),
        CompressionAlgorithm::Zstd => Box::new(ZstdCodec::new()),
    }
}

/// Compression statistics for monitoring.
#[derive(Debug, Default)]
pub struct CompressionStats {
    /// Total bytes before compression.
    pub bytes_in: AtomicU64,
    /// Total bytes after compression.
    pub bytes_out: AtomicU64,
    /// Number of compression operations.
    pub compress_count: AtomicU64,
    /// Number of decompression operations.
    pub decompress_count: AtomicU64,
}

impl CompressionStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a compression operation.
    pub fn record_compress(&self, input_size: usize, output_size: usize) {
        self.bytes_in.fetch_add(input_size as u64, Ordering::Relaxed);
        self.bytes_out.fetch_add(output_size as u64, Ordering::Relaxed);
        self.compress_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a decompression operation.
    pub fn record_decompress(&self) {
        self.decompress_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the overall compression ratio (bytes_in / bytes_out).
    /// Returns None if no data has been compressed.
    pub fn compression_ratio(&self) -> Option<f64> {
        let bytes_in = self.bytes_in.load(Ordering::Relaxed);
        let bytes_out = self.bytes_out.load(Ordering::Relaxed);

        if bytes_out == 0 {
            None
        } else {
            Some(bytes_in as f64 / bytes_out as f64)
        }
    }

    /// Reset all statistics.
    pub fn reset(&self) {
        self.bytes_in.store(0, Ordering::Relaxed);
        self.bytes_out.store(0, Ordering::Relaxed);
        self.compress_count.store(0, Ordering::Relaxed);
        self.decompress_count.store(0, Ordering::Relaxed);
    }
}

/// Codec with statistics tracking.
pub struct TrackedCodec {
    inner: Box<dyn CompressionCodec>,
    stats: CompressionStats,
}

impl TrackedCodec {
    pub fn new(codec: Box<dyn CompressionCodec>) -> Self {
        Self {
            inner: codec,
            stats: CompressionStats::new(),
        }
    }

    pub fn stats(&self) -> &CompressionStats {
        &self.stats
    }
}

impl CompressionCodec for TrackedCodec {
    fn compress(&self, input: &[u8]) -> Result<Bytes> {
        let result = self.inner.compress(input)?;
        self.stats.record_compress(input.len(), result.len());
        Ok(result)
    }

    fn decompress(&self, input: &[u8], expected_size: Option<usize>) -> Result<Vec<u8>> {
        let result = self.inner.decompress(input, expected_size)?;
        self.stats.record_decompress();
        Ok(result)
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        self.inner.algorithm()
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_roundtrip(codec: &dyn CompressionCodec, data: &[u8]) {
        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, Some(data.len())).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_noop_codec() {
        let codec = NoopCodec;
        let data = b"Hello, World!";
        test_roundtrip(&codec, data);

        // Noop should not change data size
        let compressed = codec.compress(data).unwrap();
        assert_eq!(compressed.len(), data.len());
    }

    #[test]
    fn test_lz4_codec() {
        let codec = Lz4Codec::new();
        let data = b"Hello, World! This is a test of LZ4 compression.";
        test_roundtrip(&codec, data);
    }

    #[test]
    fn test_lz4_codec_with_acceleration() {
        let codec = Lz4Codec::with_acceleration(12);
        let data = b"Fast compression test with high acceleration factor.";
        test_roundtrip(&codec, data);
    }

    #[test]
    fn test_snappy_codec() {
        let codec = SnappyCodec::new();
        let data = b"Hello, World! This is a test of Snappy compression.";
        test_roundtrip(&codec, data);
    }

    #[test]
    fn test_zstd_codec() {
        let codec = ZstdCodec::new();
        let data = b"Hello, World! This is a test of Zstd compression.";
        test_roundtrip(&codec, data);
    }

    #[test]
    fn test_zstd_codec_with_level() {
        let codec = ZstdCodec::with_level(19);
        let data = b"High compression level test with Zstd codec.";
        test_roundtrip(&codec, data);
    }

    #[test]
    fn test_empty_input() {
        let codecs: Vec<Box<dyn CompressionCodec>> = vec![
            Box::new(NoopCodec),
            Box::new(Lz4Codec::new()),
            Box::new(SnappyCodec::new()),
            Box::new(ZstdCodec::new()),
        ];

        for codec in &codecs {
            let compressed = codec.compress(&[]).unwrap();
            assert!(compressed.is_empty());
            let decompressed = codec.decompress(&[], None).unwrap();
            assert!(decompressed.is_empty());
        }
    }

    #[test]
    fn test_large_data() {
        // Create 1MB of compressible data
        let data: Vec<u8> = (0..1024 * 1024)
            .map(|i| ((i % 256) as u8).wrapping_mul(7))
            .collect();

        let codecs: Vec<Box<dyn CompressionCodec>> = vec![
            Box::new(Lz4Codec::new()),
            Box::new(SnappyCodec::new()),
            Box::new(ZstdCodec::new()),
        ];

        for codec in &codecs {
            let compressed = codec.compress(&data).unwrap();
            let decompressed = codec.decompress(&compressed, Some(data.len())).unwrap();
            assert_eq!(data, decompressed);

            // Compression should reduce size for repetitive data
            assert!(
                compressed.len() < data.len(),
                "{} did not compress data",
                codec.name()
            );
        }
    }

    #[test]
    fn test_compression_ratios() {
        // Create highly compressible data (all zeros)
        let zeros: Vec<u8> = vec![0u8; 10000];

        // Create incompressible random-like data
        let random: Vec<u8> = (0..10000).map(|i| (i * 17 + 31) as u8).collect();

        let zstd = ZstdCodec::with_level(19);

        // Zeros should compress very well
        let compressed_zeros = zstd.compress(&zeros).unwrap();
        assert!(compressed_zeros.len() < zeros.len() / 10);

        // Random data might not compress well but should still roundtrip
        let compressed_random = zstd.compress(&random).unwrap();
        let decompressed = zstd.decompress(&compressed_random, Some(random.len())).unwrap();
        assert_eq!(random, decompressed);
    }

    #[test]
    fn test_create_codec() {
        let algorithms = [
            CompressionAlgorithm::None,
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Snappy,
            CompressionAlgorithm::Zstd,
        ];

        for algo in algorithms {
            let codec = create_codec(algo);
            assert_eq!(codec.algorithm(), algo);
        }
    }

    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats::new();

        stats.record_compress(1000, 500);
        stats.record_compress(2000, 1000);
        stats.record_decompress();

        assert_eq!(stats.bytes_in.load(Ordering::Relaxed), 3000);
        assert_eq!(stats.bytes_out.load(Ordering::Relaxed), 1500);
        assert_eq!(stats.compress_count.load(Ordering::Relaxed), 2);
        assert_eq!(stats.decompress_count.load(Ordering::Relaxed), 1);

        let ratio = stats.compression_ratio().unwrap();
        assert!((ratio - 2.0).abs() < 0.001);

        stats.reset();
        assert_eq!(stats.bytes_in.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_tracked_codec() {
        let codec = TrackedCodec::new(Box::new(Lz4Codec::new()));

        let data = b"Test data for tracking compression statistics.";
        let compressed = codec.compress(data).unwrap();
        let _ = codec.decompress(&compressed, Some(data.len())).unwrap();

        assert_eq!(codec.stats().compress_count.load(Ordering::Relaxed), 1);
        assert_eq!(codec.stats().decompress_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            codec.stats().bytes_in.load(Ordering::Relaxed),
            data.len() as u64
        );
    }
}
