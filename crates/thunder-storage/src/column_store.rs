//! Column store implementation for ThunderDB.
//!
//! Provides Arrow-based columnar storage for OLAP workloads:
//! - Parquet-like segment format
//! - Column statistics for predicate pushdown
//! - Background compaction
//! - Write buffers for batched ingestion

use crate::compression::{create_codec, CompressionCodec};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thunder_common::config::CompressionAlgorithm;
use thunder_common::prelude::*;

/// Column statistics for predicate pushdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Column name
    pub column_name: String,
    /// Number of values
    pub row_count: u64,
    /// Number of NULL values
    pub null_count: u64,
    /// Minimum value (as bytes)
    pub min_value: Option<Vec<u8>>,
    /// Maximum value (as bytes)
    pub max_value: Option<Vec<u8>>,
    /// Number of distinct values (approximate)
    pub distinct_count: Option<u64>,
}

impl ColumnStatistics {
    pub fn new(column_name: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
            row_count: 0,
            null_count: 0,
            min_value: None,
            max_value: None,
            distinct_count: None,
        }
    }
}

/// Segment metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    /// Segment ID
    pub segment_id: u64,
    /// Table ID
    pub table_id: TableId,
    /// Row count
    pub row_count: u64,
    /// Column chunk metadata
    pub column_chunks: Vec<ColumnChunkMetadata>,
    /// Segment level (for compaction)
    pub level: u32,
    /// Creation timestamp
    pub created_at: i64,
    /// Size in bytes
    pub size_bytes: u64,
}

/// Column chunk metadata within a segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnChunkMetadata {
    /// Column name
    pub column_name: String,
    /// Column index in schema
    pub column_index: usize,
    /// Offset in segment file
    pub offset: u64,
    /// Compressed size
    pub compressed_size: u64,
    /// Uncompressed size
    pub uncompressed_size: u64,
    /// Compression algorithm
    pub compression: CompressionAlgorithm,
    /// Column statistics
    pub statistics: ColumnStatistics,
}

/// Write buffer for batched ingestion.
pub struct WriteBuffer {
    /// Schema
    schema: SchemaRef,
    /// Buffered batches
    batches: RwLock<Vec<RecordBatch>>,
    /// Total row count
    row_count: AtomicU64,
    /// Maximum rows before flush
    max_rows: usize,
}

impl WriteBuffer {
    pub fn new(schema: SchemaRef, max_rows: usize) -> Self {
        Self {
            schema,
            batches: RwLock::new(Vec::new()),
            row_count: AtomicU64::new(0),
            max_rows,
        }
    }

    /// Add a batch to the buffer.
    pub fn add_batch(&self, batch: RecordBatch) -> Result<bool> {
        if batch.schema() != self.schema {
            return Err(Error::InvalidArgument("Schema mismatch".into()));
        }

        let rows = batch.num_rows();
        let mut batches = self.batches.write();
        batches.push(batch);
        let total = self.row_count.fetch_add(rows as u64, Ordering::SeqCst) + rows as u64;

        Ok(total >= self.max_rows as u64)
    }

    /// Drain the buffer and return all batches.
    pub fn drain(&self) -> Vec<RecordBatch> {
        let mut batches = self.batches.write();
        self.row_count.store(0, Ordering::SeqCst);
        std::mem::take(&mut *batches)
    }

    /// Get current row count.
    pub fn row_count(&self) -> u64 {
        self.row_count.load(Ordering::SeqCst)
    }

    /// Check if buffer should be flushed.
    pub fn should_flush(&self) -> bool {
        self.row_count() >= self.max_rows as u64
    }
}

/// In-memory segment representation.
pub struct Segment {
    /// Metadata
    pub metadata: SegmentMetadata,
    /// Column data (column_name -> compressed bytes)
    columns: HashMap<String, Bytes>,
    /// Decompressed column cache
    #[allow(dead_code)]
    column_cache: DashMap<String, ArrayRef>,
}

impl Segment {
    pub fn new(metadata: SegmentMetadata) -> Self {
        Self {
            metadata,
            columns: HashMap::new(),
            column_cache: DashMap::new(),
        }
    }

    /// Add column data.
    pub fn add_column(&mut self, name: String, data: Bytes) {
        self.columns.insert(name, data);
    }

    /// Get column data (compressed).
    pub fn get_column_compressed(&self, name: &str) -> Option<&Bytes> {
        self.columns.get(name)
    }
}

/// Column store implementation.
pub struct ColumnStoreImpl {
    /// Table ID to segments mapping
    segments: DashMap<TableId, Vec<Arc<Segment>>>,
    /// Table ID to write buffer mapping
    write_buffers: DashMap<TableId, Arc<WriteBuffer>>,
    /// Table ID to schema mapping
    schemas: DashMap<TableId, SchemaRef>,
    /// Next segment ID
    next_segment_id: AtomicU64,
    /// Compression codec
    codec: Box<dyn CompressionCodec>,
    /// Maximum rows per write buffer
    max_buffer_rows: usize,
}

impl ColumnStoreImpl {
    /// Create a new column store.
    pub fn new(compression: CompressionAlgorithm, max_buffer_rows: usize) -> Self {
        Self {
            segments: DashMap::new(),
            write_buffers: DashMap::new(),
            schemas: DashMap::new(),
            next_segment_id: AtomicU64::new(1),
            codec: create_codec(compression),
            max_buffer_rows,
        }
    }

    /// Register a table schema.
    pub fn register_table(&self, table_id: TableId, schema: SchemaRef) {
        self.schemas.insert(table_id, schema.clone());
        self.write_buffers.insert(
            table_id,
            Arc::new(WriteBuffer::new(schema, self.max_buffer_rows)),
        );
        self.segments.entry(table_id).or_insert_with(Vec::new);
    }

    /// Append a batch of rows.
    pub async fn append_batch(&self, table_id: TableId, batch: RecordBatch) -> Result<()> {
        let buffer = self
            .write_buffers
            .get(&table_id)
            .ok_or_else(|| Error::NotFound("Table".into(), table_id.0.to_string()))?;

        let should_flush = buffer.add_batch(batch)?;

        if should_flush {
            self.flush_buffer(table_id).await?;
        }

        Ok(())
    }

    /// Flush write buffer to segment.
    pub async fn flush_buffer(&self, table_id: TableId) -> Result<Option<SegmentMetadata>> {
        let buffer = match self.write_buffers.get(&table_id) {
            Some(b) => b.clone(),
            None => return Ok(None),
        };

        let batches = buffer.drain();
        if batches.is_empty() {
            return Ok(None);
        }

        let schema = match self.schemas.get(&table_id) {
            Some(s) => s.clone(),
            None => return Err(Error::NotFound("Schema".into(), table_id.0.to_string())),
        };

        // Create segment from batches
        let segment = self.create_segment(table_id, schema, batches)?;
        let metadata = segment.metadata.clone();

        // Add segment to table
        self.segments
            .entry(table_id)
            .or_insert_with(Vec::new)
            .push(Arc::new(segment));

        Ok(Some(metadata))
    }

    /// Create a segment from batches.
    fn create_segment(
        &self,
        table_id: TableId,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Segment> {
        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Merge batches
        let merged = arrow::compute::concat_batches(&schema, batches.iter())
            .map_err(|e| Error::Internal(format!("Failed to merge batches: {}", e)))?;

        let row_count = merged.num_rows() as u64;
        let mut column_chunks = Vec::new();
        let mut columns = HashMap::new();
        let mut total_size = 0u64;

        // Process each column
        for (i, field) in schema.fields().iter().enumerate() {
            let column = merged.column(i);
            let column_name = field.name().clone();

            // Serialize column to bytes (simplified - in reality we'd use IPC format)
            let column_bytes = serialize_column(column.clone())?;
            let uncompressed_size = column_bytes.len() as u64;

            // Compress column
            let compressed = self.codec.compress(&column_bytes)?;
            let compressed_size = compressed.len() as u64;

            // Compute statistics
            let statistics = compute_column_statistics(&column_name, column.clone());

            column_chunks.push(ColumnChunkMetadata {
                column_name: column_name.clone(),
                column_index: i,
                offset: total_size,
                compressed_size,
                uncompressed_size,
                compression: self.codec.algorithm(),
                statistics,
            });

            columns.insert(column_name, compressed);
            total_size += compressed_size;
        }

        let metadata = SegmentMetadata {
            segment_id,
            table_id,
            row_count,
            column_chunks,
            level: 0,
            created_at: now,
            size_bytes: total_size,
        };

        let mut segment = Segment::new(metadata);
        for (name, data) in columns {
            segment.add_column(name, data);
        }

        Ok(segment)
    }

    /// Scan columns with optional predicate.
    pub async fn scan(
        &self,
        table_id: TableId,
        columns: &[ColumnId],
        predicate: Option<&str>,
    ) -> Result<RecordBatch> {
        let schema = self
            .schemas
            .get(&table_id)
            .ok_or_else(|| Error::NotFound("Schema".into(), table_id.0.to_string()))?;

        let segments = self
            .segments
            .get(&table_id)
            .map(|s| s.clone())
            .unwrap_or_default();

        // Select columns to read
        let column_names: Vec<_> = if columns.is_empty() {
            schema.fields().iter().map(|f| f.name().clone()).collect()
        } else {
            columns
                .iter()
                .filter_map(|col_id| {
                    schema.fields().get(col_id.0 as usize).map(|f| f.name().clone())
                })
                .collect()
        };

        if segments.is_empty() {
            // Return empty batch with schema
            let fields: Vec<_> = column_names
                .iter()
                .filter_map(|name| schema.field_with_name(name).ok().cloned())
                .collect();
            let projection_schema = Arc::new(Schema::new(fields));
            return Ok(RecordBatch::new_empty(projection_schema));
        }

        // Collect batches from all segments
        let mut result_batches = Vec::new();

        for segment in segments.iter() {
            let batch = self.read_segment_columns(&segment, &column_names, &schema)?;
            result_batches.push(batch);
        }

        // Merge batches
        let fields: Vec<_> = column_names
            .iter()
            .filter_map(|name| schema.field_with_name(name).ok().cloned())
            .collect();
        let projection_schema = Arc::new(Schema::new(fields));

        if result_batches.is_empty() {
            return Ok(RecordBatch::new_empty(projection_schema));
        }

        let merged = arrow::compute::concat_batches(&projection_schema, result_batches.iter())
            .map_err(|e| Error::Internal(format!("Failed to merge scan results: {}", e)))?;

        // Apply predicate if provided (simplified - real impl would use DataFusion)
        // For now, we just return all rows
        let _ = predicate; // Predicate filtering would go here

        Ok(merged)
    }

    /// Read specific columns from a segment.
    fn read_segment_columns(
        &self,
        segment: &Segment,
        column_names: &[String],
        _schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let mut arrays = Vec::new();
        let mut fields = Vec::new();

        for name in column_names {
            if let Some(chunk_meta) = segment
                .metadata
                .column_chunks
                .iter()
                .find(|c| &c.column_name == name)
            {
                if let Some(compressed) = segment.get_column_compressed(name) {
                    // Decompress column
                    let decompressed = self
                        .codec
                        .decompress(compressed, Some(chunk_meta.uncompressed_size as usize))?;

                    // Deserialize column (simplified)
                    let array = deserialize_column(&decompressed)?;

                    // Create field from array type
                    fields.push(Field::new(name.clone(), array.data_type().clone(), true));
                    arrays.push(array);
                }
            }
        }

        if arrays.is_empty() {
            return Err(Error::Internal("No columns to read".into()));
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| Error::Internal(format!("Failed to create batch: {}", e)))
    }

    /// Compact segments by merging smaller ones.
    pub async fn compact(&self) -> Result<()> {
        for entry in self.segments.iter() {
            let table_id = *entry.key();
            let segments = entry.value().clone();

            // Simple compaction: merge segments at the same level
            let level_0: Vec<_> = segments
                .iter()
                .filter(|s| s.metadata.level == 0)
                .cloned()
                .collect();

            if level_0.len() >= 4 {
                // Merge level 0 segments into level 1
                // This is simplified - real implementation would be more sophisticated
                let schema = match self.schemas.get(&table_id) {
                    Some(s) => s.clone(),
                    None => continue,
                };

                // Read all batches from level 0 segments
                let column_names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
                let mut batches = Vec::new();
                for segment in &level_0 {
                    if let Ok(batch) = self.read_segment_columns(segment, &column_names, &schema) {
                        batches.push(batch);
                    }
                }

                if !batches.is_empty() {
                    // Create merged segment at level 1
                    let mut merged_segment = self.create_segment(table_id, schema, batches)?;
                    merged_segment.metadata.level = 1;

                    // Replace old segments with merged segment
                    drop(entry);
                    if let Some(mut segs) = self.segments.get_mut(&table_id) {
                        segs.retain(|s| !level_0.iter().any(|l0| l0.metadata.segment_id == s.metadata.segment_id));
                        segs.push(Arc::new(merged_segment));
                    }
                }
            }
        }

        Ok(())
    }

    /// Get segment count for a table.
    pub fn segment_count(&self, table_id: TableId) -> usize {
        self.segments.get(&table_id).map(|s| s.len()).unwrap_or(0)
    }

    /// Get total row count for a table.
    pub fn row_count(&self, table_id: TableId) -> u64 {
        self.segments
            .get(&table_id)
            .map(|s| s.iter().map(|seg| seg.metadata.row_count).sum())
            .unwrap_or(0)
    }
}

/// Serialize an Arrow array to bytes (simplified).
fn serialize_column(array: ArrayRef) -> Result<Bytes> {
    // Use Arrow IPC format for serialization
    use arrow::ipc::writer::StreamWriter;

    let field = Field::new("col", array.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array])
        .map_err(|e| Error::Internal(format!("Failed to create batch: {}", e)))?;

    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)
        .map_err(|e| Error::Internal(format!("Failed to create writer: {}", e)))?;

    writer.write(&batch)
        .map_err(|e| Error::Internal(format!("Failed to write batch: {}", e)))?;
    writer.finish()
        .map_err(|e| Error::Internal(format!("Failed to finish writer: {}", e)))?;

    Ok(Bytes::from(buf))
}

/// Deserialize an Arrow array from bytes (simplified).
fn deserialize_column(data: &[u8]) -> Result<ArrayRef> {
    use arrow::ipc::reader::StreamReader;

    let cursor = std::io::Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| Error::Internal(format!("Failed to create reader: {}", e)))?;

    let batches: Vec<_> = reader
        .filter_map(|b| b.ok())
        .collect();

    if batches.is_empty() {
        return Err(Error::Internal("No data in column".into()));
    }

    Ok(batches[0].column(0).clone())
}

/// Compute column statistics.
fn compute_column_statistics(column_name: &str, array: ArrayRef) -> ColumnStatistics {
    let row_count = array.len() as u64;
    let null_count = array.null_count() as u64;

    ColumnStatistics {
        column_name: column_name.to_string(),
        row_count,
        null_count,
        min_value: None, // Would compute from array
        max_value: None, // Would compute from array
        distinct_count: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: SchemaRef, start_id: i64, count: usize) -> RecordBatch {
        let ids: Vec<i64> = (start_id..(start_id + count as i64)).collect();
        let names: Vec<&str> = (0..count).map(|i| if i % 2 == 0 { "Alice" } else { "Bob" }).collect();

        let id_array = Arc::new(Int64Array::from(ids));
        let name_array = Arc::new(StringArray::from(names));

        RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
    }

    #[test]
    fn test_column_statistics() {
        let array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let stats = compute_column_statistics("test", array);

        assert_eq!(stats.column_name, "test");
        assert_eq!(stats.row_count, 5);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_write_buffer() {
        let schema = create_test_schema();
        let buffer = WriteBuffer::new(schema.clone(), 100);

        let batch = create_test_batch(schema, 0, 50);
        let should_flush = buffer.add_batch(batch).unwrap();

        assert!(!should_flush);
        assert_eq!(buffer.row_count(), 50);
    }

    #[test]
    fn test_write_buffer_flush() {
        let schema = create_test_schema();
        let buffer = WriteBuffer::new(schema.clone(), 100);

        let batch1 = create_test_batch(schema.clone(), 0, 60);
        let batch2 = create_test_batch(schema, 60, 60);

        buffer.add_batch(batch1).unwrap();
        let should_flush = buffer.add_batch(batch2).unwrap();

        assert!(should_flush);
        assert_eq!(buffer.row_count(), 120);

        let batches = buffer.drain();
        assert_eq!(batches.len(), 2);
        assert_eq!(buffer.row_count(), 0);
    }

    #[tokio::test]
    async fn test_column_store_append() {
        let store = ColumnStoreImpl::new(CompressionAlgorithm::Lz4, 100);
        let table_id = TableId(1);
        let schema = create_test_schema();

        store.register_table(table_id, schema.clone());

        let batch = create_test_batch(schema, 0, 50);
        store.append_batch(table_id, batch).await.unwrap();

        // Buffer not full yet, no segments
        assert_eq!(store.segment_count(table_id), 0);
    }

    #[tokio::test]
    async fn test_column_store_flush() {
        let store = ColumnStoreImpl::new(CompressionAlgorithm::Lz4, 50);
        let table_id = TableId(1);
        let schema = create_test_schema();

        store.register_table(table_id, schema.clone());

        // Add enough rows to trigger flush
        let batch = create_test_batch(schema, 0, 60);
        store.append_batch(table_id, batch).await.unwrap();

        // Should have one segment now
        assert_eq!(store.segment_count(table_id), 1);
        assert_eq!(store.row_count(table_id), 60);
    }

    #[tokio::test]
    async fn test_column_store_scan() {
        let store = ColumnStoreImpl::new(CompressionAlgorithm::None, 50);
        let table_id = TableId(1);
        let schema = create_test_schema();

        store.register_table(table_id, schema.clone());

        // Add data
        let batch = create_test_batch(schema, 0, 60);
        store.append_batch(table_id, batch).await.unwrap();

        // Scan all columns
        let result = store.scan(table_id, &[], None).await.unwrap();

        assert_eq!(result.num_rows(), 60);
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn test_serialize_deserialize_column() {
        let array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let bytes = serialize_column(array.clone()).unwrap();
        let decoded = deserialize_column(&bytes).unwrap();

        assert_eq!(decoded.len(), 5);
    }
}
