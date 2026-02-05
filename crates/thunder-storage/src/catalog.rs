//! Catalog implementation for ThunderDB.
//!
//! Manages metadata for:
//! - Tables (schema, columns, constraints)
//! - Indexes (B+Tree, hash, etc.)
//! - Sequences
//! - System statistics

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thunder_common::prelude::*;

/// Column definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column ID (position in table)
    pub id: ColumnId,
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Whether column allows NULL values
    pub nullable: bool,
    /// Default value expression (if any)
    pub default_expr: Option<String>,
}

impl ColumnDef {
    pub fn new(id: ColumnId, name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            id,
            name: name.into(),
            data_type,
            nullable: true,
            default_expr: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn with_default(mut self, expr: impl Into<String>) -> Self {
        self.default_expr = Some(expr.into());
        self
    }
}

/// Data types supported by ThunderDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    /// Boolean
    Boolean,
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float64,
    /// Variable-length string
    String,
    /// Fixed-length binary
    Binary,
    /// Variable-length binary
    VarBinary,
    /// Date (days since epoch)
    Date,
    /// Time (microseconds since midnight)
    Time,
    /// Timestamp (microseconds since epoch)
    Timestamp,
    /// Timestamp with timezone
    TimestampTz,
    /// UUID
    Uuid,
    /// JSON
    Json,
    /// Decimal with precision and scale
    Decimal { precision: u8, scale: u8 },
    /// Array of another type
    Array,
    /// Vector (for embeddings)
    Vector { dimensions: u32 },
}

impl DataType {
    /// Get the size in bytes for fixed-size types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            DataType::Boolean => Some(1),
            DataType::Int8 => Some(1),
            DataType::Int16 => Some(2),
            DataType::Int32 => Some(4),
            DataType::Int64 => Some(8),
            DataType::Float32 => Some(4),
            DataType::Float64 => Some(8),
            DataType::Date => Some(4),
            DataType::Time => Some(8),
            DataType::Timestamp => Some(8),
            DataType::TimestampTz => Some(12),
            DataType::Uuid => Some(16),
            DataType::Vector { dimensions } => Some(*dimensions as usize * 4),
            _ => None,
        }
    }

    /// Check if this type is variable-length.
    pub fn is_variable_length(&self) -> bool {
        matches!(
            self,
            DataType::String
                | DataType::Binary
                | DataType::VarBinary
                | DataType::Json
                | DataType::Array
        )
    }
}

/// Primary key constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKeyConstraint {
    pub name: String,
    pub columns: Vec<ColumnId>,
}

/// Foreign key constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    pub name: String,
    pub columns: Vec<ColumnId>,
    pub referenced_table: TableId,
    pub referenced_columns: Vec<ColumnId>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

/// Foreign key action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl Default for ForeignKeyAction {
    fn default() -> Self {
        ForeignKeyAction::NoAction
    }
}

/// Unique constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraint {
    pub name: String,
    pub columns: Vec<ColumnId>,
}

/// Check constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: String,
    pub expression: String,
}

/// Table statistics for query planning.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableStats {
    /// Estimated row count
    pub row_count: u64,
    /// Number of pages
    pub page_count: u64,
    /// Average row size in bytes
    pub avg_row_size: u32,
    /// Last analyze timestamp
    pub last_analyzed: Option<i64>,
}

/// Column statistics for query planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Column ID
    pub column_id: ColumnId,
    /// Number of distinct values
    pub distinct_count: u64,
    /// Number of NULL values
    pub null_count: u64,
    /// Minimum value (as bytes)
    pub min_value: Option<Vec<u8>>,
    /// Maximum value (as bytes)
    pub max_value: Option<Vec<u8>>,
    /// Average value size (for variable-length columns)
    pub avg_size: Option<u32>,
}

/// Table descriptor containing all table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDescriptor {
    /// Table ID
    pub id: TableId,
    /// Table name
    pub name: String,
    /// Schema/namespace name
    pub schema: String,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Primary key constraint
    pub primary_key: Option<PrimaryKeyConstraint>,
    /// Foreign key constraints
    pub foreign_keys: Vec<ForeignKeyConstraint>,
    /// Unique constraints
    pub unique_constraints: Vec<UniqueConstraint>,
    /// Check constraints
    pub check_constraints: Vec<CheckConstraint>,
    /// Table statistics
    pub stats: TableStats,
    /// Column statistics
    pub column_stats: HashMap<ColumnId, ColumnStats>,
    /// First data page ID
    pub first_page_id: Option<PageId>,
    /// Is this a system table
    pub is_system: bool,
}

impl TableDescriptor {
    pub fn new(id: TableId, name: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            schema: schema.into(),
            columns: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            stats: TableStats::default(),
            column_stats: HashMap::new(),
            first_page_id: None,
            is_system: false,
        }
    }

    /// Add a column to the table.
    pub fn add_column(&mut self, column: ColumnDef) {
        self.columns.push(column);
    }

    /// Set the primary key.
    pub fn set_primary_key(&mut self, name: impl Into<String>, columns: Vec<ColumnId>) {
        self.primary_key = Some(PrimaryKeyConstraint {
            name: name.into(),
            columns,
        });
    }

    /// Get column by name.
    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column by ID.
    pub fn get_column_by_id(&self, id: ColumnId) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.id == id)
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Index type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// B+Tree index (default)
    BTree,
    /// Hash index
    Hash,
    /// GiST index (for geometric types)
    Gist,
    /// GIN index (for full-text search)
    Gin,
    /// HNSW index (for vector similarity)
    Hnsw,
}

impl Default for IndexType {
    fn default() -> Self {
        IndexType::BTree
    }
}

/// Index descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDescriptor {
    /// Index ID
    pub id: IndexId,
    /// Index name
    pub name: String,
    /// Table this index belongs to
    pub table_id: TableId,
    /// Columns in the index (in order)
    pub columns: Vec<ColumnId>,
    /// Index type
    pub index_type: IndexType,
    /// Is this a unique index
    pub is_unique: bool,
    /// Is this a primary key index
    pub is_primary: bool,
    /// Root page ID
    pub root_page_id: Option<PageId>,
    /// Index-specific options (JSON)
    pub options: HashMap<String, String>,
}

impl IndexDescriptor {
    pub fn new(
        id: IndexId,
        name: impl Into<String>,
        table_id: TableId,
        columns: Vec<ColumnId>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            table_id,
            columns,
            index_type: IndexType::BTree,
            is_unique: false,
            is_primary: false,
            root_page_id: None,
            options: HashMap::new(),
        }
    }

    pub fn unique(mut self) -> Self {
        self.is_unique = true;
        self
    }

    pub fn primary(mut self) -> Self {
        self.is_primary = true;
        self.is_unique = true;
        self
    }

    pub fn with_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }
}

/// Sequence descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceDescriptor {
    /// Sequence ID
    pub id: u64,
    /// Sequence name
    pub name: String,
    /// Schema name
    pub schema: String,
    /// Current value
    pub current_value: i64,
    /// Increment by
    pub increment: i64,
    /// Minimum value
    pub min_value: i64,
    /// Maximum value
    pub max_value: i64,
    /// Cycle when reaching max
    pub cycle: bool,
}

impl SequenceDescriptor {
    pub fn new(id: u64, name: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            schema: schema.into(),
            current_value: 0,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
        }
    }
}

/// In-memory sequence with atomic operations.
pub struct Sequence {
    descriptor: RwLock<SequenceDescriptor>,
    current: AtomicU64,
}

impl Sequence {
    pub fn new(descriptor: SequenceDescriptor) -> Self {
        let current = descriptor.current_value as u64;
        Self {
            descriptor: RwLock::new(descriptor),
            current: AtomicU64::new(current),
        }
    }

    /// Get the next value from the sequence.
    pub fn next_value(&self) -> Result<i64> {
        let desc = self.descriptor.read();
        let increment = desc.increment;
        let max_value = desc.max_value;
        let min_value = desc.min_value;
        let cycle = desc.cycle;
        drop(desc);

        loop {
            let current = self.current.load(Ordering::SeqCst) as i64;
            let next = current + increment;

            if next > max_value {
                if cycle {
                    if self
                        .current
                        .compare_exchange(
                            current as u64,
                            min_value as u64,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        return Ok(min_value);
                    }
                } else {
                    return Err(Error::Internal("Sequence exhausted".into()));
                }
            } else if self
                .current
                .compare_exchange(
                    current as u64,
                    next as u64,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return Ok(next);
            }
        }
    }

    /// Get the current value without incrementing.
    pub fn current_value(&self) -> i64 {
        self.current.load(Ordering::SeqCst) as i64
    }

    /// Set the current value.
    pub fn set_value(&self, value: i64) {
        self.current.store(value as u64, Ordering::SeqCst);
    }

    /// Get the descriptor.
    pub fn descriptor(&self) -> SequenceDescriptor {
        self.descriptor.read().clone()
    }
}

/// Catalog implementation.
pub struct Catalog {
    /// Table descriptors
    tables: DashMap<TableId, TableDescriptor>,
    /// Table name to ID mapping
    table_names: DashMap<(String, String), TableId>,
    /// Index descriptors
    indexes: DashMap<IndexId, IndexDescriptor>,
    /// Index name to ID mapping
    index_names: DashMap<(String, String), IndexId>,
    /// Table to index mapping
    table_indexes: DashMap<TableId, Vec<IndexId>>,
    /// Sequences
    sequences: DashMap<u64, Arc<Sequence>>,
    /// Sequence name to ID mapping
    sequence_names: DashMap<(String, String), u64>,
    /// Next table ID
    next_table_id: AtomicU64,
    /// Next index ID
    next_index_id: AtomicU64,
    /// Next sequence ID
    next_sequence_id: AtomicU64,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    /// Create a new empty catalog.
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            table_names: DashMap::new(),
            indexes: DashMap::new(),
            index_names: DashMap::new(),
            table_indexes: DashMap::new(),
            sequences: DashMap::new(),
            sequence_names: DashMap::new(),
            next_table_id: AtomicU64::new(1),
            next_index_id: AtomicU64::new(1),
            next_sequence_id: AtomicU64::new(1),
        }
    }

    /// Create a new table.
    pub fn create_table(&self, mut descriptor: TableDescriptor) -> Result<TableId> {
        let schema_name = (descriptor.schema.clone(), descriptor.name.clone());

        // Check if table already exists
        if self.table_names.contains_key(&schema_name) {
            return Err(Error::AlreadyExists("Table".into(), descriptor.name.clone()));
        }

        // Assign table ID if not set
        if descriptor.id.0 == 0 {
            descriptor.id = TableId(self.next_table_id.fetch_add(1, Ordering::SeqCst));
        }

        let table_id = descriptor.id;
        self.tables.insert(table_id, descriptor);
        self.table_names.insert(schema_name, table_id);
        self.table_indexes.insert(table_id, Vec::new());

        Ok(table_id)
    }

    /// Get a table by ID.
    pub fn get_table(&self, table_id: TableId) -> Option<TableDescriptor> {
        self.tables.get(&table_id).map(|r| r.clone())
    }

    /// Get a table by name.
    pub fn get_table_by_name(&self, schema: &str, name: &str) -> Option<TableDescriptor> {
        let key = (schema.to_string(), name.to_string());
        self.table_names
            .get(&key)
            .and_then(|id| self.tables.get(&id).map(|t| t.clone()))
    }

    /// Update a table.
    pub fn update_table(&self, descriptor: TableDescriptor) -> Result<()> {
        let table_id = descriptor.id;
        if !self.tables.contains_key(&table_id) {
            return Err(Error::NotFound("Table".into(), table_id.0.to_string()));
        }
        self.tables.insert(table_id, descriptor);
        Ok(())
    }

    /// Drop a table.
    pub fn drop_table(&self, table_id: TableId) -> Result<()> {
        let table = self.tables.remove(&table_id).map(|(_, v)| v);

        if let Some(table) = table {
            let key = (table.schema.clone(), table.name.clone());
            self.table_names.remove(&key);

            // Remove associated indexes
            if let Some((_, index_ids)) = self.table_indexes.remove(&table_id) {
                for index_id in index_ids {
                    if let Some((_, idx)) = self.indexes.remove(&index_id) {
                        let idx_key = (table.schema.clone(), idx.name);
                        self.index_names.remove(&idx_key);
                    }
                }
            }

            Ok(())
        } else {
            Err(Error::NotFound("Table".into(), table_id.0.to_string()))
        }
    }

    /// List all tables.
    pub fn list_tables(&self) -> Vec<TableDescriptor> {
        self.tables.iter().map(|r| r.value().clone()).collect()
    }

    /// List tables in a schema.
    pub fn list_tables_in_schema(&self, schema: &str) -> Vec<TableDescriptor> {
        self.tables
            .iter()
            .filter(|r| r.schema == schema)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Create an index.
    pub fn create_index(&self, mut descriptor: IndexDescriptor) -> Result<IndexId> {
        // Get table to find schema
        let table = self.get_table(descriptor.table_id).ok_or_else(|| {
            Error::NotFound("Table".into(), descriptor.table_id.0.to_string())
        })?;

        let schema_name = (table.schema.clone(), descriptor.name.clone());

        // Check if index already exists
        if self.index_names.contains_key(&schema_name) {
            return Err(Error::AlreadyExists("Index".into(), descriptor.name.clone()));
        }

        // Assign index ID if not set
        if descriptor.id.0 == 0 {
            descriptor.id = IndexId(self.next_index_id.fetch_add(1, Ordering::SeqCst));
        }

        let index_id = descriptor.id;
        let table_id = descriptor.table_id;

        self.indexes.insert(index_id, descriptor);
        self.index_names.insert(schema_name, index_id);

        // Add to table's index list
        self.table_indexes
            .entry(table_id)
            .or_insert_with(Vec::new)
            .push(index_id);

        Ok(index_id)
    }

    /// Get an index by ID.
    pub fn get_index(&self, index_id: IndexId) -> Option<IndexDescriptor> {
        self.indexes.get(&index_id).map(|r| r.clone())
    }

    /// Get indexes for a table.
    pub fn get_table_indexes(&self, table_id: TableId) -> Vec<IndexDescriptor> {
        self.table_indexes
            .get(&table_id)
            .map(|index_ids| {
                index_ids
                    .iter()
                    .filter_map(|id| self.indexes.get(id).map(|r| r.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drop an index.
    pub fn drop_index(&self, index_id: IndexId) -> Result<()> {
        let index = self.indexes.remove(&index_id).map(|(_, v)| v);

        if let Some(index) = index {
            // Get table for schema
            if let Some(table) = self.get_table(index.table_id) {
                let key = (table.schema, index.name.clone());
                self.index_names.remove(&key);
            }

            // Remove from table's index list
            if let Some(mut index_ids) = self.table_indexes.get_mut(&index.table_id) {
                index_ids.retain(|id| *id != index_id);
            }

            Ok(())
        } else {
            Err(Error::NotFound("Index".into(), index_id.0.to_string()))
        }
    }

    /// Create a sequence.
    pub fn create_sequence(&self, mut descriptor: SequenceDescriptor) -> Result<u64> {
        let schema_name = (descriptor.schema.clone(), descriptor.name.clone());

        if self.sequence_names.contains_key(&schema_name) {
            return Err(Error::Internal(format!(
                "Sequence {} already exists",
                descriptor.name
            )));
        }

        if descriptor.id == 0 {
            descriptor.id = self.next_sequence_id.fetch_add(1, Ordering::SeqCst);
        }

        let id = descriptor.id;
        self.sequences
            .insert(id, Arc::new(Sequence::new(descriptor)));
        self.sequence_names.insert(schema_name, id);

        Ok(id)
    }

    /// Get next value from a sequence.
    pub fn next_sequence_value(&self, sequence_id: u64) -> Result<i64> {
        self.sequences
            .get(&sequence_id)
            .ok_or_else(|| Error::Internal(format!("Sequence {} not found", sequence_id)))
            .and_then(|seq| seq.next_value())
    }

    /// Get a sequence by name.
    pub fn get_sequence(&self, schema: &str, name: &str) -> Option<Arc<Sequence>> {
        let key = (schema.to_string(), name.to_string());
        self.sequence_names
            .get(&key)
            .and_then(|id| self.sequences.get(&id).map(|s| Arc::clone(&s)))
    }

    /// Update table statistics.
    pub fn update_table_stats(&self, table_id: TableId, stats: TableStats) -> Result<()> {
        if let Some(mut table) = self.tables.get_mut(&table_id) {
            table.stats = stats;
            Ok(())
        } else {
            Err(Error::NotFound("Table".into(), table_id.0.to_string()))
        }
    }

    /// Update column statistics.
    pub fn update_column_stats(
        &self,
        table_id: TableId,
        column_id: ColumnId,
        stats: ColumnStats,
    ) -> Result<()> {
        if let Some(mut table) = self.tables.get_mut(&table_id) {
            table.column_stats.insert(column_id, stats);
            Ok(())
        } else {
            Err(Error::NotFound("Table".into(), table_id.0.to_string()))
        }
    }

    /// Get table count.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Get index count.
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_def() {
        let col = ColumnDef::new(ColumnId(1), "name", DataType::String)
            .not_null()
            .with_default("''");

        assert_eq!(col.name, "name");
        assert!(!col.nullable);
        assert_eq!(col.default_expr, Some("''".to_string()));
    }

    #[test]
    fn test_data_type_size() {
        assert_eq!(DataType::Int32.fixed_size(), Some(4));
        assert_eq!(DataType::Int64.fixed_size(), Some(8));
        assert_eq!(DataType::String.fixed_size(), None);
        assert!(DataType::String.is_variable_length());
    }

    #[test]
    fn test_table_descriptor() {
        let mut table = TableDescriptor::new(TableId(1), "users", "public");
        table.add_column(ColumnDef::new(ColumnId(0), "id", DataType::Int64).not_null());
        table.add_column(ColumnDef::new(ColumnId(1), "name", DataType::String));
        table.set_primary_key("users_pkey", vec![ColumnId(0)]);

        assert_eq!(table.column_count(), 2);
        assert!(table.get_column("id").is_some());
        assert!(table.primary_key.is_some());
    }

    #[test]
    fn test_index_descriptor() {
        let index = IndexDescriptor::new(
            IndexId(1),
            "users_name_idx",
            TableId(1),
            vec![ColumnId(1)],
        )
        .with_type(IndexType::BTree);

        assert_eq!(index.index_type, IndexType::BTree);
        assert!(!index.is_unique);
    }

    #[test]
    fn test_catalog_create_table() {
        let catalog = Catalog::new();

        let mut table = TableDescriptor::new(TableId(0), "users", "public");
        table.add_column(ColumnDef::new(ColumnId(0), "id", DataType::Int64));

        let table_id = catalog.create_table(table).unwrap();
        assert!(table_id.0 > 0);

        let retrieved = catalog.get_table(table_id).unwrap();
        assert_eq!(retrieved.name, "users");
    }

    #[test]
    fn test_catalog_create_duplicate_table() {
        let catalog = Catalog::new();

        let table1 = TableDescriptor::new(TableId(0), "users", "public");
        catalog.create_table(table1).unwrap();

        let table2 = TableDescriptor::new(TableId(0), "users", "public");
        assert!(catalog.create_table(table2).is_err());
    }

    #[test]
    fn test_catalog_get_table_by_name() {
        let catalog = Catalog::new();

        let table = TableDescriptor::new(TableId(0), "users", "public");
        catalog.create_table(table).unwrap();

        let retrieved = catalog.get_table_by_name("public", "users").unwrap();
        assert_eq!(retrieved.name, "users");
    }

    #[test]
    fn test_catalog_drop_table() {
        let catalog = Catalog::new();

        let table = TableDescriptor::new(TableId(0), "users", "public");
        let table_id = catalog.create_table(table).unwrap();

        catalog.drop_table(table_id).unwrap();
        assert!(catalog.get_table(table_id).is_none());
    }

    #[test]
    fn test_catalog_create_index() {
        let catalog = Catalog::new();

        let table = TableDescriptor::new(TableId(0), "users", "public");
        let table_id = catalog.create_table(table).unwrap();

        let index =
            IndexDescriptor::new(IndexId(0), "users_name_idx", table_id, vec![ColumnId(1)]);
        let index_id = catalog.create_index(index).unwrap();
        assert!(index_id.0 > 0);

        let indexes = catalog.get_table_indexes(table_id);
        assert_eq!(indexes.len(), 1);
    }

    #[test]
    fn test_catalog_drop_table_with_indexes() {
        let catalog = Catalog::new();

        let table = TableDescriptor::new(TableId(0), "users", "public");
        let table_id = catalog.create_table(table).unwrap();

        let index = IndexDescriptor::new(IndexId(0), "users_idx", table_id, vec![ColumnId(0)]);
        catalog.create_index(index).unwrap();

        catalog.drop_table(table_id).unwrap();
        assert_eq!(catalog.index_count(), 0);
    }

    #[test]
    fn test_sequence() {
        let desc = SequenceDescriptor::new(1, "user_id_seq", "public");
        let seq = Sequence::new(desc);

        let v1 = seq.next_value().unwrap();
        let v2 = seq.next_value().unwrap();
        let v3 = seq.next_value().unwrap();

        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
        assert_eq!(v3, 3);
    }

    #[test]
    fn test_catalog_sequence() {
        let catalog = Catalog::new();

        let desc = SequenceDescriptor::new(0, "user_id_seq", "public");
        let seq_id = catalog.create_sequence(desc).unwrap();

        let v1 = catalog.next_sequence_value(seq_id).unwrap();
        let v2 = catalog.next_sequence_value(seq_id).unwrap();

        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
    }

    #[test]
    fn test_table_stats() {
        let catalog = Catalog::new();

        let table = TableDescriptor::new(TableId(0), "users", "public");
        let table_id = catalog.create_table(table).unwrap();

        let stats = TableStats {
            row_count: 1000,
            page_count: 50,
            avg_row_size: 128,
            last_analyzed: Some(1234567890),
        };

        catalog.update_table_stats(table_id, stats.clone()).unwrap();

        let retrieved = catalog.get_table(table_id).unwrap();
        assert_eq!(retrieved.stats.row_count, 1000);
    }

    #[test]
    fn test_list_tables() {
        let catalog = Catalog::new();

        catalog
            .create_table(TableDescriptor::new(TableId(0), "users", "public"))
            .unwrap();
        catalog
            .create_table(TableDescriptor::new(TableId(0), "orders", "public"))
            .unwrap();
        catalog
            .create_table(TableDescriptor::new(TableId(0), "products", "sales"))
            .unwrap();

        let all = catalog.list_tables();
        assert_eq!(all.len(), 3);

        let public = catalog.list_tables_in_schema("public");
        assert_eq!(public.len(), 2);
    }
}
