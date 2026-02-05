//! SQL Catalog
//!
//! Provides metadata management for tables, columns, indexes, and schemas.
//! The catalog is the source of truth for schema resolution during query analysis.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thunder_common::error::SqlError;
use thunder_common::prelude::*;

/// A SQL catalog for managing database metadata
#[derive(Debug)]
pub struct Catalog {
    /// Tables indexed by name (case-insensitive)
    tables: DashMap<String, Arc<TableInfo>>,
    /// Tables indexed by ID
    tables_by_id: DashMap<TableId, Arc<TableInfo>>,
    /// Indexes indexed by name
    indexes: DashMap<String, Arc<IndexInfo>>,
    /// Indexes by table
    indexes_by_table: DashMap<TableId, Vec<Arc<IndexInfo>>>,
    /// Schema name (default: "public")
    schema_name: String,
    /// Next table ID
    next_table_id: AtomicU64,
    /// Next index ID
    next_index_id: AtomicU64,
}

/// Information about a table
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table ID
    pub id: TableId,
    /// Table name
    pub name: String,
    /// Schema of the table
    pub schema: Schema,
    /// Column information
    pub columns: Vec<ColumnInfo>,
    /// Primary key column indices
    pub primary_key: Vec<usize>,
    /// Table statistics
    pub stats: TableStats,
}

/// Information about a column
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column ID (unique within table)
    pub id: ColumnId,
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Whether the column is nullable
    pub nullable: bool,
    /// Default value expression (if any)
    pub default_expr: Option<String>,
    /// Whether this column is part of primary key
    pub is_primary_key: bool,
    /// Column statistics
    pub stats: ColumnStats,
}

/// Statistics about a table
#[derive(Debug, Clone, Default)]
pub struct TableStats {
    /// Estimated row count
    pub row_count: u64,
    /// Size in bytes
    pub size_bytes: u64,
    /// Last update timestamp
    pub last_updated: u64,
}

/// Statistics about a column
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    /// Number of distinct values
    pub distinct_count: u64,
    /// Number of null values
    pub null_count: u64,
    /// Minimum value (as string for simplicity)
    pub min_value: Option<String>,
    /// Maximum value (as string for simplicity)
    pub max_value: Option<String>,
    /// Average length (for variable-length types)
    pub avg_length: Option<f64>,
}

/// Information about an index
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index ID
    pub id: IndexId,
    /// Index name
    pub name: String,
    /// Table this index belongs to
    pub table_id: TableId,
    /// Columns in the index (by position)
    pub columns: Vec<IndexColumn>,
    /// Whether this is a unique index
    pub is_unique: bool,
    /// Whether this is the primary key index
    pub is_primary: bool,
    /// Index type
    pub index_type: IndexType,
}

/// A column in an index
#[derive(Debug, Clone)]
pub struct IndexColumn {
    /// Column index in the table schema
    pub column_index: usize,
    /// Sort order
    pub ascending: bool,
    /// Nulls first or last
    pub nulls_first: bool,
}

/// Type of index
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// B-tree index (default)
    BTree,
    /// Hash index
    Hash,
    /// GiST (Generalized Search Tree)
    GiST,
    /// GIN (Generalized Inverted Index)
    GIN,
    /// BRIN (Block Range Index)
    BRIN,
    /// Vector index (HNSW)
    Vector,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            tables_by_id: DashMap::new(),
            indexes: DashMap::new(),
            indexes_by_table: DashMap::new(),
            schema_name: "public".to_string(),
            next_table_id: AtomicU64::new(1),
            next_index_id: AtomicU64::new(1),
        }
    }

    /// Create a new catalog with a schema name
    pub fn with_schema(schema_name: impl Into<String>) -> Self {
        Self {
            schema_name: schema_name.into(),
            ..Self::new()
        }
    }

    /// Get the schema name
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    /// Register a table in the catalog
    pub fn register_table(&self, name: impl Into<String>, schema: Schema) -> Result<TableId> {
        let name = name.into();
        let name_lower = name.to_lowercase();

        // Check if table already exists
        if self.tables.contains_key(&name_lower) {
            return Err(Error::AlreadyExists("Table".to_string(), name));
        }

        let table_id = TableId(self.next_table_id.fetch_add(1, Ordering::SeqCst));

        // Build column info
        let columns: Vec<ColumnInfo> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| ColumnInfo {
                id: ColumnId(i as u32),
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
                default_expr: col.default.as_ref().map(|v| v.to_string()),
                is_primary_key: false,
                stats: ColumnStats::default(),
            })
            .collect();

        let table_info = Arc::new(TableInfo {
            id: table_id,
            name: name.clone(),
            schema,
            columns,
            primary_key: vec![],
            stats: TableStats::default(),
        });

        self.tables.insert(name_lower.clone(), table_info.clone());
        self.tables_by_id.insert(table_id, table_info);
        self.indexes_by_table.insert(table_id, vec![]);

        Ok(table_id)
    }

    /// Register a table with primary key
    pub fn register_table_with_pk(
        &self,
        name: impl Into<String>,
        schema: Schema,
        primary_key: Vec<usize>,
    ) -> Result<TableId> {
        let name = name.into();
        let name_lower = name.to_lowercase();

        if self.tables.contains_key(&name_lower) {
            return Err(Error::AlreadyExists("Table".to_string(), name));
        }

        let table_id = TableId(self.next_table_id.fetch_add(1, Ordering::SeqCst));

        // Build column info with primary key marking
        let columns: Vec<ColumnInfo> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| ColumnInfo {
                id: ColumnId(i as u32),
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable && !primary_key.contains(&i),
                default_expr: col.default.as_ref().map(|v| v.to_string()),
                is_primary_key: primary_key.contains(&i),
                stats: ColumnStats::default(),
            })
            .collect();

        let table_info = Arc::new(TableInfo {
            id: table_id,
            name: name.clone(),
            schema,
            columns,
            primary_key: primary_key.clone(),
            stats: TableStats::default(),
        });

        self.tables.insert(name_lower.clone(), table_info.clone());
        self.tables_by_id.insert(table_id, table_info);

        // Create primary key index
        let pk_index = Arc::new(IndexInfo {
            id: IndexId(self.next_index_id.fetch_add(1, Ordering::SeqCst)),
            name: format!("{}_pkey", name_lower),
            table_id,
            columns: primary_key
                .iter()
                .map(|&i| IndexColumn {
                    column_index: i,
                    ascending: true,
                    nulls_first: false,
                })
                .collect(),
            is_unique: true,
            is_primary: true,
            index_type: IndexType::BTree,
        });

        self.indexes
            .insert(pk_index.name.clone(), pk_index.clone());
        self.indexes_by_table.insert(table_id, vec![pk_index]);

        Ok(table_id)
    }

    /// Drop a table from the catalog
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let name_lower = name.to_lowercase();

        let table_info = self
            .tables
            .remove(&name_lower)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(name.to_string())))?;

        let table_id = table_info.1.id;
        self.tables_by_id.remove(&table_id);

        // Remove all indexes for this table
        if let Some((_, indexes)) = self.indexes_by_table.remove(&table_id) {
            for index in indexes {
                self.indexes.remove(&index.name);
            }
        }

        Ok(())
    }

    /// Get a table by name
    pub fn get_table(&self, name: &str) -> Option<Arc<TableInfo>> {
        self.tables.get(&name.to_lowercase()).map(|r| r.clone())
    }

    /// Get a table by ID
    pub fn get_table_by_id(&self, id: TableId) -> Option<Arc<TableInfo>> {
        self.tables_by_id.get(&id).map(|r| r.clone())
    }

    /// Check if a table exists
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(&name.to_lowercase())
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|r| r.value().name.clone()).collect()
    }

    /// Register an index
    pub fn register_index(
        &self,
        name: impl Into<String>,
        table_name: &str,
        columns: Vec<IndexColumn>,
        is_unique: bool,
        index_type: IndexType,
    ) -> Result<IndexId> {
        let name = name.into();
        let name_lower = name.to_lowercase();

        if self.indexes.contains_key(&name_lower) {
            return Err(Error::AlreadyExists("Index".to_string(), name));
        }

        let table = self
            .get_table(table_name)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?;

        let index_id = IndexId(self.next_index_id.fetch_add(1, Ordering::SeqCst));

        let index_info = Arc::new(IndexInfo {
            id: index_id,
            name: name.clone(),
            table_id: table.id,
            columns,
            is_unique,
            is_primary: false,
            index_type,
        });

        self.indexes.insert(name_lower, index_info.clone());

        // Add to indexes_by_table
        self.indexes_by_table
            .entry(table.id)
            .or_insert_with(Vec::new)
            .push(index_info);

        Ok(index_id)
    }

    /// Drop an index
    pub fn drop_index(&self, name: &str) -> Result<()> {
        let name_lower = name.to_lowercase();

        let (_, index_info) = self
            .indexes
            .remove(&name_lower)
            .ok_or_else(|| Error::NotFound("Index".to_string(), name.to_string()))?;

        // Remove from indexes_by_table
        if let Some(mut indexes) = self.indexes_by_table.get_mut(&index_info.table_id) {
            indexes.retain(|i| i.name != index_info.name);
        }

        Ok(())
    }

    /// Get an index by name
    pub fn get_index(&self, name: &str) -> Option<Arc<IndexInfo>> {
        self.indexes.get(&name.to_lowercase()).map(|r| r.clone())
    }

    /// Get all indexes for a table
    pub fn get_table_indexes(&self, table_id: TableId) -> Vec<Arc<IndexInfo>> {
        self.indexes_by_table
            .get(&table_id)
            .map(|r| r.clone())
            .unwrap_or_default()
    }

    /// Get indexes that cover a set of columns (useful for query optimization)
    pub fn find_covering_indexes(&self, table_id: TableId, columns: &[usize]) -> Vec<Arc<IndexInfo>> {
        self.get_table_indexes(table_id)
            .into_iter()
            .filter(|idx| {
                // Check if this index covers all requested columns
                let idx_cols: Vec<_> = idx.columns.iter().map(|c| c.column_index).collect();
                columns.iter().all(|c| idx_cols.contains(c))
            })
            .collect()
    }

    /// Update table statistics
    pub fn update_table_stats(&self, table_name: &str, stats: TableStats) -> Result<()> {
        let name_lower = table_name.to_lowercase();

        let old_info = self
            .tables
            .get(&name_lower)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?
            .clone();

        let new_info = Arc::new(TableInfo {
            stats,
            ..(*old_info).clone()
        });

        self.tables.insert(name_lower, new_info.clone());
        self.tables_by_id.insert(old_info.id, new_info);

        Ok(())
    }

    /// Update column statistics
    pub fn update_column_stats(
        &self,
        table_name: &str,
        column_name: &str,
        stats: ColumnStats,
    ) -> Result<()> {
        let name_lower = table_name.to_lowercase();

        let old_info = self
            .tables
            .get(&name_lower)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?
            .clone();

        let mut columns = old_info.columns.clone();
        let col_idx = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(column_name))
            .ok_or_else(|| Error::Sql(SqlError::ColumnNotFound(column_name.to_string())))?;

        columns[col_idx].stats = stats;

        let new_info = Arc::new(TableInfo {
            columns,
            ..(*old_info).clone()
        });

        self.tables.insert(name_lower, new_info.clone());
        self.tables_by_id.insert(old_info.id, new_info);

        Ok(())
    }

    /// Add a column to an existing table
    pub fn add_column(&self, table_name: &str, column: ColumnDef) -> Result<()> {
        let name_lower = table_name.to_lowercase();
        let old_info = self
            .tables
            .get(&name_lower)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?
            .clone();

        let mut new_schema_cols = old_info.schema.columns.clone();
        new_schema_cols.push(column.clone());
        let new_schema = Schema::new(new_schema_cols);

        let mut new_columns = old_info.columns.clone();
        new_columns.push(ColumnInfo {
            id: ColumnId(new_columns.len() as u32),
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            default_expr: column.default.as_ref().map(|v| v.to_string()),
            is_primary_key: false,
            stats: ColumnStats::default(),
        });

        let new_info = Arc::new(TableInfo {
            schema: new_schema,
            columns: new_columns,
            ..(*old_info).clone()
        });

        self.tables.insert(name_lower, new_info.clone());
        self.tables_by_id.insert(old_info.id, new_info);
        Ok(())
    }

    /// Drop a column from an existing table
    pub fn drop_column(&self, table_name: &str, column_name: &str) -> Result<()> {
        let name_lower = table_name.to_lowercase();
        let old_info = self
            .tables
            .get(&name_lower)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?
            .clone();

        let col_idx = old_info.schema.column_by_name(column_name)
            .map(|(i, _)| i)
            .ok_or_else(|| Error::Sql(SqlError::ColumnNotFound(column_name.to_string())))?;

        let mut new_schema_cols = old_info.schema.columns.clone();
        new_schema_cols.remove(col_idx);
        let new_schema = Schema::new(new_schema_cols);

        let mut new_columns = old_info.columns.clone();
        new_columns.remove(col_idx);

        let new_info = Arc::new(TableInfo {
            schema: new_schema,
            columns: new_columns,
            ..(*old_info).clone()
        });

        self.tables.insert(name_lower, new_info.clone());
        self.tables_by_id.insert(old_info.id, new_info);
        Ok(())
    }

    /// Rename a column in an existing table
    pub fn rename_column(&self, table_name: &str, old_name: &str, new_name: &str) -> Result<()> {
        let name_lower = table_name.to_lowercase();
        let old_info = self
            .tables
            .get(&name_lower)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?
            .clone();

        let col_idx = old_info.schema.column_by_name(old_name)
            .map(|(i, _)| i)
            .ok_or_else(|| Error::Sql(SqlError::ColumnNotFound(old_name.to_string())))?;

        let mut new_schema_cols = old_info.schema.columns.clone();
        new_schema_cols[col_idx].name = new_name.to_string();
        let new_schema = Schema::new(new_schema_cols);

        let mut new_columns = old_info.columns.clone();
        new_columns[col_idx].name = new_name.to_string();

        let new_info = Arc::new(TableInfo {
            schema: new_schema,
            columns: new_columns,
            ..(*old_info).clone()
        });

        self.tables.insert(name_lower, new_info.clone());
        self.tables_by_id.insert(old_info.id, new_info);
        Ok(())
    }

    /// Rename a table
    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()> {
        let old_lower = old_name.to_lowercase();
        let new_lower = new_name.to_lowercase();

        if self.tables.contains_key(&new_lower) {
            return Err(Error::AlreadyExists("Table".to_string(), new_name.to_string()));
        }

        let (_, old_info) = self
            .tables
            .remove(&old_lower)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(old_name.to_string())))?;

        let new_info = Arc::new(TableInfo {
            name: new_name.to_string(),
            ..(*old_info).clone()
        });

        self.tables.insert(new_lower, new_info.clone());
        self.tables_by_id.insert(old_info.id, new_info);
        Ok(())
    }

    /// Resolve a column reference to its table and index
    pub fn resolve_column(&self, table_name: &str, column_name: &str) -> Result<(Arc<TableInfo>, usize)> {
        let table = self
            .get_table(table_name)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?;

        let col_idx = table
            .schema
            .column_by_name(column_name)
            .map(|(i, _)| i)
            .ok_or_else(|| Error::Sql(SqlError::ColumnNotFound(column_name.to_string())))?;

        Ok((table, col_idx))
    }
}

/// A catalog provider trait for different catalog implementations
pub trait CatalogProvider: Send + Sync {
    /// Get a table by name
    fn get_table(&self, name: &str) -> Option<Arc<TableInfo>>;

    /// List all table names
    fn list_tables(&self) -> Vec<String>;

    /// Get an index by name
    fn get_index(&self, name: &str) -> Option<Arc<IndexInfo>>;

    /// Get all indexes for a table
    fn get_table_indexes(&self, table_id: TableId) -> Vec<Arc<IndexInfo>>;
}

impl CatalogProvider for Catalog {
    fn get_table(&self, name: &str) -> Option<Arc<TableInfo>> {
        Catalog::get_table(self, name)
    }

    fn list_tables(&self) -> Vec<String> {
        Catalog::list_tables(self)
    }

    fn get_index(&self, name: &str) -> Option<Arc<IndexInfo>> {
        Catalog::get_index(self, name)
    }

    fn get_table_indexes(&self, table_id: TableId) -> Vec<Arc<IndexInfo>> {
        Catalog::get_table_indexes(self, table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_catalog() -> Catalog {
        let catalog = Catalog::new();

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("email", DataType::String),
            ColumnDef::new("age", DataType::Int32),
        ]);

        catalog.register_table_with_pk("users", schema, vec![0]).unwrap();
        catalog
    }

    #[test]
    fn test_register_table() {
        let catalog = Catalog::new();

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("value", DataType::String),
        ]);

        let table_id = catalog.register_table("test_table", schema).unwrap();
        assert_eq!(table_id, TableId(1));

        // Table should exist
        assert!(catalog.table_exists("test_table"));
        assert!(catalog.table_exists("TEST_TABLE")); // Case insensitive

        // Duplicate should fail
        let schema2 = Schema::new(vec![ColumnDef::new("x", DataType::Int32)]);
        assert!(catalog.register_table("test_table", schema2).is_err());
    }

    #[test]
    fn test_get_table() {
        let catalog = test_catalog();

        let table = catalog.get_table("users").unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.schema.len(), 4);
        assert_eq!(table.primary_key, vec![0]);

        // Case insensitive
        let table2 = catalog.get_table("USERS").unwrap();
        assert_eq!(table.id, table2.id);

        // Non-existent
        assert!(catalog.get_table("nonexistent").is_none());
    }

    #[test]
    fn test_drop_table() {
        let catalog = test_catalog();

        assert!(catalog.table_exists("users"));
        catalog.drop_table("users").unwrap();
        assert!(!catalog.table_exists("users"));

        // Drop non-existent should fail
        assert!(catalog.drop_table("users").is_err());
    }

    #[test]
    fn test_list_tables() {
        let catalog = Catalog::new();

        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        catalog.register_table("table_a", schema.clone()).unwrap();
        catalog.register_table("table_b", schema.clone()).unwrap();
        catalog.register_table("table_c", schema).unwrap();

        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 3);
    }

    #[test]
    fn test_register_index() {
        let catalog = test_catalog();

        let index_id = catalog
            .register_index(
                "users_email_idx",
                "users",
                vec![IndexColumn {
                    column_index: 2,
                    ascending: true,
                    nulls_first: false,
                }],
                false,
                IndexType::BTree,
            )
            .unwrap();

        assert!(index_id.0 > 0);

        let index = catalog.get_index("users_email_idx").unwrap();
        assert_eq!(index.name, "users_email_idx");
        assert!(!index.is_unique);
        assert!(!index.is_primary);
    }

    #[test]
    fn test_primary_key_index() {
        let catalog = test_catalog();

        // Primary key index should be created automatically
        let table = catalog.get_table("users").unwrap();
        let indexes = catalog.get_table_indexes(table.id);

        assert_eq!(indexes.len(), 1);
        assert!(indexes[0].is_primary);
        assert!(indexes[0].is_unique);
        assert_eq!(indexes[0].name, "users_pkey");
    }

    #[test]
    fn test_drop_index() {
        let catalog = test_catalog();

        catalog
            .register_index(
                "test_idx",
                "users",
                vec![IndexColumn {
                    column_index: 1,
                    ascending: true,
                    nulls_first: false,
                }],
                false,
                IndexType::BTree,
            )
            .unwrap();

        assert!(catalog.get_index("test_idx").is_some());
        catalog.drop_index("test_idx").unwrap();
        assert!(catalog.get_index("test_idx").is_none());
    }

    #[test]
    fn test_find_covering_indexes() {
        let catalog = test_catalog();

        let table = catalog.get_table("users").unwrap();

        // Create an index on (name, email)
        catalog
            .register_index(
                "users_name_email_idx",
                "users",
                vec![
                    IndexColumn {
                        column_index: 1,
                        ascending: true,
                        nulls_first: false,
                    },
                    IndexColumn {
                        column_index: 2,
                        ascending: true,
                        nulls_first: false,
                    },
                ],
                false,
                IndexType::BTree,
            )
            .unwrap();

        // Find index covering column 1 (name)
        let indexes = catalog.find_covering_indexes(table.id, &[1]);
        assert_eq!(indexes.len(), 1);

        // Find index covering columns 1 and 2 (name, email)
        let indexes = catalog.find_covering_indexes(table.id, &[1, 2]);
        assert_eq!(indexes.len(), 1);

        // Find index covering column 3 (age) - none
        let indexes = catalog.find_covering_indexes(table.id, &[3]);
        assert_eq!(indexes.len(), 0);
    }

    #[test]
    fn test_resolve_column() {
        let catalog = test_catalog();

        let (table, idx) = catalog.resolve_column("users", "name").unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(idx, 1);

        // Case insensitive
        let (_, idx) = catalog.resolve_column("USERS", "NAME").unwrap();
        assert_eq!(idx, 1);

        // Non-existent table
        assert!(catalog.resolve_column("nonexistent", "col").is_err());

        // Non-existent column
        assert!(catalog.resolve_column("users", "nonexistent").is_err());
    }

    #[test]
    fn test_update_stats() {
        let catalog = test_catalog();

        let stats = TableStats {
            row_count: 1000,
            size_bytes: 65536,
            last_updated: 1234567890,
        };

        catalog.update_table_stats("users", stats.clone()).unwrap();

        let table = catalog.get_table("users").unwrap();
        assert_eq!(table.stats.row_count, 1000);
        assert_eq!(table.stats.size_bytes, 65536);

        let col_stats = ColumnStats {
            distinct_count: 500,
            null_count: 10,
            min_value: Some("1".to_string()),
            max_value: Some("1000".to_string()),
            avg_length: None,
        };

        catalog
            .update_column_stats("users", "id", col_stats)
            .unwrap();

        let table = catalog.get_table("users").unwrap();
        assert_eq!(table.columns[0].stats.distinct_count, 500);
    }
}
