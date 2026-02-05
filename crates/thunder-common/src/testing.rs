//! Testing utilities and fixtures

use crate::types::*;
use std::path::PathBuf;
use tempfile::TempDir;

/// Test context with automatic cleanup
pub struct TestContext {
    pub temp_dir: TempDir,
}

impl TestContext {
    pub fn new() -> Self {
        Self {
            temp_dir: TempDir::new().expect("Failed to create temp directory"),
        }
    }

    pub fn data_dir(&self) -> PathBuf {
        self.temp_dir.path().join("data")
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.temp_dir.path().join("wal")
    }

    pub fn path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Test fixtures for common scenarios
pub mod fixtures {
    use super::*;

    /// Generate sample rows for testing
    pub fn sample_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|i| {
                Row::new(vec![
                    Value::Int64(i as i64),
                    Value::String(format!("name_{}", i).into()),
                    Value::Float64(i as f64 * 1.5),
                    Value::Boolean(i % 2 == 0),
                ])
            })
            .collect()
    }

    /// Generate a sample schema
    pub fn sample_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("value", DataType::Float64),
            ColumnDef::new("active", DataType::Boolean),
        ])
    }

    /// Generate a sample users table schema
    pub fn users_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("username", DataType::Varchar(50)).not_null(),
            ColumnDef::new("email", DataType::Varchar(100)),
            ColumnDef::new("created_at", DataType::Timestamp),
            ColumnDef::new("active", DataType::Boolean)
                .default(Value::Boolean(true)),
        ])
    }

    /// Generate sample user rows
    pub fn sample_users(count: usize) -> Vec<Row> {
        (0..count)
            .map(|i| {
                Row::new(vec![
                    Value::Int64(i as i64 + 1),
                    Value::String(format!("user_{}", i).into()),
                    Value::String(format!("user_{}@example.com", i).into()),
                    Value::Timestamp(1704067200000000 + i as i64 * 86400000000), // 2024-01-01 + i days
                    Value::Boolean(true),
                ])
            })
            .collect()
    }

    /// Generate random bytes for testing
    pub fn random_bytes(len: usize) -> Vec<u8> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut result = Vec::with_capacity(len);
        let mut hasher = DefaultHasher::new();

        for i in 0..len {
            i.hash(&mut hasher);
            result.push(hasher.finish() as u8);
        }

        result
    }

    /// Generate sample vectors for testing
    pub fn sample_vectors(count: usize, dim: usize) -> Vec<Vec<f32>> {
        (0..count)
            .map(|i| {
                (0..dim)
                    .map(|j| ((i * dim + j) as f32).sin())
                    .collect()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_context() {
        let ctx = TestContext::new();
        assert!(ctx.path().exists());
        assert!(ctx.data_dir().to_str().unwrap().contains("data"));
        assert!(ctx.wal_dir().to_str().unwrap().contains("wal"));
    }

    #[test]
    fn test_sample_rows() {
        let rows = fixtures::sample_rows(10);
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0].get_i64(0), Some(0));
        assert_eq!(rows[5].get_str(1), Some("name_5"));
    }

    #[test]
    fn test_sample_schema() {
        let schema = fixtures::sample_schema();
        assert_eq!(schema.len(), 4);
        assert_eq!(schema.column(0).unwrap().name, "id");
    }

    #[test]
    fn test_sample_vectors() {
        let vectors = fixtures::sample_vectors(10, 128);
        assert_eq!(vectors.len(), 10);
        assert_eq!(vectors[0].len(), 128);
    }
}
