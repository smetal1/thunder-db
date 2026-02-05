//! Core types for ThunderDB

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

// ============================================================================
// Identifiers
// ============================================================================

/// Unique identifier for a database
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DatabaseId(pub u32);

/// Unique identifier for a table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

/// Unique identifier for a column
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnId(pub u32);

/// Unique identifier for an index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexId(pub u64);

/// Unique identifier for a row
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowId(pub u64);

/// Unique identifier for a page
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageId(pub u64);

/// Unique identifier for a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxnId(pub u64);

/// Unique identifier for a region (distributed shard)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RegionId(pub u64);

/// Unique identifier for a node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// Log Sequence Number for WAL
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl TxnId {
    pub const INVALID: TxnId = TxnId(0);

    pub fn new(timestamp_us: u64, node_id: u8, sequence: u8) -> Self {
        // Format: 48 bits timestamp | 8 bits node | 8 bits sequence
        let id = (timestamp_us << 16) | ((node_id as u64) << 8) | (sequence as u64);
        TxnId(id)
    }

    pub fn timestamp_us(&self) -> u64 {
        self.0 >> 16
    }

    pub fn node_id(&self) -> u8 {
        ((self.0 >> 8) & 0xFF) as u8
    }

    pub fn sequence(&self) -> u8 {
        (self.0 & 0xFF) as u8
    }

    pub fn is_valid(&self) -> bool {
        self.0 != 0
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn:{}", self.0)
    }
}

impl Lsn {
    pub const INVALID: Lsn = Lsn(0);

    pub fn next(&self) -> Lsn {
        Lsn(self.0 + 1)
    }
}

// ============================================================================
// Data Types
// ============================================================================

/// SQL data types supported by ThunderDB
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Fixed-precision decimal
    Decimal { precision: u8, scale: u8 },
    /// Variable-length string
    String,
    /// Fixed-length string
    Char(u32),
    /// Variable-length string with max length
    Varchar(u32),
    /// Binary data
    Binary,
    /// Fixed-length binary
    FixedBinary(u32),
    /// Date (days since epoch)
    Date,
    /// Time (microseconds since midnight)
    Time,
    /// Timestamp (microseconds since epoch)
    Timestamp,
    /// Timestamp with timezone
    TimestampTz,
    /// Interval
    Interval,
    /// UUID
    Uuid,
    /// JSON
    Json,
    /// JSONB (binary JSON)
    Jsonb,
    /// Array of another type
    Array(Box<DataType>),
    /// Vector (for embeddings)
    Vector(u32), // dimension
    /// Null type
    Null,
}

impl DataType {
    /// Returns true if this type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal { .. }
        )
    }

    /// Returns true if this type is a string type
    pub fn is_string(&self) -> bool {
        matches!(
            self,
            DataType::String | DataType::Char(_) | DataType::Varchar(_)
        )
    }

    /// Returns true if this type is a temporal type
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            DataType::Date
                | DataType::Time
                | DataType::Timestamp
                | DataType::TimestampTz
                | DataType::Interval
        )
    }

    /// Returns the byte size of this type, if fixed
    pub fn byte_size(&self) -> Option<usize> {
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
            DataType::Char(n) => Some(*n as usize),
            DataType::FixedBinary(n) => Some(*n as usize),
            DataType::Vector(dim) => Some(*dim as usize * 4), // f32 per dimension
            _ => None, // Variable-length types
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int8 => write!(f, "TINYINT"),
            DataType::Int16 => write!(f, "SMALLINT"),
            DataType::Int32 => write!(f, "INTEGER"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::Float32 => write!(f, "REAL"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::Decimal { precision, scale } => write!(f, "DECIMAL({},{})", precision, scale),
            DataType::String => write!(f, "TEXT"),
            DataType::Char(n) => write!(f, "CHAR({})", n),
            DataType::Varchar(n) => write!(f, "VARCHAR({})", n),
            DataType::Binary => write!(f, "BYTEA"),
            DataType::FixedBinary(n) => write!(f, "BINARY({})", n),
            DataType::Date => write!(f, "DATE"),
            DataType::Time => write!(f, "TIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::TimestampTz => write!(f, "TIMESTAMPTZ"),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Json => write!(f, "JSON"),
            DataType::Jsonb => write!(f, "JSONB"),
            DataType::Array(inner) => write!(f, "{}[]", inner),
            DataType::Vector(dim) => write!(f, "VECTOR({})", dim),
            DataType::Null => write!(f, "NULL"),
        }
    }
}

// ============================================================================
// Values
// ============================================================================

/// A scalar value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Decimal(i128, u8), // value, scale
    String(Arc<str>),
    Binary(Arc<[u8]>),
    Date(i32),              // days since epoch
    Time(i64),              // microseconds since midnight
    Timestamp(i64),         // microseconds since epoch
    TimestampTz(i64, i32),  // microseconds, offset seconds
    Uuid([u8; 16]),
    Json(Arc<str>),
    Vector(Arc<[f32]>),
    Array(Arc<[Value]>),
}

impl Value {
    /// Returns the data type of this value
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Boolean(_) => DataType::Boolean,
            Value::Int8(_) => DataType::Int8,
            Value::Int16(_) => DataType::Int16,
            Value::Int32(_) => DataType::Int32,
            Value::Int64(_) => DataType::Int64,
            Value::Float32(_) => DataType::Float32,
            Value::Float64(_) => DataType::Float64,
            Value::Decimal(_, scale) => DataType::Decimal {
                precision: 38,
                scale: *scale,
            },
            Value::String(_) => DataType::String,
            Value::Binary(_) => DataType::Binary,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::TimestampTz(_, _) => DataType::TimestampTz,
            Value::Uuid(_) => DataType::Uuid,
            Value::Json(_) => DataType::Json,
            Value::Vector(v) => DataType::Vector(v.len() as u32),
            Value::Array(_) => DataType::Array(Box::new(DataType::Null)), // TODO: infer inner type
        }
    }

    /// Returns true if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to get as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int8(v) => Some(*v as i64),
            Value::Int16(v) => Some(*v as i64),
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float32(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            Value::Int8(v) => Some(*v as f64),
            Value::Int16(v) => Some(*v as f64),
            Value::Int32(v) => Some(*v as f64),
            Value::Int64(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Try to get as string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            Value::Json(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Int8(v) => write!(f, "{}", v),
            Value::Int16(v) => write!(f, "{}", v),
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float32(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Decimal(v, scale) => {
                let divisor = 10i128.pow(*scale as u32);
                write!(f, "{}.{:0>width$}", v / divisor, (v % divisor).abs(), width = *scale as usize)
            }
            Value::String(v) => write!(f, "'{}'", v),
            Value::Binary(v) => write!(f, "\\x{}", hex::encode(v.as_ref())),
            Value::Date(v) => write!(f, "{}", v), // TODO: format as date
            Value::Time(v) => write!(f, "{}", v), // TODO: format as time
            Value::Timestamp(v) => write!(f, "{}", v), // TODO: format as timestamp
            Value::TimestampTz(v, tz) => write!(f, "{}+{}", v, tz),
            Value::Uuid(v) => write!(f, "{}", uuid::Uuid::from_bytes(*v)),
            Value::Json(v) => write!(f, "{}", v),
            Value::Vector(v) => write!(f, "[{}]", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")),
            Value::Array(v) => write!(f, "[{}]", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")),
        }
    }
}

// ============================================================================
// Row
// ============================================================================

/// A row of values
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn empty() -> Self {
        Self { values: vec![] }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_i64(&self, index: usize) -> Option<i64> {
        self.values.get(index).and_then(|v| v.as_i64())
    }

    pub fn get_f64(&self, index: usize) -> Option<f64> {
        self.values.get(index).and_then(|v| v.as_f64())
    }

    pub fn get_str(&self, index: usize) -> Option<&str> {
        self.values.get(index).and_then(|v| v.as_str())
    }

    pub fn get_bool(&self, index: usize) -> Option<bool> {
        self.values.get(index).and_then(|v| v.as_bool())
    }
}

// ============================================================================
// Schema
// ============================================================================

/// Definition of a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            default: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }
}

/// Schema of a table or result set
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    pub fn empty() -> Self {
        Self { columns: vec![] }
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn column(&self, index: usize) -> Option<&ColumnDef> {
        self.columns.get(index)
    }

    pub fn column_by_name(&self, name: &str) -> Option<(usize, &ColumnDef)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name.eq_ignore_ascii_case(name))
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

// ============================================================================
// Isolation Level
// ============================================================================

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum IsolationLevel {
    /// Read uncommitted (not recommended)
    ReadUncommitted,
    /// Read committed
    ReadCommitted,
    /// Repeatable read (snapshot isolation)
    RepeatableRead,
    /// Serializable (default, strongest)
    #[default]
    Serializable,
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IsolationLevel::ReadUncommitted => write!(f, "READ UNCOMMITTED"),
            IsolationLevel::ReadCommitted => write!(f, "READ COMMITTED"),
            IsolationLevel::RepeatableRead => write!(f, "REPEATABLE READ"),
            IsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_id() {
        let txn = TxnId::new(1234567890, 5, 10);
        assert_eq!(txn.timestamp_us(), 1234567890);
        assert_eq!(txn.node_id(), 5);
        assert_eq!(txn.sequence(), 10);
    }

    #[test]
    fn test_data_type_display() {
        assert_eq!(DataType::Int64.to_string(), "BIGINT");
        assert_eq!(DataType::Varchar(255).to_string(), "VARCHAR(255)");
        assert_eq!(DataType::Vector(128).to_string(), "VECTOR(128)");
    }

    #[test]
    fn test_value_types() {
        let v = Value::Int64(42);
        assert_eq!(v.as_i64(), Some(42));
        assert_eq!(v.as_f64(), Some(42.0));
        assert!(!v.is_null());

        let v = Value::Null;
        assert!(v.is_null());
    }

    #[test]
    fn test_row() {
        let row = Row::new(vec![
            Value::Int64(1),
            Value::String("hello".into()),
            Value::Boolean(true),
        ]);

        assert_eq!(row.len(), 3);
        assert_eq!(row.get_i64(0), Some(1));
        assert_eq!(row.get_str(1), Some("hello"));
        assert_eq!(row.get_bool(2), Some(true));
    }

    #[test]
    fn test_schema() {
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("name", DataType::String),
        ]);

        assert_eq!(schema.len(), 2);
        assert_eq!(schema.column_by_name("ID").map(|(i, _)| i), Some(0));
        assert_eq!(schema.column_names(), vec!["id", "name"]);
    }
}
