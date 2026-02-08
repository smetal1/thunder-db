//! CDC Event Processing
//!
//! Event transformation, serialization, and utilities for CDC events.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{CdcEvent, CdcOperation, CdcPosition};
use thunder_common::prelude::*;

// ============================================================================
// Event Envelope
// ============================================================================

/// Debezium-compatible event envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Schema information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaInfo>,
    /// Payload
    pub payload: EventPayload,
}

/// Schema information for events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Schema type
    #[serde(rename = "type")]
    pub schema_type: String,
    /// Field definitions
    #[serde(default)]
    pub fields: Vec<FieldSchema>,
    /// Optional flag
    #[serde(default)]
    pub optional: bool,
    /// Schema name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Schema version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
}

/// Field schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    /// Field name
    pub field: String,
    /// Field type
    #[serde(rename = "type")]
    pub field_type: String,
    /// Optional flag
    #[serde(default)]
    pub optional: bool,
}

/// Event payload (Debezium-compatible)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPayload {
    /// Data before the change
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<serde_json::Value>,
    /// Data after the change
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<serde_json::Value>,
    /// Source metadata
    pub source: SourceMetadata,
    /// Operation type
    pub op: String,
    /// Transaction metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TransactionMetadata>,
    /// Event timestamp (milliseconds since epoch)
    pub ts_ms: i64,
}

/// Source metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMetadata {
    /// Connector version
    pub version: String,
    /// Connector name
    pub connector: String,
    /// Database name
    pub db: String,
    /// Schema name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Table name
    pub table: String,
    /// Snapshot flag
    #[serde(default)]
    pub snapshot: bool,
    /// LSN (PostgreSQL)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
    /// Binlog file (MySQL)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// Binlog position (MySQL)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pos: Option<u64>,
    /// Resume token (MongoDB)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_token: Option<String>,
    /// Server ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_id: Option<String>,
    /// Timestamp in source database
    pub ts_sec: i64,
}

/// Transaction metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetadata {
    /// Transaction ID
    pub id: String,
    /// Total order within transaction
    #[serde(default)]
    pub total_order: u64,
    /// Data collection order
    #[serde(default)]
    pub data_collection_order: u64,
}

// ============================================================================
// Event Conversion
// ============================================================================

impl From<&CdcEvent> for EventEnvelope {
    fn from(event: &CdcEvent) -> Self {
        let op = match event.operation {
            CdcOperation::Insert => "c",
            CdcOperation::Update => "u",
            CdcOperation::Delete => "d",
            CdcOperation::Truncate => "t",
            CdcOperation::SchemaChange => "s",
            CdcOperation::Snapshot => "r",
        };

        let (lsn, file, pos, resume_token) = match &event.position {
            CdcPosition::PostgresLsn(lsn) => (Some(*lsn), None, None, None),
            CdcPosition::MysqlBinlog { file, position } => {
                (None, Some(file.clone()), Some(*position), None)
            }
            CdcPosition::MongoResumeToken(token) => {
                (None, None, None, Some(base64::encode(token)))
            }
            CdcPosition::RedisStreamId(id) => {
                (None, None, None, Some(id.clone()))
            }
            CdcPosition::Beginning | CdcPosition::End => (None, None, None, None),
        };

        let source = SourceMetadata {
            version: env!("CARGO_PKG_VERSION").to_string(),
            connector: event.source.clone(),
            db: event.database.clone(),
            schema: event.schema.clone(),
            table: event.table.clone(),
            snapshot: matches!(event.operation, CdcOperation::Snapshot),
            lsn,
            file,
            pos,
            resume_token,
            server_id: None,
            ts_sec: event.timestamp.timestamp(),
        };

        let before = event.before.as_ref().map(row_to_json);
        let after = event.after.as_ref().map(row_to_json);

        let transaction = event.txn_id.as_ref().map(|id| TransactionMetadata {
            id: id.clone(),
            total_order: 0,
            data_collection_order: 0,
        });

        EventEnvelope {
            schema: None, // Schema can be added if needed
            payload: EventPayload {
                before,
                after,
                source,
                op: op.to_string(),
                transaction,
                ts_ms: event.timestamp.timestamp_millis(),
            },
        }
    }
}

/// Convert Row to JSON value
fn row_to_json(row: &Row) -> serde_json::Value {
    let values: Vec<serde_json::Value> = row
        .values
        .iter()
        .map(value_to_json)
        .collect();
    serde_json::Value::Array(values)
}

/// Convert Value to JSON value
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Int8(v) => serde_json::Value::Number((*v).into()),
        Value::Int16(v) => serde_json::Value::Number((*v).into()),
        Value::Int32(v) => serde_json::Value::Number((*v).into()),
        Value::Int64(v) => serde_json::Value::Number((*v).into()),
        Value::Float32(v) => serde_json::json!(*v),
        Value::Float64(v) => serde_json::json!(*v),
        Value::Decimal(val, scale) => {
            // Format decimal as string with proper scale
            serde_json::Value::String(format!("{:.scale$}", *val as f64 / 10_f64.powi(*scale as i32), scale = *scale as usize))
        }
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Binary(b) => serde_json::Value::String(base64::encode(b)),
        Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::TimestampTz(_, _) => {
            serde_json::Value::String(format!("{}", value))
        }
        Value::Uuid(u) => serde_json::Value::String(hex::encode(u)),
        Value::Json(j) => {
            // Parse JSON string to serde_json::Value
            serde_json::from_str(j).unwrap_or(serde_json::Value::String(j.to_string()))
        }
        Value::Vector(v) => {
            let values: Vec<_> = v.iter().map(|f| serde_json::json!(*f)).collect();
            serde_json::Value::Array(values)
        }
        Value::Array(arr) => {
            let values: Vec<_> = arr.iter().map(value_to_json).collect();
            serde_json::Value::Array(values)
        }
    }
}

// ============================================================================
// Event Serialization
// ============================================================================

/// Event serialization format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// JSON format
    Json,
    /// JSON with schema (Debezium-style)
    JsonWithSchema,
    /// Avro format
    Avro,
    /// Protobuf format
    Protobuf,
}

/// Serialize event to bytes
pub fn serialize_event(event: &CdcEvent, format: SerializationFormat) -> Result<Vec<u8>> {
    match format {
        SerializationFormat::Json => {
            let envelope = EventEnvelope::from(event);
            serde_json::to_vec(&envelope.payload).map_err(|e| {
                Error::Serialization(format!("JSON serialization failed: {}", e))
            })
        }
        SerializationFormat::JsonWithSchema => {
            let envelope = EventEnvelope::from(event);
            serde_json::to_vec(&envelope).map_err(|e| {
                Error::Serialization(format!("JSON serialization failed: {}", e))
            })
        }
        SerializationFormat::Avro => {
            serialize_avro(event)
        }
        SerializationFormat::Protobuf => {
            serialize_protobuf(event)
        }
    }
}

/// Deserialize event from bytes
pub fn deserialize_event(data: &[u8], format: SerializationFormat) -> Result<EventPayload> {
    match format {
        SerializationFormat::Json | SerializationFormat::JsonWithSchema => {
            // Try envelope first
            if let Ok(envelope) = serde_json::from_slice::<EventEnvelope>(data) {
                return Ok(envelope.payload);
            }
            // Then try just payload
            serde_json::from_slice(data).map_err(|e| {
                Error::Serialization(format!("JSON deserialization failed: {}", e))
            })
        }
        SerializationFormat::Avro => {
            deserialize_avro(data)
        }
        SerializationFormat::Protobuf => {
            deserialize_protobuf(data)
        }
    }
}

// ============================================================================
// Avro Serialization
// ============================================================================

use apache_avro::{from_avro_datum, to_avro_datum, types::Record as AvroRecord, Schema as AvroSchema};

/// CDC event Avro schema (Debezium-compatible)
fn get_cdc_avro_schema() -> AvroSchema {
    let schema_str = r#"
    {
        "type": "record",
        "name": "CdcEvent",
        "namespace": "io.thunderdb.cdc",
        "fields": [
            {"name": "op", "type": "string"},
            {"name": "ts_ms", "type": "long"},
            {"name": "before", "type": ["null", "string"], "default": null},
            {"name": "after", "type": ["null", "string"], "default": null},
            {"name": "source_connector", "type": "string"},
            {"name": "source_db", "type": "string"},
            {"name": "source_schema", "type": ["null", "string"], "default": null},
            {"name": "source_table", "type": "string"},
            {"name": "source_lsn", "type": ["null", "long"], "default": null},
            {"name": "source_file", "type": ["null", "string"], "default": null},
            {"name": "source_pos", "type": ["null", "long"], "default": null},
            {"name": "txn_id", "type": ["null", "string"], "default": null}
        ]
    }
    "#;

    AvroSchema::parse_str(schema_str).expect("Invalid Avro schema")
}

/// Serialize CDC event to Avro bytes
fn serialize_avro(event: &CdcEvent) -> Result<Vec<u8>> {
    let schema = get_cdc_avro_schema();

    let mut record = AvroRecord::new(&schema).ok_or_else(|| {
        Error::Serialization("Failed to create Avro record".to_string())
    })?;

    let op = match event.operation {
        CdcOperation::Insert => "c",
        CdcOperation::Update => "u",
        CdcOperation::Delete => "d",
        CdcOperation::Truncate => "t",
        CdcOperation::SchemaChange => "s",
        CdcOperation::Snapshot => "r",
    };

    record.put("op", op);
    record.put("ts_ms", event.timestamp.timestamp_millis());

    // Serialize before/after as JSON strings
    let before_json = event.before.as_ref().map(|r| {
        apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::String(
            serde_json::to_string(&row_to_json(r)).unwrap_or_default()
        )))
    }).unwrap_or(apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));

    let after_json = event.after.as_ref().map(|r| {
        apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::String(
            serde_json::to_string(&row_to_json(r)).unwrap_or_default()
        )))
    }).unwrap_or(apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));

    record.put("before", before_json);
    record.put("after", after_json);
    record.put("source_connector", event.source.clone());
    record.put("source_db", event.database.clone());

    let schema_val = event.schema.as_ref().map(|s| {
        apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::String(s.clone())))
    }).unwrap_or(apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));
    record.put("source_schema", schema_val);

    record.put("source_table", event.table.clone());

    // Position-specific fields
    let (lsn, file, pos) = match &event.position {
        CdcPosition::PostgresLsn(l) => (Some(*l as i64), None, None),
        CdcPosition::MysqlBinlog { file: f, position: p } => (None, Some(f.clone()), Some(*p as i64)),
        _ => (None, None, None),
    };

    let lsn_val = lsn.map(|l| {
        apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Long(l)))
    }).unwrap_or(apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));
    record.put("source_lsn", lsn_val);

    let file_val = file.map(|f| {
        apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::String(f)))
    }).unwrap_or(apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));
    record.put("source_file", file_val);

    let pos_val = pos.map(|p| {
        apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Long(p)))
    }).unwrap_or(apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));
    record.put("source_pos", pos_val);

    let txn_val = event.txn_id.as_ref().map(|t| {
        apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::String(t.clone())))
    }).unwrap_or(apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));
    record.put("txn_id", txn_val);

    to_avro_datum(&schema, record).map_err(|e| {
        Error::Serialization(format!("Avro serialization failed: {}", e))
    })
}

/// Deserialize Avro bytes to EventPayload
fn deserialize_avro(data: &[u8]) -> Result<EventPayload> {
    let schema = get_cdc_avro_schema();

    let value = from_avro_datum(&schema, &mut &data[..], None).map_err(|e| {
        Error::Serialization(format!("Avro deserialization failed: {}", e))
    })?;

    // Extract fields from Avro record
    let record = match value {
        apache_avro::types::Value::Record(fields) => fields,
        _ => return Err(Error::Serialization("Expected Avro record".to_string())),
    };

    let fields: HashMap<String, apache_avro::types::Value> = record.into_iter().collect();

    let op = extract_avro_string(&fields, "op").unwrap_or_default();
    let ts_ms = extract_avro_long(&fields, "ts_ms").unwrap_or(0);
    let before_str = extract_avro_optional_string(&fields, "before");
    let after_str = extract_avro_optional_string(&fields, "after");
    let connector = extract_avro_string(&fields, "source_connector").unwrap_or_default();
    let db = extract_avro_string(&fields, "source_db").unwrap_or_default();
    let schema_name = extract_avro_optional_string(&fields, "source_schema");
    let table = extract_avro_string(&fields, "source_table").unwrap_or_default();
    let lsn = extract_avro_optional_long(&fields, "source_lsn");
    let file = extract_avro_optional_string(&fields, "source_file");
    let pos = extract_avro_optional_long(&fields, "source_pos");
    let txn_id = extract_avro_optional_string(&fields, "txn_id");

    let before = before_str.and_then(|s| serde_json::from_str(&s).ok());
    let after = after_str.and_then(|s| serde_json::from_str(&s).ok());

    let source = SourceMetadata {
        version: env!("CARGO_PKG_VERSION").to_string(),
        connector,
        db,
        schema: schema_name,
        table,
        snapshot: false,
        lsn: lsn.map(|l| l as u64),
        file,
        pos: pos.map(|p| p as u64),
        resume_token: None,
        server_id: None,
        ts_sec: ts_ms / 1000,
    };

    let transaction = txn_id.map(|id| TransactionMetadata {
        id,
        total_order: 0,
        data_collection_order: 0,
    });

    Ok(EventPayload {
        before,
        after,
        source,
        op,
        transaction,
        ts_ms,
    })
}

fn extract_avro_string(fields: &HashMap<String, apache_avro::types::Value>, key: &str) -> Option<String> {
    match fields.get(key)? {
        apache_avro::types::Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn extract_avro_long(fields: &HashMap<String, apache_avro::types::Value>, key: &str) -> Option<i64> {
    match fields.get(key)? {
        apache_avro::types::Value::Long(l) => Some(*l),
        _ => None,
    }
}

fn extract_avro_optional_string(fields: &HashMap<String, apache_avro::types::Value>, key: &str) -> Option<String> {
    match fields.get(key)? {
        apache_avro::types::Value::Union(_, inner) => {
            match inner.as_ref() {
                apache_avro::types::Value::String(s) => Some(s.clone()),
                _ => None,
            }
        }
        apache_avro::types::Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn extract_avro_optional_long(fields: &HashMap<String, apache_avro::types::Value>, key: &str) -> Option<i64> {
    match fields.get(key)? {
        apache_avro::types::Value::Union(_, inner) => {
            match inner.as_ref() {
                apache_avro::types::Value::Long(l) => Some(*l),
                _ => None,
            }
        }
        apache_avro::types::Value::Long(l) => Some(*l),
        _ => None,
    }
}

// ============================================================================
// Protobuf Serialization
// ============================================================================

use prost::Message;

/// CDC event Protobuf message (manually defined without .proto file)
#[derive(Clone, PartialEq, Message)]
pub struct CdcEventProto {
    #[prost(string, tag = "1")]
    pub op: String,
    #[prost(int64, tag = "2")]
    pub ts_ms: i64,
    #[prost(string, optional, tag = "3")]
    pub before: Option<String>,
    #[prost(string, optional, tag = "4")]
    pub after: Option<String>,
    #[prost(message, optional, tag = "5")]
    pub source: Option<SourceMetadataProto>,
    #[prost(string, optional, tag = "6")]
    pub txn_id: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SourceMetadataProto {
    #[prost(string, tag = "1")]
    pub version: String,
    #[prost(string, tag = "2")]
    pub connector: String,
    #[prost(string, tag = "3")]
    pub db: String,
    #[prost(string, optional, tag = "4")]
    pub schema: Option<String>,
    #[prost(string, tag = "5")]
    pub table: String,
    #[prost(bool, tag = "6")]
    pub snapshot: bool,
    #[prost(uint64, optional, tag = "7")]
    pub lsn: Option<u64>,
    #[prost(string, optional, tag = "8")]
    pub file: Option<String>,
    #[prost(uint64, optional, tag = "9")]
    pub pos: Option<u64>,
    #[prost(int64, tag = "10")]
    pub ts_sec: i64,
}

/// Serialize CDC event to Protobuf bytes
fn serialize_protobuf(event: &CdcEvent) -> Result<Vec<u8>> {
    let op = match event.operation {
        CdcOperation::Insert => "c",
        CdcOperation::Update => "u",
        CdcOperation::Delete => "d",
        CdcOperation::Truncate => "t",
        CdcOperation::SchemaChange => "s",
        CdcOperation::Snapshot => "r",
    };

    let (lsn, file, pos) = match &event.position {
        CdcPosition::PostgresLsn(l) => (Some(*l), None, None),
        CdcPosition::MysqlBinlog { file: f, position: p } => (None, Some(f.clone()), Some(*p)),
        _ => (None, None, None),
    };

    let source = SourceMetadataProto {
        version: env!("CARGO_PKG_VERSION").to_string(),
        connector: event.source.clone(),
        db: event.database.clone(),
        schema: event.schema.clone(),
        table: event.table.clone(),
        snapshot: matches!(event.operation, CdcOperation::Snapshot),
        lsn,
        file,
        pos,
        ts_sec: event.timestamp.timestamp(),
    };

    let before = event.before.as_ref().map(|r| {
        serde_json::to_string(&row_to_json(r)).unwrap_or_default()
    });

    let after = event.after.as_ref().map(|r| {
        serde_json::to_string(&row_to_json(r)).unwrap_or_default()
    });

    let proto = CdcEventProto {
        op: op.to_string(),
        ts_ms: event.timestamp.timestamp_millis(),
        before,
        after,
        source: Some(source),
        txn_id: event.txn_id.clone(),
    };

    let mut buf = Vec::with_capacity(proto.encoded_len());
    proto.encode(&mut buf).map_err(|e| {
        Error::Serialization(format!("Protobuf serialization failed: {}", e))
    })?;

    Ok(buf)
}

/// Deserialize Protobuf bytes to EventPayload
fn deserialize_protobuf(data: &[u8]) -> Result<EventPayload> {
    let proto = CdcEventProto::decode(data).map_err(|e| {
        Error::Serialization(format!("Protobuf deserialization failed: {}", e))
    })?;

    let source_proto = proto.source.unwrap_or_default();

    let source = SourceMetadata {
        version: source_proto.version,
        connector: source_proto.connector,
        db: source_proto.db,
        schema: source_proto.schema,
        table: source_proto.table,
        snapshot: source_proto.snapshot,
        lsn: source_proto.lsn,
        file: source_proto.file,
        pos: source_proto.pos,
        resume_token: None,
        server_id: None,
        ts_sec: source_proto.ts_sec,
    };

    let before = proto.before.and_then(|s| serde_json::from_str(&s).ok());
    let after = proto.after.and_then(|s| serde_json::from_str(&s).ok());

    let transaction = proto.txn_id.map(|id| TransactionMetadata {
        id,
        total_order: 0,
        data_collection_order: 0,
    });

    Ok(EventPayload {
        before,
        after,
        source,
        op: proto.op,
        transaction,
        ts_ms: proto.ts_ms,
    })
}

// ============================================================================
// Event Transformation
// ============================================================================

/// Event transformation function
pub type TransformFn = Box<dyn Fn(CdcEvent) -> Option<CdcEvent> + Send + Sync>;

/// Event transformer
pub struct EventTransformer {
    /// Named transformations
    transforms: HashMap<String, TransformFn>,
}

impl EventTransformer {
    pub fn new() -> Self {
        Self {
            transforms: HashMap::new(),
        }
    }

    /// Register a transformation
    pub fn register(&mut self, name: impl Into<String>, transform: TransformFn) {
        self.transforms.insert(name.into(), transform);
    }

    /// Apply a transformation by name
    pub fn transform(&self, name: &str, event: CdcEvent) -> Option<CdcEvent> {
        self.transforms.get(name).and_then(|f| f(event))
    }

    /// Apply multiple transformations in sequence
    pub fn transform_chain(&self, names: &[&str], mut event: CdcEvent) -> Option<CdcEvent> {
        for name in names {
            event = self.transform(name, event)?;
        }
        Some(event)
    }
}

impl Default for EventTransformer {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a filter transformation
pub fn filter_transform<F>(predicate: F) -> TransformFn
where
    F: Fn(&CdcEvent) -> bool + Send + Sync + 'static,
{
    Box::new(move |event| {
        if predicate(&event) {
            Some(event)
        } else {
            None
        }
    })
}

/// Create a map transformation
pub fn map_transform<F>(mapper: F) -> TransformFn
where
    F: Fn(CdcEvent) -> CdcEvent + Send + Sync + 'static,
{
    Box::new(move |event| Some(mapper(event)))
}

// ============================================================================
// Event Aggregation
// ============================================================================

/// Aggregates events for batch processing
pub struct EventAggregator {
    /// Buffered events by key
    buffers: HashMap<String, Vec<CdcEvent>>,
    /// Maximum buffer size
    max_size: usize,
    /// Maximum buffer age
    max_age: std::time::Duration,
    /// Buffer creation times
    creation_times: HashMap<String, std::time::Instant>,
}

impl EventAggregator {
    pub fn new(max_size: usize, max_age: std::time::Duration) -> Self {
        Self {
            buffers: HashMap::new(),
            max_size,
            max_age,
            creation_times: HashMap::new(),
        }
    }

    /// Add an event and return flushed events if buffer is full
    pub fn add(&mut self, event: CdcEvent) -> Option<Vec<CdcEvent>> {
        let key = format!("{}.{}", event.database, event.table);
        let now = std::time::Instant::now();

        // Check if buffer exists and is expired
        if let Some(created) = self.creation_times.get(&key) {
            if now.duration_since(*created) >= self.max_age {
                return self.flush(&key);
            }
        }

        // Add to buffer
        let buffer = self.buffers.entry(key.clone()).or_insert_with(Vec::new);
        if buffer.is_empty() {
            self.creation_times.insert(key.clone(), now);
        }
        buffer.push(event);

        // Check if buffer is full
        if buffer.len() >= self.max_size {
            return self.flush(&key);
        }

        None
    }

    /// Flush a specific buffer
    pub fn flush(&mut self, key: &str) -> Option<Vec<CdcEvent>> {
        self.creation_times.remove(key);
        self.buffers.remove(key)
    }

    /// Flush all buffers
    pub fn flush_all(&mut self) -> Vec<(String, Vec<CdcEvent>)> {
        let keys: Vec<_> = self.buffers.keys().cloned().collect();
        let mut result = Vec::new();

        for key in keys {
            if let Some(events) = self.flush(&key) {
                result.push((key, events));
            }
        }

        result
    }

    /// Flush expired buffers
    pub fn flush_expired(&mut self) -> Vec<(String, Vec<CdcEvent>)> {
        let now = std::time::Instant::now();
        let expired: Vec<_> = self
            .creation_times
            .iter()
            .filter(|(_, created)| now.duration_since(**created) >= self.max_age)
            .map(|(key, _)| key.clone())
            .collect();

        let mut result = Vec::new();
        for key in expired {
            if let Some(events) = self.flush(&key) {
                result.push((key, events));
            }
        }

        result
    }
}

// ============================================================================
// Event Deduplication
// ============================================================================

/// Deduplicates events based on position
pub struct EventDeduplicator {
    /// Seen positions
    seen: HashMap<String, CdcPosition>,
    /// Maximum entries to track
    max_entries: usize,
}

impl EventDeduplicator {
    pub fn new(max_entries: usize) -> Self {
        Self {
            seen: HashMap::new(),
            max_entries,
        }
    }

    /// Check if event is duplicate
    pub fn is_duplicate(&mut self, event: &CdcEvent) -> bool {
        let key = format!(
            "{}:{}:{}:{}",
            event.source, event.database, event.table, event.txn_id.as_deref().unwrap_or("")
        );

        if let Some(seen_pos) = self.seen.get(&key) {
            if *seen_pos == event.position {
                return true;
            }
        }

        // Evict oldest if at capacity
        if self.seen.len() >= self.max_entries {
            // Simple eviction: remove first key
            if let Some(first_key) = self.seen.keys().next().cloned() {
                self.seen.remove(&first_key);
            }
        }

        self.seen.insert(key, event.position.clone());
        false
    }

    /// Clear all seen positions
    pub fn clear(&mut self) {
        self.seen.clear();
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn create_test_event() -> CdcEvent {
        CdcEvent {
            source: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: Some("public".to_string()),
            table: "users".to_string(),
            operation: CdcOperation::Insert,
            before: None,
            after: Some(Row::new(vec![
                Value::Int64(1),
                Value::String("Alice".into()),
            ])),
            timestamp: Utc::now(),
            position: CdcPosition::PostgresLsn(12345),
            txn_id: Some("tx123".to_string()),
        }
    }

    #[test]
    fn test_event_envelope_conversion() {
        let event = create_test_event();
        let envelope = EventEnvelope::from(&event);

        assert_eq!(envelope.payload.op, "c");
        assert_eq!(envelope.payload.source.db, "testdb");
        assert_eq!(envelope.payload.source.table, "users");
        assert!(envelope.payload.after.is_some());
        assert!(envelope.payload.before.is_none());
    }

    #[test]
    fn test_event_serialization() {
        let event = create_test_event();
        let bytes = serialize_event(&event, SerializationFormat::Json).unwrap();
        let payload = deserialize_event(&bytes, SerializationFormat::Json).unwrap();

        assert_eq!(payload.op, "c");
        assert_eq!(payload.source.db, "testdb");
    }

    #[test]
    fn test_event_transformer() {
        let mut transformer = EventTransformer::new();

        // Add filter that only passes inserts
        transformer.register(
            "inserts_only",
            filter_transform(|e| matches!(e.operation, CdcOperation::Insert)),
        );

        // Add map that uppercases table name
        transformer.register(
            "uppercase_table",
            map_transform(|mut e| {
                e.table = e.table.to_uppercase();
                e
            }),
        );

        let event = create_test_event();

        // Test filter
        let filtered = transformer.transform("inserts_only", event.clone());
        assert!(filtered.is_some());

        // Test map
        let mapped = transformer.transform("uppercase_table", event.clone());
        assert_eq!(mapped.unwrap().table, "USERS");

        // Test chain
        let chained = transformer.transform_chain(&["inserts_only", "uppercase_table"], event);
        assert_eq!(chained.unwrap().table, "USERS");
    }

    #[test]
    fn test_event_aggregator() {
        let mut aggregator = EventAggregator::new(3, std::time::Duration::from_secs(60));

        let event1 = create_test_event();
        let event2 = create_test_event();
        let event3 = create_test_event();

        assert!(aggregator.add(event1).is_none());
        assert!(aggregator.add(event2).is_none());

        // Third event should trigger flush
        let flushed = aggregator.add(event3);
        assert!(flushed.is_some());
        assert_eq!(flushed.unwrap().len(), 3);
    }

    #[test]
    fn test_event_deduplicator() {
        let mut dedup = EventDeduplicator::new(100);
        let event = create_test_event();

        assert!(!dedup.is_duplicate(&event));
        assert!(dedup.is_duplicate(&event)); // Same event is duplicate

        // Different position is not duplicate
        let mut event2 = event.clone();
        event2.position = CdcPosition::PostgresLsn(99999);
        assert!(!dedup.is_duplicate(&event2));
    }
}

// ============================================================================
// Base64 encoding helper
// ============================================================================

mod base64 {
    

    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(data: &[u8]) -> String {
        let mut result = String::new();
        let mut i = 0;

        while i < data.len() {
            let b0 = data[i] as usize;
            let b1 = if i + 1 < data.len() { data[i + 1] as usize } else { 0 };
            let b2 = if i + 2 < data.len() { data[i + 2] as usize } else { 0 };

            result.push(ALPHABET[b0 >> 2] as char);
            result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

            if i + 1 < data.len() {
                result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
            } else {
                result.push('=');
            }

            if i + 2 < data.len() {
                result.push(ALPHABET[b2 & 0x3f] as char);
            } else {
                result.push('=');
            }

            i += 3;
        }

        result
    }
}
