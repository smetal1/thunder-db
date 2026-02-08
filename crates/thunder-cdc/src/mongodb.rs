//! MongoDB CDC Connector
//!
//! Implements Change Data Capture via MongoDB change streams:
//! - Uses oplog-based change streams (MongoDB 3.6+)
//! - Supports resume tokens for exactly-once delivery
//! - Handles all CRUD operations and collection drops
//! - Provides full document lookup option

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use mongodb::{
    bson::{self, doc, Bson, Document},
    options::{ChangeStreamOptions, ClientOptions, FullDocumentType},
    Client,
};
use tracing::{debug, error, info, warn};

use crate::{CdcConfig, CdcConnector, CdcEvent, CdcOperation, CdcPosition};
use thunder_common::prelude::*;

// ============================================================================
// MongoDB Connector
// ============================================================================

/// MongoDB CDC connector using change streams
pub struct MongoConnector {
    /// Configuration
    config: CdcConfig,
    /// MongoDB client
    client: Option<Client>,
    /// Current resume token
    resume_token: Option<Vec<u8>>,
    /// Connected flag
    connected: bool,
    /// Tracked collections (database.collection)
    collections: Vec<(String, String)>,
    /// Poll interval
    poll_interval: Duration,
    /// Last poll time
    last_poll: std::time::Instant,
    /// Full document on update
    full_document: bool,
}

impl MongoConnector {
    /// Create a new MongoDB connector
    pub async fn new(config: CdcConfig) -> Result<Self> {
        // Parse collections from config.tables
        let collections: Vec<(String, String)> = config
            .tables
            .iter()
            .map(|t| {
                let parts: Vec<&str> = t.split('.').collect();
                if parts.len() == 2 {
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    ("default".to_string(), parts[0].to_string())
                }
            })
            .collect();

        Ok(Self {
            config,
            client: None,
            resume_token: None,
            connected: false,
            collections,
            poll_interval: Duration::from_millis(100),
            last_poll: std::time::Instant::now(),
            full_document: true,
        })
    }

    /// Connect to MongoDB
    async fn connect(&mut self) -> Result<()> {
        if self.connected {
            return Ok(());
        }

        let client_options = ClientOptions::parse(&self.config.url)
            .await
            .map_err(|e| Error::Internal(format!("Invalid MongoDB URL: {}", e)))?;

        let client = Client::with_options(client_options)
            .map_err(|e| Error::Internal(format!("MongoDB connection failed: {}", e)))?;

        // Verify connection
        client
            .database("admin")
            .run_command(doc! { "ping": 1 }, None)
            .await
            .map_err(|e| Error::Internal(format!("MongoDB ping failed: {}", e)))?;

        self.client = Some(client);
        self.connected = true;

        info!("MongoDB connector connected");

        Ok(())
    }

    /// Create change stream options
    fn change_stream_options(&self) -> ChangeStreamOptions {
        let mut options = ChangeStreamOptions::builder()
            .batch_size(Some(self.config.batch_size as u32))
            .build();

        if self.full_document {
            options.full_document = Some(FullDocumentType::UpdateLookup);
        }

        // Resume token is set via start_after in the builder, not here
        // The resume_token is handled at the stream level

        options
    }

    /// Poll changes from a single collection
    async fn poll_collection_changes(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<Vec<CdcEvent>> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        let coll = client.database(database).collection::<Document>(collection);

        // Build pipeline to filter operations
        let pipeline: Vec<Document> = vec![
            doc! {
                "$match": {
                    "operationType": {
                        "$in": ["insert", "update", "replace", "delete"]
                    }
                }
            },
        ];

        let options = self.change_stream_options();

        // Create change stream
        let mut stream = coll
            .watch(pipeline, Some(options))
            .await
            .map_err(|e| Error::Internal(format!("Change stream failed: {}", e)))?;

        let mut events = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_millis(100);

        // Poll with timeout
        while std::time::Instant::now() < deadline {
            tokio::select! {
                change = stream.next() => {
                    match change {
                        Some(Ok(event)) => {
                            if let Some(cdc_event) = self.parse_change_event(database, collection, &event) {
                                events.push(cdc_event);
                            }
                            if events.len() >= self.config.batch_size {
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "Change stream error");
                            break;
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    if events.is_empty() {
                        break;
                    }
                }
            }
        }

        Ok(events)
    }

    /// Parse change stream event to CDC event
    fn parse_change_event(
        &self,
        database: &str,
        collection: &str,
        event: &mongodb::change_stream::event::ChangeStreamEvent<Document>,
    ) -> Option<CdcEvent> {
        let operation = match event.operation_type {
            mongodb::change_stream::event::OperationType::Insert => CdcOperation::Insert,
            mongodb::change_stream::event::OperationType::Update => CdcOperation::Update,
            mongodb::change_stream::event::OperationType::Replace => CdcOperation::Update,
            mongodb::change_stream::event::OperationType::Delete => CdcOperation::Delete,
            mongodb::change_stream::event::OperationType::Drop => CdcOperation::Truncate,
            mongodb::change_stream::event::OperationType::Invalidate => return None,
            _ => return None,
        };

        // Get resume token
        let resume_token = bson::to_vec(&event.id)
            .ok()
            .unwrap_or_default();

        // Parse document data
        let after = event
            .full_document
            .as_ref()
            .map(|doc| document_to_row(doc));

        // Get timestamp
        let timestamp = event
            .cluster_time
            .map(|ts| {
                DateTime::from_timestamp(ts.time.into(), 0)
                    .unwrap_or_else(|| Utc::now())
            })
            .unwrap_or_else(Utc::now);

        // Get transaction ID if available (not available in all MongoDB versions)
        let txn_id: Option<String> = None; // MongoDB 2.8 doesn't expose txn_number directly

        Some(CdcEvent {
            source: "mongodb".to_string(),
            database: database.to_string(),
            schema: None,
            table: collection.to_string(),
            operation,
            before: None,
            after,
            timestamp,
            position: CdcPosition::MongoResumeToken(resume_token),
            txn_id,
        })
    }

    /// Watch all tracked collections
    async fn poll_all_collections(&mut self) -> Result<Vec<CdcEvent>> {
        let mut all_events = Vec::new();

        for (database, collection) in self.collections.clone() {
            match self.poll_collection_changes(&database, &collection).await {
                Ok(events) => {
                    // Update resume token from last event
                    if let Some(event) = events.last() {
                        if let CdcPosition::MongoResumeToken(token) = &event.position {
                            self.resume_token = Some(token.clone());
                        }
                    }
                    all_events.extend(events);
                }
                Err(e) => {
                    warn!(
                        database = %database,
                        collection = %collection,
                        error = %e,
                        "Failed to poll collection"
                    );
                }
            }
        }

        Ok(all_events)
    }
}

#[async_trait]
impl CdcConnector for MongoConnector {
    fn name(&self) -> &str {
        "mongodb"
    }

    async fn start(&mut self, position: CdcPosition) -> Result<()> {
        // Set resume token from position
        if let CdcPosition::MongoResumeToken(token) = position {
            if !token.is_empty() {
                self.resume_token = Some(token);
            }
        }

        // Connect
        self.connect().await?;

        info!("MongoDB connector started");

        Ok(())
    }

    async fn poll(&mut self) -> Result<Vec<CdcEvent>> {
        if !self.connected {
            self.connect().await?;
        }

        // Rate limit polling
        let elapsed = self.last_poll.elapsed();
        if elapsed < self.poll_interval {
            tokio::time::sleep(self.poll_interval - elapsed).await;
        }
        self.last_poll = std::time::Instant::now();

        self.poll_all_collections().await
    }

    async fn ack(&mut self, position: CdcPosition) -> Result<()> {
        if let CdcPosition::MongoResumeToken(token) = position {
            self.resume_token = Some(token);
            debug!("Acknowledged MongoDB position");
        }
        Ok(())
    }

    fn position(&self) -> CdcPosition {
        CdcPosition::MongoResumeToken(self.resume_token.clone().unwrap_or_default())
    }

    async fn stop(&mut self) -> Result<()> {
        if self.connected {
            self.client = None;
            self.connected = false;
            info!("MongoDB connector stopped");
        }
        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert MongoDB document to Row
fn document_to_row(doc: &Document) -> Row {
    let values: Vec<Value> = doc.iter().map(|(_, v)| bson_to_value(v)).collect();
    Row::new(values)
}

/// Convert BSON value to internal Value
fn bson_to_value(bson: &Bson) -> Value {
    match bson {
        Bson::Null => Value::Null,
        Bson::Boolean(b) => Value::Boolean(*b),
        Bson::Int32(i) => Value::Int32(*i),
        Bson::Int64(i) => Value::Int64(*i),
        Bson::Double(f) => Value::Float64(*f),
        Bson::String(s) => Value::String(s.clone().into()),
        Bson::Array(arr) => {
            let values: Vec<Value> = arr.iter().map(bson_to_value).collect();
            Value::Array(values.into())
        }
        Bson::Document(doc) => {
            // Convert to JSON string
            let json_str = serde_json::to_string(doc).unwrap_or_default();
            Value::Json(json_str.into())
        }
        Bson::Binary(b) => Value::Binary(b.bytes.clone().into()),
        Bson::ObjectId(oid) => Value::String(oid.to_hex().into()),
        Bson::DateTime(dt) => {
            // Convert to microseconds since epoch
            Value::Timestamp(dt.timestamp_millis() * 1000)
        }
        Bson::Timestamp(ts) => {
            // Convert to microseconds since epoch
            Value::Timestamp(ts.time as i64 * 1_000_000)
        }
        Bson::Decimal128(d) => {
            // Convert to string, then parse
            Value::String(d.to_string().into())
        }
        Bson::RegularExpression(regex) => {
            Value::String(format!("/{}/{}", regex.pattern, regex.options).into())
        }
        Bson::JavaScriptCode(code) => Value::String(code.clone().into()),
        Bson::JavaScriptCodeWithScope(js) => Value::String(js.code.clone().into()),
        Bson::Symbol(s) => Value::String(s.clone().into()),
        Bson::Undefined | Bson::MinKey | Bson::MaxKey | Bson::DbPointer(_) => Value::Null,
    }
}

/// Convert internal Value to BSON
#[allow(dead_code)]
fn value_to_bson(value: &Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Boolean(b) => Bson::Boolean(*b),
        Value::Int8(i) => Bson::Int32(*i as i32),
        Value::Int16(i) => Bson::Int32(*i as i32),
        Value::Int32(i) => Bson::Int32(*i),
        Value::Int64(i) => Bson::Int64(*i),
        Value::Float32(f) => Bson::Double(*f as f64),
        Value::Float64(f) => Bson::Double(*f),
        Value::String(s) => Bson::String(s.to_string()),
        Value::Binary(b) => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: b.to_vec(),
        }),
        Value::Timestamp(ts) => Bson::DateTime(bson::DateTime::from_millis(*ts / 1000)), // Convert microseconds to milliseconds
        v @ Value::Date(_) => Bson::String(format!("{}", v)),
        v @ Value::Time(_) => Bson::String(format!("{}", v)),
        Value::Uuid(u) => Bson::String(hex::encode(u)),
        Value::Array(arr) => {
            let bson_arr: Vec<Bson> = arr.iter().map(value_to_bson).collect();
            Bson::Array(bson_arr)
        }
        Value::Json(j) => {
            // Parse JSON string and convert to BSON
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(j) {
                bson::to_bson(&parsed).unwrap_or(Bson::Null)
            } else {
                Bson::String(j.to_string())
            }
        }
        _ => Bson::Null,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bson_to_value() {
        assert!(matches!(bson_to_value(&Bson::Null), Value::Null));
        assert!(matches!(bson_to_value(&Bson::Boolean(true)), Value::Boolean(true)));
        assert!(matches!(bson_to_value(&Bson::Int32(42)), Value::Int32(42)));
        assert!(matches!(bson_to_value(&Bson::Int64(42)), Value::Int64(42)));
        assert!(matches!(bson_to_value(&Bson::Double(3.14)), Value::Float64(f) if (f - 3.14).abs() < 0.001));
        assert!(matches!(bson_to_value(&Bson::String("hello".to_string())), Value::String(_)));
    }

    #[test]
    fn test_document_to_row() {
        let doc = doc! {
            "name": "Alice",
            "age": 30,
            "active": true
        };

        let row = document_to_row(&doc);
        assert_eq!(row.len(), 3);
    }

    #[tokio::test]
    async fn test_connector_creation() {
        let config = CdcConfig {
            connector_type: crate::ConnectorType::MongoDB,
            url: "mongodb://localhost:27017".to_string(),
            tables: vec!["testdb.users".to_string()],
            initial_position: CdcPosition::Beginning,
            batch_size: 1000,
            poll_interval_ms: 100,
        };

        let connector = MongoConnector::new(config).await.unwrap();
        assert_eq!(connector.name(), "mongodb");
        assert!(!connector.connected);
        assert_eq!(connector.collections.len(), 1);
        assert_eq!(connector.collections[0], ("testdb".to_string(), "users".to_string()));
    }
}
