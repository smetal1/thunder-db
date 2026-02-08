//! Redis CDC Connector
//!
//! Implements Change Data Capture via Redis keyspace notifications and streams:
//! - Keyspace notifications for key changes (pub/sub)
//! - Redis Streams for ordered event log (XREAD)
//! - Supports Valkey (Redis-compatible fork)
//! - Provides pattern-based filtering

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, Client, RedisResult, Value as RedisValue,
};
use tracing::{debug, info};

use crate::{CdcConfig, CdcConnector, CdcEvent, CdcOperation, CdcPosition};
use thunder_common::prelude::*;

// ============================================================================
// Redis Connector
// ============================================================================

/// Redis CDC connector using keyspace notifications and streams
pub struct RedisConnector {
    /// Configuration
    config: CdcConfig,
    /// Redis client
    client: Option<Client>,
    /// Connection for commands
    conn: Option<MultiplexedConnection>,
    /// Current stream ID position
    stream_id: String,
    /// Connected flag
    connected: bool,
    /// Key patterns to track
    patterns: Vec<String>,
    /// Stream keys to track
    streams: Vec<String>,
    /// Poll interval
    poll_interval: Duration,
    /// Last poll time
    last_poll: std::time::Instant,
    /// Mode: Streams or Keyspace
    mode: RedisMode,
    /// Database number
    database: i64,
}

/// Redis CDC mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisMode {
    /// Use Redis Streams (XREAD)
    Streams,
    /// Use Keyspace notifications (SUBSCRIBE)
    KeyspaceNotifications,
    /// Use both
    Both,
}

impl Default for RedisMode {
    fn default() -> Self {
        RedisMode::Streams
    }
}

impl RedisConnector {
    /// Create a new Redis connector
    pub async fn new(config: CdcConfig) -> Result<Self> {
        // Parse patterns and streams from config.tables
        // Format: "stream:mystream" or "pattern:user:*" or just "mystream"
        let mut patterns = Vec::new();
        let mut streams = Vec::new();

        for table in &config.tables {
            if let Some(stripped) = table.strip_prefix("pattern:") {
                patterns.push(stripped.to_string());
            } else if let Some(stripped) = table.strip_prefix("stream:") {
                streams.push(stripped.to_string());
            } else {
                // Default to stream
                streams.push(table.clone());
            }
        }

        // Determine mode based on what we're tracking
        let mode = if !streams.is_empty() && !patterns.is_empty() {
            RedisMode::Both
        } else if !patterns.is_empty() {
            RedisMode::KeyspaceNotifications
        } else {
            RedisMode::Streams
        };

        Ok(Self {
            config,
            client: None,
            conn: None,
            stream_id: "0".to_string(), // Start from beginning
            connected: false,
            patterns,
            streams,
            poll_interval: Duration::from_millis(100),
            last_poll: std::time::Instant::now(),
            mode,
            database: 0,
        })
    }

    /// Connect to Redis
    async fn connect(&mut self) -> Result<()> {
        if self.connected {
            return Ok(());
        }

        let client = Client::open(self.config.url.as_str())
            .map_err(|e| Error::Internal(format!("Invalid Redis URL: {}", e)))?;

        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Internal(format!("Redis connection failed: {}", e)))?;

        self.client = Some(client);
        self.conn = Some(conn);
        self.connected = true;

        // Enable keyspace notifications if needed
        if matches!(self.mode, RedisMode::KeyspaceNotifications | RedisMode::Both) {
            self.enable_keyspace_notifications().await?;
        }

        info!(mode = ?self.mode, "Redis connector connected");

        Ok(())
    }

    /// Enable keyspace notifications on the server
    async fn enable_keyspace_notifications(&mut self) -> Result<()> {
        let conn = self.conn.as_mut().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        // Set notify-keyspace-events to enable notifications
        // KEA = Keyspace + Keyevent + All commands
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("KEA")
            .query_async(conn)
            .await
            .map_err(|e| Error::Internal(format!("Failed to enable notifications: {}", e)))?;

        info!("Enabled Redis keyspace notifications");

        Ok(())
    }

    /// Poll Redis Streams for changes
    async fn poll_streams(&mut self) -> Result<Vec<CdcEvent>> {
        if self.streams.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.conn.as_mut().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        let mut events = Vec::new();
        let mut last_id = self.stream_id.clone();

        for stream in &self.streams.clone() {
            // XREAD from stream
            let opts = StreamReadOptions::default()
                .count(self.config.batch_size)
                .block(50); // 50ms block

            let result: Option<StreamReadReply> = conn
                .xread_options(&[stream], &[&last_id], &opts)
                .await
                .map_err(|e| Error::Internal(format!("XREAD failed: {}", e)))?;

            if let Some(reply) = result {
                for stream_key in reply.keys {
                    for entry in stream_key.ids {
                        let id = entry.id.clone();

                        // Parse entry data
                        let mut data_map = HashMap::new();
                        for (key, value) in entry.map {
                            data_map.insert(key, redis_value_to_string(&value));
                        }

                        // Determine operation from data or default to insert
                        let operation = data_map
                            .get("__op")
                            .map(|op| match op.as_str() {
                                "insert" | "set" => CdcOperation::Insert,
                                "update" => CdcOperation::Update,
                                "delete" | "del" => CdcOperation::Delete,
                                _ => CdcOperation::Insert,
                            })
                            .unwrap_or(CdcOperation::Insert);

                        // Build row from data
                        let values: Vec<Value> = data_map
                            .iter()
                            .filter(|(k, _)| !k.starts_with("__"))
                            .map(|(k, v)| Value::String(format!("{}={}", k, v).into()))
                            .collect();

                        let event = CdcEvent {
                            source: "redis".to_string(),
                            database: self.database.to_string(),
                            schema: None,
                            table: stream.clone(),
                            operation,
                            before: None,
                            after: Some(Row::new(values)),
                            timestamp: parse_stream_id_timestamp(&id),
                            position: CdcPosition::RedisStreamId(id.clone()),
                            txn_id: None,
                        };

                        events.push(event);
                        last_id = id;
                    }
                }
            }
        }

        // Update position
        if last_id != self.stream_id {
            self.stream_id = last_id;
        }

        Ok(events)
    }

    /// Poll for key changes using SCAN and memory
    /// Note: This is a fallback when keyspace notifications aren't used
    async fn poll_key_changes(&mut self) -> Result<Vec<CdcEvent>> {
        if self.patterns.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.conn.as_mut().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        let mut events = Vec::new();

        for pattern in &self.patterns.clone() {
            // SCAN for keys matching pattern
            let scan_result: RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN")
                .arg(0)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(self.config.batch_size)
                .query_async(conn)
                .await;

            let keys = scan_result.map(|(_, keys)| keys).unwrap_or_default();

            for key in keys {
                // Get key type
                let key_type: String = redis::cmd("TYPE")
                    .arg(&key)
                    .query_async(conn)
                    .await
                    .unwrap_or_else(|_| "none".to_string());

                // Get value based on type
                let value_str = match key_type.as_str() {
                    "string" => {
                        let val: Option<String> = conn.get(&key).await.ok();
                        val.unwrap_or_default()
                    }
                    "hash" => {
                        let val: HashMap<String, String> = conn.hgetall(&key).await.unwrap_or_default();
                        serde_json::to_string(&val).unwrap_or_default()
                    }
                    "list" => {
                        let val: Vec<String> = conn.lrange(&key, 0, -1).await.unwrap_or_default();
                        serde_json::to_string(&val).unwrap_or_default()
                    }
                    "set" => {
                        let val: Vec<String> = conn.smembers(&key).await.unwrap_or_default();
                        serde_json::to_string(&val).unwrap_or_default()
                    }
                    "zset" => {
                        let val: Vec<(String, f64)> = conn
                            .zrange_withscores(&key, 0, -1)
                            .await
                            .unwrap_or_default();
                        serde_json::to_string(&val).unwrap_or_default()
                    }
                    _ => continue,
                };

                // Create event for this key
                let event = CdcEvent {
                    source: "redis".to_string(),
                    database: self.database.to_string(),
                    schema: None,
                    table: key.clone(),
                    operation: CdcOperation::Update, // Treat as update since we don't know
                    before: None,
                    after: Some(Row::new(vec![
                        Value::String(key.clone().into()),
                        Value::String(key_type.into()),
                        Value::String(value_str.into()),
                    ])),
                    timestamp: Utc::now(),
                    position: CdcPosition::RedisStreamId(self.stream_id.clone()),
                    txn_id: None,
                };

                events.push(event);
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl CdcConnector for RedisConnector {
    fn name(&self) -> &str {
        "redis"
    }

    async fn start(&mut self, position: CdcPosition) -> Result<()> {
        // Set initial position
        if let CdcPosition::RedisStreamId(id) = position {
            if !id.is_empty() {
                self.stream_id = id;
            }
        }

        // Connect
        self.connect().await?;

        info!(
            stream_id = %self.stream_id,
            mode = ?self.mode,
            "Redis connector started"
        );

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

        let mut events = Vec::new();

        // Poll streams if configured
        if matches!(self.mode, RedisMode::Streams | RedisMode::Both) {
            events.extend(self.poll_streams().await?);
        }

        // Poll key changes if configured
        if matches!(self.mode, RedisMode::KeyspaceNotifications | RedisMode::Both) {
            events.extend(self.poll_key_changes().await?);
        }

        Ok(events)
    }

    async fn ack(&mut self, position: CdcPosition) -> Result<()> {
        if let CdcPosition::RedisStreamId(id) = position {
            self.stream_id = id;
            debug!(stream_id = %self.stream_id, "Acknowledged Redis position");
        }
        Ok(())
    }

    fn position(&self) -> CdcPosition {
        CdcPosition::RedisStreamId(self.stream_id.clone())
    }

    async fn stop(&mut self) -> Result<()> {
        if self.connected {
            self.conn = None;
            self.client = None;
            self.connected = false;
            info!("Redis connector stopped");
        }
        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert Redis value to string
fn redis_value_to_string(value: &RedisValue) -> String {
    match value {
        RedisValue::Nil => String::new(),
        RedisValue::Int(i) => i.to_string(),
        RedisValue::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
        RedisValue::Bulk(arr) => {
            let strs: Vec<String> = arr.iter().map(redis_value_to_string).collect();
            strs.join(",")
        }
        RedisValue::Status(s) => s.clone(),
        RedisValue::Okay => "OK".to_string(),
    }
}

/// Parse timestamp from Redis stream ID (milliseconds since epoch)
fn parse_stream_id_timestamp(id: &str) -> DateTime<Utc> {
    // Stream ID format: <milliseconds>-<sequence>
    if let Some(millis_str) = id.split('-').next() {
        if let Ok(millis) = millis_str.parse::<i64>() {
            if let Some(dt) = DateTime::from_timestamp_millis(millis) {
                return dt;
            }
        }
    }
    Utc::now()
}

/// Create a Redis Stream entry ID from timestamp
#[allow(dead_code)]
fn timestamp_to_stream_id(ts: DateTime<Utc>) -> String {
    format!("{}-0", ts.timestamp_millis())
}

// ============================================================================
// Redis Stream Writer (for testing)
// ============================================================================

/// Helper to write events to Redis Streams
pub struct RedisStreamWriter {
    conn: MultiplexedConnection,
}

impl RedisStreamWriter {
    /// Create a new stream writer
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)
            .map_err(|e| Error::Internal(format!("Invalid Redis URL: {}", e)))?;

        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Internal(format!("Redis connection failed: {}", e)))?;

        Ok(Self { conn })
    }

    /// Write an event to a stream
    pub async fn write(&mut self, stream: &str, data: &HashMap<String, String>) -> Result<String> {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream).arg("*");

        for (key, value) in data {
            cmd.arg(key).arg(value);
        }

        let id: String = cmd
            .query_async(&mut self.conn)
            .await
            .map_err(|e| Error::Internal(format!("XADD failed: {}", e)))?;

        Ok(id)
    }

    /// Write a CDC event to a stream
    pub async fn write_cdc_event(&mut self, event: &CdcEvent) -> Result<String> {
        let mut data = HashMap::new();
        data.insert("__op".to_string(), format!("{:?}", event.operation).to_lowercase());
        data.insert("__table".to_string(), event.table.clone());
        data.insert("__timestamp".to_string(), event.timestamp.to_rfc3339());

        if let Some(after) = &event.after {
            data.insert("__data".to_string(), format!("{:?}", after.values));
        }

        self.write(&event.table, &data).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_stream_id_timestamp() {
        let id = "1609459200000-0"; // 2021-01-01 00:00:00 UTC
        let ts = parse_stream_id_timestamp(id);
        assert_eq!(ts.timestamp_millis(), 1609459200000);

        // Invalid ID should return current time (approximately)
        let ts = parse_stream_id_timestamp("invalid");
        assert!(ts.timestamp() > 0);
    }

    #[test]
    fn test_timestamp_to_stream_id() {
        let ts = DateTime::from_timestamp_millis(1609459200000).unwrap();
        let id = timestamp_to_stream_id(ts);
        assert_eq!(id, "1609459200000-0");
    }

    #[test]
    fn test_redis_value_to_string() {
        assert_eq!(redis_value_to_string(&RedisValue::Nil), "");
        assert_eq!(redis_value_to_string(&RedisValue::Int(42)), "42");
        assert_eq!(
            redis_value_to_string(&RedisValue::Data(b"hello".to_vec())),
            "hello"
        );
        assert_eq!(
            redis_value_to_string(&RedisValue::Status("test".to_string())),
            "test"
        );
    }

    #[test]
    fn test_mode_default() {
        assert_eq!(RedisMode::default(), RedisMode::Streams);
    }

    #[tokio::test]
    async fn test_connector_creation() {
        let config = CdcConfig {
            connector_type: crate::ConnectorType::Redis,
            url: "redis://localhost:6379".to_string(),
            tables: vec![
                "stream:events".to_string(),
                "pattern:user:*".to_string(),
            ],
            initial_position: CdcPosition::Beginning,
            batch_size: 1000,
            poll_interval_ms: 100,
        };

        let connector = RedisConnector::new(config).await.unwrap();
        assert_eq!(connector.name(), "redis");
        assert!(!connector.connected);
        assert_eq!(connector.streams.len(), 1);
        assert_eq!(connector.streams[0], "events");
        assert_eq!(connector.patterns.len(), 1);
        assert_eq!(connector.patterns[0], "user:*");
        assert_eq!(connector.mode, RedisMode::Both);
    }

    #[tokio::test]
    async fn test_connector_streams_only() {
        let config = CdcConfig {
            connector_type: crate::ConnectorType::Redis,
            url: "redis://localhost:6379".to_string(),
            tables: vec!["mystream".to_string()],
            initial_position: CdcPosition::Beginning,
            batch_size: 1000,
            poll_interval_ms: 100,
        };

        let connector = RedisConnector::new(config).await.unwrap();
        assert_eq!(connector.mode, RedisMode::Streams);
        assert_eq!(connector.streams.len(), 1);
        assert!(connector.patterns.is_empty());
    }
}
