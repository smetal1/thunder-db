//! # Thunder CDC
//!
//! Change Data Capture connectors for ThunderDB providing:
//! - PostgreSQL logical replication
//! - MySQL binlog
//! - MongoDB change streams
//! - Redis keyspace notifications
//!
//! ## Usage
//!
//! ```ignore
//! use thunder_cdc::{CdcConfig, ConnectorType, CdcPosition, ConnectorManager};
//!
//! // Create a PostgreSQL connector
//! let config = CdcConfig {
//!     connector_type: ConnectorType::PostgreSQL,
//!     url: "postgres://localhost/mydb".to_string(),
//!     tables: vec!["public.users".to_string()],
//!     initial_position: CdcPosition::Beginning,
//!     batch_size: 1000,
//!     poll_interval_ms: 100,
//! };
//!
//! // Create manager and subscribe
//! let manager = ConnectorManager::new(Default::default(), position_store);
//! let subscription = manager.create_subscription(config).await?;
//!
//! // Receive events
//! let mut rx = manager.subscribe();
//! while let Ok(event) = rx.recv().await {
//!     println!("Event: {:?}", event);
//! }
//! ```

pub mod connector;
pub mod event;
pub mod mongodb;
pub mod mysql;
pub mod postgres;
pub mod redis;

// Re-export connector types
pub use connector::{
    ChannelDestination, ConnectorManager, ConnectorManagerConfig, ConnectorMetrics,
    EventDestination, EventRouter, LogDestination, MemoryPositionStore, PositionStore,
    RoutingRule, SourceFilter,
};

// Re-export event types
pub use event::{
    deserialize_event, filter_transform, map_transform, serialize_event, EventAggregator,
    EventDeduplicator, EventEnvelope, EventPayload, EventTransformer, SchemaInfo,
    SerializationFormat, SourceMetadata, TransactionMetadata, TransformFn,
};

// Re-export database-specific connectors
pub use mongodb::MongoConnector;
pub use mysql::MySqlConnector;
pub use postgres::{DecoderType, PostgresConnector};
pub use redis::{RedisConnector, RedisMode, RedisStreamWriter};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use thunder_common::prelude::*;

/// CDC connector trait
#[async_trait]
pub trait CdcConnector: Send + Sync {
    /// Get connector name
    fn name(&self) -> &str;

    /// Start capturing changes from a position
    async fn start(&mut self, position: CdcPosition) -> Result<()>;

    /// Poll for next batch of changes
    async fn poll(&mut self) -> Result<Vec<CdcEvent>>;

    /// Acknowledge processed events
    async fn ack(&mut self, position: CdcPosition) -> Result<()>;

    /// Get current position
    fn position(&self) -> CdcPosition;

    /// Stop the connector
    async fn stop(&mut self) -> Result<()>;
}

/// CDC position (checkpoint)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CdcPosition {
    /// PostgreSQL LSN
    PostgresLsn(u64),
    /// MySQL binlog position
    MysqlBinlog { file: String, position: u64 },
    /// MongoDB resume token
    MongoResumeToken(Vec<u8>),
    /// Redis stream ID
    RedisStreamId(String),
    /// Beginning of stream
    Beginning,
    /// End of stream
    End,
}

/// CDC event
#[derive(Debug, Clone)]
pub struct CdcEvent {
    /// Source database/connector
    pub source: String,
    /// Database name
    pub database: String,
    /// Schema name
    pub schema: Option<String>,
    /// Table/collection name
    pub table: String,
    /// Operation type
    pub operation: CdcOperation,
    /// Data before change (for updates/deletes)
    pub before: Option<Row>,
    /// Data after change (for inserts/updates)
    pub after: Option<Row>,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Position for checkpointing
    pub position: CdcPosition,
    /// Transaction ID (if available)
    pub txn_id: Option<String>,
}

/// CDC operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
    Truncate,
    SchemaChange,
    Snapshot,
}

/// CDC connector configuration
#[derive(Debug, Clone)]
pub struct CdcConfig {
    /// Connector type
    pub connector_type: ConnectorType,
    /// Connection URL
    pub url: String,
    /// Tables to capture
    pub tables: Vec<String>,
    /// Initial position
    pub initial_position: CdcPosition,
    /// Batch size
    pub batch_size: usize,
    /// Poll interval
    pub poll_interval_ms: u64,
}

/// Supported connector types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorType {
    PostgreSQL,
    MySQL,
    MongoDB,
    Redis,
    Valkey,
}

/// CDC subscription
#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: uuid::Uuid,
    pub name: String,
    pub config: CdcConfig,
    pub status: SubscriptionStatus,
    pub current_position: CdcPosition,
    pub created_at: DateTime<Utc>,
}

/// Subscription status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionStatus {
    Active,
    Paused,
    Failed,
    Stopped,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_event() {
        let event = CdcEvent {
            source: "postgres".to_string(),
            database: "mydb".to_string(),
            schema: Some("public".to_string()),
            table: "users".to_string(),
            operation: CdcOperation::Insert,
            before: None,
            after: Some(Row::new(vec![Value::Int64(1), Value::String("Alice".into())])),
            timestamp: Utc::now(),
            position: CdcPosition::PostgresLsn(12345),
            txn_id: Some("1234".to_string()),
        };

        assert_eq!(event.operation, CdcOperation::Insert);
        assert!(event.after.is_some());
    }

    #[test]
    fn test_cdc_position() {
        let pos = CdcPosition::PostgresLsn(12345);
        assert!(matches!(pos, CdcPosition::PostgresLsn(12345)));
    }
}
