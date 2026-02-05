//! CDC Connector Manager
//!
//! Provides a unified interface for managing multiple CDC connectors:
//! - Connector registry and lifecycle management
//! - Event routing and distribution
//! - Position tracking and checkpointing
//! - Health monitoring and recovery

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    CdcConfig, CdcConnector, CdcEvent, CdcOperation, CdcPosition, ConnectorType,
    Subscription, SubscriptionStatus,
};
use thunder_common::prelude::*;

// ============================================================================
// Connector Manager
// ============================================================================

/// Manages multiple CDC connectors and distributes events
pub struct ConnectorManager {
    /// Active connectors by subscription ID
    connectors: DashMap<Uuid, ConnectorState>,
    /// Event broadcaster for fan-out
    event_tx: broadcast::Sender<CdcEvent>,
    /// Subscription registry
    subscriptions: DashMap<Uuid, Subscription>,
    /// Position store for checkpointing
    position_store: Arc<dyn PositionStore>,
    /// Configuration
    config: ConnectorManagerConfig,
    /// Metrics
    metrics: ConnectorMetrics,
}

/// State of a running connector
struct ConnectorState {
    /// The actual connector
    connector: Box<dyn CdcConnector>,
    /// Background task handle
    task_handle: Option<JoinHandle<()>>,
    /// Event channel for this connector
    event_tx: mpsc::Sender<CdcEvent>,
    /// Control channel
    control_tx: mpsc::Sender<ConnectorControl>,
    /// Status
    status: SubscriptionStatus,
    /// Last activity
    last_activity: Instant,
    /// Error count
    error_count: u32,
    /// Circuit breaker for this connector
    circuit_breaker: thunder_common::circuit_breaker::CircuitBreaker,
}

/// Control messages for connectors
enum ConnectorControl {
    Start,
    Pause,
    Resume,
    Stop,
    Checkpoint,
}

/// Manager configuration
#[derive(Debug, Clone)]
pub struct ConnectorManagerConfig {
    /// Maximum concurrent connectors
    pub max_connectors: usize,
    /// Event channel buffer size
    pub event_buffer_size: usize,
    /// Checkpoint interval
    pub checkpoint_interval: Duration,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Max consecutive errors before stopping
    pub max_errors: u32,
    /// Retry backoff base
    pub retry_backoff_base: Duration,
    /// Max retry backoff
    pub max_retry_backoff: Duration,
}

impl Default for ConnectorManagerConfig {
    fn default() -> Self {
        Self {
            max_connectors: 100,
            event_buffer_size: 10000,
            checkpoint_interval: Duration::from_secs(5),
            health_check_interval: Duration::from_secs(30),
            max_errors: 5,
            retry_backoff_base: Duration::from_millis(100),
            max_retry_backoff: Duration::from_secs(60),
        }
    }
}

/// Metrics for connector manager
#[derive(Debug, Default)]
pub struct ConnectorMetrics {
    /// Total events processed
    pub events_processed: std::sync::atomic::AtomicU64,
    /// Events per connector
    pub events_by_connector: DashMap<Uuid, u64>,
    /// Error count per connector
    pub errors_by_connector: DashMap<Uuid, u64>,
    /// Current lag (latest position - current position)
    pub lag_by_connector: DashMap<Uuid, u64>,
}

impl ConnectorManager {
    /// Create a new connector manager
    pub fn new(config: ConnectorManagerConfig, position_store: Arc<dyn PositionStore>) -> Self {
        let (event_tx, _) = broadcast::channel(config.event_buffer_size);

        Self {
            connectors: DashMap::new(),
            event_tx,
            subscriptions: DashMap::new(),
            position_store,
            config,
            metrics: ConnectorMetrics::default(),
        }
    }

    /// Create a subscription and start the connector
    pub async fn create_subscription(&self, config: CdcConfig) -> Result<Subscription> {
        if self.connectors.len() >= self.config.max_connectors {
            return Err(Error::Internal(
                "Maximum number of connectors reached".to_string(),
            ));
        }

        let id = Uuid::new_v4();
        let now = chrono::Utc::now();

        // Load checkpoint if available
        let initial_position = self
            .position_store
            .load(id)
            .await?
            .unwrap_or_else(|| config.initial_position.clone());

        let subscription = Subscription {
            id,
            name: format!("{:?}_{}", config.connector_type, id),
            config: config.clone(),
            status: SubscriptionStatus::Active,
            current_position: initial_position.clone(),
            created_at: now,
        };

        // Create connector based on type
        let connector = self.create_connector(&config).await?;

        // Start the connector
        self.start_connector(id, connector, initial_position).await?;

        self.subscriptions.insert(id, subscription.clone());

        info!(subscription_id = %id, "Created CDC subscription");

        Ok(subscription)
    }

    /// Create a connector based on configuration
    async fn create_connector(&self, config: &CdcConfig) -> Result<Box<dyn CdcConnector>> {
        match config.connector_type {
            ConnectorType::PostgreSQL => {
                let pg = crate::postgres::PostgresConnector::new(config.clone()).await?;
                Ok(Box::new(pg))
            }
            ConnectorType::MySQL => {
                let mysql = crate::mysql::MySqlConnector::new(config.clone()).await?;
                Ok(Box::new(mysql))
            }
            ConnectorType::MongoDB => {
                let mongo = crate::mongodb::MongoConnector::new(config.clone()).await?;
                Ok(Box::new(mongo))
            }
            ConnectorType::Redis | ConnectorType::Valkey => {
                let redis = crate::redis::RedisConnector::new(config.clone()).await?;
                Ok(Box::new(redis))
            }
        }
    }

    /// Start a connector
    async fn start_connector(
        &self,
        id: Uuid,
        mut connector: Box<dyn CdcConnector>,
        position: CdcPosition,
    ) -> Result<()> {
        // Create channels
        let (event_tx, _event_rx) = mpsc::channel::<CdcEvent>(self.config.event_buffer_size);
        let (control_tx, mut control_rx) = mpsc::channel::<ConnectorControl>(16);

        // Start connector
        connector.start(position).await?;

        // Create broadcast sender clone
        let broadcast_tx = self.event_tx.clone();
        let position_store = self.position_store.clone();
        let checkpoint_interval = self.config.checkpoint_interval;
        let max_errors = self.config.max_errors;

        // Clone event_tx for use in the task
        let task_event_tx = event_tx.clone();

        // Spawn polling task
        let task_handle = tokio::spawn(async move {
            let mut last_checkpoint = Instant::now();
            let mut error_count = 0u32;
            let mut running = true;

            while running {
                tokio::select! {
                    // Check for control messages
                    Some(control) = control_rx.recv() => {
                        match control {
                            ConnectorControl::Stop => {
                                running = false;
                            }
                            ConnectorControl::Pause => {
                                // Just stop polling but keep alive
                            }
                            ConnectorControl::Resume => {
                                // Resume polling
                            }
                            ConnectorControl::Checkpoint => {
                                let pos = connector.position();
                                if let Err(e) = position_store.save(id, &pos).await {
                                    error!(%id, error = %e, "Failed to save checkpoint");
                                }
                            }
                            ConnectorControl::Start => {}
                        }
                    }
                    // Poll for events
                    result = connector.poll() => {
                        match result {
                            Ok(events) => {
                                error_count = 0;
                                for event in events {
                                    // Broadcast to all subscribers
                                    let _ = broadcast_tx.send(event.clone());
                                    // Send to dedicated channel
                                    let _ = task_event_tx.send(event).await;
                                }
                            }
                            Err(e) => {
                                error_count += 1;
                                error!(%id, error = %e, error_count, "Connector poll error");

                                if error_count >= max_errors {
                                    error!(%id, "Max errors reached, stopping connector");
                                    running = false;
                                }

                                // Exponential backoff
                                let backoff = Duration::from_millis(100 * 2u64.pow(error_count.min(10)));
                                tokio::time::sleep(backoff).await;
                            }
                        }
                    }
                }

                // Periodic checkpoint
                if last_checkpoint.elapsed() >= checkpoint_interval {
                    let pos = connector.position();
                    if let Err(e) = position_store.save(id, &pos).await {
                        warn!(%id, error = %e, "Checkpoint save failed");
                    }
                    last_checkpoint = Instant::now();
                }
            }

            // Cleanup
            let _ = connector.stop().await;
        });

        // Store connector state
        let state = ConnectorState {
            connector: Box::new(DummyConnector), // Placeholder since real connector moved to task
            task_handle: Some(task_handle),
            event_tx,
            control_tx,
            status: SubscriptionStatus::Active,
            last_activity: Instant::now(),
            error_count: 0,
            circuit_breaker: thunder_common::circuit_breaker::CircuitBreaker::new(),
        };

        self.connectors.insert(id, state);

        Ok(())
    }

    /// Stop a subscription
    pub async fn stop_subscription(&self, id: Uuid) -> Result<()> {
        if let Some((_, mut state)) = self.connectors.remove(&id) {
            // Send stop signal
            let _ = state.control_tx.send(ConnectorControl::Stop).await;

            // Wait for task to complete
            if let Some(handle) = state.task_handle.take() {
                let _ = handle.await;
            }

            // Update subscription status
            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                sub.status = SubscriptionStatus::Stopped;
            }

            info!(%id, "Stopped CDC subscription");
        }

        Ok(())
    }

    /// Pause a subscription
    pub async fn pause_subscription(&self, id: Uuid) -> Result<()> {
        if let Some(mut state) = self.connectors.get_mut(&id) {
            let _ = state.control_tx.send(ConnectorControl::Pause).await;
            state.status = SubscriptionStatus::Paused;

            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                sub.status = SubscriptionStatus::Paused;
            }
        }
        Ok(())
    }

    /// Resume a subscription
    pub async fn resume_subscription(&self, id: Uuid) -> Result<()> {
        if let Some(mut state) = self.connectors.get_mut(&id) {
            let _ = state.control_tx.send(ConnectorControl::Resume).await;
            state.status = SubscriptionStatus::Active;

            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                sub.status = SubscriptionStatus::Active;
            }
        }
        Ok(())
    }

    /// Get subscription info
    pub fn get_subscription(&self, id: Uuid) -> Option<Subscription> {
        self.subscriptions.get(&id).map(|s| s.clone())
    }

    /// List all subscriptions
    pub fn list_subscriptions(&self) -> Vec<Subscription> {
        self.subscriptions.iter().map(|s| s.clone()).collect()
    }

    /// Subscribe to events from all connectors
    pub fn subscribe(&self) -> broadcast::Receiver<CdcEvent> {
        self.event_tx.subscribe()
    }

    /// Get event stream for a specific subscription
    pub fn subscription_stream(&self, _id: Uuid) -> Option<BoxStream<'static, CdcEvent>> {
        // In a real implementation, this would filter events by subscription
        let rx = self.event_tx.subscribe();
        Some(Box::pin(
            tokio_stream::wrappers::BroadcastStream::new(rx)
                .filter_map(|r| async { r.ok() })
        ))
    }

    /// Force checkpoint for a subscription
    pub async fn checkpoint(&self, id: Uuid) -> Result<()> {
        if let Some(state) = self.connectors.get(&id) {
            let _ = state.control_tx.send(ConnectorControl::Checkpoint).await;
        }
        Ok(())
    }

    /// Get metrics
    pub fn metrics(&self) -> &ConnectorMetrics {
        &self.metrics
    }
}

// Placeholder connector for task ownership
struct DummyConnector;

#[async_trait]
impl CdcConnector for DummyConnector {
    fn name(&self) -> &str { "dummy" }
    async fn start(&mut self, _position: CdcPosition) -> Result<()> { Ok(()) }
    async fn poll(&mut self) -> Result<Vec<CdcEvent>> { Ok(vec![]) }
    async fn ack(&mut self, _position: CdcPosition) -> Result<()> { Ok(()) }
    fn position(&self) -> CdcPosition { CdcPosition::Beginning }
    async fn stop(&mut self) -> Result<()> { Ok(()) }
}

// ============================================================================
// Position Store
// ============================================================================

/// Trait for storing CDC positions/checkpoints
#[async_trait]
pub trait PositionStore: Send + Sync {
    /// Save a position
    async fn save(&self, id: Uuid, position: &CdcPosition) -> Result<()>;

    /// Load a position
    async fn load(&self, id: Uuid) -> Result<Option<CdcPosition>>;

    /// Delete a position
    async fn delete(&self, id: Uuid) -> Result<()>;
}

/// In-memory position store (for testing)
pub struct MemoryPositionStore {
    positions: DashMap<Uuid, CdcPosition>,
}

impl MemoryPositionStore {
    pub fn new() -> Self {
        Self {
            positions: DashMap::new(),
        }
    }
}

impl Default for MemoryPositionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PositionStore for MemoryPositionStore {
    async fn save(&self, id: Uuid, position: &CdcPosition) -> Result<()> {
        self.positions.insert(id, position.clone());
        Ok(())
    }

    async fn load(&self, id: Uuid) -> Result<Option<CdcPosition>> {
        Ok(self.positions.get(&id).map(|p| p.clone()))
    }

    async fn delete(&self, id: Uuid) -> Result<()> {
        self.positions.remove(&id);
        Ok(())
    }
}

// ============================================================================
// Event Router
// ============================================================================

/// Routes CDC events to multiple destinations
pub struct EventRouter {
    /// Destinations by name
    destinations: DashMap<String, Box<dyn EventDestination>>,
    /// Routing rules
    rules: RwLock<Vec<RoutingRule>>,
    /// Metrics
    routed_count: std::sync::atomic::AtomicU64,
}

/// Event destination trait
#[async_trait]
pub trait EventDestination: Send + Sync {
    /// Destination name
    fn name(&self) -> &str;

    /// Send events
    async fn send(&self, events: &[CdcEvent]) -> Result<()>;

    /// Close destination
    async fn close(&self) -> Result<()>;
}

/// Routing rule
#[derive(Debug, Clone)]
pub struct RoutingRule {
    /// Rule name
    pub name: String,
    /// Source filter (database/table pattern)
    pub source_filter: SourceFilter,
    /// Operation filter
    pub operations: Vec<CdcOperation>,
    /// Destination names
    pub destinations: Vec<String>,
    /// Transform function name
    pub transform: Option<String>,
}

/// Source filter for routing
#[derive(Debug, Clone)]
pub struct SourceFilter {
    /// Database pattern (supports wildcards)
    pub database: String,
    /// Schema pattern (supports wildcards)
    pub schema: Option<String>,
    /// Table pattern (supports wildcards)
    pub table: String,
}

impl EventRouter {
    pub fn new() -> Self {
        Self {
            destinations: DashMap::new(),
            rules: RwLock::new(Vec::new()),
            routed_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Add a destination
    pub fn add_destination(&self, dest: Box<dyn EventDestination>) {
        let name = dest.name().to_string();
        self.destinations.insert(name, dest);
    }

    /// Remove a destination
    pub async fn remove_destination(&self, name: &str) -> Result<()> {
        if let Some((_, dest)) = self.destinations.remove(name) {
            dest.close().await?;
        }
        Ok(())
    }

    /// Add a routing rule
    pub async fn add_rule(&self, rule: RoutingRule) {
        let mut rules = self.rules.write().await;
        rules.push(rule);
    }

    /// Route an event to matching destinations
    pub async fn route(&self, event: &CdcEvent) -> Result<()> {
        let rules = self.rules.read().await;

        for rule in rules.iter() {
            if self.matches_rule(event, rule) {
                for dest_name in &rule.destinations {
                    if let Some(dest) = self.destinations.get(dest_name) {
                        dest.send(&[event.clone()]).await?;
                    }
                }
            }
        }

        self.routed_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Check if event matches routing rule
    fn matches_rule(&self, event: &CdcEvent, rule: &RoutingRule) -> bool {
        // Check operation filter
        if !rule.operations.is_empty() && !rule.operations.contains(&event.operation) {
            return false;
        }

        // Check source filter
        let filter = &rule.source_filter;

        // Database match
        if !self.pattern_matches(&filter.database, &event.database) {
            return false;
        }

        // Schema match (if applicable)
        if let Some(schema_pattern) = &filter.schema {
            if let Some(event_schema) = &event.schema {
                if !self.pattern_matches(schema_pattern, event_schema) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Table match
        if !self.pattern_matches(&filter.table, &event.table) {
            return false;
        }

        true
    }

    /// Simple wildcard pattern matching
    fn pattern_matches(&self, pattern: &str, value: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if pattern.ends_with('*') {
            let prefix = &pattern[..pattern.len() - 1];
            return value.starts_with(prefix);
        }
        pattern == value
    }
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Builtin Destinations
// ============================================================================

/// Log destination (for debugging)
pub struct LogDestination {
    name: String,
}

impl LogDestination {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[async_trait]
impl EventDestination for LogDestination {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, events: &[CdcEvent]) -> Result<()> {
        for event in events {
            info!(
                source = %event.source,
                database = %event.database,
                table = %event.table,
                operation = ?event.operation,
                "CDC Event"
            );
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Channel destination (sends to mpsc channel)
pub struct ChannelDestination {
    name: String,
    tx: mpsc::Sender<CdcEvent>,
}

impl ChannelDestination {
    pub fn new(name: impl Into<String>, tx: mpsc::Sender<CdcEvent>) -> Self {
        Self {
            name: name.into(),
            tx,
        }
    }
}

#[async_trait]
impl EventDestination for ChannelDestination {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, events: &[CdcEvent]) -> Result<()> {
        for event in events {
            self.tx.send(event.clone()).await.map_err(|e| {
                Error::Internal(format!("Failed to send to channel: {}", e))
            })?;
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_position_store() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryPositionStore::new();
            let id = Uuid::new_v4();
            let pos = CdcPosition::PostgresLsn(12345);

            store.save(id, &pos).await.unwrap();
            let loaded = store.load(id).await.unwrap();
            assert!(matches!(loaded, Some(CdcPosition::PostgresLsn(12345))));

            store.delete(id).await.unwrap();
            let deleted = store.load(id).await.unwrap();
            assert!(deleted.is_none());
        });
    }

    #[test]
    fn test_pattern_matching() {
        let router = EventRouter::new();

        assert!(router.pattern_matches("*", "anything"));
        assert!(router.pattern_matches("users", "users"));
        assert!(!router.pattern_matches("users", "orders"));
        assert!(router.pattern_matches("user*", "users"));
        assert!(router.pattern_matches("user*", "user_events"));
        assert!(!router.pattern_matches("user*", "orders"));
    }

    #[test]
    fn test_config_default() {
        let config = ConnectorManagerConfig::default();
        assert_eq!(config.max_connectors, 100);
        assert_eq!(config.event_buffer_size, 10000);
    }
}
