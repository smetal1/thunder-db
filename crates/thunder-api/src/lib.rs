//! # Thunder API
//!
//! API layer for ThunderDB providing:
//! - REST API (JSON)
//! - gRPC API
//! - GraphQL API
//! - WebSocket for real-time subscriptions
//! - Web Dashboard for administration
//! - Authentication and authorization

pub mod auth;
pub mod dashboard;
pub mod grpc;
pub mod graphql;
pub mod handlers;
pub mod rate_limit;
pub mod rest;
pub mod sql_validator;
pub mod websocket;

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::{routing::{get, post}, Router};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thunder_common::prelude::*;

pub use dashboard::dashboard_router;
pub use grpc::{GrpcServer, ThunderQueryService};
pub use graphql::{build_schema, graphql_router, ThunderSchema};
pub use rest::RestServer;
pub use websocket::{ws_router, WebSocketManager, WebSocketState};

// ============================================================================
// Query Engine Trait
// ============================================================================

/// Query result from the engine
#[derive(Debug)]
pub struct EngineQueryResult {
    /// Query ID
    pub query_id: String,
    /// Column names and types
    pub columns: Vec<(String, DataType)>,
    /// Result rows
    pub rows: Vec<Row>,
    /// Rows affected (for DML)
    pub rows_affected: Option<u64>,
    /// Execution time
    pub execution_time: Duration,
}

impl EngineQueryResult {
    pub fn empty() -> Self {
        Self {
            query_id: uuid::Uuid::new_v4().to_string(),
            columns: vec![],
            rows: vec![],
            rows_affected: None,
            execution_time: Duration::ZERO,
        }
    }
}

/// Engine statistics
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub queries_total: u64,
    pub queries_failed: u64,
    pub active_connections: u64,
    pub active_transactions: u64,
    pub buffer_pool_hit_ratio: f64,
    pub uptime_seconds: u64,
}

/// Table info from catalog
#[derive(Debug, Clone)]
pub struct CatalogTableInfo {
    pub name: String,
    pub schema: Schema,
}

/// Trait for database engines that can execute queries
/// This allows thunder-api to work with any database engine implementation
#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Create a new session and return its ID
    fn create_session(&self) -> uuid::Uuid;

    /// Close a session
    fn close_session(&self, session_id: uuid::Uuid);

    /// Execute a SQL query
    async fn execute_sql(&self, session_id: uuid::Uuid, sql: &str) -> Result<EngineQueryResult>;

    /// Begin a transaction
    async fn begin_transaction(&self, session_id: uuid::Uuid) -> Result<TxnId>;

    /// Commit a transaction
    async fn commit_transaction(&self, session_id: uuid::Uuid) -> Result<()>;

    /// Rollback a transaction
    async fn rollback_transaction(&self, session_id: uuid::Uuid) -> Result<()>;

    /// Get engine statistics
    fn stats(&self) -> EngineStats;

    /// List all tables
    fn list_tables(&self) -> Vec<String>;

    /// Get table info by name
    fn get_table(&self, name: &str) -> Option<CatalogTableInfo>;

    /// Vacuum a table to reclaim space from dead tuples
    async fn vacuum_table(&self, table_name: &str) -> Result<VacuumResult>;

    /// Create a backup of the database
    async fn backup(&self, target_dir: Option<&str>) -> Result<BackupResult>;

    /// List available backups
    fn list_backups(&self) -> Result<Vec<BackupInfo>>;
}

/// Result of a vacuum operation
#[derive(Debug, Clone, Default)]
pub struct VacuumResult {
    /// Table that was vacuumed
    pub table: String,
    /// Number of pages scanned
    pub pages_scanned: u64,
    /// Number of pages compacted
    pub pages_compacted: u64,
    /// Number of dead tuples removed
    pub tuples_removed: u64,
    /// Total bytes freed
    pub bytes_freed: u64,
}

/// Result of a backup operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupResult {
    /// Backup ID (timestamp-based)
    pub backup_id: String,
    /// Path to the backup directory
    pub backup_path: String,
    /// Timestamp of the backup
    pub timestamp: String,
    /// Size of the backup in bytes
    pub size_bytes: u64,
    /// LSN at time of backup
    pub lsn: u64,
}

/// Request for a backup operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRequest {
    /// Target directory for the backup (optional, defaults to data_dir/backups/<timestamp>)
    pub target_dir: Option<String>,
}

/// Request for a restore operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreRequest {
    /// Path to the backup to restore from
    pub backup_path: String,
}

/// Information about an existing backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    pub backup_id: String,
    pub backup_path: String,
    pub timestamp: String,
    pub size_bytes: u64,
}

// ============================================================================
// Application State
// ============================================================================

/// Application state for API handlers
#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    start_time: Instant,
    metrics: RwLock<ServerMetrics>,
    /// Database engine (optional - can be None for testing)
    engine: Option<Arc<dyn QueryEngine>>,
    /// Session mapping: API session ID -> Engine session ID
    sessions: RwLock<std::collections::HashMap<String, uuid::Uuid>>,
    /// Prometheus-compatible database metrics
    db_metrics: Arc<thunder_common::metrics::DatabaseMetrics>,
}

impl AppState {
    /// Create a new AppState without an engine (for testing)
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AppStateInner {
                start_time: Instant::now(),
                metrics: RwLock::new(ServerMetrics::default()),
                engine: None,
                sessions: RwLock::new(std::collections::HashMap::new()),
                db_metrics: Arc::new(thunder_common::metrics::DatabaseMetrics::new()),
            }),
        }
    }

    /// Create a new AppState with a database engine
    pub fn with_engine(engine: Arc<dyn QueryEngine>) -> Self {
        Self {
            inner: Arc::new(AppStateInner {
                start_time: Instant::now(),
                metrics: RwLock::new(ServerMetrics::default()),
                engine: Some(engine),
                sessions: RwLock::new(std::collections::HashMap::new()),
                db_metrics: Arc::new(thunder_common::metrics::DatabaseMetrics::new()),
            }),
        }
    }

    /// Get the database engine
    pub fn engine(&self) -> Option<&Arc<dyn QueryEngine>> {
        self.inner.engine.as_ref()
    }

    /// Get or create a session for API requests
    pub fn get_or_create_session(&self) -> Option<uuid::Uuid> {
        self.inner.engine.as_ref().map(|engine| engine.create_session())
    }

    /// Get a session by API session ID
    pub fn get_session(&self, api_session_id: &str) -> Option<uuid::Uuid> {
        self.inner.sessions.read().get(api_session_id).copied()
    }

    /// Register an API session mapping
    pub fn register_session(&self, api_session_id: String, engine_session_id: uuid::Uuid) {
        self.inner.sessions.write().insert(api_session_id, engine_session_id);
    }

    /// Remove a session mapping
    pub fn remove_session(&self, api_session_id: &str) -> Option<uuid::Uuid> {
        self.inner.sessions.write().remove(api_session_id)
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.inner.start_time.elapsed().as_secs()
    }

    pub fn metrics(&self) -> ServerMetrics {
        self.inner.metrics.read().clone()
    }

    pub fn record_query(&self, success: bool, duration_ms: u64) {
        let mut metrics = self.inner.metrics.write();
        metrics.queries_total += 1;
        if !success {
            metrics.queries_failed += 1;
        }
        metrics.total_query_time_ms += duration_ms;
    }

    pub fn record_connection(&self, connected: bool) {
        let mut metrics = self.inner.metrics.write();
        if connected {
            metrics.active_connections += 1;
        } else if metrics.active_connections > 0 {
            metrics.active_connections -= 1;
        }
    }

    pub fn record_transaction(&self, active: bool) {
        let mut metrics = self.inner.metrics.write();
        if active {
            metrics.active_transactions += 1;
        } else if metrics.active_transactions > 0 {
            metrics.active_transactions -= 1;
        }
    }

    /// Get the Prometheus-compatible database metrics
    pub fn db_metrics(&self) -> &Arc<thunder_common::metrics::DatabaseMetrics> {
        &self.inner.db_metrics
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

/// Server metrics
#[derive(Debug, Clone, Default)]
pub struct ServerMetrics {
    pub queries_total: u64,
    pub queries_failed: u64,
    pub active_connections: u64,
    pub active_transactions: u64,
    pub total_query_time_ms: u64,
}

// ============================================================================
// API Configuration
// ============================================================================

/// API server configuration
#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub http_port: u16,
    pub grpc_port: u16,
    pub enable_graphql: bool,
    pub enable_websocket: bool,
    pub cors_enabled: bool,
    /// CORS allowed origins. Empty = allow all (dev mode, emits warning).
    pub cors_allowed_origins: Vec<String>,
    pub max_request_size: usize,
    pub request_timeout_ms: u64,
    pub enable_compression: bool,
    pub enable_metrics: bool,
    /// Rate limit: requests per second per IP. 0 = disabled.
    pub rate_limit_rps: f64,
    /// Rate limit: burst capacity per IP.
    pub rate_limit_burst: usize,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            http_port: 8080,
            grpc_port: 9090,
            enable_graphql: true,
            enable_websocket: true,
            cors_enabled: true,
            cors_allowed_origins: Vec::new(),
            max_request_size: 10 * 1024 * 1024, // 10MB
            request_timeout_ms: 30000,
            enable_compression: true,
            enable_metrics: true,
            rate_limit_rps: 100.0,
            rate_limit_burst: 200,
        }
    }
}

impl ApiConfig {
    pub fn builder() -> ApiConfigBuilder {
        ApiConfigBuilder::default()
    }
}

/// Builder for API configuration
#[derive(Debug, Clone, Default)]
pub struct ApiConfigBuilder {
    config: ApiConfig,
}

impl ApiConfigBuilder {
    pub fn http_port(mut self, port: u16) -> Self {
        self.config.http_port = port;
        self
    }

    pub fn grpc_port(mut self, port: u16) -> Self {
        self.config.grpc_port = port;
        self
    }

    pub fn enable_graphql(mut self, enable: bool) -> Self {
        self.config.enable_graphql = enable;
        self
    }

    pub fn enable_websocket(mut self, enable: bool) -> Self {
        self.config.enable_websocket = enable;
        self
    }

    pub fn cors_enabled(mut self, enable: bool) -> Self {
        self.config.cors_enabled = enable;
        self
    }

    pub fn max_request_size(mut self, size: usize) -> Self {
        self.config.max_request_size = size;
        self
    }

    pub fn request_timeout_ms(mut self, timeout: u64) -> Self {
        self.config.request_timeout_ms = timeout;
        self
    }

    pub fn build(self) -> ApiConfig {
        self.config
    }
}

// ============================================================================
// Request/Response Types
// ============================================================================

/// Query request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
    pub timeout_ms: Option<u64>,
    pub transaction_id: Option<String>,
}

/// Query response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub query_id: String,
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub rows_affected: Option<u64>,
    pub execution_time_ms: u64,
}

/// Column information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Transaction request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRequest {
    pub isolation_level: Option<String>,
    #[serde(default)]
    pub read_only: bool,
}

/// Transaction response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResponse {
    pub transaction_id: String,
}

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
    #[serde(default)]
    pub database_ok: bool,
    #[serde(default)]
    pub storage_ok: bool,
    #[serde(default)]
    pub disk_ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Metrics response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub queries_total: u64,
    pub queries_failed: u64,
    pub active_connections: u64,
    pub active_transactions: u64,
    pub buffer_pool_hit_ratio: f64,
    pub average_query_time_ms: f64,
}

/// API error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlstate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ApiError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            sqlstate: None,
            details: None,
        }
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn with_sqlstate(mut self, sqlstate: impl Into<String>) -> Self {
        self.sqlstate = Some(sqlstate.into());
        self
    }

    /// Create an ApiError from a thunder-common Error, including SQLSTATE.
    pub fn from_engine_error(err: &thunder_common::error::Error) -> Self {
        let code = match err {
            thunder_common::error::Error::Sql(_) => "SQL_ERROR",
            thunder_common::error::Error::Query(_) => "QUERY_ERROR",
            thunder_common::error::Error::Transaction(_) => "TRANSACTION_ERROR",
            thunder_common::error::Error::Storage(_) => "STORAGE_ERROR",
            thunder_common::error::Error::PermissionDenied(_) => "PERMISSION_DENIED",
            thunder_common::error::Error::NotFound(_, _) => "NOT_FOUND",
            thunder_common::error::Error::AlreadyExists(_, _) => "ALREADY_EXISTS",
            thunder_common::error::Error::Timeout(_) => "TIMEOUT",
            thunder_common::error::Error::InvalidArgument(_) => "INVALID_REQUEST",
            _ => "INTERNAL_ERROR",
        };

        Self {
            code: code.to_string(),
            message: err.to_string(),
            sqlstate: Some(err.sqlstate().to_string()),
            details: None,
        }
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new("INVALID_REQUEST", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new("NOT_FOUND", message)
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new("INTERNAL_ERROR", message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new("UNAUTHORIZED", message)
    }

    pub fn query_error(message: impl Into<String>) -> Self {
        Self::new("QUERY_ERROR", message)
    }

    pub fn transaction_error(message: impl Into<String>) -> Self {
        Self::new("TRANSACTION_ERROR", message)
    }
}

// ============================================================================
// Router Construction
// ============================================================================

/// Create the REST API router with the given application state
pub fn create_rest_router(state: AppState) -> Router {
    Router::new()
        // Query endpoints
        .route("/api/v1/query", post(handlers::execute_query))
        // Transaction endpoints
        .route("/api/v1/transactions", post(handlers::begin_transaction))
        .route("/api/v1/transactions/:id/commit", post(handlers::commit_transaction))
        .route("/api/v1/transactions/:id/rollback", post(handlers::rollback_transaction))
        // Table endpoints
        .route("/api/v1/tables", get(handlers::list_tables))
        .route("/api/v1/tables/:name", get(handlers::get_table))
        // Admin endpoints
        .route("/api/v1/health", get(handlers::health_check))
        .route("/api/v1/metrics", get(handlers::get_metrics))
        // CDC endpoints
        .route("/api/v1/cdc/subscriptions", get(handlers::list_subscriptions))
        .route("/api/v1/cdc/subscriptions", post(handlers::create_subscription))
        // FDW endpoints
        .route("/api/v1/servers", get(handlers::list_foreign_servers))
        .route("/api/v1/servers", post(handlers::create_foreign_server))
        .with_state(state)
}

/// Create a combined API server with all protocols
pub struct ApiServer {
    config: ApiConfig,
    state: AppState,
}

impl ApiServer {
    pub fn new(config: ApiConfig, state: AppState) -> Self {
        Self { config, state }
    }

    /// Start all API servers
    pub async fn serve(self) -> std::io::Result<()> {
        let rest_server = RestServer::new(self.config.clone(), self.state.clone());

        // Start REST server
        rest_server.serve().await
    }

    /// Get the REST router
    pub fn rest_router(&self) -> Router {
        RestServer::new(self.config.clone(), self.state.clone()).router()
    }

    /// Get the GraphQL router
    pub fn graphql_router(&self) -> Router {
        graphql_router(self.state.clone())
    }

    /// Get the WebSocket router
    pub fn websocket_router(&self) -> Router {
        ws_router(self.state.clone())
    }

    /// Get the Dashboard router
    pub fn dashboard_router(&self) -> Router {
        dashboard_router(self.state.clone())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_request() {
        let json = r#"{"sql": "SELECT * FROM users", "params": []}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.sql, "SELECT * FROM users");
    }

    #[test]
    fn test_api_error() {
        let err = ApiError::new("INVALID_SQL", "Syntax error");
        assert_eq!(err.code, "INVALID_SQL");
    }

    #[test]
    fn test_api_error_helpers() {
        let err = ApiError::invalid_request("Bad input");
        assert_eq!(err.code, "INVALID_REQUEST");

        let err = ApiError::not_found("Resource not found");
        assert_eq!(err.code, "NOT_FOUND");
    }

    #[test]
    fn test_app_state() {
        let state = AppState::new();
        assert!(state.uptime_seconds() < 1);
    }

    #[test]
    fn test_app_state_metrics() {
        let state = AppState::new();
        state.record_query(true, 10);
        state.record_query(false, 20);

        let metrics = state.metrics();
        assert_eq!(metrics.queries_total, 2);
        assert_eq!(metrics.queries_failed, 1);
    }

    #[test]
    fn test_api_config_builder() {
        let config = ApiConfig::builder()
            .http_port(3000)
            .grpc_port(9000)
            .enable_graphql(false)
            .build();

        assert_eq!(config.http_port, 3000);
        assert_eq!(config.grpc_port, 9000);
        assert!(!config.enable_graphql);
    }

    #[test]
    fn test_default_config() {
        let config = ApiConfig::default();
        assert_eq!(config.http_port, 8080);
        assert_eq!(config.grpc_port, 9090);
        assert!(config.enable_graphql);
        assert!(config.enable_websocket);
    }
}
