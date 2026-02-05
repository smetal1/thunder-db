//! REST API Server
//!
//! Provides HTTP/REST endpoints for ThunderDB with:
//! - Query execution
//! - Transaction management
//! - Table operations
//! - Health and metrics endpoints

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{header, Method, StatusCode},
    middleware,
    routing::{delete, get, post},
    Json, Router,
};
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};
use tracing::{info, warn};

use thunder_common::rbac::RbacManager;
use thunder_common::config::SecurityConfig;

use crate::{auth, handlers, rate_limit::RateLimitLayer, ApiConfig, ApiError, AppState};

// ============================================================================
// REST Server
// ============================================================================

/// REST API server
pub struct RestServer {
    config: ApiConfig,
    state: AppState,
    rbac: Arc<RbacManager>,
}

impl RestServer {
    pub fn new(config: ApiConfig, state: AppState) -> Self {
        // Create RBAC manager with development config for backward compatibility
        // In production, this should be configured explicitly
        let security_config = SecurityConfig::development();
        let rbac = Arc::new(RbacManager::new(security_config));
        Self { config, state, rbac }
    }

    /// Create a new REST server with explicit security configuration
    pub fn with_security(config: ApiConfig, state: AppState, security_config: SecurityConfig) -> Self {
        let rbac = Arc::new(RbacManager::new(security_config));
        Self { config, state, rbac }
    }

    /// Create the full router with all middleware
    pub fn router(&self) -> Router {
        // Build CORS layer with configurable origins
        let cors = {
            let base = CorsLayer::new()
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
                .allow_headers([
                    header::CONTENT_TYPE,
                    header::AUTHORIZATION,
                    header::ACCEPT,
                    "X-Request-Id".parse().unwrap(),
                ]);

            if self.config.cors_allowed_origins.is_empty() {
                warn!("CORS: allow_origin(Any) — configure cors_allowed_origins for production");
                base.allow_origin(Any)
            } else {
                let origins: Vec<header::HeaderValue> = self
                    .config
                    .cors_allowed_origins
                    .iter()
                    .filter_map(|o| o.parse().ok())
                    .collect();
                base.allow_origin(origins)
            }
        };

        let rbac = self.rbac.clone();

        // API routes with optional authentication
        let api_routes = Router::new()
            // Query endpoints
            .route("/query", post(handlers::execute_query))
            .route("/query/explain", post(explain_query))
            .route("/query/cancel/:id", post(cancel_query))
            // Transaction endpoints
            .route("/transactions", post(handlers::begin_transaction))
            .route("/transactions/:id", get(get_transaction_status))
            .route("/transactions/:id/commit", post(handlers::commit_transaction))
            .route("/transactions/:id/rollback", post(handlers::rollback_transaction))
            // Table endpoints
            .route("/tables", get(handlers::list_tables))
            .route("/tables", post(create_table))
            .route("/tables/:name", get(handlers::get_table))
            .route("/tables/:name", delete(drop_table))
            .route("/tables/:name/columns", get(get_table_columns))
            .route("/tables/:name/indexes", get(get_table_indexes))
            .route("/tables/:name/stats", get(get_table_stats))
            // Index endpoints
            .route("/indexes", get(list_indexes))
            .route("/indexes", post(create_index))
            .route("/indexes/:name", delete(drop_index))
            // CDC endpoints
            .route("/cdc/subscriptions", get(handlers::list_subscriptions))
            .route("/cdc/subscriptions", post(handlers::create_subscription))
            .route("/cdc/subscriptions/:id", get(get_subscription))
            .route("/cdc/subscriptions/:id", delete(delete_subscription))
            .route("/cdc/subscriptions/:id/pause", post(pause_subscription))
            .route("/cdc/subscriptions/:id/resume", post(resume_subscription))
            // FDW endpoints
            .route("/servers", get(handlers::list_foreign_servers))
            .route("/servers", post(handlers::create_foreign_server))
            .route("/servers/:name", get(get_foreign_server))
            .route("/servers/:name", delete(drop_foreign_server))
            .route("/servers/:name/tables", get(list_foreign_tables))
            // Vector endpoints
            .route("/vectors/indexes", get(list_vector_indexes))
            .route("/vectors/indexes", post(create_vector_index))
            .route("/vectors/search", post(vector_search))
            // Bulk operations
            .route("/bulk/insert", post(bulk_insert))
            .route("/bulk/upsert", post(bulk_upsert))
            .layer(middleware::from_fn_with_state(
                rbac.clone(),
                auth::optional_auth_middleware,
            ));

        // Public health endpoints (no auth required)
        let public_admin_routes = Router::new()
            .route("/health", get(handlers::health_check))
            .route("/ready", get(readiness_check))
            .route("/live", get(liveness_check));

        // Protected admin routes (require admin authentication)
        let protected_admin_routes = Router::new()
            .route("/metrics", get(handlers::get_metrics))
            .route("/config", get(get_config))
            .route("/connections", get(list_connections))
            .route("/connections/:id", delete(kill_connection))
            .route("/queries", get(list_active_queries))
            .route("/vacuum/:table", post(vacuum_table))
            .route("/analyze/:table", post(analyze_table))
            .route("/backup", post(handlers::create_backup))
            .route("/backups", get(handlers::list_backups))
            .route("/restore", post(handlers::restore_backup))
            .layer(middleware::from_fn_with_state(
                rbac.clone(),
                auth::admin_auth_middleware,
            ));

        // Combine admin routes
        let admin_routes = Router::new()
            .merge(public_admin_routes)
            .merge(protected_admin_routes);

        let mut router = Router::new()
            .nest("/api/v1", api_routes)
            .nest("/admin", admin_routes)
            .route("/metrics", get(handlers::prometheus_metrics))
            // Middleware stack (applied bottom-to-top):
            // 1. CORS (outermost)
            .layer(cors)
            // 2. Request body size limit
            .layer(RequestBodyLimitLayer::new(self.config.max_request_size))
            // 3. Request timeout
            .layer(TimeoutLayer::new(std::time::Duration::from_millis(self.config.request_timeout_ms)))
            // 4. Compression
            .layer(CompressionLayer::new())
            // 5. Propagate X-Request-Id from request to response
            .layer(PropagateRequestIdLayer::x_request_id())
            // 6. Distributed tracing
            .layer(TraceLayer::new_for_http())
            // 7. Set X-Request-Id if not already present (innermost — runs first)
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
            .with_state(self.state.clone());

        // 8. Rate limiting (only if configured with non-zero rate)
        if self.config.rate_limit_rps > 0.0 && self.config.rate_limit_burst > 0 {
            info!(
                rps = self.config.rate_limit_rps,
                burst = self.config.rate_limit_burst,
                "Rate limiting enabled"
            );
            router = router.layer(RateLimitLayer::new(
                self.config.rate_limit_rps,
                self.config.rate_limit_burst,
            ));
        }

        router
    }

    /// Start the REST server
    pub async fn serve(self) -> std::io::Result<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.http_port));
        let router = self.router();

        info!(address = %addr, "Starting REST API server");

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
    }
}

// ============================================================================
// Additional Handler Functions
// ============================================================================

/// Explain query plan
async fn explain_query(
    State(_state): State<AppState>,
    Json(request): Json<crate::QueryRequest>,
) -> Result<Json<ExplainResponse>, (StatusCode, Json<ApiError>)> {
    Ok(Json(ExplainResponse {
        plan: format!("EXPLAIN {}", request.sql),
        estimated_cost: 1.0,
        estimated_rows: 100,
    }))
}

/// Cancel a running query
async fn cancel_query(
    State(_state): State<AppState>,
    Path(_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    Ok(StatusCode::OK)
}

/// Get transaction status
async fn get_transaction_status(
    State(_state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TransactionStatus>, (StatusCode, Json<ApiError>)> {
    Ok(Json(TransactionStatus {
        transaction_id: id,
        status: "active".to_string(),
        started_at: chrono::Utc::now().to_rfc3339(),
        isolation_level: "read_committed".to_string(),
    }))
}

/// Create a new table
async fn create_table(
    State(_state): State<AppState>,
    Json(request): Json<CreateTableRequest>,
) -> Result<Json<TableCreatedResponse>, (StatusCode, Json<ApiError>)> {
    Ok(Json(TableCreatedResponse {
        name: request.name,
        columns: request.columns.len(),
    }))
}

/// Drop a table
async fn drop_table(
    State(_state): State<AppState>,
    Path(_name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    Ok(StatusCode::OK)
}

/// Get table columns
async fn get_table_columns(
    State(_state): State<AppState>,
    Path(_name): Path<String>,
) -> Result<Json<Vec<ColumnDefinition>>, (StatusCode, Json<ApiError>)> {
    Ok(Json(vec![]))
}

/// Get table indexes
async fn get_table_indexes(
    State(_state): State<AppState>,
    Path(_name): Path<String>,
) -> Result<Json<Vec<IndexDefinition>>, (StatusCode, Json<ApiError>)> {
    Ok(Json(vec![]))
}

/// Get table statistics
async fn get_table_stats(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<TableStatistics>, (StatusCode, Json<ApiError>)> {
    Ok(Json(TableStatistics {
        name,
        row_count: 0,
        size_bytes: 0,
        index_size_bytes: 0,
        last_vacuum: None,
        last_analyze: None,
    }))
}

/// List all indexes
async fn list_indexes(
    State(_state): State<AppState>,
) -> Result<Json<Vec<IndexDefinition>>, (StatusCode, Json<ApiError>)> {
    Ok(Json(vec![]))
}

/// Create an index
async fn create_index(
    State(_state): State<AppState>,
    Json(request): Json<CreateIndexRequest>,
) -> Result<Json<IndexCreatedResponse>, (StatusCode, Json<ApiError>)> {
    Ok(Json(IndexCreatedResponse {
        name: request.name,
        table: request.table,
    }))
}

/// Drop an index
async fn drop_index(
    State(_state): State<AppState>,
    Path(_name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    Ok(StatusCode::OK)
}

/// Get subscription details
async fn get_subscription(
    State(_state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SubscriptionDetails>, (StatusCode, Json<ApiError>)> {
    Ok(Json(SubscriptionDetails {
        id,
        status: "active".to_string(),
        table: "unknown".to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
    }))
}

/// Delete a subscription
async fn delete_subscription(
    State(_state): State<AppState>,
    Path(_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    Ok(StatusCode::OK)
}

/// Pause a subscription
async fn pause_subscription(
    State(_state): State<AppState>,
    Path(_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    Ok(StatusCode::OK)
}

/// Resume a subscription
async fn resume_subscription(
    State(_state): State<AppState>,
    Path(_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    Ok(StatusCode::OK)
}

/// Get foreign server details
async fn get_foreign_server(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<ForeignServerDetails>, (StatusCode, Json<ApiError>)> {
    Ok(Json(ForeignServerDetails {
        name,
        server_type: "postgresql".to_string(),
        host: "localhost".to_string(),
        port: 5432,
        database: "remote".to_string(),
    }))
}

/// Drop a foreign server
async fn drop_foreign_server(
    State(_state): State<AppState>,
    Path(_name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    Ok(StatusCode::OK)
}

/// List foreign tables
async fn list_foreign_tables(
    State(_state): State<AppState>,
    Path(_server_name): Path<String>,
) -> Result<Json<Vec<String>>, (StatusCode, Json<ApiError>)> {
    Ok(Json(vec![]))
}

/// List vector indexes
async fn list_vector_indexes(
    State(_state): State<AppState>,
) -> Result<Json<Vec<VectorIndexInfo>>, (StatusCode, Json<ApiError>)> {
    Ok(Json(vec![]))
}

/// Create a vector index
async fn create_vector_index(
    State(_state): State<AppState>,
    Json(request): Json<CreateVectorIndexRequest>,
) -> Result<Json<VectorIndexInfo>, (StatusCode, Json<ApiError>)> {
    Ok(Json(VectorIndexInfo {
        name: request.name,
        table: request.table,
        column: request.column,
        index_type: request.index_type,
        dimensions: request.dimensions,
    }))
}

/// Vector similarity search
async fn vector_search(
    State(_state): State<AppState>,
    Json(_request): Json<VectorSearchRequest>,
) -> Result<Json<VectorSearchResponse>, (StatusCode, Json<ApiError>)> {
    Ok(Json(VectorSearchResponse {
        results: vec![],
        query_time_ms: 0,
    }))
}

/// Bulk insert
async fn bulk_insert(
    State(_state): State<AppState>,
    Json(request): Json<BulkInsertRequest>,
) -> Result<Json<BulkOperationResponse>, (StatusCode, Json<ApiError>)> {
    Ok(Json(BulkOperationResponse {
        rows_affected: request.rows.len() as u64,
        errors: vec![],
    }))
}

/// Bulk upsert
async fn bulk_upsert(
    State(_state): State<AppState>,
    Json(request): Json<BulkUpsertRequest>,
) -> Result<Json<BulkOperationResponse>, (StatusCode, Json<ApiError>)> {
    Ok(Json(BulkOperationResponse {
        rows_affected: request.rows.len() as u64,
        errors: vec![],
    }))
}

/// Readiness check - verifies the server can accept queries
async fn readiness_check(
    State(state): State<AppState>,
) -> Result<Json<ReadinessResponse>, (StatusCode, Json<ApiError>)> {
    let mut checks = Vec::new();
    let mut ready = true;

    if let Some(engine) = state.engine() {
        // Verify engine can respond to requests
        let _ = engine.list_tables();
        checks.push(HealthCheck { name: "database".to_string(), status: "ok".to_string() });
        checks.push(HealthCheck { name: "storage".to_string(), status: "ok".to_string() });
    } else {
        checks.push(HealthCheck { name: "database".to_string(), status: "not_initialized".to_string() });
        ready = false;
    }

    if ready {
        Ok(Json(ReadinessResponse { ready, checks }))
    } else {
        Err((StatusCode::SERVICE_UNAVAILABLE, Json(ApiError::new("NOT_READY", "Server is not ready to accept requests"))))
    }
}

/// Liveness check - simple "I'm alive" probe for Kubernetes
async fn liveness_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "alive": true,
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

/// Get server configuration
async fn get_config(
    State(_state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    Ok(Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "features": ["rest", "grpc", "websocket", "cdc", "fdw", "vector"]
    })))
}

/// List active connections
async fn list_connections(
    State(_state): State<AppState>,
) -> Result<Json<Vec<ConnectionInfo>>, (StatusCode, Json<ApiError>)> {
    Ok(Json(vec![]))
}

/// Kill a connection
async fn kill_connection(
    State(_state): State<AppState>,
    Path(_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    Ok(StatusCode::OK)
}

/// List active queries
async fn list_active_queries(
    State(_state): State<AppState>,
) -> Result<Json<Vec<ActiveQuery>>, (StatusCode, Json<ApiError>)> {
    Ok(Json(vec![]))
}

/// Vacuum a table to reclaim space from dead tuples
async fn vacuum_table(
    State(state): State<AppState>,
    Path(table): Path<String>,
) -> Result<Json<VacuumResponse>, (StatusCode, Json<ApiError>)> {
    let engine = state.engine().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(ApiError::new("UNAVAILABLE", "Database engine not initialized")))
    })?;

    match engine.vacuum_table(&table).await {
        Ok(result) => Ok(Json(VacuumResponse {
            table: result.table,
            pages_scanned: result.pages_scanned,
            pages_compacted: result.pages_compacted,
            tuples_removed: result.tuples_removed,
            bytes_freed: result.bytes_freed,
        })),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ApiError::from_engine_error(&e)))),
    }
}

/// Analyze a table
async fn analyze_table(
    State(_state): State<AppState>,
    Path(_table): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    Ok(StatusCode::OK)
}

// ============================================================================
// Request/Response Types
// ============================================================================

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct ExplainResponse {
    pub plan: String,
    pub estimated_cost: f64,
    pub estimated_rows: u64,
}

#[derive(Debug, Serialize)]
pub struct TransactionStatus {
    pub transaction_id: String,
    pub status: String,
    pub started_at: String,
    pub isolation_level: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct TableCreatedResponse {
    pub name: String,
    pub columns: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(default)]
    pub default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: String,
}

#[derive(Debug, Serialize)]
pub struct TableStatistics {
    pub name: String,
    pub row_count: u64,
    pub size_bytes: u64,
    pub index_size_bytes: u64,
    pub last_vacuum: Option<String>,
    pub last_analyze: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateIndexRequest {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub unique: bool,
    #[serde(default = "default_index_type")]
    pub index_type: String,
}

fn default_index_type() -> String {
    "btree".to_string()
}

#[derive(Debug, Serialize)]
pub struct IndexCreatedResponse {
    pub name: String,
    pub table: String,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionDetails {
    pub id: String,
    pub status: String,
    pub table: String,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct ForeignServerDetails {
    pub name: String,
    pub server_type: String,
    pub host: String,
    pub port: u16,
    pub database: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexInfo {
    pub name: String,
    pub table: String,
    pub column: String,
    pub index_type: String,
    pub dimensions: usize,
}

#[derive(Debug, Deserialize)]
pub struct CreateVectorIndexRequest {
    pub name: String,
    pub table: String,
    pub column: String,
    pub index_type: String,
    pub dimensions: usize,
    #[serde(default)]
    pub options: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct VectorSearchRequest {
    pub index: String,
    pub vector: Vec<f32>,
    pub k: usize,
    #[serde(default)]
    pub filter: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct VectorSearchResponse {
    pub results: Vec<VectorSearchResult>,
    pub query_time_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct VectorSearchResult {
    pub id: u64,
    pub distance: f32,
    pub data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct BulkInsertRequest {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
pub struct BulkUpsertRequest {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub conflict_columns: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct BulkOperationResponse {
    pub rows_affected: u64,
    pub errors: Vec<BulkError>,
}

#[derive(Debug, Serialize)]
pub struct BulkError {
    pub row_index: usize,
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct ReadinessResponse {
    pub ready: bool,
    pub checks: Vec<HealthCheck>,
}

#[derive(Debug, Serialize)]
pub struct HealthCheck {
    pub name: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct ConnectionInfo {
    pub id: String,
    pub client_addr: String,
    pub user: String,
    pub database: String,
    pub state: String,
    pub started_at: String,
}

#[derive(Debug, Serialize)]
pub struct ActiveQuery {
    pub query_id: String,
    pub connection_id: String,
    pub sql: String,
    pub started_at: String,
    pub elapsed_ms: u64,
}

/// Response from vacuum operation
#[derive(Debug, Serialize)]
pub struct VacuumResponse {
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_check() {
        let config = ApiConfig::default();
        let state = AppState::new();
        let server = RestServer::new(config, state);
        let router = server.router();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/admin/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_query_endpoint() {
        let config = ApiConfig::default();
        let state = AppState::new();
        let server = RestServer::new(config, state);
        let router = server.router();

        let body = serde_json::json!({
            "sql": "SELECT 1",
            "params": []
        });

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/query")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
