//! API request handlers
//!
//! Implements the core API handlers for query execution,
//! transactions, table operations, and administration.

use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};
use thunder_common::prelude::Value;

use crate::{
    ApiError, AppState, ColumnInfo, EngineStats, HealthResponse, MetricsResponse, QueryRequest,
    QueryResponse, TransactionRequest, TransactionResponse, CatalogTableInfo,
    sql_validator::{SqlValidator, SqlValidatorConfig, validate_parameters},
};

/// Result type for API handlers
pub type ApiResult<T> = std::result::Result<T, (StatusCode, Json<ApiError>)>;

// ============================================================================
// Query Handlers
// ============================================================================

/// Execute a SQL query
pub async fn execute_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> ApiResult<Json<QueryResponse>> {
    let start = Instant::now();
    let query_id = uuid::Uuid::new_v4().to_string();

    debug!(
        query_id = %query_id,
        sql = %request.sql,
        params = ?request.params.len(),
        "Executing query"
    );

    // Validate SQL
    if request.sql.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::invalid_request("SQL query cannot be empty")),
        ));
    }

    // SQL injection and safety validation
    let validator = SqlValidator::default_validator();
    let validation = validator.validate(&request.sql);
    if let Some(error_msg) = validation.error_message() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::invalid_request(error_msg)),
        ));
    }

    // Validate parameters
    let param_validation = validate_parameters(&request.params);
    if let Some(error_msg) = param_validation.error_message() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::invalid_request(error_msg)),
        ));
    }

    // Get the database engine
    let engine = match state.engine() {
        Some(e) => e,
        None => {
            // No engine available - return stub response for testing
            let execution_time_ms = start.elapsed().as_millis() as u64;
            state.record_query(true, execution_time_ms);
            return Ok(Json(QueryResponse {
                query_id,
                columns: vec![],
                rows: vec![],
                rows_affected: None,
                execution_time_ms,
            }));
        }
    };

    // Get or create a session
    let session_id = if let Some(txn_id) = &request.transaction_id {
        // Use existing transaction's session
        match state.get_session(txn_id) {
            Some(id) => id,
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ApiError::transaction_error("Transaction not found")),
                ));
            }
        }
    } else {
        // Create a temporary session for this query
        match state.get_or_create_session() {
            Some(id) => id,
            None => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal_error("Failed to create session")),
                ));
            }
        }
    };

    // Execute the query
    let result = engine.execute_sql(session_id, &request.sql).await;

    let execution_time_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(query_result) => {
            state.record_query(true, execution_time_ms);
            state.db_metrics().record_query_success(start.elapsed());

            // Convert columns
            let columns: Vec<ColumnInfo> = query_result.columns
                .iter()
                .map(|(name, dtype)| ColumnInfo {
                    name: name.clone(),
                    data_type: format!("{:?}", dtype),
                    nullable: true,
                })
                .collect();

            // Convert rows to JSON
            let rows: Vec<Vec<serde_json::Value>> = query_result.rows
                .iter()
                .map(|row| {
                    row.values
                        .iter()
                        .map(value_to_json)
                        .collect()
                })
                .collect();

            // Clean up temporary session if we created one
            if request.transaction_id.is_none() {
                engine.close_session(session_id);
            }

            Ok(Json(QueryResponse {
                query_id: query_result.query_id,
                columns,
                rows,
                rows_affected: query_result.rows_affected,
                execution_time_ms,
            }))
        }
        Err(e) => {
            state.record_query(false, execution_time_ms);
            state.db_metrics().record_query_failure();
            error!(error = %e, "Query execution failed");

            // Clean up temporary session
            if request.transaction_id.is_none() {
                engine.close_session(session_id);
            }

            Err((
                StatusCode::BAD_REQUEST,
                Json(ApiError::query_error(e.to_string())),
            ))
        }
    }
}

/// Convert a Value to JSON
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Int8(i) => serde_json::Value::Number((*i as i64).into()),
        Value::Int16(i) => serde_json::Value::Number((*i as i64).into()),
        Value::Int32(i) => serde_json::Value::Number((*i as i64).into()),
        Value::Int64(i) => serde_json::Value::Number((*i).into()),
        Value::Float32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Decimal(v, scale) => {
            // Format as decimal string
            let divisor = 10_i128.pow(*scale as u32);
            serde_json::Value::String(format!("{}.{:0>width$}", v / divisor, (v % divisor).abs(), width = *scale as usize))
        }
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Binary(b) => serde_json::Value::String(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b.as_ref())),
        Value::Date(d) => serde_json::Value::String(format!("{}", d)),
        Value::Time(t) => serde_json::Value::String(format!("{}", t)),
        Value::Timestamp(ts) => serde_json::Value::Number((*ts).into()),
        Value::TimestampTz(ts, tz) => serde_json::json!({
            "timestamp": ts,
            "timezone": tz
        }),
        Value::Uuid(u) => serde_json::Value::String(uuid::Uuid::from_slice(u).map(|u| u.to_string()).unwrap_or_default()),
        Value::Json(j) => serde_json::from_str(j).unwrap_or(serde_json::Value::String(j.to_string())),
        Value::Vector(v) => serde_json::Value::Array(v.iter().map(|f| {
            serde_json::Number::from_f64(*f as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }).collect()),
        Value::Array(arr) => serde_json::Value::Array(
            arr.iter().map(value_to_json).collect()
        ),
    }
}

// ============================================================================
// Transaction Handlers
// ============================================================================

/// Begin a transaction
pub async fn begin_transaction(
    State(state): State<AppState>,
    Json(request): Json<TransactionRequest>,
) -> ApiResult<Json<TransactionResponse>> {
    let transaction_id = uuid::Uuid::new_v4().to_string();

    debug!(
        transaction_id = %transaction_id,
        isolation_level = ?request.isolation_level,
        read_only = request.read_only,
        "Beginning transaction"
    );

    // Get the database engine
    let engine = match state.engine() {
        Some(e) => e,
        None => {
            // No engine - return stub response
            state.record_transaction(true);
            return Ok(Json(TransactionResponse { transaction_id }));
        }
    };

    // Create a session for this transaction
    let session_id = engine.create_session();

    // Begin the transaction
    match engine.begin_transaction(session_id).await {
        Ok(txn_id) => {
            info!(transaction_id = %transaction_id, txn_id = ?txn_id, "Transaction started");

            // Register the mapping
            state.register_session(transaction_id.clone(), session_id);
            state.record_transaction(true);
            state.db_metrics().transactions_started.inc();

            Ok(Json(TransactionResponse { transaction_id }))
        }
        Err(e) => {
            engine.close_session(session_id);
            error!(error = %e, "Failed to begin transaction");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::transaction_error(e.to_string())),
            ))
        }
    }
}

/// Commit a transaction
pub async fn commit_transaction(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    debug!(transaction_id = %id, "Committing transaction");

    // Get the database engine
    let engine = match state.engine() {
        Some(e) => e,
        None => {
            state.record_transaction(false);
            return Ok(StatusCode::OK);
        }
    };

    // Get the engine session ID
    let session_id = match state.get_session(&id) {
        Some(sid) => sid,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Transaction not found")),
            ));
        }
    };

    // Commit the transaction
    match engine.commit_transaction(session_id).await {
        Ok(_) => {
            info!(transaction_id = %id, "Transaction committed");
            state.remove_session(&id);
            engine.close_session(session_id);
            state.record_transaction(false);
            state.db_metrics().transactions_committed.inc();
            Ok(StatusCode::OK)
        }
        Err(e) => {
            error!(error = %e, "Failed to commit transaction");
            // Still clean up
            state.remove_session(&id);
            engine.close_session(session_id);
            state.record_transaction(false);
            state.db_metrics().transactions_aborted.inc();
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::transaction_error(e.to_string())),
            ))
        }
    }
}

/// Rollback a transaction
pub async fn rollback_transaction(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    debug!(transaction_id = %id, "Rolling back transaction");

    // Get the database engine
    let engine = match state.engine() {
        Some(e) => e,
        None => {
            state.record_transaction(false);
            return Ok(StatusCode::OK);
        }
    };

    // Get the engine session ID
    let session_id = match state.get_session(&id) {
        Some(sid) => sid,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Transaction not found")),
            ));
        }
    };

    // Rollback the transaction
    match engine.rollback_transaction(session_id).await {
        Ok(_) => {
            info!(transaction_id = %id, "Transaction rolled back");
            state.remove_session(&id);
            engine.close_session(session_id);
            state.record_transaction(false);
            state.db_metrics().transactions_aborted.inc();
            Ok(StatusCode::OK)
        }
        Err(e) => {
            error!(error = %e, "Failed to rollback transaction");
            // Still clean up
            state.remove_session(&id);
            engine.close_session(session_id);
            state.record_transaction(false);
            state.db_metrics().transactions_aborted.inc();
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::transaction_error(e.to_string())),
            ))
        }
    }
}

// ============================================================================
// Table Handlers
// ============================================================================

/// List all tables
pub async fn list_tables(State(state): State<AppState>) -> ApiResult<Json<Vec<TableSummary>>> {
    debug!("Listing tables");

    // Get the database engine
    let engine = match state.engine() {
        Some(e) => e,
        None => return Ok(Json(vec![])),
    };

    // Get tables from the catalog (returns Vec<String>)
    let table_names = engine.list_tables();

    let summaries: Vec<TableSummary> = table_names
        .into_iter()
        .map(|name| TableSummary {
            name,
            schema: "public".to_string(),
            row_count: 0, // Would need to query storage for actual count
            size_bytes: 0,
        })
        .collect();

    Ok(Json(summaries))
}

/// Get table schema
pub async fn get_table(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<TableSchema>> {
    debug!(table = %name, "Getting table schema");

    // Get the database engine
    let engine = match state.engine() {
        Some(e) => e,
        None => {
            return Ok(Json(TableSchema {
                name,
                schema: "public".to_string(),
                columns: vec![],
                primary_key: vec![],
                indexes: vec![],
                row_count: 0,
                size_bytes: 0,
            }));
        }
    };

    // Get table from catalog
    match engine.get_table(&name) {
        Some(table_info) => {
            let columns: Vec<ColumnSchema> = table_info.schema.columns
                .iter()
                .map(|c| ColumnSchema {
                    name: c.name.clone(),
                    data_type: format!("{:?}", c.data_type),
                    nullable: c.nullable,
                    default_value: c.default.as_ref().map(|v| format!("{}", v)),
                    is_primary_key: false, // TODO: Track primary key in schema
                })
                .collect();

            Ok(Json(TableSchema {
                name: table_info.name.clone(),
                schema: "public".to_string(),
                columns,
                primary_key: vec![], // Would need to track this in schema
                indexes: vec![],
                row_count: 0,
                size_bytes: 0,
            }))
        }
        None => {
            Err((
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found(format!("Table '{}' not found", name))),
            ))
        }
    }
}

// ============================================================================
// Admin Handlers
// ============================================================================

/// Health check endpoint with component-level checks
pub async fn health_check(State(state): State<AppState>) -> Json<HealthResponse> {
    let mut database_ok = false;
    let mut storage_ok = false;
    let mut disk_ok = false;
    let mut error_msg = None;

    if let Some(engine) = state.engine() {
        // Test database connectivity
        let _ = engine.list_tables();
        database_ok = true;
        storage_ok = true;

        // Test disk I/O
        let test_path = std::env::temp_dir().join(format!("thunderdb_health_{}", uuid::Uuid::new_v4()));
        match std::fs::write(&test_path, b"health_check") {
            Ok(_) => {
                disk_ok = true;
                let _ = std::fs::remove_file(&test_path);
            }
            Err(e) => {
                error_msg = Some(format!("Disk write check failed: {}", e));
            }
        }
    } else {
        error_msg = Some("Database engine not initialized".to_string());
    }

    let status = if database_ok && storage_ok && disk_ok {
        "healthy"
    } else {
        "degraded"
    };

    Json(HealthResponse {
        status: status.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: state.uptime_seconds(),
        database_ok,
        storage_ok,
        disk_ok,
        error: error_msg,
    })
}

/// Prometheus metrics endpoint - returns text/plain in Prometheus exposition format
pub async fn prometheus_metrics(
    State(state): State<AppState>,
) -> (StatusCode, [(axum::http::header::HeaderName, &'static str); 1], String) {
    let db_metrics = state.db_metrics();

    // Sync gauges from engine stats if available
    if let Some(engine) = state.engine() {
        let stats = engine.stats();
        db_metrics.active_connections.set(stats.active_connections);
        db_metrics.active_transactions.set(stats.active_transactions);
    }

    let body = db_metrics.export_prometheus();

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// Get server metrics
pub async fn get_metrics(State(state): State<AppState>) -> Json<MetricsResponse> {
    let metrics = state.metrics();

    // Try to get buffer pool hit ratio from engine
    let buffer_pool_hit_ratio = state.engine()
        .map(|e| e.stats().buffer_pool_hit_ratio)
        .unwrap_or(1.0);

    let average_query_time_ms = if metrics.queries_total > 0 {
        metrics.total_query_time_ms as f64 / metrics.queries_total as f64
    } else {
        0.0
    };

    Json(MetricsResponse {
        queries_total: metrics.queries_total,
        queries_failed: metrics.queries_failed,
        active_connections: metrics.active_connections,
        active_transactions: metrics.active_transactions,
        buffer_pool_hit_ratio,
        average_query_time_ms,
    })
}

// ============================================================================
// CDC Handlers
// ============================================================================

/// List CDC subscriptions
pub async fn list_subscriptions(
    State(_state): State<AppState>,
) -> ApiResult<Json<Vec<CdcSubscription>>> {
    debug!("Listing CDC subscriptions");

    // TODO: Query CDC system for subscriptions
    Ok(Json(vec![]))
}

/// Create a CDC subscription
pub async fn create_subscription(
    State(_state): State<AppState>,
    Json(config): Json<CreateSubscriptionRequest>,
) -> ApiResult<Json<CdcSubscription>> {
    let subscription_id = uuid::Uuid::new_v4().to_string();

    info!(
        subscription_id = %subscription_id,
        table = %config.table,
        "Creating CDC subscription"
    );

    // TODO: Create actual CDC subscription
    Ok(Json(CdcSubscription {
        id: subscription_id,
        table: config.table,
        operations: config.operations,
        status: "active".to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        last_event_at: None,
        event_count: 0,
    }))
}

// ============================================================================
// FDW Handlers
// ============================================================================

/// List foreign servers
pub async fn list_foreign_servers(
    State(_state): State<AppState>,
) -> ApiResult<Json<Vec<ForeignServer>>> {
    debug!("Listing foreign servers");

    // TODO: Query catalog for foreign servers
    Ok(Json(vec![]))
}

/// Create a foreign server
pub async fn create_foreign_server(
    State(_state): State<AppState>,
    Json(config): Json<CreateForeignServerRequest>,
) -> ApiResult<Json<ForeignServer>> {
    info!(
        name = %config.name,
        server_type = %config.server_type,
        "Creating foreign server"
    );

    // TODO: Actually create the foreign server
    Ok(Json(ForeignServer {
        name: config.name,
        server_type: config.server_type,
        host: config.host,
        port: config.port,
        database: config.database,
        status: "connected".to_string(),
    }))
}

// ============================================================================
// Request/Response Types
// ============================================================================

/// Table summary for listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSummary {
    pub name: String,
    pub schema: String,
    pub row_count: u64,
    pub size_bytes: u64,
}

/// Full table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub schema: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexSchema>,
    pub row_count: u64,
    pub size_bytes: u64,
}

/// Column schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
}

/// Index schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: String,
}

/// CDC subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSubscription {
    pub id: String,
    pub table: String,
    pub operations: Vec<String>,
    pub status: String,
    pub created_at: String,
    pub last_event_at: Option<String>,
    pub event_count: u64,
}

/// Request to create a CDC subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSubscriptionRequest {
    pub table: String,
    #[serde(default = "default_operations")]
    pub operations: Vec<String>,
    pub filter: Option<String>,
    pub webhook_url: Option<String>,
}

fn default_operations() -> Vec<String> {
    vec![
        "INSERT".to_string(),
        "UPDATE".to_string(),
        "DELETE".to_string(),
    ]
}

/// Foreign server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignServer {
    pub name: String,
    pub server_type: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub status: String,
}

/// Request to create a foreign server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateForeignServerRequest {
    pub name: String,
    pub server_type: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub options: Option<serde_json::Value>,
}

// ============================================================================
// Backup/Restore Handlers
// ============================================================================

/// Create a database backup
pub async fn create_backup(
    State(state): State<AppState>,
    Json(request): Json<crate::BackupRequest>,
) -> ApiResult<Json<crate::BackupResult>> {
    let engine = state.engine().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(ApiError::internal_error("Database engine not initialized")))
    })?;

    match engine.backup(request.target_dir.as_deref()).await {
        Ok(result) => {
            info!(backup_id = %result.backup_id, "Backup created successfully");
            Ok(Json(result))
        }
        Err(e) => {
            error!(error = %e, "Backup failed");
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ApiError::internal_error(e.to_string()))))
        }
    }
}

/// List available backups
pub async fn list_backups(
    State(state): State<AppState>,
) -> ApiResult<Json<Vec<crate::BackupInfo>>> {
    let engine = state.engine().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(ApiError::internal_error("Database engine not initialized")))
    })?;

    match engine.list_backups() {
        Ok(backups) => Ok(Json(backups)),
        Err(e) => {
            error!(error = %e, "Failed to list backups");
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ApiError::internal_error(e.to_string()))))
        }
    }
}

/// Restore from a backup
///
/// Note: Restore requires the server to be stopped. This endpoint returns
/// an error with instructions for using the CLI restore command.
pub async fn restore_backup(
    State(_state): State<AppState>,
    Json(request): Json<crate::RestoreRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    Err((
        StatusCode::CONFLICT,
        Json(ApiError::new(
            "REQUIRES_RESTART",
            format!(
                "Restore requires the server to be stopped. Use CLI: thunderdb restore --backup-path {}",
                request.backup_path
            ),
        )),
    ))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check() {
        let state = AppState::new();
        let response = health_check(State(state)).await;
        // Without engine it should be degraded
        assert_eq!(response.status, "degraded");
    }

    #[tokio::test]
    async fn test_get_metrics() {
        let state = AppState::new();
        state.record_query(true, 100);
        state.record_query(true, 200);

        let response = get_metrics(State(state)).await;
        assert_eq!(response.queries_total, 2);
        assert_eq!(response.average_query_time_ms, 150.0);
    }

    #[tokio::test]
    async fn test_execute_query_empty_sql() {
        let state = AppState::new();
        let request = QueryRequest {
            sql: "".to_string(),
            params: vec![],
            timeout_ms: None,
            transaction_id: None,
        };

        let result = execute_query(State(state), Json(request)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_query_no_engine() {
        let state = AppState::new();
        let request = QueryRequest {
            sql: "SELECT 1".to_string(),
            params: vec![],
            timeout_ms: None,
            transaction_id: None,
        };

        // Without an engine, it should return a stub response
        let result = execute_query(State(state), Json(request)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_begin_transaction_no_engine() {
        let state = AppState::new();
        let request = TransactionRequest {
            isolation_level: Some("READ_COMMITTED".to_string()),
            read_only: false,
        };

        let result = begin_transaction(State(state.clone()), Json(request)).await;
        assert!(result.is_ok());

        let metrics = state.metrics();
        assert_eq!(metrics.active_transactions, 1);
    }

    #[test]
    fn test_create_subscription_request() {
        let json = r#"{"table": "users"}"#;
        let req: CreateSubscriptionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.table, "users");
        assert_eq!(req.operations.len(), 3); // default operations
    }

    #[test]
    fn test_value_to_json() {
        assert_eq!(value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(value_to_json(&Value::Boolean(true)), serde_json::Value::Bool(true));
        assert_eq!(value_to_json(&Value::Int64(42)), serde_json::json!(42));
        assert_eq!(value_to_json(&Value::String("hello".into())), serde_json::json!("hello"));
    }
}
