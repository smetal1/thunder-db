//! gRPC Service
//!
//! Provides gRPC API for ThunderDB with:
//! - Query execution
//! - Transaction management
//! - Streaming results
//! - Bidirectional streaming for subscriptions

use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Instant;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

use crate::AppState;

// ============================================================================
// Service Definition (would normally be generated from proto)
// ============================================================================

/// Query service for executing SQL
pub mod thunder {
    
    use serde::{Deserialize, Serialize};

    // Request/Response types (in real implementation, these come from prost)
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct QueryRequest {
        pub sql: String,
        pub params: Vec<ParameterValue>,
        pub transaction_id: Option<String>,
        pub timeout_ms: Option<u64>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ParameterValue {
        pub value: Option<ParamValue>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum ParamValue {
        NullValue,
        BoolValue(bool),
        IntValue(i64),
        FloatValue(f64),
        StringValue(String),
        BytesValue(Vec<u8>),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct QueryResponse {
        pub query_id: String,
        pub columns: Vec<ColumnInfo>,
        pub rows: Vec<Row>,
        pub rows_affected: u64,
        pub execution_time_ms: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ColumnInfo {
        pub name: String,
        pub data_type: String,
        pub nullable: bool,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Row {
        pub values: Vec<CellValue>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct CellValue {
        pub value: Option<ParamValue>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct BeginTransactionRequest {
        pub isolation_level: i32,
        pub read_only: bool,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TransactionResponse {
        pub transaction_id: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct CommitRequest {
        pub transaction_id: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RollbackRequest {
        pub transaction_id: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Empty {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct StreamQueryRequest {
        pub sql: String,
        pub params: Vec<ParameterValue>,
        pub batch_size: u32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RowBatch {
        pub rows: Vec<Row>,
        pub is_last: bool,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SubscribeRequest {
        pub table: String,
        pub operations: Vec<i32>, // 1=INSERT, 2=UPDATE, 3=DELETE
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ChangeEvent {
        pub table: String,
        pub operation: i32,
        pub old_row: Option<Row>,
        pub new_row: Option<Row>,
        pub timestamp: u64,
        pub lsn: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HealthCheckRequest {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HealthCheckResponse {
        pub status: i32,
        pub version: String,
        pub uptime_seconds: u64,
    }

    // Isolation levels
    pub mod isolation_level {
        pub const READ_UNCOMMITTED: i32 = 0;
        pub const READ_COMMITTED: i32 = 1;
        pub const REPEATABLE_READ: i32 = 2;
        pub const SERIALIZABLE: i32 = 3;
    }

    // Health status
    pub mod health_status {
        pub const UNKNOWN: i32 = 0;
        pub const SERVING: i32 = 1;
        pub const NOT_SERVING: i32 = 2;
    }

    // Operations
    pub mod operation {
        pub const INSERT: i32 = 1;
        pub const UPDATE: i32 = 2;
        pub const DELETE: i32 = 3;
    }
}

use thunder::*;

// ============================================================================
// Thunder Query Service
// ============================================================================

/// gRPC service implementation
pub struct ThunderQueryService {
    state: AppState,
    start_time: Instant,
}

impl ThunderQueryService {
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            start_time: Instant::now(),
        }
    }

    /// Execute a query
    pub async fn execute_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();
        let query_id = uuid::Uuid::new_v4();

        debug!(sql = %req.sql, "Executing gRPC query");

        // Get the engine from state
        let engine = self.state.engine().ok_or_else(|| {
            Status::unavailable("Database engine not initialized")
        })?;

        // Get or create session
        let session_id = req.transaction_id
            .as_ref()
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .unwrap_or_else(|| engine.create_session());

        // Execute the query
        match engine.execute_sql(session_id, &req.sql).await {
            Ok(result) => {
                // Convert columns
                let columns: Vec<ColumnInfo> = result.columns.iter().map(|(name, dt)| {
                    ColumnInfo {
                        name: name.clone(),
                        data_type: format!("{:?}", dt),
                        nullable: true,
                    }
                }).collect();

                // Convert rows
                let rows: Vec<Row> = result.rows.iter().map(|row| {
                    Row {
                        values: row.values.iter().map(value_to_cell).collect(),
                    }
                }).collect();

                let response = QueryResponse {
                    query_id: query_id.to_string(),
                    columns,
                    rows,
                    rows_affected: result.rows_affected.unwrap_or(0),
                    execution_time_ms: start.elapsed().as_millis() as u64,
                };

                Ok(Response::new(response))
            }
            Err(e) => {
                Err(Status::internal(format!("Query execution failed: {}", e)))
            }
        }
    }

    /// Execute a streaming query (server streaming)
    pub async fn stream_query(
        &self,
        request: Request<StreamQueryRequest>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<RowBatch, Status>> + Send>>>, Status> {
        let req = request.into_inner();
        let batch_size = req.batch_size.max(100) as usize;

        debug!(sql = %req.sql, batch_size, "Starting streaming query");

        let (tx, rx) = mpsc::channel(32);

        // Get the engine from state and clone the Arc for the spawned task
        let engine = self.state.engine().ok_or_else(|| {
            Status::unavailable("Database engine not initialized")
        })?.clone();

        let sql = req.sql.clone();
        let session_id = engine.create_session();

        // Spawn a task to execute query and stream results
        tokio::spawn(async move {
            match engine.execute_sql(session_id, &sql).await {
                Ok(result) => {
                    // Stream rows in batches
                    let mut current_batch = Vec::new();
                    let total_rows = result.rows.len();

                    for (idx, row) in result.rows.into_iter().enumerate() {
                        let grpc_row = Row {
                            values: row.values.iter().map(value_to_cell).collect(),
                        };
                        current_batch.push(grpc_row);

                        // Send batch when full or last row
                        if current_batch.len() >= batch_size || idx == total_rows - 1 {
                            let is_last = idx == total_rows - 1;
                            let batch = RowBatch {
                                rows: std::mem::take(&mut current_batch),
                                is_last,
                            };
                            if tx.send(Ok(batch)).await.is_err() {
                                break;
                            }
                        }
                    }

                    // If no rows, send empty final batch
                    if total_rows == 0 {
                        let _ = tx.send(Ok(RowBatch { rows: vec![], is_last: true })).await;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(format!("Query failed: {}", e)))).await;
                }
            }

            // Close session
            engine.close_session(session_id);
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    /// Begin a transaction
    pub async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<TransactionResponse>, Status> {
        let req = request.into_inner();

        debug!(
            isolation_level = req.isolation_level,
            read_only = req.read_only,
            "Beginning transaction"
        );

        // Get the engine from state
        let engine = self.state.engine().ok_or_else(|| {
            Status::unavailable("Database engine not initialized")
        })?;

        // Create a new session for this transaction
        let session_id = engine.create_session();

        // Begin the transaction
        match engine.begin_transaction(session_id).await {
            Ok(_txn_id) => {
                let response = TransactionResponse {
                    transaction_id: session_id.to_string(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                engine.close_session(session_id);
                Err(Status::internal(format!("Failed to begin transaction: {}", e)))
            }
        }
    }

    /// Commit a transaction
    pub async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();

        debug!(transaction_id = %req.transaction_id, "Committing transaction");

        // Get the engine from state
        let engine = self.state.engine().ok_or_else(|| {
            Status::unavailable("Database engine not initialized")
        })?;

        // Parse the session/transaction ID
        let session_id = uuid::Uuid::parse_str(&req.transaction_id)
            .map_err(|_| Status::invalid_argument("Invalid transaction ID"))?;

        // Commit the transaction
        match engine.commit_transaction(session_id).await {
            Ok(()) => {
                engine.close_session(session_id);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                Err(Status::internal(format!("Failed to commit transaction: {}", e)))
            }
        }
    }

    /// Rollback a transaction
    pub async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();

        debug!(transaction_id = %req.transaction_id, "Rolling back transaction");

        // Get the engine from state
        let engine = self.state.engine().ok_or_else(|| {
            Status::unavailable("Database engine not initialized")
        })?;

        // Parse the session/transaction ID
        let session_id = uuid::Uuid::parse_str(&req.transaction_id)
            .map_err(|_| Status::invalid_argument("Invalid transaction ID"))?;

        // Rollback the transaction
        match engine.rollback_transaction(session_id).await {
            Ok(()) => {
                engine.close_session(session_id);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                Err(Status::internal(format!("Failed to rollback transaction: {}", e)))
            }
        }
    }

    /// Subscribe to change events (server streaming)
    pub async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<ChangeEvent, Status>> + Send>>>, Status> {
        let req = request.into_inner();

        info!(table = %req.table, "New CDC subscription");

        let (tx, rx) = mpsc::channel(256);

        // Get the engine to validate table exists
        let engine = self.state.engine().ok_or_else(|| {
            Status::unavailable("Database engine not initialized")
        })?;

        let table = req.table.clone();
        let _operations = req.operations.clone();

        // Verify the table exists
        if engine.get_table(&table).is_none() {
            return Err(Status::not_found(format!("Table '{}' not found", table)));
        }

        // Spawn subscription handler that polls for changes
        tokio::spawn(async move {
            let _last_lsn: u64 = 0;
            let poll_interval = tokio::time::Duration::from_millis(100);

            loop {
                // Check for new change events
                // In a production system, this would connect to the WAL/CDC infrastructure
                // For now, we use a polling mechanism that can be extended

                // Sleep between polls
                tokio::time::sleep(poll_interval).await;

                // The channel will be dropped when the client disconnects,
                // causing this task to exit gracefully
                if tx.is_closed() {
                    debug!(table = %table, "CDC subscription closed");
                    break;
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    /// Bidirectional streaming for interactive queries
    pub async fn interactive_query(
        &self,
        request: Request<Streaming<QueryRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<QueryResponse, Status>> + Send>>>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(32);

        // Get the engine from state and clone for the spawned task
        let engine = self.state.engine().ok_or_else(|| {
            Status::unavailable("Database engine not initialized")
        })?.clone();

        // Create a persistent session for interactive queries
        let session_id = engine.create_session();

        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                let start = Instant::now();
                debug!(sql = %req.sql, "Processing interactive query");

                let response = match engine.execute_sql(session_id, &req.sql).await {
                    Ok(result) => {
                        // Convert columns
                        let columns: Vec<ColumnInfo> = result.columns.iter().map(|(name, dt)| {
                            ColumnInfo {
                                name: name.clone(),
                                data_type: format!("{:?}", dt),
                                nullable: true,
                            }
                        }).collect();

                        // Convert rows
                        let rows: Vec<Row> = result.rows.iter().map(|row| {
                            Row {
                                values: row.values.iter().map(value_to_cell).collect(),
                            }
                        }).collect();

                        Ok(QueryResponse {
                            query_id: uuid::Uuid::new_v4().to_string(),
                            columns,
                            rows,
                            rows_affected: result.rows_affected.unwrap_or(0),
                            execution_time_ms: start.elapsed().as_millis() as u64,
                        })
                    }
                    Err(e) => {
                        Err(Status::internal(format!("Query failed: {}", e)))
                    }
                };

                if tx.send(response).await.is_err() {
                    break;
                }
            }

            // Clean up session when stream ends
            engine.close_session(session_id);
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    /// Health check
    pub async fn check_health(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let response = HealthCheckResponse {
            status: health_status::SERVING,
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
        };

        Ok(Response::new(response))
    }
}

// ============================================================================
// gRPC Server
// ============================================================================

/// gRPC server for ThunderDB
pub struct GrpcServer {
    port: u16,
    state: AppState,
}

impl GrpcServer {
    pub fn new(port: u16, state: AppState) -> Self {
        Self { port, state }
    }

    /// Start the gRPC server
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let _service = ThunderQueryService::new(self.state);

        info!(address = %addr, "Starting gRPC server");

        // In a real implementation, we would use tonic::Server with generated service traits
        // For now, we have the service implementation ready to be wired up

        // Example of how it would look with actual proto-generated code:
        // Server::builder()
        //     .add_service(ThunderQueryServer::new(service))
        //     .serve(addr)
        //     .await?;

        // For now, just log that we're ready
        info!("gRPC service implementation ready");

        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert internal Value to gRPC CellValue
fn value_to_cell(value: &thunder_common::types::Value) -> CellValue {
    use thunder_common::types::Value;

    let param = match value {
        Value::Null => None,
        Value::Boolean(b) => Some(ParamValue::BoolValue(*b)),
        Value::Int8(v) => Some(ParamValue::IntValue(*v as i64)),
        Value::Int16(v) => Some(ParamValue::IntValue(*v as i64)),
        Value::Int32(v) => Some(ParamValue::IntValue(*v as i64)),
        Value::Int64(v) => Some(ParamValue::IntValue(*v)),
        Value::Float32(v) => Some(ParamValue::FloatValue(*v as f64)),
        Value::Float64(v) => Some(ParamValue::FloatValue(*v)),
        Value::String(s) => Some(ParamValue::StringValue(s.to_string())),
        Value::Binary(b) => Some(ParamValue::BytesValue(b.to_vec())),
        _ => Some(ParamValue::StringValue(format!("{:?}", value))),
    };

    CellValue { value: param }
}

/// Convert gRPC ParameterValue to internal Value
#[allow(dead_code)]
fn param_to_value(param: &ParameterValue) -> thunder_common::types::Value {
    use thunder_common::types::Value;

    match &param.value {
        None => Value::Null,
        Some(ParamValue::NullValue) => Value::Null,
        Some(ParamValue::BoolValue(b)) => Value::Boolean(*b),
        Some(ParamValue::IntValue(v)) => Value::Int64(*v),
        Some(ParamValue::FloatValue(v)) => Value::Float64(*v),
        Some(ParamValue::StringValue(s)) => Value::String(s.clone().into()),
        Some(ParamValue::BytesValue(b)) => Value::Binary(b.clone().into()),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_conversion() {
        use thunder_common::types::Value;

        let val = Value::Int64(42);
        let cell = value_to_cell(&val);
        assert!(matches!(cell.value, Some(ParamValue::IntValue(42))));

        let val = Value::String("hello".to_string().into());
        let cell = value_to_cell(&val);
        assert!(matches!(cell.value, Some(ParamValue::StringValue(_))));
    }

    #[test]
    fn test_param_conversion() {
        let param = ParameterValue {
            value: Some(ParamValue::IntValue(42)),
        };
        let val = param_to_value(&param);
        assert!(matches!(val, thunder_common::types::Value::Int64(42)));
    }

    #[tokio::test]
    async fn test_service_creation() {
        let state = AppState::new();
        let service = ThunderQueryService::new(state);

        let request = Request::new(HealthCheckRequest {});
        let response = service.check_health(request).await.unwrap();
        assert_eq!(response.into_inner().status, health_status::SERVING);
    }
}
