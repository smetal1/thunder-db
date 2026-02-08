//! WebSocket Support
//!
//! Provides WebSocket API for ThunderDB with:
//! - Real-time query subscriptions
//! - Change Data Capture (CDC) streaming
//! - Live query notifications
//! - Bidirectional communication

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use crate::AppState;

// ============================================================================
// WebSocket Types
// ============================================================================

/// WebSocket message from client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ClientMessage {
    /// Subscribe to a table's changes
    Subscribe(SubscribePayload),
    /// Unsubscribe from a subscription
    Unsubscribe { subscription_id: String },
    /// Execute a query
    Query(QueryPayload),
    /// Start a live query (re-executed on changes)
    LiveQuery(LiveQueryPayload),
    /// Stop a live query
    StopLiveQuery { query_id: String },
    /// Ping for keepalive
    Ping { timestamp: u64 },
    /// Acknowledge a message
    Ack { message_id: String },
}

/// WebSocket message to client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ServerMessage {
    /// Subscription confirmed
    Subscribed(SubscriptionConfirmation),
    /// Unsubscription confirmed
    Unsubscribed { subscription_id: String },
    /// Change event from subscription
    Change(ChangeEventPayload),
    /// Query result
    QueryResult(QueryResultPayload),
    /// Live query result update
    LiveQueryUpdate(LiveQueryUpdatePayload),
    /// Error message
    Error(ErrorPayload),
    /// Pong response
    Pong { timestamp: u64 },
    /// Server notification
    Notification(NotificationPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribePayload {
    pub table: String,
    #[serde(default)]
    pub operations: Vec<String>, // "INSERT", "UPDATE", "DELETE"
    #[serde(default)]
    pub filter: Option<String>, // SQL WHERE clause
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionConfirmation {
    pub subscription_id: String,
    pub table: String,
    pub operations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEventPayload {
    pub subscription_id: String,
    pub table: String,
    pub operation: String,
    pub old_row: Option<serde_json::Value>,
    pub new_row: Option<serde_json::Value>,
    pub timestamp: u64,
    pub lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPayload {
    pub query_id: String,
    pub sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultPayload {
    pub query_id: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub rows_affected: Option<u64>,
    pub execution_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveQueryPayload {
    pub query_id: String,
    pub sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
    /// Tables to watch for changes
    pub watch_tables: Vec<String>,
    /// Debounce interval in milliseconds
    #[serde(default = "default_debounce")]
    pub debounce_ms: u64,
}

fn default_debounce() -> u64 {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveQueryUpdatePayload {
    pub query_id: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub change_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPayload {
    pub channel: String,
    pub message: String,
    pub payload: Option<serde_json::Value>,
}

// ============================================================================
// Connection State
// ============================================================================

/// State for a single WebSocket connection
#[allow(dead_code)]
struct ConnectionState {
    id: String,
    subscriptions: HashMap<String, Subscription>,
    live_queries: HashMap<String, LiveQuery>,
    sender: mpsc::Sender<ServerMessage>,
}

#[allow(dead_code)]
struct Subscription {
    id: String,
    table: String,
    operations: Vec<String>,
    filter: Option<String>,
}

#[allow(dead_code)]
struct LiveQuery {
    id: String,
    sql: String,
    params: Vec<serde_json::Value>,
    watch_tables: Vec<String>,
    debounce_ms: u64,
}

// ============================================================================
// WebSocket Manager
// ============================================================================

/// Manages all WebSocket connections
pub struct WebSocketManager {
    connections: RwLock<HashMap<String, Arc<RwLock<ConnectionState>>>>,
    change_broadcast: broadcast::Sender<ChangeEventPayload>,
    connection_counter: AtomicU64,
}

impl WebSocketManager {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            connections: RwLock::new(HashMap::new()),
            change_broadcast: tx,
            connection_counter: AtomicU64::new(0),
        }
    }

    /// Create a new connection
    fn create_connection(&self, sender: mpsc::Sender<ServerMessage>) -> String {
        let id = format!(
            "ws_{}",
            self.connection_counter.fetch_add(1, Ordering::Relaxed)
        );

        let state = Arc::new(RwLock::new(ConnectionState {
            id: id.clone(),
            subscriptions: HashMap::new(),
            live_queries: HashMap::new(),
            sender,
        }));

        self.connections.write().insert(id.clone(), state);
        info!(connection_id = %id, "WebSocket connection created");

        id
    }

    /// Remove a connection
    fn remove_connection(&self, id: &str) {
        self.connections.write().remove(id);
        info!(connection_id = %id, "WebSocket connection removed");
    }

    /// Broadcast a change event to all subscribers
    pub fn broadcast_change(&self, event: ChangeEventPayload) {
        let _ = self.change_broadcast.send(event);
    }

    /// Get the number of active connections
    pub fn connection_count(&self) -> usize {
        self.connections.read().len()
    }
}

impl Default for WebSocketManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// WebSocket Handler
// ============================================================================

/// Handle a WebSocket upgrade request
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<WebSocketState>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Shared state for WebSocket handlers
#[derive(Clone)]
pub struct WebSocketState {
    pub app_state: AppState,
    pub manager: Arc<WebSocketManager>,
}

/// Handle an individual WebSocket connection
async fn handle_socket(socket: WebSocket, state: WebSocketState) {
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Create channel for sending messages to this connection
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(256);

    // Register connection
    let connection_id = state.manager.create_connection(tx.clone());

    // Subscribe to change broadcasts
    let mut change_rx = state.manager.change_broadcast.subscribe();

    // Task to forward messages from channel to WebSocket
    let _connection_id_clone = connection_id.clone();
    let forward_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = match serde_json::to_string(&msg) {
                Ok(j) => j,
                Err(e) => {
                    error!(error = %e, "Failed to serialize message");
                    continue;
                }
            };

            if ws_sender.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
    });

    // Task to handle broadcast changes
    let tx_clone = tx.clone();
    let manager_clone = state.manager.clone();
    let connection_id_for_broadcast = connection_id.clone();
    let broadcast_task = tokio::spawn(async move {
        while let Ok(event) = change_rx.recv().await {
            // Check if this connection has a subscription for this event
            let should_send = {
                let connections = manager_clone.connections.read();
                if let Some(conn) = connections.get(&connection_id_for_broadcast) {
                    let conn_state = conn.read();
                    conn_state.subscriptions.values().any(|sub| {
                        sub.table == event.table
                            && (sub.operations.is_empty()
                                || sub.operations.contains(&event.operation))
                    })
                } else {
                    false
                }
            };

            if should_send {
                let _ = tx_clone.send(ServerMessage::Change(event)).await;
            }
        }
    });

    // Main message handling loop
    let manager = state.manager.clone();
    let connection_id_for_loop = connection_id.clone();
    while let Some(msg) = ws_receiver.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "WebSocket receive error");
                break;
            }
        };

        match msg {
            Message::Text(text) => {
                let client_msg: ClientMessage = match serde_json::from_str(&text) {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = tx
                            .send(ServerMessage::Error(ErrorPayload {
                                code: "INVALID_MESSAGE".to_string(),
                                message: format!("Failed to parse message: {}", e),
                                request_id: None,
                            }))
                            .await;
                        continue;
                    }
                };

                handle_client_message(
                    client_msg,
                    &connection_id_for_loop,
                    &tx,
                    &manager,
                    &state.app_state,
                )
                .await;
            }
            Message::Binary(_) => {
                let _ = tx
                    .send(ServerMessage::Error(ErrorPayload {
                        code: "UNSUPPORTED".to_string(),
                        message: "Binary messages not supported".to_string(),
                        request_id: None,
                    }))
                    .await;
            }
            Message::Ping(_data) => {
                // Axum handles pong automatically
            }
            Message::Pong(_) => {}
            Message::Close(_) => break,
        }
    }

    // Cleanup
    forward_task.abort();
    broadcast_task.abort();
    state.manager.remove_connection(&connection_id);
}

/// Handle a client message
async fn handle_client_message(
    msg: ClientMessage,
    connection_id: &str,
    tx: &mpsc::Sender<ServerMessage>,
    manager: &Arc<WebSocketManager>,
    app_state: &AppState,
) {
    match msg {
        ClientMessage::Subscribe(payload) => {
            let subscription_id = uuid::Uuid::new_v4().to_string();

            // Register subscription
            {
                let connections = manager.connections.read();
                if let Some(conn) = connections.get(connection_id) {
                    let mut conn_state = conn.write();
                    conn_state.subscriptions.insert(
                        subscription_id.clone(),
                        Subscription {
                            id: subscription_id.clone(),
                            table: payload.table.clone(),
                            operations: payload.operations.clone(),
                            filter: payload.filter,
                        },
                    );
                }
            }

            let _ = tx
                .send(ServerMessage::Subscribed(SubscriptionConfirmation {
                    subscription_id,
                    table: payload.table,
                    operations: payload.operations,
                }))
                .await;
        }

        ClientMessage::Unsubscribe { subscription_id } => {
            {
                let connections = manager.connections.read();
                if let Some(conn) = connections.get(connection_id) {
                    conn.write().subscriptions.remove(&subscription_id);
                }
            }

            let _ = tx
                .send(ServerMessage::Unsubscribed { subscription_id })
                .await;
        }

        ClientMessage::Query(payload) => {
            let start = std::time::Instant::now();

            // Execute the query using the database engine
            if let Some(engine) = app_state.engine() {
                let session_id = engine.create_session();

                match engine.execute_sql(session_id, &payload.sql).await {
                    Ok(result) => {
                        // Convert columns
                        let columns: Vec<String> = result.columns.iter()
                            .map(|(name, _)| name.clone())
                            .collect();

                        // Convert rows to JSON values
                        let rows: Vec<Vec<serde_json::Value>> = result.rows.iter().map(|row| {
                            row.values.iter().map(|v| value_to_json(v)).collect()
                        }).collect();

                        let query_result = QueryResultPayload {
                            query_id: payload.query_id,
                            columns,
                            rows,
                            rows_affected: result.rows_affected,
                            execution_time_ms: start.elapsed().as_millis() as u64,
                        };

                        let _ = tx.send(ServerMessage::QueryResult(query_result)).await;
                    }
                    Err(e) => {
                        let _ = tx.send(ServerMessage::Error(ErrorPayload {
                            code: "QUERY_ERROR".to_string(),
                            message: format!("Query failed: {}", e),
                            request_id: Some(payload.query_id),
                        })).await;
                    }
                }

                engine.close_session(session_id);
            } else {
                let _ = tx.send(ServerMessage::Error(ErrorPayload {
                    code: "ENGINE_NOT_AVAILABLE".to_string(),
                    message: "Database engine not initialized".to_string(),
                    request_id: Some(payload.query_id),
                })).await;
            }
        }

        ClientMessage::LiveQuery(payload) => {
            // Register live query
            {
                let connections = manager.connections.read();
                if let Some(conn) = connections.get(connection_id) {
                    let mut conn_state = conn.write();
                    conn_state.live_queries.insert(
                        payload.query_id.clone(),
                        LiveQuery {
                            id: payload.query_id.clone(),
                            sql: payload.sql.clone(),
                            params: payload.params.clone(),
                            watch_tables: payload.watch_tables.clone(),
                            debounce_ms: payload.debounce_ms,
                        },
                    );
                }
            }

            // Execute initial query
            if let Some(engine) = app_state.engine() {
                let session_id = engine.create_session();

                match engine.execute_sql(session_id, &payload.sql).await {
                    Ok(result) => {
                        // Convert columns
                        let columns: Vec<String> = result.columns.iter()
                            .map(|(name, _)| name.clone())
                            .collect();

                        // Convert rows to JSON values
                        let rows: Vec<Vec<serde_json::Value>> = result.rows.iter().map(|row| {
                            row.values.iter().map(|v| value_to_json(v)).collect()
                        }).collect();

                        let update = LiveQueryUpdatePayload {
                            query_id: payload.query_id,
                            columns,
                            rows,
                            change_count: 0,
                        };

                        let _ = tx.send(ServerMessage::LiveQueryUpdate(update)).await;
                    }
                    Err(e) => {
                        let _ = tx.send(ServerMessage::Error(ErrorPayload {
                            code: "LIVE_QUERY_ERROR".to_string(),
                            message: format!("Live query failed: {}", e),
                            request_id: Some(payload.query_id),
                        })).await;
                    }
                }

                engine.close_session(session_id);
            } else {
                let _ = tx.send(ServerMessage::Error(ErrorPayload {
                    code: "ENGINE_NOT_AVAILABLE".to_string(),
                    message: "Database engine not initialized".to_string(),
                    request_id: Some(payload.query_id),
                })).await;
            }
        }

        ClientMessage::StopLiveQuery { query_id } => {
            let connections = manager.connections.read();
            if let Some(conn) = connections.get(connection_id) {
                conn.write().live_queries.remove(&query_id);
            }
        }

        ClientMessage::Ping { timestamp } => {
            let _ = tx.send(ServerMessage::Pong { timestamp }).await;
        }

        ClientMessage::Ack { message_id } => {
            debug!(message_id = %message_id, "Message acknowledged");
        }
    }
}

// ============================================================================
// WebSocket Router
// ============================================================================

/// Create WebSocket routes
pub fn ws_router(app_state: AppState) -> Router {
    let manager = Arc::new(WebSocketManager::new());
    let ws_state = WebSocketState {
        app_state,
        manager,
    };

    Router::new()
        .route("/ws", get(ws_handler))
        .route("/ws/subscribe", get(ws_handler))
        .with_state(ws_state)
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert internal Value to JSON value
fn value_to_json(value: &thunder_common::types::Value) -> serde_json::Value {
    use thunder_common::types::Value;

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
            serde_json::Value::String(format!("{:.scale$}", *val as f64 / 10_f64.powi(*scale as i32), scale = *scale as usize))
        }
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Binary(b) => {
            // Encode as base64
            use std::fmt::Write;
            let mut encoded = String::new();
            for byte in b.iter() {
                write!(encoded, "{:02x}", byte).unwrap();
            }
            serde_json::Value::String(encoded)
        }
        Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::TimestampTz(_, _) => {
            serde_json::Value::String(format!("{}", value))
        }
        Value::Uuid(u) => {
            let mut hex = String::with_capacity(36);
            use std::fmt::Write;
            write!(hex, "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]).unwrap();
            serde_json::Value::String(hex)
        }
        Value::Json(j) => {
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
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_message_serialize() {
        let msg = ClientMessage::Subscribe(SubscribePayload {
            table: "users".to_string(),
            operations: vec!["INSERT".to_string(), "UPDATE".to_string()],
            filter: None,
        });

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("Subscribe"));
        assert!(json.contains("users"));
    }

    #[test]
    fn test_server_message_serialize() {
        let msg = ServerMessage::Subscribed(SubscriptionConfirmation {
            subscription_id: "sub_1".to_string(),
            table: "users".to_string(),
            operations: vec!["INSERT".to_string()],
        });

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("Subscribed"));
        assert!(json.contains("sub_1"));
    }

    #[test]
    fn test_client_message_deserialize() {
        let json = r#"{"type":"Ping","payload":{"timestamp":12345}}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        assert!(matches!(msg, ClientMessage::Ping { timestamp: 12345 }));
    }

    #[test]
    fn test_query_payload() {
        let payload = QueryPayload {
            query_id: "q1".to_string(),
            sql: "SELECT * FROM users".to_string(),
            params: vec![],
        };

        let json = serde_json::to_string(&payload).unwrap();
        let parsed: QueryPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.query_id, "q1");
    }

    #[test]
    fn test_websocket_manager() {
        let manager = WebSocketManager::new();
        assert_eq!(manager.connection_count(), 0);
    }
}
