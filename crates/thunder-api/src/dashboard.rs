//! # ThunderDB Web Dashboard
//!
//! Embedded web dashboard for database operations, monitoring, and administration.
//! Serves a single-page application with query editor, metrics, and table browser.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, Response, StatusCode},
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

use crate::AppState;

// ============================================================================
// Dashboard Response Types
// ============================================================================

/// Dashboard statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardStats {
    pub uptime_seconds: u64,
    pub queries_total: u64,
    pub queries_per_second: f64,
    pub active_connections: u64,
    pub active_transactions: u64,
    pub buffer_pool_hit_ratio: f64,
    pub tables_count: usize,
    pub memory_usage_mb: f64,
}

/// Table overview for dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableOverview {
    pub name: String,
    pub columns: Vec<ColumnOverview>,
    pub row_count: Option<u64>,
    pub size_bytes: Option<u64>,
}

/// Column overview
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnOverview {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
}

/// Query execution result for dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardQueryResult {
    pub success: bool,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub rows_affected: Option<u64>,
    pub execution_time_ms: u64,
    pub error: Option<String>,
}

/// Performance metrics over time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub timestamps: Vec<i64>,
    pub qps: Vec<f64>,
    pub latency_p50_ms: Vec<f64>,
    pub latency_p99_ms: Vec<f64>,
    pub memory_mb: Vec<f64>,
}

// ============================================================================
// Dashboard Handlers
// ============================================================================

/// Serve the main dashboard HTML
async fn serve_dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

/// Serve dashboard CSS
async fn serve_css() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/css")
        .body(Body::from(DASHBOARD_CSS))
        .unwrap()
}

/// Serve dashboard JavaScript
async fn serve_js() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/javascript")
        .body(Body::from(DASHBOARD_JS))
        .unwrap()
}

/// Get dashboard statistics
async fn get_stats(State(state): State<AppState>) -> Json<DashboardStats> {
    let metrics = state.metrics();
    let uptime = state.uptime_seconds();
    let tables_count = state.engine()
        .map(|e| e.list_tables().len())
        .unwrap_or(0);

    let qps = if uptime > 0 {
        metrics.queries_total as f64 / uptime as f64
    } else {
        0.0
    };

    // Estimate memory usage (in a real implementation, would use system APIs)
    let memory_usage_mb = 128.0 + (tables_count as f64 * 10.0);

    Json(DashboardStats {
        uptime_seconds: uptime,
        queries_total: metrics.queries_total,
        queries_per_second: qps,
        active_connections: metrics.active_connections,
        active_transactions: metrics.active_transactions,
        buffer_pool_hit_ratio: 0.95, // Would come from buffer pool stats
        tables_count,
        memory_usage_mb,
    })
}

/// Get all tables overview
async fn get_tables(State(state): State<AppState>) -> Json<Vec<TableOverview>> {
    let tables = state.engine()
        .map(|engine| {
            engine.list_tables().into_iter().map(|name| {
                let table_info = engine.get_table(&name);
                let columns = table_info.as_ref()
                    .map(|t| {
                        t.schema.columns.iter().map(|col| {
                            ColumnOverview {
                                name: col.name.clone(),
                                data_type: format!("{:?}", col.data_type),
                                nullable: col.nullable,
                                primary_key: false, // Primary key info stored in table constraints
                            }
                        }).collect()
                    })
                    .unwrap_or_default();

                TableOverview {
                    name,
                    columns,
                    row_count: None, // Would need to query the table
                    size_bytes: None,
                }
            }).collect()
        })
        .unwrap_or_default();

    Json(tables)
}

/// Get specific table details
async fn get_table(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
) -> Result<Json<TableOverview>, StatusCode> {
    let engine = state.engine().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let table_info = engine.get_table(&table_name).ok_or(StatusCode::NOT_FOUND)?;

    let columns = table_info.schema.columns.iter().map(|col| {
        ColumnOverview {
            name: col.name.clone(),
            data_type: format!("{:?}", col.data_type),
            nullable: col.nullable,
            primary_key: false, // Primary key info stored in table constraints
        }
    }).collect();

    Ok(Json(TableOverview {
        name: table_name,
        columns,
        row_count: None,
        size_bytes: None,
    }))
}

/// Execute a query from the dashboard
#[derive(Debug, Deserialize)]
pub struct ExecuteQueryRequest {
    pub sql: String,
}

async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<ExecuteQueryRequest>,
) -> Json<DashboardQueryResult> {
    let start = std::time::Instant::now();

    let engine = match state.engine() {
        Some(e) => e,
        None => {
            return Json(DashboardQueryResult {
                success: false,
                columns: vec![],
                rows: vec![],
                rows_affected: None,
                execution_time_ms: 0,
                error: Some("Database engine not available".to_string()),
            });
        }
    };

    let session_id = engine.create_session();
    let result = engine.execute_sql(session_id, &req.sql).await;
    engine.close_session(session_id);

    let elapsed = start.elapsed().as_millis() as u64;
    state.record_query(result.is_ok(), elapsed);

    match result {
        Ok(res) => {
            let columns: Vec<String> = res.columns.iter().map(|(name, _)| name.clone()).collect();
            let rows: Vec<Vec<serde_json::Value>> = res.rows.iter().map(|row| {
                row.values.iter().map(value_to_json).collect()
            }).collect();

            Json(DashboardQueryResult {
                success: true,
                columns,
                rows,
                rows_affected: res.rows_affected,
                execution_time_ms: elapsed,
                error: None,
            })
        }
        Err(e) => {
            Json(DashboardQueryResult {
                success: false,
                columns: vec![],
                rows: vec![],
                rows_affected: None,
                execution_time_ms: elapsed,
                error: Some(e.to_string()),
            })
        }
    }
}

/// Convert Value to JSON
fn value_to_json(value: &thunder_common::types::Value) -> serde_json::Value {
    use thunder_common::types::Value;
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Int8(n) => serde_json::json!(n),
        Value::Int16(n) => serde_json::json!(n),
        Value::Int32(n) => serde_json::json!(n),
        Value::Int64(n) => serde_json::json!(n),
        Value::Float32(n) => serde_json::json!(n),
        Value::Float64(n) => serde_json::json!(n),
        Value::Decimal(val, scale) => {
            // Format decimal with proper scale
            let str_val = if *scale > 0 {
                let divisor = 10i128.pow(*scale as u32);
                format!("{}.{:0>width$}", val / divisor, (val % divisor).abs(), width = *scale as usize)
            } else {
                val.to_string()
            };
            serde_json::json!(str_val)
        }
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Binary(b) => {
            use base64::Engine;
            serde_json::json!(base64::engine::general_purpose::STANDARD.encode(b.as_ref()))
        }
        Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::TimestampTz(_, _) => {
            serde_json::Value::String(format!("{}", value))
        }
        Value::Uuid(u) => {
            // Format UUID bytes as standard UUID string
            let hex = hex::encode(u);
            serde_json::Value::String(format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32]
            ))
        }
        Value::Json(j) => {
            // Parse JSON string to serde_json::Value
            serde_json::from_str(j).unwrap_or_else(|_| serde_json::Value::String(j.to_string()))
        }
        Value::Vector(v) => serde_json::Value::Array(v.iter().map(|f| serde_json::json!(f)).collect()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
    }
}

/// Get performance metrics
async fn get_performance(State(state): State<AppState>) -> Json<PerformanceMetrics> {
    // In a real implementation, these would be collected over time
    let _metrics = state.metrics();
    let now = chrono::Utc::now().timestamp();

    // Generate sample data points for the last hour
    let mut timestamps = Vec::new();
    let mut qps = Vec::new();
    let mut latency_p50 = Vec::new();
    let mut latency_p99 = Vec::new();
    let mut memory = Vec::new();

    for i in 0..60 {
        timestamps.push(now - (59 - i) * 60);
        let variation = (i as f64 * 0.1).sin() * 0.2;
        qps.push(100.0 + variation * 50.0);
        latency_p50.push(5.0 + variation * 2.0);
        latency_p99.push(50.0 + variation * 20.0);
        memory.push(256.0 + variation * 32.0);
    }

    Json(PerformanceMetrics {
        timestamps,
        qps,
        latency_p50_ms: latency_p50,
        latency_p99_ms: latency_p99,
        memory_mb: memory,
    })
}

/// Get CDC subscriptions
#[derive(Debug, Clone, Serialize)]
pub struct CdcSubscription {
    pub id: String,
    pub table: String,
    pub status: String,
    pub events_processed: u64,
    pub lag_ms: u64,
}

async fn get_cdc_subscriptions(State(_state): State<AppState>) -> Json<Vec<CdcSubscription>> {
    // Would fetch from CDC manager
    Json(vec![])
}

/// Get server settings
#[derive(Debug, Clone, Serialize)]
pub struct ServerSettings {
    pub max_connections: u32,
    pub buffer_pool_size_mb: u32,
    pub wal_size_mb: u32,
    pub checkpoint_interval_sec: u32,
    pub log_level: String,
}

async fn get_settings(State(_state): State<AppState>) -> Json<ServerSettings> {
    Json(ServerSettings {
        max_connections: 100,
        buffer_pool_size_mb: 256,
        wal_size_mb: 1024,
        checkpoint_interval_sec: 300,
        log_level: "info".to_string(),
    })
}

// ============================================================================
// Dashboard Router
// ============================================================================

/// Create the dashboard router
pub fn dashboard_router(state: AppState) -> Router {
    Router::new()
        // Static assets
        .route("/", get(serve_dashboard))
        .route("/dashboard", get(serve_dashboard))
        .route("/dashboard/", get(serve_dashboard))
        .route("/dashboard/css/style.css", get(serve_css))
        .route("/dashboard/js/app.js", get(serve_js))
        // API endpoints for dashboard
        .route("/dashboard/api/stats", get(get_stats))
        .route("/dashboard/api/tables", get(get_tables))
        .route("/dashboard/api/tables/:name", get(get_table))
        .route("/dashboard/api/query", post(execute_query))
        .route("/dashboard/api/performance", get(get_performance))
        .route("/dashboard/api/cdc", get(get_cdc_subscriptions))
        .route("/dashboard/api/settings", get(get_settings))
        .with_state(state)
}

// ============================================================================
// Embedded Static Assets
// ============================================================================

const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ThunderDB Dashboard</title>
    <link rel="stylesheet" href="/dashboard/css/style.css">
</head>
<body>
    <div id="app">
        <nav class="sidebar">
            <div class="logo">
                <h1>⚡ ThunderDB</h1>
            </div>
            <ul class="nav-links">
                <li><a href="#" data-page="overview" class="active">Overview</a></li>
                <li><a href="#" data-page="query">Query Editor</a></li>
                <li><a href="#" data-page="tables">Tables</a></li>
                <li><a href="#" data-page="performance">Performance</a></li>
                <li><a href="#" data-page="cdc">CDC</a></li>
                <li><a href="#" data-page="settings">Settings</a></li>
            </ul>
        </nav>
        <main class="content">
            <!-- Overview Page -->
            <section id="page-overview" class="page active">
                <h2>Dashboard Overview</h2>
                <div class="stats-grid">
                    <div class="stat-card">
                        <h3>Uptime</h3>
                        <p id="stat-uptime">-</p>
                    </div>
                    <div class="stat-card">
                        <h3>Total Queries</h3>
                        <p id="stat-queries">-</p>
                    </div>
                    <div class="stat-card">
                        <h3>Queries/sec</h3>
                        <p id="stat-qps">-</p>
                    </div>
                    <div class="stat-card">
                        <h3>Connections</h3>
                        <p id="stat-connections">-</p>
                    </div>
                    <div class="stat-card">
                        <h3>Transactions</h3>
                        <p id="stat-transactions">-</p>
                    </div>
                    <div class="stat-card">
                        <h3>Buffer Hit Ratio</h3>
                        <p id="stat-buffer-hit">-</p>
                    </div>
                    <div class="stat-card">
                        <h3>Tables</h3>
                        <p id="stat-tables">-</p>
                    </div>
                    <div class="stat-card">
                        <h3>Memory</h3>
                        <p id="stat-memory">-</p>
                    </div>
                </div>
            </section>

            <!-- Query Editor Page -->
            <section id="page-query" class="page">
                <h2>Query Editor</h2>
                <div class="query-editor">
                    <textarea id="sql-input" placeholder="Enter SQL query...
SELECT * FROM users LIMIT 10;"></textarea>
                    <div class="query-actions">
                        <button id="btn-execute" class="btn btn-primary">Execute (Ctrl+Enter)</button>
                        <button id="btn-clear" class="btn">Clear</button>
                        <span id="query-status"></span>
                    </div>
                </div>
                <div class="query-results">
                    <div id="results-info"></div>
                    <div id="results-table-container">
                        <table id="results-table">
                            <thead></thead>
                            <tbody></tbody>
                        </table>
                    </div>
                    <div id="results-error" class="error-message"></div>
                </div>
            </section>

            <!-- Tables Page -->
            <section id="page-tables" class="page">
                <h2>Database Tables</h2>
                <div class="tables-list" id="tables-list">
                    <p>Loading tables...</p>
                </div>
                <div class="table-details" id="table-details">
                    <p>Select a table to view details</p>
                </div>
            </section>

            <!-- Performance Page -->
            <section id="page-performance" class="page">
                <h2>Performance Metrics</h2>
                <div class="performance-charts">
                    <div class="chart-container">
                        <h3>Queries Per Second</h3>
                        <canvas id="chart-qps"></canvas>
                    </div>
                    <div class="chart-container">
                        <h3>Latency (ms)</h3>
                        <canvas id="chart-latency"></canvas>
                    </div>
                    <div class="chart-container">
                        <h3>Memory Usage (MB)</h3>
                        <canvas id="chart-memory"></canvas>
                    </div>
                </div>
            </section>

            <!-- CDC Page -->
            <section id="page-cdc" class="page">
                <h2>Change Data Capture</h2>
                <div class="cdc-subscriptions" id="cdc-list">
                    <p>No CDC subscriptions configured</p>
                </div>
            </section>

            <!-- Settings Page -->
            <section id="page-settings" class="page">
                <h2>Server Settings</h2>
                <div class="settings-form" id="settings-form">
                    <div class="setting-group">
                        <label>Max Connections</label>
                        <input type="number" id="setting-max-connections" disabled>
                    </div>
                    <div class="setting-group">
                        <label>Buffer Pool Size (MB)</label>
                        <input type="number" id="setting-buffer-pool" disabled>
                    </div>
                    <div class="setting-group">
                        <label>WAL Size (MB)</label>
                        <input type="number" id="setting-wal-size" disabled>
                    </div>
                    <div class="setting-group">
                        <label>Checkpoint Interval (sec)</label>
                        <input type="number" id="setting-checkpoint" disabled>
                    </div>
                    <div class="setting-group">
                        <label>Log Level</label>
                        <select id="setting-log-level" disabled>
                            <option value="trace">Trace</option>
                            <option value="debug">Debug</option>
                            <option value="info">Info</option>
                            <option value="warn">Warn</option>
                            <option value="error">Error</option>
                        </select>
                    </div>
                </div>
            </section>
        </main>
    </div>
    <script src="/dashboard/js/app.js"></script>
</body>
</html>
"##;

const DASHBOARD_CSS: &str = r##"
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

:root {
    --bg-dark: #1a1a2e;
    --bg-medium: #16213e;
    --bg-light: #0f3460;
    --accent: #e94560;
    --accent-light: #ff6b6b;
    --text-primary: #eee;
    --text-secondary: #aaa;
    --success: #4ade80;
    --warning: #fbbf24;
    --error: #ef4444;
    --border: #2a2a4a;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
    background: var(--bg-dark);
    color: var(--text-primary);
    min-height: 100vh;
}

#app {
    display: flex;
    min-height: 100vh;
}

.sidebar {
    width: 240px;
    background: var(--bg-medium);
    padding: 20px;
    border-right: 1px solid var(--border);
    position: fixed;
    height: 100vh;
    overflow-y: auto;
}

.logo h1 {
    font-size: 1.5rem;
    color: var(--accent);
    margin-bottom: 2rem;
}

.nav-links {
    list-style: none;
}

.nav-links li {
    margin-bottom: 0.5rem;
}

.nav-links a {
    display: block;
    padding: 12px 16px;
    color: var(--text-secondary);
    text-decoration: none;
    border-radius: 8px;
    transition: all 0.2s;
}

.nav-links a:hover {
    background: var(--bg-light);
    color: var(--text-primary);
}

.nav-links a.active {
    background: var(--accent);
    color: white;
}

.content {
    flex: 1;
    margin-left: 240px;
    padding: 24px;
}

.page {
    display: none;
}

.page.active {
    display: block;
}

h2 {
    font-size: 1.75rem;
    margin-bottom: 1.5rem;
    color: var(--text-primary);
}

/* Stats Grid */
.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 1rem;
}

.stat-card {
    background: var(--bg-medium);
    padding: 1.5rem;
    border-radius: 12px;
    border: 1px solid var(--border);
}

.stat-card h3 {
    font-size: 0.875rem;
    color: var(--text-secondary);
    margin-bottom: 0.5rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.stat-card p {
    font-size: 1.75rem;
    font-weight: 600;
    color: var(--text-primary);
}

/* Query Editor */
.query-editor {
    background: var(--bg-medium);
    border-radius: 12px;
    padding: 1rem;
    margin-bottom: 1.5rem;
    border: 1px solid var(--border);
}

#sql-input {
    width: 100%;
    min-height: 150px;
    background: var(--bg-dark);
    border: 1px solid var(--border);
    border-radius: 8px;
    color: var(--text-primary);
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 14px;
    padding: 1rem;
    resize: vertical;
}

#sql-input:focus {
    outline: none;
    border-color: var(--accent);
}

.query-actions {
    display: flex;
    align-items: center;
    gap: 1rem;
    margin-top: 1rem;
}

.btn {
    padding: 10px 20px;
    border: none;
    border-radius: 8px;
    cursor: pointer;
    font-size: 14px;
    font-weight: 500;
    transition: all 0.2s;
}

.btn-primary {
    background: var(--accent);
    color: white;
}

.btn-primary:hover {
    background: var(--accent-light);
}

.btn:not(.btn-primary) {
    background: var(--bg-light);
    color: var(--text-primary);
}

.btn:not(.btn-primary):hover {
    background: var(--border);
}

#query-status {
    color: var(--text-secondary);
    font-size: 14px;
}

/* Query Results */
.query-results {
    background: var(--bg-medium);
    border-radius: 12px;
    border: 1px solid var(--border);
    overflow: hidden;
}

#results-info {
    padding: 1rem;
    background: var(--bg-light);
    border-bottom: 1px solid var(--border);
    font-size: 14px;
    color: var(--text-secondary);
}

#results-table-container {
    overflow-x: auto;
    max-height: 400px;
}

#results-table {
    width: 100%;
    border-collapse: collapse;
}

#results-table th,
#results-table td {
    padding: 12px 16px;
    text-align: left;
    border-bottom: 1px solid var(--border);
}

#results-table th {
    background: var(--bg-dark);
    font-weight: 600;
    position: sticky;
    top: 0;
}

#results-table tr:hover {
    background: var(--bg-light);
}

.error-message {
    padding: 1rem;
    color: var(--error);
    background: rgba(239, 68, 68, 0.1);
    display: none;
}

.error-message.show {
    display: block;
}

/* Tables List */
.tables-list {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 1rem;
    margin-bottom: 1.5rem;
}

.table-card {
    background: var(--bg-medium);
    padding: 1rem;
    border-radius: 8px;
    border: 1px solid var(--border);
    cursor: pointer;
    transition: all 0.2s;
}

.table-card:hover {
    border-color: var(--accent);
}

.table-card h4 {
    color: var(--accent);
    margin-bottom: 0.5rem;
}

.table-card p {
    font-size: 14px;
    color: var(--text-secondary);
}

.table-details {
    background: var(--bg-medium);
    padding: 1.5rem;
    border-radius: 12px;
    border: 1px solid var(--border);
}

.column-list {
    margin-top: 1rem;
}

.column-item {
    display: flex;
    justify-content: space-between;
    padding: 0.75rem 0;
    border-bottom: 1px solid var(--border);
}

.column-item:last-child {
    border-bottom: none;
}

.column-name {
    font-weight: 500;
}

.column-type {
    color: var(--accent);
    font-family: monospace;
}

/* Performance Charts */
.performance-charts {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
    gap: 1.5rem;
}

.chart-container {
    background: var(--bg-medium);
    padding: 1.5rem;
    border-radius: 12px;
    border: 1px solid var(--border);
}

.chart-container h3 {
    margin-bottom: 1rem;
    color: var(--text-secondary);
}

.chart-container canvas {
    width: 100% !important;
    height: 200px !important;
}

/* Settings */
.settings-form {
    background: var(--bg-medium);
    padding: 1.5rem;
    border-radius: 12px;
    border: 1px solid var(--border);
    max-width: 500px;
}

.setting-group {
    margin-bottom: 1rem;
}

.setting-group label {
    display: block;
    margin-bottom: 0.5rem;
    color: var(--text-secondary);
    font-size: 14px;
}

.setting-group input,
.setting-group select {
    width: 100%;
    padding: 10px 12px;
    background: var(--bg-dark);
    border: 1px solid var(--border);
    border-radius: 8px;
    color: var(--text-primary);
    font-size: 14px;
}

.setting-group input:focus,
.setting-group select:focus {
    outline: none;
    border-color: var(--accent);
}

/* CDC */
.cdc-subscriptions {
    background: var(--bg-medium);
    padding: 1.5rem;
    border-radius: 12px;
    border: 1px solid var(--border);
}

/* Responsive */
@media (max-width: 768px) {
    .sidebar {
        width: 100%;
        height: auto;
        position: relative;
    }

    .content {
        margin-left: 0;
    }

    #app {
        flex-direction: column;
    }

    .stats-grid {
        grid-template-columns: 1fr 1fr;
    }
}
"##;

const DASHBOARD_JS: &str = r##"
// ThunderDB Dashboard Application
class Dashboard {
    constructor() {
        this.currentPage = 'overview';
        this.statsInterval = null;
        this.init();
    }

    init() {
        this.setupNavigation();
        this.setupQueryEditor();
        this.loadStats();
        this.startStatsRefresh();
    }

    // Navigation
    setupNavigation() {
        document.querySelectorAll('.nav-links a').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const page = link.dataset.page;
                this.navigateTo(page);
            });
        });
    }

    navigateTo(page) {
        // Update nav links
        document.querySelectorAll('.nav-links a').forEach(link => {
            link.classList.toggle('active', link.dataset.page === page);
        });

        // Show/hide pages
        document.querySelectorAll('.page').forEach(p => {
            p.classList.toggle('active', p.id === `page-${page}`);
        });

        this.currentPage = page;

        // Load page-specific data
        switch (page) {
            case 'overview':
                this.loadStats();
                break;
            case 'tables':
                this.loadTables();
                break;
            case 'performance':
                this.loadPerformance();
                break;
            case 'cdc':
                this.loadCdc();
                break;
            case 'settings':
                this.loadSettings();
                break;
        }
    }

    // Stats
    async loadStats() {
        try {
            const response = await fetch('/dashboard/api/stats');
            const stats = await response.json();

            document.getElementById('stat-uptime').textContent = this.formatUptime(stats.uptime_seconds);
            document.getElementById('stat-queries').textContent = stats.queries_total.toLocaleString();
            document.getElementById('stat-qps').textContent = stats.queries_per_second.toFixed(2);
            document.getElementById('stat-connections').textContent = stats.active_connections;
            document.getElementById('stat-transactions').textContent = stats.active_transactions;
            document.getElementById('stat-buffer-hit').textContent = (stats.buffer_pool_hit_ratio * 100).toFixed(1) + '%';
            document.getElementById('stat-tables').textContent = stats.tables_count;
            document.getElementById('stat-memory').textContent = stats.memory_usage_mb.toFixed(1) + ' MB';
        } catch (error) {
            console.error('Failed to load stats:', error);
        }
    }

    formatUptime(seconds) {
        const days = Math.floor(seconds / 86400);
        const hours = Math.floor((seconds % 86400) / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);

        if (days > 0) return `${days}d ${hours}h`;
        if (hours > 0) return `${hours}h ${minutes}m`;
        return `${minutes}m ${seconds % 60}s`;
    }

    startStatsRefresh() {
        this.statsInterval = setInterval(() => {
            if (this.currentPage === 'overview') {
                this.loadStats();
            }
        }, 5000);
    }

    // Query Editor
    setupQueryEditor() {
        const sqlInput = document.getElementById('sql-input');
        const btnExecute = document.getElementById('btn-execute');
        const btnClear = document.getElementById('btn-clear');

        btnExecute.addEventListener('click', () => this.executeQuery());
        btnClear.addEventListener('click', () => {
            sqlInput.value = '';
            this.clearResults();
        });

        // Ctrl+Enter to execute
        sqlInput.addEventListener('keydown', (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                e.preventDefault();
                this.executeQuery();
            }
        });
    }

    async executeQuery() {
        const sql = document.getElementById('sql-input').value.trim();
        if (!sql) return;

        const status = document.getElementById('query-status');
        status.textContent = 'Executing...';

        try {
            const response = await fetch('/dashboard/api/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql })
            });

            const result = await response.json();
            this.displayResults(result);

            if (result.success) {
                status.textContent = `Completed in ${result.execution_time_ms}ms`;
            } else {
                status.textContent = 'Query failed';
            }
        } catch (error) {
            status.textContent = 'Error executing query';
            console.error('Query error:', error);
        }
    }

    displayResults(result) {
        const info = document.getElementById('results-info');
        const thead = document.querySelector('#results-table thead');
        const tbody = document.querySelector('#results-table tbody');
        const errorDiv = document.getElementById('results-error');

        // Clear previous results
        thead.innerHTML = '';
        tbody.innerHTML = '';
        errorDiv.classList.remove('show');

        if (!result.success) {
            errorDiv.textContent = result.error || 'Unknown error';
            errorDiv.classList.add('show');
            info.textContent = '';
            return;
        }

        // Show info
        const rowCount = result.rows.length;
        const affectedText = result.rows_affected ? ` (${result.rows_affected} rows affected)` : '';
        info.textContent = `${rowCount} row${rowCount !== 1 ? 's' : ''} returned${affectedText}`;

        if (result.columns.length === 0) return;

        // Build header
        const headerRow = document.createElement('tr');
        result.columns.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);

        // Build rows
        result.rows.forEach(row => {
            const tr = document.createElement('tr');
            row.forEach(cell => {
                const td = document.createElement('td');
                td.textContent = cell === null ? 'NULL' :
                    typeof cell === 'object' ? JSON.stringify(cell) : String(cell);
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
    }

    clearResults() {
        document.querySelector('#results-table thead').innerHTML = '';
        document.querySelector('#results-table tbody').innerHTML = '';
        document.getElementById('results-info').textContent = '';
        document.getElementById('results-error').classList.remove('show');
        document.getElementById('query-status').textContent = '';
    }

    // Tables
    async loadTables() {
        const container = document.getElementById('tables-list');
        container.innerHTML = '<p>Loading tables...</p>';

        try {
            const response = await fetch('/dashboard/api/tables');
            const tables = await response.json();

            if (tables.length === 0) {
                container.innerHTML = '<p>No tables found</p>';
                return;
            }

            container.innerHTML = '';
            tables.forEach(table => {
                const card = document.createElement('div');
                card.className = 'table-card';
                card.innerHTML = `
                    <h4>${table.name}</h4>
                    <p>${table.columns.length} columns</p>
                `;
                card.addEventListener('click', () => this.showTableDetails(table));
                container.appendChild(card);
            });
        } catch (error) {
            container.innerHTML = '<p>Failed to load tables</p>';
            console.error('Failed to load tables:', error);
        }
    }

    showTableDetails(table) {
        const details = document.getElementById('table-details');
        details.innerHTML = `
            <h3>${table.name}</h3>
            <div class="column-list">
                ${table.columns.map(col => `
                    <div class="column-item">
                        <span class="column-name">${col.name}${col.primary_key ? ' (PK)' : ''}</span>
                        <span class="column-type">${col.data_type}${col.nullable ? '' : ' NOT NULL'}</span>
                    </div>
                `).join('')}
            </div>
        `;
    }

    // Performance
    async loadPerformance() {
        try {
            const response = await fetch('/dashboard/api/performance');
            const data = await response.json();
            this.renderCharts(data);
        } catch (error) {
            console.error('Failed to load performance data:', error);
        }
    }

    renderCharts(data) {
        // Simple canvas-based charts (no external library needed)
        this.drawLineChart('chart-qps', data.timestamps, data.qps, '#e94560');
        this.drawLineChart('chart-latency', data.timestamps, data.latency_p99_ms, '#4ade80');
        this.drawLineChart('chart-memory', data.timestamps, data.memory_mb, '#fbbf24');
    }

    drawLineChart(canvasId, timestamps, values, color) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;

        const ctx = canvas.getContext('2d');
        const width = canvas.width = canvas.offsetWidth * 2;
        const height = canvas.height = 400;

        ctx.clearRect(0, 0, width, height);

        if (values.length === 0) return;

        const padding = 40;
        const chartWidth = width - padding * 2;
        const chartHeight = height - padding * 2;

        const minVal = Math.min(...values) * 0.9;
        const maxVal = Math.max(...values) * 1.1;
        const range = maxVal - minVal || 1;

        // Draw grid
        ctx.strokeStyle = '#2a2a4a';
        ctx.lineWidth = 1;
        for (let i = 0; i <= 4; i++) {
            const y = padding + (chartHeight * i / 4);
            ctx.beginPath();
            ctx.moveTo(padding, y);
            ctx.lineTo(width - padding, y);
            ctx.stroke();

            // Y-axis labels
            ctx.fillStyle = '#aaa';
            ctx.font = '20px sans-serif';
            const label = (maxVal - (range * i / 4)).toFixed(1);
            ctx.fillText(label, 5, y + 5);
        }

        // Draw line
        ctx.strokeStyle = color;
        ctx.lineWidth = 3;
        ctx.beginPath();

        values.forEach((val, i) => {
            const x = padding + (chartWidth * i / (values.length - 1));
            const y = padding + chartHeight - ((val - minVal) / range * chartHeight);

            if (i === 0) {
                ctx.moveTo(x, y);
            } else {
                ctx.lineTo(x, y);
            }
        });
        ctx.stroke();

        // Draw dots
        ctx.fillStyle = color;
        values.forEach((val, i) => {
            const x = padding + (chartWidth * i / (values.length - 1));
            const y = padding + chartHeight - ((val - minVal) / range * chartHeight);
            ctx.beginPath();
            ctx.arc(x, y, 4, 0, Math.PI * 2);
            ctx.fill();
        });
    }

    // CDC
    async loadCdc() {
        try {
            const response = await fetch('/dashboard/api/cdc');
            const subscriptions = await response.json();

            const container = document.getElementById('cdc-list');
            if (subscriptions.length === 0) {
                container.innerHTML = '<p>No CDC subscriptions configured. Create subscriptions via the API or SQL commands.</p>';
                return;
            }

            container.innerHTML = subscriptions.map(sub => `
                <div class="table-card">
                    <h4>${sub.table}</h4>
                    <p>Status: ${sub.status}</p>
                    <p>Events: ${sub.events_processed.toLocaleString()}</p>
                    <p>Lag: ${sub.lag_ms}ms</p>
                </div>
            `).join('');
        } catch (error) {
            console.error('Failed to load CDC data:', error);
        }
    }

    // Settings
    async loadSettings() {
        try {
            const response = await fetch('/dashboard/api/settings');
            const settings = await response.json();

            document.getElementById('setting-max-connections').value = settings.max_connections;
            document.getElementById('setting-buffer-pool').value = settings.buffer_pool_size_mb;
            document.getElementById('setting-wal-size').value = settings.wal_size_mb;
            document.getElementById('setting-checkpoint').value = settings.checkpoint_interval_sec;
            document.getElementById('setting-log-level').value = settings.log_level;
        } catch (error) {
            console.error('Failed to load settings:', error);
        }
    }
}

// Initialize dashboard when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new Dashboard();
});
"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dashboard_stats_serialization() {
        let stats = DashboardStats {
            uptime_seconds: 3600,
            queries_total: 1000,
            queries_per_second: 10.5,
            active_connections: 5,
            active_transactions: 2,
            buffer_pool_hit_ratio: 0.95,
            tables_count: 10,
            memory_usage_mb: 256.0,
        };

        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("uptime_seconds"));
        assert!(json.contains("3600"));
    }

    #[test]
    fn test_query_result_serialization() {
        let result = DashboardQueryResult {
            success: true,
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec![serde_json::json!(1), serde_json::json!("Alice")],
            ],
            rows_affected: Some(1),
            execution_time_ms: 15,
            error: None,
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"success\":true"));
    }
}
