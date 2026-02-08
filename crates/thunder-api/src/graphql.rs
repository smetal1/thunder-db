//! GraphQL API
//!
//! Provides GraphQL endpoint for ThunderDB with:
//! - Query operations
//! - Mutation operations
//! - Subscription support

use async_graphql::{
    Context, Object, Result as GqlResult, Schema, SimpleObject, Subscription,
};
use axum::{
    extract::State,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};
use thunder_common::prelude::Value;

use crate::AppState;

// ============================================================================
// GraphQL Types
// ============================================================================

/// Query result row
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct RowResult {
    /// Column values as JSON
    pub values: serde_json::Value,
}

/// Query execution result
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Rows of data
    pub rows: Vec<RowResult>,
    /// Number of rows affected (for mutations)
    pub rows_affected: Option<i64>,
    /// Execution time in milliseconds
    pub execution_time_ms: i64,
}

/// Table information
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub schema: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
    pub row_count: i64,
}

/// Column information
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
}

/// Index information
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: String,
}

/// Transaction state
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct TransactionInfo {
    pub id: String,
    pub isolation_level: String,
    pub read_only: bool,
    pub started_at: String,
}

/// Server health status
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub version: String,
    pub uptime_seconds: i64,
    pub connections: i64,
    pub queries_per_second: f64,
}

/// Query input
#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::InputObject)]
pub struct QueryInput {
    pub sql: String,
    #[graphql(default)]
    pub params: Vec<serde_json::Value>,
    pub transaction_id: Option<String>,
}

/// Transaction options input
#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::InputObject)]
pub struct TransactionOptionsInput {
    #[graphql(default = "READ_COMMITTED")]
    pub isolation_level: String,
    #[graphql(default = false)]
    pub read_only: bool,
}

/// Bulk insert input
#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::InputObject)]
pub struct BulkInsertInput {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

// ============================================================================
// Helper Functions
// ============================================================================

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
            let divisor = 10_i128.pow(*scale as u32);
            serde_json::Value::String(format!("{}.{:0>width$}", v / divisor, (v % divisor).abs(), width = *scale as usize))
        }
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Binary(b) => serde_json::Value::String(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b.as_ref())),
        Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::TimestampTz(_, _) => {
            serde_json::Value::String(format!("{}", value))
        }
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
// Query Root
// ============================================================================

/// Root query type
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Execute a SQL query
    async fn query(&self, ctx: &Context<'_>, input: QueryInput) -> GqlResult<QueryResult> {
        let state = ctx.data::<AppState>()?;
        let start = std::time::Instant::now();

        debug!(sql = %input.sql, "Executing GraphQL query");

        // Get the database engine
        let engine = match state.engine() {
            Some(e) => e,
            None => {
                // No engine - return empty result
                return Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: None,
                    execution_time_ms: start.elapsed().as_millis() as i64,
                });
            }
        };

        // Get or create a session
        let session_id = if let Some(txn_id) = &input.transaction_id {
            match state.get_session(txn_id) {
                Some(id) => id,
                None => return Err(async_graphql::Error::new("Transaction not found")),
            }
        } else {
            match state.get_or_create_session() {
                Some(id) => id,
                None => return Err(async_graphql::Error::new("Failed to create session")),
            }
        };

        // Execute the query
        let result = engine.execute_sql(session_id, &input.sql).await;

        // Clean up temporary session
        if input.transaction_id.is_none() {
            engine.close_session(session_id);
        }

        match result {
            Ok(query_result) => {
                let columns: Vec<String> = query_result.columns
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect();

                let rows: Vec<RowResult> = query_result.rows
                    .iter()
                    .map(|row| {
                        let values: Vec<serde_json::Value> = row.values
                            .iter()
                            .map(value_to_json)
                            .collect();
                        RowResult {
                            values: serde_json::Value::Array(values),
                        }
                    })
                    .collect();

                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: query_result.rows_affected.map(|n| n as i64),
                    execution_time_ms: start.elapsed().as_millis() as i64,
                })
            }
            Err(e) => {
                error!(error = %e, "Query execution failed");
                Err(async_graphql::Error::new(e.to_string()))
            }
        }
    }

    /// List all tables
    async fn tables(&self, ctx: &Context<'_>, _schema: Option<String>) -> GqlResult<Vec<TableInfo>> {
        let state = ctx.data::<AppState>()?;

        let engine = match state.engine() {
            Some(e) => e,
            None => return Ok(vec![]),
        };

        let table_names = engine.list_tables();

        let tables: Vec<TableInfo> = table_names
            .into_iter()
            .filter_map(|name| {
                engine.get_table(&name).map(|info| {
                    let columns: Vec<ColumnInfo> = info.schema.columns
                        .iter()
                        .map(|c| ColumnInfo {
                            name: c.name.clone(),
                            data_type: format!("{:?}", c.data_type),
                            nullable: c.nullable,
                            default_value: None,
                        })
                        .collect();

                    TableInfo {
                        name: info.name.clone(),
                        schema: "public".to_string(),
                        columns,
                        primary_key: vec![],
                        row_count: 0,
                    }
                })
            })
            .collect();

        Ok(tables)
    }

    /// Get table information
    async fn table(&self, ctx: &Context<'_>, name: String) -> GqlResult<Option<TableInfo>> {
        let state = ctx.data::<AppState>()?;

        let engine = match state.engine() {
            Some(e) => e,
            None => return Ok(None),
        };

        match engine.get_table(&name) {
            Some(info) => {
                let columns: Vec<ColumnInfo> = info.schema.columns
                    .iter()
                    .map(|c| ColumnInfo {
                        name: c.name.clone(),
                        data_type: format!("{:?}", c.data_type),
                        nullable: c.nullable,
                        default_value: None,
                    })
                    .collect();

                Ok(Some(TableInfo {
                    name: info.name.clone(),
                    schema: "public".to_string(),
                    columns,
                    primary_key: vec![],
                    row_count: 0,
                }))
            }
            None => Ok(None),
        }
    }

    /// List all indexes (derived from primary keys and table metadata)
    async fn indexes(&self, ctx: &Context<'_>, table: Option<String>) -> GqlResult<Vec<IndexInfo>> {
        let state = ctx.data::<AppState>()?;

        let engine = match state.engine() {
            Some(e) => e,
            None => return Ok(vec![]),
        };

        let tables = match table {
            Some(ref name) => vec![name.clone()],
            None => engine.list_tables(),
        };

        let mut indexes = Vec::new();
        for table_name in &tables {
            if let Some(info) = engine.get_table(table_name) {
                // Report primary key as an index
                if !info.primary_key.is_empty() {
                    let pk_columns: Vec<String> = info.primary_key.iter()
                        .filter_map(|&i| info.schema.columns.get(i).map(|c| c.name.clone()))
                        .collect();
                    indexes.push(IndexInfo {
                        name: format!("{}_pkey", table_name),
                        table_name: table_name.clone(),
                        columns: pk_columns,
                        unique: true,
                        index_type: "btree".to_string(),
                    });
                }
            }
        }

        Ok(indexes)
    }

    /// Get server health status
    async fn health(&self, ctx: &Context<'_>) -> GqlResult<HealthStatus> {
        let state = ctx.data::<AppState>()?;

        let (status, connections, qps) = match state.engine() {
            Some(engine) => {
                let stats = engine.stats();
                let qps = if stats.uptime_seconds > 0 {
                    stats.queries_total as f64 / stats.uptime_seconds as f64
                } else {
                    0.0
                };
                ("healthy", stats.active_connections as i64, qps)
            }
            None => ("degraded", 0, 0.0),
        };

        Ok(HealthStatus {
            status: status.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: state.uptime_seconds() as i64,
            connections,
            queries_per_second: qps,
        })
    }

    /// Get active transactions (summary from engine stats)
    async fn transactions(&self, ctx: &Context<'_>) -> GqlResult<Vec<TransactionInfo>> {
        let state = ctx.data::<AppState>()?;

        // The QueryEngine trait exposes aggregate stats but not individual
        // transaction details. Return the count as a single summary entry
        // when transactions are active.
        match state.engine() {
            Some(engine) => {
                let stats = engine.stats();
                if stats.active_transactions > 0 {
                    Ok(vec![TransactionInfo {
                        id: format!("{} active", stats.active_transactions),
                        isolation_level: "repeatable_read".to_string(),
                        read_only: false,
                        started_at: chrono::Utc::now().to_rfc3339(),
                    }])
                } else {
                    Ok(vec![])
                }
            }
            None => Ok(vec![]),
        }
    }
}

// ============================================================================
// Mutation Root
// ============================================================================

/// Root mutation type
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Execute a SQL mutation (INSERT, UPDATE, DELETE)
    async fn execute(&self, ctx: &Context<'_>, input: QueryInput) -> GqlResult<QueryResult> {
        let state = ctx.data::<AppState>()?;
        let start = std::time::Instant::now();

        debug!(sql = %input.sql, "Executing GraphQL mutation");

        // Get the database engine
        let engine = match state.engine() {
            Some(e) => e,
            None => {
                return Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: Some(0),
                    execution_time_ms: start.elapsed().as_millis() as i64,
                });
            }
        };

        // Get or create a session
        let session_id = if let Some(txn_id) = &input.transaction_id {
            match state.get_session(txn_id) {
                Some(id) => id,
                None => return Err(async_graphql::Error::new("Transaction not found")),
            }
        } else {
            match state.get_or_create_session() {
                Some(id) => id,
                None => return Err(async_graphql::Error::new("Failed to create session")),
            }
        };

        // Execute the mutation
        let result = engine.execute_sql(session_id, &input.sql).await;

        // Clean up temporary session
        if input.transaction_id.is_none() {
            engine.close_session(session_id);
        }

        match result {
            Ok(query_result) => {
                let columns: Vec<String> = query_result.columns
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect();

                let rows: Vec<RowResult> = query_result.rows
                    .iter()
                    .map(|row| {
                        let values: Vec<serde_json::Value> = row.values
                            .iter()
                            .map(value_to_json)
                            .collect();
                        RowResult {
                            values: serde_json::Value::Array(values),
                        }
                    })
                    .collect();

                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: query_result.rows_affected.map(|n| n as i64),
                    execution_time_ms: start.elapsed().as_millis() as i64,
                })
            }
            Err(e) => {
                error!(error = %e, "Mutation execution failed");
                Err(async_graphql::Error::new(e.to_string()))
            }
        }
    }

    /// Begin a transaction
    async fn begin_transaction(
        &self,
        ctx: &Context<'_>,
        options: Option<TransactionOptionsInput>,
    ) -> GqlResult<TransactionInfo> {
        let state = ctx.data::<AppState>()?;
        let opts = options.unwrap_or_else(|| TransactionOptionsInput {
            isolation_level: "READ_COMMITTED".to_string(),
            read_only: false,
        });

        let transaction_id = uuid::Uuid::new_v4().to_string();

        // Get the database engine
        let engine = match state.engine() {
            Some(e) => e,
            None => {
                // No engine - return stub response
                return Ok(TransactionInfo {
                    id: transaction_id,
                    isolation_level: opts.isolation_level,
                    read_only: opts.read_only,
                    started_at: chrono::Utc::now().to_rfc3339(),
                });
            }
        };

        // Create a session for this transaction
        let session_id = engine.create_session();

        // Begin the transaction
        match engine.begin_transaction(session_id).await {
            Ok(txn_id) => {
                info!(transaction_id = %transaction_id, txn_id = ?txn_id, "Transaction started via GraphQL");

                // Register the mapping
                state.register_session(transaction_id.clone(), session_id);

                Ok(TransactionInfo {
                    id: transaction_id,
                    isolation_level: opts.isolation_level,
                    read_only: opts.read_only,
                    started_at: chrono::Utc::now().to_rfc3339(),
                })
            }
            Err(e) => {
                engine.close_session(session_id);
                error!(error = %e, "Failed to begin transaction");
                Err(async_graphql::Error::new(e.to_string()))
            }
        }
    }

    /// Commit a transaction
    async fn commit_transaction(&self, ctx: &Context<'_>, transaction_id: String) -> GqlResult<bool> {
        let state = ctx.data::<AppState>()?;

        debug!(transaction_id = %transaction_id, "Committing transaction via GraphQL");

        let engine = match state.engine() {
            Some(e) => e,
            None => return Ok(true),
        };

        let session_id = match state.get_session(&transaction_id) {
            Some(sid) => sid,
            None => return Err(async_graphql::Error::new("Transaction not found")),
        };

        match engine.commit_transaction(session_id).await {
            Ok(_) => {
                info!(transaction_id = %transaction_id, "Transaction committed via GraphQL");
                state.remove_session(&transaction_id);
                engine.close_session(session_id);
                Ok(true)
            }
            Err(e) => {
                state.remove_session(&transaction_id);
                engine.close_session(session_id);
                Err(async_graphql::Error::new(e.to_string()))
            }
        }
    }

    /// Rollback a transaction
    async fn rollback_transaction(&self, ctx: &Context<'_>, transaction_id: String) -> GqlResult<bool> {
        let state = ctx.data::<AppState>()?;

        debug!(transaction_id = %transaction_id, "Rolling back transaction via GraphQL");

        let engine = match state.engine() {
            Some(e) => e,
            None => return Ok(true),
        };

        let session_id = match state.get_session(&transaction_id) {
            Some(sid) => sid,
            None => return Err(async_graphql::Error::new("Transaction not found")),
        };

        match engine.rollback_transaction(session_id).await {
            Ok(_) => {
                info!(transaction_id = %transaction_id, "Transaction rolled back via GraphQL");
                state.remove_session(&transaction_id);
                engine.close_session(session_id);
                Ok(true)
            }
            Err(e) => {
                state.remove_session(&transaction_id);
                engine.close_session(session_id);
                Err(async_graphql::Error::new(e.to_string()))
            }
        }
    }

    /// Bulk insert rows
    async fn bulk_insert(&self, ctx: &Context<'_>, input: BulkInsertInput) -> GqlResult<i64> {
        let state = ctx.data::<AppState>()?;

        debug!(table = %input.table, rows = input.rows.len(), "Bulk insert via GraphQL");

        let engine = match state.engine() {
            Some(e) => e,
            None => return Ok(input.rows.len() as i64),
        };

        // Build INSERT statement
        let columns_str = input.columns.join(", ");
        let mut values_parts: Vec<String> = Vec::new();

        for row in &input.rows {
            let row_values: Vec<String> = row.iter().map(|v| json_to_sql_literal(v)).collect();
            values_parts.push(format!("({})", row_values.join(", ")));
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            input.table,
            columns_str,
            values_parts.join(", ")
        );

        // Create session and execute
        let session_id = match state.get_or_create_session() {
            Some(id) => id,
            None => return Err(async_graphql::Error::new("Failed to create session")),
        };

        let result = engine.execute_sql(session_id, &sql).await;
        engine.close_session(session_id);

        match result {
            Ok(query_result) => Ok(query_result.rows_affected.unwrap_or(input.rows.len() as u64) as i64),
            Err(e) => Err(async_graphql::Error::new(e.to_string())),
        }
    }

    /// Create a table
    async fn create_table(
        &self,
        ctx: &Context<'_>,
        name: String,
        columns: Vec<ColumnDefinitionInput>,
        primary_key: Option<Vec<String>>,
    ) -> GqlResult<TableInfo> {
        let state = ctx.data::<AppState>()?;

        debug!(name = %name, "Creating table via GraphQL");

        let engine = match state.engine() {
            Some(e) => e,
            None => {
                return Ok(TableInfo {
                    name,
                    schema: "public".to_string(),
                    columns: columns
                        .into_iter()
                        .map(|c| ColumnInfo {
                            name: c.name,
                            data_type: c.data_type,
                            nullable: c.nullable,
                            default_value: c.default_value,
                        })
                        .collect(),
                    primary_key: primary_key.unwrap_or_default(),
                    row_count: 0,
                });
            }
        };

        // Build CREATE TABLE statement
        let column_defs: Vec<String> = columns.iter().map(|c| {
            let mut def = format!("{} {}", c.name, c.data_type);
            if !c.nullable {
                def.push_str(" NOT NULL");
            }
            if let Some(ref default) = c.default_value {
                def.push_str(&format!(" DEFAULT {}", default));
            }
            def
        }).collect();

        let mut sql = format!("CREATE TABLE {} ({})", name, column_defs.join(", "));

        if let Some(ref pk) = primary_key {
            if !pk.is_empty() {
                sql.push_str(&format!(", PRIMARY KEY ({})", pk.join(", ")));
            }
        }

        sql.push(')');

        // Execute DDL
        let session_id = match state.get_or_create_session() {
            Some(id) => id,
            None => return Err(async_graphql::Error::new("Failed to create session")),
        };

        let result = engine.execute_sql(session_id, &sql).await;
        engine.close_session(session_id);

        match result {
            Ok(_) => {
                info!(name = %name, "Table created via GraphQL");
                Ok(TableInfo {
                    name,
                    schema: "public".to_string(),
                    columns: columns
                        .into_iter()
                        .map(|c| ColumnInfo {
                            name: c.name,
                            data_type: c.data_type,
                            nullable: c.nullable,
                            default_value: c.default_value,
                        })
                        .collect(),
                    primary_key: primary_key.unwrap_or_default(),
                    row_count: 0,
                })
            }
            Err(e) => Err(async_graphql::Error::new(e.to_string())),
        }
    }

    /// Drop a table
    async fn drop_table(&self, ctx: &Context<'_>, name: String, if_exists: Option<bool>) -> GqlResult<bool> {
        let state = ctx.data::<AppState>()?;

        debug!(name = %name, if_exists = ?if_exists, "Dropping table via GraphQL");

        let engine = match state.engine() {
            Some(e) => e,
            None => return Ok(true),
        };

        let sql = if if_exists.unwrap_or(false) {
            format!("DROP TABLE IF EXISTS {}", name)
        } else {
            format!("DROP TABLE {}", name)
        };

        let session_id = match state.get_or_create_session() {
            Some(id) => id,
            None => return Err(async_graphql::Error::new("Failed to create session")),
        };

        let result = engine.execute_sql(session_id, &sql).await;
        engine.close_session(session_id);

        match result {
            Ok(_) => {
                info!(name = %name, "Table dropped via GraphQL");
                Ok(true)
            }
            Err(e) => Err(async_graphql::Error::new(e.to_string())),
        }
    }

    /// Create an index
    async fn create_index(
        &self,
        ctx: &Context<'_>,
        name: String,
        table: String,
        columns: Vec<String>,
        unique: Option<bool>,
    ) -> GqlResult<IndexInfo> {
        let state = ctx.data::<AppState>()?;

        debug!(name = %name, table = %table, "Creating index via GraphQL");

        let is_unique = unique.unwrap_or(false);

        let engine = match state.engine() {
            Some(e) => e,
            None => {
                return Ok(IndexInfo {
                    name,
                    table_name: table,
                    columns,
                    unique: is_unique,
                    index_type: "btree".to_string(),
                });
            }
        };

        let unique_str = if is_unique { "UNIQUE " } else { "" };
        let sql = format!(
            "CREATE {}INDEX {} ON {} ({})",
            unique_str, name, table, columns.join(", ")
        );

        let session_id = match state.get_or_create_session() {
            Some(id) => id,
            None => return Err(async_graphql::Error::new("Failed to create session")),
        };

        let result = engine.execute_sql(session_id, &sql).await;
        engine.close_session(session_id);

        match result {
            Ok(_) => {
                info!(name = %name, "Index created via GraphQL");
                Ok(IndexInfo {
                    name,
                    table_name: table,
                    columns,
                    unique: is_unique,
                    index_type: "btree".to_string(),
                })
            }
            Err(e) => Err(async_graphql::Error::new(e.to_string())),
        }
    }

    /// Drop an index
    async fn drop_index(&self, ctx: &Context<'_>, name: String) -> GqlResult<bool> {
        let state = ctx.data::<AppState>()?;

        debug!(name = %name, "Dropping index via GraphQL");

        let engine = match state.engine() {
            Some(e) => e,
            None => return Ok(true),
        };

        let sql = format!("DROP INDEX {}", name);

        let session_id = match state.get_or_create_session() {
            Some(id) => id,
            None => return Err(async_graphql::Error::new("Failed to create session")),
        };

        let result = engine.execute_sql(session_id, &sql).await;
        engine.close_session(session_id);

        match result {
            Ok(_) => {
                info!(name = %name, "Index dropped via GraphQL");
                Ok(true)
            }
            Err(e) => Err(async_graphql::Error::new(e.to_string())),
        }
    }
}

/// Convert JSON value to SQL literal
fn json_to_sql_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(arr) => {
            let elements: Vec<String> = arr.iter().map(json_to_sql_literal).collect();
            format!("ARRAY[{}]", elements.join(", "))
        }
        serde_json::Value::Object(_) => format!("'{}'", value.to_string().replace('\'', "''")),
    }
}

/// Column definition input
#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::InputObject)]
pub struct ColumnDefinitionInput {
    pub name: String,
    pub data_type: String,
    #[graphql(default = true)]
    pub nullable: bool,
    pub default_value: Option<String>,
}

// ============================================================================
// Subscription Root
// ============================================================================

/// Root subscription type for real-time updates
pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    /// Subscribe to table changes via CDC
    async fn table_changes(
        &self,
        ctx: &Context<'_>,
        table: String,
        operations: Option<Vec<String>>,
    ) -> impl futures::Stream<Item = ChangeEvent> {
        info!(table = %table, "New GraphQL subscription for table changes");

        let state = ctx.data::<AppState>().ok().cloned();
        let op_filter: Option<Vec<String>> = operations.map(|ops| {
            ops.into_iter().map(|o| o.to_uppercase()).collect()
        });

        match state.and_then(|s| s.cdc_manager().cloned()) {
            Some(manager) => {
                let rx = manager.subscribe();
                let stream = tokio_stream::wrappers::BroadcastStream::new(rx)
                    .filter_map(move |result| {
                        let table = table.clone();
                        let op_filter = op_filter.clone();
                        async move {
                            let event = result.ok()?;
                            // Filter by table name
                            if event.table != table {
                                return None;
                            }
                            let op_str = format!("{:?}", event.operation).to_uppercase();
                            // Filter by operation type if specified
                            if let Some(ref ops) = op_filter {
                                if !ops.contains(&op_str) {
                                    return None;
                                }
                            }
                            Some(ChangeEvent {
                                table: event.table,
                                operation: op_str,
                                old_row: event.before.map(|row| {
                                    serde_json::to_value(&row).unwrap_or_default()
                                }),
                                new_row: event.after.map(|row| {
                                    serde_json::to_value(&row).unwrap_or_default()
                                }),
                                timestamp: event.timestamp.to_rfc3339(),
                            })
                        }
                    });
                futures::stream::StreamExt::boxed(stream)
            }
            None => {
                // No CDC manager - return empty stream
                futures::stream::StreamExt::boxed(futures::stream::pending())
            }
        }
    }

    /// Subscribe to query notifications (LISTEN/NOTIFY)
    async fn notifications(
        &self,
        ctx: &Context<'_>,
        channel: String,
    ) -> impl futures::Stream<Item = Notification> {
        info!(channel = %channel, "New notification subscription");

        let state = ctx.data::<AppState>().ok().cloned();

        match state.and_then(|s| s.cdc_manager().cloned()) {
            Some(manager) => {
                // Notifications are delivered through the CDC event stream
                // filtering for NOTIFY events on the specified channel
                let rx = manager.subscribe();
                let stream = tokio_stream::wrappers::BroadcastStream::new(rx)
                    .filter_map(move |result| {
                        let channel = channel.clone();
                        async move {
                            let event = result.ok()?;
                            // Match NOTIFY events on this channel
                            if event.table == channel {
                                let payload = event.after
                                    .and_then(|row| row.values.first().map(|v| format!("{}", v)))
                                    .unwrap_or_default();
                                Some(Notification {
                                    channel,
                                    payload,
                                    timestamp: event.timestamp.to_rfc3339(),
                                })
                            } else {
                                None
                            }
                        }
                    });
                futures::stream::StreamExt::boxed(stream)
            }
            None => {
                futures::stream::StreamExt::boxed(futures::stream::pending())
            }
        }
    }
}

/// Change event for subscriptions
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub table: String,
    pub operation: String, // INSERT, UPDATE, DELETE
    pub old_row: Option<serde_json::Value>,
    pub new_row: Option<serde_json::Value>,
    pub timestamp: String,
}

/// Notification event
#[derive(Debug, Clone, SimpleObject, Serialize, Deserialize)]
pub struct Notification {
    pub channel: String,
    pub payload: String,
    pub timestamp: String,
}

// ============================================================================
// Schema Builder
// ============================================================================

/// Build the GraphQL schema
pub type ThunderSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

pub fn build_schema(state: AppState) -> ThunderSchema {
    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(state)
        .finish()
}

// ============================================================================
// GraphQL Request/Response types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct GraphQLRequest {
    pub query: String,
    #[serde(default)]
    pub operation_name: Option<String>,
    #[serde(default)]
    pub variables: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct GraphQLResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<GraphQLError>,
}

#[derive(Debug, Serialize)]
pub struct GraphQLError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locations: Option<Vec<Location>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct Location {
    pub line: usize,
    pub column: usize,
}

// ============================================================================
// Axum Handlers
// ============================================================================

/// State for GraphQL handlers
#[derive(Clone)]
pub struct GraphQLState {
    pub schema: ThunderSchema,
}

/// Handle GraphQL query/mutation requests
pub async fn graphql_handler(
    State(state): State<GraphQLState>,
    Json(request): Json<GraphQLRequest>,
) -> Json<GraphQLResponse> {
    let mut gql_request = async_graphql::Request::new(&request.query);

    if let Some(op_name) = request.operation_name {
        gql_request = gql_request.operation_name(op_name);
    }

    if let Some(vars) = request.variables {
        let variables = async_graphql::Variables::from_json(vars);
        gql_request = gql_request.variables(variables);
    }

    let response = state.schema.execute(gql_request).await;

    let data = if response.data != async_graphql::Value::Null {
        Some(response.data.into_json().unwrap_or(serde_json::Value::Null))
    } else {
        None
    };

    let errors: Vec<GraphQLError> = response.errors.into_iter().map(|e| {
        GraphQLError {
            message: e.message,
            locations: if e.locations.is_empty() {
                None
            } else {
                Some(e.locations.into_iter().map(|l| Location {
                    line: l.line,
                    column: l.column,
                }).collect())
            },
            path: if e.path.is_empty() {
                None
            } else {
                Some(e.path.iter().map(|p| {
                    match p {
                        async_graphql::PathSegment::Field(s) => s.to_string(),
                        async_graphql::PathSegment::Index(i) => i.to_string(),
                    }
                }).collect())
            },
        }
    }).collect();

    Json(GraphQLResponse { data, errors })
}

/// GraphQL playground/GraphiQL handler
pub async fn graphql_playground() -> Html<String> {
    Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql")
            .subscription_endpoint("/graphql/ws"),
    ))
}

// ============================================================================
// Router
// ============================================================================

/// Create GraphQL routes
pub fn graphql_router(state: AppState) -> Router {
    let schema = build_schema(state);
    let gql_state = GraphQLState { schema };

    Router::new()
        .route("/graphql", post(graphql_handler))
        .route("/graphql", get(graphql_playground))
        .with_state(gql_state)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_query() {
        let state = AppState::new();
        let schema = build_schema(state);

        let result = schema
            .execute("{ health { status version } }")
            .await;

        assert!(result.errors.is_empty());
        let data = result.data.into_json().unwrap();
        // Without engine it should be degraded
        assert_eq!(data["health"]["status"], "degraded");
    }

    #[tokio::test]
    async fn test_tables_query() {
        let state = AppState::new();
        let schema = build_schema(state);

        let result = schema
            .execute("{ tables { name schema } }")
            .await;

        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_query_execution() {
        let state = AppState::new();
        let schema = build_schema(state);

        let result = schema
            .execute(r#"
                query {
                    query(input: { sql: "SELECT 1" }) {
                        columns
                        rows { values }
                        executionTimeMs
                    }
                }
            "#)
            .await;

        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        let state = AppState::new();
        let schema = build_schema(state);

        let result = schema
            .execute(r#"
                mutation {
                    beginTransaction {
                        id
                        isolationLevel
                    }
                }
            "#)
            .await;

        assert!(result.errors.is_empty());
        let data = result.data.into_json().unwrap();
        assert!(!data["beginTransaction"]["id"].as_str().unwrap().is_empty());
    }

    #[test]
    fn test_query_input() {
        let input = QueryInput {
            sql: "SELECT * FROM users".to_string(),
            params: vec![],
            transaction_id: None,
        };

        assert_eq!(input.sql, "SELECT * FROM users");
    }

    #[test]
    fn test_json_to_sql_literal() {
        assert_eq!(json_to_sql_literal(&serde_json::json!(null)), "NULL");
        assert_eq!(json_to_sql_literal(&serde_json::json!(true)), "TRUE");
        assert_eq!(json_to_sql_literal(&serde_json::json!(42)), "42");
        assert_eq!(json_to_sql_literal(&serde_json::json!("hello")), "'hello'");
        assert_eq!(json_to_sql_literal(&serde_json::json!("it's")), "'it''s'");
    }
}
