//! # Thunder Client
//!
//! Native Rust client for ThunderDB providing:
//! - Connection pooling
//! - Prepared statements
//! - Transaction support
//! - Async/await API

pub mod connection;
pub mod pool;
pub mod statement;
pub mod transaction;

use std::sync::Arc;

use thunder_common::prelude::*;
use tracing::info;

pub use connection::{Connection, ConnectionState};
pub use pool::{ConnectionPool, PoolConfig, PooledConnection, PoolStats};
pub use statement::{PreparedStatement, StatementBuilder, StatementCache, CacheStats};
pub use transaction::{IsolationLevel, Transaction, TransactionBuilder, TransactionOptions};

// ============================================================================
// Client Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub pool_size: usize,
    pub connect_timeout_ms: u64,
    pub query_timeout_ms: u64,
    pub ssl_mode: SslMode,
    pub application_name: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            user: "thunder".to_string(),
            password: None,
            database: "thunder".to_string(),
            pool_size: 10,
            connect_timeout_ms: 5000,
            query_timeout_ms: 30000,
            ssl_mode: SslMode::Prefer,
            application_name: None,
        }
    }
}

impl ClientConfig {
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    Disable,
    #[default]
    Prefer,
    Require,
}

// ============================================================================
// Client Configuration Builder
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct ClientConfigBuilder {
    config: ClientConfig,
}

impl ClientConfigBuilder {
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.config.host = host.into();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.config.user = user.into();
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.config.password = Some(password.into());
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.config.database = database.into();
        self
    }

    pub fn pool_size(mut self, size: usize) -> Self {
        self.config.pool_size = size;
        self
    }

    pub fn connect_timeout_ms(mut self, ms: u64) -> Self {
        self.config.connect_timeout_ms = ms;
        self
    }

    pub fn query_timeout_ms(mut self, ms: u64) -> Self {
        self.config.query_timeout_ms = ms;
        self
    }

    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.config.ssl_mode = mode;
        self
    }

    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.config.application_name = Some(name.into());
        self
    }

    pub fn build(self) -> ClientConfig {
        self.config
    }
}

// ============================================================================
// ThunderDB Client
// ============================================================================

pub struct Client {
    #[allow(dead_code)]
    config: ClientConfig,
    pool: ConnectionPool,
    statement_cache: Arc<StatementCache>,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let pool_config = PoolConfig {
            min_size: 1,
            max_size: config.pool_size,
            acquire_timeout_ms: config.connect_timeout_ms,
            ..Default::default()
        };

        let pool = ConnectionPool::new(pool_config, config.clone()).await?;
        let statement_cache = Arc::new(StatementCache::new(100));

        info!(
            host = %config.host,
            port = config.port,
            database = %config.database,
            "ThunderDB client created"
        );

        Ok(Self { config, pool, statement_cache })
    }

    pub async fn connect(conn_str: &str) -> Result<Self> {
        let config = parse_connection_string(conn_str)?;
        Self::new(config).await
    }

    pub async fn connection(&self) -> Result<PooledConnection> {
        self.pool.acquire().await
    }

    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let mut conn = self.pool.acquire().await?;

        if params.is_empty() {
            conn.query(sql).await
        } else {
            let stmt = self.statement_cache.get_or_create(sql);
            let prepared = PreparedStatement::new(stmt.name.clone(), stmt.sql.clone());
            prepared.query(&mut conn, params).await
        }
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let mut conn = self.pool.acquire().await?;

        if params.is_empty() {
            conn.execute(sql).await
        } else {
            let stmt = self.statement_cache.get_or_create(sql);
            let prepared = PreparedStatement::new(stmt.name.clone(), stmt.sql.clone());
            prepared.execute(&mut conn, params).await
        }
    }

    pub async fn begin(&self) -> Result<OwnedTransaction> {
        let conn = self.pool.acquire().await?;
        OwnedTransaction::begin(conn).await
    }

    pub async fn begin_with_options(&self, options: TransactionOptions) -> Result<OwnedTransaction> {
        let conn = self.pool.acquire().await?;
        OwnedTransaction::begin_with_options(conn, options).await
    }

    pub fn prepare(&self, sql: &str) -> PreparedStatement {
        let cached = self.statement_cache.get_or_create(sql);
        PreparedStatement::new(cached.name.clone(), cached.sql.clone())
    }

    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.connection_mut().ping().await
    }

    pub fn pool_stats(&self) -> PoolStats {
        self.pool.stats()
    }

    pub fn cache_stats(&self) -> CacheStats {
        self.statement_cache.stats()
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}

// ============================================================================
// Owned Transaction
// ============================================================================

pub struct OwnedTransaction {
    conn: PooledConnection,
    id: String,
    finished: bool,
}

impl OwnedTransaction {
    pub async fn begin(conn: PooledConnection) -> Result<Self> {
        Self::begin_with_options(conn, TransactionOptions::default()).await
    }

    pub async fn begin_with_options(mut conn: PooledConnection, options: TransactionOptions) -> Result<Self> {
        let id = uuid::Uuid::new_v4().to_string();
        let sql = build_begin_sql(&options);
        conn.execute(&sql).await?;

        Ok(Self { conn, id, finished: false })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        if self.finished {
            return Err(Error::Internal("Transaction already finished".to_string()));
        }
        self.conn.query(sql).await
    }

    pub async fn execute(&mut self, sql: &str) -> Result<u64> {
        if self.finished {
            return Err(Error::Internal("Transaction already finished".to_string()));
        }
        self.conn.execute(sql).await
    }

    pub async fn commit(mut self) -> Result<()> {
        if self.finished {
            return Err(Error::Internal("Transaction already finished".to_string()));
        }
        self.finished = true;
        self.conn.execute("COMMIT").await?;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<()> {
        if self.finished {
            return Err(Error::Internal("Transaction already finished".to_string()));
        }
        self.finished = true;
        self.conn.execute("ROLLBACK").await?;
        Ok(())
    }
}

impl Drop for OwnedTransaction {
    fn drop(&mut self) {
        if !self.finished {
            tracing::warn!(txn_id = %self.id, "Transaction dropped without commit/rollback");
        }
    }
}

// ============================================================================
// Query Result
// ============================================================================

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

impl QueryResult {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn first(&self) -> Option<&Row> {
        self.rows.first()
    }

    pub fn get_one<T: FromValue>(&self, column: usize) -> Option<T> {
        self.first()
            .and_then(|row| row.get(column))
            .and_then(|v| T::from_value(v))
    }

    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }
}

impl IntoIterator for QueryResult {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

// ============================================================================
// Value Conversion Trait
// ============================================================================

pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Option<Self>;
}

impl FromValue for i32 {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Int32(v) => Some(*v),
            Value::Int64(v) => (*v).try_into().ok(),
            _ => None,
        }
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_i64()
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_f64()
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_str().map(|s| s.to_string())
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_bool()
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Option<Self> {
        if value.is_null() {
            Some(None)
        } else {
            T::from_value(value).map(Some)
        }
    }
}

// ============================================================================
// Connection String Parser
// ============================================================================

fn parse_connection_string(conn_str: &str) -> Result<ClientConfig> {
    let url = url::Url::parse(conn_str)
        .map_err(|e| Error::Config(format!("Invalid connection string: {}", e)))?;

    let mut config = ClientConfig {
        host: url.host_str().unwrap_or("localhost").to_string(),
        port: url.port().unwrap_or(5432),
        user: if url.username().is_empty() {
            "thunder".to_string()
        } else {
            url.username().to_string()
        },
        password: url.password().map(|s| s.to_string()),
        database: url.path().trim_start_matches('/').to_string(),
        ..Default::default()
    };

    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "pool_size" => {
                if let Ok(size) = value.parse() {
                    config.pool_size = size;
                }
            }
            "connect_timeout" => {
                if let Ok(ms) = value.parse() {
                    config.connect_timeout_ms = ms;
                }
            }
            "query_timeout" => {
                if let Ok(ms) = value.parse() {
                    config.query_timeout_ms = ms;
                }
            }
            "sslmode" => {
                config.ssl_mode = match value.as_ref() {
                    "disable" => SslMode::Disable,
                    "prefer" => SslMode::Prefer,
                    "require" => SslMode::Require,
                    _ => SslMode::Prefer,
                };
            }
            "application_name" => {
                config.application_name = Some(value.to_string());
            }
            _ => {}
        }
    }

    if config.database.is_empty() {
        config.database = config.user.clone();
    }

    Ok(config)
}

// ============================================================================
// Helpers
// ============================================================================

fn build_begin_sql(options: &TransactionOptions) -> String {
    let isolation = match options.isolation_level {
        IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
        IsolationLevel::ReadCommitted => "READ COMMITTED",
        IsolationLevel::RepeatableRead => "REPEATABLE READ",
        IsolationLevel::Serializable => "SERIALIZABLE",
    };

    let mut parts = vec!["BEGIN", "ISOLATION LEVEL", isolation];

    if options.read_only {
        parts.push("READ ONLY");
    } else {
        parts.push("READ WRITE");
    }

    if options.deferrable && options.read_only && options.isolation_level == IsolationLevel::Serializable {
        parts.push("DEFERRABLE");
    }

    parts.join(" ")
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_connection_string() {
        let config = parse_connection_string("thunderdb://alice:secret@localhost:5432/mydb").unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.user, "alice");
        assert_eq!(config.password, Some("secret".to_string()));
        assert_eq!(config.database, "mydb");
    }

    #[test]
    fn test_parse_connection_string_with_options() {
        let config = parse_connection_string(
            "thunderdb://user@localhost/db?pool_size=20&sslmode=require"
        ).unwrap();
        assert_eq!(config.pool_size, 20);
        assert_eq!(config.ssl_mode, SslMode::Require);
    }

    #[test]
    fn test_default_config() {
        let config = ClientConfig::default();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.pool_size, 10);
    }

    #[test]
    fn test_config_builder() {
        let config = ClientConfig::builder()
            .host("db.example.com")
            .port(5433)
            .user("admin")
            .password("secret123")
            .database("production")
            .pool_size(50)
            .ssl_mode(SslMode::Require)
            .build();

        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 5433);
        assert_eq!(config.user, "admin");
        assert_eq!(config.password, Some("secret123".to_string()));
        assert_eq!(config.database, "production");
        assert_eq!(config.pool_size, 50);
        assert_eq!(config.ssl_mode, SslMode::Require);
    }

    #[test]
    fn test_query_result() {
        let result = QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                Row::new(vec![Value::Int32(1), Value::String(Arc::from("alice"))]),
                Row::new(vec![Value::Int32(2), Value::String(Arc::from("bob"))]),
            ],
        };

        assert_eq!(result.len(), 2);
        assert!(!result.is_empty());
        assert!(result.first().is_some());
    }

    #[test]
    fn test_from_value() {
        assert_eq!(i32::from_value(&Value::Int32(42)), Some(42));
        assert_eq!(i64::from_value(&Value::Int64(100)), Some(100));
        assert_eq!(f64::from_value(&Value::Float64(3.14)), Some(3.14));
        assert_eq!(String::from_value(&Value::String(Arc::from("hello"))), Some("hello".to_string()));
        assert_eq!(bool::from_value(&Value::Boolean(true)), Some(true));

        assert_eq!(Option::<i32>::from_value(&Value::Null), Some(None));
        assert_eq!(Option::<i32>::from_value(&Value::Int32(42)), Some(Some(42)));
    }
}
