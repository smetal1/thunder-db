//! # Thunder Protocol
//!
//! Wire protocol implementations for ThunderDB providing:
//! - PostgreSQL wire protocol (v3)
//! - MySQL wire protocol
//! - RESP protocol (Redis/Valkey compatible)
//!
//! # Usage
//!
//! Each protocol provides a handler and server abstraction:
//!
//! ```ignore
//! use thunder_protocol::postgres::{PostgresHandler, PostgresServer, QueryExecutor};
//! use thunder_protocol::mysql::{MySqlHandler, MySqlServer};
//! use thunder_protocol::resp::{RespHandler, RespServer, RespExecutor};
//! ```

pub mod postgres;
pub mod mysql;
pub mod resp;
pub mod session;
pub mod auth;
pub mod tls;

// Re-export PostgreSQL protocol types
pub use postgres::{
    PostgresHandler,
    PostgresServer,
    QueryExecutor,
    QueryResult as PgQueryResult,
    ColumnInfo,
    datatype_to_oid,
};

// Re-export MySQL protocol types
pub use mysql::{
    MySqlHandler,
    MySqlServer,
};

// Re-export RESP protocol types
pub use resp::{
    RespHandler,
    RespServer,
    RespExecutor,
    RespValue,
    RespParser,
    RespError,
    SetOptions,
    InMemoryStore,
    encode_resp,
};

// Re-export authentication types
pub use auth::{
    compute_md5_hash,
    mysql_native_password,
    mysql_caching_sha2,
    sha256_hash,
    hmac_sha256,
    pbkdf2_sha256,
    xor_bytes,
    generate_salt,
    ScramClient,
    ScramServer,
};

// Re-export TLS types
pub use tls::{
    TlsConfig,
    TlsError,
    MaybeTlsStream,
    ReloadableTlsAcceptor,
};

// Re-export session management types
pub use session::{
    SessionConfig,
    SessionStatus,
    SessionContext,
    SessionManager,
    SessionStats,
    SessionInfo,
    SessionHandle,
    SessionParameters,
    TransactionState,
    Savepoint,
    Cursor,
    PreparedStatementEntry,
};

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use thunder_common::prelude::*;
use tracing::warn;

/// Protocol handler trait
#[async_trait]
pub trait ProtocolHandler: Send + Sync {
    /// Handle an incoming connection
    async fn handle_connection(&self, stream: tokio::net::TcpStream) -> Result<()>;
}

/// Session state for a client connection
#[derive(Debug)]
pub struct Session {
    pub id: uuid::Uuid,
    pub user: Option<String>,
    pub database: Option<String>,
    pub current_txn: Option<TxnId>,
    pub isolation_level: IsolationLevel,
    pub parameters: std::collections::HashMap<String, String>,
    pub prepared_statements: std::collections::HashMap<String, PreparedStatement>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            user: None,
            database: None,
            current_txn: None,
            isolation_level: IsolationLevel::default(),
            parameters: std::collections::HashMap::new(),
            prepared_statements: std::collections::HashMap::new(),
        }
    }

    pub fn set_user(&mut self, user: String) {
        self.user = Some(user);
    }

    pub fn set_database(&mut self, database: String) {
        self.database = Some(database);
    }

    pub fn in_transaction(&self) -> bool {
        self.current_txn.is_some()
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// Prepared statement
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub name: String,
    pub query: String,
    pub param_types: Vec<DataType>,
}

/// Authentication method
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    /// No authentication
    Trust,
    /// Clear text password
    Password,
    /// MD5 password
    Md5,
    /// SCRAM-SHA-256
    ScramSha256,
}

/// Authentication result
#[derive(Debug)]
pub enum AuthResult {
    Success { user: String },
    Failure { message: String },
    Continue { data: Vec<u8> },
}

/// Protocol server configuration
#[derive(Debug, Clone)]
pub struct ProtocolConfig {
    pub listen_addr: String,
    pub port: u16,
    pub max_connections: usize,
    pub auth_method: AuthMethod,
    /// TLS configuration. If Some, TLS is enabled.
    pub tls: Option<TlsConfig>,
    /// Per-database authentication rules (evaluated in order, first match wins).
    /// When non-empty, these override `auth_method` for matching connections.
    pub auth_rules: Vec<thunder_common::config::DatabaseAuthRule>,
}

impl ProtocolConfig {
    /// Check if TLS is enabled.
    pub fn tls_enabled(&self) -> bool {
        self.tls.is_some()
    }
}

/// Set TCP keepalive on a stream to detect dead connections.
pub fn set_tcp_keepalive(stream: &tokio::net::TcpStream) -> std::io::Result<()> {
    let sock_ref = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(60))
        .with_interval(std::time::Duration::from_secs(10));
    sock_ref.set_tcp_keepalive(&keepalive)
}

/// Connection limiter to enforce max_connections.
/// Uses a semaphore to limit concurrent connections.
#[derive(Debug)]
pub struct ConnectionLimiter {
    semaphore: Arc<Semaphore>,
    max_connections: usize,
}

impl ConnectionLimiter {
    /// Create a new connection limiter with the given max connections.
    pub fn new(max_connections: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_connections)),
            max_connections,
        }
    }

    /// Try to acquire a connection permit.
    /// Returns None if the connection limit has been reached.
    pub fn try_acquire(&self) -> Option<ConnectionPermit> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => Some(ConnectionPermit { _permit: permit }),
            Err(_) => {
                warn!(
                    "Connection limit reached ({}/{})",
                    self.max_connections, self.max_connections
                );
                None
            }
        }
    }

    /// Get current number of available connection slots.
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get total max connections.
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Get current number of active connections.
    pub fn active_connections(&self) -> usize {
        self.max_connections - self.semaphore.available_permits()
    }
}

impl Clone for ConnectionLimiter {
    fn clone(&self) -> Self {
        Self {
            semaphore: self.semaphore.clone(),
            max_connections: self.max_connections,
        }
    }
}

/// A permit representing an active connection.
/// When dropped, the connection slot is released.
#[derive(Debug)]
pub struct ConnectionPermit {
    _permit: OwnedSemaphorePermit,
}

impl ConnectionPermit {
    /// Consume the permit without releasing it (for testing).
    #[cfg(test)]
    pub fn forget(self) {
        std::mem::forget(self._permit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_limiter_basic() {
        let limiter = ConnectionLimiter::new(3);

        assert_eq!(limiter.max_connections(), 3);
        assert_eq!(limiter.available(), 3);
        assert_eq!(limiter.active_connections(), 0);

        // Acquire first permit
        let permit1 = limiter.try_acquire();
        assert!(permit1.is_some());
        assert_eq!(limiter.active_connections(), 1);
        assert_eq!(limiter.available(), 2);

        // Acquire second permit
        let permit2 = limiter.try_acquire();
        assert!(permit2.is_some());
        assert_eq!(limiter.active_connections(), 2);

        // Acquire third permit
        let permit3 = limiter.try_acquire();
        assert!(permit3.is_some());
        assert_eq!(limiter.active_connections(), 3);
        assert_eq!(limiter.available(), 0);

        // Fourth should fail
        let permit4 = limiter.try_acquire();
        assert!(permit4.is_none());

        // Drop a permit and try again
        drop(permit1);
        assert_eq!(limiter.active_connections(), 2);

        let permit5 = limiter.try_acquire();
        assert!(permit5.is_some());
    }

    #[test]
    fn test_connection_limiter_clone() {
        let limiter1 = ConnectionLimiter::new(2);
        let limiter2 = limiter1.clone();

        // They share the same semaphore
        let _permit1 = limiter1.try_acquire().unwrap();
        assert_eq!(limiter2.active_connections(), 1);

        let _permit2 = limiter2.try_acquire().unwrap();
        assert_eq!(limiter1.active_connections(), 2);

        // Both should report limit reached
        assert!(limiter1.try_acquire().is_none());
        assert!(limiter2.try_acquire().is_none());
    }

    #[test]
    fn test_session() {
        let mut session = Session::new();
        assert!(!session.in_transaction());

        session.set_user("alice".to_string());
        session.set_database("mydb".to_string());

        assert_eq!(session.user, Some("alice".to_string()));
        assert_eq!(session.database, Some("mydb".to_string()));
    }
}
