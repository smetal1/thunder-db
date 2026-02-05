//! PostgreSQL Wire Protocol (v3) Implementation
//!
//! Implements the PostgreSQL frontend/backend protocol for client connections.
//! See: https://www.postgresql.org/docs/current/protocol.html

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use thunder_common::prelude::*;

use crate::{AuthMethod, AuthResult, ConnectionLimiter, MaybeTlsStream, PreparedStatement, ProtocolConfig, ProtocolHandler, ReloadableTlsAcceptor, Session, TlsConfig, TlsError};
use crate::auth::{compute_md5_hash, verify_password};

/// Global registry for cancel tokens, keyed by (process_id, secret_key)
type CancelRegistry = Arc<DashMap<(i32, i32), CancellationToken>>;

// ============================================================================
// PostgreSQL Protocol Constants
// ============================================================================

/// Protocol version 3.0
const PROTOCOL_VERSION_3: i32 = 196608; // (3 << 16) | 0

/// SSL request code
const SSL_REQUEST_CODE: i32 = 80877103;

/// Cancel request code
const CANCEL_REQUEST_CODE: i32 = 80877102;

// Backend message types (server -> client)
const MSG_AUTHENTICATION: u8 = b'R';
const MSG_BACKEND_KEY_DATA: u8 = b'K';
const MSG_BIND_COMPLETE: u8 = b'2';
const MSG_CLOSE_COMPLETE: u8 = b'3';
const MSG_COMMAND_COMPLETE: u8 = b'C';
const MSG_DATA_ROW: u8 = b'D';
const MSG_EMPTY_QUERY_RESPONSE: u8 = b'I';
const MSG_ERROR_RESPONSE: u8 = b'E';
const MSG_NO_DATA: u8 = b'n';
const MSG_NOTICE_RESPONSE: u8 = b'N';
const MSG_PARAMETER_DESCRIPTION: u8 = b't';
const MSG_PARAMETER_STATUS: u8 = b'S';
const MSG_PARSE_COMPLETE: u8 = b'1';
const MSG_PORTAL_SUSPENDED: u8 = b's';
const MSG_READY_FOR_QUERY: u8 = b'Z';
const MSG_ROW_DESCRIPTION: u8 = b'T';

// Frontend message types (client -> server)
const MSG_BIND: u8 = b'B';
const MSG_CLOSE: u8 = b'C';
const MSG_DESCRIBE: u8 = b'D';
const MSG_EXECUTE: u8 = b'E';
const MSG_FLUSH: u8 = b'H';
const MSG_PARSE: u8 = b'P';
const MSG_PASSWORD: u8 = b'p';
const MSG_QUERY: u8 = b'Q';
const MSG_SYNC: u8 = b'S';
const MSG_TERMINATE: u8 = b'X';

// Authentication types
const AUTH_OK: i32 = 0;
const AUTH_CLEARTEXT_PASSWORD: i32 = 3;
const AUTH_MD5_PASSWORD: i32 = 5;
const AUTH_SASL: i32 = 10;
const AUTH_SASL_CONTINUE: i32 = 11;
const AUTH_SASL_FINAL: i32 = 12;

// Transaction states
const TXN_IDLE: u8 = b'I';
const TXN_IN_TRANSACTION: u8 = b'T';
const TXN_FAILED: u8 = b'E';

// ============================================================================
// Query Executor Trait
// ============================================================================

/// Query executor interface for the protocol handler
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Execute a SQL query and return results
    async fn execute(&self, session_id: uuid::Uuid, sql: &str) -> Result<QueryResult>;

    /// Create a new session
    fn create_session(&self) -> uuid::Uuid;

    /// Close a session
    fn close_session(&self, session_id: uuid::Uuid);

    /// Begin a transaction
    async fn begin_transaction(&self, session_id: uuid::Uuid) -> Result<TxnId>;

    /// Commit a transaction
    async fn commit_transaction(&self, session_id: uuid::Uuid) -> Result<()>;

    /// Rollback a transaction
    async fn rollback_transaction(&self, session_id: uuid::Uuid) -> Result<()>;
}

/// Query result from the executor
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<Option<String>>>,
    pub rows_affected: Option<u64>,
    pub command_tag: String,
}

/// Column information for result sets
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub table_oid: i32,
    pub column_id: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16, // 0 = text, 1 = binary
}

impl Default for ColumnInfo {
    fn default() -> Self {
        Self {
            name: String::new(),
            table_oid: 0,
            column_id: 0,
            type_oid: 25, // TEXT
            type_size: -1,
            type_modifier: -1,
            format: 0, // text
        }
    }
}

// ============================================================================
// PostgreSQL Protocol Handler
// ============================================================================

/// PostgreSQL protocol handler
pub struct PostgresHandler<E: QueryExecutor> {
    executor: Arc<E>,
    config: ProtocolConfig,
    /// Registry of active queries that can be cancelled
    cancel_registry: CancelRegistry,
    /// Reloadable TLS acceptor for secure connections (supports certificate hot-reload)
    tls_acceptor: Option<Arc<ReloadableTlsAcceptor>>,
}

impl<E: QueryExecutor> PostgresHandler<E> {
    pub fn new(executor: Arc<E>, config: ProtocolConfig) -> Self {
        // Build reloadable TLS acceptor if TLS is configured
        let tls_acceptor = config.tls.as_ref().and_then(|tls_config| {
            match ReloadableTlsAcceptor::new(tls_config.clone()) {
                Ok(acceptor) => {
                    info!("TLS enabled for PostgreSQL connections (hot-reload capable)");
                    Some(Arc::new(acceptor))
                }
                Err(e) => {
                    error!("Failed to initialize TLS: {}. TLS will be disabled.", e);
                    None
                }
            }
        });

        Self {
            executor,
            config,
            cancel_registry: Arc::new(DashMap::new()),
            tls_acceptor,
        }
    }

    /// Check if TLS is available
    pub fn tls_available(&self) -> bool {
        self.tls_acceptor.is_some()
    }

    /// Reload TLS certificates from disk.
    ///
    /// Returns `Ok(())` if TLS certificates were reloaded successfully, or if
    /// TLS is not configured (no-op). Returns an error only if TLS is configured
    /// but the new certificates could not be loaded; in that case the previous
    /// certificates remain active.
    pub fn reload_tls(&self) -> std::result::Result<(), TlsError> {
        if let Some(ref acceptor) = self.tls_acceptor {
            acceptor.reload()?;
        }
        Ok(())
    }
}

#[async_trait]
impl<E: QueryExecutor + 'static> ProtocolHandler for PostgresHandler<E> {
    async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let peer_addr = stream.peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        info!("New PostgreSQL connection from {}", peer_addr);

        // Handle potential TLS upgrade
        let ssl_result = match self.handle_ssl_negotiation(stream, &peer_addr).await {
            Ok(result) => result,
            Err(e) => {
                // Cancel requests return an error, but that's expected
                if e.to_string().contains("Cancel request") {
                    debug!("Cancel request processed for {}", peer_addr);
                    return Ok(());
                }
                return Err(e);
            }
        };

        let mut conn = PostgresConnection::new(
            ssl_result,
            self.executor.clone(),
            self.config.clone(),
            self.cancel_registry.clone(),
        );

        match conn.run().await {
            Ok(()) => {
                info!("PostgreSQL connection from {} closed normally", peer_addr);
            }
            Err(e) => {
                warn!("PostgreSQL connection from {} closed with error: {}", peer_addr, e);
            }
        }

        Ok(())
    }
}

impl<E: QueryExecutor + 'static> PostgresHandler<E> {
    /// Handle SSL negotiation at the start of a connection.
    /// Returns the (possibly upgraded) stream and any initial bytes already read.
    async fn handle_ssl_negotiation(
        &self,
        mut stream: TcpStream,
        peer_addr: &str,
    ) -> Result<SslNegotiationResult> {
        // Read the startup message header (length + code)
        let mut header_buf = [0u8; 8];
        stream.read_exact(&mut header_buf).await
            .map_err(|e| Error::Internal(format!("Failed to read startup message: {}", e)))?;

        let length = i32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let code = i32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);

        if code == SSL_REQUEST_CODE {
            // Client is requesting SSL
            if let Some(ref reloadable) = self.tls_acceptor {
                // Accept SSL - send 'S' and perform TLS handshake
                debug!("SSL requested from {}, accepting", peer_addr);
                stream.write_all(&[b'S']).await
                    .map_err(|e| Error::Internal(format!("Failed to send SSL accept: {}", e)))?;

                // Get the latest TLS acceptor (picks up any hot-reloaded certificates)
                let acceptor = reloadable.current();

                // Perform TLS handshake
                return match acceptor.accept(stream).await {
                    Ok(tls_stream) => {
                        info!("TLS handshake completed with {}", peer_addr);
                        // TLS connections get a fresh startup message
                        Ok(SslNegotiationResult {
                            stream: MaybeTlsStream::Tls(tls_stream),
                            initial_bytes: None,
                        })
                    }
                    Err(e) => {
                        Err(Error::Internal(format!("TLS handshake failed: {}", e)))
                    }
                };
            } else {
                // SSL not available - send 'N' and wait for new startup message
                debug!("SSL requested from {} but not configured, rejecting", peer_addr);
                stream.write_all(&[b'N']).await
                    .map_err(|e| Error::Internal(format!("Failed to send SSL reject: {}", e)))?;

                // After rejecting SSL, client sends a new startup message
                return Ok(SslNegotiationResult {
                    stream: MaybeTlsStream::Plain(stream),
                    initial_bytes: None,
                });
            }
        } else if code == CANCEL_REQUEST_CODE {
            // Handle cancel request inline - these are short-lived
            if length == 16 {
                let mut cancel_data = [0u8; 8];
                stream.read_exact(&mut cancel_data).await
                    .map_err(|e| Error::Internal(format!("Failed to read cancel request: {}", e)))?;

                let process_id = i32::from_be_bytes([cancel_data[0], cancel_data[1], cancel_data[2], cancel_data[3]]);
                let secret_key = i32::from_be_bytes([cancel_data[4], cancel_data[5], cancel_data[6], cancel_data[7]]);

                if let Some(token) = self.cancel_registry.get(&(process_id, secret_key)) {
                    debug!("Cancelling query for process_id={}, secret_key={}", process_id, secret_key);
                    token.cancel();
                }
            }
            // Cancel requests just close the connection
            return Err(Error::Internal("Cancel request handled".into()));
        } else {
            // Normal startup message - pass the initial bytes to PostgresConnection
            debug!("Normal startup from {}, protocol version: {}", peer_addr, code);
        }

        // Return plain stream with initial bytes that were already read
        Ok(SslNegotiationResult {
            stream: MaybeTlsStream::Plain(stream),
            initial_bytes: Some(header_buf),
        })
    }
}

// ============================================================================
// PostgreSQL Connection
// ============================================================================

/// Effective authentication method resolved from per-database rules or global config
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectiveAuth {
    /// No authentication required
    Trust,
    /// Cleartext password authentication
    Password,
    /// MD5 password authentication
    Md5,
    /// Connection is rejected
    Reject,
}

/// Result of SSL negotiation
struct SslNegotiationResult {
    stream: MaybeTlsStream<TcpStream>,
    /// Initial bytes already read (length + code) for non-SSL connections
    initial_bytes: Option<[u8; 8]>,
}

/// State machine for a PostgreSQL connection
struct PostgresConnection<E: QueryExecutor> {
    stream: MaybeTlsStream<TcpStream>,
    executor: Arc<E>,
    config: ProtocolConfig,
    session: Session,
    session_id: Option<uuid::Uuid>,
    read_buf: BytesMut,
    write_buf: BytesMut,
    backend_key: (i32, i32), // (process_id, secret_key)
    transaction_state: u8,
    prepared_statements: HashMap<String, PreparedStatementInfo>,
    portals: HashMap<String, PortalInfo>,
    /// Reference to the global cancel registry
    cancel_registry: CancelRegistry,
    /// Current query cancellation token (if a query is running)
    current_cancel_token: Option<CancellationToken>,
    /// Initial bytes already read during SSL negotiation
    initial_startup_bytes: Option<[u8; 8]>,
}

#[derive(Debug, Clone)]
struct PreparedStatementInfo {
    query: String,
    param_types: Vec<i32>,
    columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
struct PortalInfo {
    statement_name: String,
    params: Vec<Option<Vec<u8>>>,
}

impl<E: QueryExecutor> PostgresConnection<E> {
    fn new(
        ssl_result: SslNegotiationResult,
        executor: Arc<E>,
        config: ProtocolConfig,
        cancel_registry: CancelRegistry,
    ) -> Self {
        // Generate random backend key for this connection
        let process_id = rand::random::<i32>().abs();
        let secret_key = rand::random::<i32>().abs();

        Self {
            stream: ssl_result.stream,
            executor,
            config,
            session: Session::new(),
            session_id: None,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(8192),
            backend_key: (process_id, secret_key),
            transaction_state: TXN_IDLE,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            cancel_registry,
            current_cancel_token: None,
            initial_startup_bytes: ssl_result.initial_bytes,
        }
    }

    async fn run(&mut self) -> Result<()> {
        // Handle startup
        self.handle_startup().await?;

        // Create session in executor
        self.session_id = Some(self.executor.create_session());

        // Send authentication response and initial parameters
        self.complete_startup().await?;

        // Main message loop
        const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
        loop {
            let msg_type = match tokio::time::timeout(IDLE_TIMEOUT, self.read_message_type()).await {
                Ok(Ok(t)) => t,
                Ok(Err(_)) => break, // Connection closed
                Err(_) => {
                    debug!("PostgreSQL connection idle for {:?}, closing", IDLE_TIMEOUT);
                    break;
                }
            };

            match msg_type {
                MSG_QUERY => self.handle_simple_query().await?,
                MSG_PARSE => self.handle_parse().await?,
                MSG_BIND => self.handle_bind().await?,
                MSG_DESCRIBE => self.handle_describe().await?,
                MSG_EXECUTE => self.handle_execute().await?,
                MSG_SYNC => self.handle_sync().await?,
                MSG_CLOSE => self.handle_close().await?,
                MSG_FLUSH => self.handle_flush().await?,
                MSG_TERMINATE => {
                    debug!("Received Terminate message");
                    break;
                }
                _ => {
                    warn!("Unknown message type: {}", msg_type as char);
                    self.send_error("XX000", "Unknown message type").await?;
                    self.send_ready_for_query().await?;
                }
            }
        }

        // Cleanup session
        if let Some(session_id) = self.session_id {
            self.executor.close_session(session_id);
        }

        Ok(())
    }

    // ========================================================================
    // Startup Handling
    // ========================================================================

    async fn handle_startup(&mut self) -> Result<()> {
        // Check if we have initial bytes from SSL negotiation
        let (length, code) = if let Some(initial_bytes) = self.initial_startup_bytes.take() {
            let length = i32::from_be_bytes([initial_bytes[0], initial_bytes[1], initial_bytes[2], initial_bytes[3]]);
            let code = i32::from_be_bytes([initial_bytes[4], initial_bytes[5], initial_bytes[6], initial_bytes[7]]);
            (length, code)
        } else {
            // Read startup message length and code
            let length = self.read_i32().await?;
            if length < 8 || length > 10000 {
                return Err(Error::Internal("Invalid startup message length".into()));
            }
            let code = self.read_i32().await?;
            (length, code)
        };

        if length < 8 || length > 10000 {
            return Err(Error::Internal("Invalid startup message length".into()));
        }

        match code {
            SSL_REQUEST_CODE => {
                // SSL should have been handled at the handler level
                // If we get here, just reject it
                self.stream.write_all(&[b'N']).await
                    .map_err(|e| Error::Internal(e.to_string()))?;
                // Recurse to handle actual startup using Box::pin to avoid infinite type size
                return Box::pin(self.handle_startup()).await;
            }
            CANCEL_REQUEST_CODE => {
                // Cancel should have been handled at the handler level
                // If we get here, read and handle it
                let process_id = self.read_i32().await?;
                let secret_key = self.read_i32().await?;

                let key = (process_id, secret_key);
                if let Some(token) = self.cancel_registry.get(&key) {
                    debug!("Cancelling query for process_id={}, secret_key={}", process_id, secret_key);
                    token.cancel();
                }
                return Ok(());
            }
            PROTOCOL_VERSION_3 => {
                // Parse startup parameters
                let remaining = (length - 8) as usize;
                let mut params_buf = vec![0u8; remaining];
                self.stream.read_exact(&mut params_buf).await
                    .map_err(|e| Error::Internal(e.to_string()))?;

                self.parse_startup_params(&params_buf)?;
            }
            _ => {
                return Err(Error::Internal(format!("Unsupported protocol version: {}", code)));
            }
        }

        Ok(())
    }

    fn parse_startup_params(&mut self, buf: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(buf);

        while cursor.has_remaining() {
            let name = read_cstring(&mut cursor)?;
            if name.is_empty() {
                break;
            }
            let value = read_cstring(&mut cursor)?;

            debug!("Startup param: {} = {}", name, value);

            match name.as_str() {
                "user" => self.session.set_user(value.clone()),
                "database" => self.session.set_database(value.clone()),
                _ => {
                    self.session.parameters.insert(name, value);
                }
            }
        }

        Ok(())
    }

    /// Resolve the effective authentication method for this connection.
    ///
    /// If per-database `auth_rules` are configured and a rule matches the
    /// current user/database, it takes precedence over the global `auth_method`.
    fn resolve_auth_method(&self) -> EffectiveAuth {
        if !self.config.auth_rules.is_empty() {
            let user = self.session.user.as_deref().unwrap_or("*");
            let database = self.session.database.as_deref().unwrap_or("*");

            for rule in &self.config.auth_rules {
                let db_match = rule.database == "*" || rule.database == database;
                let user_match = rule.user == "*" || rule.user == user;
                let source_match = rule.source == "*"; // CIDR matching deferred

                if db_match && user_match && source_match {
                    return match rule.method {
                        thunder_common::config::AuthRuleMethod::Trust => EffectiveAuth::Trust,
                        thunder_common::config::AuthRuleMethod::Password => EffectiveAuth::Password,
                        thunder_common::config::AuthRuleMethod::Reject => EffectiveAuth::Reject,
                    };
                }
            }
        }

        // Fall back to global auth method
        match self.config.auth_method {
            AuthMethod::Trust => EffectiveAuth::Trust,
            AuthMethod::Password => EffectiveAuth::Password,
            AuthMethod::Md5 | AuthMethod::ScramSha256 => EffectiveAuth::Md5,
        }
    }

    async fn complete_startup(&mut self) -> Result<()> {
        // Determine effective auth method: per-database rules override global config
        let effective_method = self.resolve_auth_method();

        // Handle authentication based on the resolved method
        match effective_method {
            EffectiveAuth::Trust => {
                debug!("Auth rule: Trust — skipping password check");
                self.send_auth_ok().await?;
            }
            EffectiveAuth::Reject => {
                let user = self.session.user.as_deref().unwrap_or("unknown");
                let db = self.session.database.as_deref().unwrap_or("unknown");
                warn!("Auth rule: Reject — connection denied for user='{}' database='{}'", user, db);
                self.send_error("28000", "Connection rejected by authentication rule").await?;
                return Err(Error::Internal("Connection rejected by auth rule".into()));
            }
            EffectiveAuth::Password => {
                self.send_auth_cleartext().await?;
                self.handle_password_response().await?;
            }
            EffectiveAuth::Md5 => {
                let salt: [u8; 4] = rand::random();
                self.send_auth_md5(&salt).await?;
                self.handle_md5_response(&salt).await?;
            }
        }

        // Send backend key data
        self.send_backend_key_data().await?;

        // Send parameter status messages
        self.send_parameter_status("server_version", "15.0 (ThunderDB)").await?;
        self.send_parameter_status("server_encoding", "UTF8").await?;
        self.send_parameter_status("client_encoding", "UTF8").await?;
        self.send_parameter_status("DateStyle", "ISO, MDY").await?;
        self.send_parameter_status("TimeZone", "UTC").await?;
        self.send_parameter_status("integer_datetimes", "on").await?;
        self.send_parameter_status("standard_conforming_strings", "on").await?;

        // Ready for queries
        self.send_ready_for_query().await?;

        Ok(())
    }

    async fn handle_password_response(&mut self) -> Result<()> {
        let msg_type = self.read_message_type().await?;
        if msg_type != MSG_PASSWORD {
            return Err(Error::Internal("Expected password message".into()));
        }

        let _length = self.read_i32().await?;
        let password = self.read_cstring().await?;

        // For now, accept any password in trust mode or verify against hardcoded
        // In production, this would check against a user database
        if verify_password(&password, "thunder") {
            self.send_auth_ok().await
        } else {
            self.send_error("28P01", "Password authentication failed").await?;
            Err(Error::Internal("Authentication failed".into()))
        }
    }

    async fn handle_md5_response(&mut self, salt: &[u8; 4]) -> Result<()> {
        let msg_type = self.read_message_type().await?;
        if msg_type != MSG_PASSWORD {
            return Err(Error::Internal("Expected password message".into()));
        }

        let _length = self.read_i32().await?;
        let client_hash = self.read_cstring().await?;

        // Compute expected hash: md5(md5(password + user) + salt)
        let user = self.session.user.as_deref().unwrap_or("postgres");
        let expected = compute_md5_hash("thunder", user, salt);

        if client_hash == expected {
            self.send_auth_ok().await
        } else {
            self.send_error("28P01", "Password authentication failed").await?;
            Err(Error::Internal("Authentication failed".into()))
        }
    }

    // ========================================================================
    // Simple Query Protocol
    // ========================================================================

    async fn handle_simple_query(&mut self) -> Result<()> {
        let _length = self.read_i32().await?;
        let query = self.read_cstring().await?;

        debug!("Simple query: {}", query);

        let session_id = self.session_id.ok_or_else(||
            Error::Internal("No session".into()))?;

        // Check for empty query
        if query.trim().is_empty() {
            self.send_empty_query_response().await?;
            self.send_ready_for_query().await?;
            return Ok(());
        }

        // Create and register cancellation token
        let cancel_token = CancellationToken::new();
        self.cancel_registry.insert(self.backend_key, cancel_token.clone());
        self.current_cancel_token = Some(cancel_token.clone());

        // Execute query with cancellation support
        let result = tokio::select! {
            res = self.executor.execute(session_id, &query) => res,
            _ = cancel_token.cancelled() => {
                Err(Error::Internal("Query cancelled by user request".into()))
            }
        };

        // Unregister the cancel token
        self.cancel_registry.remove(&self.backend_key);
        self.current_cancel_token = None;

        match result {
            Ok(result) => {
                // Update transaction state
                self.update_transaction_state(&query);

                // Send results
                if !result.columns.is_empty() {
                    self.send_row_description(&result.columns).await?;
                    for row in &result.rows {
                        self.send_data_row(row).await?;
                    }
                }

                self.send_command_complete(&result.command_tag).await?;
            }
            Err(e) => {
                self.transaction_state = TXN_FAILED;
                self.send_error(e.sqlstate(), &e.to_string()).await?;
            }
        }

        self.send_ready_for_query().await?;
        Ok(())
    }

    fn update_transaction_state(&mut self, query: &str) {
        let upper = query.to_uppercase();
        if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
            self.transaction_state = TXN_IN_TRANSACTION;
        } else if upper.starts_with("COMMIT") || upper.starts_with("END") {
            self.transaction_state = TXN_IDLE;
        } else if upper.starts_with("ROLLBACK") || upper.starts_with("ABORT") {
            self.transaction_state = TXN_IDLE;
        }
    }

    // ========================================================================
    // Extended Query Protocol
    // ========================================================================

    async fn handle_parse(&mut self) -> Result<()> {
        let _length = self.read_i32().await?;
        let stmt_name = self.read_cstring().await?;
        let query = self.read_cstring().await?;
        let num_params = self.read_i16().await?;

        let mut param_types = Vec::with_capacity(num_params as usize);
        for _ in 0..num_params {
            param_types.push(self.read_i32().await?);
        }

        debug!("Parse: name='{}', query='{}', params={}", stmt_name, query, num_params);

        // Store prepared statement
        self.prepared_statements.insert(stmt_name.clone(), PreparedStatementInfo {
            query,
            param_types,
            columns: vec![], // Will be filled on Describe
        });

        self.send_parse_complete().await
    }

    async fn handle_bind(&mut self) -> Result<()> {
        let _length = self.read_i32().await?;
        let portal_name = self.read_cstring().await?;
        let stmt_name = self.read_cstring().await?;

        // Read parameter format codes
        let num_format_codes = self.read_i16().await?;
        let mut _param_formats = Vec::with_capacity(num_format_codes as usize);
        for _ in 0..num_format_codes {
            _param_formats.push(self.read_i16().await?);
        }

        // Read parameter values
        const MAX_PARAMETER_SIZE: usize = 256 * 1024 * 1024; // 256 MB
        let num_params = self.read_i16().await?;
        let mut params = Vec::with_capacity(num_params as usize);
        for _ in 0..num_params {
            let len = self.read_i32().await?;
            if len == -1 {
                params.push(None);
            } else if (len as usize) > MAX_PARAMETER_SIZE {
                return Err(Error::Internal(format!(
                    "Parameter value size {} exceeds maximum of {} bytes",
                    len, MAX_PARAMETER_SIZE
                )));
            } else {
                let mut value = vec![0u8; len as usize];
                self.stream.read_exact(&mut value).await
                    .map_err(|e| Error::Internal(e.to_string()))?;
                params.push(Some(value));
            }
        }

        // Read result format codes
        let num_result_formats = self.read_i16().await?;
        for _ in 0..num_result_formats {
            let _ = self.read_i16().await?;
        }

        debug!("Bind: portal='{}', stmt='{}', params={}", portal_name, stmt_name, num_params);

        // Store portal
        self.portals.insert(portal_name, PortalInfo {
            statement_name: stmt_name,
            params,
        });

        self.send_bind_complete().await
    }

    async fn handle_describe(&mut self) -> Result<()> {
        let _length = self.read_i32().await?;
        let describe_type = self.read_u8().await?;
        let name = self.read_cstring().await?;

        debug!("Describe: type={}, name='{}'", describe_type as char, name);

        match describe_type {
            b'S' => {
                // Describe prepared statement
                if let Some(stmt) = self.prepared_statements.get(&name).cloned() {
                    // Send parameter description
                    self.send_parameter_description(&stmt.param_types).await?;

                    // Send row description (if SELECT)
                    if !stmt.columns.is_empty() {
                        self.send_row_description(&stmt.columns).await?;
                    } else {
                        self.send_no_data().await?;
                    }
                } else {
                    self.send_error("26000", &format!("Prepared statement '{}' does not exist", name)).await?;
                }
            }
            b'P' => {
                // Describe portal
                if let Some(portal) = self.portals.get(&name).cloned() {
                    if let Some(stmt) = self.prepared_statements.get(&portal.statement_name).cloned() {
                        if !stmt.columns.is_empty() {
                            self.send_row_description(&stmt.columns).await?;
                        } else {
                            self.send_no_data().await?;
                        }
                    }
                } else {
                    self.send_error("34000", &format!("Portal '{}' does not exist", name)).await?;
                }
            }
            _ => {
                self.send_error("XX000", "Invalid describe type").await?;
            }
        }

        Ok(())
    }

    async fn handle_execute(&mut self) -> Result<()> {
        let _length = self.read_i32().await?;
        let portal_name = self.read_cstring().await?;
        let max_rows = self.read_i32().await?;

        debug!("Execute: portal='{}', max_rows={}", portal_name, max_rows);

        let session_id = self.session_id.ok_or_else(||
            Error::Internal("No session".into()))?;

        // Get portal and statement
        let portal = self.portals.get(&portal_name)
            .ok_or_else(|| Error::Internal(format!("Portal '{}' not found", portal_name)))?
            .clone();

        let stmt = self.prepared_statements.get(&portal.statement_name)
            .ok_or_else(|| Error::Internal(format!("Statement '{}' not found", portal.statement_name)))?
            .clone();

        // Substitute parameters in query
        let query = self.substitute_params(&stmt.query, &portal.params)?;

        // Create and register cancellation token
        let cancel_token = CancellationToken::new();
        self.cancel_registry.insert(self.backend_key, cancel_token.clone());
        self.current_cancel_token = Some(cancel_token.clone());

        // Execute with cancellation support
        let result = tokio::select! {
            res = self.executor.execute(session_id, &query) => res,
            _ = cancel_token.cancelled() => {
                Err(Error::Internal("Query cancelled by user request".into()))
            }
        };

        // Unregister the cancel token
        self.cancel_registry.remove(&self.backend_key);
        self.current_cancel_token = None;

        match result {
            Ok(result) => {
                self.update_transaction_state(&query);

                // Send data rows
                for row in &result.rows {
                    self.send_data_row(row).await?;
                }

                self.send_command_complete(&result.command_tag).await?;
            }
            Err(e) => {
                self.transaction_state = TXN_FAILED;
                self.send_error(e.sqlstate(), &e.to_string()).await?;
            }
        }

        Ok(())
    }

    fn substitute_params(&self, query: &str, params: &[Option<Vec<u8>>]) -> Result<String> {
        let mut result = query.to_string();
        for (i, param) in params.iter().enumerate() {
            let placeholder = format!("${}", i + 1);
            let value = match param {
                Some(bytes) => {
                    let s = String::from_utf8_lossy(bytes);
                    format!("'{}'", s.replace('\'', "''"))
                }
                None => "NULL".to_string(),
            };
            result = result.replace(&placeholder, &value);
        }
        Ok(result)
    }

    async fn handle_sync(&mut self) -> Result<()> {
        let _length = self.read_i32().await?;
        debug!("Sync");

        // Reset error state if in failed transaction
        if self.transaction_state == TXN_FAILED {
            self.transaction_state = TXN_IDLE;
        }

        self.send_ready_for_query().await
    }

    async fn handle_close(&mut self) -> Result<()> {
        let _length = self.read_i32().await?;
        let close_type = self.read_u8().await?;
        let name = self.read_cstring().await?;

        debug!("Close: type={}, name='{}'", close_type as char, name);

        match close_type {
            b'S' => {
                self.prepared_statements.remove(&name);
            }
            b'P' => {
                self.portals.remove(&name);
            }
            _ => {}
        }

        self.send_close_complete().await
    }

    async fn handle_flush(&mut self) -> Result<()> {
        let _length = self.read_i32().await?;
        debug!("Flush");
        self.flush().await
    }

    // ========================================================================
    // Message Reading
    // ========================================================================

    async fn read_message_type(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.stream.read_exact(&mut buf).await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(buf[0])
    }

    async fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.stream.read_exact(&mut buf).await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(buf[0])
    }

    async fn read_i16(&mut self) -> Result<i16> {
        let mut buf = [0u8; 2];
        self.stream.read_exact(&mut buf).await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(i16::from_be_bytes(buf))
    }

    async fn read_i32(&mut self) -> Result<i32> {
        let mut buf = [0u8; 4];
        self.stream.read_exact(&mut buf).await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(i32::from_be_bytes(buf))
    }

    async fn read_cstring(&mut self) -> Result<String> {
        let mut bytes = Vec::new();
        loop {
            let b = self.read_u8().await?;
            if b == 0 {
                break;
            }
            bytes.push(b);
        }
        String::from_utf8(bytes)
            .map_err(|e| Error::Internal(e.to_string()))
    }

    // ========================================================================
    // Message Writing
    // ========================================================================

    async fn send_auth_ok(&mut self) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_AUTHENTICATION);
        self.write_buf.put_i32(8); // length
        self.write_buf.put_i32(AUTH_OK);
        self.flush().await
    }

    async fn send_auth_cleartext(&mut self) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_AUTHENTICATION);
        self.write_buf.put_i32(8);
        self.write_buf.put_i32(AUTH_CLEARTEXT_PASSWORD);
        self.flush().await
    }

    async fn send_auth_md5(&mut self, salt: &[u8; 4]) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_AUTHENTICATION);
        self.write_buf.put_i32(12);
        self.write_buf.put_i32(AUTH_MD5_PASSWORD);
        self.write_buf.put_slice(salt);
        self.flush().await
    }

    async fn send_backend_key_data(&mut self) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_BACKEND_KEY_DATA);
        self.write_buf.put_i32(12);
        self.write_buf.put_i32(self.backend_key.0);
        self.write_buf.put_i32(self.backend_key.1);
        self.flush().await
    }

    async fn send_parameter_status(&mut self, name: &str, value: &str) -> Result<()> {
        let len = 4 + name.len() + 1 + value.len() + 1;
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_PARAMETER_STATUS);
        self.write_buf.put_i32(len as i32);
        self.write_buf.put_slice(name.as_bytes());
        self.write_buf.put_u8(0);
        self.write_buf.put_slice(value.as_bytes());
        self.write_buf.put_u8(0);
        self.flush().await
    }

    async fn send_ready_for_query(&mut self) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_READY_FOR_QUERY);
        self.write_buf.put_i32(5);
        self.write_buf.put_u8(self.transaction_state);
        self.flush().await
    }

    async fn send_row_description(&mut self, columns: &[ColumnInfo]) -> Result<()> {
        // Calculate message length
        let mut len = 4 + 2; // length + field count
        for col in columns {
            len += col.name.len() + 1 + 18; // name + null + fixed fields
        }

        self.write_buf.clear();
        self.write_buf.put_u8(MSG_ROW_DESCRIPTION);
        self.write_buf.put_i32(len as i32);
        self.write_buf.put_i16(columns.len() as i16);

        for col in columns {
            self.write_buf.put_slice(col.name.as_bytes());
            self.write_buf.put_u8(0);
            self.write_buf.put_i32(col.table_oid);
            self.write_buf.put_i16(col.column_id);
            self.write_buf.put_i32(col.type_oid);
            self.write_buf.put_i16(col.type_size);
            self.write_buf.put_i32(col.type_modifier);
            self.write_buf.put_i16(col.format);
        }

        self.flush().await
    }

    async fn send_data_row(&mut self, values: &[Option<String>]) -> Result<()> {
        // Calculate length
        let mut len = 4 + 2; // length + column count
        for val in values {
            len += 4; // length field
            if let Some(s) = val {
                len += s.len();
            }
        }

        self.write_buf.clear();
        self.write_buf.put_u8(MSG_DATA_ROW);
        self.write_buf.put_i32(len as i32);
        self.write_buf.put_i16(values.len() as i16);

        for val in values {
            match val {
                Some(s) => {
                    self.write_buf.put_i32(s.len() as i32);
                    self.write_buf.put_slice(s.as_bytes());
                }
                None => {
                    self.write_buf.put_i32(-1); // NULL
                }
            }
        }

        self.flush().await
    }

    async fn send_command_complete(&mut self, tag: &str) -> Result<()> {
        let len = 4 + tag.len() + 1;
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_COMMAND_COMPLETE);
        self.write_buf.put_i32(len as i32);
        self.write_buf.put_slice(tag.as_bytes());
        self.write_buf.put_u8(0);
        self.flush().await
    }

    async fn send_empty_query_response(&mut self) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_EMPTY_QUERY_RESPONSE);
        self.write_buf.put_i32(4);
        self.flush().await
    }

    async fn send_error(&mut self, code: &str, message: &str) -> Result<()> {
        // Calculate length: 4 + severity + code + message + terminator
        let len = 4 + 1 + 6 + 1 + 1 + code.len() + 1 + 1 + message.len() + 1 + 1;

        self.write_buf.clear();
        self.write_buf.put_u8(MSG_ERROR_RESPONSE);
        self.write_buf.put_i32(len as i32);

        // Severity
        self.write_buf.put_u8(b'S');
        self.write_buf.put_slice(b"ERROR\0");

        // Code
        self.write_buf.put_u8(b'C');
        self.write_buf.put_slice(code.as_bytes());
        self.write_buf.put_u8(0);

        // Message
        self.write_buf.put_u8(b'M');
        self.write_buf.put_slice(message.as_bytes());
        self.write_buf.put_u8(0);

        // Terminator
        self.write_buf.put_u8(0);

        self.flush().await
    }

    async fn send_parse_complete(&mut self) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_PARSE_COMPLETE);
        self.write_buf.put_i32(4);
        self.flush().await
    }

    async fn send_bind_complete(&mut self) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_BIND_COMPLETE);
        self.write_buf.put_i32(4);
        self.flush().await
    }

    async fn send_close_complete(&mut self) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_CLOSE_COMPLETE);
        self.write_buf.put_i32(4);
        self.flush().await
    }

    async fn send_no_data(&mut self) -> Result<()> {
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_NO_DATA);
        self.write_buf.put_i32(4);
        self.flush().await
    }

    async fn send_parameter_description(&mut self, types: &[i32]) -> Result<()> {
        let len = 4 + 2 + types.len() * 4;
        self.write_buf.clear();
        self.write_buf.put_u8(MSG_PARAMETER_DESCRIPTION);
        self.write_buf.put_i32(len as i32);
        self.write_buf.put_i16(types.len() as i16);
        for &t in types {
            self.write_buf.put_i32(t);
        }
        self.flush().await
    }

    async fn flush(&mut self) -> Result<()> {
        self.stream.write_all(&self.write_buf).await
            .map_err(|e| Error::Internal(e.to_string()))?;
        self.stream.flush().await
            .map_err(|e| Error::Internal(e.to_string()))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn read_cstring(cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let mut bytes = Vec::new();
    while cursor.has_remaining() {
        let b = cursor.get_u8();
        if b == 0 {
            break;
        }
        bytes.push(b);
    }
    String::from_utf8(bytes)
        .map_err(|e| Error::Internal(e.to_string()))
}

/// Map ThunderDB DataType to PostgreSQL OID
pub fn datatype_to_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Boolean => 16,      // bool
        DataType::Int8 => 18,         // char
        DataType::Int16 => 21,        // int2
        DataType::Int32 => 23,        // int4
        DataType::Int64 => 20,        // int8
        DataType::Float32 => 700,     // float4
        DataType::Float64 => 701,     // float8
        DataType::String => 25,       // text
        DataType::Varchar(_) => 1043, // varchar
        DataType::Char(_) => 1042,    // bpchar
        DataType::Binary => 17,       // bytea
        DataType::Date => 1082,       // date
        DataType::Time => 1083,       // time
        DataType::Timestamp => 1114,  // timestamp
        DataType::TimestampTz => 1184,// timestamptz
        DataType::Uuid => 2950,       // uuid
        DataType::Json => 114,        // json
        DataType::Jsonb => 3802,      // jsonb
        DataType::Null => 705,        // unknown
        _ => 25, // default to text
    }
}

// ============================================================================
// PostgreSQL Server
// ============================================================================

/// PostgreSQL protocol server
pub struct PostgresServer<E: QueryExecutor + 'static> {
    handler: Arc<PostgresHandler<E>>,
    config: ProtocolConfig,
}

impl<E: QueryExecutor + 'static> PostgresServer<E> {
    pub fn new(executor: Arc<E>, config: ProtocolConfig) -> Self {
        let handler = Arc::new(PostgresHandler::new(executor, config.clone()));
        Self { handler, config }
    }

    /// Run the PostgreSQL server
    pub async fn run(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.listen_addr, self.config.port);
        let listener = tokio::net::TcpListener::bind(&addr).await
            .map_err(|e| Error::Internal(format!("Failed to bind to {}: {}", addr, e)))?;

        let limiter = ConnectionLimiter::new(self.config.max_connections);
        let tls_status = if self.handler.tls_available() { "enabled" } else { "disabled" };
        info!(
            "PostgreSQL server listening on {} (max_connections: {}, TLS: {})",
            addr, self.config.max_connections, tls_status
        );

        loop {
            let (stream, peer_addr) = listener.accept().await
                .map_err(|e| Error::Internal(e.to_string()))?;

            // Try to acquire a connection permit
            let permit = match limiter.try_acquire() {
                Some(permit) => permit,
                None => {
                    // Connection limit reached - close the connection
                    warn!(
                        "Rejecting connection from {}: max connections ({}) reached",
                        peer_addr, self.config.max_connections
                    );
                    drop(stream);
                    continue;
                }
            };

            debug!(
                "PostgreSQL connection from {} (active: {}/{})",
                peer_addr,
                limiter.active_connections(),
                limiter.max_connections()
            );

            let handler = self.handler.clone();
            tokio::spawn(async move {
                // Permit is held for the duration of the connection
                let _permit = permit;
                if let Err(e) = handler.handle_connection(stream).await {
                    error!("Connection error: {}", e);
                }
                // Permit is dropped here, releasing the connection slot
            });
        }
    }
}

// ============================================================================
// Standalone serve function for main.rs integration
// ============================================================================

/// Serve PostgreSQL protocol connections on the given listener
///
/// This is a convenience function for use in main.rs that accepts a pre-bound
/// TcpListener and an executor implementing QueryExecutor.
pub async fn serve<E: QueryExecutor + 'static>(
    listener: tokio::net::TcpListener,
    executor: Arc<E>,
) {
    serve_with_cancel(listener, executor, ProtocolConfig {
        listen_addr: "0.0.0.0".to_string(),
        port: 5432,
        max_connections: 1000,
        auth_method: crate::AuthMethod::Trust,
        tls: None,
        auth_rules: Vec::new(),
    }, tokio_util::sync::CancellationToken::new()).await
}

/// Serve PostgreSQL protocol connections with TLS support.
pub async fn serve_with_tls<E: QueryExecutor + 'static>(
    listener: tokio::net::TcpListener,
    executor: Arc<E>,
    tls_config: TlsConfig,
) {
    serve_with_cancel(listener, executor, ProtocolConfig {
        listen_addr: "0.0.0.0".to_string(),
        port: 5432,
        max_connections: 1000,
        auth_method: crate::AuthMethod::Trust,
        tls: Some(tls_config),
        auth_rules: Vec::new(),
    }, tokio_util::sync::CancellationToken::new()).await
}

/// Serve PostgreSQL protocol connections with custom configuration.
pub async fn serve_with_config<E: QueryExecutor + 'static>(
    listener: tokio::net::TcpListener,
    executor: Arc<E>,
    config: ProtocolConfig,
) {
    serve_with_cancel(listener, executor, config, tokio_util::sync::CancellationToken::new()).await
}

/// Serve PostgreSQL protocol connections with cancellation support for graceful shutdown.
pub async fn serve_with_cancel<E: QueryExecutor + 'static>(
    listener: tokio::net::TcpListener,
    executor: Arc<E>,
    config: ProtocolConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    let handler = Arc::new(PostgresHandler::new(executor, config.clone()));
    serve_handler_with_cancel(listener, handler, config, cancel_token).await
}

/// Serve PostgreSQL protocol connections using a pre-built handler.
///
/// This variant accepts an existing `Arc<PostgresHandler>` so callers can retain
/// a reference for runtime operations such as TLS certificate hot-reload via
/// [`PostgresHandler::reload_tls`].
pub async fn serve_handler_with_cancel<E: QueryExecutor + 'static>(
    listener: tokio::net::TcpListener,
    handler: Arc<PostgresHandler<E>>,
    config: ProtocolConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    let limiter = ConnectionLimiter::new(config.max_connections);

    let tls_status = if handler.tls_available() { "enabled" } else { "disabled" };
    info!(
        "PostgreSQL protocol server started (max_connections: {}, TLS: {})",
        config.max_connections, tls_status
    );

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        // Set TCP keepalive
                        if let Err(e) = crate::set_tcp_keepalive(&stream) {
                            warn!("Failed to set TCP keepalive for {}: {}", peer_addr, e);
                        }

                        // Try to acquire a connection permit
                        let permit = match limiter.try_acquire() {
                            Some(permit) => permit,
                            None => {
                                warn!(
                                    "Rejecting PostgreSQL connection from {}: max connections ({}) reached",
                                    peer_addr, config.max_connections
                                );
                                drop(stream);
                                continue;
                            }
                        };

                        debug!(
                            "PostgreSQL connection from {} (active: {}/{})",
                            peer_addr,
                            limiter.active_connections(),
                            limiter.max_connections()
                        );

                        let handler = handler.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(e) = handler.handle_connection(stream).await {
                                error!("PostgreSQL connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept PostgreSQL connection: {}", e);
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                info!("PostgreSQL protocol server shutting down");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_to_oid() {
        assert_eq!(datatype_to_oid(&DataType::Int32), 23);
        assert_eq!(datatype_to_oid(&DataType::String), 25);
        assert_eq!(datatype_to_oid(&DataType::Boolean), 16);
    }

    #[test]
    fn test_column_info_default() {
        let col = ColumnInfo::default();
        assert_eq!(col.type_oid, 25); // TEXT
        assert_eq!(col.format, 0);    // text format
    }
}
