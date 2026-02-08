//! Session Management
//!
//! Provides comprehensive session lifecycle management for ThunderDB including:
//! - Session creation, tracking, and cleanup
//! - Connection pooling and session reuse
//! - Session variables and runtime parameters
//! - Prepared statements and cursors (portals)
//! - Transaction state tracking
//! - Session timeouts and limits

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use thunder_common::prelude::*;

// ============================================================================
// Session Configuration
// ============================================================================

/// Session manager configuration
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Maximum number of concurrent sessions
    pub max_sessions: usize,
    /// Session idle timeout
    pub idle_timeout: Duration,
    /// Session absolute timeout (max lifetime)
    pub absolute_timeout: Duration,
    /// Statement timeout for queries
    pub statement_timeout: Duration,
    /// Maximum prepared statements per session
    pub max_prepared_statements: usize,
    /// Maximum cursors per session
    pub max_cursors: usize,
    /// Enable session pooling
    pub enable_pooling: bool,
    /// Pool size for reusable sessions
    pub pool_size: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_sessions: 1000,
            idle_timeout: Duration::from_secs(300),      // 5 minutes
            absolute_timeout: Duration::from_secs(3600), // 1 hour
            statement_timeout: Duration::from_secs(30),  // 30 seconds
            max_prepared_statements: 100,
            max_cursors: 50,
            enable_pooling: true,
            pool_size: 100,
        }
    }
}

// ============================================================================
// Session State
// ============================================================================

/// Session status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    /// Session is being initialized
    Initializing,
    /// Session is active and ready
    Active,
    /// Session is executing a query
    Busy,
    /// Session is idle in transaction
    IdleInTransaction,
    /// Session is idle in aborted transaction
    IdleInAbortedTransaction,
    /// Session is being closed
    Closing,
    /// Session is closed
    Closed,
}

/// Transaction state within a session
#[derive(Debug, Clone)]
pub struct TransactionState {
    /// Transaction ID
    pub txn_id: TxnId,
    /// Start time
    pub start_time: Instant,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Read-only transaction
    pub read_only: bool,
    /// Deferred constraints
    pub deferred: bool,
    /// Savepoints
    pub savepoints: Vec<Savepoint>,
    /// Whether the transaction is aborted
    pub aborted: bool,
}

impl TransactionState {
    pub fn new(txn_id: TxnId, isolation_level: IsolationLevel) -> Self {
        Self {
            txn_id,
            start_time: Instant::now(),
            isolation_level,
            read_only: false,
            deferred: false,
            savepoints: Vec::new(),
            aborted: false,
        }
    }
}

/// Savepoint within a transaction
#[derive(Debug, Clone)]
pub struct Savepoint {
    pub name: String,
    pub created_at: Instant,
}

/// Cursor (portal) for query results
#[derive(Debug)]
pub struct Cursor {
    /// Cursor name
    pub name: String,
    /// Associated prepared statement name
    pub statement_name: String,
    /// Bound parameter values
    pub parameters: Vec<Value>,
    /// Current position in result set
    pub position: usize,
    /// Whether cursor is scrollable
    pub scrollable: bool,
    /// Hold cursor open after transaction commit
    pub with_hold: bool,
    /// Creation time
    pub created_at: Instant,
}

impl Cursor {
    pub fn new(name: String, statement_name: String) -> Self {
        Self {
            name,
            statement_name,
            parameters: Vec::new(),
            position: 0,
            scrollable: false,
            with_hold: false,
            created_at: Instant::now(),
        }
    }
}

/// Extended prepared statement with metadata
#[derive(Debug, Clone)]
pub struct PreparedStatementEntry {
    /// Statement name
    pub name: String,
    /// SQL query text
    pub query: String,
    /// Parameter types
    pub param_types: Vec<DataType>,
    /// Result column types (if known)
    pub result_types: Option<Vec<DataType>>,
    /// Number of times executed
    pub execution_count: u64,
    /// Last execution time
    pub last_executed: Option<Instant>,
    /// Creation time
    pub created_at: Instant,
}

impl PreparedStatementEntry {
    pub fn new(name: String, query: String, param_types: Vec<DataType>) -> Self {
        Self {
            name,
            query,
            param_types,
            result_types: None,
            execution_count: 0,
            last_executed: None,
            created_at: Instant::now(),
        }
    }
}

/// Session runtime parameters
#[derive(Debug, Clone)]
pub struct SessionParameters {
    /// Search path for schema resolution
    pub search_path: Vec<String>,
    /// Current schema
    pub current_schema: String,
    /// Timezone
    pub timezone: String,
    /// Date style
    pub date_style: String,
    /// Client encoding
    pub client_encoding: String,
    /// Server encoding
    pub server_encoding: String,
    /// Application name
    pub application_name: String,
    /// Statement timeout (milliseconds)
    pub statement_timeout_ms: u64,
    /// Lock timeout (milliseconds)
    pub lock_timeout_ms: u64,
    /// Work memory (KB)
    pub work_mem_kb: u64,
    /// Extra float digits
    pub extra_float_digits: i32,
    /// Custom parameters
    pub custom: HashMap<String, String>,
}

impl Default for SessionParameters {
    fn default() -> Self {
        Self {
            search_path: vec!["public".to_string()],
            current_schema: "public".to_string(),
            timezone: "UTC".to_string(),
            date_style: "ISO, MDY".to_string(),
            client_encoding: "UTF8".to_string(),
            server_encoding: "UTF8".to_string(),
            application_name: String::new(),
            statement_timeout_ms: 0, // 0 = no timeout
            lock_timeout_ms: 0,
            work_mem_kb: 4096, // 4MB
            extra_float_digits: 1,
            custom: HashMap::new(),
        }
    }
}

impl SessionParameters {
    /// Get a parameter value
    pub fn get(&self, name: &str) -> Option<String> {
        match name.to_lowercase().as_str() {
            "search_path" => Some(self.search_path.join(", ")),
            "current_schema" => Some(self.current_schema.clone()),
            "timezone" | "time zone" => Some(self.timezone.clone()),
            "datestyle" => Some(self.date_style.clone()),
            "client_encoding" => Some(self.client_encoding.clone()),
            "server_encoding" => Some(self.server_encoding.clone()),
            "application_name" => Some(self.application_name.clone()),
            "statement_timeout" => Some(self.statement_timeout_ms.to_string()),
            "lock_timeout" => Some(self.lock_timeout_ms.to_string()),
            "work_mem" => Some(format!("{}kB", self.work_mem_kb)),
            "extra_float_digits" => Some(self.extra_float_digits.to_string()),
            _ => self.custom.get(name).cloned(),
        }
    }

    /// Set a parameter value
    pub fn set(&mut self, name: &str, value: &str) -> Result<()> {
        match name.to_lowercase().as_str() {
            "search_path" => {
                self.search_path = value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
            }
            "current_schema" => {
                self.current_schema = value.to_string();
            }
            "timezone" | "time zone" => {
                self.timezone = value.to_string();
            }
            "datestyle" => {
                self.date_style = value.to_string();
            }
            "client_encoding" => {
                self.client_encoding = value.to_string();
            }
            "application_name" => {
                self.application_name = value.to_string();
            }
            "statement_timeout" => {
                self.statement_timeout_ms = value
                    .parse()
                    .map_err(|_| Error::Internal(format!("Invalid statement_timeout: {}", value)))?;
            }
            "lock_timeout" => {
                self.lock_timeout_ms = value
                    .parse()
                    .map_err(|_| Error::Internal(format!("Invalid lock_timeout: {}", value)))?;
            }
            "work_mem" => {
                // Parse values like "4MB", "4096kB", "4096"
                let normalized = value.to_lowercase();
                let kb = if normalized.ends_with("mb") {
                    normalized.trim_end_matches("mb").trim().parse::<u64>()
                        .map(|v| v * 1024)
                } else if normalized.ends_with("kb") {
                    normalized.trim_end_matches("kb").trim().parse::<u64>()
                } else {
                    normalized.parse::<u64>()
                };
                self.work_mem_kb = kb.map_err(|_|
                    Error::Internal(format!("Invalid work_mem: {}", value)))?;
            }
            "extra_float_digits" => {
                self.extra_float_digits = value
                    .parse()
                    .map_err(|_| Error::Internal(format!("Invalid extra_float_digits: {}", value)))?;
            }
            _ => {
                self.custom.insert(name.to_string(), value.to_string());
            }
        }
        Ok(())
    }

    /// Reset a parameter to default
    pub fn reset(&mut self, name: &str) {
        let default = SessionParameters::default();
        match name.to_lowercase().as_str() {
            "search_path" => self.search_path = default.search_path,
            "current_schema" => self.current_schema = default.current_schema,
            "timezone" | "time zone" => self.timezone = default.timezone,
            "datestyle" => self.date_style = default.date_style,
            "client_encoding" => self.client_encoding = default.client_encoding,
            "application_name" => self.application_name = default.application_name,
            "statement_timeout" => self.statement_timeout_ms = default.statement_timeout_ms,
            "lock_timeout" => self.lock_timeout_ms = default.lock_timeout_ms,
            "work_mem" => self.work_mem_kb = default.work_mem_kb,
            "extra_float_digits" => self.extra_float_digits = default.extra_float_digits,
            "all" => *self = default,
            _ => { self.custom.remove(name); }
        }
    }
}

// ============================================================================
// Session Context
// ============================================================================

/// Full session context with all state
pub struct SessionContext {
    /// Unique session ID
    pub id: Uuid,
    /// Session status
    pub status: SessionStatus,
    /// Authenticated user
    pub user: Option<String>,
    /// Current database
    pub database: Option<String>,
    /// Client address
    pub client_addr: Option<String>,
    /// Protocol (postgres, mysql, resp)
    pub protocol: String,
    /// Session parameters
    pub parameters: SessionParameters,
    /// Current transaction state
    pub transaction: Option<TransactionState>,
    /// Prepared statements (LRU cache — least recently used entries evicted when full)
    pub prepared_statements: LruCache<String, PreparedStatementEntry>,
    /// Open cursors (portals)
    pub cursors: HashMap<String, Cursor>,
    /// Session creation time
    pub created_at: Instant,
    /// Last activity time
    pub last_activity: Instant,
    /// Total queries executed
    pub query_count: u64,
    /// Total errors encountered
    pub error_count: u64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Bytes received
    pub bytes_received: u64,
    /// Current query (if executing)
    pub current_query: Option<String>,
    /// Query start time
    pub query_start: Option<Instant>,
    /// Cancellation token for current query
    cancel_tx: Option<mpsc::Sender<()>>,
}

impl SessionContext {
    /// Create a new session context
    pub fn new(protocol: &str) -> Self {
        let now = Instant::now();
        Self {
            id: Uuid::new_v4(),
            status: SessionStatus::Initializing,
            user: None,
            database: None,
            client_addr: None,
            protocol: protocol.to_string(),
            parameters: SessionParameters::default(),
            transaction: None,
            prepared_statements: LruCache::new(NonZeroUsize::new(256).unwrap()),
            cursors: HashMap::new(),
            created_at: now,
            last_activity: now,
            query_count: 0,
            error_count: 0,
            bytes_sent: 0,
            bytes_received: 0,
            current_query: None,
            query_start: None,
            cancel_tx: None,
        }
    }

    /// Mark session as active
    pub fn activate(&mut self) {
        self.status = SessionStatus::Active;
        self.last_activity = Instant::now();
    }

    /// Check if session is in a transaction
    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    /// Check if session is in an aborted transaction
    pub fn in_aborted_transaction(&self) -> bool {
        self.transaction.as_ref().map(|t| t.aborted).unwrap_or(false)
    }

    /// Begin a new transaction
    pub fn begin_transaction(&mut self, txn_id: TxnId, isolation: IsolationLevel) -> Result<()> {
        if self.transaction.is_some() {
            return Err(Error::Internal("Transaction already in progress".into()));
        }
        self.transaction = Some(TransactionState::new(txn_id, isolation));
        self.status = SessionStatus::IdleInTransaction;
        self.last_activity = Instant::now();
        Ok(())
    }

    /// Commit the current transaction
    pub fn commit_transaction(&mut self) -> Result<TxnId> {
        let txn = self.transaction.take()
            .ok_or_else(|| Error::Internal("No transaction in progress".into()))?;

        if txn.aborted {
            return Err(Error::Internal("Cannot commit aborted transaction".into()));
        }

        // Close non-holdable cursors
        self.cursors.retain(|_, c| c.with_hold);

        self.status = SessionStatus::Active;
        self.last_activity = Instant::now();
        Ok(txn.txn_id)
    }

    /// Rollback the current transaction
    pub fn rollback_transaction(&mut self) -> Result<Option<TxnId>> {
        let txn_id = self.transaction.take().map(|t| t.txn_id);

        // Close all cursors
        self.cursors.clear();

        self.status = SessionStatus::Active;
        self.last_activity = Instant::now();
        Ok(txn_id)
    }

    /// Abort the transaction (mark as failed)
    pub fn abort_transaction(&mut self) {
        if let Some(ref mut txn) = self.transaction {
            txn.aborted = true;
            self.status = SessionStatus::IdleInAbortedTransaction;
        }
    }

    /// Create a savepoint
    pub fn create_savepoint(&mut self, name: &str) -> Result<()> {
        let txn = self.transaction.as_mut()
            .ok_or_else(|| Error::Internal("No transaction in progress".into()))?;

        txn.savepoints.push(Savepoint {
            name: name.to_string(),
            created_at: Instant::now(),
        });

        self.last_activity = Instant::now();
        Ok(())
    }

    /// Rollback to a savepoint
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let txn = self.transaction.as_mut()
            .ok_or_else(|| Error::Internal("No transaction in progress".into()))?;

        // Find and remove savepoint and all subsequent ones
        if let Some(pos) = txn.savepoints.iter().position(|s| s.name == name) {
            txn.savepoints.truncate(pos + 1);
            txn.aborted = false; // Clear abort state
            self.status = SessionStatus::IdleInTransaction;
        } else {
            return Err(Error::Internal(format!("Savepoint '{}' not found", name)));
        }

        self.last_activity = Instant::now();
        Ok(())
    }

    /// Release a savepoint
    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let txn = self.transaction.as_mut()
            .ok_or_else(|| Error::Internal("No transaction in progress".into()))?;

        if let Some(pos) = txn.savepoints.iter().position(|s| s.name == name) {
            txn.savepoints.remove(pos);
        } else {
            return Err(Error::Internal(format!("Savepoint '{}' not found", name)));
        }

        self.last_activity = Instant::now();
        Ok(())
    }

    /// Add a prepared statement (LRU: evicts least recently used if at capacity)
    pub fn add_prepared_statement(&mut self, stmt: PreparedStatementEntry, _max_statements: usize) -> Result<()> {
        // LRU cache handles eviction automatically when capacity is exceeded.
        // The _max_statements parameter is kept for API compatibility but the
        // cache capacity is set at construction time (256).
        self.prepared_statements.push(stmt.name.clone(), stmt);
        self.last_activity = Instant::now();
        Ok(())
    }

    /// Get a prepared statement (promotes to most recently used)
    pub fn get_prepared_statement(&mut self, name: &str) -> Option<&PreparedStatementEntry> {
        self.prepared_statements.get(name)
    }

    /// Peek at a prepared statement without promoting it in the LRU
    pub fn peek_prepared_statement(&self, name: &str) -> Option<&PreparedStatementEntry> {
        self.prepared_statements.peek(name)
    }

    /// Remove a prepared statement
    pub fn remove_prepared_statement(&mut self, name: &str) -> Option<PreparedStatementEntry> {
        self.last_activity = Instant::now();
        self.prepared_statements.pop(name)
    }

    /// Add a cursor
    pub fn add_cursor(&mut self, cursor: Cursor, max_cursors: usize) -> Result<()> {
        if self.cursors.len() >= max_cursors {
            return Err(Error::Internal(format!(
                "Maximum cursors ({}) exceeded", max_cursors
            )));
        }

        self.cursors.insert(cursor.name.clone(), cursor);
        self.last_activity = Instant::now();
        Ok(())
    }

    /// Get a cursor
    pub fn get_cursor(&self, name: &str) -> Option<&Cursor> {
        self.cursors.get(name)
    }

    /// Get a mutable cursor
    pub fn get_cursor_mut(&mut self, name: &str) -> Option<&mut Cursor> {
        self.cursors.get_mut(name)
    }

    /// Remove a cursor
    pub fn remove_cursor(&mut self, name: &str) -> Option<Cursor> {
        self.last_activity = Instant::now();
        self.cursors.remove(name)
    }

    /// Record query start
    pub fn start_query(&mut self, query: &str) -> mpsc::Receiver<()> {
        let (tx, rx) = mpsc::channel(1);
        self.cancel_tx = Some(tx);
        self.current_query = Some(query.to_string());
        self.query_start = Some(Instant::now());
        self.status = SessionStatus::Busy;
        rx
    }

    /// Record query end
    pub fn end_query(&mut self, success: bool) {
        self.cancel_tx = None;
        self.current_query = None;
        self.query_start = None;
        self.query_count += 1;
        if !success {
            self.error_count += 1;
        }
        self.last_activity = Instant::now();

        // Restore status based on transaction state
        self.status = match &self.transaction {
            Some(t) if t.aborted => SessionStatus::IdleInAbortedTransaction,
            Some(_) => SessionStatus::IdleInTransaction,
            None => SessionStatus::Active,
        };
    }

    /// Cancel the current query
    pub async fn cancel_query(&mut self) -> bool {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(()).await;
            true
        } else {
            false
        }
    }

    /// Check if session has timed out
    pub fn is_timed_out(&self, idle_timeout: Duration, absolute_timeout: Duration) -> bool {
        let now = Instant::now();

        // Check absolute timeout
        if now.duration_since(self.created_at) > absolute_timeout {
            return true;
        }

        // Check idle timeout (only if not executing)
        if self.status != SessionStatus::Busy {
            if now.duration_since(self.last_activity) > idle_timeout {
                return true;
            }
        }

        false
    }

    /// Get session duration
    pub fn duration(&self) -> Duration {
        Instant::now().duration_since(self.created_at)
    }

    /// Get idle duration
    pub fn idle_duration(&self) -> Duration {
        Instant::now().duration_since(self.last_activity)
    }

    /// Get current query duration
    pub fn query_duration(&self) -> Option<Duration> {
        self.query_start.map(|start| Instant::now().duration_since(start))
    }

    /// Record bytes sent
    pub fn record_bytes_sent(&mut self, bytes: u64) {
        self.bytes_sent += bytes;
    }

    /// Record bytes received
    pub fn record_bytes_received(&mut self, bytes: u64) {
        self.bytes_received += bytes;
    }

    /// Reset session for pooling reuse
    pub fn reset_for_reuse(&mut self) {
        // Keep: id, protocol, created_at
        self.status = SessionStatus::Initializing;
        self.user = None;
        self.database = None;
        self.client_addr = None;
        self.parameters = SessionParameters::default();
        self.transaction = None;
        self.prepared_statements = LruCache::new(NonZeroUsize::new(256).unwrap());
        self.cursors.clear();
        self.last_activity = Instant::now();
        self.query_count = 0;
        self.error_count = 0;
        self.bytes_sent = 0;
        self.bytes_received = 0;
        self.current_query = None;
        self.query_start = None;
        self.cancel_tx = None;
    }
}

impl std::fmt::Debug for SessionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionContext")
            .field("id", &self.id)
            .field("status", &self.status)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("protocol", &self.protocol)
            .field("in_transaction", &self.in_transaction())
            .field("query_count", &self.query_count)
            .finish()
    }
}

// ============================================================================
// Session Manager
// ============================================================================

/// Session statistics
#[derive(Debug, Clone, Default)]
pub struct SessionStats {
    /// Total sessions created
    pub total_created: u64,
    /// Total sessions closed
    pub total_closed: u64,
    /// Current active sessions
    pub active_sessions: usize,
    /// Sessions timed out
    pub timed_out: u64,
    /// Sessions in transaction
    pub in_transaction: usize,
    /// Sessions currently executing
    pub busy: usize,
    /// Pooled sessions available
    pub pooled_available: usize,
}

/// Session manager for handling multiple client sessions
pub struct SessionManager {
    /// Configuration
    config: SessionConfig,
    /// Active sessions by ID
    sessions: DashMap<Uuid, Arc<RwLock<SessionContext>>>,
    /// Session pool for reuse
    pool: Mutex<Vec<Arc<RwLock<SessionContext>>>>,
    /// Statistics
    stats: SessionManagerStats,
    /// Shutdown flag
    shutdown: AtomicUsize,
}

struct SessionManagerStats {
    total_created: AtomicU64,
    total_closed: AtomicU64,
    timed_out: AtomicU64,
}

impl SessionManager {
    /// Create a new session manager
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            sessions: DashMap::new(),
            pool: Mutex::new(Vec::new()),
            stats: SessionManagerStats {
                total_created: AtomicU64::new(0),
                total_closed: AtomicU64::new(0),
                timed_out: AtomicU64::new(0),
            },
            shutdown: AtomicUsize::new(0),
        }
    }

    /// Create a new session
    pub fn create_session(&self, protocol: &str) -> Result<Arc<RwLock<SessionContext>>> {
        // Check max sessions
        if self.sessions.len() >= self.config.max_sessions {
            return Err(Error::Internal(format!(
                "Maximum sessions ({}) exceeded", self.config.max_sessions
            )));
        }

        // Try to get from pool first
        let session = if self.config.enable_pooling {
            let mut pool = self.pool.lock();
            if let Some(pooled) = pool.pop() {
                pooled.write().reset_for_reuse();
                debug!("Reusing pooled session");
                pooled
            } else {
                Arc::new(RwLock::new(SessionContext::new(protocol)))
            }
        } else {
            Arc::new(RwLock::new(SessionContext::new(protocol)))
        };

        let id = session.read().id;
        self.sessions.insert(id, session.clone());
        self.stats.total_created.fetch_add(1, Ordering::Relaxed);

        info!("Created session {}", id);
        Ok(session)
    }

    /// Get an existing session
    pub fn get_session(&self, id: Uuid) -> Option<Arc<RwLock<SessionContext>>> {
        self.sessions.get(&id).map(|s| s.clone())
    }

    /// Close a session
    pub fn close_session(&self, id: Uuid) -> bool {
        if let Some((_, session)) = self.sessions.remove(&id) {
            let mut ctx = session.write();
            ctx.status = SessionStatus::Closed;

            // Return to pool if pooling enabled and pool not full
            if self.config.enable_pooling {
                let mut pool = self.pool.lock();
                if pool.len() < self.config.pool_size {
                    drop(ctx);
                    pool.push(session);
                    debug!("Returned session {} to pool", id);
                }
            }

            self.stats.total_closed.fetch_add(1, Ordering::Relaxed);
            info!("Closed session {}", id);
            true
        } else {
            false
        }
    }

    /// Get session count
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Get all session IDs
    pub fn session_ids(&self) -> Vec<Uuid> {
        self.sessions.iter().map(|e| *e.key()).collect()
    }

    /// Get session statistics
    pub fn stats(&self) -> SessionStats {
        let mut in_transaction = 0;
        let mut busy = 0;

        for entry in self.sessions.iter() {
            let ctx = entry.value().read();
            if ctx.in_transaction() {
                in_transaction += 1;
            }
            if ctx.status == SessionStatus::Busy {
                busy += 1;
            }
        }

        SessionStats {
            total_created: self.stats.total_created.load(Ordering::Relaxed),
            total_closed: self.stats.total_closed.load(Ordering::Relaxed),
            active_sessions: self.sessions.len(),
            timed_out: self.stats.timed_out.load(Ordering::Relaxed),
            in_transaction,
            busy,
            pooled_available: self.pool.lock().len(),
        }
    }

    /// Clean up timed out sessions
    pub fn cleanup_timed_out(&self) -> Vec<Uuid> {
        let mut timed_out = Vec::new();

        for entry in self.sessions.iter() {
            let ctx = entry.value().read();
            if ctx.is_timed_out(self.config.idle_timeout, self.config.absolute_timeout) {
                timed_out.push(*entry.key());
            }
        }

        for id in &timed_out {
            warn!("Session {} timed out", id);
            self.close_session(*id);
            self.stats.timed_out.fetch_add(1, Ordering::Relaxed);
        }

        timed_out
    }

    /// Cancel a query in a session
    pub async fn cancel_query(&self, id: Uuid) -> bool {
        if let Some(session) = self.get_session(id) {
            let mut ctx = session.write();
            ctx.cancel_query().await
        } else {
            false
        }
    }

    /// List sessions with their info (for pg_stat_activity equivalent)
    pub fn list_sessions(&self) -> Vec<SessionInfo> {
        self.sessions.iter().map(|entry| {
            let ctx = entry.value().read();
            SessionInfo {
                id: ctx.id,
                user: ctx.user.clone(),
                database: ctx.database.clone(),
                client_addr: ctx.client_addr.clone(),
                protocol: ctx.protocol.clone(),
                status: ctx.status,
                query: ctx.current_query.clone(),
                query_start: ctx.query_start.map(|t| Instant::now().duration_since(t)),
                transaction_start: ctx.transaction.as_ref().map(|t| Instant::now().duration_since(t.start_time)),
                backend_start: Some(ctx.duration()),
            }
        }).collect()
    }

    /// Shutdown the session manager
    pub fn shutdown(&self) {
        self.shutdown.store(1, Ordering::SeqCst);

        // Close all sessions
        let ids: Vec<_> = self.sessions.iter().map(|e| *e.key()).collect();
        for id in ids {
            self.close_session(id);
        }

        // Clear pool
        self.pool.lock().clear();

        info!("Session manager shutdown complete");
    }

    /// Start the cleanup task
    pub fn start_cleanup_task(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                if self.shutdown.load(Ordering::SeqCst) != 0 {
                    break;
                }

                let timed_out = self.cleanup_timed_out();
                if !timed_out.is_empty() {
                    info!("Cleaned up {} timed out sessions", timed_out.len());
                }
            }
        })
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new(SessionConfig::default())
    }
}

/// Session information for listing
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub id: Uuid,
    pub user: Option<String>,
    pub database: Option<String>,
    pub client_addr: Option<String>,
    pub protocol: String,
    pub status: SessionStatus,
    pub query: Option<String>,
    pub query_start: Option<Duration>,
    pub transaction_start: Option<Duration>,
    pub backend_start: Option<Duration>,
}

// ============================================================================
// Session Handle (for convenient access)
// ============================================================================

/// Handle for working with a session
pub struct SessionHandle {
    session: Arc<RwLock<SessionContext>>,
    manager: Arc<SessionManager>,
}

impl SessionHandle {
    /// Create a new session handle
    pub fn new(session: Arc<RwLock<SessionContext>>, manager: Arc<SessionManager>) -> Self {
        Self { session, manager }
    }

    /// Get the session ID
    pub fn id(&self) -> Uuid {
        self.session.read().id
    }

    /// Execute an operation with read access
    pub fn with_read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&SessionContext) -> R,
    {
        f(&self.session.read())
    }

    /// Execute an operation with write access
    pub fn with_write<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut SessionContext) -> R,
    {
        f(&mut self.session.write())
    }

    /// Close this session
    pub fn close(self) {
        self.manager.close_session(self.id());
    }
}

impl Drop for SessionHandle {
    fn drop(&mut self) {
        // Update last activity when handle is dropped
        self.session.write().last_activity = Instant::now();
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_context_creation() {
        let ctx = SessionContext::new("postgres");
        assert_eq!(ctx.status, SessionStatus::Initializing);
        assert!(!ctx.in_transaction());
        assert!(ctx.prepared_statements.is_empty());
        assert!(ctx.cursors.is_empty());
    }

    #[test]
    fn test_session_activation() {
        let mut ctx = SessionContext::new("postgres");
        ctx.activate();
        assert_eq!(ctx.status, SessionStatus::Active);
    }

    #[test]
    fn test_transaction_lifecycle() {
        let mut ctx = SessionContext::new("postgres");
        ctx.activate();

        // Begin transaction
        ctx.begin_transaction(TxnId(1), IsolationLevel::ReadCommitted).unwrap();
        assert!(ctx.in_transaction());
        assert_eq!(ctx.status, SessionStatus::IdleInTransaction);

        // Commit
        let txn_id = ctx.commit_transaction().unwrap();
        assert_eq!(txn_id, TxnId(1));
        assert!(!ctx.in_transaction());
        assert_eq!(ctx.status, SessionStatus::Active);
    }

    #[test]
    fn test_savepoints() {
        let mut ctx = SessionContext::new("postgres");
        ctx.activate();
        ctx.begin_transaction(TxnId(1), IsolationLevel::ReadCommitted).unwrap();

        // Create savepoints
        ctx.create_savepoint("sp1").unwrap();
        ctx.create_savepoint("sp2").unwrap();

        let txn = ctx.transaction.as_ref().unwrap();
        assert_eq!(txn.savepoints.len(), 2);

        // Rollback to sp1
        ctx.rollback_to_savepoint("sp1").unwrap();
        let txn = ctx.transaction.as_ref().unwrap();
        assert_eq!(txn.savepoints.len(), 1);

        // Release sp1
        ctx.release_savepoint("sp1").unwrap();
        let txn = ctx.transaction.as_ref().unwrap();
        assert_eq!(txn.savepoints.len(), 0);
    }

    #[test]
    fn test_prepared_statements() {
        let mut ctx = SessionContext::new("postgres");

        let stmt = PreparedStatementEntry::new(
            "stmt1".to_string(),
            "SELECT * FROM users WHERE id = $1".to_string(),
            vec![DataType::Int64],
        );

        ctx.add_prepared_statement(stmt, 100).unwrap();
        assert!(ctx.get_prepared_statement("stmt1").is_some());

        ctx.remove_prepared_statement("stmt1");
        assert!(ctx.get_prepared_statement("stmt1").is_none());
    }

    #[test]
    fn test_cursors() {
        let mut ctx = SessionContext::new("postgres");

        let cursor = Cursor::new("c1".to_string(), "stmt1".to_string());
        ctx.add_cursor(cursor, 50).unwrap();

        assert!(ctx.get_cursor("c1").is_some());
        ctx.remove_cursor("c1");
        assert!(ctx.get_cursor("c1").is_none());
    }

    #[test]
    fn test_session_parameters() {
        let mut params = SessionParameters::default();

        params.set("search_path", "public, myschema").unwrap();
        assert_eq!(params.search_path, vec!["public", "myschema"]);

        params.set("statement_timeout", "5000").unwrap();
        assert_eq!(params.statement_timeout_ms, 5000);

        params.set("work_mem", "8MB").unwrap();
        assert_eq!(params.work_mem_kb, 8192);

        params.reset("search_path");
        assert_eq!(params.search_path, vec!["public"]);
    }

    #[test]
    fn test_session_manager() {
        let manager = SessionManager::new(SessionConfig::default());

        let session = manager.create_session("postgres").unwrap();
        let id = session.read().id;

        assert_eq!(manager.session_count(), 1);
        assert!(manager.get_session(id).is_some());

        manager.close_session(id);
        assert_eq!(manager.session_count(), 0);
    }

    #[test]
    fn test_session_manager_max_sessions() {
        let config = SessionConfig {
            max_sessions: 2,
            ..Default::default()
        };
        let manager = SessionManager::new(config);

        let _s1 = manager.create_session("postgres").unwrap();
        let _s2 = manager.create_session("postgres").unwrap();

        // Third session should fail
        let result = manager.create_session("postgres");
        assert!(result.is_err());
    }

    #[test]
    fn test_session_pooling() {
        let config = SessionConfig {
            enable_pooling: true,
            pool_size: 5,
            ..Default::default()
        };
        let manager = SessionManager::new(config);

        // Create and close a session
        let session = manager.create_session("postgres").unwrap();
        let id = session.read().id;
        manager.close_session(id);

        // Pool should have one session
        assert_eq!(manager.pool.lock().len(), 1);

        // Next create should reuse pooled session
        let _session2 = manager.create_session("postgres").unwrap();
        assert_eq!(manager.pool.lock().len(), 0);
    }

    #[test]
    fn test_session_timeout_check() {
        let mut ctx = SessionContext::new("postgres");
        ctx.activate();

        // Session should not be timed out immediately
        assert!(!ctx.is_timed_out(Duration::from_secs(300), Duration::from_secs(3600)));

        // Manually set old activity time for testing
        ctx.last_activity = Instant::now() - Duration::from_secs(400);
        assert!(ctx.is_timed_out(Duration::from_secs(300), Duration::from_secs(3600)));
    }

    #[test]
    fn test_session_stats() {
        let manager = SessionManager::new(SessionConfig::default());

        let s1 = manager.create_session("postgres").unwrap();
        let s2 = manager.create_session("mysql").unwrap();

        // Begin transaction in one session
        s1.write().activate();
        s1.write().begin_transaction(TxnId(1), IsolationLevel::ReadCommitted).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.active_sessions, 2);
        assert_eq!(stats.in_transaction, 1);
        assert_eq!(stats.total_created, 2);
    }

    #[test]
    fn test_list_sessions() {
        let manager = SessionManager::new(SessionConfig::default());

        let session = manager.create_session("postgres").unwrap();
        {
            let mut ctx = session.write();
            ctx.user = Some("alice".to_string());
            ctx.database = Some("mydb".to_string());
            ctx.activate();
        }

        let sessions = manager.list_sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].user, Some("alice".to_string()));
        assert_eq!(sessions[0].database, Some("mydb".to_string()));
        assert_eq!(sessions[0].protocol, "postgres");
    }

    #[tokio::test]
    async fn test_query_cancellation() {
        let mut ctx = SessionContext::new("postgres");
        ctx.activate();

        let _cancel_rx = ctx.start_query("SELECT * FROM large_table");
        assert_eq!(ctx.status, SessionStatus::Busy);

        let cancelled = ctx.cancel_query().await;
        assert!(cancelled);

        // Cancel again should return false
        let cancelled_again = ctx.cancel_query().await;
        assert!(!cancelled_again);
    }

    #[test]
    fn test_aborted_transaction() {
        let mut ctx = SessionContext::new("postgres");
        ctx.activate();

        ctx.begin_transaction(TxnId(1), IsolationLevel::ReadCommitted).unwrap();
        ctx.abort_transaction();

        assert!(ctx.in_aborted_transaction());
        assert_eq!(ctx.status, SessionStatus::IdleInAbortedTransaction);

        // Commit should fail
        let result = ctx.commit_transaction();
        assert!(result.is_err());

        // Rollback should succeed
        ctx.begin_transaction(TxnId(2), IsolationLevel::ReadCommitted).unwrap();
        ctx.abort_transaction();
        let result = ctx.rollback_transaction();
        assert!(result.is_ok());
    }

    #[test]
    fn test_session_reset_for_reuse() {
        let mut ctx = SessionContext::new("postgres");
        ctx.user = Some("alice".to_string());
        ctx.database = Some("mydb".to_string());
        ctx.query_count = 100;
        ctx.prepared_statements.push("stmt1".to_string(), PreparedStatementEntry::new(
            "stmt1".to_string(),
            "SELECT 1".to_string(),
            vec![],
        ));

        ctx.reset_for_reuse();

        assert!(ctx.user.is_none());
        assert!(ctx.database.is_none());
        assert_eq!(ctx.query_count, 0);
        assert!(ctx.prepared_statements.is_empty());
        assert_eq!(ctx.status, SessionStatus::Initializing);
    }
}
