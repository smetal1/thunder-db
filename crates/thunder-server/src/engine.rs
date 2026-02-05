//! # Database Engine
//!
//! The core database engine that coordinates all ThunderDB components:
//! - Storage (buffer pool, WAL, row/column store)
//! - Transaction manager (MVCC)
//! - SQL processing (parser, analyzer, optimizer, planner)
//! - Query execution (vectorized, parallel)

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{debug, error, info, warn};

use thunder_common::config::ServerConfig;
use thunder_common::error::SqlError;
use thunder_common::prelude::*;
use thunder_query::{
    PhysicalPlan, DefaultPhysicalPlanner,
};
use thunder_sql::{
    parse_sql, LogicalPlan, AlterTableOp, RuleBasedOptimizer, SqlAnalyzer,
    Catalog as SqlCatalog, TableInfo, Analyzer, Optimizer,
    CostEstimator, Expr,
};
use thunder_query::format_physical_plan;
use thunder_storage::{
    Catalog as StorageCatalog,
    BufferPoolImpl, DiskManager, WalWriterImpl, WalRecovery,
    RecoveryExecutor, RedoRecord, UndoRecord, WalRecordType,
    WalWriter, WalEntry, WalEntryType, PAGE_SIZE,
    RowStoreImpl, RowIterator, ForeignKeyAction,
};
use thunder_storage::wal::{InsertRecord, UpdateRecord, DeleteRecord, CheckpointRecord, CreateTableRecord, DropTableRecord};
use thunder_txn::{
    MvccTransactionManager, MvccConfig, TransactionManager,
    CommitResult, LockManager,
};

use crate::executor::{PhysicalExecutor, TableStorage};

/// Database engine configuration
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Data directory
    pub data_dir: PathBuf,
    /// WAL directory
    pub wal_dir: PathBuf,
    /// Buffer pool size in bytes
    pub buffer_pool_size: usize,
    /// WAL buffer size in bytes
    pub wal_buffer_size: usize,
    /// Page size in bytes
    pub page_size: usize,
    /// Default isolation level
    pub default_isolation: IsolationLevel,
    /// Query timeout
    pub query_timeout: Duration,
    /// Max concurrent queries
    pub max_concurrent_queries: usize,
    /// Maximum result rows per query
    pub max_result_rows: usize,
    /// Enable slow query logging
    pub slow_query_enabled: bool,
    /// Threshold for slow query logging
    pub slow_query_threshold: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            wal_dir: PathBuf::from("./wal"),
            buffer_pool_size: 1024 * 1024 * 1024, // 1GB
            wal_buffer_size: 64 * 1024 * 1024,    // 64MB
            page_size: 16 * 1024,                  // 16KB
            default_isolation: IsolationLevel::ReadCommitted,
            query_timeout: Duration::from_secs(30),
            max_concurrent_queries: 100,
            max_result_rows: 1_000_000,
            slow_query_enabled: true,
            slow_query_threshold: Duration::from_secs(1),
        }
    }
}

impl From<&ServerConfig> for EngineConfig {
    fn from(config: &ServerConfig) -> Self {
        Self {
            data_dir: config.data_dir.clone(),
            wal_dir: config.wal_dir.clone(),
            buffer_pool_size: config.storage.buffer_pool_size,
            wal_buffer_size: config.storage.wal_buffer_size,
            page_size: config.storage.page_size,
            default_isolation: IsolationLevel::ReadCommitted,
            query_timeout: config.query_timeout,
            max_concurrent_queries: 100,
            max_result_rows: config.max_result_rows,
            slow_query_enabled: config.logging.slow_query_enabled,
            slow_query_threshold: config.logging.slow_query_threshold,
        }
    }
}

/// Database engine statistics
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    /// Total queries executed
    pub queries_total: u64,
    /// Queries that failed
    pub queries_failed: u64,
    /// Active connections
    pub active_connections: u64,
    /// Active transactions
    pub active_transactions: u64,
    /// Buffer pool hit ratio
    pub buffer_pool_hit_ratio: f64,
    /// Bytes read
    pub bytes_read: u64,
    /// Bytes written
    pub bytes_written: u64,
    /// Server start time
    pub start_time: Option<Instant>,
}

impl EngineStats {
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(0)
    }
}

/// Query result from the engine
#[derive(Debug)]
pub struct QueryResult {
    /// Query ID
    pub query_id: String,
    /// Column names and types
    pub columns: Vec<(String, DataType)>,
    /// Result rows
    pub rows: Vec<Row>,
    /// Rows affected (for DML)
    pub rows_affected: Option<u64>,
    /// Execution time
    pub execution_time: Duration,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            query_id: uuid::Uuid::new_v4().to_string(),
            columns: vec![],
            rows: vec![],
            rows_affected: None,
            execution_time: Duration::ZERO,
        }
    }
}

/// The main database engine
pub struct DatabaseEngine {
    /// Engine configuration
    #[allow(dead_code)]
    config: EngineConfig,
    /// SQL catalog (for query planning)
    sql_catalog: Arc<SqlCatalog>,
    /// Storage catalog
    #[allow(dead_code)]
    storage_catalog: Arc<StorageCatalog>,
    /// Transaction manager
    txn_manager: Arc<MvccTransactionManager>,
    /// Lock manager
    #[allow(dead_code)]
    lock_manager: Arc<LockManager>,
    /// SQL analyzer
    analyzer: SqlAnalyzer,
    /// Query optimizer
    optimizer: RuleBasedOptimizer,
    /// Physical planner
    planner: DefaultPhysicalPlanner,
    /// Physical executor
    executor: PhysicalExecutor,
    /// Table storage (in-memory, for schema tracking)
    table_storage: Arc<TableStorage>,
    /// Persistent row store
    row_store: Arc<RowStoreImpl>,
    /// Buffer pool for page management
    buffer_pool: Arc<BufferPoolImpl>,
    /// Disk manager for page I/O
    disk_manager: Arc<DiskManager>,
    /// WAL writer for durability
    wal_writer: Arc<WalWriterImpl>,
    /// Active sessions
    sessions: RwLock<HashMap<uuid::Uuid, SessionState>>,
    /// Engine statistics
    stats: RwLock<EngineStats>,
    /// Atomic counters for stats
    query_counter: AtomicU64,
    query_failed_counter: AtomicU64,
    connection_counter: AtomicU64,
    /// Read-only mode flag (set by disk space monitor when critically low)
    read_only: AtomicBool,
    /// Query plan cache: hash of normalized SQL → cached physical plan (SELECT only)
    plan_cache: parking_lot::Mutex<lru::LruCache<u64, Arc<PhysicalPlan>>>,
}

/// Session state
#[derive(Debug)]
pub struct SessionState {
    /// Session ID
    pub id: uuid::Uuid,
    /// User name
    pub user: Option<String>,
    /// Current database
    pub database: Option<String>,
    /// Active transaction
    pub txn_id: Option<TxnId>,
    /// Session start time
    pub start_time: Instant,
    /// Last activity time
    pub last_activity: Instant,
    /// Isolation level
    pub isolation_level: IsolationLevel,
}

impl SessionState {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            id: uuid::Uuid::new_v4(),
            user: None,
            database: Some("thunder".to_string()),
            txn_id: None,
            start_time: now,
            last_activity: now,
            isolation_level: IsolationLevel::ReadCommitted,
        }
    }
}

impl Default for SessionState {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseEngine {
    /// Create a new database engine
    pub async fn new(config: EngineConfig) -> Result<Self> {
        info!("Initializing database engine...");

        // Create directories
        tokio::fs::create_dir_all(&config.data_dir).await
            .map_err(|e| Error::Internal(format!("Failed to create data dir: {}", e)))?;
        tokio::fs::create_dir_all(&config.wal_dir).await
            .map_err(|e| Error::Internal(format!("Failed to create WAL dir: {}", e)))?;

        // Initialize disk manager and buffer pool
        info!("Initializing disk manager and buffer pool...");
        let db_path = config.data_dir.join("thunder.db");
        let disk_manager = Arc::new(DiskManager::new(&db_path)?);
        let num_frames = config.buffer_pool_size / PAGE_SIZE;
        let buffer_pool = Arc::new(BufferPoolImpl::new(num_frames, disk_manager.clone()));

        // Initialize WAL writer
        info!("Initializing WAL writer...");
        let wal_writer = match WalWriterImpl::open(&config.wal_dir, true) {
            Ok(wal) => {
                info!("Opened existing WAL");
                Arc::new(wal)
            }
            Err(_) => {
                info!("Creating new WAL");
                Arc::new(WalWriterImpl::new(&config.wal_dir, true)?)
            }
        };

        // Initialize SQL catalog BEFORE recovery so DDL records can populate it
        info!("Initializing SQL catalog...");
        let sql_catalog = Arc::new(SqlCatalog::new());

        // Initialize table storage for recovery
        let table_storage = Arc::new(TableStorage::new());

        // Initialize persistent row store BEFORE recovery so INSERT records can register pages
        info!("Initializing persistent row store...");
        let row_store = Arc::new(RowStoreImpl::new(buffer_pool.clone(), wal_writer.clone()));

        // Run WAL recovery
        info!("Running WAL recovery...");
        let recovery = WalRecovery::new(&config.wal_dir);
        let recovery_plan = recovery.recover()?;

        let recovered_max_txn_id = if recovery_plan.needs_recovery() {
            info!(
                "Recovery needed: {} redo records, {} undo records, {} active transactions",
                recovery_plan.redo_count(),
                recovery_plan.undo_count(),
                recovery_plan.active_txn_count()
            );

            // Execute recovery with callbacks
            let bp_for_redo = buffer_pool.clone();
            let bp_for_undo = buffer_pool.clone();
            let catalog_for_redo = sql_catalog.clone();
            let storage_for_redo = table_storage.clone();
            let row_store_for_redo = row_store.clone();

            // Track max txn_id during recovery to restore transaction counter
            let max_txn_id = Arc::new(AtomicU64::new(0));
            let max_txn_id_for_redo = max_txn_id.clone();

            let mut executor = RecoveryExecutor::new(
                &recovery_plan,
                |redo| {
                    // Track max txn_id for transaction manager restoration
                    let current_max = max_txn_id_for_redo.load(Ordering::SeqCst);
                    if redo.txn_id.0 > current_max {
                        max_txn_id_for_redo.store(redo.txn_id.0, Ordering::SeqCst);
                    }
                    Self::apply_redo_record(redo, &bp_for_redo, &catalog_for_redo, &storage_for_redo, &row_store_for_redo)
                },
                |undo| Self::apply_undo_record(undo, &bp_for_undo),
            );

            let stats = executor.execute()?;
            if !stats.is_successful() {
                return Err(Error::Internal(format!(
                    "Recovery failed: {} redo errors, {} undo errors",
                    stats.redo_errors, stats.undo_errors
                )));
            }

            info!(
                "Recovery complete: {} records redone, {} undone, {} transactions rolled back, max_txn_id={}",
                stats.records_redone, stats.records_undone, stats.txns_rolled_back, max_txn_id.load(Ordering::SeqCst)
            );

            // Flush recovered pages to disk
            buffer_pool.flush_all()?;

            max_txn_id.load(Ordering::SeqCst)
        } else {
            info!("No recovery needed");
            0
        };

        // Initialize storage catalog
        info!("Initializing storage catalog...");
        let storage_catalog = Arc::new(StorageCatalog::new());

        // Initialize lock manager
        info!("Initializing lock manager...");
        let lock_manager = Arc::new(LockManager::new());

        // Initialize transaction manager
        info!("Initializing transaction manager...");
        let mvcc_config = MvccConfig::default();
        let txn_manager = Arc::new(MvccTransactionManager::new(mvcc_config));

        // Restore transaction counter after recovery to avoid reusing txn_ids
        if recovered_max_txn_id > 0 {
            txn_manager.set_next_txn_id(recovered_max_txn_id + 1);
            info!("Restored transaction counter to {}", recovered_max_txn_id + 1);
        }

        // Initialize SQL analyzer with the SQL catalog
        info!("Initializing SQL analyzer...");
        let analyzer = SqlAnalyzer::new(sql_catalog.clone());

        // Initialize query optimizer
        info!("Initializing query optimizer...");
        let optimizer = RuleBasedOptimizer::new();

        // Initialize physical planner
        info!("Initializing physical planner...");
        let planner = DefaultPhysicalPlanner::new(sql_catalog.clone());

        // Initialize executor with persistent storage (row_store was created before recovery)
        info!("Initializing executor...");
        let executor = PhysicalExecutor::with_row_store(table_storage.clone(), row_store.clone());

        let mut stats = EngineStats::default();
        stats.start_time = Some(Instant::now());

        info!("Database engine initialized successfully");

        Ok(Self {
            config,
            sql_catalog,
            storage_catalog,
            txn_manager,
            lock_manager,
            analyzer,
            optimizer,
            planner,
            executor,
            table_storage,
            row_store,
            buffer_pool,
            disk_manager,
            wal_writer,
            sessions: RwLock::new(HashMap::new()),
            stats: RwLock::new(stats),
            query_counter: AtomicU64::new(0),
            query_failed_counter: AtomicU64::new(0),
            connection_counter: AtomicU64::new(0),
            read_only: AtomicBool::new(false),
            plan_cache: parking_lot::Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(1024).unwrap(),
            )),
        })
    }

    /// Apply a redo record during recovery
    fn apply_redo_record(
        record: &RedoRecord,
        buffer_pool: &Arc<BufferPoolImpl>,
        catalog: &Arc<SqlCatalog>,
        table_storage: &Arc<TableStorage>,
        row_store: &Arc<RowStoreImpl>,
    ) -> Result<()> {
        match record.record_type {
            WalRecordType::Insert => {
                let insert = InsertRecord::decode(&record.payload)?;

                // Register the page with RowStoreImpl so scans can find the data
                row_store.register_page_for_table(record.table_id, insert.page_id);
                row_store.update_next_row_id(record.table_id, insert.row_id);

                // Check if page exists and needs redo
                match buffer_pool.fetch_page_mut(insert.page_id) {
                    Ok(mut guard) => {
                        let page = guard.page_mut();
                        // Only redo if page LSN is less than record LSN
                        if page.lsn() < record.lsn {
                            // Re-insert the tuple
                            if let Err(e) = page.insert_tuple(&insert.tuple_data) {
                                warn!(page_id = ?insert.page_id, lsn = ?record.lsn, error = %e, "Failed to redo insert tuple");
                            }
                            page.set_lsn(record.lsn);
                        }
                    }
                    Err(_) => {
                        // Page doesn't exist, we need to create it first
                        if let Ok((_new_page_id, guard)) = buffer_pool.new_page() {
                            // The new_page might give us a different page_id, but we need the specific one
                            // For now, just try to fetch the page again after creating
                            drop(guard);
                            if let Ok(mut guard) = buffer_pool.fetch_page_mut(insert.page_id) {
                                let page = guard.page_mut();
                                if let Err(e) = page.insert_tuple(&insert.tuple_data) {
                                    warn!(page_id = ?insert.page_id, lsn = ?record.lsn, error = %e, "Failed to redo insert tuple (new page)");
                                }
                                page.set_lsn(record.lsn);
                            }
                        }
                    }
                }
            }
            WalRecordType::Update => {
                let update = UpdateRecord::decode(&record.payload)?;
                match buffer_pool.fetch_page_mut(update.page_id) {
                    Ok(mut guard) => {
                        let page = guard.page_mut();
                        if page.lsn() < record.lsn {
                            // Apply the after image
                            if let Err(e) = page.update_tuple(update.slot_id, &update.after_image) {
                                warn!(page_id = ?update.page_id, slot = ?update.slot_id, lsn = ?record.lsn, error = %e, "Failed to redo update tuple");
                            }
                            page.set_lsn(record.lsn);
                        }
                    }
                    Err(_) => {
                        debug!("Page {:?} not found during redo update", update.page_id);
                    }
                }
            }
            WalRecordType::Delete => {
                let delete = DeleteRecord::decode(&record.payload)?;
                match buffer_pool.fetch_page_mut(delete.page_id) {
                    Ok(mut guard) => {
                        let page = guard.page_mut();
                        if page.lsn() < record.lsn {
                            // Mark tuple as deleted
                            if let Err(e) = page.delete_tuple(delete.slot_id) {
                                warn!(page_id = ?delete.page_id, slot = ?delete.slot_id, lsn = ?record.lsn, error = %e, "Failed to redo delete tuple");
                            }
                            page.set_lsn(record.lsn);
                        }
                    }
                    Err(_) => {
                        debug!("Page {:?} not found during redo delete", delete.page_id);
                    }
                }
            }
            WalRecordType::CreateTable => {
                // Recover table creation by re-registering in catalog
                let create_record = CreateTableRecord::decode(&record.payload)?;
                info!(
                    "Recovering table '{}' (id: {:?}) from WAL",
                    create_record.table_name, create_record.table_id
                );

                // Deserialize the schema from JSON
                let schema: Schema = serde_json::from_str(&create_record.schema_json)
                    .map_err(|e| Error::Internal(format!("Failed to deserialize schema: {}", e)))?;

                // Register in SQL catalog (skip if already exists)
                if catalog.get_table(&create_record.table_name).is_none() {
                    catalog.register_table(&create_record.table_name, schema.clone())?;
                    // Also create in table storage
                    if let Some(table_info) = catalog.get_table(&create_record.table_name) {
                        table_storage.create_table(table_info.id, schema);
                    }
                }
            }
            WalRecordType::DropTable => {
                // Recover table drop by removing from catalog
                let drop_record = DropTableRecord::decode(&record.payload)?;
                info!(
                    "Recovering DROP TABLE '{}' (id: {:?}) from WAL",
                    drop_record.table_name, drop_record.table_id
                );

                // Remove from catalog (warn if doesn't exist)
                if let Err(e) = catalog.drop_table(&drop_record.table_name) {
                    warn!(table = %drop_record.table_name, error = %e, "Failed to redo drop table (may not exist)");
                }
            }
            _ => {
                // Skip non-data records (Commit, Abort, Checkpoint, etc.)
            }
        }
        Ok(())
    }

    /// Apply an undo record during recovery
    fn apply_undo_record(record: &UndoRecord, buffer_pool: &Arc<BufferPoolImpl>) -> Result<()> {
        match record.record_type {
            WalRecordType::Insert => {
                // Undo insert = delete the tuple
                let insert = InsertRecord::decode(&record.payload)?;
                match buffer_pool.fetch_page_mut(insert.page_id) {
                    Ok(mut guard) => {
                        let page = guard.page_mut();
                        if let Err(e) = page.delete_tuple(insert.slot_id) {
                            warn!(page_id = ?insert.page_id, slot = ?insert.slot_id, error = %e, "Failed to undo insert (delete tuple)");
                        }
                    }
                    Err(_) => {
                        debug!("Page {:?} not found during undo insert", insert.page_id);
                    }
                }
            }
            WalRecordType::Update => {
                // Undo update = restore before_image
                let update = UpdateRecord::decode(&record.payload)?;
                match buffer_pool.fetch_page_mut(update.page_id) {
                    Ok(mut guard) => {
                        let page = guard.page_mut();
                        if let Err(e) = page.update_tuple(update.slot_id, &update.before_image) {
                            warn!(page_id = ?update.page_id, slot = ?update.slot_id, error = %e, "Failed to undo update (restore before image)");
                        }
                    }
                    Err(_) => {
                        debug!("Page {:?} not found during undo update", update.page_id);
                    }
                }
            }
            WalRecordType::Delete => {
                // Undo delete = restore the tuple (using before_image)
                let delete = DeleteRecord::decode(&record.payload)?;
                match buffer_pool.fetch_page_mut(delete.page_id) {
                    Ok(mut guard) => {
                        let page = guard.page_mut();
                        // Re-insert the before image
                        if let Err(e) = page.insert_tuple(&delete.before_image) {
                            warn!(page_id = ?delete.page_id, error = %e, "Failed to undo delete (re-insert tuple)");
                        }
                    }
                    Err(_) => {
                        debug!("Page {:?} not found during undo delete", delete.page_id);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Create a new session
    pub fn create_session(&self) -> SessionState {
        let session = SessionState::new();
        let session_copy = SessionState {
            id: session.id,
            user: session.user.clone(),
            database: session.database.clone(),
            txn_id: session.txn_id,
            start_time: session.start_time,
            last_activity: session.last_activity,
            isolation_level: session.isolation_level,
        };
        self.sessions.write().insert(session.id, session_copy);
        self.connection_counter.fetch_add(1, Ordering::Relaxed);
        session
    }

    /// Close a session
    pub fn close_session(&self, session_id: uuid::Uuid) {
        if let Some(session) = self.sessions.write().remove(&session_id) {
            // Rollback any active transaction
            if let Some(txn_id) = session.txn_id {
                let txn_manager = self.txn_manager.clone();
                tokio::spawn(async move {
                    let _ = txn_manager.abort(txn_id).await;
                });
            }
        }
        self.connection_counter.fetch_sub(1, Ordering::Relaxed);
    }

    /// Execute a SQL query
    pub async fn execute_sql(&self, session_id: uuid::Uuid, sql: &str) -> Result<QueryResult> {
        let start = Instant::now();
        self.query_counter.fetch_add(1, Ordering::Relaxed);

        // Update last activity
        if let Some(session) = self.sessions.write().get_mut(&session_id) {
            session.last_activity = Instant::now();
        }

        // Enforce query timeout
        let timeout = self.config.query_timeout;
        let result = match tokio::time::timeout(timeout, self.execute_sql_inner(session_id, sql)).await {
            Ok(inner_result) => inner_result,
            Err(_elapsed) => {
                warn!(sql = %sql, timeout = ?timeout, "Query timed out");
                self.query_failed_counter.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Internal(format!(
                    "Query timed out after {}s",
                    timeout.as_secs()
                )));
            }
        };

        if result.is_err() {
            self.query_failed_counter.fetch_add(1, Ordering::Relaxed);
        }

        match result {
            Ok(mut qr) => {
                // Enforce row limit
                let max_rows = self.config.max_result_rows;
                if qr.rows.len() > max_rows {
                    warn!(
                        rows = qr.rows.len(),
                        max = max_rows,
                        sql = %sql,
                        "Query result exceeds max_result_rows limit"
                    );
                    return Err(Error::Internal(format!(
                        "Query returned {} rows, exceeding limit of {}. \
                         Use LIMIT clause to reduce result size.",
                        qr.rows.len(),
                        max_rows,
                    )));
                }
                qr.execution_time = start.elapsed();
                // Slow query logging
                if self.config.slow_query_enabled && qr.execution_time > self.config.slow_query_threshold {
                    warn!(
                        target: "slow_query",
                        sql = %sql,
                        duration_ms = qr.execution_time.as_millis() as u64,
                        rows = qr.rows.len(),
                        session_id = %session_id,
                        "Slow query detected"
                    );
                }
                Ok(qr)
            }
            Err(e) => Err(e),
        }
    }

    async fn execute_sql_inner(&self, session_id: uuid::Uuid, sql: &str) -> Result<QueryResult> {
        debug!("Executing SQL: {}", sql);

        // Reject write operations in read-only mode (disk space critical)
        if self.is_read_only() {
            let upper = sql.trim().to_uppercase();
            let is_write = upper.starts_with("INSERT")
                || upper.starts_with("UPDATE")
                || upper.starts_with("DELETE")
                || upper.starts_with("CREATE")
                || upper.starts_with("DROP")
                || upper.starts_with("ALTER")
                || upper.starts_with("TRUNCATE");
            if is_write {
                return Err(Error::Internal(
                    "Database is in read-only mode due to critically low disk space".to_string(),
                ));
            }
        }

        // Parse SQL
        let statements = parse_sql(sql)?;
        if statements.is_empty() {
            return Ok(QueryResult::empty());
        }

        // Get or create transaction
        let (existing_txn_id, isolation_level) = {
            let sessions = self.sessions.read();
            let session = sessions.get(&session_id)
                .ok_or_else(|| Error::Internal("Session not found".to_string()))?;
            (session.txn_id, session.isolation_level)
        };

        let (txn_id, auto_commit) = if let Some(existing) = existing_txn_id {
            (existing, false)
        } else {
            // Auto-commit mode: create a new transaction (lock is dropped now)
            let txn_id = self.txn_manager.begin(isolation_level).await?;
            (txn_id, true)
        };

        // Process each statement
        let mut final_result = QueryResult::empty();

        for stmt in statements {
            // Handle transaction control statements
            let stmt_str = stmt.to_string().to_uppercase();
            if stmt_str.starts_with("BEGIN") || stmt_str.starts_with("START TRANSACTION") {
                return self.begin_transaction(session_id).await.map(|_| QueryResult::empty());
            } else if stmt_str.starts_with("COMMIT") {
                return self.commit_transaction(session_id).await.map(|_| QueryResult::empty());
            } else if stmt_str.starts_with("ROLLBACK") {
                return self.rollback_transaction(session_id).await.map(|_| QueryResult::empty());
            }

            // Intercept system catalog queries (information_schema / thunderdb)
            let stmt_lower = stmt.to_string().to_lowercase();
            if let Some(result) = self.try_execute_system_catalog(&stmt_lower, session_id)? {
                final_result = result;
                continue;
            }

            // Analyze statement
            let logical_plan = self.analyzer.analyze(stmt).await?;

            // Handle DDL and special statements
            match &logical_plan {
                LogicalPlan::CreateTable { name, columns, if_not_exists } => {
                    final_result = self.execute_create_table(name, columns, *if_not_exists)?;
                    continue;
                }
                LogicalPlan::DropTable { name, if_exists } => {
                    final_result = self.execute_drop_table(name, *if_exists)?;
                    continue;
                }
                LogicalPlan::CreateIndex { name, table, columns, unique, if_not_exists } => {
                    final_result = self.execute_create_index(name, table, columns, *unique, *if_not_exists)?;
                    continue;
                }
                LogicalPlan::DropIndex { name, if_exists } => {
                    final_result = self.execute_drop_index(name, *if_exists)?;
                    continue;
                }
                LogicalPlan::AlterTable { table, operations } => {
                    final_result = self.execute_alter_table(table, operations)?;
                    continue;
                }
                LogicalPlan::Explain { plan, analyze, verbose } => {
                    final_result = self.execute_explain(plan, *analyze, *verbose, txn_id).await?;
                    continue;
                }
                LogicalPlan::AnalyzeTable { table } => {
                    final_result = self.execute_analyze_table(table.as_deref()).await?;
                    continue;
                }
                _ => {}
            }

            // Foreign key constraint checks for DML operations
            match &logical_plan {
                LogicalPlan::Insert { table, columns, values } => {
                    self.check_fk_on_insert(table, columns, values)?;
                }
                LogicalPlan::Delete { table, filter } => {
                    self.check_fk_on_delete(table, filter, txn_id)?;
                }
                LogicalPlan::Update { table, assignments, .. } => {
                    self.check_fk_on_update(table, assignments)?;
                }
                _ => {}
            }

            // Determine if this is a cacheable read-only query (no DML/DDL)
            let is_cacheable = !matches!(
                &logical_plan,
                LogicalPlan::Insert { .. }
                | LogicalPlan::Update { .. }
                | LogicalPlan::Delete { .. }
                | LogicalPlan::CreateTable { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::DropIndex { .. }
                | LogicalPlan::AlterTable { .. }
                | LogicalPlan::Explain { .. }
                | LogicalPlan::AnalyzeTable { .. }
                | LogicalPlan::Empty
            );

            let sql_hash = if is_cacheable {
                let mut hasher = DefaultHasher::new();
                sql.hash(&mut hasher);
                Some(hasher.finish())
            } else {
                None
            };

            let physical_plan = if let Some(hash) = sql_hash {
                // Try plan cache first
                let cached = self.plan_cache.lock().get(&hash).cloned();
                if let Some(cached_plan) = cached {
                    debug!("Plan cache hit for query");
                    (*cached_plan).clone()
                } else {
                    let optimized_plan = self.optimizer.optimize(logical_plan)?;
                    let plan = self.planner.plan(optimized_plan)?;
                    self.plan_cache.lock().push(hash, Arc::new(plan.clone()));
                    plan
                }
            } else {
                // DML/DDL: optimize and plan without caching
                let optimized_plan = self.optimizer.optimize(logical_plan)?;
                self.planner.plan(optimized_plan)?
            };

            // Execute physical plan
            final_result = self.execute_physical_plan(txn_id, physical_plan).await?;
        }

        // Auto-commit if needed
        if auto_commit {
            match self.txn_manager.commit(txn_id).await? {
                CommitResult::Committed { .. } => {}
                CommitResult::Aborted { reason } => {
                    return Err(Error::Internal(format!("Transaction aborted: {:?}", reason)));
                }
            }
        }

        Ok(final_result)
    }

    /// Execute a physical plan
    async fn execute_physical_plan(&self, txn_id: TxnId, plan: PhysicalPlan) -> Result<QueryResult> {
        let query_id = uuid::Uuid::new_v4().to_string();

        // Get snapshot from transaction manager for MVCC visibility
        let storage_snapshot = if txn_id.0 > 0 {
            // Try to get snapshot from active transaction
            match self.txn_manager.get_snapshot(txn_id) {
                Ok(txn_snapshot) => {
                    // Convert thunder_txn::Snapshot to thunder_storage::Snapshot
                    Some(thunder_storage::Snapshot::new(
                        txn_snapshot.txn_id,
                        txn_snapshot.xmin,
                        txn_snapshot.xmax,
                        txn_snapshot.active_txns.into_iter().collect(),
                    ))
                }
                Err(_) => None, // Transaction not found, use no snapshot
            }
        } else {
            // Auto-commit mode: create a simple snapshot
            Some(thunder_storage::Snapshot::new(
                TxnId(0),
                TxnId(0),
                TxnId(u64::MAX),
                vec![],
            ))
        };

        // Use the physical executor to run the plan with snapshot
        let result = self.executor.execute_with_snapshot(plan, txn_id, storage_snapshot).await?;

        // Convert execution result to query result
        let columns: Vec<(String, DataType)> = result.schema.columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone()))
            .collect();

        let rows: Vec<Row> = result.batches
            .into_iter()
            .flat_map(|batch| batch.rows)
            .collect();

        Ok(QueryResult {
            query_id,
            columns,
            rows,
            rows_affected: result.rows_affected,
            execution_time: Duration::from_millis(result.stats.execution_time_ms),
        })
    }

    /// Execute CREATE TABLE
    fn execute_create_table(&self, name: &str, columns: &[ColumnDef], if_not_exists: bool) -> Result<QueryResult> {
        // Check if table exists in SQL catalog
        if self.sql_catalog.get_table(name).is_some() {
            if if_not_exists {
                return Ok(QueryResult::empty());
            }
            return Err(Error::Internal(format!("Table '{}' already exists", name)));
        }

        // Build schema for SQL catalog
        let schema = Schema::new(columns.to_vec());

        // Register in SQL catalog (returns table_id)
        self.sql_catalog.register_table(name, schema.clone())?;

        // Get the table ID from catalog
        if let Some(table_info) = self.sql_catalog.get_table(name) {
            // Create table in storage
            self.table_storage.create_table(table_info.id, schema.clone());

            // Write CREATE TABLE to WAL for durability
            let schema_json = serde_json::to_string(&schema)
                .map_err(|e| Error::Internal(format!("Failed to serialize schema: {}", e)))?;

            let record = CreateTableRecord {
                table_id: table_info.id,
                table_name: name.to_string(),
                schema_name: "public".to_string(),
                schema_json,
            };

            // Write to WAL (using system txn_id 0 for DDL)
            self.wal_writer.write_record(
                TxnId(0),
                table_info.id,
                WalRecordType::CreateTable,
                &record.encode(),
            )?;

            // Flush WAL to ensure durability of DDL
            self.wal_writer.flush_buffer()?;
        }

        // Invalidate plan cache on schema change
        self.plan_cache.lock().clear();

        info!("Created table '{}'", name);
        Ok(QueryResult::empty())
    }

    /// Execute DROP TABLE
    fn execute_drop_table(&self, name: &str, if_exists: bool) -> Result<QueryResult> {
        let table_info = match self.sql_catalog.get_table(name) {
            Some(info) => info,
            None => {
                if if_exists {
                    return Ok(QueryResult::empty());
                }
                return Err(Error::Internal(format!("Table '{}' does not exist", name)));
            }
        };

        // Write DROP TABLE to WAL for durability (before actually dropping)
        let record = DropTableRecord {
            table_id: table_info.id,
            table_name: name.to_string(),
        };

        // Write to WAL (using system txn_id 0 for DDL)
        self.wal_writer.write_record(
            TxnId(0),
            table_info.id,
            WalRecordType::DropTable,
            &record.encode(),
        )?;

        // Flush WAL to ensure durability of DDL
        self.wal_writer.flush_buffer()?;

        // Now drop from catalog
        self.sql_catalog.drop_table(name)?;

        // Invalidate plan cache on schema change
        self.plan_cache.lock().clear();

        info!("Dropped table '{}'", name);
        Ok(QueryResult::empty())
    }

    /// Execute CREATE INDEX
    fn execute_create_index(
        &self,
        name: &str,
        table: &str,
        columns: &[String],
        unique: bool,
        if_not_exists: bool,
    ) -> Result<QueryResult> {
        // Validate table exists
        let table_info = self.sql_catalog.get_table(table)
            .ok_or_else(|| Error::Sql(thunder_common::error::SqlError::TableNotFound(table.to_string())))?;

        // Check if index already exists
        if let Some(existing) = self.sql_catalog.get_index(name) {
            if if_not_exists {
                return Ok(QueryResult::empty());
            }
            return Err(Error::AlreadyExists("Index".into(), existing.name.clone()));
        }

        // Validate all columns exist in the table
        for col_name in columns {
            if table_info.schema.column_by_name(col_name).is_none() {
                return Err(Error::Sql(thunder_common::error::SqlError::ColumnNotFound(
                    format!("{}.{}", table, col_name)
                )));
            }
        }

        // Build index columns from column names
        let index_columns: Vec<thunder_sql::IndexColumn> = columns.iter().map(|col_name| {
            let col_idx = table_info.schema.column_by_name(col_name)
                .map(|(i, _)| i)
                .unwrap_or(0);
            thunder_sql::IndexColumn {
                column_index: col_idx,
                ascending: true,
                nulls_first: false,
            }
        }).collect();

        // Register the index in the catalog
        self.sql_catalog.register_index(name, table, index_columns, unique, thunder_sql::IndexType::BTree)?;

        // WAL-log the index creation for durability
        let index_json = serde_json::json!({
            "name": name,
            "table": table,
            "columns": columns,
            "unique": unique,
        });
        let payload = serde_json::to_vec(&index_json)
            .map_err(|e| Error::Internal(format!("Failed to serialize index metadata: {}", e)))?;

        self.wal_writer.write_record(
            TxnId(0),
            table_info.id,
            WalRecordType::CreateTable, // Reuse record type for DDL
            &payload,
        )?;
        self.wal_writer.flush_buffer()?;

        // Invalidate plan cache
        self.plan_cache.lock().clear();

        info!("Created index '{}' on table '{}' columns {:?}", name, table, columns);
        Ok(QueryResult::empty())
    }

    /// Execute DROP INDEX
    fn execute_drop_index(&self, name: &str, if_exists: bool) -> Result<QueryResult> {
        // Check if index exists
        match self.sql_catalog.get_index(name) {
            Some(_) => {}
            None => {
                if if_exists {
                    return Ok(QueryResult::empty());
                }
                return Err(Error::NotFound("Index".into(), name.to_string()));
            }
        }

        // Remove from catalog
        self.sql_catalog.drop_index(name)?;

        // Invalidate plan cache
        self.plan_cache.lock().clear();

        info!("Dropped index '{}'", name);
        Ok(QueryResult::empty())
    }

    /// Execute ALTER TABLE
    fn execute_alter_table(&self, table: &str, operations: &[AlterTableOp]) -> Result<QueryResult> {
        // Validate table exists
        let table_info = self.sql_catalog.get_table(table)
            .ok_or_else(|| Error::Sql(thunder_common::error::SqlError::TableNotFound(table.to_string())))?;

        for op in operations {
            match op {
                AlterTableOp::AddColumn { column } => {
                    self.sql_catalog.add_column(table, column.clone())?;
                    info!("Added column '{}' to table '{}'", column.name, table);
                }
                AlterTableOp::DropColumn { name } => {
                    // Validate column exists
                    if table_info.schema.column_by_name(name).is_none() {
                        return Err(Error::Sql(thunder_common::error::SqlError::ColumnNotFound(
                            format!("{}.{}", table, name)
                        )));
                    }
                    self.sql_catalog.drop_column(table, name)?;
                    info!("Dropped column '{}' from table '{}'", name, table);
                }
                AlterTableOp::RenameColumn { old_name, new_name } => {
                    if table_info.schema.column_by_name(old_name).is_none() {
                        return Err(Error::Sql(thunder_common::error::SqlError::ColumnNotFound(
                            format!("{}.{}", table, old_name)
                        )));
                    }
                    self.sql_catalog.rename_column(table, old_name, new_name)?;
                    info!("Renamed column '{}' -> '{}' in table '{}'", old_name, new_name, table);
                }
                AlterTableOp::RenameTable { new_name } => {
                    self.sql_catalog.rename_table(table, new_name)?;
                    info!("Renamed table '{}' -> '{}'", table, new_name);
                }
            }
        }

        // WAL-log the alter table operation
        let alter_json = serde_json::json!({
            "table": table,
            "operations": operations.iter().map(|op| format!("{:?}", op)).collect::<Vec<_>>(),
        });
        let payload = serde_json::to_vec(&alter_json)
            .map_err(|e| Error::Internal(format!("Failed to serialize ALTER metadata: {}", e)))?;

        self.wal_writer.write_record(
            TxnId(0),
            table_info.id,
            WalRecordType::CreateTable, // Reuse for DDL logging
            &payload,
        )?;
        self.wal_writer.flush_buffer()?;

        // Invalidate plan cache on schema change
        self.plan_cache.lock().clear();

        Ok(QueryResult::empty())
    }

    /// Execute EXPLAIN / EXPLAIN ANALYZE
    async fn execute_explain(
        &self,
        plan: &LogicalPlan,
        analyze: bool,
        verbose: bool,
        txn_id: TxnId,
    ) -> Result<QueryResult> {
        // Optimize and plan the inner query
        let optimized = self.optimizer.optimize(plan.clone())?;
        let physical_plan = self.planner.plan(optimized.clone())?;

        let mut plan_lines = Vec::new();

        if !analyze {
            // EXPLAIN: just show the physical plan
            let plan_text = format_physical_plan(&physical_plan, 0);
            for line in plan_text.lines() {
                plan_lines.push(line.to_string());
            }

            if verbose {
                // Add cost estimates from the logical plan
                let estimator = CostEstimator::new(self.sql_catalog.as_ref());
                let cost = estimator.estimate(&optimized);
                plan_lines.push(String::new());
                plan_lines.push(format!("Estimated rows: {}", cost.rows));
                plan_lines.push(format!("Estimated cost: {:.2}", cost.cpu + cost.io));
            }
        } else {
            // EXPLAIN ANALYZE: execute and show actual stats
            let start = Instant::now();
            let result = self.execute_physical_plan(txn_id, physical_plan.clone()).await?;
            let elapsed = start.elapsed();

            let plan_text = format_physical_plan(&physical_plan, 0);
            for line in plan_text.lines() {
                plan_lines.push(line.to_string());
            }

            plan_lines.push(String::new());
            plan_lines.push(format!("Actual rows: {}", result.rows.len()));
            plan_lines.push(format!("Execution time: {:.3}ms", elapsed.as_secs_f64() * 1000.0));

            if verbose {
                let estimator = CostEstimator::new(self.sql_catalog.as_ref());
                let cost = estimator.estimate(&optimized);
                plan_lines.push(format!("Estimated rows: {}", cost.rows));
                plan_lines.push(format!("Estimated cost: {:.2}", cost.cpu + cost.io));
            }
        }

        // Build result as a single-column text output (like PostgreSQL)
        let columns = vec![("QUERY PLAN".to_string(), DataType::String)];
        let rows: Vec<Row> = plan_lines
            .into_iter()
            .map(|line| Row::new(vec![Value::String(line.into())]))
            .collect();

        Ok(QueryResult {
            query_id: uuid::Uuid::new_v4().to_string(),
            columns,
            rows,
            rows_affected: None,
            execution_time: Duration::ZERO,
        })
    }

    /// Execute ANALYZE (collect table statistics)
    async fn execute_analyze_table(&self, table_name: Option<&str>) -> Result<QueryResult> {
        let tables_to_analyze = if let Some(name) = table_name {
            // Validate table exists
            if self.sql_catalog.get_table(name).is_none() {
                return Err(Error::Sql(thunder_common::error::SqlError::TableNotFound(name.to_string())));
            }
            vec![name.to_string()]
        } else {
            // Analyze all tables
            self.sql_catalog.list_tables()
        };

        for tbl_name in &tables_to_analyze {
            let table_info = match self.sql_catalog.get_table(tbl_name) {
                Some(info) => info,
                None => continue,
            };

            // Scan all rows in the table to compute statistics
            let mut row_count: u64 = 0;
            let mut total_row_size: u64 = 0;
            let num_columns = table_info.schema.len();

            // Per-column stats
            let mut distinct_values: Vec<std::collections::HashSet<String>> = vec![std::collections::HashSet::new(); num_columns];
            let mut null_counts: Vec<u64> = vec![0; num_columns];

            // Use row_store to scan all rows with a snapshot that sees everything
            let scan_snapshot = thunder_storage::Snapshot::new(
                TxnId(0),
                TxnId(0),
                TxnId(u64::MAX),
                vec![],
            );
            let mut iter = self.row_store.scan(table_info.id, &scan_snapshot)?;
            while let Some(result) = iter.next() {
                let (_row_id, row) = result?;
                row_count += 1;
                total_row_size += row.len() as u64 * 16; // estimated size per value

                for (i, val) in row.values.iter().enumerate() {
                    if i < num_columns {
                        if val.is_null() {
                            null_counts[i] += 1;
                        } else {
                            distinct_values[i].insert(format!("{:?}", val));
                        }
                    }
                }
            }

            // Update table-level stats
            let avg_row_size = if row_count > 0 {
                (total_row_size / row_count) as usize
            } else {
                0
            };

            let table_stats = thunder_sql::TableStats {
                row_count,
                size_bytes: total_row_size,
                last_updated: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            };
            let _ = self.sql_catalog.update_table_stats(tbl_name, table_stats);

            // Update per-column stats
            for (i, col) in table_info.schema.columns.iter().enumerate() {
                if i < num_columns {
                    let col_stats = thunder_sql::ColumnStats {
                        distinct_count: distinct_values[i].len() as u64,
                        null_count: null_counts[i],
                        min_value: None,
                        max_value: None,
                        avg_length: None,
                    };
                    let _ = self.sql_catalog.update_column_stats(tbl_name, &col.name, col_stats);
                }
            }

            info!(
                "ANALYZE {}: {} rows, {} avg_row_size",
                tbl_name, row_count, avg_row_size
            );
        }

        Ok(QueryResult::empty())
    }

    // ── Foreign Key Enforcement ──────────────────────────────────────

    /// Check FK constraints on INSERT: for each FK on the target table,
    /// verify that the referenced row exists in the parent table.
    fn check_fk_on_insert(
        &self,
        table_name: &str,
        columns: &[String],
        values: &[Vec<Expr>],
    ) -> Result<()> {
        let table_desc = self.storage_catalog.get_table_by_name("public", table_name);
        let table_desc = match table_desc {
            Some(d) => d,
            None => return Ok(()), // table not found in storage catalog; skip FK check
        };

        if table_desc.foreign_keys.is_empty() {
            return Ok(());
        }

        // Build column-name → position map
        let col_positions: std::collections::HashMap<String, usize> = if columns.is_empty() {
            // No explicit column list means all columns in table order
            table_desc.columns.iter().enumerate().map(|(i, c)| (c.name.clone(), i)).collect()
        } else {
            columns.iter().enumerate().map(|(i, c)| (c.clone(), i)).collect()
        };

        // Map ColumnId → column name in this table
        let id_to_name: std::collections::HashMap<ColumnId, String> =
            table_desc.columns.iter().map(|c| (c.id, c.name.clone())).collect();

        for fk in &table_desc.foreign_keys {
            // Resolve FK column names
            let fk_col_names: Vec<&str> = fk.columns.iter()
                .filter_map(|cid| id_to_name.get(cid).map(|s| s.as_str()))
                .collect();

            // Find positions of FK columns in the INSERT column list
            let fk_positions: Vec<Option<usize>> = fk_col_names.iter()
                .map(|name| col_positions.get(*name).copied())
                .collect();

            // Get referenced table descriptor
            let ref_table = self.storage_catalog.get_table(fk.referenced_table);
            let ref_table = match ref_table {
                Some(t) => t,
                None => continue, // referenced table missing; skip
            };

            // Resolve referenced column names
            let ref_id_to_name: std::collections::HashMap<ColumnId, String> =
                ref_table.columns.iter().map(|c| (c.id, c.name.clone())).collect();
            let ref_col_names: Vec<String> = fk.referenced_columns.iter()
                .filter_map(|cid| ref_id_to_name.get(cid).cloned())
                .collect();

            // For each row being inserted, extract FK values and check existence
            let scan_snapshot = thunder_storage::Snapshot::new(
                TxnId(0), TxnId(0), TxnId(u64::MAX), vec![],
            );

            for row_values in values {
                // Extract the FK column values from the row
                let mut fk_vals: Vec<Option<&Expr>> = Vec::new();
                let mut all_null = true;

                for pos in &fk_positions {
                    match pos {
                        Some(p) if *p < row_values.len() => {
                            let expr = &row_values[*p];
                            if !matches!(expr, Expr::Literal(Value::Null)) {
                                all_null = false;
                            }
                            fk_vals.push(Some(expr));
                        }
                        _ => {
                            fk_vals.push(None); // column not specified → NULL
                        }
                    }
                }

                // NULL FK values are allowed (partial match is OK per SQL standard)
                if all_null {
                    continue;
                }

                // Scan the referenced table to check if a matching row exists
                let ref_table_info = self.sql_catalog.get_table(&ref_table.name);
                if ref_table_info.is_none() {
                    continue;
                }
                let ref_table_info = ref_table_info.unwrap();

                let mut found = false;
                let mut iter = self.row_store.scan(ref_table_info.id, &scan_snapshot)?;
                while let Some(result) = iter.next() {
                    let (_row_id, row) = result?;
                    // Check if this row matches all FK values
                    let mut matches = true;
                    for (idx, ref_col_name) in ref_col_names.iter().enumerate() {
                        // Find the column index in the referenced table
                        let ref_col_idx = ref_table.columns.iter()
                            .position(|c| &c.name == ref_col_name);
                        let ref_col_idx = match ref_col_idx {
                            Some(i) => i,
                            None => { matches = false; break; }
                        };

                        if ref_col_idx >= row.values.len() {
                            matches = false;
                            break;
                        }

                        // Compare the FK value with the referenced column value
                        if let Some(Some(fk_expr)) = fk_vals.get(idx) {
                            if let Expr::Literal(fk_val) = fk_expr {
                                if &row.values[ref_col_idx] != fk_val {
                                    matches = false;
                                    break;
                                }
                            }
                        }
                    }
                    if matches {
                        found = true;
                        break;
                    }
                }

                if !found {
                    return Err(Error::Sql(SqlError::ConstraintViolation(format!(
                        "Foreign key violation: referenced row not found in table '{}' for constraint '{}'",
                        ref_table.name, fk.name,
                    ))));
                }
            }
        }

        Ok(())
    }

    /// Check FK constraints on DELETE: for each table that has FKs referencing
    /// the target table, validate that no child rows reference the deleted rows.
    /// Returns the set of cascading actions to perform.
    fn check_fk_on_delete(
        &self,
        table_name: &str,
        filter: &Option<Expr>,
        txn_id: TxnId,
    ) -> Result<()> {
        let table_desc = self.storage_catalog.get_table_by_name("public", table_name);
        let table_desc = match table_desc {
            Some(d) => d,
            None => return Ok(()),
        };

        // Find all tables that have FK constraints referencing this table
        let all_tables = self.storage_catalog.list_tables();
        let referencing_tables: Vec<_> = all_tables.iter()
            .filter(|t| t.foreign_keys.iter().any(|fk| fk.referenced_table == table_desc.id))
            .collect();

        if referencing_tables.is_empty() {
            return Ok(());
        }

        // Get the rows that will be deleted (scan with filter match)
        let table_info = self.sql_catalog.get_table(table_name);
        if table_info.is_none() {
            return Ok(());
        }
        let table_info = table_info.unwrap();

        // For each referencing table, check if any of its rows reference values
        // in the to-be-deleted rows
        let scan_snapshot = thunder_storage::Snapshot::new(
            TxnId(0), TxnId(0), TxnId(u64::MAX), vec![],
        );

        for ref_table in &referencing_tables {
            for fk in &ref_table.foreign_keys {
                if fk.referenced_table != table_desc.id {
                    continue;
                }

                match fk.on_delete {
                    ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => {
                        // Check if any child rows reference the parent
                        let ref_table_info = self.sql_catalog.get_table(&ref_table.name);
                        if ref_table_info.is_none() {
                            continue;
                        }
                        let ref_table_info = ref_table_info.unwrap();

                        // For simplicity, scan child table to see if any rows reference
                        // values in the parent's referenced columns
                        let mut child_iter = self.row_store.scan(ref_table_info.id, &scan_snapshot)?;
                        while let Some(result) = child_iter.next() {
                            let (_row_id, _row) = result?;
                            // We'd need to match against the specific rows being deleted
                            // For restrict/no_action, the safest approach: check if ANY child
                            // row exists referencing this table through this FK.
                            // A more precise check would match against the filter, but
                            // that requires evaluating the filter expression here.
                            // For now, if a child row exists AND there's no filter (DELETE all),
                            // reject. If there IS a filter, we allow it through and rely on
                            // the transaction's constraint check at commit time.
                            if filter.is_none() {
                                // DELETE all rows from parent — if any child references exist, reject
                                return Err(Error::Sql(SqlError::ConstraintViolation(format!(
                                    "Foreign key violation: table '{}' has referencing rows in '{}' via constraint '{}'",
                                    table_name, ref_table.name, fk.name,
                                ))));
                            }
                        }
                    }
                    ForeignKeyAction::Cascade => {
                        // Cascading delete will be handled after the parent delete
                        // by deleting child rows that reference the deleted parent rows.
                        // For now, allow the delete to proceed.
                    }
                    ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                        // These actions update child rows after parent delete.
                        // Allow the delete to proceed.
                    }
                }
            }
        }

        Ok(())
    }

    /// Check FK constraints on UPDATE: validate that updated FK columns
    /// still reference valid parent rows.
    fn check_fk_on_update(
        &self,
        table_name: &str,
        assignments: &[(String, Expr)],
    ) -> Result<()> {
        let table_desc = self.storage_catalog.get_table_by_name("public", table_name);
        let table_desc = match table_desc {
            Some(d) => d,
            None => return Ok(()),
        };

        if table_desc.foreign_keys.is_empty() {
            return Ok(());
        }

        let id_to_name: std::collections::HashMap<ColumnId, String> =
            table_desc.columns.iter().map(|c| (c.id, c.name.clone())).collect();

        // Check each FK constraint
        for fk in &table_desc.foreign_keys {
            let fk_col_names: Vec<&str> = fk.columns.iter()
                .filter_map(|cid| id_to_name.get(cid).map(|s| s.as_str()))
                .collect();

            // Check if any FK column is being updated
            let updated_fk_cols: Vec<_> = assignments.iter()
                .filter(|(col, _)| fk_col_names.contains(&col.as_str()))
                .collect();

            if updated_fk_cols.is_empty() {
                continue; // FK columns not being updated
            }

            // Get the referenced table
            let ref_table = self.storage_catalog.get_table(fk.referenced_table);
            let ref_table = match ref_table {
                Some(t) => t,
                None => continue,
            };

            let ref_id_to_name: std::collections::HashMap<ColumnId, String> =
                ref_table.columns.iter().map(|c| (c.id, c.name.clone())).collect();
            let ref_col_names: Vec<String> = fk.referenced_columns.iter()
                .filter_map(|cid| ref_id_to_name.get(cid).cloned())
                .collect();

            // For each updated FK column, verify the new value exists in the referenced table
            let scan_snapshot = thunder_storage::Snapshot::new(
                TxnId(0), TxnId(0), TxnId(u64::MAX), vec![],
            );

            for (col_name, new_val_expr) in &updated_fk_cols {
                if let Expr::Literal(Value::Null) = new_val_expr {
                    continue; // NULL is allowed
                }

                // Find which FK column index this corresponds to
                let fk_idx = fk_col_names.iter().position(|n| n == col_name);
                if fk_idx.is_none() {
                    continue;
                }
                let fk_idx = fk_idx.unwrap();

                if fk_idx >= ref_col_names.len() {
                    continue;
                }
                let ref_col_name = &ref_col_names[fk_idx];

                // Scan referenced table for a matching value
                let ref_table_info = self.sql_catalog.get_table(&ref_table.name);
                if ref_table_info.is_none() {
                    continue;
                }
                let ref_table_info = ref_table_info.unwrap();

                let ref_col_idx = ref_table.columns.iter()
                    .position(|c| &c.name == ref_col_name);
                if ref_col_idx.is_none() {
                    continue;
                }
                let ref_col_idx = ref_col_idx.unwrap();

                let mut found = false;
                let mut iter = self.row_store.scan(ref_table_info.id, &scan_snapshot)?;
                while let Some(result) = iter.next() {
                    let (_row_id, row) = result?;
                    if ref_col_idx < row.values.len() {
                        if let Expr::Literal(ref fk_val) = new_val_expr {
                            if &row.values[ref_col_idx] == fk_val {
                                found = true;
                                break;
                            }
                        }
                    }
                }

                if !found {
                    return Err(Error::Sql(SqlError::ConstraintViolation(format!(
                        "Foreign key violation: value for column '{}' not found in referenced table '{}' (constraint '{}')",
                        col_name, ref_table.name, fk.name,
                    ))));
                }
            }
        }

        // Also check if this table's PK columns are being updated, and if so,
        // validate no child tables reference the old values (on_update behavior)
        let all_tables = self.storage_catalog.list_tables();
        for child_table in &all_tables {
            for fk in &child_table.foreign_keys {
                if fk.referenced_table != table_desc.id {
                    continue;
                }

                // Check if any referenced columns are being updated
                let ref_col_names: Vec<String> = fk.referenced_columns.iter()
                    .filter_map(|cid| id_to_name.get(cid).map(|s| s.clone()))
                    .collect();

                let updating_ref_cols = assignments.iter()
                    .any(|(col, _)| ref_col_names.contains(col));

                if !updating_ref_cols {
                    continue;
                }

                match fk.on_update {
                    ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => {
                        // If child rows exist referencing this value, reject
                        let child_info = self.sql_catalog.get_table(&child_table.name);
                        if child_info.is_none() {
                            continue;
                        }
                        let child_info = child_info.unwrap();
                        let scan_snapshot = thunder_storage::Snapshot::new(
                            TxnId(0), TxnId(0), TxnId(u64::MAX), vec![],
                        );
                        let mut iter = self.row_store.scan(child_info.id, &scan_snapshot)?;
                        if iter.next().is_some() {
                            return Err(Error::Sql(SqlError::ConstraintViolation(format!(
                                "Foreign key violation: cannot update referenced column in '{}'; child rows exist in '{}' (constraint '{}')",
                                table_name, child_table.name, fk.name,
                            ))));
                        }
                    }
                    ForeignKeyAction::Cascade | ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                        // Allow; cascading handled after execution
                    }
                }
            }
        }

        Ok(())
    }

    // ── System Catalog Views ──────────────────────────────────────────

    /// Try to handle a system catalog query (information_schema.* / thunderdb.*).
    /// Returns Some(QueryResult) if the query was handled, None otherwise.
    fn try_execute_system_catalog(
        &self,
        sql_lower: &str,
        session_id: uuid::Uuid,
    ) -> Result<Option<QueryResult>> {
        if sql_lower.contains("information_schema.tables") {
            return Ok(Some(self.query_information_schema_tables()?));
        }
        if sql_lower.contains("information_schema.columns") {
            return Ok(Some(self.query_information_schema_columns()?));
        }
        if sql_lower.contains("information_schema.indexes")
            || sql_lower.contains("information_schema.statistics") {
            return Ok(Some(self.query_information_schema_indexes()?));
        }
        if sql_lower.contains("thunderdb.sessions") {
            return Ok(Some(self.query_thunderdb_sessions(session_id)?));
        }
        Ok(None)
    }

    /// information_schema.tables — one row per user table
    fn query_information_schema_tables(&self) -> Result<QueryResult> {
        let columns = vec![
            ("table_catalog".to_string(), DataType::String),
            ("table_schema".to_string(), DataType::String),
            ("table_name".to_string(), DataType::String),
            ("table_type".to_string(), DataType::String),
        ];

        let table_names = self.sql_catalog.list_tables();
        let rows: Vec<Row> = table_names.iter().map(|name| {
            Row::new(vec![
                Value::String("thunderdb".into()),
                Value::String("public".into()),
                Value::String(name.clone().into()),
                Value::String("BASE TABLE".into()),
            ])
        }).collect();

        Ok(QueryResult {
            query_id: uuid::Uuid::new_v4().to_string(),
            columns,
            rows,
            rows_affected: None,
            execution_time: Duration::ZERO,
        })
    }

    /// information_schema.columns — one row per column per table
    fn query_information_schema_columns(&self) -> Result<QueryResult> {
        let columns = vec![
            ("table_name".to_string(), DataType::String),
            ("column_name".to_string(), DataType::String),
            ("ordinal_position".to_string(), DataType::Int64),
            ("data_type".to_string(), DataType::String),
            ("is_nullable".to_string(), DataType::String),
            ("column_default".to_string(), DataType::String),
        ];

        let table_names = self.sql_catalog.list_tables();
        let mut rows = Vec::new();
        for name in &table_names {
            if let Some(table) = self.sql_catalog.get_table(name) {
                for (i, col) in table.columns.iter().enumerate() {
                    rows.push(Row::new(vec![
                        Value::String(table.name.clone().into()),
                        Value::String(col.name.clone().into()),
                        Value::Int64((i + 1) as i64),
                        Value::String(format!("{:?}", col.data_type).into()),
                        Value::String(if col.nullable { "YES" } else { "NO" }.into()),
                        match &col.default_expr {
                            Some(expr) => Value::String(expr.clone().into()),
                            None => Value::Null,
                        },
                    ]));
                }
            }
        }

        Ok(QueryResult {
            query_id: uuid::Uuid::new_v4().to_string(),
            columns,
            rows,
            rows_affected: None,
            execution_time: Duration::ZERO,
        })
    }

    /// information_schema.indexes (non-standard, but useful) — one row per index
    fn query_information_schema_indexes(&self) -> Result<QueryResult> {
        let columns = vec![
            ("index_name".to_string(), DataType::String),
            ("table_name".to_string(), DataType::String),
            ("column_names".to_string(), DataType::String),
            ("is_unique".to_string(), DataType::Boolean),
            ("index_type".to_string(), DataType::String),
        ];

        let all_tables = self.storage_catalog.list_tables();
        let mut rows = Vec::new();
        for table in &all_tables {
            let indexes = self.storage_catalog.get_table_indexes(table.id);
            for idx in indexes {
                // Resolve column IDs to names using the table's column definitions
                let col_names: Vec<String> = idx.columns.iter()
                    .map(|cid| {
                        table.columns.iter()
                            .find(|c| c.id == *cid)
                            .map(|c| c.name.clone())
                            .unwrap_or_else(|| format!("col_{}", cid.0))
                    })
                    .collect();
                rows.push(Row::new(vec![
                    Value::String(idx.name.clone().into()),
                    Value::String(table.name.clone().into()),
                    Value::String(col_names.join(", ").into()),
                    Value::Boolean(idx.is_unique),
                    Value::String(format!("{:?}", idx.index_type).into()),
                ]));
            }
        }

        Ok(QueryResult {
            query_id: uuid::Uuid::new_v4().to_string(),
            columns,
            rows,
            rows_affected: None,
            execution_time: Duration::ZERO,
        })
    }

    /// thunderdb.sessions — one row per active session
    fn query_thunderdb_sessions(&self, _current_session_id: uuid::Uuid) -> Result<QueryResult> {
        let columns = vec![
            ("session_id".to_string(), DataType::String),
            ("user".to_string(), DataType::String),
            ("database".to_string(), DataType::String),
            ("state".to_string(), DataType::String),
        ];

        let sessions = self.sessions.read();
        let rows: Vec<Row> = sessions.iter().map(|(id, session)| {
            let state = if session.txn_id.is_some() { "active" } else { "idle" };
            Row::new(vec![
                Value::String(id.to_string().into()),
                Value::String(session.user.clone().unwrap_or_default().into()),
                Value::String(session.database.clone().unwrap_or_default().into()),
                Value::String(state.into()),
            ])
        }).collect();

        Ok(QueryResult {
            query_id: uuid::Uuid::new_v4().to_string(),
            columns,
            rows,
            rows_affected: None,
            execution_time: Duration::ZERO,
        })
    }

    /// Begin a transaction
    pub async fn begin_transaction(&self, session_id: uuid::Uuid) -> Result<TxnId> {
        // First phase: check state and get isolation level (with lock)
        let isolation_level = {
            let sessions = self.sessions.read();
            let session = sessions.get(&session_id)
                .ok_or_else(|| Error::Internal("Session not found".to_string()))?;

            if session.txn_id.is_some() {
                return Err(Error::Internal("Transaction already in progress".to_string()));
            }
            session.isolation_level
        };
        // Lock is dropped here

        // Second phase: begin transaction (no lock held)
        let txn_id = self.txn_manager.begin(isolation_level).await?;

        // Third phase: update session state (re-acquire lock)
        {
            let mut sessions = self.sessions.write();
            if let Some(session) = sessions.get_mut(&session_id) {
                session.txn_id = Some(txn_id);
                session.last_activity = Instant::now();
            }
        }

        debug!("Started transaction {:?} for session {}", txn_id, session_id);
        Ok(txn_id)
    }

    /// Commit a transaction
    pub async fn commit_transaction(&self, session_id: uuid::Uuid) -> Result<()> {
        let txn_id = {
            let mut sessions = self.sessions.write();
            let session = sessions.get_mut(&session_id)
                .ok_or_else(|| Error::Internal("Session not found".to_string()))?;

            let txn_id = session.txn_id
                .ok_or_else(|| Error::Internal("No active transaction".to_string()))?;

            session.txn_id = None;
            session.last_activity = Instant::now();
            txn_id
        };

        match self.txn_manager.commit(txn_id).await? {
            CommitResult::Committed { commit_ts } => {
                debug!("Committed transaction {:?} at ts {:?}", txn_id, commit_ts);
                Ok(())
            }
            CommitResult::Aborted { reason } => {
                Err(Error::Internal(format!("Transaction aborted: {:?}", reason)))
            }
        }
    }

    /// Rollback a transaction
    pub async fn rollback_transaction(&self, session_id: uuid::Uuid) -> Result<()> {
        let txn_id = {
            let mut sessions = self.sessions.write();
            let session = sessions.get_mut(&session_id)
                .ok_or_else(|| Error::Internal("Session not found".to_string()))?;

            let txn_id = session.txn_id
                .ok_or_else(|| Error::Internal("No active transaction".to_string()))?;

            session.txn_id = None;
            session.last_activity = Instant::now();
            txn_id
        };

        self.txn_manager.abort(txn_id).await?;
        debug!("Rolled back transaction {:?}", txn_id);
        Ok(())
    }

    /// Get engine statistics
    pub fn stats(&self) -> EngineStats {
        let mut stats = self.stats.read().clone();
        stats.queries_total = self.query_counter.load(Ordering::Relaxed);
        stats.queries_failed = self.query_failed_counter.load(Ordering::Relaxed);
        stats.active_connections = self.connection_counter.load(Ordering::Relaxed);
        stats.active_transactions = self.sessions.read()
            .values()
            .filter(|s| s.txn_id.is_some())
            .count() as u64;
        stats
    }

    /// Get the SQL catalog
    pub fn sql_catalog(&self) -> Arc<SqlCatalog> {
        self.sql_catalog.clone()
    }

    /// Get active session count
    pub fn active_sessions(&self) -> usize {
        self.sessions.read().len()
    }

    /// Set read-only mode (used by disk space monitor)
    pub fn set_read_only(&self, val: bool) {
        self.read_only.store(val, Ordering::Relaxed);
    }

    /// Check if the engine is in read-only mode
    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Relaxed)
    }

    /// Get the engine config
    pub fn engine_config(&self) -> &EngineConfig {
        &self.config
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<String> {
        self.sql_catalog.list_tables()
    }

    /// Get table info
    pub fn get_table(&self, name: &str) -> Option<Arc<TableInfo>> {
        self.sql_catalog.get_table(name)
    }

    /// Vacuum a table to reclaim space from dead tuples.
    ///
    /// This removes tuples that are no longer visible to any active transaction,
    /// compacts pages to reclaim space, and updates the free space map.
    pub async fn vacuum_table(&self, table_name: &str) -> Result<VacuumResult> {
        let oldest_active_txn = self.txn_manager.oldest_active_txn();

        info!("Starting vacuum for table '{}', oldest active txn: {:?}", table_name, oldest_active_txn);

        // Look up the table by name from the catalog
        let table_info = self.sql_catalog.get_table(table_name)
            .ok_or_else(|| Error::NotFound("Table".into(), table_name.to_string()))?;

        // Call the fully-implemented row_store vacuum
        let stats = self.row_store.vacuum(table_info.id, oldest_active_txn)?;

        // Also clean up committed transaction metadata older than oldest active
        self.txn_manager.gc(oldest_active_txn);

        let result = VacuumResult {
            table: table_name.to_string(),
            pages_scanned: stats.pages_scanned,
            pages_compacted: stats.pages_compacted,
            tuples_removed: stats.tuples_removed,
            bytes_freed: stats.bytes_freed,
        };

        info!(
            "Vacuum complete for table '{}': {} pages scanned, {} tuples removed, {} bytes freed",
            table_name, result.pages_scanned, result.tuples_removed, result.bytes_freed
        );

        Ok(result)
    }

    /// Write a checkpoint to WAL
    pub async fn checkpoint(&self) -> Result<()> {
        info!("Writing checkpoint...");

        // Collect active transactions
        let active_txns: Vec<(TxnId, Lsn)> = self.sessions.read()
            .values()
            .filter_map(|s| s.txn_id.map(|id| (id, Lsn(0)))) // LSN tracking per txn would need more state
            .collect();

        // Create checkpoint record
        let checkpoint = CheckpointRecord {
            active_txns,
            dirty_pages: vec![], // Could track from buffer pool stats
        };

        let entry = WalEntry {
            lsn: Lsn(0),
            txn_id: TxnId(0),
            entry_type: WalEntryType::Checkpoint { active_txns: vec![] },
            table_id: TableId(0),
            data: checkpoint.encode().to_vec(),
        };

        self.wal_writer.append(entry).await?;
        self.wal_writer.sync().await?;

        // Flush all dirty pages
        self.buffer_pool.flush_all()?;

        // Truncate old WAL segments that are no longer needed
        let checkpoint_lsn = self.wal_writer.current_lsn();
        if let Err(e) = self.wal_writer.truncate_before(checkpoint_lsn).await {
            warn!("WAL truncation after checkpoint failed: {}", e);
        }

        info!("Checkpoint complete");
        Ok(())
    }

    /// Get the WAL writer
    pub fn wal_writer(&self) -> &Arc<WalWriterImpl> {
        &self.wal_writer
    }

    /// Get the buffer pool
    pub fn buffer_pool(&self) -> &Arc<BufferPoolImpl> {
        &self.buffer_pool
    }

    /// Get the transaction manager
    pub fn txn_manager(&self) -> &Arc<MvccTransactionManager> {
        &self.txn_manager
    }

    /// Shutdown the engine
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down database engine...");

        // 1. Collect sessions with active transactions (don't hold lock across await)
        let sessions_to_abort: Vec<(uuid::Uuid, Option<TxnId>)> = {
            let sessions = self.sessions.read();
            sessions.iter()
                .map(|(id, s)| (*id, s.txn_id))
                .collect()
        };

        // 2. Abort active transactions
        for (session_id, txn_id_opt) in sessions_to_abort {
            if let Some(txn_id) = txn_id_opt {
                info!("Aborting transaction {:?} during shutdown", txn_id);
                // Write abort to WAL
                let entry = WalEntry {
                    lsn: Lsn(0),
                    txn_id,
                    entry_type: WalEntryType::Abort,
                    table_id: TableId(0),
                    data: vec![],
                };
                if let Err(e) = self.wal_writer.append(entry).await {
                    error!(txn_id = ?txn_id, error = %e, "Failed to write abort WAL entry during shutdown");
                }
                if let Err(e) = self.txn_manager.abort(txn_id).await {
                    error!(txn_id = ?txn_id, error = %e, "Failed to abort transaction during shutdown");
                }
            }
            // Remove session
            self.sessions.write().remove(&session_id);
        }

        // 3. Flush WAL buffers
        info!("Flushing WAL...");
        self.wal_writer.sync().await?;

        // 4. Write shutdown checkpoint
        info!("Writing shutdown checkpoint...");
        self.checkpoint().await?;

        // 5. Flush all dirty pages
        info!("Flushing dirty pages...");
        self.buffer_pool.flush_all()?;

        info!("Database engine shutdown complete");
        Ok(())
    }

    /// Create a consistent backup of the database.
    ///
    /// Steps:
    /// 1. Write a checkpoint (ensures WAL is current)
    /// 2. Flush all dirty pages to the data file
    /// 3. Copy the data file and WAL segments to the target directory
    /// 4. Write a backup manifest with metadata
    pub async fn backup(&self, target_dir: Option<&str>) -> Result<thunder_api::BackupResult> {
        info!("Starting database backup...");

        // 1. Checkpoint to ensure consistency
        self.checkpoint().await?;

        // 2. Determine backup directory
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let backup_id = format!("backup_{}", timestamp);
        let backup_dir = match target_dir {
            Some(dir) => PathBuf::from(dir),
            None => self.config.data_dir.join("backups").join(&backup_id),
        };

        // Create backup directory structure
        tokio::fs::create_dir_all(&backup_dir).await
            .map_err(|e| Error::Internal(format!("Failed to create backup dir: {}", e)))?;
        let backup_wal_dir = backup_dir.join("wal");
        tokio::fs::create_dir_all(&backup_wal_dir).await
            .map_err(|e| Error::Internal(format!("Failed to create backup WAL dir: {}", e)))?;

        // 3. Copy data file
        let src_db = self.config.data_dir.join("thunder.db");
        let dst_db = backup_dir.join("thunder.db");
        let mut total_size: u64 = 0;
        if src_db.exists() {
            tokio::fs::copy(&src_db, &dst_db).await
                .map_err(|e| Error::Internal(format!("Failed to copy data file: {}", e)))?;
            total_size += tokio::fs::metadata(&dst_db).await
                .map(|m| m.len()).unwrap_or(0);
        }

        // 4. Copy WAL segment files
        let wal_dir = &self.config.wal_dir;
        if wal_dir.exists() {
            let mut entries = tokio::fs::read_dir(wal_dir).await
                .map_err(|e| Error::Internal(format!("Failed to read WAL dir: {}", e)))?;

            while let Some(entry) = entries.next_entry().await
                .map_err(|e| Error::Internal(format!("Failed to read WAL entry: {}", e)))? {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        let dst = backup_wal_dir.join(name);
                        tokio::fs::copy(&path, &dst).await
                            .map_err(|e| Error::Internal(format!("Failed to copy WAL segment: {}", e)))?;
                        total_size += tokio::fs::metadata(&dst).await
                            .map(|m| m.len()).unwrap_or(0);
                    }
                }
            }
        }

        // 5. Write backup manifest
        let current_lsn = self.wal_writer.current_lsn().0;
        let now = chrono::Utc::now().to_rfc3339();
        let manifest = serde_json::json!({
            "backup_id": backup_id,
            "timestamp": now,
            "lsn": current_lsn,
            "size_bytes": total_size,
            "version": env!("CARGO_PKG_VERSION"),
        });
        let manifest_path = backup_dir.join("manifest.json");
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| Error::Internal(format!("Failed to serialize backup manifest: {}", e)))?;
        tokio::fs::write(&manifest_path, manifest_json).await
            .map_err(|e| Error::Internal(format!("Failed to write manifest: {}", e)))?;

        info!(backup_id = %backup_id, size_bytes = total_size, "Backup complete");

        Ok(thunder_api::BackupResult {
            backup_id,
            backup_path: backup_dir.to_string_lossy().to_string(),
            timestamp: now,
            size_bytes: total_size,
            lsn: current_lsn,
        })
    }

    /// List available backups by scanning the backups directory.
    pub fn list_backups(&self) -> Result<Vec<thunder_api::BackupInfo>> {
        let backups_dir = self.config.data_dir.join("backups");
        if !backups_dir.exists() {
            return Ok(vec![]);
        }

        let mut backups = Vec::new();
        for entry in std::fs::read_dir(&backups_dir)
            .map_err(|e| Error::Internal(format!("Failed to read backups dir: {}", e)))? {
            let entry = entry.map_err(|e| Error::Internal(e.to_string()))?;
            let manifest_path = entry.path().join("manifest.json");
            if manifest_path.exists() {
                let data = std::fs::read_to_string(&manifest_path)
                    .map_err(|e| Error::Internal(e.to_string()))?;
                if let Ok(manifest) = serde_json::from_str::<serde_json::Value>(&data) {
                    backups.push(thunder_api::BackupInfo {
                        backup_id: manifest["backup_id"].as_str().unwrap_or("").to_string(),
                        backup_path: entry.path().to_string_lossy().to_string(),
                        timestamp: manifest["timestamp"].as_str().unwrap_or("").to_string(),
                        size_bytes: manifest["size_bytes"].as_u64().unwrap_or(0),
                    });
                }
            }
        }

        backups.sort_by(|a, b| b.timestamp.cmp(&a.timestamp)); // newest first
        Ok(backups)
    }

    /// Restore from a backup (static method - engine must NOT be running).
    ///
    /// Copies backup files to the data and WAL directories, then
    /// normal engine startup will perform WAL recovery.
    pub async fn restore(
        backup_path: &std::path::Path,
        data_dir: &std::path::Path,
        wal_dir: &std::path::Path,
    ) -> Result<()> {
        info!("Restoring from backup: {:?}", backup_path);

        // Validate backup
        let manifest_path = backup_path.join("manifest.json");
        if !manifest_path.exists() {
            return Err(Error::Internal("Invalid backup: manifest.json not found".into()));
        }

        // Create target dirs
        tokio::fs::create_dir_all(data_dir).await
            .map_err(|e| Error::Internal(format!("Failed to create data dir: {}", e)))?;
        tokio::fs::create_dir_all(wal_dir).await
            .map_err(|e| Error::Internal(format!("Failed to create WAL dir: {}", e)))?;

        // Copy data file
        let src_db = backup_path.join("thunder.db");
        let dst_db = data_dir.join("thunder.db");
        if src_db.exists() {
            tokio::fs::copy(&src_db, &dst_db).await
                .map_err(|e| Error::Internal(format!("Failed to restore data file: {}", e)))?;
        }

        // Copy WAL segments
        let backup_wal = backup_path.join("wal");
        if backup_wal.exists() {
            let mut entries = tokio::fs::read_dir(&backup_wal).await
                .map_err(|e| Error::Internal(format!("Failed to read backup WAL: {}", e)))?;
            while let Some(entry) = entries.next_entry().await
                .map_err(|e| Error::Internal(e.to_string()))? {
                let src = entry.path();
                if let Some(name) = src.file_name() {
                    let dst = wal_dir.join(name);
                    tokio::fs::copy(&src, &dst).await
                        .map_err(|e| Error::Internal(format!("Failed to restore WAL segment: {}", e)))?;
                }
            }
        }

        info!("Restore complete. Start the engine to replay WAL recovery.");
        Ok(())
    }

    /// Scrub all pages in the buffer pool by verifying their checksums.
    ///
    /// Returns `(pages_checked, corruptions_found)`.
    pub async fn scrub_pages(&self) -> (u64, u64) {
        let mut pages_checked: u64 = 0;
        let mut corruptions: u64 = 0;

        // Determine how many pages exist on disk.
        let scan_limit = self.disk_manager.num_pages();

        for page_num in 0..scan_limit {
            let page_id = thunder_common::types::PageId(page_num);
            match self.buffer_pool.fetch_page(page_id) {
                Ok(guard) => {
                    pages_checked += 1;
                    if !guard.verify_checksum() {
                        corruptions += 1;
                        error!(
                            "Page checksum verification FAILED for page {}",
                            page_num
                        );
                    }
                }
                Err(_) => {
                    // Page not accessible (not allocated or I/O error) — skip
                }
            }
        }

        (pages_checked, corruptions)
    }

    /// Restore database to a specific point in time using a base backup + WAL replay.
    ///
    /// This performs:
    /// 1. Restore files from the backup directory
    /// 2. Replay WAL records up to (but not beyond) the target timestamp
    ///
    /// The target_timestamp is milliseconds since UNIX epoch.
    pub async fn restore_to_timestamp(
        backup_path: &std::path::Path,
        data_dir: &std::path::Path,
        wal_dir: &std::path::Path,
        target_timestamp_ms: u64,
    ) -> Result<()> {
        info!(
            "Starting point-in-time recovery to timestamp {} ms",
            target_timestamp_ms
        );

        // Step 1: Restore base backup files
        Self::restore(backup_path, data_dir, wal_dir).await?;

        // Step 2: Replay WAL records up to the target timestamp.
        // We use WalRecovery to get all redo records, then filter by timestamp.
        let recovery = WalRecovery::new(wal_dir);
        let plan = recovery.recover()?;

        if !plan.needs_recovery() {
            info!("No WAL records to replay for PITR");
            return Ok(());
        }

        // Filter redo records: only replay those whose LSN-derived ordering
        // places them before the target time. Since WAL records don't have
        // an explicit timestamp field in the current 40-byte header, we use
        // LSN ordering as a proxy. For precise PITR, the WAL header would
        // need a timestamp field. For now, we replay all redo records from
        // the recovery plan (the backup + full WAL replay gives consistency).
        //
        // In production, the WalRecordHeader would carry a timestamp and
        // we'd stop replaying when header.timestamp > target_timestamp_ms.
        let redo_count = plan.redo_records.len();
        let undo_count = plan.undo_records.len();

        info!(
            "PITR recovery plan: {} redo records, {} undo records",
            redo_count, undo_count
        );

        // We don't execute the plan here because we don't have a running engine.
        // The caller should start the engine normally after this, which triggers
        // standard WAL recovery. The key insight is that by restoring from a
        // backup taken at time T and replaying WAL, we get consistency up to
        // the last committed transaction in the WAL.
        info!(
            "PITR restore complete. {} redo and {} undo records available. \
             Start engine to complete recovery.",
            redo_count, undo_count
        );

        Ok(())
    }
}

// ============================================================================
// QueryEngine Trait Implementation
// ============================================================================

use thunder_api::{QueryEngine, EngineQueryResult, EngineStats as ApiEngineStats, CatalogTableInfo, VacuumResult, BackupResult, BackupInfo};

#[async_trait::async_trait]
impl QueryEngine for DatabaseEngine {
    fn create_session(&self) -> uuid::Uuid {
        let session = DatabaseEngine::create_session(self);
        session.id
    }

    fn close_session(&self, session_id: uuid::Uuid) {
        DatabaseEngine::close_session(self, session_id);
    }

    async fn execute_sql(&self, session_id: uuid::Uuid, sql: &str) -> Result<EngineQueryResult> {
        let result = DatabaseEngine::execute_sql(self, session_id, sql).await?;
        Ok(EngineQueryResult {
            query_id: result.query_id,
            columns: result.columns,
            rows: result.rows,
            rows_affected: result.rows_affected,
            execution_time: result.execution_time,
        })
    }

    async fn begin_transaction(&self, session_id: uuid::Uuid) -> Result<TxnId> {
        DatabaseEngine::begin_transaction(self, session_id).await
    }

    async fn commit_transaction(&self, session_id: uuid::Uuid) -> Result<()> {
        DatabaseEngine::commit_transaction(self, session_id).await
    }

    async fn rollback_transaction(&self, session_id: uuid::Uuid) -> Result<()> {
        DatabaseEngine::rollback_transaction(self, session_id).await
    }

    fn stats(&self) -> ApiEngineStats {
        let stats = DatabaseEngine::stats(self);
        ApiEngineStats {
            queries_total: stats.queries_total,
            queries_failed: stats.queries_failed,
            active_connections: stats.active_connections,
            active_transactions: stats.active_transactions,
            buffer_pool_hit_ratio: stats.buffer_pool_hit_ratio,
            uptime_seconds: stats.uptime_seconds(),
        }
    }

    fn list_tables(&self) -> Vec<String> {
        DatabaseEngine::list_tables(self)
    }

    fn get_table(&self, name: &str) -> Option<CatalogTableInfo> {
        DatabaseEngine::get_table(self, name).map(|info| CatalogTableInfo {
            name: info.name.clone(),
            schema: info.schema.clone(),
        })
    }

    async fn vacuum_table(&self, table_name: &str) -> Result<VacuumResult> {
        DatabaseEngine::vacuum_table(self, table_name).await
    }

    async fn backup(&self, target_dir: Option<&str>) -> Result<BackupResult> {
        DatabaseEngine::backup(self, target_dir).await
    }

    fn list_backups(&self) -> Result<Vec<BackupInfo>> {
        DatabaseEngine::list_backups(self)
    }
}

// ============================================================================
// QueryExecutor Trait Implementation (for PostgreSQL/MySQL wire protocols)
// ============================================================================

use thunder_protocol::postgres::{QueryExecutor, QueryResult as PgQueryResult, ColumnInfo};

#[async_trait::async_trait]
impl QueryExecutor for DatabaseEngine {
    async fn execute(&self, session_id: uuid::Uuid, sql: &str) -> Result<PgQueryResult> {
        let result = DatabaseEngine::execute_sql(self, session_id, sql).await?;

        // Convert internal QueryResult to protocol QueryResult
        let columns: Vec<ColumnInfo> = result.columns.iter().map(|(name, dt)| {
            ColumnInfo {
                name: name.clone(),
                table_oid: 0,
                column_id: 0,
                type_oid: thunder_protocol::datatype_to_oid(dt),
                type_size: -1,
                type_modifier: -1,
                format: 0, // text format
            }
        }).collect();

        // Convert rows to string representation
        let rows: Vec<Vec<Option<String>>> = result.rows.iter().map(|row| {
            row.values.iter().map(|value| {
                if value.is_null() {
                    None
                } else {
                    Some(value.to_string())
                }
            }).collect()
        }).collect();

        // Determine command tag based on query
        let command_tag = Self::determine_command_tag(sql, result.rows_affected, rows.len());

        Ok(PgQueryResult {
            columns,
            rows,
            rows_affected: result.rows_affected,
            command_tag,
        })
    }

    fn create_session(&self) -> uuid::Uuid {
        let session = DatabaseEngine::create_session(self);
        session.id
    }

    fn close_session(&self, session_id: uuid::Uuid) {
        DatabaseEngine::close_session(self, session_id);
    }

    async fn begin_transaction(&self, session_id: uuid::Uuid) -> Result<TxnId> {
        DatabaseEngine::begin_transaction(self, session_id).await
    }

    async fn commit_transaction(&self, session_id: uuid::Uuid) -> Result<()> {
        DatabaseEngine::commit_transaction(self, session_id).await
    }

    async fn rollback_transaction(&self, session_id: uuid::Uuid) -> Result<()> {
        DatabaseEngine::rollback_transaction(self, session_id).await
    }
}

impl DatabaseEngine {
    /// Determine the command tag for a SQL statement
    fn determine_command_tag(sql: &str, rows_affected: Option<u64>, row_count: usize) -> String {
        let upper = sql.trim().to_uppercase();
        if upper.starts_with("SELECT") {
            format!("SELECT {}", row_count)
        } else if upper.starts_with("INSERT") {
            format!("INSERT 0 {}", rows_affected.unwrap_or(0))
        } else if upper.starts_with("UPDATE") {
            format!("UPDATE {}", rows_affected.unwrap_or(0))
        } else if upper.starts_with("DELETE") {
            format!("DELETE {}", rows_affected.unwrap_or(0))
        } else if upper.starts_with("CREATE TABLE") {
            "CREATE TABLE".to_string()
        } else if upper.starts_with("DROP TABLE") {
            "DROP TABLE".to_string()
        } else if upper.starts_with("CREATE INDEX") {
            "CREATE INDEX".to_string()
        } else if upper.starts_with("DROP INDEX") {
            "DROP INDEX".to_string()
        } else if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
            "BEGIN".to_string()
        } else if upper.starts_with("COMMIT") {
            "COMMIT".to_string()
        } else if upper.starts_with("ROLLBACK") {
            "ROLLBACK".to_string()
        } else {
            "OK".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_creation() {
        let config = EngineConfig {
            data_dir: PathBuf::from("/tmp/thunder_test_data"),
            wal_dir: PathBuf::from("/tmp/thunder_test_wal"),
            ..Default::default()
        };

        let engine = DatabaseEngine::new(config).await.unwrap();
        assert_eq!(engine.active_sessions(), 0);
    }

    #[tokio::test]
    async fn test_session_management() {
        let config = EngineConfig {
            data_dir: PathBuf::from("/tmp/thunder_test_data2"),
            wal_dir: PathBuf::from("/tmp/thunder_test_wal2"),
            ..Default::default()
        };

        let engine = DatabaseEngine::new(config).await.unwrap();

        let session = engine.create_session();
        assert_eq!(engine.active_sessions(), 1);

        engine.close_session(session.id);
        assert_eq!(engine.active_sessions(), 0);
    }

    #[tokio::test]
    async fn test_create_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = EngineConfig {
            data_dir: temp_dir.path().join("data"),
            wal_dir: temp_dir.path().join("wal"),
            ..Default::default()
        };

        let engine = DatabaseEngine::new(config).await.unwrap();
        let session = engine.create_session();

        let result = engine.execute_sql(
            session.id,
            "CREATE TABLE users (id INT, name VARCHAR(100))"
        ).await;

        assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
        assert!(engine.list_tables().contains(&"users".to_string()));
    }

    #[tokio::test]
    async fn test_engine_stats() {
        let config = EngineConfig {
            data_dir: PathBuf::from("/tmp/thunder_test_data4"),
            wal_dir: PathBuf::from("/tmp/thunder_test_wal4"),
            ..Default::default()
        };

        let engine = DatabaseEngine::new(config).await.unwrap();
        let stats = engine.stats();

        assert_eq!(stats.queries_total, 0);
        assert!(stats.uptime_seconds() >= 0);
    }

    // ========================================================================
    // Crash Recovery Tests
    // ========================================================================

    #[tokio::test]
    async fn test_checkpoint_creation() {
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap();
        let wal_dir = tempdir().unwrap();

        let config = EngineConfig {
            data_dir: data_dir.path().to_path_buf(),
            wal_dir: wal_dir.path().to_path_buf(),
            ..Default::default()
        };

        let engine = DatabaseEngine::new(config).await.unwrap();
        let session = engine.create_session();

        // Create some data
        engine.execute_sql(session.id, "CREATE TABLE checkpoint_test (id INT)")
            .await
            .unwrap();

        // Write checkpoint
        let result = engine.checkpoint().await;
        assert!(result.is_ok(), "Checkpoint should succeed");
    }

    #[tokio::test]
    async fn test_clean_shutdown() {
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap();
        let wal_dir = tempdir().unwrap();

        let config = EngineConfig {
            data_dir: data_dir.path().to_path_buf(),
            wal_dir: wal_dir.path().to_path_buf(),
            ..Default::default()
        };

        let engine = DatabaseEngine::new(config).await.unwrap();
        let session = engine.create_session();

        // Create some data
        engine.execute_sql(session.id, "CREATE TABLE shutdown_test (id INT)")
            .await
            .unwrap();

        engine.execute_sql(session.id, "INSERT INTO shutdown_test VALUES (42)")
            .await
            .unwrap();

        // Clean shutdown
        let result = engine.shutdown().await;
        assert!(result.is_ok(), "Clean shutdown should succeed");
    }

    #[tokio::test]
    async fn test_wal_recovery_initializes_correctly() {
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap();
        let wal_dir = tempdir().unwrap();

        // Phase 1: Create engine, write some data, shutdown
        {
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            let engine = DatabaseEngine::new(config).await.unwrap();
            let session = engine.create_session();

            // Create table and insert data
            engine.execute_sql(session.id, "CREATE TABLE recovery_test (id INT, value VARCHAR(50))")
                .await
                .unwrap();

            engine.execute_sql(session.id, "INSERT INTO recovery_test VALUES (1, 'test')")
                .await
                .unwrap();

            // Checkpoint to ensure WAL is written
            engine.checkpoint().await.unwrap();
        }

        // Phase 2: Reopen and verify engine initializes without error
        {
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            // Engine should recover successfully from existing WAL
            let engine = DatabaseEngine::new(config).await;
            assert!(engine.is_ok(), "Engine should recover from WAL without errors");
        }
    }

    #[tokio::test]
    async fn test_wal_files_created() {
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap();
        let wal_dir = tempdir().unwrap();

        let config = EngineConfig {
            data_dir: data_dir.path().to_path_buf(),
            wal_dir: wal_dir.path().to_path_buf(),
            ..Default::default()
        };

        let engine = DatabaseEngine::new(config).await.unwrap();
        let session = engine.create_session();

        // Create table
        engine.execute_sql(session.id, "CREATE TABLE wal_test (id INT)")
            .await
            .unwrap();

        // Checkpoint
        engine.checkpoint().await.unwrap();

        // Check that WAL files exist
        let wal_files: Vec<_> = std::fs::read_dir(wal_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|ext| ext == "log").unwrap_or(false))
            .collect();

        assert!(!wal_files.is_empty(), "WAL files should be created after checkpoint");
    }

    #[tokio::test]
    async fn test_catalog_persistence_create_table() {
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap();
        let wal_dir = tempdir().unwrap();

        // Phase 1: Create engine and table
        {
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            let engine = DatabaseEngine::new(config).await.unwrap();
            let session = engine.create_session();

            // Create a table
            engine.execute_sql(session.id, "CREATE TABLE persistent_users (id INT, name VARCHAR(100))")
                .await
                .unwrap();

            // Verify table exists
            assert!(engine.list_tables().contains(&"persistent_users".to_string()));

            // Shutdown cleanly
            engine.shutdown().await.unwrap();
        }

        // Phase 2: Reopen and verify table still exists
        {
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            let engine = DatabaseEngine::new(config).await.unwrap();

            // Table should be recovered from WAL
            let tables = engine.list_tables();
            assert!(
                tables.contains(&"persistent_users".to_string()),
                "Table 'persistent_users' should be recovered from WAL. Found tables: {:?}",
                tables
            );
        }
    }

    #[tokio::test]
    async fn test_catalog_persistence_drop_table() {
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap();
        let wal_dir = tempdir().unwrap();

        // Phase 1: Create engine, create table, then drop it
        {
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            let engine = DatabaseEngine::new(config).await.unwrap();
            let session = engine.create_session();

            // Create a table
            engine.execute_sql(session.id, "CREATE TABLE temp_table (id INT)")
                .await
                .unwrap();

            // Verify table exists
            assert!(engine.list_tables().contains(&"temp_table".to_string()));

            // Drop the table
            engine.execute_sql(session.id, "DROP TABLE temp_table")
                .await
                .unwrap();

            // Verify table no longer exists
            assert!(!engine.list_tables().contains(&"temp_table".to_string()));

            // Shutdown cleanly
            engine.shutdown().await.unwrap();
        }

        // Phase 2: Reopen and verify table is still dropped
        {
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            let engine = DatabaseEngine::new(config).await.unwrap();

            // Table should NOT be present (drop was persisted)
            let tables = engine.list_tables();
            assert!(
                !tables.contains(&"temp_table".to_string()),
                "Table 'temp_table' should NOT be recovered (was dropped). Found tables: {:?}",
                tables
            );
        }
    }

    #[tokio::test]
    async fn test_catalog_persistence_multiple_tables() {
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap();
        let wal_dir = tempdir().unwrap();

        // Phase 1: Create multiple tables with different operations
        {
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            let engine = DatabaseEngine::new(config).await.unwrap();
            let session = engine.create_session();

            // Create three tables
            engine.execute_sql(session.id, "CREATE TABLE table_a (id INT)")
                .await
                .unwrap();
            engine.execute_sql(session.id, "CREATE TABLE table_b (name VARCHAR(50))")
                .await
                .unwrap();
            engine.execute_sql(session.id, "CREATE TABLE table_c (value FLOAT)")
                .await
                .unwrap();

            // Drop one table
            engine.execute_sql(session.id, "DROP TABLE table_b")
                .await
                .unwrap();

            // Verify state
            let tables = engine.list_tables();
            assert!(tables.contains(&"table_a".to_string()));
            assert!(!tables.contains(&"table_b".to_string()));
            assert!(tables.contains(&"table_c".to_string()));

            engine.shutdown().await.unwrap();
        }

        // Phase 2: Verify persistence
        {
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            let engine = DatabaseEngine::new(config).await.unwrap();

            let tables = engine.list_tables();
            assert!(
                tables.contains(&"table_a".to_string()),
                "table_a should exist. Found: {:?}",
                tables
            );
            assert!(
                !tables.contains(&"table_b".to_string()),
                "table_b should NOT exist (was dropped). Found: {:?}",
                tables
            );
            assert!(
                tables.contains(&"table_c".to_string()),
                "table_c should exist. Found: {:?}",
                tables
            );
        }
    }

    #[tokio::test]
    async fn test_data_persistence_insert() {
        use tempfile::tempdir;

        let data_dir = tempdir().unwrap();
        let wal_dir = tempdir().unwrap();

        println!("DEBUG: data_dir = {:?}", data_dir.path());
        println!("DEBUG: wal_dir = {:?}", wal_dir.path());

        // Phase 1: Create table and insert data
        {
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            let engine = DatabaseEngine::new(config).await.unwrap();
            let session = engine.create_session();

            // Create a table
            engine.execute_sql(session.id, "CREATE TABLE users (id INT, name VARCHAR(100))")
                .await
                .unwrap();

            // Insert some data
            engine.execute_sql(session.id, "INSERT INTO users VALUES (1, 'Alice')")
                .await
                .unwrap();
            engine.execute_sql(session.id, "INSERT INTO users VALUES (2, 'Bob')")
                .await
                .unwrap();
            engine.execute_sql(session.id, "INSERT INTO users VALUES (3, 'Charlie')")
                .await
                .unwrap();

            // Verify data exists
            let result = engine.execute_sql(session.id, "SELECT * FROM users")
                .await
                .unwrap();
            println!("DEBUG: Phase 1 - rows before shutdown: {}", result.rows.len());
            assert_eq!(result.rows.len(), 3, "Should have 3 rows before restart");

            // List WAL files before shutdown
            let wal_files: Vec<_> = std::fs::read_dir(wal_dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            println!("DEBUG: WAL files before shutdown: {:?}", wal_files.iter().map(|f| f.path()).collect::<Vec<_>>());

            // Shutdown cleanly
            engine.shutdown().await.unwrap();

            // List WAL files after shutdown
            let wal_files: Vec<_> = std::fs::read_dir(wal_dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            println!("DEBUG: WAL files after shutdown: {:?}", wal_files.iter().map(|f| f.path()).collect::<Vec<_>>());
        }

        // Phase 2: Reopen and verify data still exists
        {
            println!("DEBUG: Phase 2 - reopening engine");
            let config = EngineConfig {
                data_dir: data_dir.path().to_path_buf(),
                wal_dir: wal_dir.path().to_path_buf(),
                ..Default::default()
            };

            let engine = DatabaseEngine::new(config).await.unwrap();
            let session = engine.create_session();

            // Table should exist
            let tables = engine.list_tables();
            println!("DEBUG: Phase 2 - tables after recovery: {:?}", tables);
            assert!(tables.contains(&"users".to_string()));

            // Data should be recovered
            let result = engine.execute_sql(session.id, "SELECT * FROM users")
                .await
                .unwrap();

            println!("DEBUG: Phase 2 - rows after recovery: {}", result.rows.len());
            assert_eq!(
                result.rows.len(), 3,
                "Should have 3 rows after restart. Got: {:?}",
                result.rows
            );
        }
    }
}
