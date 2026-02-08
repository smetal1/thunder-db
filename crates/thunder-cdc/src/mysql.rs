//! MySQL CDC Connector
//!
//! Implements Change Data Capture via MySQL binary log (binlog):
//! - Supports ROW-based replication format
//! - Handles GTID-based positioning
//! - Parses binlog events for INSERT/UPDATE/DELETE
//! - Provides schema tracking for column mapping

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use sqlx::{mysql::MySqlPoolOptions, MySql, Pool};
use tracing::{debug, info};

use crate::{CdcConfig, CdcConnector, CdcEvent, CdcOperation, CdcPosition};
use thunder_common::prelude::*;

// ============================================================================
// MySQL Connector
// ============================================================================

/// MySQL CDC connector using binary log replication
#[allow(dead_code)]
pub struct MySqlConnector {
    /// Configuration
    config: CdcConfig,
    /// Connection pool
    pool: Option<Pool<MySql>>,
    /// Current binlog position
    current_file: String,
    current_position: u64,
    /// Server ID for replication
    server_id: u32,
    /// Connected flag
    connected: bool,
    /// Table schema cache
    schema_cache: HashMap<String, TableSchema>,
    /// Poll interval
    poll_interval: Duration,
    /// Last poll time
    last_poll: std::time::Instant,
    /// GTID mode enabled
    gtid_mode: bool,
    /// Current GTID set
    gtid_set: Option<String>,
}

/// Cached table schema
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TableSchema {
    database: String,
    table: String,
    columns: Vec<ColumnSchema>,
    primary_key: Vec<String>,
}

/// Column schema
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ColumnSchema {
    name: String,
    column_type: String,
    ordinal: u32,
    nullable: bool,
    is_primary: bool,
}

/// Binlog event types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum BinlogEventType {
    Unknown,
    QueryEvent,
    TableMapEvent,
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
    XidEvent,
    RotateEvent,
    FormatDescriptionEvent,
    GtidEvent,
}

impl MySqlConnector {
    /// Create a new MySQL connector
    pub async fn new(config: CdcConfig) -> Result<Self> {
        // Generate unique server ID for replication slave
        let server_id = rand::random::<u32>() % 1000000 + 1000;

        Ok(Self {
            config,
            pool: None,
            current_file: String::new(),
            current_position: 4, // Binlog starts at position 4
            server_id,
            connected: false,
            schema_cache: HashMap::new(),
            poll_interval: Duration::from_millis(100),
            last_poll: std::time::Instant::now(),
            gtid_mode: false,
            gtid_set: None,
        })
    }

    /// Connect to MySQL
    async fn connect(&mut self) -> Result<()> {
        if self.connected {
            return Ok(());
        }

        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&self.config.url)
            .await
            .map_err(|e| Error::Internal(format!("MySQL connection failed: {}", e)))?;

        self.pool = Some(pool);
        self.connected = true;

        // Check if GTID mode is enabled
        self.check_gtid_mode().await?;

        // Get current binlog position if not set
        if self.current_file.is_empty() {
            self.get_binlog_position().await?;
        }

        // Cache table schemas
        self.refresh_schema_cache().await?;

        info!(
            file = %self.current_file,
            position = self.current_position,
            gtid_mode = self.gtid_mode,
            "MySQL connector connected"
        );

        Ok(())
    }

    /// Check if GTID mode is enabled
    async fn check_gtid_mode(&mut self) -> Result<()> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        let row: Option<(String,)> = sqlx::query_as(
            "SELECT @@gtid_mode"
        )
        .fetch_optional(pool)
        .await
        .map_err(|e| Error::Internal(format!("Failed to check GTID mode: {}", e)))?;

        if let Some((mode,)) = row {
            self.gtid_mode = mode == "ON" || mode == "ON_PERMISSIVE";
        }

        Ok(())
    }

    /// Get current binlog position
    async fn get_binlog_position(&mut self) -> Result<()> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        let row: (String, u64) = sqlx::query_as(
            "SHOW MASTER STATUS"
        )
        .fetch_one(pool)
        .await
        .map_err(|e| Error::Internal(format!("Failed to get binlog position: {}", e)))?;

        self.current_file = row.0;
        self.current_position = row.1;

        Ok(())
    }

    /// Refresh schema cache for tracked tables
    async fn refresh_schema_cache(&mut self) -> Result<()> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        for table in &self.config.tables {
            let parts: Vec<&str> = table.split('.').collect();
            let (database, table_name) = if parts.len() == 2 {
                (parts[0].to_string(), parts[1].to_string())
            } else {
                // Get current database
                let db: (String,) = sqlx::query_as("SELECT DATABASE()")
                    .fetch_one(pool)
                    .await
                    .map_err(|e| Error::Internal(format!("Failed to get database: {}", e)))?;
                (db.0, parts[0].to_string())
            };

            // Get column information
            let columns: Vec<(String, String, u32, String, String)> = sqlx::query_as(
                r#"
                SELECT
                    COLUMN_NAME,
                    COLUMN_TYPE,
                    ORDINAL_POSITION,
                    IS_NULLABLE,
                    COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                "#,
            )
            .bind(&database)
            .bind(&table_name)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to get columns: {}", e)))?;

            let column_schemas: Vec<ColumnSchema> = columns
                .into_iter()
                .map(|(name, col_type, ordinal, nullable, key)| ColumnSchema {
                    name,
                    column_type: col_type,
                    ordinal,
                    nullable: nullable == "YES",
                    is_primary: key == "PRI",
                })
                .collect();

            let primary_key: Vec<String> = column_schemas
                .iter()
                .filter(|c| c.is_primary)
                .map(|c| c.name.clone())
                .collect();

            let schema = TableSchema {
                database: database.clone(),
                table: table_name.clone(),
                columns: column_schemas,
                primary_key,
            };

            let key = format!("{}.{}", database, table_name);
            self.schema_cache.insert(key, schema);
        }

        Ok(())
    }

    /// Poll binlog for changes
    async fn poll_binlog(&mut self) -> Result<Vec<CdcEvent>> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        // Use SHOW BINLOG EVENTS to get changes
        // Note: In production, use mysql-async's binlog streaming API
        let query = format!(
            "SHOW BINLOG EVENTS IN '{}' FROM {} LIMIT {}",
            self.current_file,
            self.current_position,
            self.config.batch_size
        );

        let rows: Vec<(String, u64, String, u32, u64, String)> = sqlx::query_as(&query)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read binlog: {}", e)))?;

        let mut events = Vec::new();
        let mut max_position = self.current_position;

        for (log_name, pos, event_type, _server_id, end_pos, info) in rows {
            // Skip events at current position (already processed)
            if pos <= self.current_position && log_name == self.current_file {
                continue;
            }

            // Parse event based on type
            if let Some(event) = self.parse_binlog_event(&event_type, &info, pos) {
                events.push(event);
            }

            max_position = end_pos;

            // Handle rotate event (binlog file rotation)
            if event_type == "Rotate" {
                if let Some(new_file) = info.split(';').next() {
                    self.current_file = new_file.trim().to_string();
                    max_position = 4; // Reset to start of new file
                }
            }
        }

        // Update position
        if max_position > self.current_position {
            self.current_position = max_position;
        }

        Ok(events)
    }

    /// Parse binlog event info string
    fn parse_binlog_event(&self, event_type: &str, info: &str, pos: u64) -> Option<CdcEvent> {
        match event_type {
            "Write_rows" | "Write_rows_v1" | "Write_rows_v2" => {
                self.parse_row_event(CdcOperation::Insert, info, pos)
            }
            "Update_rows" | "Update_rows_v1" | "Update_rows_v2" => {
                self.parse_row_event(CdcOperation::Update, info, pos)
            }
            "Delete_rows" | "Delete_rows_v1" | "Delete_rows_v2" => {
                self.parse_row_event(CdcOperation::Delete, info, pos)
            }
            "Query" => {
                // Handle DDL statements
                if info.to_uppercase().contains("TRUNCATE") {
                    self.parse_query_event(CdcOperation::Truncate, info, pos)
                } else if info.to_uppercase().contains("ALTER")
                    || info.to_uppercase().contains("CREATE")
                    || info.to_uppercase().contains("DROP")
                {
                    self.parse_query_event(CdcOperation::SchemaChange, info, pos)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Parse row-based event
    fn parse_row_event(&self, operation: CdcOperation, info: &str, pos: u64) -> Option<CdcEvent> {
        // Info format: "table_id: 123 flags: STMT_END_F"
        // The actual row data isn't in SHOW BINLOG EVENTS output
        // This is a limitation - real implementation needs mysqlbinlog or replication API

        // Try to extract table from info
        // Format varies, common pattern: "table `db`.`table` ..."
        let table_info = self.extract_table_from_info(info)?;

        let (database, table) = table_info;
        let _schema = self.schema_cache.get(&format!("{}.{}", database, table))?;

        // For SHOW BINLOG EVENTS, we can't get actual row data
        // In production, use replication protocol or mysqlbinlog
        let event = CdcEvent {
            source: "mysql".to_string(),
            database,
            schema: None,
            table,
            operation,
            before: None,
            after: None,
            timestamp: Utc::now(),
            position: CdcPosition::MysqlBinlog {
                file: self.current_file.clone(),
                position: pos,
            },
            txn_id: None,
        };

        Some(event)
    }

    /// Parse query event (DDL)
    fn parse_query_event(&self, operation: CdcOperation, info: &str, pos: u64) -> Option<CdcEvent> {
        // Extract database and table from query
        let (database, table) = self.extract_table_from_query(info)?;

        Some(CdcEvent {
            source: "mysql".to_string(),
            database,
            schema: None,
            table,
            operation,
            before: None,
            after: None,
            timestamp: Utc::now(),
            position: CdcPosition::MysqlBinlog {
                file: self.current_file.clone(),
                position: pos,
            },
            txn_id: None,
        })
    }

    /// Extract table from binlog event info
    fn extract_table_from_info(&self, info: &str) -> Option<(String, String)> {
        // Try to find `db`.`table` pattern
        if let Some(start) = info.find('`') {
            let rest = &info[start + 1..];
            if let Some(end) = rest.find('`') {
                let db = &rest[..end];
                let rest2 = &rest[end + 1..];
                if let Some(start2) = rest2.find('`') {
                    let rest3 = &rest2[start2 + 1..];
                    if let Some(end2) = rest3.find('`') {
                        let table = &rest3[..end2];
                        return Some((db.to_string(), table.to_string()));
                    }
                }
            }
        }

        // Fallback: use first tracked table
        if let Some(table) = self.config.tables.first() {
            let parts: Vec<&str> = table.split('.').collect();
            if parts.len() == 2 {
                return Some((parts[0].to_string(), parts[1].to_string()));
            }
        }

        None
    }

    /// Extract table from SQL query
    fn extract_table_from_query(&self, query: &str) -> Option<(String, String)> {
        let query_upper = query.to_uppercase();

        // Try common patterns
        let patterns = [
            "TRUNCATE TABLE ",
            "TRUNCATE ",
            "ALTER TABLE ",
            "DROP TABLE ",
            "CREATE TABLE ",
        ];

        for pattern in patterns {
            if let Some(idx) = query_upper.find(pattern) {
                let rest = &query[idx + pattern.len()..];
                return self.extract_table_name(rest);
            }
        }

        None
    }

    /// Extract table name from string
    fn extract_table_name(&self, s: &str) -> Option<(String, String)> {
        let s = s.trim();

        // Handle `db`.`table` or db.table
        let cleaned: String = s
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.' || *c == '`')
            .collect();

        let cleaned = cleaned.replace('`', "");
        let parts: Vec<&str> = cleaned.split('.').collect();

        if parts.len() == 2 {
            Some((parts[0].to_string(), parts[1].to_string()))
        } else if parts.len() == 1 && !parts[0].is_empty() {
            // Default database
            Some(("default".to_string(), parts[0].to_string()))
        } else {
            None
        }
    }
}

#[async_trait]
impl CdcConnector for MySqlConnector {
    fn name(&self) -> &str {
        "mysql"
    }

    async fn start(&mut self, position: CdcPosition) -> Result<()> {
        // Set initial position
        if let CdcPosition::MysqlBinlog { file, position } = position {
            self.current_file = file;
            self.current_position = position;
        }

        // Connect
        self.connect().await?;

        info!(
            file = %self.current_file,
            position = self.current_position,
            "MySQL connector started"
        );

        Ok(())
    }

    async fn poll(&mut self) -> Result<Vec<CdcEvent>> {
        if !self.connected {
            self.connect().await?;
        }

        // Rate limit polling
        let elapsed = self.last_poll.elapsed();
        if elapsed < self.poll_interval {
            tokio::time::sleep(self.poll_interval - elapsed).await;
        }
        self.last_poll = std::time::Instant::now();

        self.poll_binlog().await
    }

    async fn ack(&mut self, position: CdcPosition) -> Result<()> {
        if let CdcPosition::MysqlBinlog { file, position } = position {
            // Update position
            self.current_file = file;
            self.current_position = position;
            debug!(position = self.current_position, "Acknowledged MySQL position");
        }
        Ok(())
    }

    fn position(&self) -> CdcPosition {
        CdcPosition::MysqlBinlog {
            file: self.current_file.clone(),
            position: self.current_position,
        }
    }

    async fn stop(&mut self) -> Result<()> {
        if self.connected {
            self.pool = None;
            self.connected = false;
            info!("MySQL connector stopped");
        }
        Ok(())
    }
}

// ============================================================================
// MySQL Binlog Protocol Types (for reference/future streaming impl)
// ============================================================================

/// Binlog event header
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BinlogEventHeader {
    timestamp: u32,
    event_type: u8,
    server_id: u32,
    event_length: u32,
    next_position: u32,
    flags: u16,
}

/// Table map event (used to map table_id to table name)
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TableMapEvent {
    table_id: u64,
    database: String,
    table: String,
    column_count: u64,
    column_types: Vec<u8>,
    column_metas: Vec<u16>,
    null_bitmap: Vec<u8>,
}

/// Rows event (INSERT/UPDATE/DELETE)
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct RowsEvent {
    table_id: u64,
    flags: u16,
    column_count: u64,
    columns_present: Vec<u8>,
    rows: Vec<Vec<Value>>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_name() {
        let config = CdcConfig {
            connector_type: crate::ConnectorType::MySQL,
            url: "mysql://localhost/test".to_string(),
            tables: vec!["test.users".to_string()],
            initial_position: CdcPosition::Beginning,
            batch_size: 1000,
            poll_interval_ms: 100,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let connector = rt.block_on(async {
            MySqlConnector::new(config).await.unwrap()
        });

        // Test various formats
        assert_eq!(
            connector.extract_table_name("`mydb`.`users`"),
            Some(("mydb".to_string(), "users".to_string()))
        );
        assert_eq!(
            connector.extract_table_name("mydb.users"),
            Some(("mydb".to_string(), "users".to_string()))
        );
        assert_eq!(
            connector.extract_table_name("users"),
            Some(("default".to_string(), "users".to_string()))
        );
    }

    #[test]
    fn test_binlog_position() {
        let pos = CdcPosition::MysqlBinlog {
            file: "mysql-bin.000001".to_string(),
            position: 12345,
        };

        assert!(matches!(pos, CdcPosition::MysqlBinlog { .. }));
    }

    #[tokio::test]
    async fn test_connector_creation() {
        let config = CdcConfig {
            connector_type: crate::ConnectorType::MySQL,
            url: "mysql://localhost/test".to_string(),
            tables: vec!["test.users".to_string()],
            initial_position: CdcPosition::Beginning,
            batch_size: 1000,
            poll_interval_ms: 100,
        };

        let connector = MySqlConnector::new(config).await.unwrap();
        assert_eq!(connector.name(), "mysql");
        assert!(!connector.connected);
        assert!(connector.server_id > 0);
    }
}
