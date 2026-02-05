//! MySQL Wire Protocol Implementation
//!
//! Implements the MySQL client/server protocol.
//! See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basics.html

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

use thunder_common::prelude::*;

use crate::{AuthMethod, ConnectionLimiter, MaybeTlsStream, ProtocolConfig, ProtocolHandler, Session};
use crate::auth::{mysql_native_password, mysql_caching_sha2};
use crate::postgres::{QueryExecutor, QueryResult, ColumnInfo};

// ============================================================================
// MySQL Protocol Constants
// ============================================================================

/// MySQL protocol version
const PROTOCOL_VERSION: u8 = 10;

/// Server version string
const SERVER_VERSION: &str = "8.0.35-ThunderDB";

// Capability flags
const CLIENT_LONG_PASSWORD: u32 = 0x0001;
const CLIENT_FOUND_ROWS: u32 = 0x0002;
const CLIENT_LONG_FLAG: u32 = 0x0004;
const CLIENT_CONNECT_WITH_DB: u32 = 0x0008;
const CLIENT_NO_SCHEMA: u32 = 0x0010;
const CLIENT_COMPRESS: u32 = 0x0020;
const CLIENT_ODBC: u32 = 0x0040;
const CLIENT_LOCAL_FILES: u32 = 0x0080;
const CLIENT_IGNORE_SPACE: u32 = 0x0100;
const CLIENT_PROTOCOL_41: u32 = 0x0200;
const CLIENT_INTERACTIVE: u32 = 0x0400;
const CLIENT_SSL: u32 = 0x0800;
const CLIENT_IGNORE_SIGPIPE: u32 = 0x1000;
const CLIENT_TRANSACTIONS: u32 = 0x2000;
const CLIENT_RESERVED: u32 = 0x4000;
const CLIENT_SECURE_CONNECTION: u32 = 0x8000;
const CLIENT_MULTI_STATEMENTS: u32 = 0x00010000;
const CLIENT_MULTI_RESULTS: u32 = 0x00020000;
const CLIENT_PS_MULTI_RESULTS: u32 = 0x00040000;
const CLIENT_PLUGIN_AUTH: u32 = 0x00080000;
const CLIENT_CONNECT_ATTRS: u32 = 0x00100000;
const CLIENT_PLUGIN_AUTH_LENENC_DATA: u32 = 0x00200000;
const CLIENT_DEPRECATE_EOF: u32 = 0x01000000;

// Server capabilities
const SERVER_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_FOUND_ROWS
    | CLIENT_LONG_FLAG
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_MULTI_RESULTS
    | CLIENT_PLUGIN_AUTH
    | CLIENT_DEPRECATE_EOF;

// Server status flags
const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
const SERVER_STATUS_IN_TRANS: u16 = 0x0001;

// Command types
const COM_SLEEP: u8 = 0x00;
const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_FIELD_LIST: u8 = 0x04;
const COM_CREATE_DB: u8 = 0x05;
const COM_DROP_DB: u8 = 0x06;
const COM_REFRESH: u8 = 0x07;
const COM_SHUTDOWN: u8 = 0x08;
const COM_STATISTICS: u8 = 0x09;
const COM_PROCESS_INFO: u8 = 0x0a;
const COM_CONNECT: u8 = 0x0b;
const COM_PROCESS_KILL: u8 = 0x0c;
const COM_DEBUG: u8 = 0x0d;
const COM_PING: u8 = 0x0e;
const COM_STMT_PREPARE: u8 = 0x16;
const COM_STMT_EXECUTE: u8 = 0x17;
const COM_STMT_SEND_LONG_DATA: u8 = 0x18;
const COM_STMT_CLOSE: u8 = 0x19;
const COM_STMT_RESET: u8 = 0x1a;
const COM_SET_OPTION: u8 = 0x1b;
const COM_STMT_FETCH: u8 = 0x1c;

// Response packet headers
const OK_PACKET: u8 = 0x00;
const EOF_PACKET: u8 = 0xfe;
const ERR_PACKET: u8 = 0xff;

// Column types
const MYSQL_TYPE_DECIMAL: u8 = 0x00;
const MYSQL_TYPE_TINY: u8 = 0x01;
const MYSQL_TYPE_SHORT: u8 = 0x02;
const MYSQL_TYPE_LONG: u8 = 0x03;
const MYSQL_TYPE_FLOAT: u8 = 0x04;
const MYSQL_TYPE_DOUBLE: u8 = 0x05;
const MYSQL_TYPE_NULL: u8 = 0x06;
const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
const MYSQL_TYPE_LONGLONG: u8 = 0x08;
const MYSQL_TYPE_INT24: u8 = 0x09;
const MYSQL_TYPE_DATE: u8 = 0x0a;
const MYSQL_TYPE_TIME: u8 = 0x0b;
const MYSQL_TYPE_DATETIME: u8 = 0x0c;
const MYSQL_TYPE_YEAR: u8 = 0x0d;
const MYSQL_TYPE_VARCHAR: u8 = 0x0f;
const MYSQL_TYPE_BIT: u8 = 0x10;
const MYSQL_TYPE_JSON: u8 = 0xf5;
const MYSQL_TYPE_NEWDECIMAL: u8 = 0xf6;
const MYSQL_TYPE_BLOB: u8 = 0xfc;
const MYSQL_TYPE_VAR_STRING: u8 = 0xfd;
const MYSQL_TYPE_STRING: u8 = 0xfe;

// ============================================================================
// MySQL Protocol Handler
// ============================================================================

/// MySQL protocol handler
pub struct MySqlHandler<E: QueryExecutor> {
    executor: Arc<E>,
    config: ProtocolConfig,
    tls_acceptor: Option<TlsAcceptor>,
}

impl<E: QueryExecutor> MySqlHandler<E> {
    pub fn new(executor: Arc<E>, config: ProtocolConfig) -> Self {
        // Build TLS acceptor if TLS is configured
        let tls_acceptor = config.tls.as_ref().and_then(|tls_config| {
            match tls_config.build_acceptor() {
                Ok(acceptor) => Some(acceptor),
                Err(e) => {
                    error!("Failed to build TLS acceptor: {}", e);
                    None
                }
            }
        });

        Self { executor, config, tls_acceptor }
    }

    /// Check if TLS is available
    pub fn tls_available(&self) -> bool {
        self.tls_acceptor.is_some()
    }
}

#[async_trait]
impl<E: QueryExecutor + 'static> ProtocolHandler for MySqlHandler<E> {
    async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let peer_addr = stream.peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        let tls_status = if self.tls_available() { " (TLS available)" } else { "" };
        info!("New MySQL connection from {}{}", peer_addr, tls_status);

        let mut conn = MySqlConnection::new(
            stream,
            self.executor.clone(),
            self.config.clone(),
            self.tls_acceptor.clone(),
        );

        match conn.run().await {
            Ok(()) => {
                info!("MySQL connection from {} closed normally", peer_addr);
            }
            Err(e) => {
                warn!("MySQL connection from {} closed with error: {}", peer_addr, e);
            }
        }

        Ok(())
    }
}

// ============================================================================
// MySQL Connection
// ============================================================================

struct MySqlConnection<E: QueryExecutor> {
    /// Stream wrapped in Option to allow taking ownership for TLS upgrade
    stream: Option<MaybeTlsStream<TcpStream>>,
    executor: Arc<E>,
    config: ProtocolConfig,
    session: Session,
    session_id: Option<uuid::Uuid>,
    sequence_id: u8,
    connection_id: u32,
    scramble: [u8; 20],
    client_capabilities: u32,
    server_status: u16,
    prepared_statements: HashMap<u32, PreparedStatementInfo>,
    next_stmt_id: u32,
    tls_acceptor: Option<TlsAcceptor>,
    tls_available: bool,
}

#[derive(Debug, Clone)]
struct PreparedStatementInfo {
    query: String,
    param_count: usize,
    column_count: usize,
}

impl<E: QueryExecutor> MySqlConnection<E> {
    fn new(stream: TcpStream, executor: Arc<E>, config: ProtocolConfig, tls_acceptor: Option<TlsAcceptor>) -> Self {
        let connection_id = rand::random::<u32>();
        let scramble: [u8; 20] = rand::random();
        let tls_available = tls_acceptor.is_some();

        Self {
            stream: Some(MaybeTlsStream::Plain(stream)),
            executor,
            config,
            session: Session::new(),
            session_id: None,
            sequence_id: 0,
            connection_id,
            scramble,
            client_capabilities: 0,
            server_status: SERVER_STATUS_AUTOCOMMIT,
            prepared_statements: HashMap::new(),
            next_stmt_id: 1,
            tls_acceptor,
            tls_available,
        }
    }

    /// Get server capabilities, including SSL if TLS is available
    fn server_capabilities(&self) -> u32 {
        if self.tls_available {
            SERVER_CAPABILITIES | CLIENT_SSL
        } else {
            SERVER_CAPABILITIES
        }
    }

    /// Get a mutable reference to the stream
    fn stream_mut(&mut self) -> Result<&mut MaybeTlsStream<TcpStream>> {
        self.stream.as_mut().ok_or_else(|| Error::Internal("Stream not available".into()))
    }

    async fn run(&mut self) -> Result<()> {
        // Send initial handshake
        self.send_handshake().await?;

        // Read handshake response
        self.handle_handshake_response().await?;

        // Create session
        self.session_id = Some(self.executor.create_session());

        // Send OK packet
        self.send_ok(0, 0).await?;

        // Command loop
        const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
        loop {
            let packet = match tokio::time::timeout(IDLE_TIMEOUT, self.read_packet()).await {
                Ok(Ok(p)) => p,
                Ok(Err(_)) => break,
                Err(_) => {
                    debug!("MySQL connection idle for {:?}, closing", IDLE_TIMEOUT);
                    break;
                }
            };

            if packet.is_empty() {
                break;
            }

            let command = packet[0];

            match command {
                COM_QUIT => {
                    debug!("Received COM_QUIT");
                    break;
                }
                COM_QUERY => {
                    self.handle_query(&packet[1..]).await?;
                }
                COM_INIT_DB => {
                    self.handle_init_db(&packet[1..]).await?;
                }
                COM_PING => {
                    self.send_ok(0, 0).await?;
                }
                COM_STMT_PREPARE => {
                    self.handle_stmt_prepare(&packet[1..]).await?;
                }
                COM_STMT_EXECUTE => {
                    self.handle_stmt_execute(&packet[1..]).await?;
                }
                COM_STMT_CLOSE => {
                    self.handle_stmt_close(&packet[1..]).await?;
                }
                COM_FIELD_LIST => {
                    self.handle_field_list(&packet[1..]).await?;
                }
                COM_STATISTICS => {
                    self.send_statistics().await?;
                }
                _ => {
                    warn!("Unknown command: 0x{:02x}", command);
                    self.send_error(1047, "08S01", "Unknown command").await?;
                }
            }
        }

        // Cleanup
        if let Some(session_id) = self.session_id {
            self.executor.close_session(session_id);
        }

        Ok(())
    }

    // ========================================================================
    // Handshake
    // ========================================================================

    async fn send_handshake(&mut self) -> Result<()> {
        let mut packet = BytesMut::new();
        let capabilities = self.server_capabilities();

        // Protocol version
        packet.put_u8(PROTOCOL_VERSION);

        // Server version (null-terminated)
        packet.put_slice(SERVER_VERSION.as_bytes());
        packet.put_u8(0);

        // Connection ID
        packet.put_u32_le(self.connection_id);

        // Auth-plugin-data-part-1 (first 8 bytes of scramble)
        packet.put_slice(&self.scramble[..8]);
        packet.put_u8(0); // filler

        // Capability flags (lower 2 bytes)
        packet.put_u16_le((capabilities & 0xFFFF) as u16);

        // Character set (utf8mb4)
        packet.put_u8(255);

        // Status flags
        packet.put_u16_le(self.server_status);

        // Capability flags (upper 2 bytes)
        packet.put_u16_le(((capabilities >> 16) & 0xFFFF) as u16);

        // Auth-plugin-data-len
        packet.put_u8(21); // Length of auth data

        // Reserved (10 bytes)
        packet.put_slice(&[0u8; 10]);

        // Auth-plugin-data-part-2 (remaining 12 bytes of scramble + null)
        packet.put_slice(&self.scramble[8..]);
        packet.put_u8(0);

        // Auth-plugin name
        packet.put_slice(b"mysql_native_password\0");

        self.write_packet(&packet).await
    }

    async fn handle_handshake_response(&mut self) -> Result<()> {
        let packet = self.read_packet().await?;

        if packet.len() < 32 {
            return Err(Error::Internal("Invalid handshake response".into()));
        }

        // Parse capability flags first to check for SSL request
        let client_capabilities = u32::from_le_bytes([
            packet[0], packet[1], packet[2], packet[3]
        ]);

        // Check if this is an SSL request (32 bytes with CLIENT_SSL flag)
        if (client_capabilities & CLIENT_SSL) != 0 && self.tls_available {
            // Client is requesting SSL upgrade
            debug!("Client requesting SSL upgrade");

            // Perform TLS handshake
            let acceptor = self.tls_acceptor.take()
                .ok_or_else(|| Error::Internal("TLS acceptor not available".into()))?;

            // We need to upgrade the connection to TLS
            // Take ownership of the stream and upgrade it
            self.upgrade_to_tls(acceptor).await?;

            // After TLS upgrade, client sends the real handshake response
            let packet = self.read_packet().await?;
            return self.parse_handshake_response(&packet);
        }

        // No SSL, parse the handshake response directly
        self.parse_handshake_response(&packet)
    }

    /// Upgrade the connection to TLS
    async fn upgrade_to_tls(&mut self, acceptor: TlsAcceptor) -> Result<()> {
        // Take ownership of the stream for upgrade
        let stream = self.stream.take()
            .ok_or_else(|| Error::Internal("Stream not available for TLS upgrade".into()))?;

        match stream {
            MaybeTlsStream::Plain(plain_stream) => {
                match acceptor.accept(plain_stream).await {
                    Ok(tls_stream) => {
                        info!("MySQL TLS handshake completed successfully");
                        self.stream = Some(MaybeTlsStream::Tls(tls_stream));
                        Ok(())
                    }
                    Err(e) => {
                        Err(Error::Internal(format!("TLS handshake failed: {}", e)))
                    }
                }
            }
            MaybeTlsStream::Tls(_) => {
                Err(Error::Internal("Already using TLS".into()))
            }
        }
    }

    fn parse_handshake_response(&mut self, packet: &[u8]) -> Result<()> {
        if packet.len() < 32 {
            return Err(Error::Internal("Invalid handshake response".into()));
        }

        let mut pos = 0;

        // Capability flags
        self.client_capabilities = u32::from_le_bytes([
            packet[pos], packet[pos+1], packet[pos+2], packet[pos+3]
        ]);
        pos += 4;

        // Max packet size
        let _max_packet = u32::from_le_bytes([
            packet[pos], packet[pos+1], packet[pos+2], packet[pos+3]
        ]);
        pos += 4;

        // Character set
        let _charset = packet[pos];
        pos += 1;

        // Reserved (23 bytes)
        pos += 23;

        // Username (null-terminated)
        let username_end = packet[pos..].iter().position(|&b| b == 0)
            .ok_or_else(|| Error::Internal("Invalid username".into()))?;
        let username = String::from_utf8_lossy(&packet[pos..pos+username_end]).to_string();
        pos += username_end + 1;

        self.session.set_user(username);

        // Auth response
        if (self.client_capabilities & CLIENT_PLUGIN_AUTH_LENENC_DATA) != 0 {
            let auth_len = packet[pos] as usize;
            pos += 1;
            let auth_response = &packet[pos..pos+auth_len];
            pos += auth_len;

            // Verify password
            let expected = mysql_native_password("thunder", &self.scramble);
            if auth_response != expected.as_slice() {
                // For now, accept any password
                debug!("Password verification (allowing any)");
            }
        } else if (self.client_capabilities & CLIENT_SECURE_CONNECTION) != 0 {
            let auth_len = packet[pos] as usize;
            pos += 1;
            let _auth_response = &packet[pos..pos+auth_len];
            pos += auth_len;
        }

        // Database (if CLIENT_CONNECT_WITH_DB)
        if (self.client_capabilities & CLIENT_CONNECT_WITH_DB) != 0 && pos < packet.len() {
            let db_end = packet[pos..].iter().position(|&b| b == 0)
                .unwrap_or(packet.len() - pos);
            let database = String::from_utf8_lossy(&packet[pos..pos+db_end]).to_string();
            if !database.is_empty() {
                self.session.set_database(database);
            }
        }

        Ok(())
    }

    // ========================================================================
    // Query Handling
    // ========================================================================

    async fn handle_query(&mut self, data: &[u8]) -> Result<()> {
        let query = String::from_utf8_lossy(data).to_string();
        debug!("MySQL query: {}", query);

        let session_id = self.session_id.ok_or_else(||
            Error::Internal("No session".into()))?;

        // Handle special queries
        let upper = query.trim().to_uppercase();
        if upper.starts_with("SET ") {
            // Accept SET commands silently
            self.send_ok(0, 0).await?;
            return Ok(());
        }
        if upper.starts_with("SELECT @@") || upper.contains("INFORMATION_SCHEMA") {
            // Handle system variable queries
            return self.handle_system_query(&query).await;
        }

        // Execute query
        match self.executor.execute(session_id, &query).await {
            Ok(result) => {
                if !result.columns.is_empty() {
                    self.send_result_set(&result).await?;
                } else {
                    let affected = result.rows_affected.unwrap_or(0);
                    self.send_ok(affected, 0).await?;
                }
            }
            Err(e) => {
                self.send_error(1064, "42000", &e.to_string()).await?;
            }
        }

        Ok(())
    }

    async fn handle_system_query(&mut self, _query: &str) -> Result<()> {
        // Return empty result for system queries
        self.send_ok(0, 0).await
    }

    async fn handle_init_db(&mut self, data: &[u8]) -> Result<()> {
        let database = String::from_utf8_lossy(data).to_string();
        debug!("Init DB: {}", database);
        self.session.set_database(database);
        self.send_ok(0, 0).await
    }

    async fn handle_field_list(&mut self, _data: &[u8]) -> Result<()> {
        // Send empty field list
        self.send_eof().await
    }

    async fn send_statistics(&mut self) -> Result<()> {
        let stats = "Uptime: 100  Threads: 1  Questions: 1  Slow queries: 0  Opens: 0  Flush tables: 0  Open tables: 0  Queries per second avg: 0.0";
        let mut packet = BytesMut::new();
        packet.put_slice(stats.as_bytes());
        self.write_packet(&packet).await
    }

    // ========================================================================
    // Prepared Statements
    // ========================================================================

    async fn handle_stmt_prepare(&mut self, data: &[u8]) -> Result<()> {
        let query = String::from_utf8_lossy(data).to_string();
        debug!("Prepare: {}", query);

        // Count parameters (?)
        let param_count = query.matches('?').count();

        let stmt_id = self.next_stmt_id;
        self.next_stmt_id += 1;

        self.prepared_statements.insert(stmt_id, PreparedStatementInfo {
            query,
            param_count,
            column_count: 0,
        });

        // Send COM_STMT_PREPARE_OK
        let mut packet = BytesMut::new();
        packet.put_u8(0x00); // status
        packet.put_u32_le(stmt_id);
        packet.put_u16_le(0); // column count (will be determined at execute)
        packet.put_u16_le(param_count as u16);
        packet.put_u8(0); // reserved
        packet.put_u16_le(0); // warning count

        self.write_packet(&packet).await?;

        // Send parameter definitions if any
        if param_count > 0 {
            for _ in 0..param_count {
                self.send_column_definition("?", MYSQL_TYPE_VAR_STRING).await?;
            }
            if (self.client_capabilities & CLIENT_DEPRECATE_EOF) == 0 {
                self.send_eof().await?;
            }
        }

        Ok(())
    }

    async fn handle_stmt_execute(&mut self, data: &[u8]) -> Result<()> {
        if data.len() < 9 {
            return self.send_error(1064, "42000", "Invalid execute packet").await;
        }

        let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let _flags = data[4];
        let _iteration_count = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);

        let stmt = match self.prepared_statements.get(&stmt_id) {
            Some(s) => s.clone(),
            None => {
                return self.send_error(1243, "HY000", "Unknown prepared statement").await;
            }
        };

        // Parse parameters from packet
        let mut query = stmt.query.clone();
        let mut param_pos = 9;

        if stmt.param_count > 0 && data.len() > param_pos {
            // Skip null bitmap
            let null_bitmap_len = (stmt.param_count + 7) / 8;
            param_pos += null_bitmap_len;

            // Check new-params-bound flag
            if data.len() > param_pos && data[param_pos] == 1 {
                param_pos += 1;
                // Skip type definitions
                param_pos += stmt.param_count * 2;
            }

            // Read parameter values
            // Simplified: just use the remaining data as string values
            let params_data = &data[param_pos..];
            let params_str = String::from_utf8_lossy(params_data);

            // Replace ? with the value (very simplified)
            for value in params_str.split('\0').filter(|s| !s.is_empty()) {
                if let Some(pos) = query.find('?') {
                    query.replace_range(pos..pos+1, &format!("'{}'", value.replace('\'', "''")));
                }
            }
        }

        debug!("Execute prepared: {}", query);

        let session_id = self.session_id.ok_or_else(||
            Error::Internal("No session".into()))?;

        match self.executor.execute(session_id, &query).await {
            Ok(result) => {
                if !result.columns.is_empty() {
                    self.send_result_set(&result).await?;
                } else {
                    let affected = result.rows_affected.unwrap_or(0);
                    self.send_ok(affected, 0).await?;
                }
            }
            Err(e) => {
                self.send_error(1064, "42000", &e.to_string()).await?;
            }
        }

        Ok(())
    }

    async fn handle_stmt_close(&mut self, data: &[u8]) -> Result<()> {
        if data.len() >= 4 {
            let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            self.prepared_statements.remove(&stmt_id);
            debug!("Closed statement {}", stmt_id);
        }
        // COM_STMT_CLOSE doesn't send a response
        Ok(())
    }

    // ========================================================================
    // Result Set
    // ========================================================================

    async fn send_result_set(&mut self, result: &QueryResult) -> Result<()> {
        // Send column count
        self.write_lenenc_int(result.columns.len() as u64).await?;

        // Send column definitions
        for col in &result.columns {
            self.send_column_definition(&col.name, datatype_to_mysql_type(col.type_oid)).await?;
        }

        // Send EOF (if not deprecated)
        if (self.client_capabilities & CLIENT_DEPRECATE_EOF) == 0 {
            self.send_eof().await?;
        }

        // Send rows
        for row in &result.rows {
            self.send_text_row(row).await?;
        }

        // Send EOF or OK
        if (self.client_capabilities & CLIENT_DEPRECATE_EOF) != 0 {
            self.send_ok(0, 0).await?;
        } else {
            self.send_eof().await?;
        }

        Ok(())
    }

    async fn send_column_definition(&mut self, name: &str, column_type: u8) -> Result<()> {
        let mut packet = BytesMut::new();

        // Catalog (always "def")
        write_lenenc_string(&mut packet, "def");
        // Schema
        write_lenenc_string(&mut packet, "");
        // Table (virtual)
        write_lenenc_string(&mut packet, "");
        // Org table
        write_lenenc_string(&mut packet, "");
        // Name
        write_lenenc_string(&mut packet, name);
        // Org name
        write_lenenc_string(&mut packet, name);
        // Length of fixed fields
        packet.put_u8(0x0c);
        // Character set (utf8mb4)
        packet.put_u16_le(255);
        // Column length
        packet.put_u32_le(255);
        // Column type
        packet.put_u8(column_type);
        // Flags
        packet.put_u16_le(0);
        // Decimals
        packet.put_u8(0);
        // Filler
        packet.put_u16_le(0);

        self.write_packet(&packet).await
    }

    async fn send_text_row(&mut self, values: &[Option<String>]) -> Result<()> {
        let mut packet = BytesMut::new();

        for val in values {
            match val {
                Some(s) => write_lenenc_string(&mut packet, s),
                None => packet.put_u8(0xfb), // NULL
            }
        }

        self.write_packet(&packet).await
    }

    // ========================================================================
    // Packet I/O
    // ========================================================================

    async fn read_packet(&mut self) -> Result<Vec<u8>> {
        let stream = self.stream_mut()?;

        // Read header (4 bytes: 3 length + 1 sequence)
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await
            .map_err(|e| Error::Internal(e.to_string()))?;

        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        self.sequence_id = header[3].wrapping_add(1);

        // Read payload
        let mut payload = vec![0u8; length];
        let stream = self.stream_mut()?;
        stream.read_exact(&mut payload).await
            .map_err(|e| Error::Internal(e.to_string()))?;

        Ok(payload)
    }

    async fn write_packet(&mut self, payload: &[u8]) -> Result<()> {
        let length = payload.len();

        // Write header
        let header = [
            (length & 0xFF) as u8,
            ((length >> 8) & 0xFF) as u8,
            ((length >> 16) & 0xFF) as u8,
            self.sequence_id,
        ];

        let stream = self.stream_mut()?;
        stream.write_all(&header).await
            .map_err(|e| Error::Internal(e.to_string()))?;
        stream.write_all(payload).await
            .map_err(|e| Error::Internal(e.to_string()))?;

        self.sequence_id = self.sequence_id.wrapping_add(1);

        Ok(())
    }

    async fn write_lenenc_int(&mut self, value: u64) -> Result<()> {
        let mut packet = BytesMut::new();
        write_lenenc_int(&mut packet, value);
        self.write_packet(&packet).await
    }

    async fn send_ok(&mut self, affected_rows: u64, last_insert_id: u64) -> Result<()> {
        let mut packet = BytesMut::new();
        packet.put_u8(OK_PACKET);
        write_lenenc_int(&mut packet, affected_rows);
        write_lenenc_int(&mut packet, last_insert_id);

        if (self.client_capabilities & CLIENT_PROTOCOL_41) != 0 {
            packet.put_u16_le(self.server_status);
            packet.put_u16_le(0); // warnings
        }

        self.sequence_id = 0;
        self.write_packet(&packet).await
    }

    async fn send_eof(&mut self) -> Result<()> {
        let mut packet = BytesMut::new();
        packet.put_u8(EOF_PACKET);

        if (self.client_capabilities & CLIENT_PROTOCOL_41) != 0 {
            packet.put_u16_le(0); // warnings
            packet.put_u16_le(self.server_status);
        }

        self.write_packet(&packet).await
    }

    async fn send_error(&mut self, code: u16, state: &str, message: &str) -> Result<()> {
        let mut packet = BytesMut::new();
        packet.put_u8(ERR_PACKET);
        packet.put_u16_le(code);

        if (self.client_capabilities & CLIENT_PROTOCOL_41) != 0 {
            packet.put_u8(b'#');
            packet.put_slice(state.as_bytes());
        }

        packet.put_slice(message.as_bytes());

        self.sequence_id = 0;
        self.write_packet(&packet).await
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn write_lenenc_int(buf: &mut BytesMut, value: u64) {
    if value < 251 {
        buf.put_u8(value as u8);
    } else if value < 65536 {
        buf.put_u8(0xfc);
        buf.put_u16_le(value as u16);
    } else if value < 16777216 {
        buf.put_u8(0xfd);
        buf.put_u8((value & 0xFF) as u8);
        buf.put_u8(((value >> 8) & 0xFF) as u8);
        buf.put_u8(((value >> 16) & 0xFF) as u8);
    } else {
        buf.put_u8(0xfe);
        buf.put_u64_le(value);
    }
}

fn write_lenenc_string(buf: &mut BytesMut, s: &str) {
    write_lenenc_int(buf, s.len() as u64);
    buf.put_slice(s.as_bytes());
}

/// Convert PostgreSQL OID to MySQL column type
fn datatype_to_mysql_type(oid: i32) -> u8 {
    match oid {
        16 => MYSQL_TYPE_TINY,     // bool -> tiny
        18 => MYSQL_TYPE_TINY,     // char
        21 => MYSQL_TYPE_SHORT,    // int2
        23 => MYSQL_TYPE_LONG,     // int4
        20 => MYSQL_TYPE_LONGLONG, // int8
        700 => MYSQL_TYPE_FLOAT,   // float4
        701 => MYSQL_TYPE_DOUBLE,  // float8
        25 | 1043 | 1042 => MYSQL_TYPE_VAR_STRING, // text, varchar, char
        17 => MYSQL_TYPE_BLOB,     // bytea
        1082 => MYSQL_TYPE_DATE,   // date
        1083 => MYSQL_TYPE_TIME,   // time
        1114 | 1184 => MYSQL_TYPE_TIMESTAMP, // timestamp(tz)
        114 | 3802 => MYSQL_TYPE_JSON, // json(b)
        _ => MYSQL_TYPE_VAR_STRING,
    }
}

// ============================================================================
// MySQL Server
// ============================================================================

/// MySQL protocol server
pub struct MySqlServer<E: QueryExecutor + 'static> {
    handler: Arc<MySqlHandler<E>>,
    config: ProtocolConfig,
}

impl<E: QueryExecutor + 'static> MySqlServer<E> {
    pub fn new(executor: Arc<E>, config: ProtocolConfig) -> Self {
        let handler = Arc::new(MySqlHandler::new(executor, config.clone()));
        Self { handler, config }
    }

    /// Run the MySQL server
    pub async fn run(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.listen_addr, self.config.port);
        let listener = tokio::net::TcpListener::bind(&addr).await
            .map_err(|e| Error::Internal(format!("Failed to bind to {}: {}", addr, e)))?;

        let limiter = ConnectionLimiter::new(self.config.max_connections);
        let tls_status = if self.handler.tls_available() { ", TLS enabled" } else { "" };
        info!(
            "MySQL server listening on {} (max_connections: {}{})",
            addr, self.config.max_connections, tls_status
        );

        loop {
            let (stream, peer_addr) = listener.accept().await
                .map_err(|e| Error::Internal(e.to_string()))?;

            // Try to acquire a connection permit
            let permit = match limiter.try_acquire() {
                Some(permit) => permit,
                None => {
                    warn!(
                        "Rejecting MySQL connection from {}: max connections ({}) reached",
                        peer_addr, self.config.max_connections
                    );
                    drop(stream);
                    continue;
                }
            };

            debug!(
                "MySQL connection from {} (active: {}/{})",
                peer_addr,
                limiter.active_connections(),
                limiter.max_connections()
            );

            let handler = self.handler.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = handler.handle_connection(stream).await {
                    error!("MySQL connection error: {}", e);
                }
            });
        }
    }
}

// ============================================================================
// Standalone serve function for main.rs integration
// ============================================================================

/// Serve MySQL protocol connections on the given listener
///
/// This is a convenience function for use in main.rs that accepts a pre-bound
/// TcpListener and an executor implementing QueryExecutor.
pub async fn serve<E: QueryExecutor + 'static>(
    listener: tokio::net::TcpListener,
    executor: Arc<E>,
) {
    serve_with_cancel(listener, executor, crate::ProtocolConfig {
        listen_addr: "0.0.0.0".to_string(),
        port: 3306,
        max_connections: 1000,
        auth_method: crate::AuthMethod::Trust,
        tls: None,
        auth_rules: Vec::new(),
    }, tokio_util::sync::CancellationToken::new()).await
}

/// Serve MySQL protocol connections with custom configuration.
pub async fn serve_with_config<E: QueryExecutor + 'static>(
    listener: tokio::net::TcpListener,
    executor: Arc<E>,
    config: crate::ProtocolConfig,
) {
    serve_with_cancel(listener, executor, config, tokio_util::sync::CancellationToken::new()).await
}

/// Serve MySQL protocol connections with cancellation support for graceful shutdown.
pub async fn serve_with_cancel<E: QueryExecutor + 'static>(
    listener: tokio::net::TcpListener,
    executor: Arc<E>,
    config: crate::ProtocolConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    let limiter = ConnectionLimiter::new(config.max_connections);
    let handler = Arc::new(MySqlHandler::new(executor, config.clone()));
    let tls_status = if handler.tls_available() { ", TLS enabled" } else { "" };

    info!(
        "MySQL protocol server started (max_connections: {}{})",
        config.max_connections, tls_status
    );

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        if let Err(e) = crate::set_tcp_keepalive(&stream) {
                            warn!("Failed to set TCP keepalive for {}: {}", peer_addr, e);
                        }

                        let permit = match limiter.try_acquire() {
                            Some(permit) => permit,
                            None => {
                                warn!(
                                    "Rejecting MySQL connection from {}: max connections ({}) reached",
                                    peer_addr, config.max_connections
                                );
                                drop(stream);
                                continue;
                            }
                        };

                        debug!(
                            "MySQL connection from {} (active: {}/{})",
                            peer_addr,
                            limiter.active_connections(),
                            limiter.max_connections()
                        );

                        let handler = handler.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(e) = handler.handle_connection(stream).await {
                                error!("MySQL connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept MySQL connection: {}", e);
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                info!("MySQL protocol server shutting down");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_conversion() {
        assert_eq!(datatype_to_mysql_type(23), MYSQL_TYPE_LONG);
        assert_eq!(datatype_to_mysql_type(25), MYSQL_TYPE_VAR_STRING);
        assert_eq!(datatype_to_mysql_type(16), MYSQL_TYPE_TINY);
    }

    #[test]
    fn test_lenenc_int() {
        let mut buf = BytesMut::new();
        write_lenenc_int(&mut buf, 250);
        assert_eq!(buf[0], 250);

        let mut buf = BytesMut::new();
        write_lenenc_int(&mut buf, 300);
        assert_eq!(buf[0], 0xfc);
    }
}
