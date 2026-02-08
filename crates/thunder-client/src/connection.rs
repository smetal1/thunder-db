//! Connection Module
//!
//! Provides the underlying TCP/TLS connection to ThunderDB server.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, trace};

use thunder_common::error::ProtocolError;
use thunder_common::prelude::*;

use crate::{ClientConfig, QueryResult};

// ============================================================================
// Protocol Constants
// ============================================================================

/// Protocol version (compatible with PostgreSQL wire protocol)
const PROTOCOL_VERSION: i32 = 196608; // 3.0

/// Message types (PostgreSQL wire protocol)
mod message_type {
    pub const AUTHENTICATION: u8 = b'R';
    pub const PARAMETER_STATUS: u8 = b'S';
    pub const BACKEND_KEY_DATA: u8 = b'K';
    pub const READY_FOR_QUERY: u8 = b'Z';
    pub const ROW_DESCRIPTION: u8 = b'T';
    pub const DATA_ROW: u8 = b'D';
    pub const COMMAND_COMPLETE: u8 = b'C';
    pub const ERROR_RESPONSE: u8 = b'E';
    pub const NOTICE_RESPONSE: u8 = b'N';
    pub const _PARSE_COMPLETE: u8 = b'1';
    pub const _BIND_COMPLETE: u8 = b'2';
    pub const _CLOSE_COMPLETE: u8 = b'3';
    pub const _NO_DATA: u8 = b'n';
    pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
}

/// Frontend message types
mod frontend_message {
    pub const QUERY: u8 = b'Q';
    pub const _PARSE: u8 = b'P';
    pub const _BIND: u8 = b'B';
    pub const _EXECUTE: u8 = b'E';
    pub const _DESCRIBE: u8 = b'D';
    pub const _SYNC: u8 = b'S';
    pub const _CLOSE: u8 = b'C';
    pub const TERMINATE: u8 = b'X';
}

// ============================================================================
// Connection State
// ============================================================================

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Connecting to server
    Connecting,
    /// Connected, performing authentication
    Authenticating,
    /// Ready for queries
    Ready,
    /// In a transaction
    InTransaction,
    /// Error state (transaction failed)
    Error,
    /// Terminated
    Terminated,
}

// ============================================================================
// Connection
// ============================================================================

/// A single connection to ThunderDB
pub struct Connection {
    /// Connection ID (unique per client)
    id: u64,
    /// Configuration
    config: ClientConfig,
    /// TCP stream (possibly TLS wrapped)
    stream: Option<Mutex<ConnectionStream>>,
    /// Current state
    state: parking_lot::RwLock<ConnectionState>,
    /// Backend process ID
    backend_pid: AtomicU64,
    /// Backend secret key
    backend_key: AtomicU64,
    /// Server parameters
    server_params: parking_lot::RwLock<std::collections::HashMap<String, String>>,
    /// Last activity time
    last_activity: parking_lot::RwLock<Instant>,
    /// Query counter
    query_count: AtomicU64,
}

/// Connection stream (TCP or TLS)
enum ConnectionStream {
    Plain(BufReader<TcpStream>, BufWriter<TcpStream>),
}

impl Connection {
    /// Create a new connection
    pub fn new(id: u64, config: ClientConfig) -> Self {
        Self {
            id,
            config,
            stream: None,
            state: parking_lot::RwLock::new(ConnectionState::Disconnected),
            backend_pid: AtomicU64::new(0),
            backend_key: AtomicU64::new(0),
            server_params: parking_lot::RwLock::new(std::collections::HashMap::new()),
            last_activity: parking_lot::RwLock::new(Instant::now()),
            query_count: AtomicU64::new(0),
        }
    }

    /// Get connection ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get current state
    pub fn state(&self) -> ConnectionState {
        *self.state.read()
    }

    /// Check if connection is ready for queries
    pub fn is_ready(&self) -> bool {
        matches!(self.state(), ConnectionState::Ready | ConnectionState::InTransaction)
    }

    /// Get query count
    pub fn query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    /// Get last activity time
    pub fn last_activity(&self) -> Instant {
        *self.last_activity.read()
    }

    /// Connect to the server
    pub async fn connect(&mut self) -> Result<()> {
        *self.state.write() = ConnectionState::Connecting;

        let addr_str = format!("{}:{}", self.config.host, self.config.port);
        let addr: SocketAddr = addr_str.parse()
            .map_err(|e| Error::Config(format!("Invalid address '{}': {}", addr_str, e)))?;

        debug!(id = self.id, addr = %addr, "Connecting to server");

        // Connect with timeout
        let connect_timeout = Duration::from_millis(self.config.connect_timeout_ms);
        let stream = timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout("Connection timeout".to_string()))?
            .map_err(|e| Error::Io(e))?;

        // Set TCP options
        stream.set_nodelay(true)?;

        // Split for reader/writer
        let (read_half, write_half) = stream.into_split();
        let _reader = BufReader::new(read_half.reunite(write_half).unwrap());

        // Reconnect for split
        let stream2 = timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout("Connection timeout".to_string()))?
            .map_err(|e| Error::Io(e))?;

        let (read_half, write_half) = stream2.into_split();
        let _reader = BufReader::new(read_half.reunite(write_half).unwrap());

        let stream3 = timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout("Connection timeout".to_string()))?
            .map_err(|e| Error::Io(e))?;
        let (_, _write_half2) = stream3.into_split();

        // Actually, let's use a simpler approach - just use one stream
        let stream = timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout("Connection timeout".to_string()))?
            .map_err(|e| Error::Io(e))?;

        stream.set_nodelay(true)?;

        // Use std stream for cloning
        let std_stream = stream.into_std()?;
        let std_stream2 = std_stream.try_clone()
            .map_err(|e| Error::Internal(format!("Failed to clone stream: {}", e)))?;

        let reader = BufReader::new(TcpStream::from_std(std_stream)?);
        let writer = BufWriter::new(TcpStream::from_std(std_stream2)?);

        self.stream = Some(Mutex::new(ConnectionStream::Plain(reader, writer)));

        // Perform startup handshake
        self.startup().await?;

        *self.state.write() = ConnectionState::Ready;
        *self.last_activity.write() = Instant::now();

        debug!(id = self.id, "Connection established");
        Ok(())
    }

    /// Perform startup handshake
    async fn startup(&mut self) -> Result<()> {
        *self.state.write() = ConnectionState::Authenticating;

        let stream_guard = self.stream.as_ref()
            .ok_or_else(|| Error::Protocol(ProtocolError::InvalidMessage("Not connected".to_string())))?;
        let mut stream = stream_guard.lock().await;

        // Send startup message
        let mut startup = BytesMut::new();
        startup.put_i32(PROTOCOL_VERSION);
        startup.put_slice(b"user\0");
        startup.put_slice(self.config.user.as_bytes());
        startup.put_u8(0);
        startup.put_slice(b"database\0");
        startup.put_slice(self.config.database.as_bytes());
        startup.put_u8(0);
        startup.put_u8(0); // Terminate parameter list

        // Write length-prefixed startup message
        let len = startup.len() + 4;
        let mut msg = BytesMut::with_capacity(len);
        msg.put_i32(len as i32);
        msg.extend_from_slice(&startup);

        match &mut *stream {
            ConnectionStream::Plain(_, writer) => {
                writer.write_all(&msg).await?;
                writer.flush().await?;
            }
        }

        // Process server responses
        loop {
            let (msg_type, payload) = self.read_message_inner(&mut stream).await?;

            match msg_type {
                message_type::AUTHENTICATION => {
                    self.handle_authentication(&payload, &mut stream).await?;
                }
                message_type::PARAMETER_STATUS => {
                    self.handle_parameter_status(&payload)?;
                }
                message_type::BACKEND_KEY_DATA => {
                    self.handle_backend_key(&payload)?;
                }
                message_type::READY_FOR_QUERY => {
                    trace!("Startup complete, ready for queries");
                    break;
                }
                message_type::ERROR_RESPONSE => {
                    return Err(self.parse_error_response(&payload));
                }
                _ => {
                    trace!(msg_type = msg_type, "Ignoring unknown startup message");
                }
            }
        }

        Ok(())
    }

    /// Read a message from the stream
    async fn read_message_inner(&self, stream: &mut ConnectionStream) -> Result<(u8, Bytes)> {
        let (reader, _) = match stream {
            ConnectionStream::Plain(r, w) => (r, w),
        };

        // Read message type (1 byte)
        let msg_type = reader.read_u8().await?;

        // Read message length (4 bytes, includes self)
        let len = reader.read_i32().await? as usize;
        if len < 4 {
            return Err(Error::Protocol(ProtocolError::InvalidMessage("Invalid message length".to_string())));
        }

        // Read message body
        let body_len = len - 4;
        let mut body = vec![0u8; body_len];
        reader.read_exact(&mut body).await?;

        Ok((msg_type, Bytes::from(body)))
    }

    /// Handle authentication response
    async fn handle_authentication(&self, payload: &[u8], stream: &mut ConnectionStream) -> Result<()> {
        if payload.len() < 4 {
            return Err(Error::Protocol(ProtocolError::InvalidMessage("Invalid authentication message".to_string())));
        }

        let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

        match auth_type {
            0 => {
                // AuthenticationOk
                trace!("Authentication successful");
                Ok(())
            }
            3 => {
                // AuthenticationCleartextPassword
                self.send_password(stream).await
            }
            5 => {
                // AuthenticationMD5Password
                if payload.len() < 8 {
                    return Err(Error::Protocol(ProtocolError::InvalidMessage("Invalid MD5 auth message".to_string())));
                }
                let salt = &payload[4..8];
                self.send_md5_password(salt, stream).await
            }
            _ => Err(Error::Protocol(ProtocolError::AuthenticationFailed(
                format!("Unsupported authentication type: {}", auth_type)
            ))),
        }
    }

    /// Send cleartext password
    async fn send_password(&self, stream: &mut ConnectionStream) -> Result<()> {
        let password = self.config.password.as_deref().unwrap_or("");

        let mut msg = BytesMut::new();
        msg.put_u8(b'p');
        let password_bytes = password.as_bytes();
        msg.put_i32((4 + password_bytes.len() + 1) as i32);
        msg.put_slice(password_bytes);
        msg.put_u8(0);

        match stream {
            ConnectionStream::Plain(_, writer) => {
                writer.write_all(&msg).await?;
                writer.flush().await?;
            }
        }

        Ok(())
    }

    /// Send MD5 hashed password
    async fn send_md5_password(&self, salt: &[u8], stream: &mut ConnectionStream) -> Result<()> {
        let password = self.config.password.as_deref().unwrap_or("");
        let user = &self.config.user;

        // MD5(MD5(password + user) + salt)
        let inner = format!("{}{}", password, user);
        let inner_hash = format!("{:x}", md5::compute(inner.as_bytes()));
        let mut outer_input = inner_hash.into_bytes();
        outer_input.extend_from_slice(salt);
        let outer_hash = format!("md5{:x}", md5::compute(&outer_input));

        let mut msg = BytesMut::new();
        msg.put_u8(b'p');
        let hash_bytes = outer_hash.as_bytes();
        msg.put_i32((4 + hash_bytes.len() + 1) as i32);
        msg.put_slice(hash_bytes);
        msg.put_u8(0);

        match stream {
            ConnectionStream::Plain(_, writer) => {
                writer.write_all(&msg).await?;
                writer.flush().await?;
            }
        }

        Ok(())
    }

    /// Handle parameter status message
    fn handle_parameter_status(&self, payload: &[u8]) -> Result<()> {
        let parts: Vec<&[u8]> = payload.split(|&b| b == 0).collect();
        if parts.len() >= 2 {
            let name = String::from_utf8_lossy(parts[0]).to_string();
            let value = String::from_utf8_lossy(parts[1]).to_string();
            trace!(name = %name, value = %value, "Server parameter");
            self.server_params.write().insert(name, value);
        }
        Ok(())
    }

    /// Handle backend key data
    fn handle_backend_key(&self, payload: &[u8]) -> Result<()> {
        if payload.len() >= 8 {
            let pid = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let key = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            self.backend_pid.store(pid as u64, Ordering::Relaxed);
            self.backend_key.store(key as u64, Ordering::Relaxed);
            trace!(pid = pid, key = key, "Backend key data");
        }
        Ok(())
    }

    /// Parse error response
    fn parse_error_response(&self, payload: &[u8]) -> Error {
        let mut message = String::new();
        let mut code = String::new();
        let mut i = 0;

        while i < payload.len() {
            let field_type = payload[i];
            if field_type == 0 {
                break;
            }
            i += 1;

            let end = payload[i..].iter().position(|&b| b == 0).unwrap_or(payload.len() - i);
            let value = String::from_utf8_lossy(&payload[i..i + end]).to_string();
            i += end + 1;

            match field_type {
                b'M' => message = value,
                b'C' => code = value,
                _ => {}
            }
        }

        Error::Protocol(ProtocolError::InvalidMessage(format!("[{}] {}", code, message)))
    }

    /// Execute a simple query
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        if !self.is_ready() {
            return Err(Error::Protocol(ProtocolError::InvalidMessage("Connection not ready".to_string())));
        }

        *self.last_activity.write() = Instant::now();
        self.query_count.fetch_add(1, Ordering::Relaxed);

        let stream_guard = self.stream.as_ref()
            .ok_or_else(|| Error::Protocol(ProtocolError::InvalidMessage("Not connected".to_string())))?;
        let mut stream = stream_guard.lock().await;

        // Send Query message
        let mut msg = BytesMut::new();
        msg.put_u8(frontend_message::QUERY);
        let sql_bytes = sql.as_bytes();
        msg.put_i32((4 + sql_bytes.len() + 1) as i32);
        msg.put_slice(sql_bytes);
        msg.put_u8(0);

        match &mut *stream {
            ConnectionStream::Plain(_, writer) => {
                writer.write_all(&msg).await?;
                writer.flush().await?;
            }
        }

        // Read response
        let mut columns = Vec::new();
        let mut rows = Vec::new();

        loop {
            let (msg_type, payload) = self.read_message_inner(&mut stream).await?;

            match msg_type {
                message_type::ROW_DESCRIPTION => {
                    columns = self.parse_row_description(&payload)?;
                }
                message_type::DATA_ROW => {
                    let row = self.parse_data_row(&payload)?;
                    rows.push(row);
                }
                message_type::COMMAND_COMPLETE => {
                    trace!(tag = ?String::from_utf8_lossy(&payload), "Command complete");
                }
                message_type::READY_FOR_QUERY => {
                    self.handle_ready_for_query(&payload)?;
                    break;
                }
                message_type::ERROR_RESPONSE => {
                    return Err(self.parse_error_response(&payload));
                }
                message_type::NOTICE_RESPONSE => {
                    trace!("Notice received");
                }
                message_type::EMPTY_QUERY_RESPONSE => {
                    trace!("Empty query");
                }
                _ => {
                    trace!(msg_type = msg_type, "Ignoring unknown message");
                }
            }
        }

        Ok(QueryResult { columns, rows })
    }

    /// Execute a statement (INSERT, UPDATE, DELETE)
    pub async fn execute(&mut self, sql: &str) -> Result<u64> {
        if !self.is_ready() {
            return Err(Error::Protocol(ProtocolError::InvalidMessage("Connection not ready".to_string())));
        }

        *self.last_activity.write() = Instant::now();
        self.query_count.fetch_add(1, Ordering::Relaxed);

        let stream_guard = self.stream.as_ref()
            .ok_or_else(|| Error::Protocol(ProtocolError::InvalidMessage("Not connected".to_string())))?;
        let mut stream = stream_guard.lock().await;

        // Send Query message
        let mut msg = BytesMut::new();
        msg.put_u8(frontend_message::QUERY);
        let sql_bytes = sql.as_bytes();
        msg.put_i32((4 + sql_bytes.len() + 1) as i32);
        msg.put_slice(sql_bytes);
        msg.put_u8(0);

        match &mut *stream {
            ConnectionStream::Plain(_, writer) => {
                writer.write_all(&msg).await?;
                writer.flush().await?;
            }
        }

        // Read response
        let mut rows_affected = 0u64;

        loop {
            let (msg_type, payload) = self.read_message_inner(&mut stream).await?;

            match msg_type {
                message_type::COMMAND_COMPLETE => {
                    let tag = String::from_utf8_lossy(&payload);
                    rows_affected = self.parse_command_tag(&tag);
                }
                message_type::READY_FOR_QUERY => {
                    self.handle_ready_for_query(&payload)?;
                    break;
                }
                message_type::ERROR_RESPONSE => {
                    return Err(self.parse_error_response(&payload));
                }
                _ => {}
            }
        }

        Ok(rows_affected)
    }

    /// Parse row description message
    fn parse_row_description(&self, payload: &[u8]) -> Result<Vec<String>> {
        if payload.len() < 2 {
            return Ok(Vec::new());
        }

        let field_count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
        let mut columns = Vec::with_capacity(field_count);
        let mut pos = 2;

        for _ in 0..field_count {
            let name_end = payload[pos..].iter().position(|&b| b == 0)
                .ok_or_else(|| Error::Protocol(ProtocolError::InvalidMessage("Invalid row description".to_string())))?;
            let name = String::from_utf8_lossy(&payload[pos..pos + name_end]).to_string();
            columns.push(name);
            pos += name_end + 1;
            pos += 18; // Skip table OID, column attr, type OID, type size, type mod, format
        }

        Ok(columns)
    }

    /// Parse data row message
    fn parse_data_row(&self, payload: &[u8]) -> Result<Row> {
        if payload.len() < 2 {
            return Ok(Row::empty());
        }

        let field_count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
        let mut values = Vec::with_capacity(field_count);
        let mut pos = 2;

        for _ in 0..field_count {
            if pos + 4 > payload.len() {
                break;
            }

            let len = i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
            pos += 4;

            if len == -1 {
                values.push(Value::Null);
            } else {
                let len = len as usize;
                if pos + len > payload.len() {
                    break;
                }
                let data = &payload[pos..pos + len];
                let s = String::from_utf8_lossy(data).to_string();
                values.push(Value::String(std::sync::Arc::from(s.as_str())));
                pos += len;
            }
        }

        Ok(Row::new(values))
    }

    /// Parse command tag for row count
    fn parse_command_tag(&self, tag: &str) -> u64 {
        let parts: Vec<&str> = tag.split_whitespace().collect();
        parts.last()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    /// Handle ready for query message
    fn handle_ready_for_query(&self, payload: &[u8]) -> Result<()> {
        if let Some(&status) = payload.first() {
            let new_state = match status {
                b'I' => ConnectionState::Ready,
                b'T' => ConnectionState::InTransaction,
                b'E' => ConnectionState::Error,
                _ => ConnectionState::Ready,
            };
            *self.state.write() = new_state;
        }
        Ok(())
    }

    /// Ping the server
    pub async fn ping(&mut self) -> Result<()> {
        self.execute("SELECT 1").await?;
        Ok(())
    }

    /// Close the connection
    pub async fn close(&mut self) -> Result<()> {
        if let Some(stream_guard) = &self.stream {
            let mut stream = stream_guard.lock().await;

            let mut msg = BytesMut::new();
            msg.put_u8(frontend_message::TERMINATE);
            msg.put_i32(4);

            match &mut *stream {
                ConnectionStream::Plain(_, writer) => {
                    let _ = writer.write_all(&msg).await;
                    let _ = writer.flush().await;
                }
            }
        }

        self.stream = None;
        *self.state.write() = ConnectionState::Terminated;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state() {
        let config = ClientConfig::default();
        let conn = Connection::new(1, config);
        assert_eq!(conn.state(), ConnectionState::Disconnected);
        assert!(!conn.is_ready());
    }

    #[test]
    fn test_parse_command_tag() {
        let config = ClientConfig::default();
        let conn = Connection::new(1, config);

        assert_eq!(conn.parse_command_tag("INSERT 0 5"), 5);
        assert_eq!(conn.parse_command_tag("UPDATE 10"), 10);
        assert_eq!(conn.parse_command_tag("DELETE 3"), 3);
    }
}
