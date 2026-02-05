//! RESP Protocol Implementation
//!
//! Implements the Redis Serialization Protocol (RESP) for Redis/Valkey compatibility.
//! Supports RESP2 and RESP3 data types and common Redis commands.
//!
//! # Protocol Overview
//!
//! RESP is a text-based protocol with the following data types:
//! - Simple Strings: `+OK\r\n`
//! - Errors: `-ERR message\r\n`
//! - Integers: `:1000\r\n`
//! - Bulk Strings: `$6\r\nfoobar\r\n`
//! - Arrays: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
//! - Null: `$-1\r\n` (RESP2) or `_\r\n` (RESP3)

use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut, Buf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

use crate::{ConnectionLimiter, MaybeTlsStream, TlsConfig};

// ============================================================================
// RESP Value Types
// ============================================================================

/// RESP protocol value types
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// Simple string: +OK\r\n
    SimpleString(String),
    /// Error: -ERR message\r\n
    Error(String),
    /// Integer: :1000\r\n
    Integer(i64),
    /// Bulk string: $6\r\nfoobar\r\n
    BulkString(Bytes),
    /// Array: *2\r\n...
    Array(Vec<RespValue>),
    /// Null value
    Null,
    /// Boolean (RESP3): #t\r\n or #f\r\n
    Boolean(bool),
    /// Double (RESP3): ,1.23\r\n
    Double(f64),
    /// Map (RESP3): %2\r\n...
    Map(Vec<(RespValue, RespValue)>),
}

impl RespValue {
    /// Create a simple string
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    /// Create an error
    pub fn error(msg: impl Into<String>) -> Self {
        RespValue::Error(msg.into())
    }

    /// Create a bulk string from bytes
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        RespValue::BulkString(data.into())
    }

    /// Create a bulk string from a string
    pub fn bulk_string(s: impl AsRef<str>) -> Self {
        RespValue::BulkString(Bytes::copy_from_slice(s.as_ref().as_bytes()))
    }

    /// Create an integer
    pub fn integer(n: i64) -> Self {
        RespValue::Integer(n)
    }

    /// Create an array
    pub fn array(items: Vec<RespValue>) -> Self {
        RespValue::Array(items)
    }

    /// Get as string if possible
    pub fn as_str(&self) -> Option<&str> {
        match self {
            RespValue::SimpleString(s) => Some(s),
            RespValue::BulkString(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Get as bytes if possible
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(b) => Some(b),
            RespValue::SimpleString(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Get as integer if possible
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespValue::Integer(n) => Some(*n),
            RespValue::BulkString(b) => {
                std::str::from_utf8(b).ok()?.parse().ok()
            }
            _ => None,
        }
    }
}

// ============================================================================
// RESP Parser
// ============================================================================

/// RESP protocol parser
pub struct RespParser {
    buffer: BytesMut,
}

impl RespParser {
    /// Create a new parser
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Add data to the buffer
    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to parse a complete RESP value from the buffer
    pub fn parse(&mut self) -> Result<Option<RespValue>, RespError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        match self.parse_value() {
            Ok(Some((value, consumed))) => {
                self.buffer.advance(consumed);
                Ok(Some(value))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn parse_value(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let type_byte = self.buffer[0];
        match type_byte {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            b'_' => self.parse_null(),
            b'#' => self.parse_boolean(),
            b',' => self.parse_double(),
            b'%' => self.parse_map(),
            _ => Err(RespError::Protocol(format!(
                "Unknown type byte: {:02x}",
                type_byte
            ))),
        }
    }

    fn find_crlf(&self, start: usize) -> Option<usize> {
        for i in start..self.buffer.len().saturating_sub(1) {
            if self.buffer[i] == b'\r' && self.buffer[i + 1] == b'\n' {
                return Some(i);
            }
        }
        None
    }

    fn parse_simple_string(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        if let Some(crlf) = self.find_crlf(1) {
            let s = String::from_utf8(self.buffer[1..crlf].to_vec())
                .map_err(|e| RespError::Protocol(format!("Invalid UTF-8: {}", e)))?;
            Ok(Some((RespValue::SimpleString(s), crlf + 2)))
        } else {
            Ok(None)
        }
    }

    fn parse_error(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        if let Some(crlf) = self.find_crlf(1) {
            let s = String::from_utf8(self.buffer[1..crlf].to_vec())
                .map_err(|e| RespError::Protocol(format!("Invalid UTF-8: {}", e)))?;
            Ok(Some((RespValue::Error(s), crlf + 2)))
        } else {
            Ok(None)
        }
    }

    fn parse_integer(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        if let Some(crlf) = self.find_crlf(1) {
            let s = std::str::from_utf8(&self.buffer[1..crlf])
                .map_err(|e| RespError::Protocol(format!("Invalid UTF-8: {}", e)))?;
            let n: i64 = s
                .parse()
                .map_err(|e| RespError::Protocol(format!("Invalid integer: {}", e)))?;
            Ok(Some((RespValue::Integer(n), crlf + 2)))
        } else {
            Ok(None)
        }
    }

    fn parse_bulk_string(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        let crlf = match self.find_crlf(1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len_str = std::str::from_utf8(&self.buffer[1..crlf])
            .map_err(|e| RespError::Protocol(format!("Invalid UTF-8: {}", e)))?;
        let len: i64 = len_str
            .parse()
            .map_err(|e| RespError::Protocol(format!("Invalid length: {}", e)))?;

        if len == -1 {
            return Ok(Some((RespValue::Null, crlf + 2)));
        }

        const MAX_BULK_STRING_SIZE: i64 = 512 * 1024 * 1024; // 512 MB
        if len > MAX_BULK_STRING_SIZE {
            return Err(RespError::Protocol(format!(
                "Bulk string size {} exceeds maximum of {} bytes",
                len, MAX_BULK_STRING_SIZE
            )));
        }

        let len = len as usize;
        let data_start = crlf + 2;
        let data_end = data_start + len;
        let total_len = data_end + 2;

        if self.buffer.len() < total_len {
            return Ok(None);
        }

        let data = Bytes::copy_from_slice(&self.buffer[data_start..data_end]);
        Ok(Some((RespValue::BulkString(data), total_len)))
    }

    fn parse_array(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        let crlf = match self.find_crlf(1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len_str = std::str::from_utf8(&self.buffer[1..crlf])
            .map_err(|e| RespError::Protocol(format!("Invalid UTF-8: {}", e)))?;
        let len: i64 = len_str
            .parse()
            .map_err(|e| RespError::Protocol(format!("Invalid length: {}", e)))?;

        if len == -1 {
            return Ok(Some((RespValue::Null, crlf + 2)));
        }

        let len = len as usize;
        let mut items = Vec::with_capacity(len);
        let mut pos = crlf + 2;

        for _ in 0..len {
            let sub_buffer = &self.buffer[pos..];
            let sub_parser = RespParser {
                buffer: BytesMut::from(sub_buffer),
            };

            match sub_parser.parse_value()? {
                Some((value, consumed)) => {
                    items.push(value);
                    pos += consumed;
                }
                None => return Ok(None),
            }
        }

        Ok(Some((RespValue::Array(items), pos)))
    }

    fn parse_null(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        if self.buffer.len() >= 3 && self.buffer[1] == b'\r' && self.buffer[2] == b'\n' {
            Ok(Some((RespValue::Null, 3)))
        } else if self.buffer.len() < 3 {
            Ok(None)
        } else {
            Err(RespError::Protocol("Invalid null".into()))
        }
    }

    fn parse_boolean(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        if self.buffer.len() >= 4 && self.buffer[2] == b'\r' && self.buffer[3] == b'\n' {
            let value = match self.buffer[1] {
                b't' => true,
                b'f' => false,
                _ => return Err(RespError::Protocol("Invalid boolean".into())),
            };
            Ok(Some((RespValue::Boolean(value), 4)))
        } else if self.buffer.len() < 4 {
            Ok(None)
        } else {
            Err(RespError::Protocol("Invalid boolean".into()))
        }
    }

    fn parse_double(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        if let Some(crlf) = self.find_crlf(1) {
            let s = std::str::from_utf8(&self.buffer[1..crlf])
                .map_err(|e| RespError::Protocol(format!("Invalid UTF-8: {}", e)))?;
            let n: f64 = s
                .parse()
                .map_err(|e| RespError::Protocol(format!("Invalid double: {}", e)))?;
            Ok(Some((RespValue::Double(n), crlf + 2)))
        } else {
            Ok(None)
        }
    }

    fn parse_map(&self) -> Result<Option<(RespValue, usize)>, RespError> {
        let crlf = match self.find_crlf(1) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len_str = std::str::from_utf8(&self.buffer[1..crlf])
            .map_err(|e| RespError::Protocol(format!("Invalid UTF-8: {}", e)))?;
        let len: i64 = len_str
            .parse()
            .map_err(|e| RespError::Protocol(format!("Invalid length: {}", e)))?;

        if len == -1 {
            return Ok(Some((RespValue::Null, crlf + 2)));
        }

        let len = len as usize;
        let mut items = Vec::with_capacity(len);
        let mut pos = crlf + 2;

        for _ in 0..len {
            // Parse key
            let sub_buffer = &self.buffer[pos..];
            let sub_parser = RespParser {
                buffer: BytesMut::from(sub_buffer),
            };

            let (key, consumed) = match sub_parser.parse_value()? {
                Some(v) => v,
                None => return Ok(None),
            };
            pos += consumed;

            // Parse value
            let sub_buffer = &self.buffer[pos..];
            let sub_parser = RespParser {
                buffer: BytesMut::from(sub_buffer),
            };

            let (value, consumed) = match sub_parser.parse_value()? {
                Some(v) => v,
                None => return Ok(None),
            };
            pos += consumed;

            items.push((key, value));
        }

        Ok(Some((RespValue::Map(items), pos)))
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// RESP Encoder
// ============================================================================

/// Encode a RESP value to bytes
pub fn encode_resp(value: &RespValue) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_value(value, &mut buf);
    buf
}

fn encode_value(value: &RespValue, buf: &mut Vec<u8>) {
    match value {
        RespValue::SimpleString(s) => {
            buf.push(b'+');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Error(s) => {
            buf.push(b'-');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            buf.push(b':');
            buf.extend_from_slice(n.to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(data) => {
            buf.push(b'$');
            buf.extend_from_slice(data.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Array(items) => {
            buf.push(b'*');
            buf.extend_from_slice(items.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            for item in items {
                encode_value(item, buf);
            }
        }
        RespValue::Null => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        RespValue::Boolean(b) => {
            buf.push(b'#');
            buf.push(if *b { b't' } else { b'f' });
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Double(n) => {
            buf.push(b',');
            buf.extend_from_slice(n.to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Map(items) => {
            buf.push(b'%');
            buf.extend_from_slice(items.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            for (key, value) in items {
                encode_value(key, buf);
                encode_value(value, buf);
            }
        }
    }
}

// ============================================================================
// Error Types
// ============================================================================

/// RESP protocol errors
#[derive(Debug, thiserror::Error)]
pub enum RespError {
    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Command error: {0}")]
    Command(String),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Wrong type")]
    WrongType,

    #[error("Connection closed")]
    ConnectionClosed,
}

// ============================================================================
// Key-Value Store Trait
// ============================================================================

/// Trait for RESP command execution
#[async_trait]
pub trait RespExecutor: Send + Sync {
    /// String commands
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, RespError>;
    async fn set(&self, key: &[u8], value: &[u8], options: SetOptions) -> Result<(), RespError>;
    async fn del(&self, keys: &[&[u8]]) -> Result<i64, RespError>;
    async fn exists(&self, keys: &[&[u8]]) -> Result<i64, RespError>;
    async fn incr(&self, key: &[u8]) -> Result<i64, RespError>;
    async fn decr(&self, key: &[u8]) -> Result<i64, RespError>;
    async fn mget(&self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>, RespError>;
    async fn mset(&self, pairs: &[(&[u8], &[u8])]) -> Result<(), RespError>;

    /// Key commands
    async fn keys(&self, pattern: &[u8]) -> Result<Vec<Bytes>, RespError>;
    async fn key_type(&self, key: &[u8]) -> Result<String, RespError>;
    async fn expire(&self, key: &[u8], seconds: i64) -> Result<bool, RespError>;
    async fn ttl(&self, key: &[u8]) -> Result<i64, RespError>;
    async fn persist(&self, key: &[u8]) -> Result<bool, RespError>;

    /// Hash commands
    async fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, RespError>;
    async fn hset(&self, key: &[u8], pairs: &[(&[u8], &[u8])]) -> Result<i64, RespError>;
    async fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> Result<i64, RespError>;
    async fn hgetall(&self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, RespError>;
    async fn hkeys(&self, key: &[u8]) -> Result<Vec<Bytes>, RespError>;
    async fn hvals(&self, key: &[u8]) -> Result<Vec<Bytes>, RespError>;
    async fn hlen(&self, key: &[u8]) -> Result<i64, RespError>;
    async fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool, RespError>;

    /// List commands
    async fn lpush(&self, key: &[u8], values: &[&[u8]]) -> Result<i64, RespError>;
    async fn rpush(&self, key: &[u8], values: &[&[u8]]) -> Result<i64, RespError>;
    async fn lpop(&self, key: &[u8], count: Option<i64>) -> Result<Vec<Bytes>, RespError>;
    async fn rpop(&self, key: &[u8], count: Option<i64>) -> Result<Vec<Bytes>, RespError>;
    async fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Bytes>, RespError>;
    async fn llen(&self, key: &[u8]) -> Result<i64, RespError>;
    async fn lindex(&self, key: &[u8], index: i64) -> Result<Option<Bytes>, RespError>;

    /// Set commands
    async fn sadd(&self, key: &[u8], members: &[&[u8]]) -> Result<i64, RespError>;
    async fn srem(&self, key: &[u8], members: &[&[u8]]) -> Result<i64, RespError>;
    async fn smembers(&self, key: &[u8]) -> Result<Vec<Bytes>, RespError>;
    async fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool, RespError>;
    async fn scard(&self, key: &[u8]) -> Result<i64, RespError>;

    /// Sorted set commands
    async fn zadd(&self, key: &[u8], members: &[(f64, &[u8])]) -> Result<i64, RespError>;
    async fn zrem(&self, key: &[u8], members: &[&[u8]]) -> Result<i64, RespError>;
    async fn zrange(&self, key: &[u8], start: i64, stop: i64, with_scores: bool) -> Result<Vec<(Bytes, Option<f64>)>, RespError>;
    async fn zscore(&self, key: &[u8], member: &[u8]) -> Result<Option<f64>, RespError>;
    async fn zcard(&self, key: &[u8]) -> Result<i64, RespError>;

    /// Server commands
    async fn dbsize(&self) -> Result<i64, RespError>;
    async fn flushdb(&self) -> Result<(), RespError>;
    async fn info(&self, section: Option<&str>) -> Result<String, RespError>;

    /// Authentication
    async fn auth(&self, password: &str, username: Option<&str>) -> Result<(), RespError>;
}

/// SET command options
#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    /// Expiration in seconds
    pub ex: Option<i64>,
    /// Expiration in milliseconds
    pub px: Option<i64>,
    /// Set if key exists
    pub xx: bool,
    /// Set if key does not exist
    pub nx: bool,
    /// Keep existing TTL
    pub keepttl: bool,
    /// Get old value
    pub get: bool,
}

// ============================================================================
// RESP Connection Handler
// ============================================================================

/// RESP protocol handler
pub struct RespHandler<E: RespExecutor> {
    executor: Arc<E>,
    password: Option<String>,
    tls_acceptor: Option<TlsAcceptor>,
}

impl<E: RespExecutor> RespHandler<E> {
    /// Create a new RESP handler
    pub fn new(executor: Arc<E>) -> Self {
        Self {
            executor,
            password: None,
            tls_acceptor: None,
        }
    }

    /// Set authentication password
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Configure TLS for this handler
    pub fn with_tls(mut self, tls_config: &TlsConfig) -> Self {
        match tls_config.build_acceptor() {
            Ok(acceptor) => {
                self.tls_acceptor = Some(acceptor);
            }
            Err(e) => {
                error!("Failed to build TLS acceptor: {}", e);
            }
        }
        self
    }

    /// Check if TLS is available
    pub fn tls_available(&self) -> bool {
        self.tls_acceptor.is_some()
    }

    /// Handle a client connection (plain or upgradeable)
    pub async fn handle_connection(&self, stream: TcpStream) -> Result<(), RespError> {
        let peer_addr = stream.peer_addr().ok();
        info!("RESP connection from {:?}", peer_addr);

        let mut conn = RespConnection::new(MaybeTlsStream::Plain(stream), self.executor.clone());
        conn.require_auth = self.password.is_some();

        self.run_connection_loop(&mut conn, peer_addr).await
    }

    /// Handle a TLS connection (TLS from the start)
    pub async fn handle_tls_connection(&self, stream: TcpStream) -> Result<(), RespError> {
        let peer_addr = stream.peer_addr().ok();

        let acceptor = self.tls_acceptor.as_ref()
            .ok_or_else(|| RespError::Protocol("TLS not configured".into()))?;

        // Perform TLS handshake
        let tls_stream = acceptor.accept(stream).await
            .map_err(|e| RespError::Protocol(format!("TLS handshake failed: {}", e)))?;

        info!("RESP TLS connection from {:?}", peer_addr);

        let mut conn = RespConnection::new(MaybeTlsStream::Tls(tls_stream), self.executor.clone());
        conn.require_auth = self.password.is_some();

        self.run_connection_loop(&mut conn, peer_addr).await
    }

    /// Run the command loop for a connection
    async fn run_connection_loop(
        &self,
        conn: &mut RespConnection<E>,
        peer_addr: Option<std::net::SocketAddr>,
    ) -> Result<(), RespError> {
        const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
        loop {
            match tokio::time::timeout(IDLE_TIMEOUT, conn.process_command()).await {
                Ok(Ok(true)) => continue,
                Ok(Ok(false)) => {
                    debug!("Client {:?} disconnected", peer_addr);
                    break;
                }
                Ok(Err(RespError::ConnectionClosed)) => {
                    debug!("Client {:?} closed connection", peer_addr);
                    break;
                }
                Ok(Err(e)) => {
                    error!("Error handling RESP command: {}", e);
                    let _ = conn.write_response(RespValue::error(format!("ERR {}", e))).await;
                }
                Err(_) => {
                    debug!("RESP client {:?} idle for {:?}, closing", peer_addr, IDLE_TIMEOUT);
                    break;
                }
            }
        }

        Ok(())
    }
}

/// RESP connection state
pub struct RespConnection<E: RespExecutor> {
    stream: MaybeTlsStream<TcpStream>,
    parser: RespParser,
    executor: Arc<E>,
    authenticated: bool,
    require_auth: bool,
    selected_db: u32,
}

impl<E: RespExecutor> RespConnection<E> {
    /// Create a new connection with a MaybeTlsStream
    pub fn new(stream: MaybeTlsStream<TcpStream>, executor: Arc<E>) -> Self {
        Self {
            stream,
            parser: RespParser::new(),
            executor,
            authenticated: false,
            require_auth: false,
            selected_db: 0,
        }
    }

    /// Create a new connection from a plain TcpStream
    pub fn from_plain(stream: TcpStream, executor: Arc<E>) -> Self {
        Self::new(MaybeTlsStream::Plain(stream), executor)
    }

    /// Process a single command
    pub async fn process_command(&mut self) -> Result<bool, RespError> {
        // Read data
        let mut buf = [0u8; 4096];
        let n = self.stream.read(&mut buf).await?;

        if n == 0 {
            return Err(RespError::ConnectionClosed);
        }

        self.parser.extend(&buf[..n]);

        // Try to parse commands
        while let Some(value) = self.parser.parse()? {
            let response = self.execute_command(value).await?;
            self.write_response(response).await?;
        }

        Ok(true)
    }

    /// Write a response
    pub async fn write_response(&mut self, value: RespValue) -> Result<(), RespError> {
        let encoded = encode_resp(&value);
        self.stream.write_all(&encoded).await?;
        Ok(())
    }

    /// Execute a command
    async fn execute_command(&mut self, value: RespValue) -> Result<RespValue, RespError> {
        let args = match value {
            RespValue::Array(items) => items,
            _ => return Ok(RespValue::error("ERR invalid command format")),
        };

        if args.is_empty() {
            return Ok(RespValue::error("ERR empty command"));
        }

        let cmd = match args[0].as_str() {
            Some(s) => s.to_uppercase(),
            None => return Ok(RespValue::error("ERR invalid command")),
        };

        // Check authentication for commands that require it
        if self.require_auth && !self.authenticated {
            if cmd != "AUTH" && cmd != "PING" && cmd != "QUIT" {
                return Ok(RespValue::error("NOAUTH Authentication required"));
            }
        }

        // Dispatch command
        match cmd.as_str() {
            // Connection commands
            "PING" => self.cmd_ping(&args[1..]).await,
            "ECHO" => self.cmd_echo(&args[1..]).await,
            "AUTH" => self.cmd_auth(&args[1..]).await,
            "SELECT" => self.cmd_select(&args[1..]).await,
            "QUIT" => Ok(RespValue::ok()),
            "COMMAND" => self.cmd_command(&args[1..]).await,

            // String commands
            "GET" => self.cmd_get(&args[1..]).await,
            "SET" => self.cmd_set(&args[1..]).await,
            "MGET" => self.cmd_mget(&args[1..]).await,
            "MSET" => self.cmd_mset(&args[1..]).await,
            "INCR" => self.cmd_incr(&args[1..]).await,
            "DECR" => self.cmd_decr(&args[1..]).await,
            "INCRBY" => self.cmd_incrby(&args[1..]).await,
            "DECRBY" => self.cmd_decrby(&args[1..]).await,
            "APPEND" => self.cmd_append(&args[1..]).await,
            "STRLEN" => self.cmd_strlen(&args[1..]).await,
            "GETSET" => self.cmd_getset(&args[1..]).await,
            "SETNX" => self.cmd_setnx(&args[1..]).await,
            "SETEX" => self.cmd_setex(&args[1..]).await,

            // Key commands
            "DEL" => self.cmd_del(&args[1..]).await,
            "EXISTS" => self.cmd_exists(&args[1..]).await,
            "KEYS" => self.cmd_keys(&args[1..]).await,
            "TYPE" => self.cmd_type(&args[1..]).await,
            "EXPIRE" => self.cmd_expire(&args[1..]).await,
            "EXPIREAT" => self.cmd_expireat(&args[1..]).await,
            "TTL" => self.cmd_ttl(&args[1..]).await,
            "PTTL" => self.cmd_pttl(&args[1..]).await,
            "PERSIST" => self.cmd_persist(&args[1..]).await,
            "RENAME" => self.cmd_rename(&args[1..]).await,

            // Hash commands
            "HGET" => self.cmd_hget(&args[1..]).await,
            "HSET" => self.cmd_hset(&args[1..]).await,
            "HMGET" => self.cmd_hmget(&args[1..]).await,
            "HMSET" => self.cmd_hmset(&args[1..]).await,
            "HDEL" => self.cmd_hdel(&args[1..]).await,
            "HGETALL" => self.cmd_hgetall(&args[1..]).await,
            "HKEYS" => self.cmd_hkeys(&args[1..]).await,
            "HVALS" => self.cmd_hvals(&args[1..]).await,
            "HLEN" => self.cmd_hlen(&args[1..]).await,
            "HEXISTS" => self.cmd_hexists(&args[1..]).await,
            "HINCRBY" => self.cmd_hincrby(&args[1..]).await,

            // List commands
            "LPUSH" => self.cmd_lpush(&args[1..]).await,
            "RPUSH" => self.cmd_rpush(&args[1..]).await,
            "LPOP" => self.cmd_lpop(&args[1..]).await,
            "RPOP" => self.cmd_rpop(&args[1..]).await,
            "LRANGE" => self.cmd_lrange(&args[1..]).await,
            "LLEN" => self.cmd_llen(&args[1..]).await,
            "LINDEX" => self.cmd_lindex(&args[1..]).await,

            // Set commands
            "SADD" => self.cmd_sadd(&args[1..]).await,
            "SREM" => self.cmd_srem(&args[1..]).await,
            "SMEMBERS" => self.cmd_smembers(&args[1..]).await,
            "SISMEMBER" => self.cmd_sismember(&args[1..]).await,
            "SCARD" => self.cmd_scard(&args[1..]).await,

            // Sorted set commands
            "ZADD" => self.cmd_zadd(&args[1..]).await,
            "ZREM" => self.cmd_zrem(&args[1..]).await,
            "ZRANGE" => self.cmd_zrange(&args[1..]).await,
            "ZSCORE" => self.cmd_zscore(&args[1..]).await,
            "ZCARD" => self.cmd_zcard(&args[1..]).await,

            // Server commands
            "DBSIZE" => self.cmd_dbsize().await,
            "FLUSHDB" => self.cmd_flushdb().await,
            "FLUSHALL" => self.cmd_flushdb().await, // Same as FLUSHDB for now
            "INFO" => self.cmd_info(&args[1..]).await,

            _ => Ok(RespValue::error(format!("ERR unknown command '{}'", cmd))),
        }
    }

    // ========================================================================
    // Connection Commands
    // ========================================================================

    async fn cmd_ping(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            Ok(RespValue::SimpleString("PONG".to_string()))
        } else if let Some(msg) = args[0].as_bytes() {
            Ok(RespValue::bulk(Bytes::copy_from_slice(msg)))
        } else {
            Ok(RespValue::SimpleString("PONG".to_string()))
        }
    }

    async fn cmd_echo(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'echo' command"));
        }
        if let Some(msg) = args[0].as_bytes() {
            Ok(RespValue::bulk(Bytes::copy_from_slice(msg)))
        } else {
            Ok(RespValue::error("ERR invalid argument"))
        }
    }

    async fn cmd_auth(&mut self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'auth' command"));
        }

        let (username, password) = if args.len() >= 2 {
            (args[0].as_str(), args[1].as_str().unwrap_or(""))
        } else {
            (None, args[0].as_str().unwrap_or(""))
        };

        match self.executor.auth(password, username).await {
            Ok(()) => {
                self.authenticated = true;
                Ok(RespValue::ok())
            }
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    async fn cmd_select(&mut self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'select' command"));
        }

        let db = match args[0].as_integer() {
            Some(n) if n >= 0 && n < 16 => n as u32,
            _ => return Ok(RespValue::error("ERR invalid DB index")),
        };

        self.selected_db = db;
        Ok(RespValue::ok())
    }

    async fn cmd_command(&self, _args: &[RespValue]) -> Result<RespValue, RespError> {
        // Return simplified command info
        Ok(RespValue::array(vec![]))
    }

    // ========================================================================
    // String Commands
    // ========================================================================

    async fn cmd_get(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'get' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        match self.executor.get(key).await? {
            Some(value) => Ok(RespValue::bulk(value)),
            None => Ok(RespValue::Null),
        }
    }

    async fn cmd_set(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'set' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let value = match args[1].as_bytes() {
            Some(v) => v,
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        let mut options = SetOptions::default();
        let mut i = 2;

        while i < args.len() {
            let opt = args[i].as_str().map(|s| s.to_uppercase());
            match opt.as_deref() {
                Some("EX") => {
                    i += 1;
                    options.ex = args.get(i).and_then(|v| v.as_integer());
                }
                Some("PX") => {
                    i += 1;
                    options.px = args.get(i).and_then(|v| v.as_integer());
                }
                Some("NX") => options.nx = true,
                Some("XX") => options.xx = true,
                Some("KEEPTTL") => options.keepttl = true,
                Some("GET") => options.get = true,
                _ => {}
            }
            i += 1;
        }

        self.executor.set(key, value, options).await?;
        Ok(RespValue::ok())
    }

    async fn cmd_mget(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'mget' command"));
        }

        let keys: Vec<&[u8]> = args
            .iter()
            .filter_map(|v| v.as_bytes())
            .collect();

        let values = self.executor.mget(&keys).await?;
        let result: Vec<RespValue> = values
            .into_iter()
            .map(|v| match v {
                Some(data) => RespValue::bulk(data),
                None => RespValue::Null,
            })
            .collect();

        Ok(RespValue::array(result))
    }

    async fn cmd_mset(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 || args.len() % 2 != 0 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'mset' command"));
        }

        let mut pairs = Vec::new();
        for chunk in args.chunks(2) {
            let key = match chunk[0].as_bytes() {
                Some(k) => k,
                None => return Ok(RespValue::error("ERR invalid key")),
            };
            let value = match chunk[1].as_bytes() {
                Some(v) => v,
                None => return Ok(RespValue::error("ERR invalid value")),
            };
            pairs.push((key, value));
        }

        self.executor.mset(&pairs).await?;
        Ok(RespValue::ok())
    }

    async fn cmd_incr(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'incr' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let result = self.executor.incr(key).await?;
        Ok(RespValue::integer(result))
    }

    async fn cmd_decr(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'decr' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let result = self.executor.decr(key).await?;
        Ok(RespValue::integer(result))
    }

    async fn cmd_incrby(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'incrby' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let increment = match args[1].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        // Get current value, add increment, set new value
        let current = self.executor.get(key).await?.and_then(|v| {
            std::str::from_utf8(&v).ok()?.parse::<i64>().ok()
        }).unwrap_or(0);

        let new_value = current + increment;
        self.executor.set(key, new_value.to_string().as_bytes(), SetOptions::default()).await?;
        Ok(RespValue::integer(new_value))
    }

    async fn cmd_decrby(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'decrby' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let decrement = match args[1].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let current = self.executor.get(key).await?.and_then(|v| {
            std::str::from_utf8(&v).ok()?.parse::<i64>().ok()
        }).unwrap_or(0);

        let new_value = current - decrement;
        self.executor.set(key, new_value.to_string().as_bytes(), SetOptions::default()).await?;
        Ok(RespValue::integer(new_value))
    }

    async fn cmd_append(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'append' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let append_value = match args[1].as_bytes() {
            Some(v) => v,
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        let current = self.executor.get(key).await?.unwrap_or_default();
        let mut new_value = current.to_vec();
        new_value.extend_from_slice(append_value);
        let len = new_value.len() as i64;

        self.executor.set(key, &new_value, SetOptions::default()).await?;
        Ok(RespValue::integer(len))
    }

    async fn cmd_strlen(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'strlen' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let len = self.executor.get(key).await?.map(|v| v.len() as i64).unwrap_or(0);
        Ok(RespValue::integer(len))
    }

    async fn cmd_getset(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'getset' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let value = match args[1].as_bytes() {
            Some(v) => v,
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        let old = self.executor.get(key).await?;
        self.executor.set(key, value, SetOptions::default()).await?;

        match old {
            Some(v) => Ok(RespValue::bulk(v)),
            None => Ok(RespValue::Null),
        }
    }

    async fn cmd_setnx(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'setnx' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let value = match args[1].as_bytes() {
            Some(v) => v,
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        if self.executor.get(key).await?.is_some() {
            Ok(RespValue::integer(0))
        } else {
            self.executor.set(key, value, SetOptions::default()).await?;
            Ok(RespValue::integer(1))
        }
    }

    async fn cmd_setex(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'setex' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let seconds = match args[1].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let value = match args[2].as_bytes() {
            Some(v) => v,
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        self.executor.set(key, value, SetOptions { ex: Some(seconds), ..Default::default() }).await?;
        Ok(RespValue::ok())
    }

    // ========================================================================
    // Key Commands
    // ========================================================================

    async fn cmd_del(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'del' command"));
        }

        let keys: Vec<&[u8]> = args
            .iter()
            .filter_map(|v| v.as_bytes())
            .collect();

        let count = self.executor.del(&keys).await?;
        Ok(RespValue::integer(count))
    }

    async fn cmd_exists(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'exists' command"));
        }

        let keys: Vec<&[u8]> = args
            .iter()
            .filter_map(|v| v.as_bytes())
            .collect();

        let count = self.executor.exists(&keys).await?;
        Ok(RespValue::integer(count))
    }

    async fn cmd_keys(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        let pattern = args.first()
            .and_then(|v| v.as_bytes())
            .unwrap_or(b"*");

        let keys = self.executor.keys(pattern).await?;
        let result: Vec<RespValue> = keys
            .into_iter()
            .map(|k| RespValue::bulk(k))
            .collect();

        Ok(RespValue::array(result))
    }

    async fn cmd_type(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'type' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let type_str = self.executor.key_type(key).await?;
        Ok(RespValue::SimpleString(type_str))
    }

    async fn cmd_expire(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'expire' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let seconds = match args[1].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let result = self.executor.expire(key, seconds).await?;
        Ok(RespValue::integer(if result { 1 } else { 0 }))
    }

    async fn cmd_expireat(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'expireat' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let timestamp = match args[1].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        // Convert absolute timestamp to relative TTL
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let ttl = timestamp - now;

        let result = self.executor.expire(key, ttl).await?;
        Ok(RespValue::integer(if result { 1 } else { 0 }))
    }

    async fn cmd_ttl(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'ttl' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let ttl = self.executor.ttl(key).await?;
        Ok(RespValue::integer(ttl))
    }

    async fn cmd_pttl(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'pttl' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let ttl = self.executor.ttl(key).await?;
        // Convert to milliseconds
        Ok(RespValue::integer(if ttl > 0 { ttl * 1000 } else { ttl }))
    }

    async fn cmd_persist(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'persist' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let result = self.executor.persist(key).await?;
        Ok(RespValue::integer(if result { 1 } else { 0 }))
    }

    async fn cmd_rename(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'rename' command"));
        }

        let old_key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let new_key = match args[1].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        // Get value from old key
        let value = match self.executor.get(old_key).await? {
            Some(v) => v,
            None => return Ok(RespValue::error("ERR no such key")),
        };

        // Set new key and delete old key
        self.executor.set(new_key, &value, SetOptions::default()).await?;
        self.executor.del(&[old_key]).await?;

        Ok(RespValue::ok())
    }

    // ========================================================================
    // Hash Commands
    // ========================================================================

    async fn cmd_hget(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hget' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let field = match args[1].as_bytes() {
            Some(f) => f,
            None => return Ok(RespValue::error("ERR invalid field")),
        };

        match self.executor.hget(key, field).await? {
            Some(value) => Ok(RespValue::bulk(value)),
            None => Ok(RespValue::Null),
        }
    }

    async fn cmd_hset(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hset' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let mut pairs = Vec::new();
        for chunk in args[1..].chunks(2) {
            let field = match chunk[0].as_bytes() {
                Some(f) => f,
                None => return Ok(RespValue::error("ERR invalid field")),
            };
            let value = match chunk[1].as_bytes() {
                Some(v) => v,
                None => return Ok(RespValue::error("ERR invalid value")),
            };
            pairs.push((field, value));
        }

        let count = self.executor.hset(key, &pairs).await?;
        Ok(RespValue::integer(count))
    }

    async fn cmd_hmget(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hmget' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let mut results = Vec::new();
        for field_arg in &args[1..] {
            let field = match field_arg.as_bytes() {
                Some(f) => f,
                None => {
                    results.push(RespValue::Null);
                    continue;
                }
            };

            match self.executor.hget(key, field).await? {
                Some(v) => results.push(RespValue::bulk(v)),
                None => results.push(RespValue::Null),
            }
        }

        Ok(RespValue::array(results))
    }

    async fn cmd_hmset(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        // Same as HSET but returns OK instead of count
        self.cmd_hset(args).await?;
        Ok(RespValue::ok())
    }

    async fn cmd_hdel(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hdel' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let fields: Vec<&[u8]> = args[1..]
            .iter()
            .filter_map(|v| v.as_bytes())
            .collect();

        let count = self.executor.hdel(key, &fields).await?;
        Ok(RespValue::integer(count))
    }

    async fn cmd_hgetall(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hgetall' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let pairs = self.executor.hgetall(key).await?;
        let mut result = Vec::with_capacity(pairs.len() * 2);

        for (field, value) in pairs {
            result.push(RespValue::bulk(field));
            result.push(RespValue::bulk(value));
        }

        Ok(RespValue::array(result))
    }

    async fn cmd_hkeys(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hkeys' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let keys = self.executor.hkeys(key).await?;
        let result: Vec<RespValue> = keys.into_iter().map(|k| RespValue::bulk(k)).collect();
        Ok(RespValue::array(result))
    }

    async fn cmd_hvals(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hvals' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let vals = self.executor.hvals(key).await?;
        let result: Vec<RespValue> = vals.into_iter().map(|v| RespValue::bulk(v)).collect();
        Ok(RespValue::array(result))
    }

    async fn cmd_hlen(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hlen' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let len = self.executor.hlen(key).await?;
        Ok(RespValue::integer(len))
    }

    async fn cmd_hexists(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hexists' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let field = match args[1].as_bytes() {
            Some(f) => f,
            None => return Ok(RespValue::error("ERR invalid field")),
        };

        let exists = self.executor.hexists(key, field).await?;
        Ok(RespValue::integer(if exists { 1 } else { 0 }))
    }

    async fn cmd_hincrby(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'hincrby' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let field = match args[1].as_bytes() {
            Some(f) => f,
            None => return Ok(RespValue::error("ERR invalid field")),
        };

        let increment = match args[2].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        // Get current value
        let current = self.executor.hget(key, field).await?.and_then(|v| {
            std::str::from_utf8(&v).ok()?.parse::<i64>().ok()
        }).unwrap_or(0);

        let new_value = current + increment;
        self.executor.hset(key, &[(field, new_value.to_string().as_bytes())]).await?;
        Ok(RespValue::integer(new_value))
    }

    // ========================================================================
    // List Commands
    // ========================================================================

    async fn cmd_lpush(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'lpush' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let values: Vec<&[u8]> = args[1..]
            .iter()
            .filter_map(|v| v.as_bytes())
            .collect();

        let len = self.executor.lpush(key, &values).await?;
        Ok(RespValue::integer(len))
    }

    async fn cmd_rpush(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'rpush' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let values: Vec<&[u8]> = args[1..]
            .iter()
            .filter_map(|v| v.as_bytes())
            .collect();

        let len = self.executor.rpush(key, &values).await?;
        Ok(RespValue::integer(len))
    }

    async fn cmd_lpop(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'lpop' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let count = args.get(1).and_then(|v| v.as_integer());
        let values = self.executor.lpop(key, count).await?;

        if values.is_empty() {
            Ok(RespValue::Null)
        } else if count.is_some() {
            Ok(RespValue::array(values.into_iter().map(|v| RespValue::bulk(v)).collect()))
        } else {
            Ok(RespValue::bulk(values.into_iter().next().expect("lpop: values verified non-empty")))
        }
    }

    async fn cmd_rpop(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'rpop' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let count = args.get(1).and_then(|v| v.as_integer());
        let values = self.executor.rpop(key, count).await?;

        if values.is_empty() {
            Ok(RespValue::Null)
        } else if count.is_some() {
            Ok(RespValue::array(values.into_iter().map(|v| RespValue::bulk(v)).collect()))
        } else {
            Ok(RespValue::bulk(values.into_iter().next().expect("rpop: values verified non-empty")))
        }
    }

    async fn cmd_lrange(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'lrange' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let start = match args[1].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let stop = match args[2].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let values = self.executor.lrange(key, start, stop).await?;
        let result: Vec<RespValue> = values.into_iter().map(|v| RespValue::bulk(v)).collect();
        Ok(RespValue::array(result))
    }

    async fn cmd_llen(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'llen' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let len = self.executor.llen(key).await?;
        Ok(RespValue::integer(len))
    }

    async fn cmd_lindex(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'lindex' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let index = match args[1].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        match self.executor.lindex(key, index).await? {
            Some(v) => Ok(RespValue::bulk(v)),
            None => Ok(RespValue::Null),
        }
    }

    // ========================================================================
    // Set Commands
    // ========================================================================

    async fn cmd_sadd(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'sadd' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let members: Vec<&[u8]> = args[1..]
            .iter()
            .filter_map(|v| v.as_bytes())
            .collect();

        let count = self.executor.sadd(key, &members).await?;
        Ok(RespValue::integer(count))
    }

    async fn cmd_srem(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'srem' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let members: Vec<&[u8]> = args[1..]
            .iter()
            .filter_map(|v| v.as_bytes())
            .collect();

        let count = self.executor.srem(key, &members).await?;
        Ok(RespValue::integer(count))
    }

    async fn cmd_smembers(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'smembers' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let members = self.executor.smembers(key).await?;
        let result: Vec<RespValue> = members.into_iter().map(|m| RespValue::bulk(m)).collect();
        Ok(RespValue::array(result))
    }

    async fn cmd_sismember(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'sismember' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let member = match args[1].as_bytes() {
            Some(m) => m,
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        let is_member = self.executor.sismember(key, member).await?;
        Ok(RespValue::integer(if is_member { 1 } else { 0 }))
    }

    async fn cmd_scard(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'scard' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let count = self.executor.scard(key).await?;
        Ok(RespValue::integer(count))
    }

    // ========================================================================
    // Sorted Set Commands
    // ========================================================================

    async fn cmd_zadd(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zadd' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let mut members = Vec::new();
        for chunk in args[1..].chunks(2) {
            let score = match chunk[0].as_str().and_then(|s| s.parse::<f64>().ok())
                .or_else(|| chunk[0].as_integer().map(|n| n as f64)) {
                Some(s) => s,
                None => return Ok(RespValue::error("ERR value is not a valid float")),
            };
            let member = match chunk[1].as_bytes() {
                Some(m) => m,
                None => return Ok(RespValue::error("ERR invalid member")),
            };
            members.push((score, member));
        }

        let count = self.executor.zadd(key, &members).await?;
        Ok(RespValue::integer(count))
    }

    async fn cmd_zrem(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrem' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let members: Vec<&[u8]> = args[1..]
            .iter()
            .filter_map(|v| v.as_bytes())
            .collect();

        let count = self.executor.zrem(key, &members).await?;
        Ok(RespValue::integer(count))
    }

    async fn cmd_zrange(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrange' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let start = match args[1].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let stop = match args[2].as_integer() {
            Some(n) => n,
            None => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let with_scores = args.get(3)
            .and_then(|v| v.as_str())
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false);

        let members = self.executor.zrange(key, start, stop, with_scores).await?;

        let result: Vec<RespValue> = if with_scores {
            members.into_iter()
                .flat_map(|(m, s)| {
                    vec![
                        RespValue::bulk(m),
                        RespValue::bulk_string(s.unwrap_or(0.0).to_string()),
                    ]
                })
                .collect()
        } else {
            members.into_iter()
                .map(|(m, _)| RespValue::bulk(m))
                .collect()
        };

        Ok(RespValue::array(result))
    }

    async fn cmd_zscore(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zscore' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let member = match args[1].as_bytes() {
            Some(m) => m,
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        match self.executor.zscore(key, member).await? {
            Some(score) => Ok(RespValue::bulk_string(score.to_string())),
            None => Ok(RespValue::Null),
        }
    }

    async fn cmd_zcard(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zcard' command"));
        }

        let key = match args[0].as_bytes() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let count = self.executor.zcard(key).await?;
        Ok(RespValue::integer(count))
    }

    // ========================================================================
    // Server Commands
    // ========================================================================

    async fn cmd_dbsize(&self) -> Result<RespValue, RespError> {
        let size = self.executor.dbsize().await?;
        Ok(RespValue::integer(size))
    }

    async fn cmd_flushdb(&self) -> Result<RespValue, RespError> {
        self.executor.flushdb().await?;
        Ok(RespValue::ok())
    }

    async fn cmd_info(&self, args: &[RespValue]) -> Result<RespValue, RespError> {
        let section = args.first().and_then(|v| v.as_str());
        let info = self.executor.info(section).await?;
        Ok(RespValue::bulk_string(info))
    }
}

// ============================================================================
// RESP Server
// ============================================================================

/// RESP protocol server
pub struct RespServer<E: RespExecutor + 'static> {
    handler: Arc<RespHandler<E>>,
    addr: String,
    max_connections: usize,
    /// If true, all connections require TLS (TLS from the start)
    require_tls: bool,
}

impl<E: RespExecutor + 'static> RespServer<E> {
    /// Create a new RESP server
    pub fn new(executor: Arc<E>, addr: impl Into<String>) -> Self {
        Self {
            handler: Arc::new(RespHandler::new(executor)),
            addr: addr.into(),
            max_connections: 1000,
            require_tls: false,
        }
    }

    /// Set authentication password
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.handler = Arc::new(RespHandler {
            executor: self.handler.executor.clone(),
            password: Some(password.into()),
            tls_acceptor: self.handler.tls_acceptor.clone(),
        });
        self
    }

    /// Configure TLS for this server
    pub fn with_tls(mut self, tls_config: &TlsConfig) -> Self {
        self.handler = Arc::new(RespHandler {
            executor: self.handler.executor.clone(),
            password: self.handler.password.clone(),
            tls_acceptor: match tls_config.build_acceptor() {
                Ok(acceptor) => Some(acceptor),
                Err(e) => {
                    error!("Failed to build TLS acceptor: {}", e);
                    None
                }
            },
        });
        self.require_tls = true;
        self
    }

    /// Set maximum number of concurrent connections
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Check if TLS is available
    pub fn tls_available(&self) -> bool {
        self.handler.tls_available()
    }

    /// Run the server
    pub async fn run(self) -> Result<(), RespError> {
        let listener = TcpListener::bind(&self.addr).await?;
        let limiter = ConnectionLimiter::new(self.max_connections);
        let tls_status = if self.require_tls && self.tls_available() {
            ", TLS required"
        } else if self.tls_available() {
            ", TLS available"
        } else {
            ""
        };
        info!(
            "RESP server listening on {} (max_connections: {}{})",
            self.addr, self.max_connections, tls_status
        );

        loop {
            let (stream, peer_addr) = listener.accept().await?;

            // Try to acquire a connection permit
            let permit = match limiter.try_acquire() {
                Some(permit) => permit,
                None => {
                    warn!(
                        "Rejecting RESP connection from {}: max connections ({}) reached",
                        peer_addr, self.max_connections
                    );
                    drop(stream);
                    continue;
                }
            };

            debug!(
                "RESP connection from {} (active: {}/{})",
                peer_addr,
                limiter.active_connections(),
                limiter.max_connections()
            );

            let handler = self.handler.clone();
            let require_tls = self.require_tls;
            tokio::spawn(async move {
                let _permit = permit;
                let result = if require_tls && handler.tls_available() {
                    handler.handle_tls_connection(stream).await
                } else {
                    handler.handle_connection(stream).await
                };
                if let Err(e) = result {
                    error!("Connection error from {:?}: {}", peer_addr, e);
                }
            });
        }
    }
}

// ============================================================================
// In-Memory KV Store (Reference Implementation)
// ============================================================================

/// In-memory key-value store for testing
pub struct InMemoryStore {
    data: RwLock<HashMap<Bytes, StoreValue>>,
    password: Option<String>,
}

#[derive(Clone)]
enum StoreValue {
    String(Bytes),
    List(Vec<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    Set(std::collections::HashSet<Bytes>),
    ZSet(Vec<(f64, Bytes)>),
}

impl InMemoryStore {
    /// Create a new in-memory store
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            password: None,
        }
    }

    /// Set authentication password
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RespExecutor for InMemoryStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::String(v)) => Ok(Some(v.clone())),
            Some(_) => Err(RespError::WrongType),
            None => Ok(None),
        }
    }

    async fn set(&self, key: &[u8], value: &[u8], options: SetOptions) -> Result<(), RespError> {
        let mut data = self.data.write().await;

        if options.nx && data.contains_key(key) {
            return Ok(());
        }
        if options.xx && !data.contains_key(key) {
            return Ok(());
        }

        data.insert(
            Bytes::copy_from_slice(key),
            StoreValue::String(Bytes::copy_from_slice(value)),
        );
        Ok(())
    }

    async fn del(&self, keys: &[&[u8]]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        let mut count = 0;
        for key in keys {
            if data.remove(*key).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn exists(&self, keys: &[&[u8]]) -> Result<i64, RespError> {
        let data = self.data.read().await;
        let count = keys.iter().filter(|k| data.contains_key(&Bytes::copy_from_slice(*k))).count() as i64;
        Ok(count)
    }

    async fn incr(&self, key: &[u8]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        let value = match data.get(key) {
            Some(StoreValue::String(v)) => {
                std::str::from_utf8(v)
                    .map_err(|_| RespError::Command("ERR value is not an integer".into()))?
                    .parse::<i64>()
                    .map_err(|_| RespError::Command("ERR value is not an integer".into()))?
            }
            Some(_) => return Err(RespError::WrongType),
            None => 0,
        };

        let new_value = value + 1;
        data.insert(
            Bytes::copy_from_slice(key),
            StoreValue::String(Bytes::copy_from_slice(new_value.to_string().as_bytes())),
        );
        Ok(new_value)
    }

    async fn decr(&self, key: &[u8]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        let value = match data.get(key) {
            Some(StoreValue::String(v)) => {
                std::str::from_utf8(v)
                    .map_err(|_| RespError::Command("ERR value is not an integer".into()))?
                    .parse::<i64>()
                    .map_err(|_| RespError::Command("ERR value is not an integer".into()))?
            }
            Some(_) => return Err(RespError::WrongType),
            None => 0,
        };

        let new_value = value - 1;
        data.insert(
            Bytes::copy_from_slice(key),
            StoreValue::String(Bytes::copy_from_slice(new_value.to_string().as_bytes())),
        );
        Ok(new_value)
    }

    async fn mget(&self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>, RespError> {
        let data = self.data.read().await;
        Ok(keys
            .iter()
            .map(|k| match data.get(*k) {
                Some(StoreValue::String(v)) => Some(v.clone()),
                _ => None,
            })
            .collect())
    }

    async fn mset(&self, pairs: &[(&[u8], &[u8])]) -> Result<(), RespError> {
        let mut data = self.data.write().await;
        for (key, value) in pairs {
            data.insert(
                Bytes::copy_from_slice(key),
                StoreValue::String(Bytes::copy_from_slice(value)),
            );
        }
        Ok(())
    }

    async fn keys(&self, pattern: &[u8]) -> Result<Vec<Bytes>, RespError> {
        let data = self.data.read().await;
        let pattern_str = std::str::from_utf8(pattern).unwrap_or("*");

        // Simple glob matching
        let keys: Vec<Bytes> = if pattern_str == "*" {
            data.keys().cloned().collect()
        } else {
            data.keys()
                .filter(|k| {
                    let key_str = std::str::from_utf8(k).unwrap_or("");
                    glob_match(pattern_str, key_str)
                })
                .cloned()
                .collect()
        };

        Ok(keys)
    }

    async fn key_type(&self, key: &[u8]) -> Result<String, RespError> {
        let data = self.data.read().await;
        let type_str = match data.get(key) {
            Some(StoreValue::String(_)) => "string",
            Some(StoreValue::List(_)) => "list",
            Some(StoreValue::Hash(_)) => "hash",
            Some(StoreValue::Set(_)) => "set",
            Some(StoreValue::ZSet(_)) => "zset",
            None => "none",
        };
        Ok(type_str.to_string())
    }

    async fn expire(&self, _key: &[u8], _seconds: i64) -> Result<bool, RespError> {
        // TTL not implemented in simple store
        Ok(false)
    }

    async fn ttl(&self, _key: &[u8]) -> Result<i64, RespError> {
        // -1 means no TTL
        Ok(-1)
    }

    async fn persist(&self, _key: &[u8]) -> Result<bool, RespError> {
        Ok(false)
    }

    async fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::Hash(h)) => Ok(h.get(field).cloned()),
            Some(_) => Err(RespError::WrongType),
            None => Ok(None),
        }
    }

    async fn hset(&self, key: &[u8], pairs: &[(&[u8], &[u8])]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        let hash = data
            .entry(Bytes::copy_from_slice(key))
            .or_insert_with(|| StoreValue::Hash(HashMap::new()));

        match hash {
            StoreValue::Hash(h) => {
                let mut count = 0;
                for (field, value) in pairs {
                    if h.insert(
                        Bytes::copy_from_slice(field),
                        Bytes::copy_from_slice(value),
                    ).is_none() {
                        count += 1;
                    }
                }
                Ok(count)
            }
            _ => Err(RespError::WrongType),
        }
    }

    async fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        match data.get_mut(key) {
            Some(StoreValue::Hash(h)) => {
                let mut count = 0;
                for field in fields {
                    if h.remove(*field).is_some() {
                        count += 1;
                    }
                }
                Ok(count)
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(0),
        }
    }

    async fn hgetall(&self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::Hash(h)) => {
                Ok(h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    async fn hkeys(&self, key: &[u8]) -> Result<Vec<Bytes>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::Hash(h)) => Ok(h.keys().cloned().collect()),
            Some(_) => Err(RespError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    async fn hvals(&self, key: &[u8]) -> Result<Vec<Bytes>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::Hash(h)) => Ok(h.values().cloned().collect()),
            Some(_) => Err(RespError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    async fn hlen(&self, key: &[u8]) -> Result<i64, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::Hash(h)) => Ok(h.len() as i64),
            Some(_) => Err(RespError::WrongType),
            None => Ok(0),
        }
    }

    async fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::Hash(h)) => Ok(h.contains_key(field)),
            Some(_) => Err(RespError::WrongType),
            None => Ok(false),
        }
    }

    async fn lpush(&self, key: &[u8], values: &[&[u8]]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        let list = data
            .entry(Bytes::copy_from_slice(key))
            .or_insert_with(|| StoreValue::List(Vec::new()));

        match list {
            StoreValue::List(l) => {
                for value in values.iter().rev() {
                    l.insert(0, Bytes::copy_from_slice(value));
                }
                Ok(l.len() as i64)
            }
            _ => Err(RespError::WrongType),
        }
    }

    async fn rpush(&self, key: &[u8], values: &[&[u8]]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        let list = data
            .entry(Bytes::copy_from_slice(key))
            .or_insert_with(|| StoreValue::List(Vec::new()));

        match list {
            StoreValue::List(l) => {
                for value in values {
                    l.push(Bytes::copy_from_slice(value));
                }
                Ok(l.len() as i64)
            }
            _ => Err(RespError::WrongType),
        }
    }

    async fn lpop(&self, key: &[u8], count: Option<i64>) -> Result<Vec<Bytes>, RespError> {
        let mut data = self.data.write().await;
        match data.get_mut(key) {
            Some(StoreValue::List(l)) => {
                let count = count.unwrap_or(1) as usize;
                let mut result = Vec::with_capacity(count);
                for _ in 0..count {
                    if l.is_empty() {
                        break;
                    }
                    result.push(l.remove(0));
                }
                Ok(result)
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    async fn rpop(&self, key: &[u8], count: Option<i64>) -> Result<Vec<Bytes>, RespError> {
        let mut data = self.data.write().await;
        match data.get_mut(key) {
            Some(StoreValue::List(l)) => {
                let count = count.unwrap_or(1) as usize;
                let mut result = Vec::with_capacity(count);
                for _ in 0..count {
                    if let Some(v) = l.pop() {
                        result.push(v);
                    } else {
                        break;
                    }
                }
                Ok(result)
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    async fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Bytes>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::List(l)) => {
                let len = l.len() as i64;
                let start = if start < 0 { (len + start).max(0) } else { start } as usize;
                let stop = if stop < 0 { (len + stop).max(0) } else { stop } as usize;

                if start > stop || start >= l.len() {
                    return Ok(Vec::new());
                }

                let end = (stop + 1).min(l.len());
                Ok(l[start..end].to_vec())
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    async fn llen(&self, key: &[u8]) -> Result<i64, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::List(l)) => Ok(l.len() as i64),
            Some(_) => Err(RespError::WrongType),
            None => Ok(0),
        }
    }

    async fn lindex(&self, key: &[u8], index: i64) -> Result<Option<Bytes>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::List(l)) => {
                let idx = if index < 0 {
                    (l.len() as i64 + index) as usize
                } else {
                    index as usize
                };
                Ok(l.get(idx).cloned())
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(None),
        }
    }

    async fn sadd(&self, key: &[u8], members: &[&[u8]]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        let set = data
            .entry(Bytes::copy_from_slice(key))
            .or_insert_with(|| StoreValue::Set(std::collections::HashSet::new()));

        match set {
            StoreValue::Set(s) => {
                let mut count = 0;
                for member in members {
                    if s.insert(Bytes::copy_from_slice(member)) {
                        count += 1;
                    }
                }
                Ok(count)
            }
            _ => Err(RespError::WrongType),
        }
    }

    async fn srem(&self, key: &[u8], members: &[&[u8]]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        match data.get_mut(key) {
            Some(StoreValue::Set(s)) => {
                let mut count = 0;
                for member in members {
                    if s.remove(*member) {
                        count += 1;
                    }
                }
                Ok(count)
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(0),
        }
    }

    async fn smembers(&self, key: &[u8]) -> Result<Vec<Bytes>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::Set(s)) => Ok(s.iter().cloned().collect()),
            Some(_) => Err(RespError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    async fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::Set(s)) => Ok(s.contains(member)),
            Some(_) => Err(RespError::WrongType),
            None => Ok(false),
        }
    }

    async fn scard(&self, key: &[u8]) -> Result<i64, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::Set(s)) => Ok(s.len() as i64),
            Some(_) => Err(RespError::WrongType),
            None => Ok(0),
        }
    }

    async fn zadd(&self, key: &[u8], members: &[(f64, &[u8])]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        let zset = data
            .entry(Bytes::copy_from_slice(key))
            .or_insert_with(|| StoreValue::ZSet(Vec::new()));

        match zset {
            StoreValue::ZSet(z) => {
                let mut count = 0;
                for (score, member) in members {
                    let member_bytes = Bytes::copy_from_slice(member);
                    // Remove existing entry if present
                    let existed = z.iter().position(|(_, m)| m == &member_bytes);
                    if let Some(pos) = existed {
                        z.remove(pos);
                    } else {
                        count += 1;
                    }
                    // Insert with new score
                    let insert_pos = z.iter().position(|(s, _)| s > score).unwrap_or(z.len());
                    z.insert(insert_pos, (*score, member_bytes));
                }
                Ok(count)
            }
            _ => Err(RespError::WrongType),
        }
    }

    async fn zrem(&self, key: &[u8], members: &[&[u8]]) -> Result<i64, RespError> {
        let mut data = self.data.write().await;
        match data.get_mut(key) {
            Some(StoreValue::ZSet(z)) => {
                let mut count = 0;
                for member in members {
                    let pos = z.iter().position(|(_, m)| m.as_ref() == *member);
                    if let Some(pos) = pos {
                        z.remove(pos);
                        count += 1;
                    }
                }
                Ok(count)
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(0),
        }
    }

    async fn zrange(&self, key: &[u8], start: i64, stop: i64, with_scores: bool) -> Result<Vec<(Bytes, Option<f64>)>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::ZSet(z)) => {
                let len = z.len() as i64;
                let start = if start < 0 { (len + start).max(0) } else { start } as usize;
                let stop = if stop < 0 { (len + stop).max(0) } else { stop } as usize;

                if start > stop || start >= z.len() {
                    return Ok(Vec::new());
                }

                let end = (stop + 1).min(z.len());
                Ok(z[start..end]
                    .iter()
                    .map(|(score, member)| {
                        (member.clone(), if with_scores { Some(*score) } else { None })
                    })
                    .collect())
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    async fn zscore(&self, key: &[u8], member: &[u8]) -> Result<Option<f64>, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::ZSet(z)) => {
                Ok(z.iter()
                    .find(|(_, m)| m.as_ref() == member)
                    .map(|(score, _)| *score))
            }
            Some(_) => Err(RespError::WrongType),
            None => Ok(None),
        }
    }

    async fn zcard(&self, key: &[u8]) -> Result<i64, RespError> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(StoreValue::ZSet(z)) => Ok(z.len() as i64),
            Some(_) => Err(RespError::WrongType),
            None => Ok(0),
        }
    }

    async fn dbsize(&self) -> Result<i64, RespError> {
        let data = self.data.read().await;
        Ok(data.len() as i64)
    }

    async fn flushdb(&self) -> Result<(), RespError> {
        let mut data = self.data.write().await;
        data.clear();
        Ok(())
    }

    async fn info(&self, section: Option<&str>) -> Result<String, RespError> {
        let data = self.data.read().await;
        let info = match section {
            Some("server") | None => format!(
                "# Server\r\n\
                redis_version:7.0.0-thunderdb\r\n\
                redis_mode:standalone\r\n\
                os:ThunderDB\r\n\
                \r\n\
                # Keyspace\r\n\
                db0:keys={},expires=0\r\n",
                data.len()
            ),
            Some("keyspace") => format!(
                "# Keyspace\r\n\
                db0:keys={},expires=0\r\n",
                data.len()
            ),
            Some(_) => String::new(),
        };
        Ok(info)
    }

    async fn auth(&self, password: &str, _username: Option<&str>) -> Result<(), RespError> {
        match &self.password {
            Some(expected) if expected == password => Ok(()),
            Some(_) => Err(RespError::Auth("WRONGPASS invalid password".into())),
            None => Ok(()), // No password required
        }
    }
}

// Simple glob matching for KEYS command
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while let Some(p) = pattern_chars.next() {
        match p {
            '*' => {
                // Skip consecutive *
                while pattern_chars.peek() == Some(&'*') {
                    pattern_chars.next();
                }
                // If * is at end, match rest
                if pattern_chars.peek().is_none() {
                    return true;
                }
                // Try matching rest of pattern at each position
                while text_chars.peek().is_some() {
                    let remaining_pattern: String = pattern_chars.clone().collect();
                    let remaining_text: String = text_chars.clone().collect();
                    if glob_match(&remaining_pattern, &remaining_text) {
                        return true;
                    }
                    text_chars.next();
                }
                return false;
            }
            '?' => {
                if text_chars.next().is_none() {
                    return false;
                }
            }
            '[' => {
                // Character class
                let mut matched = false;
                let negate = pattern_chars.peek() == Some(&'^');
                if negate {
                    pattern_chars.next();
                }
                let text_char = match text_chars.next() {
                    Some(c) => c,
                    None => return false,
                };
                while let Some(c) = pattern_chars.next() {
                    if c == ']' {
                        break;
                    }
                    if c == text_char {
                        matched = true;
                    }
                }
                if matched == negate {
                    return false;
                }
            }
            '\\' => {
                // Escape next character
                let next = pattern_chars.next();
                let text_char = text_chars.next();
                if next != text_char {
                    return false;
                }
            }
            c => {
                if text_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    text_chars.peek().is_none()
}

// ============================================================================
// Standalone serve function for main.rs integration
// ============================================================================

/// Default max connections for RESP protocol
const DEFAULT_RESP_MAX_CONNECTIONS: usize = 1000;

/// Serve RESP protocol connections on the given listener
///
/// This is a convenience function for use in main.rs that accepts a pre-bound
/// TcpListener and an executor implementing RespExecutor.
pub async fn serve<E: RespExecutor + 'static>(
    listener: TcpListener,
    executor: Arc<E>,
) {
    serve_with_cancel(listener, executor, DEFAULT_RESP_MAX_CONNECTIONS, tokio_util::sync::CancellationToken::new()).await
}

/// Serve RESP protocol connections with custom max connections limit.
pub async fn serve_with_max_connections<E: RespExecutor + 'static>(
    listener: TcpListener,
    executor: Arc<E>,
    max_connections: usize,
) {
    serve_with_cancel(listener, executor, max_connections, tokio_util::sync::CancellationToken::new()).await
}

/// Serve RESP protocol connections with cancellation support for graceful shutdown.
pub async fn serve_with_cancel<E: RespExecutor + 'static>(
    listener: TcpListener,
    executor: Arc<E>,
    max_connections: usize,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    let limiter = ConnectionLimiter::new(max_connections);
    let handler = Arc::new(RespHandler::new(executor));

    info!(
        "RESP protocol server started (max_connections: {})",
        max_connections
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
                                    "Rejecting RESP connection from {}: max connections ({}) reached",
                                    peer_addr, max_connections
                                );
                                drop(stream);
                                continue;
                            }
                        };

                        debug!(
                            "RESP connection from {} (active: {}/{})",
                            peer_addr,
                            limiter.active_connections(),
                            limiter.max_connections()
                        );

                        let handler = handler.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(e) = handler.handle_connection(stream).await {
                                error!("RESP connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept RESP connection: {}", e);
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                info!("RESP protocol server shutting down");
                break;
            }
        }
    }
}

/// Serve RESP protocol connections with TLS required.
///
/// All connections will start with a TLS handshake.
pub async fn serve_with_tls<E: RespExecutor + 'static>(
    listener: TcpListener,
    executor: Arc<E>,
    tls_config: &TlsConfig,
    max_connections: usize,
) {
    let tls_acceptor = match tls_config.build_acceptor() {
        Ok(acceptor) => acceptor,
        Err(e) => {
            error!("Failed to build TLS acceptor: {}", e);
            return;
        }
    };

    let limiter = ConnectionLimiter::new(max_connections);
    let handler = Arc::new(RespHandler {
        executor,
        password: None,
        tls_acceptor: Some(tls_acceptor),
    });

    info!(
        "RESP protocol server started with TLS (max_connections: {})",
        max_connections
    );

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                // Try to acquire a connection permit
                let permit = match limiter.try_acquire() {
                    Some(permit) => permit,
                    None => {
                        warn!(
                            "Rejecting RESP connection from {}: max connections ({}) reached",
                            peer_addr, max_connections
                        );
                        drop(stream);
                        continue;
                    }
                };

                debug!(
                    "RESP TLS connection from {} (active: {}/{})",
                    peer_addr,
                    limiter.active_connections(),
                    limiter.max_connections()
                );

                let handler = handler.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) = handler.handle_tls_connection(stream).await {
                        error!("RESP TLS connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept RESP connection: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resp_parser_simple_string() {
        let mut parser = RespParser::new();
        parser.extend(b"+OK\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_resp_parser_error() {
        let mut parser = RespParser::new();
        parser.extend(b"-ERR unknown command\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::Error("ERR unknown command".to_string()));
    }

    #[test]
    fn test_resp_parser_integer() {
        let mut parser = RespParser::new();
        parser.extend(b":1000\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::Integer(1000));
    }

    #[test]
    fn test_resp_parser_bulk_string() {
        let mut parser = RespParser::new();
        parser.extend(b"$6\r\nfoobar\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::BulkString(Bytes::from("foobar")));
    }

    #[test]
    fn test_resp_parser_null() {
        let mut parser = RespParser::new();
        parser.extend(b"$-1\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(value, RespValue::Null);
    }

    #[test]
    fn test_resp_parser_array() {
        let mut parser = RespParser::new();
        parser.extend(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

        let value = parser.parse().unwrap().unwrap();
        assert_eq!(
            value,
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("foo")),
                RespValue::BulkString(Bytes::from("bar")),
            ])
        );
    }

    #[test]
    fn test_encode_simple_string() {
        let encoded = encode_resp(&RespValue::SimpleString("OK".to_string()));
        assert_eq!(encoded, b"+OK\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let encoded = encode_resp(&RespValue::Integer(42));
        assert_eq!(encoded, b":42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let encoded = encode_resp(&RespValue::BulkString(Bytes::from("hello")));
        assert_eq!(encoded, b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_null() {
        let encoded = encode_resp(&RespValue::Null);
        assert_eq!(encoded, b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let encoded = encode_resp(&RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("GET")),
            RespValue::BulkString(Bytes::from("key")),
        ]));
        assert_eq!(encoded, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("foo*", "foobar"));
        assert!(glob_match("*bar", "foobar"));
        assert!(glob_match("foo*bar", "foobazbar"));
        assert!(glob_match("?oo", "foo"));
        assert!(!glob_match("foo", "bar"));
        assert!(glob_match("user:*", "user:123"));
    }

    #[tokio::test]
    async fn test_in_memory_store_string() {
        let store = InMemoryStore::new();

        store.set(b"key", b"value", SetOptions::default()).await.unwrap();
        let value = store.get(b"key").await.unwrap();
        assert_eq!(value, Some(Bytes::from("value")));
    }

    #[tokio::test]
    async fn test_in_memory_store_hash() {
        let store = InMemoryStore::new();

        store.hset(b"hash", &[(b"field1".as_ref(), b"value1".as_ref())]).await.unwrap();
        let value = store.hget(b"hash", b"field1").await.unwrap();
        assert_eq!(value, Some(Bytes::from("value1")));
    }

    #[tokio::test]
    async fn test_in_memory_store_list() {
        let store = InMemoryStore::new();

        store.rpush(b"list", &[b"a", b"b", b"c"]).await.unwrap();
        let values = store.lrange(b"list", 0, -1).await.unwrap();
        assert_eq!(values.len(), 3);
    }
}
