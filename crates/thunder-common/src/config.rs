//! Configuration types for ThunderDB

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Main server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Unique node identifier
    pub node_id: u64,

    /// Address to listen on
    pub listen_addr: String,

    /// PostgreSQL protocol port
    pub pg_port: u16,

    /// MySQL protocol port
    pub mysql_port: u16,

    /// Redis protocol port
    pub resp_port: u16,

    /// gRPC port for cluster communication
    pub grpc_port: u16,

    /// REST API port
    pub http_port: u16,

    /// Data directory
    pub data_dir: PathBuf,

    /// WAL directory
    pub wal_dir: PathBuf,

    /// Storage configuration
    pub storage: StorageConfig,

    /// Cluster configuration
    pub cluster: ClusterConfig,

    /// Security configuration
    pub security: SecurityConfig,

    /// Logging configuration
    pub logging: LoggingConfig,

    /// Query timeout (queries exceeding this are cancelled)
    #[serde(default = "default_query_timeout", with = "humantime_serde")]
    pub query_timeout: Duration,

    /// Maximum result rows per query (prevents OOM on huge results)
    #[serde(default = "default_max_result_rows")]
    pub max_result_rows: usize,
}

fn default_query_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_max_result_rows() -> usize {
    1_000_000
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            listen_addr: "0.0.0.0".to_string(),
            pg_port: 5432,
            mysql_port: 3306,
            resp_port: 6379,
            grpc_port: 9090,
            http_port: 8080,
            data_dir: PathBuf::from("./data"),
            wal_dir: PathBuf::from("./wal"),
            storage: StorageConfig::default(),
            cluster: ClusterConfig::default(),
            security: SecurityConfig::default(),
            logging: LoggingConfig::default(),
            query_timeout: default_query_timeout(),
            max_result_rows: default_max_result_rows(),
        }
    }
}

impl ServerConfig {
    /// Validate the configuration and return any errors.
    /// Fatal errors are returned as `Err(Vec<String>)`.
    /// Warnings are logged but do not cause failure.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Security validation
        if let Err(sec_errors) = self.security.validate_for_production() {
            errors.extend(sec_errors);
        }

        // Port conflict detection
        let ports = [
            ("pg", self.pg_port),
            ("mysql", self.mysql_port),
            ("resp", self.resp_port),
            ("http", self.http_port),
            ("grpc", self.grpc_port),
        ];
        for i in 0..ports.len() {
            for j in (i + 1)..ports.len() {
                if ports[i].1 == ports[j].1 {
                    errors.push(format!(
                        "Port conflict: {} and {} both use port {}",
                        ports[i].0, ports[j].0, ports[i].1
                    ));
                }
            }
        }

        // Page size must be power of 2 and >= 4096
        let ps = self.storage.page_size;
        if ps < 4096 || !ps.is_power_of_two() {
            errors.push(format!(
                "page_size must be a power of 2 and >= 4096, got {}",
                ps
            ));
        }

        // Query timeout sanity
        if self.query_timeout.is_zero() {
            errors.push("query_timeout must be > 0".to_string());
        }

        // Max result rows sanity
        if self.max_result_rows == 0 {
            errors.push("max_result_rows must be > 0".to_string());
        }

        // Warnings (logged but not fatal)
        if !self.data_dir.exists() {
            tracing::warn!("data_dir {:?} does not exist yet (will be created)", self.data_dir);
        }
        if !self.wal_dir.exists() {
            tracing::warn!("wal_dir {:?} does not exist yet (will be created)", self.wal_dir);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Storage engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Buffer pool size in bytes
    pub buffer_pool_size: usize,

    /// WAL buffer size in bytes
    pub wal_buffer_size: usize,

    /// Page size in bytes
    pub page_size: usize,

    /// Checkpoint interval
    #[serde(with = "humantime_serde")]
    pub checkpoint_interval: Duration,

    /// Number of compaction threads
    pub compaction_threads: usize,

    /// Enable direct I/O
    pub direct_io: bool,

    /// Enable compression
    pub compression: bool,

    /// Compression algorithm
    pub compression_algorithm: CompressionAlgorithm,

    /// Maximum WAL size before forcing checkpoint
    pub max_wal_size: usize,

    /// Sync WAL on every commit
    pub sync_commit: bool,

    /// Disk space warning threshold (percent free)
    #[serde(default = "default_disk_warn_percent")]
    pub disk_space_warn_percent: f64,

    /// Disk space critical threshold — database goes read-only below this
    #[serde(default = "default_disk_critical_percent")]
    pub disk_space_critical_percent: f64,

    /// How often to check disk space
    #[serde(default = "default_disk_check_interval", with = "humantime_serde")]
    pub disk_check_interval: Duration,

    /// Enable automatic backups
    #[serde(default)]
    pub backup_enabled: bool,

    /// Interval between automatic backups
    #[serde(default = "default_backup_interval", with = "humantime_serde")]
    pub backup_interval: Duration,

    /// Number of backups to retain (oldest are pruned)
    #[serde(default = "default_backup_retention_count")]
    pub backup_retention_count: usize,

    /// Custom backup directory (defaults to data_dir/backups)
    #[serde(default)]
    pub backup_dir: Option<PathBuf>,

    /// Data retention / TTL configuration
    #[serde(default)]
    pub retention: RetentionConfig,

    /// Enable data-at-rest encryption
    #[serde(default)]
    pub encryption_enabled: bool,

    /// Path to the 32-byte AES-256 encryption key file
    #[serde(default)]
    pub encryption_key_path: Option<PathBuf>,
}

fn default_backup_interval() -> Duration {
    Duration::from_secs(86400) // 24 hours
}

fn default_backup_retention_count() -> usize {
    7
}

fn default_disk_warn_percent() -> f64 {
    10.0
}

fn default_disk_critical_percent() -> f64 {
    5.0
}

fn default_disk_check_interval() -> Duration {
    Duration::from_secs(30)
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            buffer_pool_size: 1024 * 1024 * 1024, // 1GB
            wal_buffer_size: 64 * 1024 * 1024,    // 64MB
            page_size: 16 * 1024,                  // 16KB
            checkpoint_interval: Duration::from_secs(300), // 5 minutes
            compaction_threads: 4,
            direct_io: true,
            compression: true,
            compression_algorithm: CompressionAlgorithm::Lz4,
            max_wal_size: 1024 * 1024 * 1024, // 1GB
            sync_commit: true,
            disk_space_warn_percent: default_disk_warn_percent(),
            disk_space_critical_percent: default_disk_critical_percent(),
            disk_check_interval: default_disk_check_interval(),
            backup_enabled: false,
            backup_interval: default_backup_interval(),
            backup_retention_count: default_backup_retention_count(),
            backup_dir: None,
            retention: RetentionConfig::default(),
            encryption_enabled: false,
            encryption_key_path: None,
        }
    }
}

/// Data retention / TTL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Enable automatic data retention enforcement
    #[serde(default)]
    pub enabled: bool,

    /// Default TTL for tables without explicit TTL in seconds (None = no default)
    #[serde(default)]
    pub default_ttl_secs: Option<u64>,

    /// How often to check for expired rows
    #[serde(default = "default_retention_check_interval", with = "humantime_serde")]
    pub check_interval: Duration,
}

fn default_retention_check_interval() -> Duration {
    Duration::from_secs(3600) // 1 hour
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_ttl_secs: None,
            check_interval: default_retention_check_interval(),
        }
    }
}

/// Data classification for PII/GDPR compliance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataClassification {
    /// Publicly accessible data
    Public,
    /// Internal use only
    Internal,
    /// Confidential — requires access controls
    Confidential,
    /// Restricted / PII — requires masking in logs
    Restricted,
}

/// Compression algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompressionAlgorithm {
    None,
    #[default]
    Lz4,
    Snappy,
    Zstd,
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cluster name
    pub cluster_name: String,

    /// List of peer addresses
    pub peers: Vec<String>,

    /// Raft election timeout
    #[serde(with = "humantime_serde")]
    pub raft_election_timeout: Duration,

    /// Raft heartbeat interval
    #[serde(with = "humantime_serde")]
    pub raft_heartbeat_interval: Duration,

    /// Replication factor
    pub replication_factor: usize,

    /// Maximum region size before splitting
    pub max_region_size: usize,

    /// Minimum region size before merging
    pub min_region_size: usize,

    /// Enable auto-balancing
    pub auto_balance: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_name: "thunderdb".to_string(),
            peers: vec![],
            raft_election_timeout: Duration::from_millis(1000),
            raft_heartbeat_interval: Duration::from_millis(100),
            replication_factor: 3,
            max_region_size: 256 * 1024 * 1024, // 256MB
            min_region_size: 64 * 1024 * 1024,  // 64MB
            auto_balance: true,
        }
    }
}

/// Authentication rule method for per-database auth
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthRuleMethod {
    Trust,
    Password,
    Reject,
}

/// Per-database authentication rule (similar to pg_hba.conf)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseAuthRule {
    /// Database name, or "*" for all databases
    pub database: String,
    /// User name, or "*" for all users
    pub user: String,
    /// Source CIDR pattern, or "*" for all sources
    pub source: String,
    /// Authentication method to apply
    pub method: AuthRuleMethod,
}

impl Default for DatabaseAuthRule {
    fn default() -> Self {
        Self {
            database: "*".to_string(),
            user: "*".to_string(),
            source: "*".to_string(),
            method: AuthRuleMethod::Password,
        }
    }
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Enable authentication (MUST be true for production)
    pub authentication_enabled: bool,

    /// Enable TLS (recommended for production)
    pub tls_enabled: bool,

    /// TLS certificate path
    pub tls_cert_path: Option<PathBuf>,

    /// TLS key path
    pub tls_key_path: Option<PathBuf>,

    /// TLS CA certificate path
    pub tls_ca_path: Option<PathBuf>,

    /// Superuser username (MUST be explicitly configured)
    pub superuser: Option<String>,

    /// Superuser password hash (MUST be explicitly configured)
    /// Use `SecurityConfig::hash_password()` to generate
    pub superuser_password_hash: Option<String>,

    /// Plaintext superuser password (DEPRECATED — use superuser_password_hash)
    /// If set, will be auto-hashed at startup and cleared from memory.
    #[serde(default, skip_serializing)]
    pub superuser_password: Option<String>,

    /// Require password change on first login
    pub require_password_change: bool,

    /// Minimum password length
    pub min_password_length: usize,

    /// Maximum failed login attempts before lockout
    pub max_login_attempts: u32,

    /// Lockout duration in seconds
    pub lockout_duration_secs: u64,

    /// API key for admin endpoints (optional, alternative to user auth)
    pub admin_api_key: Option<String>,

    /// Require TLS for all connections (when true, server refuses to start without TLS)
    #[serde(default)]
    pub require_tls: bool,

    /// CORS allowed origins. Empty = allow all (dev mode).
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,

    /// Rate limit: max requests per second per client IP (0 = disabled)
    #[serde(default = "default_rate_limit_rps")]
    pub rate_limit_requests_per_second: f64,

    /// Rate limit: burst capacity per client IP
    #[serde(default = "default_rate_limit_burst")]
    pub rate_limit_burst: usize,

    /// Mask PII columns in audit/slow-query logs
    #[serde(default = "default_true")]
    pub pii_log_masking: bool,

    /// Per-database authentication rules (evaluated in order, first match wins)
    #[serde(default)]
    pub auth_rules: Vec<DatabaseAuthRule>,
}

fn default_rate_limit_rps() -> f64 {
    100.0
}

fn default_rate_limit_burst() -> usize {
    200
}

fn default_true() -> bool {
    true
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            authentication_enabled: true,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            // SECURITY: No default credentials - must be explicitly configured
            superuser: None,
            superuser_password_hash: None,
            superuser_password: None,
            require_password_change: true,
            min_password_length: 12,
            max_login_attempts: 5,
            lockout_duration_secs: 300, // 5 minutes
            admin_api_key: None,
            require_tls: false,
            cors_allowed_origins: Vec::new(),
            rate_limit_requests_per_second: default_rate_limit_rps(),
            rate_limit_burst: default_rate_limit_burst(),
            pii_log_masking: true,
            auth_rules: Vec::new(),
        }
    }
}

impl SecurityConfig {
    /// If `superuser_password` is set but `superuser_password_hash` is not,
    /// hash the plaintext password with Argon2id, store the hash, and clear
    /// the plaintext from memory. Logs a deprecation warning.
    pub fn resolve_password(&mut self) {
        if self.superuser_password_hash.is_none() {
            if let Some(ref plaintext) = self.superuser_password {
                tracing::warn!(
                    "Config uses deprecated 'superuser_password' field. \
                     Please use 'superuser_password_hash' instead. \
                     Generate a hash with: thunderdb --hash-password \"<password>\""
                );
                self.superuser_password_hash = Some(Self::hash_password(plaintext));
            }
        }
        // Clear plaintext from memory regardless
        self.superuser_password = None;
    }

    /// Hash a password using Argon2id with a random salt
    pub fn hash_password(password: &str) -> String {
        use argon2::{Argon2, PasswordHasher, password_hash::{rand_core::OsRng, SaltString}};
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .expect("Failed to hash password");
        format!("argon2:{}", password_hash)
    }

    /// Verify a password against a hash (supports both Argon2 and legacy SHA256)
    pub fn verify_password(password: &str, hash: &str) -> bool {
        if let Some(argon2_hash) = hash.strip_prefix("argon2:") {
            use argon2::{Argon2, PasswordHash, PasswordVerifier};
            if let Ok(parsed_hash) = PasswordHash::new(argon2_hash) {
                let argon2 = Argon2::default();
                return argon2.verify_password(password.as_bytes(), &parsed_hash).is_ok();
            }
            false
        } else if let Some(expected) = hash.strip_prefix("sha256:") {
            // Legacy SHA256 support for backward compatibility
            use sha2::{Sha256, Digest};
            let salt = "thunderdb_salt_v1";
            let mut hasher = Sha256::new();
            hasher.update(salt.as_bytes());
            hasher.update(password.as_bytes());
            let computed = hex::encode(hasher.finalize());
            use subtle::ConstantTimeEq;
            computed.as_bytes().ct_eq(expected.as_bytes()).into()
        } else {
            false
        }
    }

    /// Check if the configuration is valid for production use
    pub fn validate_for_production(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if !self.authentication_enabled {
            errors.push("Authentication must be enabled for production".to_string());
        }

        if self.superuser.is_none() {
            errors.push("Superuser username must be configured".to_string());
        }

        if self.superuser_password_hash.is_none() {
            errors.push("Superuser password must be configured".to_string());
        }

        if let Some(ref password_hash) = self.superuser_password_hash {
            if password_hash.starts_with("sha256:") {
                errors.push("SHA256 password hashing is deprecated. Re-hash with SecurityConfig::hash_password() for Argon2id.".to_string());
            } else if !password_hash.starts_with("argon2:") {
                errors.push("Password must be hashed (use SecurityConfig::hash_password())".to_string());
            }
        }

        if self.require_tls && !self.tls_enabled {
            errors.push("require_tls is true but tls_enabled is false".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Create a development configuration (NOT for production)
    /// This creates default credentials for local development only
    pub fn development() -> Self {
        Self {
            authentication_enabled: true,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            superuser: Some("admin".to_string()),
            superuser_password_hash: Some(Self::hash_password("admin_dev_only")),
            superuser_password: None,
            require_password_change: false,
            min_password_length: 8,
            max_login_attempts: 10,
            lockout_duration_secs: 60,
            admin_api_key: Some("dev-api-key-not-for-production".to_string()),
            require_tls: false,
            cors_allowed_origins: Vec::new(),
            rate_limit_requests_per_second: 0.0, // disabled in dev
            rate_limit_burst: 0,
            pii_log_masking: false,
            auth_rules: Vec::new(),
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level
    pub level: String,

    /// Log format (json or text)
    pub format: String,

    /// Log file path (None for stdout)
    pub file: Option<PathBuf>,

    /// Enable slow query logging
    pub slow_query_enabled: bool,

    /// Slow query threshold
    #[serde(with = "humantime_serde")]
    pub slow_query_threshold: Duration,

    /// OpenTelemetry OTLP endpoint (e.g. "http://localhost:4317")
    /// Set to enable trace export. Requires the `otel` feature on thunder-server.
    #[serde(default)]
    pub otel_endpoint: Option<String>,

    /// OpenTelemetry service name
    #[serde(default = "default_otel_service_name")]
    pub otel_service_name: String,
}

fn default_otel_service_name() -> String {
    "thunderdb".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "text".to_string(),
            file: None,
            slow_query_enabled: true,
            slow_query_threshold: Duration::from_secs(1),
            otel_endpoint: None,
            otel_service_name: default_otel_service_name(),
        }
    }
}

/// Duration serialization helper
mod humantime_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = humantime::format_duration(*duration).to_string();
        s.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        humantime::parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.pg_port, 5432);
        assert_eq!(config.storage.buffer_pool_size, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_config_serialization() {
        let config = ServerConfig::default();
        let toml = toml::to_string(&config).unwrap();
        let parsed: ServerConfig = toml::from_str(&toml).unwrap();
        assert_eq!(config.node_id, parsed.node_id);
    }
}
