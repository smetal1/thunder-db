//! Property-based tests for ThunderDB common types
//!
//! Uses proptest to verify invariants across randomized inputs:
//! - SQLSTATE codes are always valid 5-character strings
//! - Config serialization round-trips correctly
//! - Circuit breaker state transitions are consistent

use proptest::prelude::*;
use thunder_common::config::{ServerConfig, SecurityConfig, StorageConfig};
use thunder_common::error::*;

// ============================================================================
// SQLSTATE Code Properties
// ============================================================================

/// Generate an arbitrary Error variant
fn arbitrary_error() -> impl Strategy<Value = Error> {
    prop_oneof![
        // Storage errors (PageNotFound/PageCorrupted take u64)
        any::<u64>().prop_map(|n| Error::Storage(StorageError::PageNotFound(n))),
        any::<u64>().prop_map(|n| Error::Storage(StorageError::PageCorrupted(n))),
        (0..1u32).prop_map(|_| Error::Storage(StorageError::BufferPoolFull)),
        (0..1u32).prop_map(|_| Error::Storage(StorageError::DiskFull)),
        any::<String>().prop_map(|s| Error::Storage(StorageError::IoError(s))),
        any::<String>().prop_map(|s| Error::Storage(StorageError::WalWriteFailed(s))),
        any::<String>().prop_map(|s| Error::Storage(StorageError::CheckpointFailed(s))),
        any::<String>().prop_map(|s| Error::Storage(StorageError::IndexError(s))),
        // Transaction errors
        any::<String>().prop_map(|s| Error::Transaction(TransactionError::WriteConflict(s))),
        (0..1u32).prop_map(|_| Error::Transaction(TransactionError::Deadlock)),
        (0..1u32).prop_map(|_| Error::Transaction(TransactionError::LockTimeout)),
        (0..1u32).prop_map(|_| Error::Transaction(TransactionError::SerializationFailure)),
        (0..1u32).prop_map(|_| Error::Transaction(TransactionError::TooOld)),
        // SQL errors
        any::<String>().prop_map(|s| Error::Sql(SqlError::ParseError(s))),
        any::<String>().prop_map(|s| Error::Sql(SqlError::TableNotFound(s))),
        any::<String>().prop_map(|s| Error::Sql(SqlError::ColumnNotFound(s))),
        (any::<String>(), any::<String>()).prop_map(|(e, a)| Error::Sql(SqlError::TypeMismatch { expected: e, actual: a })),
        any::<String>().prop_map(|s| Error::Sql(SqlError::ConstraintViolation(s))),
        any::<String>().prop_map(|s| Error::Sql(SqlError::AmbiguousColumn(s))),
        // Query errors
        (0..1u32).prop_map(|_| Error::Query(QueryError::Timeout)),
        (0..1u32).prop_map(|_| Error::Query(QueryError::OutOfMemory)),
        (0..1u32).prop_map(|_| Error::Query(QueryError::Cancelled)),
        any::<String>().prop_map(|s| Error::Query(QueryError::ExecutionError(s))),
        // Top-level errors
        any::<String>().prop_map(Error::Internal),
        (any::<String>(), any::<String>()).prop_map(|(a, b)| Error::NotFound(a, b)),
        (any::<String>(), any::<String>()).prop_map(|(a, b)| Error::AlreadyExists(a, b)),
        any::<String>().prop_map(Error::PermissionDenied),
        any::<String>().prop_map(Error::Timeout),
        any::<String>().prop_map(Error::Cancelled),
        any::<String>().prop_map(Error::InvalidArgument),
        any::<String>().prop_map(Error::ResourceExhausted),
        any::<String>().prop_map(Error::Config),
        any::<String>().prop_map(Error::Serialization),
    ]
}

proptest! {
    /// All SQLSTATE codes must be exactly 5 ASCII characters
    #[test]
    fn sqlstate_always_five_chars(error in arbitrary_error()) {
        let code = error.sqlstate();
        prop_assert_eq!(code.len(), 5, "SQLSTATE '{}' is not 5 chars for error: {:?}", code, error);
        prop_assert!(code.chars().all(|c| c.is_ascii_alphanumeric()),
            "SQLSTATE '{}' contains non-alphanumeric chars", code);
    }

    /// Severity must be one of the known values
    #[test]
    fn severity_is_valid(error in arbitrary_error()) {
        let severity = error.severity();
        prop_assert!(
            severity == "ERROR" || severity == "FATAL" || severity == "PANIC",
            "Invalid severity '{}' for error: {:?}", severity, error
        );
    }

    /// SQLSTATE class (first 2 chars) should be a recognized PostgreSQL class
    #[test]
    fn sqlstate_has_valid_class(error in arbitrary_error()) {
        let code = error.sqlstate();
        let class = &code[..2];
        // Known PostgreSQL error classes
        let valid_classes = [
            "00", "01", "02", "03", "08", "09", "0A", "0B", "0F", "0L", "0P", "0Z",
            "20", "21", "22", "23", "24", "25", "26", "27", "28", "2B", "2C", "2D", "2F",
            "34", "38", "39", "3B", "3C", "3D", "3F",
            "40", "42", "44",
            "53", "54", "55", "57", "58",
            "72",
            "F0",
            "HV",
            "P0",
            "XX",
        ];
        prop_assert!(
            valid_classes.contains(&class),
            "SQLSTATE class '{}' from code '{}' is not a known PostgreSQL class (error: {:?})",
            class, code, error
        );
    }
}

// ============================================================================
// Config Serialization Properties
// ============================================================================

proptest! {
    /// ServerConfig serialization round-trip: default config survives toml round-trip
    #[test]
    fn config_default_round_trip(_dummy in 0..1u32) {
        let config = ServerConfig::default();
        let serialized = toml::to_string(&config).expect("Failed to serialize default config");
        let deserialized: ServerConfig = toml::from_str(&serialized)
            .expect("Failed to deserialize config");

        // Verify key fields survived the round-trip
        prop_assert_eq!(config.listen_addr, deserialized.listen_addr);
        prop_assert_eq!(config.http_port, deserialized.http_port);
        prop_assert_eq!(config.pg_port, deserialized.pg_port);
        prop_assert_eq!(config.mysql_port, deserialized.mysql_port);
        prop_assert_eq!(config.resp_port, deserialized.resp_port);
    }

    /// SecurityConfig serialization round-trip
    #[test]
    fn security_config_round_trip(_dummy in 0..1u32) {
        let config = SecurityConfig::default();
        let serialized = toml::to_string(&config).expect("Failed to serialize security config");
        let deserialized: SecurityConfig = toml::from_str(&serialized)
            .expect("Failed to deserialize security config");

        prop_assert_eq!(config.authentication_enabled, deserialized.authentication_enabled);
        prop_assert_eq!(config.tls_enabled, deserialized.tls_enabled);
        prop_assert_eq!(config.require_tls, deserialized.require_tls);
    }

    /// StorageConfig serialization round-trip
    #[test]
    fn storage_config_round_trip(_dummy in 0..1u32) {
        let config = StorageConfig::default();
        let serialized = toml::to_string(&config).expect("Failed to serialize storage config");
        let deserialized: StorageConfig = toml::from_str(&serialized)
            .expect("Failed to deserialize storage config");

        prop_assert_eq!(config.buffer_pool_size, deserialized.buffer_pool_size);
        prop_assert_eq!(config.page_size, deserialized.page_size);
        prop_assert_eq!(config.backup_enabled, deserialized.backup_enabled);
    }
}

// ============================================================================
// Circuit Breaker Properties
// ============================================================================

proptest! {
    /// Circuit breaker starts in closed state and allows requests
    #[test]
    fn circuit_breaker_starts_closed(_dummy in 0..1u32) {
        let cb = thunder_common::circuit_breaker::CircuitBreaker::new();
        prop_assert!(cb.allow_request());
        prop_assert_eq!(cb.state_name(), "closed");
    }

    /// N consecutive failures (where N >= threshold) opens the circuit
    #[test]
    fn circuit_breaker_opens_on_failures(n in 5u32..20) {
        let cb = thunder_common::circuit_breaker::CircuitBreaker::new();
        for _ in 0..n {
            cb.record_failure();
        }
        // After >= threshold (default 5) failures, circuit should be open
        prop_assert!(!cb.allow_request(), "Circuit should be open after {} failures", n);
    }

    /// Success after failures (below threshold) keeps circuit closed
    #[test]
    fn circuit_breaker_recovers_on_success(n in 1u32..4) {
        let cb = thunder_common::circuit_breaker::CircuitBreaker::new();
        for _ in 0..n {
            cb.record_failure();
        }
        cb.record_success();
        prop_assert!(cb.allow_request(), "Circuit should be closed after success resets failures");
    }
}

// ============================================================================
// API Key Hashing Properties
// ============================================================================

proptest! {
    /// Hashed API keys always start with "sha256:" prefix
    #[test]
    fn api_key_hash_has_prefix(key in "[a-zA-Z0-9]{8,64}") {
        let hashed = thunder_common::rbac::RbacManager::hash_api_key(&key);
        prop_assert!(hashed.starts_with("sha256:"), "Hash '{}' missing sha256 prefix", hashed);
    }

    /// Same input always produces the same hash (deterministic)
    #[test]
    fn api_key_hash_deterministic(key in "[a-zA-Z0-9]{8,64}") {
        let hash1 = thunder_common::rbac::RbacManager::hash_api_key(&key);
        let hash2 = thunder_common::rbac::RbacManager::hash_api_key(&key);
        prop_assert_eq!(hash1, hash2);
    }

    /// Different inputs produce different hashes (collision resistance)
    #[test]
    fn api_key_hash_no_collision(
        key1 in "[a-zA-Z0-9]{8,32}",
        key2 in "[a-zA-Z0-9]{8,32}"
    ) {
        prop_assume!(key1 != key2);
        let hash1 = thunder_common::rbac::RbacManager::hash_api_key(&key1);
        let hash2 = thunder_common::rbac::RbacManager::hash_api_key(&key2);
        prop_assert_ne!(hash1, hash2,
            "Hash collision between '{}' and '{}'", key1, key2);
    }
}
