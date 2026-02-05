//! Error types for ThunderDB

use thiserror::Error;

/// Result type alias using ThunderDB's Error type
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for ThunderDB
#[derive(Error, Debug)]
pub enum Error {
    // Storage errors
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    // Transaction errors
    #[error("Transaction error: {0}")]
    Transaction(#[from] TransactionError),

    // SQL errors
    #[error("SQL error: {0}")]
    Sql(#[from] SqlError),

    // Query errors
    #[error("Query error: {0}")]
    Query(#[from] QueryError),

    // Cluster errors
    #[error("Cluster error: {0}")]
    Cluster(#[from] ClusterError),

    // Protocol errors
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    // IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    // Serialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),

    // Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    // Internal errors
    #[error("Internal error: {0}")]
    Internal(String),

    // Not found
    #[error("{0} not found: {1}")]
    NotFound(String, String),

    // Already exists
    #[error("{0} already exists: {1}")]
    AlreadyExists(String, String),

    // Invalid argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    // Permission denied
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    // Resource exhausted
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    // Timeout
    #[error("Operation timed out: {0}")]
    Timeout(String),

    // Cancelled
    #[error("Operation cancelled: {0}")]
    Cancelled(String),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Page not found: {0}")]
    PageNotFound(u64),

    #[error("Page corrupted: {0}")]
    PageCorrupted(u64),

    #[error("Buffer pool full")]
    BufferPoolFull,

    #[error("WAL write failed: {0}")]
    WalWriteFailed(String),

    #[error("Checkpoint failed: {0}")]
    CheckpointFailed(String),

    #[error("Index error: {0}")]
    IndexError(String),

    #[error("Compression error: {0}")]
    CompressionError(String),

    #[error("Disk full")]
    DiskFull,

    #[error("I/O error: {0}")]
    IoError(String),
}

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Transaction not found: {0}")]
    NotFound(u64),

    #[error("Transaction already committed: {0}")]
    AlreadyCommitted(u64),

    #[error("Transaction already aborted: {0}")]
    AlreadyAborted(u64),

    #[error("Write conflict on row {0}")]
    WriteConflict(String),

    #[error("Deadlock detected")]
    Deadlock,

    #[error("Serialization failure")]
    SerializationFailure,

    #[error("Lock timeout")]
    LockTimeout,

    #[error("Transaction too old")]
    TooOld,
}

#[derive(Error, Debug)]
pub enum SqlError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("Ambiguous column: {0}")]
    AmbiguousColumn(String),

    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Out of memory")]
    OutOfMemory,

    #[error("Query cancelled")]
    Cancelled,

    #[error("Query timeout")]
    Timeout,

    #[error("Division by zero")]
    DivisionByZero,

    #[error("Overflow")]
    Overflow,
}

#[derive(Error, Debug)]
pub enum ClusterError {
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Leader not found")]
    LeaderNotFound,

    #[error("Not leader")]
    NotLeader,

    #[error("Region not found: {0}")]
    RegionNotFound(u64),

    #[error("Consensus error: {0}")]
    ConsensusError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Split in progress")]
    SplitInProgress,

    #[error("Merge in progress")]
    MergeInProgress,
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Protocol version mismatch: {0}")]
    VersionMismatch(String),

    #[error("Unsupported command: {0}")]
    UnsupportedCommand(String),
}

impl Error {
    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(msg.into())
    }

    pub fn not_found(kind: impl Into<String>, name: impl Into<String>) -> Self {
        Error::NotFound(kind.into(), name.into())
    }

    pub fn already_exists(kind: impl Into<String>, name: impl Into<String>) -> Self {
        Error::AlreadyExists(kind.into(), name.into())
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Error::InvalidArgument(msg.into())
    }

    /// Return a PostgreSQL-compatible SQLSTATE code for this error.
    ///
    /// Codes follow the PostgreSQL convention:
    /// <https://www.postgresql.org/docs/current/errcodes-appendix.html>
    pub fn sqlstate(&self) -> &'static str {
        match self {
            // Storage errors
            Error::Storage(se) => match se {
                StorageError::PageNotFound(_) => "58030",   // io_error
                StorageError::PageCorrupted(_) => "XX001",  // data_corrupted
                StorageError::BufferPoolFull => "53200",    // too_many_connections (resource)
                StorageError::WalWriteFailed(_) => "58030", // io_error
                StorageError::CheckpointFailed(_) => "58030",
                StorageError::IndexError(_) => "XX000",     // internal_error
                StorageError::CompressionError(_) => "XX000",
                StorageError::DiskFull => "53100",          // disk_full
                StorageError::IoError(_) => "58030",        // io_error
            },
            // Transaction errors
            Error::Transaction(te) => match te {
                TransactionError::NotFound(_) => "25P01",       // no_active_sql_transaction
                TransactionError::AlreadyCommitted(_) => "25000", // invalid_transaction_state
                TransactionError::AlreadyAborted(_) => "25P02",   // in_failed_sql_transaction
                TransactionError::WriteConflict(_) => "40001",    // serialization_failure
                TransactionError::Deadlock => "40P01",            // deadlock_detected
                TransactionError::SerializationFailure => "40001",
                TransactionError::LockTimeout => "55P03",         // lock_not_available
                TransactionError::TooOld => "25000",
            },
            // SQL errors
            Error::Sql(se) => match se {
                SqlError::ParseError(_) => "42601",         // syntax_error
                SqlError::TableNotFound(_) => "42P01",      // undefined_table
                SqlError::ColumnNotFound(_) => "42703",     // undefined_column
                SqlError::TypeMismatch { .. } => "42804",   // datatype_mismatch
                SqlError::ConstraintViolation(_) => "23000", // integrity_constraint_violation
                SqlError::AmbiguousColumn(_) => "42702",    // ambiguous_column
                SqlError::InvalidExpression(_) => "42601",
                SqlError::UnsupportedFeature(_) => "0A000", // feature_not_supported
            },
            // Query errors
            Error::Query(qe) => match qe {
                QueryError::ExecutionError(_) => "XX000",
                QueryError::OutOfMemory => "53200",   // out_of_memory
                QueryError::Cancelled => "57014",     // query_canceled
                QueryError::Timeout => "57014",       // query_canceled
                QueryError::DivisionByZero => "22012", // division_by_zero
                QueryError::Overflow => "22003",       // numeric_value_out_of_range
            },
            // Cluster errors
            Error::Cluster(ce) => match ce {
                ClusterError::NodeNotFound(_) => "08001",   // sqlclient_unable_to_establish_sqlconnection
                ClusterError::LeaderNotFound => "08001",
                ClusterError::NotLeader => "25006",          // read_only_sql_transaction
                ClusterError::RegionNotFound(_) => "42704",
                ClusterError::ConsensusError(_) => "XX000",
                ClusterError::NetworkError(_) => "08006",    // connection_failure
                ClusterError::SplitInProgress => "55000",    // object_not_in_prerequisite_state
                ClusterError::MergeInProgress => "55000",
            },
            // Protocol errors
            Error::Protocol(pe) => match pe {
                ProtocolError::InvalidMessage(_) => "08P01",     // protocol_violation
                ProtocolError::AuthenticationFailed(_) => "28P01", // invalid_password
                ProtocolError::ConnectionClosed => "08003",       // connection_does_not_exist
                ProtocolError::VersionMismatch(_) => "08004",     // sqlserver_rejected_establishment
                ProtocolError::UnsupportedCommand(_) => "0A000",
            },
            // Top-level error variants
            Error::Io(_) => "58030",                   // io_error
            Error::Serialization(_) => "XX000",
            Error::Config(_) => "F0000",               // config_file_error
            Error::Internal(_) => "XX000",             // internal_error
            Error::NotFound(_, _) => "42704",          // undefined_object
            Error::AlreadyExists(_, _) => "42710",     // duplicate_object
            Error::InvalidArgument(_) => "22023",      // invalid_parameter_value
            Error::PermissionDenied(_) => "42501",     // insufficient_privilege
            Error::ResourceExhausted(_) => "53000",    // insufficient_resources
            Error::Timeout(_) => "57014",              // query_canceled
            Error::Cancelled(_) => "57014",
        }
    }

    /// Return the PostgreSQL-compatible error severity.
    pub fn severity(&self) -> &'static str {
        match self {
            Error::Storage(StorageError::PageCorrupted(_)) => "FATAL",
            Error::Storage(StorageError::DiskFull) => "FATAL",
            Error::Internal(_) => "ERROR",
            _ => "ERROR",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::not_found("Table", "users");
        assert_eq!(err.to_string(), "Table not found: users");

        let err = Error::Transaction(TransactionError::Deadlock);
        assert_eq!(err.to_string(), "Transaction error: Deadlock detected");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn test_sqlstate_codes() {
        // Storage errors
        assert_eq!(Error::Storage(StorageError::DiskFull).sqlstate(), "53100");
        assert_eq!(Error::Storage(StorageError::PageCorrupted(42)).sqlstate(), "XX001");
        assert_eq!(Error::Storage(StorageError::PageNotFound(1)).sqlstate(), "58030");

        // Transaction errors
        assert_eq!(Error::Transaction(TransactionError::Deadlock).sqlstate(), "40P01");
        assert_eq!(Error::Transaction(TransactionError::WriteConflict("k".into())).sqlstate(), "40001");
        assert_eq!(Error::Transaction(TransactionError::LockTimeout).sqlstate(), "55P03");

        // SQL errors
        assert_eq!(Error::Sql(SqlError::ParseError("bad".into())).sqlstate(), "42601");
        assert_eq!(Error::Sql(SqlError::TableNotFound("t".into())).sqlstate(), "42P01");
        assert_eq!(Error::Sql(SqlError::ColumnNotFound("c".into())).sqlstate(), "42703");
        assert_eq!(Error::Sql(SqlError::ConstraintViolation("pk".into())).sqlstate(), "23000");

        // Query errors
        assert_eq!(Error::Query(QueryError::Timeout).sqlstate(), "57014");
        assert_eq!(Error::Query(QueryError::DivisionByZero).sqlstate(), "22012");

        // Top-level errors
        assert_eq!(Error::PermissionDenied("no".into()).sqlstate(), "42501");
        assert_eq!(Error::not_found("Table", "x").sqlstate(), "42704");
        assert_eq!(Error::already_exists("Table", "x").sqlstate(), "42710");
        assert_eq!(Error::Timeout("slow".into()).sqlstate(), "57014");
        assert_eq!(Error::internal("oops").sqlstate(), "XX000");
    }

    #[test]
    fn test_sqlstate_all_five_chars() {
        // Verify all SQLSTATE codes are exactly 5 characters
        let errors: Vec<Error> = vec![
            Error::Storage(StorageError::PageNotFound(0)),
            Error::Storage(StorageError::PageCorrupted(0)),
            Error::Storage(StorageError::BufferPoolFull),
            Error::Storage(StorageError::DiskFull),
            Error::Transaction(TransactionError::Deadlock),
            Error::Transaction(TransactionError::LockTimeout),
            Error::Sql(SqlError::ParseError("".into())),
            Error::Sql(SqlError::TableNotFound("".into())),
            Error::Sql(SqlError::UnsupportedFeature("".into())),
            Error::Query(QueryError::Timeout),
            Error::Query(QueryError::OutOfMemory),
            Error::Query(QueryError::DivisionByZero),
            Error::PermissionDenied("".into()),
            Error::NotFound("".into(), "".into()),
            Error::AlreadyExists("".into(), "".into()),
            Error::Timeout("".into()),
            Error::internal(""),
            Error::Config("".into()),
        ];
        for err in &errors {
            let code = err.sqlstate();
            assert_eq!(code.len(), 5, "SQLSTATE for {:?} is '{}' (not 5 chars)", err, code);
        }
    }

    #[test]
    fn test_severity() {
        assert_eq!(Error::Storage(StorageError::DiskFull).severity(), "FATAL");
        assert_eq!(Error::Storage(StorageError::PageCorrupted(0)).severity(), "FATAL");
        assert_eq!(Error::internal("oops").severity(), "ERROR");
        assert_eq!(Error::Sql(SqlError::ParseError("x".into())).severity(), "ERROR");
    }
}
