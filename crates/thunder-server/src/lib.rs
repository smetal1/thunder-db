//! # Thunder Server
//!
//! Main server library for ThunderDB providing:
//! - Database engine coordinating all components
//! - Session management
//! - Query execution
//! - Transaction management
//! - Background workers (vacuum, checkpoint)

pub mod engine;
pub mod executor;
pub mod workers;

// Re-export engine types
pub use engine::{DatabaseEngine, EngineConfig, EngineStats, QueryResult, SessionState};

// Re-export worker types
pub use workers::{WorkerConfig, WorkerManager};

// Re-export executor types
pub use executor::{PhysicalExecutor, TableStorage, ExprEvaluator, AggregateAccumulator};

// Re-export dependencies for convenience
pub use thunder_api;
pub use thunder_cdc;
pub use thunder_cluster;
pub use thunder_common;
pub use thunder_fdw;
pub use thunder_protocol;
pub use thunder_query;
pub use thunder_sql;
pub use thunder_storage;
pub use thunder_txn;
pub use thunder_vector;
