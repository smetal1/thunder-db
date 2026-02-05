//! # Thunder Common
//!
//! Common types, errors, and utilities shared across all ThunderDB crates.

pub mod audit;
pub mod circuit_breaker;
pub mod config;
pub mod error;
pub mod rbac;
pub mod types;
pub mod utils;
pub mod testing;
pub mod metrics;

pub use config::*;
pub use error::{Error, Result};
pub use rbac::*;
pub use types::*;

/// Re-export commonly used external types
pub mod prelude {
    pub use super::error::{Error, Result};
    pub use super::types::*;
    pub use super::config::*;
    pub use async_trait::async_trait;
    pub use bytes::Bytes;
    pub use tracing::{debug, error, info, trace, warn, instrument};
}
