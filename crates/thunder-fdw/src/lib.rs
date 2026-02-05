//! # Thunder FDW
//!
//! Foreign Data Wrappers for ThunderDB providing:
//! - Query external databases (PostgreSQL, MySQL, MongoDB, Redis)
//! - Predicate pushdown optimization
//! - Join across data sources
//! - Plugin system for custom connectors
//!
//! ## Example
//!
//! ```ignore
//! use thunder_fdw::{ForeignServer, plugin::CombinedRegistry};
//!
//! // Create a foreign server
//! let server = ForeignServer {
//!     name: "remote_pg".to_string(),
//!     connector_type: "postgres".to_string(),
//!     options: [
//!         ("host".to_string(), "localhost".to_string()),
//!         ("port".to_string(), "5432".to_string()),
//!         ("dbname".to_string(), "mydb".to_string()),
//!     ].into(),
//! };
//!
//! // Create FDW instance
//! let registry = CombinedRegistry::new();
//! let fdw = registry.create_fdw(&server)?;
//! ```

pub mod connector;
pub mod mongodb;
pub mod mysql;
pub mod plugin;
pub mod postgres;
pub mod pushdown;
pub mod redis;

// Re-exports from connector
pub use connector::{
    CostEstimator, FdwConnectionPool, ParamStyle, PoolStats, QueryBuilder, SchemaMapper,
    TableMapping,
};

// Re-exports from postgres
pub use postgres::PostgresFdw;

// Re-exports from mysql
pub use mysql::MySqlFdw;

// Re-exports from mongodb
pub use mongodb::MongoFdw;

// Re-exports from redis
pub use redis::{RedisDataType, RedisFdw};

// Re-exports from plugin
pub use plugin::{
    BuiltinRegistry, CombinedRegistry, PluginDescriptor, PluginInfo, PluginLoader,
    PLUGIN_API_VERSION,
};

// Re-exports from pushdown
pub use pushdown::{
    AggregateExpr, AggregateFunction, AggregationPushdownAnalyzer, AggregationPushdownPlan,
    JoinCondition, JoinPushdownAnalyzer, JoinPushdownPlan, JoinType, LimitPushdownAnalyzer,
    LimitPushdownPlan, PushdownAnalyzer, PushdownCostEstimate, PushdownCostModel,
    PushdownDecision, PushdownPlan,
};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use thunder_common::prelude::*;

/// Foreign Data Wrapper trait
#[async_trait]
pub trait ForeignDataWrapper: Send + Sync {
    /// Get connector name
    fn name(&self) -> &str;

    /// Estimate cost for query planning
    fn estimate_size(&self, quals: &[Qual]) -> Result<(usize, f64)>;

    /// Get column statistics
    fn get_statistics(&self, column: &str) -> Option<ColumnStatistics>;

    /// Begin a scan with pushdown predicates
    async fn begin_scan(
        &self,
        columns: &[String],
        quals: &[Qual],
        limit: Option<usize>,
    ) -> Result<Box<dyn ForeignScan>>;

    /// Modify data (if supported)
    async fn modify(&self, op: ModifyOperation) -> Result<u64>;

    /// Import schema from foreign server
    async fn import_schema(&self, schema: &str) -> Result<Vec<ForeignTableDef>>;

    /// Get capabilities
    fn capabilities(&self) -> FdwCapabilities;
}

/// Foreign scan trait
#[async_trait]
pub trait ForeignScan: Send {
    /// Get next batch of rows
    async fn next(&mut self) -> Result<Option<RecordBatch>>;

    /// Get schema
    fn schema(&self) -> &Schema;

    /// Reset scan to beginning
    async fn reset(&mut self) -> Result<()>;
}

/// Query qualifier for pushdown
#[derive(Debug, Clone)]
pub struct Qual {
    pub column: String,
    pub operator: QualOperator,
    pub value: Value,
}

/// Qualifier operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Like,
    In,
    IsNull,
    IsNotNull,
}

/// Column statistics for query planning
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
}

/// Modify operation types
#[derive(Debug, Clone)]
pub enum ModifyOperation {
    Insert { table: String, rows: Vec<Row> },
    Update { table: String, assignments: Vec<(String, Value)>, quals: Vec<Qual> },
    Delete { table: String, quals: Vec<Qual> },
}

/// Foreign table definition
#[derive(Debug, Clone)]
pub struct ForeignTableDef {
    pub name: String,
    pub schema: Schema,
    pub options: std::collections::HashMap<String, String>,
}

/// Field definition for FDW schemas (alias for ColumnDef for compatibility)
pub type FieldDef = ColumnDef;

/// FDW capabilities
#[derive(Debug, Clone)]
pub struct FdwCapabilities {
    pub supports_predicate_pushdown: bool,
    pub supports_limit_pushdown: bool,
    pub supports_aggregation_pushdown: bool,
    pub supports_modification: bool,
    pub supports_transactions: bool,
    pub max_connections: usize,
}

/// Foreign server configuration
#[derive(Debug, Clone)]
pub struct ForeignServer {
    pub name: String,
    pub connector_type: String,
    pub options: std::collections::HashMap<String, String>,
}

/// Connector registry
pub struct ConnectorRegistry {
    connectors: std::collections::HashMap<String, Box<dyn Fn(&ForeignServer) -> Result<Box<dyn ForeignDataWrapper>> + Send + Sync>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            connectors: std::collections::HashMap::new(),
        }
    }

    pub fn register<F>(&mut self, name: &str, factory: F)
    where
        F: Fn(&ForeignServer) -> Result<Box<dyn ForeignDataWrapper>> + Send + Sync + 'static,
    {
        self.connectors.insert(name.to_string(), Box::new(factory));
    }

    pub fn create(&self, server: &ForeignServer) -> Result<Box<dyn ForeignDataWrapper>> {
        let factory = self.connectors.get(&server.connector_type)
            .ok_or_else(|| Error::not_found("Connector", &server.connector_type))?;
        factory(server)
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qual() {
        let qual = Qual {
            column: "age".to_string(),
            operator: QualOperator::Gt,
            value: Value::Int64(18),
        };

        assert_eq!(qual.column, "age");
        assert_eq!(qual.operator, QualOperator::Gt);
    }

    #[test]
    fn test_connector_registry() {
        let registry = ConnectorRegistry::new();
        assert!(registry.connectors.is_empty());
    }
}
