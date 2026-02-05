//! FDW Connector Management
//!
//! Provides infrastructure for managing foreign data wrappers:
//! - Connection pooling
//! - Connector lifecycle management
//! - Query execution coordination

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::{
    ColumnStatistics, FdwCapabilities, ForeignDataWrapper, ForeignScan, ForeignServer,
    ForeignTableDef, ModifyOperation, Qual, QualOperator,
};
use thunder_common::prelude::*;

// ============================================================================
// Connection Pool
// ============================================================================

/// Connection pool for FDW connections
pub struct FdwConnectionPool {
    /// Active connections by server name
    connections: DashMap<String, PooledConnection>,
    /// Maximum connections per server
    max_connections: usize,
    /// Connection idle timeout
    idle_timeout: Duration,
    /// Maximum connection age
    max_age: Duration,
    /// Circuit breaker to protect against cascading failures
    circuit_breaker: thunder_common::circuit_breaker::CircuitBreaker,
}

/// Pooled connection wrapper
struct PooledConnection {
    /// The actual FDW
    fdw: Arc<dyn ForeignDataWrapper>,
    /// Creation time
    created_at: Instant,
    /// Last use time
    last_used: Instant,
    /// Active use count
    use_count: usize,
}

impl FdwConnectionPool {
    /// Create a new connection pool
    pub fn new(max_connections: usize, idle_timeout: Duration, max_age: Duration) -> Self {
        Self {
            connections: DashMap::new(),
            max_connections,
            idle_timeout,
            max_age,
            circuit_breaker: thunder_common::circuit_breaker::CircuitBreaker::new(),
        }
    }

    /// Get or create a connection for a server.
    /// Guarded by a circuit breaker — if external service is repeatedly failing,
    /// subsequent requests fail fast instead of blocking on timeouts.
    pub async fn get<F, Fut>(&self, server: &ForeignServer, factory: F) -> Result<Arc<dyn ForeignDataWrapper>>
    where
        F: FnOnce(&ForeignServer) -> Fut,
        Fut: std::future::Future<Output = Result<Box<dyn ForeignDataWrapper>>>,
    {
        // Circuit breaker check: fail fast if external service is down
        if !self.circuit_breaker.allow_request() {
            return Err(Error::Internal(format!(
                "Circuit breaker open for FDW connection pool (server: {}). External service unavailable.",
                server.name
            )));
        }

        let key = server.name.clone();

        // Check for existing valid connection
        if let Some(mut conn) = self.connections.get_mut(&key) {
            let now = Instant::now();

            // Check if connection is still valid
            if now.duration_since(conn.created_at) < self.max_age
                && now.duration_since(conn.last_used) < self.idle_timeout
            {
                conn.last_used = now;
                conn.use_count += 1;
                return Ok(conn.fdw.clone());
            } else {
                // Connection expired, remove it
                drop(conn);
                self.connections.remove(&key);
            }
        }

        // Create new connection
        match factory(server).await {
            Ok(boxed_fdw) => {
                let fdw: Arc<dyn ForeignDataWrapper> = Arc::from(boxed_fdw);
                let conn = PooledConnection {
                    fdw: fdw.clone(),
                    created_at: Instant::now(),
                    last_used: Instant::now(),
                    use_count: 1,
                };
                self.connections.insert(key, conn);
                self.circuit_breaker.record_success();
                Ok(fdw)
            }
            Err(e) => {
                self.circuit_breaker.record_failure();
                Err(e)
            }
        }
    }

    /// Remove a connection
    pub fn remove(&self, server_name: &str) {
        self.connections.remove(server_name);
    }

    /// Clean up expired connections
    pub fn cleanup(&self) {
        let now = Instant::now();
        self.connections.retain(|_, conn| {
            now.duration_since(conn.created_at) < self.max_age
                && now.duration_since(conn.last_used) < self.idle_timeout
        });
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let mut total_uses = 0usize;
        for entry in self.connections.iter() {
            total_uses += entry.use_count;
        }

        PoolStats {
            active_connections: self.connections.len(),
            max_connections: self.max_connections,
            total_uses,
        }
    }
}

impl Default for FdwConnectionPool {
    fn default() -> Self {
        Self::new(
            100,
            Duration::from_secs(300),  // 5 minutes idle
            Duration::from_secs(3600), // 1 hour max age
        )
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub active_connections: usize,
    pub max_connections: usize,
    pub total_uses: usize,
}

// ============================================================================
// Query Builder
// ============================================================================

/// Builds SQL queries from qualifiers
pub struct QueryBuilder {
    /// Column mapping (local -> remote)
    column_map: HashMap<String, String>,
    /// Parameter style
    param_style: ParamStyle,
}

/// Parameter style for prepared statements
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamStyle {
    /// Numbered ($1, $2) - PostgreSQL
    Numbered,
    /// Positional (?, ?) - MySQL
    Positional,
    /// Named (:name) - Oracle
    Named,
}

impl QueryBuilder {
    pub fn new(param_style: ParamStyle) -> Self {
        Self {
            column_map: HashMap::new(),
            param_style,
        }
    }

    /// Set column mapping
    pub fn with_column_map(mut self, map: HashMap<String, String>) -> Self {
        self.column_map = map;
        self
    }

    /// Build WHERE clause from qualifiers
    pub fn build_where_clause(&self, quals: &[Qual]) -> (String, Vec<Value>) {
        if quals.is_empty() {
            return (String::new(), Vec::new());
        }

        let mut clauses = Vec::new();
        let mut params = Vec::new();

        for (i, qual) in quals.iter().enumerate() {
            let col = self.column_map
                .get(&qual.column)
                .cloned()
                .unwrap_or_else(|| qual.column.clone());

            let param = match self.param_style {
                ParamStyle::Numbered => format!("${}", i + 1),
                ParamStyle::Positional => "?".to_string(),
                ParamStyle::Named => format!(":{}", qual.column),
            };

            let clause = match qual.operator {
                QualOperator::Eq => format!("{} = {}", col, param),
                QualOperator::NotEq => format!("{} != {}", col, param),
                QualOperator::Lt => format!("{} < {}", col, param),
                QualOperator::LtEq => format!("{} <= {}", col, param),
                QualOperator::Gt => format!("{} > {}", col, param),
                QualOperator::GtEq => format!("{} >= {}", col, param),
                QualOperator::Like => format!("{} LIKE {}", col, param),
                QualOperator::In => format!("{} IN ({})", col, param),
                QualOperator::IsNull => format!("{} IS NULL", col),
                QualOperator::IsNotNull => format!("{} IS NOT NULL", col),
            };

            clauses.push(clause);

            // Don't add params for IS NULL / IS NOT NULL
            if !matches!(qual.operator, QualOperator::IsNull | QualOperator::IsNotNull) {
                params.push(qual.value.clone());
            }
        }

        (format!("WHERE {}", clauses.join(" AND ")), params)
    }

    /// Build SELECT statement
    pub fn build_select(
        &self,
        table: &str,
        columns: &[String],
        quals: &[Qual],
        limit: Option<usize>,
    ) -> (String, Vec<Value>) {
        let cols = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| {
                    self.column_map
                        .get(c)
                        .cloned()
                        .unwrap_or_else(|| c.clone())
                })
                .collect::<Vec<_>>()
                .join(", ")
        };

        let (where_clause, params) = self.build_where_clause(quals);

        let mut sql = format!("SELECT {} FROM {}", cols, table);

        if !where_clause.is_empty() {
            sql.push(' ');
            sql.push_str(&where_clause);
        }

        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        (sql, params)
    }

    /// Build INSERT statement
    pub fn build_insert(&self, table: &str, columns: &[String]) -> String {
        let cols = columns.join(", ");
        let placeholders: Vec<String> = (0..columns.len())
            .map(|i| match self.param_style {
                ParamStyle::Numbered => format!("${}", i + 1),
                ParamStyle::Positional => "?".to_string(),
                ParamStyle::Named => format!(":{}", columns[i]),
            })
            .collect();

        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table,
            cols,
            placeholders.join(", ")
        )
    }

    /// Build UPDATE statement
    pub fn build_update(
        &self,
        table: &str,
        assignments: &[(String, Value)],
        quals: &[Qual],
    ) -> (String, Vec<Value>) {
        let set_clauses: Vec<String> = assignments
            .iter()
            .enumerate()
            .map(|(i, (col, _))| {
                let param = match self.param_style {
                    ParamStyle::Numbered => format!("${}", i + 1),
                    ParamStyle::Positional => "?".to_string(),
                    ParamStyle::Named => format!(":{}", col),
                };
                format!("{} = {}", col, param)
            })
            .collect();

        let mut params: Vec<Value> = assignments.iter().map(|(_, v)| v.clone()).collect();

        let (where_clause, where_params) = self.build_where_clause(quals);
        params.extend(where_params);

        let sql = format!(
            "UPDATE {} SET {} {}",
            table,
            set_clauses.join(", "),
            where_clause
        );

        (sql, params)
    }

    /// Build DELETE statement
    pub fn build_delete(&self, table: &str, quals: &[Qual]) -> (String, Vec<Value>) {
        let (where_clause, params) = self.build_where_clause(quals);

        let sql = format!("DELETE FROM {} {}", table, where_clause);

        (sql, params)
    }
}

// ============================================================================
// Cost Estimation
// ============================================================================

/// Cost estimator for FDW queries
pub struct CostEstimator;

impl CostEstimator {
    /// Estimate selectivity of a qualifier
    pub fn estimate_selectivity(qual: &Qual, stats: Option<&ColumnStatistics>) -> f64 {
        // Default selectivities based on operator
        let default_selectivity = match qual.operator {
            QualOperator::Eq => 0.01,        // 1%
            QualOperator::NotEq => 0.99,     // 99%
            QualOperator::Lt | QualOperator::Gt => 0.33,  // 33%
            QualOperator::LtEq | QualOperator::GtEq => 0.33,
            QualOperator::Like => 0.05,      // 5%
            QualOperator::In => 0.10,        // 10%
            QualOperator::IsNull => 0.01,    // 1%
            QualOperator::IsNotNull => 0.99, // 99%
        };

        // Adjust based on statistics if available
        if let Some(stats) = stats {
            let total = stats.distinct_count.max(1) as f64;

            match qual.operator {
                QualOperator::Eq => 1.0 / total,
                QualOperator::NotEq => 1.0 - (1.0 / total),
                QualOperator::IsNull => {
                    stats.null_count as f64 / total.max(stats.null_count as f64)
                }
                QualOperator::IsNotNull => {
                    1.0 - (stats.null_count as f64 / total.max(stats.null_count as f64))
                }
                _ => default_selectivity,
            }
        } else {
            default_selectivity
        }
    }

    /// Estimate combined selectivity for multiple qualifiers
    pub fn estimate_combined_selectivity(
        quals: &[Qual],
        stats: &HashMap<String, ColumnStatistics>,
    ) -> f64 {
        let mut selectivity = 1.0;

        for qual in quals {
            let col_stats = stats.get(&qual.column);
            selectivity *= Self::estimate_selectivity(qual, col_stats);
        }

        selectivity.max(0.0001) // Don't go below 0.01%
    }

    /// Estimate cost of a foreign scan
    pub fn estimate_scan_cost(
        base_rows: usize,
        selectivity: f64,
        network_cost: f64,
        per_row_cost: f64,
    ) -> f64 {
        let estimated_rows = (base_rows as f64 * selectivity).max(1.0);
        network_cost + (estimated_rows * per_row_cost)
    }
}

// ============================================================================
// Schema Mapping
// ============================================================================

/// Maps between local and foreign schemas
pub struct SchemaMapper {
    /// Column mappings by table
    table_mappings: HashMap<String, TableMapping>,
}

/// Table mapping definition
#[derive(Debug, Clone)]
pub struct TableMapping {
    /// Local table name
    pub local_name: String,
    /// Remote table name
    pub remote_name: String,
    /// Column mappings (local -> remote)
    pub columns: HashMap<String, String>,
    /// Remote schema/database
    pub remote_schema: Option<String>,
}

impl SchemaMapper {
    pub fn new() -> Self {
        Self {
            table_mappings: HashMap::new(),
        }
    }

    /// Add a table mapping
    pub fn add_table(&mut self, mapping: TableMapping) {
        self.table_mappings.insert(mapping.local_name.clone(), mapping);
    }

    /// Get remote table name
    pub fn get_remote_table(&self, local_table: &str) -> Option<&str> {
        self.table_mappings
            .get(local_table)
            .map(|m| m.remote_name.as_str())
    }

    /// Get remote column name
    pub fn get_remote_column(&self, local_table: &str, local_column: &str) -> Option<&str> {
        self.table_mappings
            .get(local_table)
            .and_then(|m| m.columns.get(local_column))
            .map(|s| s.as_str())
    }

    /// Get full remote table reference
    pub fn get_remote_reference(&self, local_table: &str) -> Option<String> {
        self.table_mappings.get(local_table).map(|m| {
            if let Some(schema) = &m.remote_schema {
                format!("{}.{}", schema, m.remote_name)
            } else {
                m.remote_name.clone()
            }
        })
    }
}

impl Default for SchemaMapper {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_builder_select() {
        let builder = QueryBuilder::new(ParamStyle::Numbered);
        let (sql, params) = builder.build_select(
            "users",
            &["id".to_string(), "name".to_string()],
            &[Qual {
                column: "age".to_string(),
                operator: QualOperator::Gt,
                value: Value::Int64(18),
            }],
            Some(10),
        );

        assert!(sql.contains("SELECT id, name FROM users"));
        assert!(sql.contains("WHERE age > $1"));
        assert!(sql.contains("LIMIT 10"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_query_builder_insert() {
        let builder = QueryBuilder::new(ParamStyle::Positional);
        let sql = builder.build_insert("users", &["id".to_string(), "name".to_string()]);

        assert_eq!(sql, "INSERT INTO users (id, name) VALUES (?, ?)");
    }

    #[test]
    fn test_cost_estimator() {
        let qual = Qual {
            column: "id".to_string(),
            operator: QualOperator::Eq,
            value: Value::Int64(1),
        };

        let sel = CostEstimator::estimate_selectivity(&qual, None);
        assert!(sel > 0.0 && sel < 1.0);
    }

    #[test]
    fn test_schema_mapper() {
        let mut mapper = SchemaMapper::new();
        mapper.add_table(TableMapping {
            local_name: "users".to_string(),
            remote_name: "customers".to_string(),
            columns: HashMap::from([
                ("user_id".to_string(), "customer_id".to_string()),
            ]),
            remote_schema: Some("public".to_string()),
        });

        assert_eq!(mapper.get_remote_table("users"), Some("customers"));
        assert_eq!(mapper.get_remote_column("users", "user_id"), Some("customer_id"));
        assert_eq!(mapper.get_remote_reference("users"), Some("public.customers".to_string()));
    }
}
