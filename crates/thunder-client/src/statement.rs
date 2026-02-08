//! Prepared Statements
//!
//! Provides prepared statement support with parameter binding.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use thunder_common::error::QueryError;
use thunder_common::prelude::*;

use crate::pool::PooledConnection;
use crate::QueryResult;

// ============================================================================
// Statement Cache
// ============================================================================

/// Statement cache for reusing prepared statements
pub struct StatementCache {
    cache: RwLock<HashMap<String, Arc<CachedStatement>>>,
    max_size: usize,
    counter: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl StatementCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_size,
            counter: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get_or_create(&self, sql: &str) -> Arc<CachedStatement> {
        if let Some(stmt) = self.cache.read().get(sql) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Arc::clone(stmt);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);

        let name = format!("__stmt_{}", self.counter.fetch_add(1, Ordering::Relaxed));
        let stmt = Arc::new(CachedStatement {
            name,
            sql: sql.to_string(),
            param_count: count_parameters(sql),
        });

        let mut cache = self.cache.write();
        if cache.len() >= self.max_size {
            if let Some(key) = cache.keys().next().cloned() {
                cache.remove(&key);
            }
        }
        cache.insert(sql.to_string(), Arc::clone(&stmt));

        stmt
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.cache.read().len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }

    pub fn clear(&self) {
        self.cache.write().clear();
    }
}

impl Default for StatementCache {
    fn default() -> Self {
        Self::new(100)
    }
}

pub struct CachedStatement {
    pub name: String,
    pub sql: String,
    pub param_count: usize,
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub hits: u64,
    pub misses: u64,
}

// ============================================================================
// Prepared Statement
// ============================================================================

pub struct PreparedStatement {
    name: String,
    sql: String,
    param_count: usize,
}

impl PreparedStatement {
    pub fn new(name: String, sql: String) -> Self {
        let param_count = count_parameters(&sql);
        Self { name, sql, param_count }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn param_count(&self) -> usize {
        self.param_count
    }

    pub fn bind(&self, params: &[Value]) -> Result<BoundStatement> {
        if params.len() != self.param_count {
            return Err(Error::Query(QueryError::ExecutionError(format!(
                "Expected {} parameters, got {}",
                self.param_count,
                params.len()
            ))));
        }

        Ok(BoundStatement {
            sql: self.sql.clone(),
            params: params.to_vec(),
        })
    }

    pub async fn execute(&self, conn: &mut PooledConnection, params: &[Value]) -> Result<u64> {
        let bound = self.bind(params)?;
        bound.execute(conn).await
    }

    pub async fn query(&self, conn: &mut PooledConnection, params: &[Value]) -> Result<QueryResult> {
        let bound = self.bind(params)?;
        bound.query(conn).await
    }
}

// ============================================================================
// Bound Statement
// ============================================================================

pub struct BoundStatement {
    sql: String,
    params: Vec<Value>,
}

impl BoundStatement {
    pub async fn execute(&self, conn: &mut PooledConnection) -> Result<u64> {
        let sql = self.interpolate_params();
        conn.execute(&sql).await
    }

    pub async fn query(&self, conn: &mut PooledConnection) -> Result<QueryResult> {
        let sql = self.interpolate_params();
        conn.query(&sql).await
    }

    fn interpolate_params(&self) -> String {
        let mut result = self.sql.clone();

        for (i, param) in self.params.iter().enumerate().rev() {
            let placeholder = format!("${}", i + 1);
            let value_str = encode_param(param);
            result = result.replace(&placeholder, &value_str);
        }

        result
    }
}

// ============================================================================
// Parameter Encoding
// ============================================================================

fn count_parameters(sql: &str) -> usize {
    let mut max_param = 0;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' {
            let mut num_str = String::new();
            while let Some(&d) = chars.peek() {
                if d.is_ascii_digit() {
                    num_str.push(d);
                    chars.next();
                } else {
                    break;
                }
            }
            if let Ok(num) = num_str.parse::<usize>() {
                max_param = max_param.max(num);
            }
        }
    }

    max_param
}

fn encode_param(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int8(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Decimal(v, scale) => {
            let divisor = 10i128.pow(*scale as u32);
            format!("{}.{:0>width$}", v / divisor, (v % divisor).abs(), width = *scale as usize)
        }
        Value::String(s) => format!("'{}'", escape_string(s)),
        Value::Binary(b) => format!("'\\x{}'", hex::encode(b.as_ref())),
        v @ Value::Date(_) => format!("'{}'::date", v),
        v @ Value::Time(_) => format!("'{}'::time", v),
        v @ Value::Timestamp(_) => format!("'{}'::timestamp", v),
        v @ Value::TimestampTz(_, _) => format!("'{}'::timestamptz", v),
        Value::Uuid(u) => format!("'{}'::uuid", uuid::Uuid::from_bytes(*u)),
        Value::Json(j) => format!("'{}'::jsonb", escape_string(j)),
        Value::Vector(v) => {
            let vals: Vec<String> = v.iter().map(|x| x.to_string()).collect();
            format!("'[{}]'::vector", vals.join(","))
        }
        Value::Array(arr) => {
            let vals: Vec<String> = arr.iter().map(encode_param).collect();
            format!("ARRAY[{}]", vals.join(","))
        }
    }
}

fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

// ============================================================================
// Statement Builder
// ============================================================================

pub struct StatementBuilder {
    sql: String,
    params: Vec<Value>,
}

impl StatementBuilder {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    pub fn param(mut self, value: impl Into<Value>) -> Self {
        self.params.push(value.into());
        self
    }

    pub fn params(mut self, values: impl IntoIterator<Item = impl Into<Value>>) -> Self {
        for v in values {
            self.params.push(v.into());
        }
        self
    }

    pub async fn execute(self, conn: &mut PooledConnection) -> Result<u64> {
        let stmt = PreparedStatement::new(String::new(), self.sql);
        stmt.execute(conn, &self.params).await
    }

    pub async fn query(self, conn: &mut PooledConnection) -> Result<QueryResult> {
        let stmt = PreparedStatement::new(String::new(), self.sql);
        stmt.query(conn, &self.params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_parameters() {
        assert_eq!(count_parameters("SELECT * FROM users"), 0);
        assert_eq!(count_parameters("SELECT * FROM users WHERE id = $1"), 1);
        assert_eq!(count_parameters("INSERT INTO users (a, b) VALUES ($1, $2)"), 2);
    }

    #[test]
    fn test_encode_param() {
        assert_eq!(encode_param(&Value::Null), "NULL");
        assert_eq!(encode_param(&Value::Int32(42)), "42");
        assert_eq!(encode_param(&Value::Boolean(true)), "TRUE");
    }

    #[test]
    fn test_statement_cache() {
        let cache = StatementCache::new(10);
        let stmt1 = cache.get_or_create("SELECT 1");
        let stmt2 = cache.get_or_create("SELECT 1");
        assert_eq!(stmt1.name, stmt2.name);
    }
}
