//! Redis Foreign Data Wrapper
//!
//! Implements FDW for querying Redis databases:
//! - Key-value access as table rows
//! - Hash fields as columns
//! - Pattern-based key scanning
//! - CRUD operations

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use tracing::{debug, info};

use crate::{
    ColumnStatistics, FdwCapabilities, ForeignDataWrapper, ForeignScan, ForeignServer,
    ForeignTableDef, ModifyOperation, Qual, QualOperator,
};
use crate::FieldDef;
use thunder_common::prelude::*;

// ============================================================================
// Redis FDW
// ============================================================================

/// Redis Foreign Data Wrapper
pub struct RedisFdw {
    /// Server configuration
    server: ForeignServer,
    /// Redis client
    client: Option<Client>,
    /// Connection
    connection: Option<MultiplexedConnection>,
    /// Key pattern (prefix)
    key_prefix: String,
    /// Data type (hash, string, list, set, zset)
    data_type: RedisDataType,
    /// Schema for hash fields
    hash_schema: Option<Schema>,
    /// Statistics
    statistics: HashMap<String, ColumnStatistics>,
}

/// Redis data types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisDataType {
    String,
    Hash,
    List,
    Set,
    ZSet,
}

impl RedisFdw {
    /// Create a new Redis FDW
    pub async fn new(server: ForeignServer) -> Result<Self> {
        let key_prefix = server
            .options
            .get("key_prefix")
            .cloned()
            .unwrap_or_default();

        let data_type = match server
            .options
            .get("data_type")
            .map(|s| s.as_str())
            .unwrap_or("hash")
        {
            "string" => RedisDataType::String,
            "hash" => RedisDataType::Hash,
            "list" => RedisDataType::List,
            "set" => RedisDataType::Set,
            "zset" => RedisDataType::ZSet,
            _ => RedisDataType::Hash,
        };

        Ok(Self {
            server,
            client: None,
            connection: None,
            key_prefix,
            data_type,
            hash_schema: None,
            statistics: HashMap::new(),
        })
    }

    /// Connect to Redis
    pub async fn connect(&mut self) -> Result<()> {
        if self.connection.is_some() {
            return Ok(());
        }

        let url = self
            .server
            .options
            .get("url")
            .cloned()
            .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());

        let client = Client::open(url.as_str())
            .map_err(|e| Error::Internal(format!("Redis client creation failed: {}", e)))?;

        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Internal(format!("Redis connection failed: {}", e)))?;

        self.client = Some(client);
        self.connection = Some(connection);

        info!(server = %self.server.name, "Redis FDW connected");

        Ok(())
    }

    /// Get connection reference
    #[allow(dead_code)]
    fn connection(&mut self) -> Result<&mut MultiplexedConnection> {
        self.connection
            .as_mut()
            .ok_or_else(|| Error::Internal("Not connected".to_string()))
    }

    /// Build key pattern from qualifiers
    fn build_key_pattern(&self, quals: &[Qual]) -> String {
        // Look for key = 'value' qualifier
        for qual in quals {
            if qual.column == "key" && qual.operator == QualOperator::Eq {
                if let Value::String(s) = &qual.value {
                    return format!("{}{}", self.key_prefix, s);
                }
            }
        }

        // Default: scan all keys with prefix
        format!("{}*", self.key_prefix)
    }

    /// Infer schema from sample keys
    #[allow(dead_code)]
    async fn infer_schema(&mut self) -> Result<Schema> {
        if let Some(schema) = &self.hash_schema {
            return Ok(schema.clone());
        }

        // Copy values needed before borrowing connection
        let data_type = self.data_type;
        let key_prefix = self.key_prefix.clone();

        // Key column is always present
        let mut fields = vec![FieldDef {
            name: "key".to_string(),
            data_type: DataType::String,
            nullable: false,
            default: None,
        }];

        match data_type {
            RedisDataType::String => {
                fields.push(FieldDef {
                    name: "value".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                });
            }
            RedisDataType::Hash => {
                // Sample a key to get hash fields
                let pattern = format!("{}*", key_prefix);
                let conn = self.connection()?;
                let keys: Vec<String> = redis::cmd("SCAN")
                    .arg(0)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(1)
                    .query_async(conn)
                    .await
                    .map(|(_, keys): (u64, Vec<String>)| keys)
                    .unwrap_or_default();

                if let Some(key) = keys.first() {
                    let conn = self.connection()?;
                    let field_names: Vec<String> = conn.hkeys(key).await.unwrap_or_default();

                    for field_name in field_names {
                        fields.push(FieldDef {
                            name: field_name,
                            data_type: DataType::String,
                            nullable: true,
                            default: None,
                        });
                    }
                }
            }
            RedisDataType::List | RedisDataType::Set => {
                fields.push(FieldDef {
                    name: "value".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                });
                fields.push(FieldDef {
                    name: "index".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    default: None,
                });
            }
            RedisDataType::ZSet => {
                fields.push(FieldDef {
                    name: "member".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                });
                fields.push(FieldDef {
                    name: "score".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    default: None,
                });
            }
        }

        let schema = Schema { columns: fields };
        self.hash_schema = Some(schema.clone());

        Ok(schema)
    }
}

#[async_trait]
impl ForeignDataWrapper for RedisFdw {
    fn name(&self) -> &str {
        "redis"
    }

    fn estimate_size(&self, quals: &[Qual]) -> Result<(usize, f64)> {
        // Check if we have a specific key
        let has_key_eq = quals.iter().any(|q| {
            q.column == "key" && q.operator == QualOperator::Eq
        });

        if has_key_eq {
            // Single key lookup
            Ok((1, 0.1))
        } else {
            // Key scan
            Ok((1000, 10.0))
        }
    }

    fn get_statistics(&self, column: &str) -> Option<ColumnStatistics> {
        self.statistics.get(column).cloned()
    }

    async fn begin_scan(
        &self,
        columns: &[String],
        quals: &[Qual],
        limit: Option<usize>,
    ) -> Result<Box<dyn ForeignScan>> {
        let conn = self.connection.clone().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        let key_pattern = self.build_key_pattern(quals);
        let schema = self.hash_schema.clone().unwrap_or_else(|| Schema {
            columns: vec![FieldDef {
                name: "key".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
            }],
        });

        debug!(
            pattern = %key_pattern,
            data_type = ?self.data_type,
            "Redis FDW scan"
        );

        let scan = RedisScan::new(
            conn,
            key_pattern,
            self.data_type,
            schema,
            columns.to_vec(),
            limit,
            1000,
        )
        .await?;

        Ok(Box::new(scan))
    }

    async fn modify(&self, op: ModifyOperation) -> Result<u64> {
        let mut conn = self.connection.clone().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        match op {
            ModifyOperation::Insert { table: _, rows } => {
                if rows.is_empty() {
                    return Ok(0);
                }

                let schema = self.hash_schema.as_ref().ok_or_else(|| {
                    Error::Internal("Schema not available".to_string())
                })?;

                let mut count = 0u64;

                for row in rows {
                    // First column should be the key
                    let key = if let Some(Value::String(k)) = row.values.first() {
                        format!("{}{}", self.key_prefix, k)
                    } else {
                        continue;
                    };

                    match self.data_type {
                        RedisDataType::String => {
                            if row.values.len() > 1 {
                                if let Value::String(v) = &row.values[1] {
                                    let _: () = conn.set(&key, v.as_ref()).await.map_err(|e| {
                                        Error::Internal(format!("Set failed: {}", e))
                                    })?;
                                    count += 1;
                                }
                            }
                        }
                        RedisDataType::Hash => {
                            let mut hash_values: Vec<(String, String)> = Vec::new();

                            for (i, field) in schema.columns.iter().enumerate().skip(1) {
                                if i < row.values.len() {
                                    let value_str = match &row.values[i] {
                                        Value::String(s) => s.to_string(),
                                        Value::Int64(n) => n.to_string(),
                                        Value::Float64(f) => f.to_string(),
                                        Value::Boolean(b) => b.to_string(),
                                        _ => continue,
                                    };
                                    hash_values.push((field.name.clone(), value_str));
                                }
                            }

                            if !hash_values.is_empty() {
                                let _: () = conn.hset_multiple(&key, &hash_values).await.map_err(|e| {
                                    Error::Internal(format!("HSET failed: {}", e))
                                })?;
                                count += 1;
                            }
                        }
                        _ => {
                            return Err(Error::Internal(format!(
                                "Insert not supported for {:?}",
                                self.data_type
                            )));
                        }
                    }
                }

                Ok(count)
            }
            ModifyOperation::Update {
                table: _,
                assignments,
                quals,
            } => {
                // Get key from quals
                let key = quals
                    .iter()
                    .find(|q| q.column == "key" && q.operator == QualOperator::Eq)
                    .and_then(|q| {
                        if let Value::String(s) = &q.value {
                            Some(format!("{}{}", self.key_prefix, s))
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| Error::Internal("Key required for update".to_string()))?;

                match self.data_type {
                    RedisDataType::String => {
                        if let Some((_, value)) = assignments.first() {
                            if let Value::String(v) = value {
                                let _: () = conn.set(&key, v.as_ref()).await.map_err(|e| {
                                    Error::Internal(format!("Set failed: {}", e))
                                })?;
                                return Ok(1);
                            }
                        }
                    }
                    RedisDataType::Hash => {
                        let hash_values: Vec<(String, String)> = assignments
                            .iter()
                            .filter_map(|(col, val)| {
                                let value_str = match val {
                                    Value::String(s) => s.to_string(),
                                    Value::Int64(n) => n.to_string(),
                                    Value::Float64(f) => f.to_string(),
                                    Value::Boolean(b) => b.to_string(),
                                    _ => return None,
                                };
                                Some((col.clone(), value_str))
                            })
                            .collect();

                        if !hash_values.is_empty() {
                            let _: () = conn.hset_multiple(&key, &hash_values).await.map_err(|e| {
                                Error::Internal(format!("HSET failed: {}", e))
                            })?;
                            return Ok(1);
                        }
                    }
                    _ => {
                        return Err(Error::Internal(format!(
                            "Update not supported for {:?}",
                            self.data_type
                        )));
                    }
                }

                Ok(0)
            }
            ModifyOperation::Delete { table: _, quals } => {
                // Get key from quals
                let key = quals
                    .iter()
                    .find(|q| q.column == "key" && q.operator == QualOperator::Eq)
                    .and_then(|q| {
                        if let Value::String(s) = &q.value {
                            Some(format!("{}{}", self.key_prefix, s))
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| Error::Internal("Key required for delete".to_string()))?;

                let deleted: i64 = conn.del(&key).await.map_err(|e| {
                    Error::Internal(format!("DEL failed: {}", e))
                })?;

                Ok(deleted as u64)
            }
        }
    }

    async fn import_schema(&self, _schema: &str) -> Result<Vec<ForeignTableDef>> {
        // Redis doesn't have traditional schemas
        // Return a single table based on data type
        let mut fields = vec![FieldDef {
            name: "key".to_string(),
            data_type: DataType::String,
            nullable: false,
            default: None,
        }];

        match self.data_type {
            RedisDataType::String => {
                fields.push(FieldDef {
                    name: "value".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                });
            }
            RedisDataType::Hash => {
                fields.push(FieldDef {
                    name: "field".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                });
                fields.push(FieldDef {
                    name: "value".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                });
            }
            _ => {
                fields.push(FieldDef {
                    name: "value".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                });
            }
        }

        Ok(vec![ForeignTableDef {
            name: "redis_data".to_string(),
            schema: Schema { columns: fields },
            options: HashMap::new(),
        }])
    }

    fn capabilities(&self) -> FdwCapabilities {
        FdwCapabilities {
            supports_predicate_pushdown: true,
            supports_limit_pushdown: true,
            supports_aggregation_pushdown: false,
            supports_modification: true,
            supports_transactions: false, // Redis transactions are different
            max_connections: 100,
        }
    }
}

// ============================================================================
// Redis Scan
// ============================================================================

/// Redis foreign scan implementation
pub struct RedisScan {
    /// Rows data
    rows: Vec<Vec<String>>,
    /// Current position
    position: usize,
    /// Schema
    schema: Schema,
    /// Batch size
    batch_size: usize,
}

impl RedisScan {
    async fn new(
        mut conn: MultiplexedConnection,
        key_pattern: String,
        data_type: RedisDataType,
        schema: Schema,
        _columns: Vec<String>,
        limit: Option<usize>,
        batch_size: usize,
    ) -> Result<Self> {
        let mut rows = Vec::new();

        // Check if it's a specific key or pattern
        if !key_pattern.contains('*') {
            // Specific key lookup
            match data_type {
                RedisDataType::String => {
                    if let Ok(value) = conn.get::<_, String>(&key_pattern).await {
                        rows.push(vec![key_pattern.clone(), value]);
                    }
                }
                RedisDataType::Hash => {
                    if let Ok(hash_data) = conn.hgetall::<_, HashMap<String, String>>(&key_pattern).await {
                        let mut row = vec![key_pattern.clone()];
                        for field in &schema.columns[1..] {
                            row.push(hash_data.get(&field.name).cloned().unwrap_or_default());
                        }
                        rows.push(row);
                    }
                }
                _ => {}
            }
        } else {
            // Pattern scan
            let mut cursor = 0u64;
            let scan_limit = limit.unwrap_or(10000);

            loop {
                let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&key_pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| Error::Internal(format!("SCAN failed: {}", e)))?;

                for key in keys {
                    if rows.len() >= scan_limit {
                        break;
                    }

                    match data_type {
                        RedisDataType::String => {
                            if let Ok(value) = conn.get::<_, String>(&key).await {
                                rows.push(vec![key, value]);
                            }
                        }
                        RedisDataType::Hash => {
                            if let Ok(hash_data) = conn.hgetall::<_, HashMap<String, String>>(&key).await {
                                let mut row = vec![key];
                                for field in &schema.columns[1..] {
                                    row.push(hash_data.get(&field.name).cloned().unwrap_or_default());
                                }
                                rows.push(row);
                            }
                        }
                        RedisDataType::List => {
                            if let Ok(values) = conn.lrange::<_, Vec<String>>(&key, 0, -1).await {
                                for (i, value) in values.into_iter().enumerate() {
                                    rows.push(vec![key.clone(), value, i.to_string()]);
                                }
                            }
                        }
                        RedisDataType::Set => {
                            if let Ok(members) = conn.smembers::<_, Vec<String>>(&key).await {
                                for (i, member) in members.into_iter().enumerate() {
                                    rows.push(vec![key.clone(), member, i.to_string()]);
                                }
                            }
                        }
                        RedisDataType::ZSet => {
                            if let Ok(members) = conn.zrange_withscores::<_, Vec<(String, f64)>>(&key, 0, -1).await {
                                for (member, score) in members {
                                    rows.push(vec![key.clone(), member, score.to_string()]);
                                }
                            }
                        }
                    }
                }

                cursor = new_cursor;
                if cursor == 0 || rows.len() >= scan_limit {
                    break;
                }
            }
        }

        Ok(Self {
            rows,
            position: 0,
            schema,
            batch_size,
        })
    }
}

#[async_trait]
impl ForeignScan for RedisScan {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.position >= self.rows.len() {
            return Ok(None);
        }

        let end = (self.position + self.batch_size).min(self.rows.len());
        let batch_rows = &self.rows[self.position..end];
        self.position = end;

        if batch_rows.is_empty() {
            return Ok(None);
        }

        // Build Arrow arrays
        let arrow_schema = schema_to_arrow(&self.schema);
        let mut columns: Vec<ArrayRef> = Vec::new();

        for (i, _field) in self.schema.columns.iter().enumerate() {
            let values: Vec<Option<String>> = batch_rows
                .iter()
                .map(|row| row.get(i).cloned())
                .collect();
            columns.push(Arc::new(StringArray::from(values)));
        }

        let batch = RecordBatch::try_new(arrow_schema, columns)
            .map_err(|e| Error::Internal(format!("Failed to create RecordBatch: {}", e)))?;

        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn reset(&mut self) -> Result<()> {
        self.position = 0;
        Ok(())
    }
}

/// Convert Schema to Arrow schema
fn schema_to_arrow(schema: &Schema) -> Arc<ArrowSchema> {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|f| Field::new(&f.name, ArrowDataType::Utf8, f.nullable))
        .collect();

    Arc::new(ArrowSchema::new(fields))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities() {
        let server = ForeignServer {
            name: "test".to_string(),
            connector_type: "redis".to_string(),
            options: HashMap::new(),
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let fdw = rt.block_on(RedisFdw::new(server)).unwrap();

        let caps = fdw.capabilities();
        assert!(caps.supports_predicate_pushdown);
        assert!(caps.supports_limit_pushdown);
        assert!(caps.supports_modification);
        assert!(!caps.supports_transactions);
    }

    #[test]
    fn test_key_pattern_building() {
        let mut server = ForeignServer {
            name: "test".to_string(),
            connector_type: "redis".to_string(),
            options: HashMap::new(),
        };
        server.options.insert("key_prefix".to_string(), "user:".to_string());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let fdw = rt.block_on(RedisFdw::new(server)).unwrap();

        // With specific key
        let quals = vec![Qual {
            column: "key".to_string(),
            operator: QualOperator::Eq,
            value: Value::String("123".into()),
        }];
        assert_eq!(fdw.build_key_pattern(&quals), "user:123");

        // Without specific key
        let empty_quals: Vec<Qual> = vec![];
        assert_eq!(fdw.build_key_pattern(&empty_quals), "user:*");
    }

    #[test]
    fn test_data_type_parsing() {
        let mut server = ForeignServer {
            name: "test".to_string(),
            connector_type: "redis".to_string(),
            options: HashMap::new(),
        };

        // Default is hash
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fdw = rt.block_on(RedisFdw::new(server.clone())).unwrap();
        assert_eq!(fdw.data_type, RedisDataType::Hash);

        // String type
        server.options.insert("data_type".to_string(), "string".to_string());
        let fdw = rt.block_on(RedisFdw::new(server.clone())).unwrap();
        assert_eq!(fdw.data_type, RedisDataType::String);

        // ZSet type
        server.options.insert("data_type".to_string(), "zset".to_string());
        let fdw = rt.block_on(RedisFdw::new(server)).unwrap();
        assert_eq!(fdw.data_type, RedisDataType::ZSet);
    }
}
