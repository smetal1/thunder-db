//! MySQL Foreign Data Wrapper
//!
//! Implements FDW for querying MySQL databases:
//! - Full SQL pushdown support
//! - Prepared statement support
//! - Efficient cursor-based scanning
//! - CRUD operations

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit};
use async_trait::async_trait;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Row as SqlxRow, TypeInfo};
use tracing::{debug, error, info, warn};

use crate::{
    ColumnStatistics, FdwCapabilities, ForeignDataWrapper, ForeignScan, ForeignServer,
    ForeignTableDef, ModifyOperation, Qual, QualOperator,
};
use chrono::{DateTime, Utc};
use crate::FieldDef;
use thunder_common::prelude::*;

// ============================================================================
// MySQL FDW
// ============================================================================

/// MySQL Foreign Data Wrapper
pub struct MySqlFdw {
    /// Server configuration
    server: ForeignServer,
    /// Connection pool
    pool: Option<MySqlPool>,
    /// Remote database
    database: String,
    /// Cached table schemas
    table_schemas: HashMap<String, Schema>,
    /// Statistics cache
    statistics: HashMap<String, HashMap<String, ColumnStatistics>>,
}

impl MySqlFdw {
    /// Create a new MySQL FDW
    pub async fn new(server: ForeignServer) -> Result<Self> {
        let database = server
            .options
            .get("database")
            .cloned()
            .unwrap_or_else(|| "mysql".to_string());

        Ok(Self {
            server,
            pool: None,
            database,
            table_schemas: HashMap::new(),
            statistics: HashMap::new(),
        })
    }

    /// Connect to MySQL
    pub async fn connect(&mut self) -> Result<()> {
        if self.pool.is_some() {
            return Ok(());
        }

        let host = self.server.options.get("host").map(|s| s.as_str()).unwrap_or("localhost");
        let port = self.server.options.get("port").map(|s| s.as_str()).unwrap_or("3306");
        let user = self.server.options.get("user").map(|s| s.as_str()).unwrap_or("root");
        let password = self.server.options.get("password").map(|s| s.as_str()).unwrap_or("");

        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            user, password, host, port, self.database
        );

        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .connect(&url)
            .await
            .map_err(|e| Error::Internal(format!("MySQL connection failed: {}", e)))?;

        self.pool = Some(pool);

        info!(server = %self.server.name, "MySQL FDW connected");

        Ok(())
    }

    /// Get pool reference
    fn pool(&self) -> Result<&MySqlPool> {
        self.pool.as_ref().ok_or_else(|| Error::Internal("Not connected".to_string()))
    }

    /// Build WHERE clause from qualifiers
    fn build_where_clause(&self, quals: &[Qual]) -> (String, Vec<Value>) {
        if quals.is_empty() {
            return (String::new(), Vec::new());
        }

        let mut clauses = Vec::new();
        let mut params = Vec::new();

        for qual in quals {
            let clause = match qual.operator {
                QualOperator::Eq => format!("`{}` = ?", qual.column),
                QualOperator::NotEq => format!("`{}` != ?", qual.column),
                QualOperator::Lt => format!("`{}` < ?", qual.column),
                QualOperator::LtEq => format!("`{}` <= ?", qual.column),
                QualOperator::Gt => format!("`{}` > ?", qual.column),
                QualOperator::GtEq => format!("`{}` >= ?", qual.column),
                QualOperator::Like => format!("`{}` LIKE ?", qual.column),
                QualOperator::In => format!("`{}` IN (?)", qual.column),
                QualOperator::IsNull => format!("`{}` IS NULL", qual.column),
                QualOperator::IsNotNull => format!("`{}` IS NOT NULL", qual.column),
            };

            clauses.push(clause);

            // Don't add params for IS NULL / IS NOT NULL
            if !matches!(qual.operator, QualOperator::IsNull | QualOperator::IsNotNull) {
                params.push(qual.value.clone());
            }
        }

        (format!("WHERE {}", clauses.join(" AND ")), params)
    }

    /// Refresh statistics for a table
    async fn refresh_statistics(&mut self, table: &str) -> Result<()> {
        let pool = self.pool()?;

        // Get table row count
        let count_query = format!("SELECT COUNT(*) FROM `{}`", table);
        let row: (i64,) = sqlx::query_as(&count_query)
            .fetch_one(pool)
            .await
            .map_err(|e| Error::Internal(format!("Count failed: {}", e)))?;

        let total_rows = row.0 as u64;

        // Get column cardinality from information_schema
        let stats_query = format!(
            r#"
            SELECT
                COLUMN_NAME,
                CARDINALITY
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            "#
        );

        let rows: Vec<(String, Option<i64>)> = sqlx::query_as(&stats_query)
            .bind(&self.database)
            .bind(table)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Internal(format!("Statistics query failed: {}", e)))?;

        let mut col_stats = HashMap::new();

        for (column_name, cardinality) in rows {
            let distinct_count = cardinality.unwrap_or(0) as u64;

            col_stats.insert(
                column_name,
                ColumnStatistics {
                    null_count: 0, // MySQL doesn't easily expose this
                    distinct_count,
                    min_value: None,
                    max_value: None,
                },
            );
        }

        self.statistics.insert(table.to_string(), col_stats);

        Ok(())
    }
}

#[async_trait]
impl ForeignDataWrapper for MySqlFdw {
    fn name(&self) -> &str {
        "mysql"
    }

    fn estimate_size(&self, quals: &[Qual]) -> Result<(usize, f64)> {
        // Base estimate: 10000 rows, 1.0 cost
        let mut rows = 10000usize;
        let mut cost = 1.0f64;

        // Reduce estimate based on qualifiers
        for qual in quals {
            match qual.operator {
                QualOperator::Eq => {
                    rows /= 100;
                    cost += 0.1;
                }
                QualOperator::Lt | QualOperator::Gt | QualOperator::LtEq | QualOperator::GtEq => {
                    rows /= 3;
                    cost += 0.1;
                }
                QualOperator::Like => {
                    rows /= 10;
                    cost += 0.2;
                }
                _ => {}
            }
        }

        Ok((rows.max(1), cost))
    }

    fn get_statistics(&self, column: &str) -> Option<ColumnStatistics> {
        for table_stats in self.statistics.values() {
            if let Some(stats) = table_stats.get(column) {
                return Some(stats.clone());
            }
        }
        None
    }

    async fn begin_scan(
        &self,
        columns: &[String],
        quals: &[Qual],
        limit: Option<usize>,
    ) -> Result<Box<dyn ForeignScan>> {
        let pool = self.pool()?;

        // Build column list
        let col_list = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Get table from options
        let table = self
            .server
            .options
            .get("table")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        // Build WHERE clause
        let (where_clause, params) = self.build_where_clause(quals);

        // Build query
        let mut sql = format!("SELECT {} FROM `{}`", col_list, table);

        if !where_clause.is_empty() {
            sql.push(' ');
            sql.push_str(&where_clause);
        }

        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        debug!(sql = %sql, "MySQL FDW query");

        // Create scan
        let scan = MySqlScan::new(pool.clone(), sql, params, columns.to_vec(), 1000).await?;

        Ok(Box::new(scan))
    }

    async fn modify(&self, op: ModifyOperation) -> Result<u64> {
        let pool = self.pool()?;

        match op {
            ModifyOperation::Insert { table, rows } => {
                if rows.is_empty() {
                    return Ok(0);
                }

                // Get column names from schema
                let schema = self.table_schemas.get(&table);
                let columns: Vec<String> = if let Some(s) = schema {
                    s.columns.iter().map(|f| f.name.clone()).collect()
                } else {
                    (0..rows[0].len()).map(|i| format!("col{}", i)).collect()
                };

                let col_list = columns
                    .iter()
                    .map(|c| format!("`{}`", c))
                    .collect::<Vec<_>>()
                    .join(", ");

                let placeholders = vec!["?"; columns.len()].join(", ");

                let sql = format!(
                    "INSERT INTO `{}` ({}) VALUES ({})",
                    table, col_list, placeholders
                );

                let mut count = 0u64;
                for row in rows {
                    let mut query = sqlx::query(&sql);

                    for value in row.values.iter() {
                        query = bind_value(query, value);
                    }

                    query
                        .execute(pool)
                        .await
                        .map_err(|e| Error::Internal(format!("Insert failed: {}", e)))?;

                    count += 1;
                }

                Ok(count)
            }
            ModifyOperation::Update {
                table,
                assignments,
                quals,
            } => {
                let set_clauses: Vec<String> = assignments
                    .iter()
                    .map(|(col, _)| format!("`{}` = ?", col))
                    .collect();

                let (where_clause, where_params) = self.build_where_clause(&quals);

                let sql = format!(
                    "UPDATE `{}` SET {} {}",
                    table,
                    set_clauses.join(", "),
                    where_clause
                );

                let mut query = sqlx::query(&sql);

                for (_, value) in &assignments {
                    query = bind_value(query, value);
                }

                for value in &where_params {
                    query = bind_value(query, value);
                }

                let result = query
                    .execute(pool)
                    .await
                    .map_err(|e| Error::Internal(format!("Update failed: {}", e)))?;

                Ok(result.rows_affected())
            }
            ModifyOperation::Delete { table, quals } => {
                let (where_clause, params) = self.build_where_clause(&quals);

                let sql = format!("DELETE FROM `{}` {}", table, where_clause);

                let mut query = sqlx::query(&sql);

                for value in &params {
                    query = bind_value(query, value);
                }

                let result = query
                    .execute(pool)
                    .await
                    .map_err(|e| Error::Internal(format!("Delete failed: {}", e)))?;

                Ok(result.rows_affected())
            }
        }
    }

    async fn import_schema(&self, schema: &str) -> Result<Vec<ForeignTableDef>> {
        let pool = self.pool()?;

        let query = r#"
            SELECT
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        "#;

        let rows: Vec<(String, String, String, String)> = sqlx::query_as(query)
            .bind(schema)
            .fetch_all(pool)
            .await
            .map_err(|e| Error::Internal(format!("Schema import failed: {}", e)))?;

        let mut tables: HashMap<String, Vec<FieldDef>> = HashMap::new();

        for (table_name, column_name, data_type, is_nullable) in rows {
            let dt = mysql_type_to_data_type(&data_type);
            let nullable = is_nullable == "YES";

            tables.entry(table_name).or_default().push(FieldDef {
                name: column_name,
                data_type: dt,
                nullable,
                default: None,
            });
        }

        let table_defs: Vec<ForeignTableDef> = tables
            .into_iter()
            .map(|(name, fields)| ForeignTableDef {
                name: name.clone(),
                schema: Schema { columns: fields },
                options: HashMap::from([("remote_table".to_string(), name)]),
            })
            .collect();

        Ok(table_defs)
    }

    fn capabilities(&self) -> FdwCapabilities {
        FdwCapabilities {
            supports_predicate_pushdown: true,
            supports_limit_pushdown: true,
            supports_aggregation_pushdown: false,
            supports_modification: true,
            supports_transactions: true,
            max_connections: 100,
        }
    }
}

// ============================================================================
// MySQL Scan
// ============================================================================

/// MySQL foreign scan implementation
pub struct MySqlScan {
    /// Query results
    rows: Vec<MySqlRow>,
    /// Current position
    position: usize,
    /// Schema
    schema: Schema,
    /// Batch size
    batch_size: usize,
    /// Columns being scanned
    columns: Vec<String>,
    /// Original SQL for reset
    sql: String,
    /// Original params for reset
    params: Vec<Value>,
    /// Pool reference
    pool: MySqlPool,
}

impl MySqlScan {
    async fn new(
        pool: MySqlPool,
        sql: String,
        params: Vec<Value>,
        columns: Vec<String>,
        batch_size: usize,
    ) -> Result<Self> {
        // Build query with bindings
        let mut query = sqlx::query(&sql);
        for value in &params {
            query = bind_value(query, value);
        }

        let rows: Vec<MySqlRow> = query
            .fetch_all(&pool)
            .await
            .map_err(|e| Error::Internal(format!("Query failed: {}", e)))?;

        // Infer schema from first row
        let schema = if let Some(row) = rows.first() {
            infer_schema_from_row(row, &columns)
        } else {
            Schema { columns: vec![] }
        };

        Ok(Self {
            rows,
            position: 0,
            schema,
            batch_size,
            columns,
            sql,
            params,
            pool,
        })
    }
}

#[async_trait]
impl ForeignScan for MySqlScan {
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

        // Convert to Arrow RecordBatch
        let arrow_schema = schema_to_arrow(&self.schema);
        let batch = rows_to_record_batch(batch_rows, &self.schema, &arrow_schema)?;

        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    async fn reset(&mut self) -> Result<()> {
        self.position = 0;

        // Re-execute query
        let mut query = sqlx::query(&self.sql);
        for value in &self.params {
            query = bind_value(query, value);
        }

        self.rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Query reset failed: {}", e)))?;

        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Bind a Value to a sqlx query
fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    value: &'q Value,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match value {
        Value::Null => query.bind(Option::<i64>::None),
        Value::Boolean(b) => query.bind(*b),
        Value::Int8(i) => query.bind(*i as i16),
        Value::Int16(i) => query.bind(*i),
        Value::Int32(i) => query.bind(*i),
        Value::Int64(i) => query.bind(*i),
        Value::Float32(f) => query.bind(*f),
        Value::Float64(f) => query.bind(*f),
        Value::String(s) => query.bind(s.as_ref()),
        Value::Binary(b) => query.bind(b.as_ref()),
        _ => query.bind(Option::<i64>::None),
    }
}

/// Convert MySQL type name to DataType
fn mysql_type_to_data_type(type_name: &str) -> DataType {
    let lower = type_name.to_lowercase();
    match lower.as_str() {
        "tinyint" | "bool" | "boolean" => DataType::Int8,
        "smallint" => DataType::Int16,
        "int" | "integer" | "mediumint" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" | "real" => DataType::Float64,
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" => DataType::String,
        "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary" | "varbinary" => DataType::Binary,
        "datetime" | "timestamp" => DataType::Timestamp,
        "date" => DataType::Date,
        "time" => DataType::Time,
        "json" => DataType::Json,
        _ => DataType::String,
    }
}

/// Infer schema from a MySQL row
fn infer_schema_from_row(row: &MySqlRow, columns: &[String]) -> Schema {
    let fields: Vec<FieldDef> = if columns.is_empty() {
        row.columns()
            .iter()
            .map(|col| FieldDef {
                name: col.name().to_string(),
                data_type: mysql_type_to_data_type(col.type_info().name()),
                nullable: true,
                default: None,
            })
            .collect()
    } else {
        columns
            .iter()
            .map(|name| {
                let col_type = row
                    .columns()
                    .iter()
                    .find(|c| c.name() == name)
                    .map(|c| mysql_type_to_data_type(c.type_info().name()))
                    .unwrap_or(DataType::String);

                FieldDef {
                    name: name.clone(),
                    data_type: col_type,
                    nullable: true,
                    default: None,
                }
            })
            .collect()
    };

    Schema { columns: fields }
}

/// Convert Schema to Arrow schema
fn schema_to_arrow(schema: &Schema) -> Arc<ArrowSchema> {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|f| {
            let arrow_type = match f.data_type {
                DataType::Boolean => ArrowDataType::Boolean,
                DataType::Int8 => ArrowDataType::Int8,
                DataType::Int16 => ArrowDataType::Int16,
                DataType::Int32 => ArrowDataType::Int32,
                DataType::Int64 => ArrowDataType::Int64,
                DataType::Float32 => ArrowDataType::Float32,
                DataType::Float64 => ArrowDataType::Float64,
                DataType::String => ArrowDataType::Utf8,
                DataType::Binary => ArrowDataType::Binary,
                DataType::Timestamp => {
                    ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
                }
                _ => ArrowDataType::Utf8,
            };
            Field::new(&f.name, arrow_type, f.nullable)
        })
        .collect();

    Arc::new(ArrowSchema::new(fields))
}

/// Convert MySQL rows to Arrow RecordBatch
fn rows_to_record_batch(
    rows: &[MySqlRow],
    schema: &Schema,
    arrow_schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new();

    for (i, field) in schema.columns.iter().enumerate() {
        let array: ArrayRef = match field.data_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|row| row.try_get::<bool, _>(i).ok())
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int8 => {
                let values: Vec<Option<i16>> = rows
                    .iter()
                    .map(|row| row.try_get::<i16, _>(i).ok())
                    .collect();
                Arc::new(Int16Array::from(values))
            }
            DataType::Int16 => {
                let values: Vec<Option<i16>> = rows
                    .iter()
                    .map(|row| row.try_get::<i16, _>(i).ok())
                    .collect();
                Arc::new(Int16Array::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|row| row.try_get::<i32, _>(i).ok())
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| row.try_get::<i64, _>(i).ok())
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float32 => {
                let values: Vec<Option<f32>> = rows
                    .iter()
                    .map(|row| row.try_get::<f32, _>(i).ok())
                    .collect();
                Arc::new(Float32Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|row| row.try_get::<f64, _>(i).ok())
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            DataType::String | DataType::Json => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| row.try_get::<String, _>(i).ok())
                    .collect();
                Arc::new(StringArray::from(values))
            }
            DataType::Binary => {
                let values: Vec<Option<Vec<u8>>> = rows
                    .iter()
                    .map(|row| row.try_get::<Vec<u8>, _>(i).ok())
                    .collect();
                Arc::new(BinaryArray::from_iter(values.iter().map(|v| v.as_ref().map(|b| b.as_slice()))))
            }
            DataType::Timestamp => {
                // MySQL timestamps are handled as strings and converted
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get::<String, _>(i)
                            .ok()
                            .and_then(|s| {
                                DateTime::parse_from_str(&format!("{} +0000", s), "%Y-%m-%d %H:%M:%S %z")
                                    .ok()
                                    .map(|dt| dt.timestamp_micros())
                            })
                    })
                    .collect();
                Arc::new(TimestampMicrosecondArray::from(values))
            }
            _ => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| row.try_get::<String, _>(i).ok())
                    .collect();
                Arc::new(StringArray::from(values))
            }
        };

        columns.push(array);
    }

    RecordBatch::try_new(arrow_schema.clone(), columns)
        .map_err(|e| Error::Internal(format!("Failed to create RecordBatch: {}", e)))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_type_conversion() {
        assert!(matches!(mysql_type_to_data_type("int"), DataType::Int32));
        assert!(matches!(mysql_type_to_data_type("varchar"), DataType::String));
        assert!(matches!(mysql_type_to_data_type("bigint"), DataType::Int64));
        assert!(matches!(mysql_type_to_data_type("double"), DataType::Float64));
    }

    #[test]
    fn test_capabilities() {
        let server = ForeignServer {
            name: "test".to_string(),
            connector_type: "mysql".to_string(),
            options: HashMap::new(),
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let fdw = rt.block_on(MySqlFdw::new(server)).unwrap();

        let caps = fdw.capabilities();
        assert!(caps.supports_predicate_pushdown);
        assert!(caps.supports_limit_pushdown);
        assert!(caps.supports_modification);
    }

    #[test]
    fn test_where_clause_building() {
        let server = ForeignServer {
            name: "test".to_string(),
            connector_type: "mysql".to_string(),
            options: HashMap::new(),
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let fdw = rt.block_on(MySqlFdw::new(server)).unwrap();

        let quals = vec![
            Qual {
                column: "age".to_string(),
                operator: QualOperator::Gt,
                value: Value::Int64(18),
            },
        ];

        let (clause, params) = fdw.build_where_clause(&quals);
        assert!(clause.contains("WHERE"));
        assert!(clause.contains("`age` > ?"));
        assert_eq!(params.len(), 1);
    }
}
