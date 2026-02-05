//! MongoDB Foreign Data Wrapper
//!
//! Implements FDW for querying MongoDB databases:
//! - Document-to-row mapping
//! - Query pushdown for filters
//! - Projection pushdown
//! - CRUD operations

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit};
use async_trait::async_trait;
use futures::StreamExt;
use mongodb::{
    bson::{self, doc, Bson, Document},
    options::ClientOptions,
    Client, Collection,
};
use tracing::{debug, error, info, warn};

use crate::{
    ColumnStatistics, FdwCapabilities, ForeignDataWrapper, ForeignScan, ForeignServer,
    ForeignTableDef, ModifyOperation, Qual, QualOperator,
};
use crate::FieldDef;
use thunder_common::prelude::*;

// ============================================================================
// MongoDB FDW
// ============================================================================

/// MongoDB Foreign Data Wrapper
pub struct MongoFdw {
    /// Server configuration
    server: ForeignServer,
    /// MongoDB client
    client: Option<Client>,
    /// Database name
    database: String,
    /// Collection name
    collection: String,
    /// Document schema mapping
    schema_mapping: HashMap<String, String>,
    /// Cached schema
    cached_schema: Option<Schema>,
    /// Statistics cache
    statistics: HashMap<String, ColumnStatistics>,
}

impl MongoFdw {
    /// Create a new MongoDB FDW
    pub async fn new(server: ForeignServer) -> Result<Self> {
        let database = server
            .options
            .get("database")
            .cloned()
            .unwrap_or_else(|| "test".to_string());

        let collection = server
            .options
            .get("collection")
            .cloned()
            .unwrap_or_else(|| "documents".to_string());

        Ok(Self {
            server,
            client: None,
            database,
            collection,
            schema_mapping: HashMap::new(),
            cached_schema: None,
            statistics: HashMap::new(),
        })
    }

    /// Connect to MongoDB
    pub async fn connect(&mut self) -> Result<()> {
        if self.client.is_some() {
            return Ok(());
        }

        let url = self
            .server
            .options
            .get("url")
            .cloned()
            .unwrap_or_else(|| "mongodb://localhost:27017".to_string());

        let client_options = ClientOptions::parse(&url)
            .await
            .map_err(|e| Error::Internal(format!("Invalid MongoDB URL: {}", e)))?;

        let client = Client::with_options(client_options)
            .map_err(|e| Error::Internal(format!("MongoDB connection failed: {}", e)))?;

        // Verify connection
        client
            .database("admin")
            .run_command(doc! { "ping": 1 }, None)
            .await
            .map_err(|e| Error::Internal(format!("MongoDB ping failed: {}", e)))?;

        self.client = Some(client);

        info!(server = %self.server.name, "MongoDB FDW connected");

        Ok(())
    }

    /// Get collection reference
    fn get_collection(&self) -> Result<Collection<Document>> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        Ok(client.database(&self.database).collection(&self.collection))
    }

    /// Build MongoDB filter from qualifiers
    fn build_filter(&self, quals: &[Qual]) -> Document {
        if quals.is_empty() {
            return doc! {};
        }

        let mut filter = Document::new();

        for qual in quals {
            let field = self
                .schema_mapping
                .get(&qual.column)
                .cloned()
                .unwrap_or_else(|| qual.column.clone());

            let condition = match qual.operator {
                QualOperator::Eq => value_to_bson(&qual.value),
                QualOperator::NotEq => doc! { "$ne": value_to_bson(&qual.value) }.into(),
                QualOperator::Lt => doc! { "$lt": value_to_bson(&qual.value) }.into(),
                QualOperator::LtEq => doc! { "$lte": value_to_bson(&qual.value) }.into(),
                QualOperator::Gt => doc! { "$gt": value_to_bson(&qual.value) }.into(),
                QualOperator::GtEq => doc! { "$gte": value_to_bson(&qual.value) }.into(),
                QualOperator::Like => {
                    // Convert SQL LIKE to regex
                    if let Value::String(s) = &qual.value {
                        let pattern = s.replace('%', ".*").replace('_', ".");
                        doc! { "$regex": pattern, "$options": "i" }.into()
                    } else {
                        continue;
                    }
                }
                QualOperator::In => {
                    if let Value::Array(arr) = &qual.value {
                        let values: Vec<Bson> = arr.iter().map(value_to_bson).collect();
                        doc! { "$in": values }.into()
                    } else {
                        continue;
                    }
                }
                QualOperator::IsNull => doc! { "$eq": Bson::Null }.into(),
                QualOperator::IsNotNull => doc! { "$ne": Bson::Null }.into(),
            };

            filter.insert(field, condition);
        }

        filter
    }

    /// Build projection document
    fn build_projection(&self, columns: &[String]) -> Document {
        if columns.is_empty() {
            return doc! {};
        }

        let mut projection = Document::new();
        for col in columns {
            let field = self
                .schema_mapping
                .get(col)
                .cloned()
                .unwrap_or_else(|| col.clone());
            projection.insert(field, 1);
        }

        projection
    }

    /// Infer schema from sample documents
    async fn infer_schema(&mut self) -> Result<Schema> {
        if let Some(schema) = &self.cached_schema {
            return Ok(schema.clone());
        }

        let collection = self.get_collection()?;

        // Sample a few documents to infer schema
        let mut cursor = collection
            .find(None, None)
            .await
            .map_err(|e| Error::Internal(format!("Failed to sample documents: {}", e)))?;

        let mut field_types: HashMap<String, DataType> = HashMap::new();

        let mut count = 0;
        while let Some(doc) = cursor.next().await {
            if count >= 100 {
                break;
            }

            if let Ok(doc) = doc {
                for (key, value) in doc.iter() {
                    if key == "_id" {
                        continue;
                    }

                    let data_type = bson_to_data_type(value);
                    field_types.entry(key.clone()).or_insert(data_type);
                }
            }

            count += 1;
        }

        let fields: Vec<FieldDef> = field_types
            .into_iter()
            .map(|(name, data_type)| FieldDef {
                name,
                data_type,
                nullable: true,
                default: None,
            })
            .collect();

        let schema = Schema { columns: fields };
        self.cached_schema = Some(schema.clone());

        Ok(schema)
    }

    /// Refresh statistics
    async fn refresh_statistics(&mut self) -> Result<()> {
        let collection = self.get_collection()?;

        // Get total count
        let total_count = collection
            .count_documents(None, None)
            .await
            .map_err(|e| Error::Internal(format!("Count failed: {}", e)))?;

        // Get schema
        let schema = self.infer_schema().await?;

        for field in &schema.columns {
            // Estimate distinct values
            let distinct_query = doc! {
                "distinct": &self.collection,
                "key": &field.name
            };

            if let Ok(result) = self
                .client
                .as_ref()
                .unwrap()
                .database(&self.database)
                .run_command(distinct_query, None)
                .await
            {
                if let Some(values) = result.get_array("values").ok() {
                    self.statistics.insert(
                        field.name.clone(),
                        ColumnStatistics {
                            null_count: 0,
                            distinct_count: values.len() as u64,
                            min_value: None,
                            max_value: None,
                        },
                    );
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ForeignDataWrapper for MongoFdw {
    fn name(&self) -> &str {
        "mongodb"
    }

    fn estimate_size(&self, quals: &[Qual]) -> Result<(usize, f64)> {
        // Base estimate
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
                _ => {}
            }
        }

        Ok((rows.max(1), cost))
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
        let collection = self.get_collection()?;

        let filter = self.build_filter(quals);
        let projection = self.build_projection(columns);

        debug!(
            filter = ?filter,
            projection = ?projection,
            "MongoDB FDW query"
        );

        let scan = MongoScan::new(
            collection,
            filter,
            projection,
            columns.to_vec(),
            limit,
            1000,
        )
        .await?;

        Ok(Box::new(scan))
    }

    async fn modify(&self, op: ModifyOperation) -> Result<u64> {
        let collection = self.get_collection()?;

        match op {
            ModifyOperation::Insert { table: _, rows } => {
                if rows.is_empty() {
                    return Ok(0);
                }

                let schema = self.cached_schema.as_ref().ok_or_else(|| {
                    Error::Internal("Schema not available".to_string())
                })?;

                let documents: Vec<Document> = rows
                    .iter()
                    .map(|row| row_to_document(row, schema))
                    .collect();

                let result = collection
                    .insert_many(documents, None)
                    .await
                    .map_err(|e| Error::Internal(format!("Insert failed: {}", e)))?;

                Ok(result.inserted_ids.len() as u64)
            }
            ModifyOperation::Update {
                table: _,
                assignments,
                quals,
            } => {
                let filter = self.build_filter(&quals);

                let mut update = Document::new();
                let mut set_doc = Document::new();

                for (col, value) in assignments {
                    set_doc.insert(col, value_to_bson(&value));
                }

                update.insert("$set", set_doc);

                let result = collection
                    .update_many(filter, update, None)
                    .await
                    .map_err(|e| Error::Internal(format!("Update failed: {}", e)))?;

                Ok(result.modified_count)
            }
            ModifyOperation::Delete { table: _, quals } => {
                let filter = self.build_filter(&quals);

                let result = collection
                    .delete_many(filter, None)
                    .await
                    .map_err(|e| Error::Internal(format!("Delete failed: {}", e)))?;

                Ok(result.deleted_count)
            }
        }
    }

    async fn import_schema(&self, _schema: &str) -> Result<Vec<ForeignTableDef>> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::Internal("Not connected".to_string())
        })?;

        // List collections in the database
        let db = client.database(&self.database);
        let collections = db
            .list_collection_names(None)
            .await
            .map_err(|e| Error::Internal(format!("Failed to list collections: {}", e)))?;

        let mut table_defs = Vec::new();

        for coll_name in collections {
            // Sample documents to infer schema
            let collection: Collection<Document> = db.collection(&coll_name);

            let mut cursor = collection
                .find(None, None)
                .await
                .map_err(|e| Error::Internal(format!("Failed to sample: {}", e)))?;

            let mut field_types: HashMap<String, DataType> = HashMap::new();

            let mut count = 0;
            while let Some(doc) = cursor.next().await {
                if count >= 10 {
                    break;
                }

                if let Ok(doc) = doc {
                    for (key, value) in doc.iter() {
                        if key == "_id" {
                            continue;
                        }
                        let data_type = bson_to_data_type(value);
                        field_types.entry(key.clone()).or_insert(data_type);
                    }
                }

                count += 1;
            }

            let fields: Vec<FieldDef> = field_types
                .into_iter()
                .map(|(name, data_type)| FieldDef {
                    name,
                    data_type,
                    nullable: true,
                    default: None,
                })
                .collect();

            table_defs.push(ForeignTableDef {
                name: coll_name.clone(),
                schema: Schema { columns: fields },
                options: HashMap::from([("collection".to_string(), coll_name)]),
            });
        }

        Ok(table_defs)
    }

    fn capabilities(&self) -> FdwCapabilities {
        FdwCapabilities {
            supports_predicate_pushdown: true,
            supports_limit_pushdown: true,
            supports_aggregation_pushdown: false,
            supports_modification: true,
            supports_transactions: false, // MongoDB transactions require replica set
            max_connections: 100,
        }
    }
}

// ============================================================================
// MongoDB Scan
// ============================================================================

/// MongoDB foreign scan implementation
pub struct MongoScan {
    /// Documents
    documents: Vec<Document>,
    /// Current position
    position: usize,
    /// Schema
    schema: Schema,
    /// Batch size
    batch_size: usize,
    /// Columns to return
    columns: Vec<String>,
}

impl MongoScan {
    async fn new(
        collection: Collection<Document>,
        filter: Document,
        projection: Document,
        columns: Vec<String>,
        limit: Option<usize>,
        batch_size: usize,
    ) -> Result<Self> {
        let mut options = mongodb::options::FindOptions::default();
        if !projection.is_empty() {
            options.projection = Some(projection);
        }
        if let Some(limit) = limit {
            options.limit = Some(limit as i64);
        }

        let mut cursor = collection
            .find(Some(filter), Some(options))
            .await
            .map_err(|e| Error::Internal(format!("Query failed: {}", e)))?;

        let mut documents = Vec::new();
        while let Some(doc) = cursor.next().await {
            match doc {
                Ok(d) => documents.push(d),
                Err(e) => {
                    warn!(error = %e, "Error reading document");
                    break;
                }
            }
        }

        // Infer schema from documents
        let schema = if let Some(doc) = documents.first() {
            let fields: Vec<FieldDef> = if columns.is_empty() {
                doc.iter()
                    .filter(|(k, _)| *k != "_id")
                    .map(|(k, v)| FieldDef {
                        name: k.clone(),
                        data_type: bson_to_data_type(v),
                        nullable: true,
                        default: None,
                    })
                    .collect()
            } else {
                columns
                    .iter()
                    .map(|col| {
                        let data_type = doc
                            .get(col)
                            .map(bson_to_data_type)
                            .unwrap_or(DataType::String);
                        FieldDef {
                            name: col.clone(),
                            data_type,
                            nullable: true,
                            default: None,
                        }
                    })
                    .collect()
            };
            Schema { columns: fields }
        } else {
            Schema { columns: vec![] }
        };

        Ok(Self {
            documents,
            position: 0,
            schema,
            batch_size,
            columns,
        })
    }
}

#[async_trait]
impl ForeignScan for MongoScan {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.position >= self.documents.len() {
            return Ok(None);
        }

        let end = (self.position + self.batch_size).min(self.documents.len());
        let batch_docs = &self.documents[self.position..end];
        self.position = end;

        if batch_docs.is_empty() {
            return Ok(None);
        }

        let arrow_schema = schema_to_arrow(&self.schema);
        let batch = documents_to_record_batch(batch_docs, &self.schema, &arrow_schema)?;

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

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert Value to BSON
fn value_to_bson(value: &Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Boolean(b) => Bson::Boolean(*b),
        Value::Int8(i) => Bson::Int32(*i as i32),
        Value::Int16(i) => Bson::Int32(*i as i32),
        Value::Int32(i) => Bson::Int32(*i),
        Value::Int64(i) => Bson::Int64(*i),
        Value::Float32(f) => Bson::Double(*f as f64),
        Value::Float64(f) => Bson::Double(*f),
        Value::String(s) => Bson::String(s.to_string()),
        Value::Binary(b) => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: b.to_vec(),
        }),
        Value::Array(arr) => {
            let values: Vec<Bson> = arr.iter().map(value_to_bson).collect();
            Bson::Array(values)
        }
        Value::Json(j) => {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(j) {
                bson::to_bson(&parsed).unwrap_or(Bson::Null)
            } else {
                Bson::String(j.to_string())
            }
        }
        _ => Bson::Null,
    }
}

/// Convert BSON to Value
fn bson_to_value(bson: &Bson) -> Value {
    match bson {
        Bson::Null => Value::Null,
        Bson::Boolean(b) => Value::Boolean(*b),
        Bson::Int32(i) => Value::Int32(*i),
        Bson::Int64(i) => Value::Int64(*i),
        Bson::Double(f) => Value::Float64(*f),
        Bson::String(s) => Value::String(s.clone().into()),
        Bson::Array(arr) => {
            let values: Vec<Value> = arr.iter().map(bson_to_value).collect();
            Value::Array(values.into())
        }
        Bson::Document(doc) => {
            let json_str = serde_json::to_string(doc).unwrap_or_default();
            Value::Json(json_str.into())
        }
        Bson::Binary(b) => Value::Binary(b.bytes.clone().into()),
        Bson::ObjectId(oid) => Value::String(oid.to_hex().into()),
        Bson::DateTime(dt) => Value::Timestamp(dt.timestamp_millis() * 1000),
        _ => Value::Null,
    }
}

/// Infer DataType from BSON value
fn bson_to_data_type(bson: &Bson) -> DataType {
    match bson {
        Bson::Null => DataType::String,
        Bson::Boolean(_) => DataType::Boolean,
        Bson::Int32(_) => DataType::Int32,
        Bson::Int64(_) => DataType::Int64,
        Bson::Double(_) => DataType::Float64,
        Bson::String(_) => DataType::String,
        Bson::Array(_) => DataType::Json,
        Bson::Document(_) => DataType::Json,
        Bson::Binary(_) => DataType::Binary,
        Bson::DateTime(_) => DataType::Timestamp,
        _ => DataType::String,
    }
}

/// Convert Row to Document
fn row_to_document(row: &Row, schema: &Schema) -> Document {
    let mut doc = Document::new();

    for (i, field) in schema.columns.iter().enumerate() {
        if i < row.values.len() {
            doc.insert(&field.name, value_to_bson(&row.values[i]));
        }
    }

    doc
}

/// Convert Schema to Arrow schema
fn schema_to_arrow(schema: &Schema) -> Arc<ArrowSchema> {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|f| {
            let arrow_type = match f.data_type {
                DataType::Boolean => ArrowDataType::Boolean,
                DataType::Int32 => ArrowDataType::Int32,
                DataType::Int64 => ArrowDataType::Int64,
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

/// Convert documents to Arrow RecordBatch
fn documents_to_record_batch(
    docs: &[Document],
    schema: &Schema,
    arrow_schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in &schema.columns {
        let array: ArrayRef = match field.data_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = docs
                    .iter()
                    .map(|doc| doc.get_bool(&field.name).ok())
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = docs
                    .iter()
                    .map(|doc| doc.get_i32(&field.name).ok())
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = docs
                    .iter()
                    .map(|doc| doc.get_i64(&field.name).ok())
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = docs
                    .iter()
                    .map(|doc| doc.get_f64(&field.name).ok())
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            DataType::String | DataType::Json => {
                let values: Vec<Option<String>> = docs
                    .iter()
                    .map(|doc| doc.get_str(&field.name).ok().map(|s| s.to_string()))
                    .collect();
                Arc::new(StringArray::from(values))
            }
            DataType::Binary => {
                let values: Vec<Option<Vec<u8>>> = docs
                    .iter()
                    .map(|doc| {
                        doc.get_binary_generic(&field.name)
                            .ok()
                            .map(|b| b.to_vec())
                    })
                    .collect();
                Arc::new(BinaryArray::from_iter(values.iter().map(|v| v.as_ref().map(|b| b.as_slice()))))
            }
            DataType::Timestamp => {
                let values: Vec<Option<i64>> = docs
                    .iter()
                    .map(|doc| {
                        doc.get_datetime(&field.name)
                            .ok()
                            .map(|dt| dt.timestamp_millis() * 1000)
                    })
                    .collect();
                Arc::new(TimestampMicrosecondArray::from(values))
            }
            _ => {
                let values: Vec<Option<String>> = docs
                    .iter()
                    .map(|doc| doc.get_str(&field.name).ok().map(|s| s.to_string()))
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
    fn test_value_to_bson() {
        assert!(matches!(value_to_bson(&Value::Null), Bson::Null));
        assert!(matches!(value_to_bson(&Value::Boolean(true)), Bson::Boolean(true)));
        assert!(matches!(value_to_bson(&Value::Int32(42)), Bson::Int32(42)));
        assert!(matches!(value_to_bson(&Value::Int64(42)), Bson::Int64(42)));
    }

    #[test]
    fn test_bson_to_value() {
        assert!(matches!(bson_to_value(&Bson::Null), Value::Null));
        assert!(matches!(bson_to_value(&Bson::Boolean(true)), Value::Boolean(true)));
        assert!(matches!(bson_to_value(&Bson::Int32(42)), Value::Int32(42)));
    }

    #[test]
    fn test_capabilities() {
        let server = ForeignServer {
            name: "test".to_string(),
            connector_type: "mongodb".to_string(),
            options: HashMap::new(),
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let fdw = rt.block_on(MongoFdw::new(server)).unwrap();

        let caps = fdw.capabilities();
        assert!(caps.supports_predicate_pushdown);
        assert!(caps.supports_limit_pushdown);
        assert!(caps.supports_modification);
        assert!(!caps.supports_transactions);
    }

    #[test]
    fn test_filter_building() {
        let server = ForeignServer {
            name: "test".to_string(),
            connector_type: "mongodb".to_string(),
            options: HashMap::new(),
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let fdw = rt.block_on(MongoFdw::new(server)).unwrap();

        let quals = vec![Qual {
            column: "age".to_string(),
            operator: QualOperator::Gt,
            value: Value::Int64(18),
        }];

        let filter = fdw.build_filter(&quals);
        assert!(filter.contains_key("age"));
    }
}
