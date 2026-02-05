//! ML-Native Database Features
//!
//! Provides first-class support for machine learning workflows:
//! - Tensor/array data types for embeddings and features
//! - Feature store operations
//! - Dataset management for training
//! - Model registry and versioning
//! - Distributed training data management
//! - AI agent memory and state storage
//!
//! # SQL Extensions for ML
//!
//! ```sql
//! -- Create table with tensor columns
//! CREATE TABLE embeddings (
//!     id BIGINT PRIMARY KEY,
//!     text TEXT,
//!     embedding VECTOR(768),  -- Fixed-size vector
//!     features TENSOR         -- Dynamic tensor
//! );
//!
//! -- Vector similarity search
//! SELECT * FROM embeddings
//! ORDER BY embedding <-> query_embedding
//! LIMIT 10;
//!
//! -- Feature store operations
//! SELECT FEATURES('user_features', user_id) FROM users;
//!
//! -- Dataset creation for training
//! CREATE DATASET training_data AS
//! SELECT features, label FROM samples
//! WITH (format='parquet', shuffle=true, split=0.8);
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use parking_lot::RwLock;

use thunder_common::prelude::*;
use thunder_common::error::SqlError;

// ============================================================================
// ML Data Types
// ============================================================================

/// ML-specific data types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MLDataType {
    /// Fixed-size vector (e.g., embeddings)
    Vector(usize),
    /// Dynamic tensor with shape
    Tensor(Vec<usize>),
    /// Sparse vector (index, value pairs)
    SparseVector(usize),
    /// Quantized vector (int8/int4)
    QuantizedVector { dim: usize, bits: u8 },
    /// Feature map (named features)
    FeatureMap,
    /// JSON-encoded ML metadata
    MLMetadata,
}

impl MLDataType {
    pub fn vector(dim: usize) -> Self {
        Self::Vector(dim)
    }

    pub fn tensor(shape: Vec<usize>) -> Self {
        Self::Tensor(shape)
    }

    pub fn embedding(dim: usize) -> Self {
        Self::Vector(dim)
    }

    /// Get the number of elements
    pub fn num_elements(&self) -> usize {
        match self {
            MLDataType::Vector(dim) => *dim,
            MLDataType::Tensor(shape) => shape.iter().product(),
            MLDataType::SparseVector(max_dim) => *max_dim,
            MLDataType::QuantizedVector { dim, .. } => *dim,
            MLDataType::FeatureMap => 0, // Dynamic
            MLDataType::MLMetadata => 0,
        }
    }

    /// Get storage size in bytes (for f32)
    pub fn storage_bytes(&self) -> usize {
        match self {
            MLDataType::Vector(dim) => dim * 4,
            MLDataType::Tensor(shape) => shape.iter().product::<usize>() * 4,
            MLDataType::SparseVector(_) => 0, // Dynamic
            MLDataType::QuantizedVector { dim, bits } => (dim * (*bits as usize) + 7) / 8,
            MLDataType::FeatureMap => 0,
            MLDataType::MLMetadata => 0,
        }
    }
}

// ============================================================================
// Tensor Operations
// ============================================================================

/// Tensor information for ML operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TensorInfo {
    pub name: String,
    pub shape: Vec<usize>,
    pub dtype: String,
    pub device: String,
    pub requires_grad: bool,
}

/// Distance metrics for vector similarity
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Euclidean distance (L2)
    Euclidean,
    /// Cosine similarity
    Cosine,
    /// Inner product (dot product)
    InnerProduct,
    /// Manhattan distance (L1)
    Manhattan,
    /// Hamming distance (for binary vectors)
    Hamming,
}

impl Default for DistanceMetric {
    fn default() -> Self {
        Self::Cosine
    }
}

/// Vector index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    pub metric: DistanceMetric,
    pub dimension: usize,
    /// Index type: flat, ivf, hnsw
    pub index_type: String,
    /// Number of lists for IVF
    pub n_lists: Option<usize>,
    /// M parameter for HNSW
    pub hnsw_m: Option<usize>,
    /// ef_construction for HNSW
    pub hnsw_ef_construction: Option<usize>,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            metric: DistanceMetric::Cosine,
            dimension: 768,
            index_type: "hnsw".to_string(),
            n_lists: None,
            hnsw_m: Some(16),
            hnsw_ef_construction: Some(200),
        }
    }
}

// ============================================================================
// Feature Store
// ============================================================================

/// Feature column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureColumn {
    pub name: String,
    pub dtype: MLDataType,
    pub description: Option<String>,
    /// Transformation applied (e.g., "normalize", "one_hot")
    pub transform: Option<String>,
    /// Source column/expression
    pub source: Option<String>,
    /// Feature importance score
    pub importance: Option<f64>,
}

/// Feature group (collection of related features)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureGroup {
    pub name: String,
    pub entity_key: String,
    pub features: Vec<FeatureColumn>,
    pub description: Option<String>,
    /// TTL for feature freshness
    pub ttl_seconds: Option<u64>,
    /// Source table/view
    pub source: String,
    /// Timestamp column for point-in-time lookups
    pub timestamp_column: Option<String>,
}

/// Feature store registry
pub struct FeatureStore {
    groups: RwLock<HashMap<String, FeatureGroup>>,
}

impl Default for FeatureStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FeatureStore {
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_group(&self, group: FeatureGroup) {
        self.groups.write().insert(group.name.clone(), group);
    }

    pub fn get_group(&self, name: &str) -> Option<FeatureGroup> {
        self.groups.read().get(name).cloned()
    }

    pub fn list_groups(&self) -> Vec<String> {
        self.groups.read().keys().cloned().collect()
    }

    /// Generate SQL for feature retrieval
    pub fn generate_feature_sql(&self, group_name: &str, entity_ids: &[i64]) -> Result<String> {
        let group = self.get_group(group_name)
            .ok_or_else(|| Error::Sql(SqlError::ParseError(
                format!("Feature group not found: {}", group_name)
            )))?;

        let columns = group.features.iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let ids = entity_ids.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        Ok(format!(
            "SELECT {}, {} FROM {} WHERE {} IN ({})",
            group.entity_key, columns, group.source, group.entity_key, ids
        ))
    }
}

// ============================================================================
// Dataset Management
// ============================================================================

/// Dataset information for ML training
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetInfo {
    pub name: String,
    pub description: Option<String>,
    /// Number of samples
    pub num_samples: usize,
    /// Feature columns
    pub features: Vec<FeatureColumn>,
    /// Label/target column
    pub label_column: Option<String>,
    /// Storage format (parquet, arrow, csv)
    pub format: String,
    /// Storage location
    pub location: PathBuf,
    /// Train/validation/test split ratios
    pub splits: Option<DatasetSplits>,
    /// Creation timestamp
    pub created_at: i64,
    /// Schema version
    pub version: u32,
}

/// Dataset split configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSplits {
    pub train: f32,
    pub validation: f32,
    pub test: f32,
    /// Random seed for reproducibility
    pub seed: Option<u64>,
}

impl Default for DatasetSplits {
    fn default() -> Self {
        Self {
            train: 0.8,
            validation: 0.1,
            test: 0.1,
            seed: Some(42),
        }
    }
}

/// Dataset builder for creating training datasets
pub struct DatasetBuilder {
    name: String,
    query: String,
    features: Vec<FeatureColumn>,
    label_column: Option<String>,
    format: String,
    location: Option<PathBuf>,
    splits: Option<DatasetSplits>,
    shuffle: bool,
    batch_size: usize,
}

impl DatasetBuilder {
    pub fn new(name: &str, query: &str) -> Self {
        Self {
            name: name.to_string(),
            query: query.to_string(),
            features: Vec::new(),
            label_column: None,
            format: "parquet".to_string(),
            location: None,
            splits: None,
            shuffle: true,
            batch_size: 1024,
        }
    }

    pub fn with_features(mut self, features: Vec<FeatureColumn>) -> Self {
        self.features = features;
        self
    }

    pub fn with_label(mut self, column: &str) -> Self {
        self.label_column = Some(column.to_string());
        self
    }

    pub fn with_format(mut self, format: &str) -> Self {
        self.format = format.to_string();
        self
    }

    pub fn with_location<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.location = Some(path.into());
        self
    }

    pub fn with_splits(mut self, splits: DatasetSplits) -> Self {
        self.splits = Some(splits);
        self
    }

    pub fn shuffle(mut self, shuffle: bool) -> Self {
        self.shuffle = shuffle;
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Build dataset info (actual materialization would be done by executor)
    pub fn build(self) -> DatasetInfo {
        DatasetInfo {
            name: self.name,
            description: None,
            num_samples: 0, // Set after materialization
            features: self.features,
            label_column: self.label_column,
            format: self.format,
            location: self.location.unwrap_or_else(|| PathBuf::from(".")),
            splits: self.splits,
            created_at: chrono::Utc::now().timestamp(),
            version: 1,
        }
    }
}

// ============================================================================
// Model Registry
// ============================================================================

/// Model metadata for model registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetadata {
    pub name: String,
    pub version: String,
    pub framework: String,
    pub description: Option<String>,
    /// Model architecture/type
    pub architecture: String,
    /// Training metrics
    pub metrics: HashMap<String, f64>,
    /// Hyperparameters used
    pub hyperparameters: HashMap<String, serde_json::Value>,
    /// Input schema
    pub input_schema: Vec<FeatureColumn>,
    /// Output schema
    pub output_schema: Vec<FeatureColumn>,
    /// Model artifact location
    pub artifact_path: PathBuf,
    /// Model size in bytes
    pub size_bytes: u64,
    /// Creation timestamp
    pub created_at: i64,
    /// Training duration in seconds
    pub training_duration_secs: Option<u64>,
    /// Tags for organization
    pub tags: Vec<String>,
    /// Stage: development, staging, production
    pub stage: String,
}

/// Model registry for tracking ML models
pub struct MLModelRegistry {
    models: RwLock<HashMap<String, Vec<ModelMetadata>>>,
}

impl Default for MLModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MLModelRegistry {
    pub fn new() -> Self {
        Self {
            models: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, model: ModelMetadata) {
        let mut models = self.models.write();
        models
            .entry(model.name.clone())
            .or_insert_with(Vec::new)
            .push(model);
    }

    pub fn get_latest(&self, name: &str) -> Option<ModelMetadata> {
        self.models.read()
            .get(name)
            .and_then(|versions| versions.last().cloned())
    }

    pub fn get_version(&self, name: &str, version: &str) -> Option<ModelMetadata> {
        self.models.read()
            .get(name)
            .and_then(|versions| versions.iter().find(|m| m.version == version).cloned())
    }

    pub fn list_models(&self) -> Vec<String> {
        self.models.read().keys().cloned().collect()
    }

    pub fn list_versions(&self, name: &str) -> Vec<String> {
        self.models.read()
            .get(name)
            .map(|versions| versions.iter().map(|m| m.version.clone()).collect())
            .unwrap_or_default()
    }

    pub fn promote_to_stage(&self, name: &str, version: &str, stage: &str) -> Result<()> {
        let mut models = self.models.write();
        if let Some(versions) = models.get_mut(name) {
            if let Some(model) = versions.iter_mut().find(|m| m.version == version) {
                model.stage = stage.to_string();
                return Ok(());
            }
        }
        Err(Error::Sql(SqlError::ParseError(
            format!("Model {}:{} not found", name, version)
        )))
    }
}

// ============================================================================
// Distributed Training Support
// ============================================================================

/// Distributed training configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedConfig {
    /// Number of workers
    pub num_workers: usize,
    /// World size (total processes)
    pub world_size: usize,
    /// Current rank
    pub rank: usize,
    /// Master address
    pub master_addr: String,
    /// Master port
    pub master_port: u16,
    /// Backend: nccl, gloo, mpi
    pub backend: String,
    /// Strategy: ddp, fsdp, deepspeed
    pub strategy: String,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            num_workers: 1,
            world_size: 1,
            rank: 0,
            master_addr: "localhost".to_string(),
            master_port: 29500,
            backend: "gloo".to_string(),
            strategy: "ddp".to_string(),
        }
    }
}

/// Training job configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingConfig {
    /// Job name
    pub name: String,
    /// Dataset name
    pub dataset: String,
    /// Model name
    pub model: String,
    /// Batch size per device
    pub batch_size: usize,
    /// Learning rate
    pub learning_rate: f64,
    /// Number of epochs
    pub epochs: usize,
    /// Gradient accumulation steps
    pub gradient_accumulation_steps: usize,
    /// Mixed precision: no, fp16, bf16
    pub mixed_precision: String,
    /// Distributed configuration
    pub distributed: Option<DistributedConfig>,
    /// Checkpoint directory
    pub checkpoint_dir: PathBuf,
    /// Checkpoint frequency (steps)
    pub checkpoint_steps: usize,
    /// Evaluation frequency (steps)
    pub eval_steps: usize,
    /// Logging frequency (steps)
    pub logging_steps: usize,
    /// Random seed
    pub seed: u64,
}

impl Default for TrainingConfig {
    fn default() -> Self {
        Self {
            name: "training_job".to_string(),
            dataset: "".to_string(),
            model: "".to_string(),
            batch_size: 32,
            learning_rate: 1e-4,
            epochs: 3,
            gradient_accumulation_steps: 1,
            mixed_precision: "no".to_string(),
            distributed: None,
            checkpoint_dir: PathBuf::from("checkpoints"),
            checkpoint_steps: 1000,
            eval_steps: 500,
            logging_steps: 100,
            seed: 42,
        }
    }
}

/// Data partition for distributed training
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPartition {
    /// Partition index
    pub partition_id: usize,
    /// Total partitions
    pub total_partitions: usize,
    /// Start offset in dataset
    pub start_offset: usize,
    /// End offset in dataset
    pub end_offset: usize,
    /// Number of samples in this partition
    pub num_samples: usize,
}

impl DataPartition {
    /// Create partitions for distributed training
    pub fn create_partitions(total_samples: usize, num_partitions: usize) -> Vec<Self> {
        let samples_per_partition = total_samples / num_partitions;
        let remainder = total_samples % num_partitions;

        let mut partitions = Vec::with_capacity(num_partitions);
        let mut offset = 0;

        for i in 0..num_partitions {
            let extra = if i < remainder { 1 } else { 0 };
            let partition_size = samples_per_partition + extra;

            partitions.push(DataPartition {
                partition_id: i,
                total_partitions: num_partitions,
                start_offset: offset,
                end_offset: offset + partition_size,
                num_samples: partition_size,
            });

            offset += partition_size;
        }

        partitions
    }

    /// Generate SQL for this partition
    pub fn to_sql(&self, base_query: &str) -> String {
        format!(
            "{} LIMIT {} OFFSET {}",
            base_query, self.num_samples, self.start_offset
        )
    }
}

// ============================================================================
// AI Agent Support
// ============================================================================

/// Agent memory storage for AI agents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMemory {
    /// Memory ID
    pub id: String,
    /// Agent ID this memory belongs to
    pub agent_id: String,
    /// Memory type: short_term, long_term, episodic, semantic
    pub memory_type: String,
    /// Content (text or structured)
    pub content: serde_json::Value,
    /// Embedding vector for similarity search
    pub embedding: Option<Vec<f32>>,
    /// Importance score (0.0 - 1.0)
    pub importance: f64,
    /// Timestamp of creation
    pub created_at: i64,
    /// Last access timestamp
    pub last_accessed: i64,
    /// Access count
    pub access_count: u64,
    /// Metadata tags
    pub tags: Vec<String>,
}

/// Agent context for stateful operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentContext {
    /// Agent identifier
    pub agent_id: String,
    /// Session identifier
    pub session_id: String,
    /// Conversation history (recent messages)
    pub conversation: Vec<ConversationMessage>,
    /// Working memory (current task context)
    pub working_memory: HashMap<String, serde_json::Value>,
    /// Tool results from recent actions
    pub tool_results: Vec<ToolResult>,
    /// Current goal/objective
    pub current_goal: Option<String>,
    /// Active plans/steps
    pub active_plans: Vec<String>,
}

/// Conversation message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationMessage {
    pub role: String,
    pub content: String,
    pub timestamp: i64,
    pub metadata: Option<HashMap<String, String>>,
}

/// Tool execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub tool_name: String,
    pub input: serde_json::Value,
    pub output: serde_json::Value,
    pub success: bool,
    pub error: Option<String>,
    pub timestamp: i64,
    pub duration_ms: u64,
}

impl AgentContext {
    pub fn new(agent_id: &str, session_id: &str) -> Self {
        Self {
            agent_id: agent_id.to_string(),
            session_id: session_id.to_string(),
            conversation: Vec::new(),
            working_memory: HashMap::new(),
            tool_results: Vec::new(),
            current_goal: None,
            active_plans: Vec::new(),
        }
    }

    pub fn add_message(&mut self, role: &str, content: &str) {
        self.conversation.push(ConversationMessage {
            role: role.to_string(),
            content: content.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            metadata: None,
        });
    }

    pub fn add_tool_result(&mut self, result: ToolResult) {
        self.tool_results.push(result);
    }

    pub fn set_goal(&mut self, goal: &str) {
        self.current_goal = Some(goal.to_string());
    }

    pub fn add_plan_step(&mut self, step: &str) {
        self.active_plans.push(step.to_string());
    }

    /// Generate SQL to persist agent state
    pub fn to_insert_sql(&self, table: &str) -> Result<String> {
        let json = serde_json::to_string(self)
            .map_err(|e| Error::Sql(SqlError::ParseError(e.to_string())))?;

        Ok(format!(
            "INSERT INTO {} (agent_id, session_id, context) VALUES ('{}', '{}', '{}')",
            table, self.agent_id, self.session_id, json.replace('\'', "''")
        ))
    }
}

/// Agent memory store for persistence
pub struct AgentMemoryStore {
    memories: RwLock<HashMap<String, Vec<AgentMemory>>>,
    contexts: RwLock<HashMap<String, AgentContext>>,
}

impl Default for AgentMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentMemoryStore {
    pub fn new() -> Self {
        Self {
            memories: RwLock::new(HashMap::new()),
            contexts: RwLock::new(HashMap::new()),
        }
    }

    pub fn store_memory(&self, memory: AgentMemory) {
        self.memories.write()
            .entry(memory.agent_id.clone())
            .or_insert_with(Vec::new)
            .push(memory);
    }

    pub fn get_memories(&self, agent_id: &str, memory_type: Option<&str>, limit: usize) -> Vec<AgentMemory> {
        self.memories.read()
            .get(agent_id)
            .map(|memories| {
                memories.iter()
                    .filter(|m| memory_type.map_or(true, |t| m.memory_type == t))
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn save_context(&self, context: AgentContext) {
        self.contexts.write().insert(context.session_id.clone(), context);
    }

    pub fn get_context(&self, session_id: &str) -> Option<AgentContext> {
        self.contexts.read().get(session_id).cloned()
    }

    /// Search memories by similarity (placeholder - would use vector index)
    pub fn search_similar(&self, agent_id: &str, query_embedding: &[f32], limit: usize) -> Vec<AgentMemory> {
        self.memories.read()
            .get(agent_id)
            .map(|memories| {
                let mut scored: Vec<_> = memories.iter()
                    .filter_map(|m| {
                        m.embedding.as_ref().map(|emb| {
                            let score = cosine_similarity(query_embedding, emb);
                            (score, m.clone())
                        })
                    })
                    .collect();
                scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                scored.into_iter().take(limit).map(|(_, m)| m).collect()
            })
            .unwrap_or_default()
    }
}

/// Compute cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

// ============================================================================
// SQL Extensions for ML
// ============================================================================

/// ML SQL function registry
pub struct MLFunctions;

impl MLFunctions {
    /// Parse ML-specific SQL extensions
    pub fn parse_vector_literal(s: &str) -> Result<Vec<f32>> {
        // Parse [1.0, 2.0, 3.0] format
        let s = s.trim();
        if !s.starts_with('[') || !s.ends_with(']') {
            return Err(Error::Sql(SqlError::ParseError(
                "Vector literal must be enclosed in []".to_string()
            )));
        }

        let inner = &s[1..s.len()-1];
        inner.split(',')
            .map(|v| v.trim().parse::<f32>()
                .map_err(|e| Error::Sql(SqlError::ParseError(e.to_string()))))
            .collect()
    }

    /// Generate SQL for vector similarity search
    pub fn vector_search_sql(
        table: &str,
        vector_column: &str,
        query_vector: &[f32],
        metric: DistanceMetric,
        limit: usize,
    ) -> String {
        let vector_str = format!("[{}]",
            query_vector.iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let operator = match metric {
            DistanceMetric::Euclidean => "<->",
            DistanceMetric::Cosine => "<=>",
            DistanceMetric::InnerProduct => "<#>",
            _ => "<->",
        };

        format!(
            "SELECT *, {} {} '{}' AS distance FROM {} ORDER BY distance LIMIT {}",
            vector_column, operator, vector_str, table, limit
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ml_data_type() {
        let vec_type = MLDataType::vector(768);
        assert_eq!(vec_type.num_elements(), 768);
        assert_eq!(vec_type.storage_bytes(), 768 * 4);

        let tensor_type = MLDataType::tensor(vec![32, 768]);
        assert_eq!(tensor_type.num_elements(), 32 * 768);
    }

    #[test]
    fn test_feature_store() {
        let store = FeatureStore::new();

        let group = FeatureGroup {
            name: "user_features".to_string(),
            entity_key: "user_id".to_string(),
            features: vec![
                FeatureColumn {
                    name: "age".to_string(),
                    dtype: MLDataType::Vector(1),
                    description: None,
                    transform: Some("normalize".to_string()),
                    source: None,
                    importance: None,
                },
            ],
            description: None,
            ttl_seconds: Some(3600),
            source: "users".to_string(),
            timestamp_column: None,
        };

        store.register_group(group);
        assert!(store.get_group("user_features").is_some());
    }

    #[test]
    fn test_dataset_builder() {
        let dataset = DatasetBuilder::new("training", "SELECT * FROM features")
            .with_label("target")
            .with_splits(DatasetSplits::default())
            .batch_size(64)
            .build();

        assert_eq!(dataset.name, "training");
        assert_eq!(dataset.label_column, Some("target".to_string()));
    }

    #[test]
    fn test_data_partition() {
        let partitions = DataPartition::create_partitions(1000, 4);
        assert_eq!(partitions.len(), 4);

        let total: usize = partitions.iter().map(|p| p.num_samples).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn test_agent_context() {
        let mut ctx = AgentContext::new("agent-1", "session-1");
        ctx.add_message("user", "Hello");
        ctx.add_message("assistant", "Hi there!");
        ctx.set_goal("Answer questions");

        assert_eq!(ctx.conversation.len(), 2);
        assert_eq!(ctx.current_goal, Some("Answer questions".to_string()));
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);

        let c = vec![0.0, 1.0, 0.0];
        assert!((cosine_similarity(&a, &c) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_vector_literal_parse() {
        let vec = MLFunctions::parse_vector_literal("[1.0, 2.0, 3.0]").unwrap();
        assert_eq!(vec, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_vector_search_sql() {
        let sql = MLFunctions::vector_search_sql(
            "embeddings",
            "embedding",
            &[1.0, 2.0, 3.0],
            DistanceMetric::Cosine,
            10,
        );
        assert!(sql.contains("embeddings"));
        assert!(sql.contains("<=>"));
        assert!(sql.contains("LIMIT 10"));
    }
}
