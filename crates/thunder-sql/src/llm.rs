//! LLM Integration for Natural Language to SQL
//!
//! Provides llama.cpp integration for intelligent NL-to-SQL conversion.
//! Supports local model inference with schema-aware prompting.
//!
//! # Features
//! - Local LLM inference via llama.cpp
//! - Schema-aware SQL generation
//! - Model caching and management
//! - Streaming inference support
//!
//! # Example
//! ```ignore
//! use thunder_sql::llm::{LlmEngine, LlmConfig};
//!
//! let config = LlmConfig::default()
//!     .with_model_path("models/sqlcoder-7b.gguf");
//! let engine = LlmEngine::new(config)?;
//!
//! let sql = engine.text_to_sql(
//!     "Show me all users who signed up last month",
//!     &schema_info
//! ).await?;
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use thunder_common::prelude::*;
use thunder_common::error::SqlError;

// ============================================================================
// Configuration
// ============================================================================

/// LLM Engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    /// Path to the GGUF model file
    pub model_path: Option<PathBuf>,
    /// Model name for auto-download (e.g., "defog/sqlcoder-7b-2")
    pub model_name: Option<String>,
    /// Number of GPU layers to offload (-1 for all, 0 for CPU only)
    pub n_gpu_layers: i32,
    /// Context window size
    pub n_ctx: u32,
    /// Number of threads for inference
    pub n_threads: u32,
    /// Temperature for sampling (0.0 = deterministic)
    pub temperature: f32,
    /// Top-p sampling
    pub top_p: f32,
    /// Maximum tokens to generate
    pub max_tokens: u32,
    /// Enable flash attention
    pub flash_attention: bool,
    /// Cache directory for downloaded models
    pub cache_dir: PathBuf,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            model_path: None,
            model_name: Some("defog/sqlcoder-7b-2".to_string()),
            n_gpu_layers: 0, // CPU by default
            n_ctx: 4096,
            n_threads: 4,
            temperature: 0.0, // Deterministic for SQL
            top_p: 0.95,
            max_tokens: 512,
            flash_attention: false,
            cache_dir: dirs::cache_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("thunderdb/models"),
        }
    }
}

impl LlmConfig {
    pub fn with_model_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.model_path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn with_model_name(mut self, name: &str) -> Self {
        self.model_name = Some(name.to_string());
        self
    }

    pub fn with_gpu_layers(mut self, layers: i32) -> Self {
        self.n_gpu_layers = layers;
        self
    }

    pub fn with_context_size(mut self, size: u32) -> Self {
        self.n_ctx = size;
        self
    }

    pub fn with_threads(mut self, threads: u32) -> Self {
        self.n_threads = threads;
        self
    }

    pub fn cpu_only(mut self) -> Self {
        self.n_gpu_layers = 0;
        self
    }

    pub fn gpu_offload_all(mut self) -> Self {
        self.n_gpu_layers = -1;
        self
    }
}

// ============================================================================
// Schema Information
// ============================================================================

/// Database schema information for context-aware SQL generation
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchemaInfo {
    /// Table definitions
    pub tables: Vec<TableSchema>,
    /// Additional context (e.g., business rules, common queries)
    pub context: Option<String>,
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub description: Option<String>,
    pub sample_queries: Vec<String>,
}

/// Column schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub foreign_key: Option<ForeignKeyRef>,
    pub description: Option<String>,
}

/// Foreign key reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
}

impl SchemaInfo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(mut self, table: TableSchema) -> Self {
        self.tables.push(table);
        self
    }

    pub fn with_context(mut self, context: &str) -> Self {
        self.context = Some(context.to_string());
        self
    }

    /// Generate schema DDL for prompt context
    pub fn to_ddl(&self) -> String {
        let mut ddl = String::new();
        for table in &self.tables {
            ddl.push_str(&format!("CREATE TABLE {} (\n", table.name));
            for (i, col) in table.columns.iter().enumerate() {
                let nullable = if col.nullable { "" } else { " NOT NULL" };
                let pk = if col.primary_key { " PRIMARY KEY" } else { "" };
                let comma = if i < table.columns.len() - 1 { "," } else { "" };
                ddl.push_str(&format!(
                    "  {} {}{}{}{}\n",
                    col.name, col.data_type, nullable, pk, comma
                ));
            }
            ddl.push_str(");\n\n");
        }
        ddl
    }
}

// ============================================================================
// LLM Response
// ============================================================================

/// Result from LLM inference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmResponse {
    /// Generated SQL query
    pub sql: String,
    /// Raw model output
    pub raw_output: String,
    /// Token usage statistics
    pub usage: TokenUsage,
    /// Inference time in milliseconds
    pub inference_time_ms: u64,
    /// Model used for inference
    pub model: String,
}

/// Token usage statistics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

// ============================================================================
// LLM Engine (with llama.cpp)
// ============================================================================

#[cfg(feature = "llm")]
mod engine {
    use super::*;
    use llama_cpp_2::context::params::LlamaContextParams;
    use llama_cpp_2::llama_backend::LlamaBackend;
    use llama_cpp_2::llama_batch::LlamaBatch;
    use llama_cpp_2::model::params::LlamaModelParams;
    use llama_cpp_2::model::LlamaModel;
    use llama_cpp_2::token::data_array::LlamaTokenDataArray;
    use std::num::NonZeroU32;
    use std::time::Instant;

    /// LLM Engine for text-to-SQL conversion
    pub struct LlmEngine {
        config: LlmConfig,
        backend: LlamaBackend,
        model: Option<Arc<LlamaModel>>,
        loaded: bool,
    }

    impl LlmEngine {
        /// Create a new LLM engine
        pub fn new(config: LlmConfig) -> Result<Self> {
            let backend = LlamaBackend::init()
                .map_err(|e| Error::Sql(SqlError::ParseError(format!("Failed to init llama backend: {}", e))))?;

            Ok(Self {
                config,
                backend,
                model: None,
                loaded: false,
            })
        }

        /// Load the model
        pub fn load_model(&mut self) -> Result<()> {
            let model_path = self.resolve_model_path()?;

            let model_params = LlamaModelParams::default()
                .with_n_gpu_layers(self.config.n_gpu_layers as u32);

            let model = LlamaModel::load_from_file(&self.backend, &model_path, &model_params)
                .map_err(|e| Error::Sql(SqlError::ParseError(format!("Failed to load model: {}", e))))?;

            self.model = Some(Arc::new(model));
            self.loaded = true;

            tracing::info!("Loaded LLM model from {:?}", model_path);
            Ok(())
        }

        /// Resolve model path (download if necessary)
        fn resolve_model_path(&self) -> Result<PathBuf> {
            if let Some(ref path) = self.config.model_path {
                if path.exists() {
                    return Ok(path.clone());
                }
                return Err(Error::Sql(SqlError::ParseError(
                    format!("Model file not found: {:?}", path)
                )));
            }

            // Model needs to be downloaded - return error with instructions
            Err(Error::Sql(SqlError::ParseError(
                "No model loaded. Please download a GGUF model and set model_path.\n\
                 Recommended: SQLCoder from https://huggingface.co/defog/sqlcoder-7b-2".to_string()
            )))
        }

        /// Convert natural language to SQL
        pub fn text_to_sql(&self, query: &str, schema: &SchemaInfo) -> Result<LlmResponse> {
            let model = self.model.as_ref()
                .ok_or_else(|| Error::Sql(SqlError::ParseError("Model not loaded".to_string())))?;

            let prompt = self.build_prompt(query, schema);
            let start = Instant::now();

            // Create context
            let ctx_params = LlamaContextParams::default()
                .with_n_ctx(NonZeroU32::new(self.config.n_ctx).unwrap())
                .with_n_threads(self.config.n_threads)
                .with_n_threads_batch(self.config.n_threads);

            let mut ctx = model.new_context(&self.backend, ctx_params)
                .map_err(|e| Error::Sql(SqlError::ParseError(format!("Failed to create context: {}", e))))?;

            // Tokenize prompt
            let tokens = model.str_to_token(&prompt, llama_cpp_2::model::AddBos::Always)
                .map_err(|e| Error::Sql(SqlError::ParseError(format!("Tokenization failed: {}", e))))?;

            let prompt_tokens = tokens.len() as u32;

            // Create batch and add tokens
            let mut batch = LlamaBatch::new(self.config.n_ctx as usize, 1);
            for (i, token) in tokens.iter().enumerate() {
                let is_last = i == tokens.len() - 1;
                batch.add(*token, i as i32, &[0], is_last)
                    .map_err(|e| Error::Sql(SqlError::ParseError(format!("Batch add failed: {}", e))))?;
            }

            // Decode prompt
            ctx.decode(&mut batch)
                .map_err(|e| Error::Sql(SqlError::ParseError(format!("Decode failed: {}", e))))?;

            // Generate tokens
            let mut output_tokens = Vec::new();
            let mut n_cur = tokens.len();

            for _ in 0..self.config.max_tokens {
                // Sample next token
                let candidates = ctx.candidates_ith(batch.n_tokens() - 1);
                let mut candidates_data = LlamaTokenDataArray::from_iter(candidates, false);

                // Apply temperature
                if self.config.temperature > 0.0 {
                    ctx.sample_temp(&mut candidates_data, self.config.temperature);
                    ctx.sample_top_p(&mut candidates_data, self.config.top_p, 1);
                }

                let new_token = ctx.sample_token_greedy(candidates_data);

                // Check for EOS
                if model.is_eog_token(new_token) {
                    break;
                }

                output_tokens.push(new_token);

                // Prepare next batch
                batch.clear();
                batch.add(new_token, n_cur as i32, &[0], true)
                    .map_err(|e| Error::Sql(SqlError::ParseError(format!("Batch add failed: {}", e))))?;
                n_cur += 1;

                ctx.decode(&mut batch)
                    .map_err(|e| Error::Sql(SqlError::ParseError(format!("Decode failed: {}", e))))?;
            }

            // Convert tokens to string
            let raw_output = output_tokens.iter()
                .filter_map(|t| model.token_to_str(*t, llama_cpp_2::model::Special::Tokenize).ok())
                .collect::<String>();

            let sql = self.extract_sql(&raw_output);
            let inference_time_ms = start.elapsed().as_millis() as u64;

            Ok(LlmResponse {
                sql,
                raw_output,
                usage: TokenUsage {
                    prompt_tokens,
                    completion_tokens: output_tokens.len() as u32,
                    total_tokens: prompt_tokens + output_tokens.len() as u32,
                },
                inference_time_ms,
                model: self.config.model_name.clone().unwrap_or_else(|| "local".to_string()),
            })
        }

        /// Build the prompt for text-to-SQL
        fn build_prompt(&self, query: &str, schema: &SchemaInfo) -> String {
            let schema_ddl = schema.to_ddl();
            let context = schema.context.as_deref().unwrap_or("");

            // SQLCoder-style prompt format
            format!(
                r#"### Task
Generate a SQL query to answer [QUESTION]{query}[/QUESTION]

### Database Schema
The query will run on a database with the following schema:
{schema_ddl}

{context}

### Answer
Given the database schema, here is the SQL query that answers [QUESTION]{query}[/QUESTION]
[SQL]
"#,
                query = query,
                schema_ddl = schema_ddl,
                context = context
            )
        }

        /// Extract SQL from model output
        fn extract_sql(&self, output: &str) -> String {
            // Try to find SQL between markers
            if let Some(start) = output.find("[SQL]") {
                let sql_start = start + 5;
                if let Some(end) = output[sql_start..].find("[/SQL]") {
                    return output[sql_start..sql_start + end].trim().to_string();
                }
                // No end marker, take until end or semicolon
                let rest = &output[sql_start..];
                if let Some(end) = rest.find(';') {
                    return rest[..=end].trim().to_string();
                }
                return rest.trim().to_string();
            }

            // Fallback: find SELECT/INSERT/UPDATE/DELETE
            let output_upper = output.to_uppercase();
            for keyword in &["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"] {
                if let Some(start) = output_upper.find(keyword) {
                    let rest = &output[start..];
                    if let Some(end) = rest.find(';') {
                        return rest[..=end].trim().to_string();
                    }
                    return rest.trim().to_string();
                }
            }

            output.trim().to_string()
        }

        /// Check if model is loaded
        pub fn is_loaded(&self) -> bool {
            self.loaded
        }

        /// Get current config
        pub fn config(&self) -> &LlmConfig {
            &self.config
        }
    }
}

#[cfg(feature = "llm")]
pub use engine::LlmEngine;

// ============================================================================
// Stub Implementation (when LLM feature is disabled)
// ============================================================================

#[cfg(not(feature = "llm"))]
pub struct LlmEngine {
    config: LlmConfig,
}

#[cfg(not(feature = "llm"))]
impl LlmEngine {
    pub fn new(config: LlmConfig) -> Result<Self> {
        Ok(Self { config })
    }

    pub fn load_model(&mut self) -> Result<()> {
        Err(Error::Sql(SqlError::ParseError(
            "LLM feature not enabled. Rebuild with --features llm".to_string()
        )))
    }

    pub fn text_to_sql(&self, _query: &str, _schema: &SchemaInfo) -> Result<LlmResponse> {
        Err(Error::Sql(SqlError::ParseError(
            "LLM feature not enabled. Rebuild with --features llm".to_string()
        )))
    }

    pub fn is_loaded(&self) -> bool {
        false
    }

    pub fn config(&self) -> &LlmConfig {
        &self.config
    }
}

// ============================================================================
// Model Registry
// ============================================================================

/// Registry of known models for easy setup
#[derive(Debug, Clone)]
pub struct ModelRegistry {
    models: HashMap<String, ModelInfo>,
}

/// Information about a known model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub name: String,
    pub description: String,
    pub hf_repo: String,
    pub filename: String,
    pub size_gb: f32,
    pub recommended_ctx: u32,
    pub license: String,
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelRegistry {
    pub fn new() -> Self {
        let mut models = HashMap::new();

        // SQLCoder models
        models.insert("sqlcoder-7b".to_string(), ModelInfo {
            name: "SQLCoder 7B".to_string(),
            description: "Defog's SQLCoder model, fine-tuned for text-to-SQL".to_string(),
            hf_repo: "defog/sqlcoder-7b-2".to_string(),
            filename: "sqlcoder-7b-q4_k_m.gguf".to_string(),
            size_gb: 4.1,
            recommended_ctx: 4096,
            license: "Apache-2.0".to_string(),
        });

        models.insert("sqlcoder-15b".to_string(), ModelInfo {
            name: "SQLCoder 15B".to_string(),
            description: "Larger SQLCoder model for complex queries".to_string(),
            hf_repo: "defog/sqlcoder-15b".to_string(),
            filename: "sqlcoder-15b-q4_k_m.gguf".to_string(),
            size_gb: 8.8,
            recommended_ctx: 4096,
            license: "Apache-2.0".to_string(),
        });

        // Llama models
        models.insert("llama3.2-1b".to_string(), ModelInfo {
            name: "Llama 3.2 1B".to_string(),
            description: "Meta's smallest Llama model, fast inference".to_string(),
            hf_repo: "meta-llama/Llama-3.2-1B-GGUF".to_string(),
            filename: "llama-3.2-1b-q4_k_m.gguf".to_string(),
            size_gb: 0.8,
            recommended_ctx: 2048,
            license: "Llama 3.2 Community".to_string(),
        });

        models.insert("llama3.2-3b".to_string(), ModelInfo {
            name: "Llama 3.2 3B".to_string(),
            description: "Meta's 3B Llama model, good balance of speed and quality".to_string(),
            hf_repo: "meta-llama/Llama-3.2-3B-GGUF".to_string(),
            filename: "llama-3.2-3b-q4_k_m.gguf".to_string(),
            size_gb: 2.0,
            recommended_ctx: 2048,
            license: "Llama 3.2 Community".to_string(),
        });

        // Phi models
        models.insert("phi3-mini".to_string(), ModelInfo {
            name: "Phi-3 Mini".to_string(),
            description: "Microsoft's Phi-3 Mini, excellent reasoning".to_string(),
            hf_repo: "microsoft/Phi-3-mini-4k-instruct-gguf".to_string(),
            filename: "Phi-3-mini-4k-instruct-q4.gguf".to_string(),
            size_gb: 2.2,
            recommended_ctx: 4096,
            license: "MIT".to_string(),
        });

        Self { models }
    }

    pub fn get(&self, name: &str) -> Option<&ModelInfo> {
        self.models.get(name)
    }

    pub fn list(&self) -> Vec<&ModelInfo> {
        self.models.values().collect()
    }

    pub fn recommended_for_sql() -> &'static str {
        "sqlcoder-7b"
    }
}

// ============================================================================
// Prompt Templates
// ============================================================================

/// Pre-built prompt templates for different use cases
pub struct PromptTemplates;

impl PromptTemplates {
    /// SQLCoder-style prompt for text-to-SQL
    pub fn sqlcoder(query: &str, schema: &str) -> String {
        format!(
            r#"### Task
Generate a SQL query to answer [QUESTION]{question}[/QUESTION]

### Database Schema
{schema}

### Answer
Given the database schema, here is the SQL query that answers [QUESTION]{question}[/QUESTION]
[SQL]
"#,
            question = query,
            schema = schema
        )
    }

    /// Simple prompt for general LLMs
    pub fn simple(query: &str, schema: &str) -> String {
        format!(
            r#"You are a SQL expert. Given the following database schema:

{schema}

Write a SQL query to: {query}

Only output the SQL query, nothing else."#,
            query = query,
            schema = schema
        )
    }

    /// Chain-of-thought prompt for complex queries
    pub fn chain_of_thought(query: &str, schema: &str) -> String {
        format!(
            r#"You are a SQL expert. Given the following database schema:

{schema}

User request: {query}

Let's think step by step:
1. What tables are involved?
2. What columns do we need?
3. What are the join conditions?
4. What filters apply?
5. What aggregations are needed?

Based on this analysis, write the SQL query:
```sql
"#,
            query = query,
            schema = schema
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
    fn test_config_default() {
        let config = LlmConfig::default();
        assert_eq!(config.n_gpu_layers, 0);
        assert_eq!(config.temperature, 0.0);
        assert_eq!(config.n_ctx, 4096);
    }

    #[test]
    fn test_config_builder() {
        let config = LlmConfig::default()
            .with_model_path("/path/to/model.gguf")
            .with_gpu_layers(10)
            .with_context_size(8192);

        assert_eq!(config.model_path, Some(PathBuf::from("/path/to/model.gguf")));
        assert_eq!(config.n_gpu_layers, 10);
        assert_eq!(config.n_ctx, 8192);
    }

    #[test]
    fn test_schema_to_ddl() {
        let schema = SchemaInfo::new()
            .add_table(TableSchema {
                name: "users".to_string(),
                columns: vec![
                    ColumnSchema {
                        name: "id".to_string(),
                        data_type: "BIGINT".to_string(),
                        nullable: false,
                        primary_key: true,
                        foreign_key: None,
                        description: None,
                    },
                    ColumnSchema {
                        name: "name".to_string(),
                        data_type: "VARCHAR(255)".to_string(),
                        nullable: false,
                        primary_key: false,
                        foreign_key: None,
                        description: None,
                    },
                ],
                description: None,
                sample_queries: vec![],
            });

        let ddl = schema.to_ddl();
        assert!(ddl.contains("CREATE TABLE users"));
        assert!(ddl.contains("id BIGINT NOT NULL PRIMARY KEY"));
        assert!(ddl.contains("name VARCHAR(255) NOT NULL"));
    }

    #[test]
    fn test_model_registry() {
        let registry = ModelRegistry::new();
        assert!(registry.get("sqlcoder-7b").is_some());
        assert!(registry.get("llama3.2-1b").is_some());
        assert!(registry.get("phi3-mini").is_some());
    }

    #[test]
    fn test_prompt_templates() {
        let prompt = PromptTemplates::sqlcoder(
            "Show all users",
            "CREATE TABLE users (id INT, name TEXT);"
        );
        assert!(prompt.contains("Show all users"));
        assert!(prompt.contains("CREATE TABLE users"));
    }

    #[test]
    fn test_llm_engine_stub() {
        let config = LlmConfig::default();
        let engine = LlmEngine::new(config).unwrap();
        assert!(!engine.is_loaded());
    }
}
