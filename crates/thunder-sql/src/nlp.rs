//! Natural Language Query Interface
//!
//! Enables AI/LLM-native database interactions:
//! - Plain English to SQL conversion
//! - Dynamic variable replacement with {{variable}} syntax
//! - Context-aware query generation
//! - Schema-aware natural language understanding

use std::collections::HashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};

use thunder_common::prelude::*;
use thunder_common::error::SqlError;

// ============================================================================
// Natural Language Query Types
// ============================================================================

/// Natural language query request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NLQuery {
    /// The natural language query text
    pub text: String,
    /// Variables to substitute ({{var_name}} -> value)
    pub variables: HashMap<String, String>,
    /// Context hints (table names, column preferences)
    pub context: Option<QueryContext>,
}

/// Query context for better understanding
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryContext {
    /// Preferred tables to query
    pub tables: Vec<String>,
    /// User's role/permissions context
    pub role: Option<String>,
    /// Previous queries for context
    pub history: Vec<String>,
    /// Custom entity mappings (e.g., "employees" -> "employment" table)
    pub entity_mappings: HashMap<String, String>,
}

/// Result of natural language processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NLQueryResult {
    /// Original natural language query
    pub original: String,
    /// Generated SQL query
    pub sql: String,
    /// Detected intent
    pub intent: QueryIntent,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f64,
    /// Explanation of the generated query
    pub explanation: String,
    /// Suggested alternatives
    pub alternatives: Vec<String>,
}

/// Detected query intent
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QueryIntent {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Aggregate,
    Join,
    Unknown,
}

impl std::fmt::Display for QueryIntent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryIntent::Select => write!(f, "SELECT"),
            QueryIntent::Insert => write!(f, "INSERT"),
            QueryIntent::Update => write!(f, "UPDATE"),
            QueryIntent::Delete => write!(f, "DELETE"),
            QueryIntent::Create => write!(f, "CREATE"),
            QueryIntent::Drop => write!(f, "DROP"),
            QueryIntent::Aggregate => write!(f, "AGGREGATE"),
            QueryIntent::Join => write!(f, "JOIN"),
            QueryIntent::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

// ============================================================================
// Pattern Definitions
// ============================================================================

/// Query pattern for matching natural language
#[derive(Debug, Clone)]
struct QueryPattern {
    /// Regex pattern to match
    pattern: Regex,
    /// SQL template with placeholders
    #[allow(dead_code)]
    template: String,
    /// Intent of this pattern
    intent: QueryIntent,
    /// Base confidence score
    confidence: f64,
    /// Description for explanation
    description: String,
}

// ============================================================================
// Natural Language Processor
// ============================================================================

/// Main NLP processor for converting natural language to SQL
pub struct NLProcessor {
    patterns: Vec<QueryPattern>,
    stop_words: Vec<&'static str>,
    aggregations: HashMap<&'static str, &'static str>,
    comparisons: HashMap<&'static str, &'static str>,
    schema_cache: parking_lot::RwLock<HashMap<String, Vec<String>>>,
}

impl Default for NLProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl NLProcessor {
    pub fn new() -> Self {
        let mut processor = Self {
            patterns: Vec::new(),
            stop_words: vec![
                "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
                "have", "has", "had", "do", "does", "did", "will", "would", "could",
                "should", "may", "might", "must", "shall", "can", "need", "dare",
                "ought", "used", "to", "of", "in", "for", "on", "with", "at", "by",
                "about", "against", "between", "into", "through", "during", "before",
                "after", "above", "below", "from", "up", "down", "out", "off", "over",
                "under", "again", "further", "then", "once", "please", "just", "also",
                "me", "my", "i", "want", "give", "tell", "find",
            ],
            aggregations: HashMap::from([
                ("count", "COUNT"),
                ("total", "SUM"),
                ("sum", "SUM"),
                ("average", "AVG"),
                ("avg", "AVG"),
                ("mean", "AVG"),
                ("minimum", "MIN"),
                ("min", "MIN"),
                ("maximum", "MAX"),
                ("max", "MAX"),
                ("number of", "COUNT"),
                ("how many", "COUNT"),
            ]),
            comparisons: HashMap::from([
                ("greater than", ">"),
                ("more than", ">"),
                ("above", ">"),
                ("over", ">"),
                ("exceeds", ">"),
                ("less than", "<"),
                ("below", "<"),
                ("under", "<"),
                ("fewer than", "<"),
                ("equal to", "="),
                ("equals", "="),
                ("is", "="),
                ("not equal", "<>"),
                ("not", "<>"),
                ("at least", ">="),
                ("at most", "<="),
                ("between", "BETWEEN"),
                ("like", "LIKE"),
                ("contains", "LIKE"),
                ("starts with", "LIKE"),
                ("ends with", "LIKE"),
            ]),
            schema_cache: parking_lot::RwLock::new(HashMap::new()),
        };

        processor.init_patterns();
        processor
    }

    fn init_patterns(&mut self) {
        // SELECT patterns
        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(show|get|fetch|retrieve|list|display|find)\s+(all\s+)?(.*?)\s*(from\s+)?(\w+)(\s+where\s+(.*))?$").unwrap(),
            template: "SELECT {columns} FROM {table}{where}".to_string(),
            intent: QueryIntent::Select,
            confidence: 0.85,
            description: "Retrieve records from a table".to_string(),
        });

        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^what\s+(is|are)\s+(the\s+)?(.+?)\s+(of|for|in)\s+(\w+)(\s+where\s+(.*))?$").unwrap(),
            template: "SELECT {columns} FROM {table}{where}".to_string(),
            intent: QueryIntent::Select,
            confidence: 0.80,
            description: "Query specific information".to_string(),
        });

        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(select|query)\s+(.+?)\s+from\s+(\w+)(.*)$").unwrap(),
            template: "SELECT {columns} FROM {table}{rest}".to_string(),
            intent: QueryIntent::Select,
            confidence: 0.95,
            description: "Direct SQL-like select".to_string(),
        });

        // COUNT/Aggregation patterns
        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(how many|count|number of)\s+(\w+)(\s+where\s+(.*))?$").unwrap(),
            template: "SELECT COUNT(*) FROM {table}{where}".to_string(),
            intent: QueryIntent::Aggregate,
            confidence: 0.90,
            description: "Count records in a table".to_string(),
        });

        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(total|sum|average|avg|min|max|minimum|maximum)\s+(of\s+)?(\w+)\s+(in|from|of)\s+(\w+)(\s+where\s+(.*))?$").unwrap(),
            template: "SELECT {agg}({column}) FROM {table}{where}".to_string(),
            intent: QueryIntent::Aggregate,
            confidence: 0.88,
            description: "Aggregate calculation".to_string(),
        });

        // INSERT patterns
        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(add|insert|create|new)\s+(a\s+)?(\w+)\s+(with|having|:)\s+(.+)$").unwrap(),
            template: "INSERT INTO {table} ({columns}) VALUES ({values})".to_string(),
            intent: QueryIntent::Insert,
            confidence: 0.85,
            description: "Insert a new record".to_string(),
        });

        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(add|insert)\s+(into\s+)?(\w+)\s*[:\(]\s*(.+)$").unwrap(),
            template: "INSERT INTO {table} ({columns}) VALUES ({values})".to_string(),
            intent: QueryIntent::Insert,
            confidence: 0.90,
            description: "Insert with explicit columns".to_string(),
        });

        // UPDATE patterns
        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(update|change|modify|set)\s+(\w+)\s+(set\s+)?(.+?)\s+(where|for|with)\s+(.+)$").unwrap(),
            template: "UPDATE {table} SET {assignments} WHERE {condition}".to_string(),
            intent: QueryIntent::Update,
            confidence: 0.85,
            description: "Update existing records".to_string(),
        });

        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(change|set)\s+(.+?)\s+(to|=)\s+(.+?)\s+(in|for)\s+(\w+)\s+(where|when|if)\s+(.+)$").unwrap(),
            template: "UPDATE {table} SET {column} = {value} WHERE {condition}".to_string(),
            intent: QueryIntent::Update,
            confidence: 0.82,
            description: "Change specific field".to_string(),
        });

        // DELETE patterns
        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(delete|remove|drop)\s+(from\s+)?(\w+)\s+(where|when|if)\s+(.+)$").unwrap(),
            template: "DELETE FROM {table} WHERE {condition}".to_string(),
            intent: QueryIntent::Delete,
            confidence: 0.88,
            description: "Delete records with condition".to_string(),
        });

        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(delete|remove)\s+(all\s+)?(\w+)(\s+records)?$").unwrap(),
            template: "DELETE FROM {table}".to_string(),
            intent: QueryIntent::Delete,
            confidence: 0.75,
            description: "Delete all records (dangerous)".to_string(),
        });

        // CREATE TABLE patterns
        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^create\s+(a\s+)?(table|collection)\s+(\w+)\s+(with|having)\s+(.+)$").unwrap(),
            template: "CREATE TABLE {table} ({columns})".to_string(),
            intent: QueryIntent::Create,
            confidence: 0.90,
            description: "Create a new table".to_string(),
        });

        // JOIN patterns
        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(show|get|find)\s+(.+?)\s+from\s+(\w+)\s+(and|with|joined?\s+with)\s+(\w+)(\s+on\s+(.+))?$").unwrap(),
            template: "SELECT {columns} FROM {table1} JOIN {table2}{on_clause}".to_string(),
            intent: QueryIntent::Join,
            confidence: 0.80,
            description: "Query with table join".to_string(),
        });

        // Sort patterns for ORDER BY
        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(show|get|list)\s+(.+?)\s+from\s+(\w+)\s+(sorted|ordered)\s+by\s+(\w+)(\s+(asc|desc))?$").unwrap(),
            template: "SELECT {columns} FROM {table} ORDER BY {order_column}{order_dir}".to_string(),
            intent: QueryIntent::Select,
            confidence: 0.85,
            description: "Query with ordering".to_string(),
        });

        // Top/Limit patterns
        self.patterns.push(QueryPattern {
            pattern: Regex::new(r"(?i)^(show|get|find)\s+(top|first)\s+(\d+)\s+(.+?)\s+from\s+(\w+)(.*)$").unwrap(),
            template: "SELECT {columns} FROM {table}{rest} LIMIT {limit}".to_string(),
            intent: QueryIntent::Select,
            confidence: 0.88,
            description: "Query with limit".to_string(),
        });
    }

    /// Process a natural language query
    pub fn process(&self, query: NLQuery) -> Result<NLQueryResult> {
        let text = self.preprocess(&query.text);
        let text = self.substitute_variables(&text, &query.variables);

        // Try pattern matching first
        for pattern in &self.patterns {
            if let Some(result) = self.try_pattern(&text, pattern, &query.context) {
                return Ok(result);
            }
        }

        // Fallback to semantic analysis
        self.semantic_analysis(&text, &query.context)
    }

    /// Preprocess the query text
    fn preprocess(&self, text: &str) -> String {
        let text = text.trim().to_lowercase();
        // Remove punctuation except for operators
        let re = Regex::new(r"[,;!?]").unwrap();
        re.replace_all(&text, " ").to_string()
    }

    /// Substitute variables in the text
    fn substitute_variables(&self, text: &str, variables: &HashMap<String, String>) -> String {
        let mut result = text.to_string();
        // Match double-brace variable pattern
        let re = Regex::new(r"\{\{(\w+)\}\}").unwrap();

        for cap in re.captures_iter(text) {
            if let Some(var_match) = cap.get(1) {
                let var_name = var_match.as_str();
                if let Some(value) = variables.get(var_name) {
                    let pattern = format!("{{{{{}}}}}", var_name);
                    result = result.replace(&pattern, value);
                }
            }
        }

        result
    }

    /// Try to match a pattern
    fn try_pattern(&self, text: &str, pattern: &QueryPattern, context: &Option<QueryContext>) -> Option<NLQueryResult> {
        if let Some(caps) = pattern.pattern.captures(text) {
            let sql = self.build_sql_from_pattern(pattern, &caps, context);
            if let Some(sql) = sql {
                return Some(NLQueryResult {
                    original: text.to_string(),
                    sql: sql.clone(),
                    intent: pattern.intent.clone(),
                    confidence: pattern.confidence,
                    explanation: pattern.description.clone(),
                    alternatives: self.generate_alternatives(&sql, &pattern.intent),
                });
            }
        }
        None
    }

    /// Build SQL from pattern match
    fn build_sql_from_pattern(
        &self,
        pattern: &QueryPattern,
        caps: &regex::Captures,
        context: &Option<QueryContext>,
    ) -> Option<String> {
        match pattern.intent {
            QueryIntent::Select => self.build_select(caps, context),
            QueryIntent::Aggregate => self.build_aggregate(caps, context),
            QueryIntent::Insert => self.build_insert(caps, context),
            QueryIntent::Update => self.build_update(caps, context),
            QueryIntent::Delete => self.build_delete(caps, context),
            QueryIntent::Create => self.build_create(caps, context),
            QueryIntent::Join => self.build_join(caps, context),
            _ => None,
        }
    }

    fn build_select(&self, caps: &regex::Captures, context: &Option<QueryContext>) -> Option<String> {
        // Extract components from capture groups
        let columns_raw = caps.get(3).map(|m| m.as_str()).unwrap_or("*");
        let table = caps.get(5).or_else(|| caps.get(4)).map(|m| m.as_str())?;
        let where_clause = caps.get(7).map(|m| m.as_str());

        let columns = self.parse_columns(columns_raw);
        let table = self.resolve_table_name(table, context);

        let mut sql = format!("SELECT {} FROM {}", columns, table);

        if let Some(where_str) = where_clause {
            let condition = self.parse_condition(where_str);
            sql.push_str(&format!(" WHERE {}", condition));
        }

        Some(sql)
    }

    fn build_aggregate(&self, caps: &regex::Captures, context: &Option<QueryContext>) -> Option<String> {
        let agg_word = caps.get(1).map(|m| m.as_str().to_lowercase())?;
        let column = caps.get(3).map(|m| m.as_str()).unwrap_or("*");
        let table = caps.get(5).or_else(|| caps.get(2)).map(|m| m.as_str())?;
        let where_clause = caps.get(7).map(|m| m.as_str());

        let agg_func = self.aggregations.iter()
            .find(|(k, _)| agg_word.contains(*k))
            .map(|(_, v)| *v)
            .unwrap_or("COUNT");

        let table = self.resolve_table_name(table, context);

        let col = if column == "*" || agg_func == "COUNT" {
            "*".to_string()
        } else {
            self.clean_identifier(column)
        };

        let mut sql = format!("SELECT {}({}) FROM {}", agg_func, col, table);

        if let Some(where_str) = where_clause {
            let condition = self.parse_condition(where_str);
            sql.push_str(&format!(" WHERE {}", condition));
        }

        Some(sql)
    }

    fn build_insert(&self, caps: &regex::Captures, context: &Option<QueryContext>) -> Option<String> {
        let table = caps.get(3).map(|m| m.as_str())?;
        let data_str = caps.get(5).or_else(|| caps.get(4)).map(|m| m.as_str())?;

        let table = self.resolve_table_name(table, context);
        let (columns, values) = self.parse_insert_data(data_str);

        Some(format!("INSERT INTO {} ({}) VALUES ({})", table, columns, values))
    }

    fn build_update(&self, caps: &regex::Captures, context: &Option<QueryContext>) -> Option<String> {
        let table = caps.get(2).or_else(|| caps.get(6)).map(|m| m.as_str())?;
        let assignments_str = caps.get(4).map(|m| m.as_str())?;
        let condition_str = caps.get(6).or_else(|| caps.get(8)).map(|m| m.as_str())?;

        let table = self.resolve_table_name(table, context);
        let assignments = self.parse_assignments(assignments_str);
        let condition = self.parse_condition(condition_str);

        Some(format!("UPDATE {} SET {} WHERE {}", table, assignments, condition))
    }

    fn build_delete(&self, caps: &regex::Captures, context: &Option<QueryContext>) -> Option<String> {
        let table = caps.get(3).map(|m| m.as_str())?;
        let condition_str = caps.get(5).map(|m| m.as_str());

        let table = self.resolve_table_name(table, context);

        if let Some(cond) = condition_str {
            let condition = self.parse_condition(cond);
            Some(format!("DELETE FROM {} WHERE {}", table, condition))
        } else {
            Some(format!("DELETE FROM {}", table))
        }
    }

    fn build_create(&self, caps: &regex::Captures, context: &Option<QueryContext>) -> Option<String> {
        let table = caps.get(3).map(|m| m.as_str())?;
        let columns_str = caps.get(5).map(|m| m.as_str())?;

        let table = self.resolve_table_name(table, context);
        let columns = self.parse_column_definitions(columns_str);

        Some(format!("CREATE TABLE {} ({})", table, columns))
    }

    fn build_join(&self, caps: &regex::Captures, context: &Option<QueryContext>) -> Option<String> {
        let columns_raw = caps.get(2).map(|m| m.as_str()).unwrap_or("*");
        let table1 = caps.get(3).map(|m| m.as_str())?;
        let table2 = caps.get(5).map(|m| m.as_str())?;
        let on_clause = caps.get(7).map(|m| m.as_str());

        let columns = self.parse_columns(columns_raw);
        let table1 = self.resolve_table_name(table1, context);
        let table2 = self.resolve_table_name(table2, context);

        let mut sql = format!("SELECT {} FROM {} JOIN {}", columns, table1, table2);

        if let Some(on_str) = on_clause {
            sql.push_str(&format!(" ON {}", self.parse_condition(on_str)));
        }

        Some(sql)
    }

    /// Semantic analysis fallback
    fn semantic_analysis(&self, text: &str, context: &Option<QueryContext>) -> Result<NLQueryResult> {
        let words: Vec<&str> = text.split_whitespace()
            .filter(|w| !self.stop_words.contains(w))
            .collect();

        // Detect intent from keywords
        let intent = self.detect_intent(&words);

        // Extract entities
        let tables = self.extract_tables(&words, context);
        let columns = self.extract_columns(&words);
        let conditions = self.extract_conditions(text);

        // Build SQL based on intent
        let sql = match intent {
            QueryIntent::Select => {
                let table = tables.first().cloned().unwrap_or_else(|| "unknown_table".to_string());
                let cols = if columns.is_empty() { "*".to_string() } else { columns.join(", ") };
                let mut sql = format!("SELECT {} FROM {}", cols, table);
                if !conditions.is_empty() {
                    sql.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
                }
                sql
            }
            QueryIntent::Aggregate => {
                let table = tables.first().cloned().unwrap_or_else(|| "unknown_table".to_string());
                let agg = self.find_aggregation(&words).unwrap_or("COUNT");
                let col = columns.first().map(|s| s.as_str()).unwrap_or("*");
                format!("SELECT {}({}) FROM {}", agg, col, table)
            }
            _ => {
                return Err(Error::Sql(SqlError::ParseError(
                    format!("Could not understand query: {}", text)
                )));
            }
        };

        Ok(NLQueryResult {
            original: text.to_string(),
            sql,
            intent,
            confidence: 0.5,
            explanation: "Generated using semantic analysis".to_string(),
            alternatives: vec![],
        })
    }

    fn detect_intent(&self, words: &[&str]) -> QueryIntent {
        for word in words {
            match *word {
                "show" | "get" | "find" | "list" | "display" | "fetch" | "retrieve" | "select" => {
                    return QueryIntent::Select;
                }
                "count" | "total" | "sum" | "average" | "avg" | "min" | "max" => {
                    return QueryIntent::Aggregate;
                }
                "add" | "insert" | "create" | "new" => {
                    return QueryIntent::Insert;
                }
                "update" | "change" | "modify" | "set" => {
                    return QueryIntent::Update;
                }
                "delete" | "remove" | "drop" => {
                    return QueryIntent::Delete;
                }
                _ => {}
            }
        }
        QueryIntent::Select // Default
    }

    fn find_aggregation(&self, words: &[&str]) -> Option<&'static str> {
        for word in words {
            if let Some(agg) = self.aggregations.get(word) {
                return Some(agg);
            }
        }
        None
    }

    fn extract_tables(&self, words: &[&str], context: &Option<QueryContext>) -> Vec<String> {
        let mut tables = Vec::new();

        // Check context entity mappings first
        if let Some(ctx) = context {
            for word in words {
                if let Some(table) = ctx.entity_mappings.get(*word) {
                    tables.push(table.clone());
                }
            }
            // If tables specified in context, use those
            if !ctx.tables.is_empty() {
                return ctx.tables.clone();
            }
        }

        // Look for table-like words (nouns, plurals)
        for word in words {
            if self.looks_like_table(word) {
                tables.push(self.clean_identifier(word));
            }
        }

        tables
    }

    fn extract_columns(&self, words: &[&str]) -> Vec<String> {
        let mut columns = Vec::new();

        for word in words {
            // Skip common non-column words
            if !self.stop_words.contains(word) &&
               !["from", "where", "and", "or", "table"].contains(word) &&
               !self.looks_like_table(word) {
                columns.push(self.clean_identifier(word));
            }
        }

        columns
    }

    fn extract_conditions(&self, text: &str) -> Vec<String> {
        let mut conditions = Vec::new();

        // Look for comparison patterns
        for (phrase, op) in &self.comparisons {
            if text.contains(phrase) {
                // Extract the condition parts
                if let Some(idx) = text.find(phrase) {
                    let before = &text[..idx].trim();
                    let after = &text[idx + phrase.len()..].trim();

                    // Get the last word before as column
                    if let Some(col) = before.split_whitespace().last() {
                        // Get the first word/value after as value
                        if let Some(val) = after.split_whitespace().next() {
                            let val_formatted = if val.parse::<f64>().is_ok() {
                                val.to_string()
                            } else {
                                format!("'{}'", val)
                            };
                            conditions.push(format!("{} {} {}", col, op, val_formatted));
                        }
                    }
                }
            }
        }

        conditions
    }

    fn looks_like_table(&self, word: &str) -> bool {
        // Heuristics for table names
        word.ends_with('s') || // Plural
        word.ends_with("es") ||
        word.contains('_') ||
        word.len() > 4
    }

    fn clean_identifier(&self, s: &str) -> String {
        // Remove common suffixes and clean up
        let s = s.trim_end_matches(',').trim_end_matches('.');
        s.replace(' ', "_").to_lowercase()
    }

    fn parse_columns(&self, columns_str: &str) -> String {
        if columns_str.is_empty() || columns_str == "all" || columns_str == "everything" {
            return "*".to_string();
        }

        let cols: Vec<&str> = columns_str
            .split(|c| c == ',' || c == ' ')
            .filter(|s| !s.is_empty() && !self.stop_words.contains(s))
            .collect();

        if cols.is_empty() {
            "*".to_string()
        } else {
            cols.join(", ")
        }
    }

    fn parse_condition(&self, condition_str: &str) -> String {
        let mut result = condition_str.to_string();

        // Replace natural language comparisons with SQL operators
        for (phrase, op) in &self.comparisons {
            let pattern = format!(r"(?i)\b{}\b", regex::escape(phrase));
            if let Ok(re) = Regex::new(&pattern) {
                result = re.replace_all(&result, *op).to_string();
            }
        }

        // Handle "X is Y" -> "X = Y"
        if let Ok(re) = Regex::new(r"(\w+)\s+is\s+(\w+)") {
            result = re.replace_all(&result, "$1 = '$2'").to_string();
        }

        // Handle unquoted values after equals
        if let Ok(re) = Regex::new(r#"=\s*(\w+)(?!['\"])"#) {
            result = re.replace_all(&result, "= '$1'").to_string();
        }

        result
    }

    fn parse_insert_data(&self, data_str: &str) -> (String, String) {
        let mut columns = Vec::new();
        let mut values = Vec::new();

        // Parse column=value or column:value pairs
        if let Ok(re) = Regex::new(r"(\w+)\s*[=:]\s*([^,]+)") {
            for cap in re.captures_iter(data_str) {
                if let (Some(col), Some(val)) = (cap.get(1), cap.get(2)) {
                    columns.push(col.as_str().to_string());
                    let val_str = val.as_str().trim();
                    // Quote if not a number
                    if val_str.parse::<f64>().is_ok() {
                        values.push(val_str.to_string());
                    } else {
                        let cleaned = val_str.trim_matches('\'');
                        values.push(format!("'{}'", cleaned));
                    }
                }
            }
        }

        (columns.join(", "), values.join(", "))
    }

    fn parse_assignments(&self, assignments_str: &str) -> String {
        self.parse_condition(assignments_str)
    }

    fn parse_column_definitions(&self, cols_str: &str) -> String {
        let mut definitions = Vec::new();

        // Parse column definitions
        let parts: Vec<&str> = cols_str.split(',').collect();

        for part in parts {
            let part = part.trim();
            let words: Vec<&str> = part.split_whitespace().collect();

            if words.len() >= 2 {
                let col_name = words[0];
                let col_type = self.infer_sql_type(words[1]);
                definitions.push(format!("{} {}", col_name, col_type));
            } else if words.len() == 1 {
                // Infer type from name
                let col_name = words[0];
                let col_type = self.infer_type_from_name(col_name);
                definitions.push(format!("{} {}", col_name, col_type));
            }
        }

        if definitions.is_empty() {
            "id BIGINT PRIMARY KEY".to_string()
        } else {
            definitions.join(", ")
        }
    }

    fn infer_sql_type(&self, type_hint: &str) -> &str {
        match type_hint.to_lowercase().as_str() {
            "text" | "string" | "str" | "varchar" => "VARCHAR(255)",
            "int" | "integer" | "number" | "num" => "INT",
            "bigint" | "long" => "BIGINT",
            "float" | "double" | "decimal" | "money" => "DECIMAL(12,2)",
            "bool" | "boolean" | "flag" => "BOOLEAN",
            "date" => "DATE",
            "time" => "TIME",
            "datetime" | "timestamp" => "TIMESTAMP",
            "json" | "object" => "JSON",
            _ => "VARCHAR(255)",
        }
    }

    fn infer_type_from_name(&self, name: &str) -> &str {
        let name_lower = name.to_lowercase();
        if name_lower.contains("id") {
            "BIGINT"
        } else if name_lower.contains("name") || name_lower.contains("title") || name_lower.contains("description") {
            "VARCHAR(255)"
        } else if name_lower.contains("email") {
            "VARCHAR(150)"
        } else if name_lower.contains("phone") {
            "VARCHAR(20)"
        } else if name_lower.contains("date") || name_lower.contains("_at") {
            "TIMESTAMP"
        } else if name_lower.contains("price") || name_lower.contains("amount") || name_lower.contains("salary") {
            "DECIMAL(12,2)"
        } else if name_lower.contains("count") || name_lower.contains("quantity") || name_lower.contains("age") {
            "INT"
        } else if name_lower.contains("is_") || name_lower.contains("has_") || name_lower.contains("active") {
            "BOOLEAN"
        } else {
            "VARCHAR(255)"
        }
    }

    fn resolve_table_name(&self, name: &str, context: &Option<QueryContext>) -> String {
        let clean_name = self.clean_identifier(name);

        // Check entity mappings
        if let Some(ctx) = context {
            if let Some(mapped) = ctx.entity_mappings.get(&clean_name) {
                return mapped.clone();
            }
        }

        clean_name
    }

    fn generate_alternatives(&self, sql: &str, intent: &QueryIntent) -> Vec<String> {
        let mut alternatives = Vec::new();

        match intent {
            QueryIntent::Select => {
                // Suggest with LIMIT
                if !sql.contains("LIMIT") {
                    alternatives.push(format!("{} LIMIT 10", sql));
                }
                // Suggest with ORDER BY
                if !sql.contains("ORDER BY") {
                    alternatives.push(format!("{} ORDER BY 1", sql));
                }
            }
            QueryIntent::Aggregate => {
                // Suggest GROUP BY variants
                if !sql.contains("GROUP BY") {
                    alternatives.push(format!("{} GROUP BY 1", sql));
                }
            }
            _ => {}
        }

        alternatives
    }

    /// Register schema information for better query understanding
    pub fn register_schema(&self, table: &str, columns: Vec<String>) {
        self.schema_cache.write().insert(table.to_string(), columns);
    }
}

// ============================================================================
// Dynamic Variable Processor
// ============================================================================

/// Process queries with dynamic variable replacement
pub struct VariableProcessor;

impl VariableProcessor {
    /// Replace double-brace variable placeholders with values
    pub fn process(query: &str, variables: &HashMap<String, String>) -> String {
        let re = Regex::new(r"\{\{(\w+)\}\}").unwrap();

        re.replace_all(query, |caps: &regex::Captures| {
            let var_name = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            variables.get(var_name).cloned().unwrap_or_else(|| {
                format!("{{{{{}}}}}", var_name)
            })
        }).to_string()
    }

    /// Extract variable names from a query
    pub fn extract_variables(query: &str) -> Vec<String> {
        let re = Regex::new(r"\{\{(\w+)\}\}").unwrap();
        re.captures_iter(query)
            .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
            .collect()
    }

    /// Validate that all variables are provided
    pub fn validate(query: &str, variables: &HashMap<String, String>) -> Result<()> {
        let required = Self::extract_variables(query);
        let missing: Vec<&String> = required.iter()
            .filter(|v| !variables.contains_key(*v))
            .collect();

        if missing.is_empty() {
            Ok(())
        } else {
            let missing_str = missing.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
            Err(Error::Sql(SqlError::ParseError(
                format!("Missing variables: {}", missing_str)
            )))
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nlprocessor_new() {
        let processor = NLProcessor::new();
        assert!(!processor.patterns.is_empty());
        assert!(!processor.stop_words.is_empty());
        assert!(!processor.aggregations.is_empty());
    }

    #[test]
    fn test_simple_select() {
        let processor = NLProcessor::new();
        let query = NLQuery {
            text: "show all users".to_string(),
            variables: HashMap::new(),
            context: None,
        };
        let result = processor.process(query).unwrap();
        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.to_lowercase().contains("users"));
        assert_eq!(result.intent, QueryIntent::Select);
    }

    #[test]
    fn test_count_query() {
        let processor = NLProcessor::new();
        let query = NLQuery {
            text: "count users".to_string(),
            variables: HashMap::new(),
            context: None,
        };
        let result = processor.process(query).unwrap();
        assert!(result.sql.contains("COUNT"));
        assert_eq!(result.intent, QueryIntent::Aggregate);
    }

    #[test]
    fn test_variable_substitution() {
        let mut variables = HashMap::new();
        variables.insert("table".to_string(), "employees".to_string());
        variables.insert("limit".to_string(), "10".to_string());

        let result = VariableProcessor::process("SELECT * FROM {{table}} LIMIT {{limit}}", &variables);
        assert_eq!(result, "SELECT * FROM employees LIMIT 10");
    }

    #[test]
    fn test_extract_variables() {
        let vars = VariableProcessor::extract_variables("SELECT * FROM {{table}} WHERE id = {{id}}");
        assert_eq!(vars.len(), 2);
        assert!(vars.contains(&"table".to_string()));
        assert!(vars.contains(&"id".to_string()));
    }

    #[test]
    fn test_validate_missing_variables() {
        let variables = HashMap::new();
        let result = VariableProcessor::validate("SELECT * FROM {{table}}", &variables);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_all_variables_present() {
        let mut variables = HashMap::new();
        variables.insert("table".to_string(), "users".to_string());
        let result = VariableProcessor::validate("SELECT * FROM {{table}}", &variables);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_with_context() {
        let processor = NLProcessor::new();
        let mut entity_mappings = HashMap::new();
        entity_mappings.insert("employees".to_string(), "employment".to_string());

        let query = NLQuery {
            text: "show all employees".to_string(),
            variables: HashMap::new(),
            context: Some(QueryContext {
                tables: vec![],
                role: None,
                history: vec![],
                entity_mappings,
            }),
        };
        let result = processor.process(query).unwrap();
        assert!(result.sql.contains("employment"));
    }

    #[test]
    fn test_select_with_where() {
        let processor = NLProcessor::new();
        let query = NLQuery {
            text: "get users from users where status is active".to_string(),
            variables: HashMap::new(),
            context: None,
        };
        let result = processor.process(query).unwrap();
        assert!(result.sql.contains("WHERE"));
    }

    #[test]
    fn test_intent_detection() {
        let processor = NLProcessor::new();

        // Select intent
        let words = vec!["show", "users"];
        assert_eq!(processor.detect_intent(&words), QueryIntent::Select);

        // Aggregate intent
        let words = vec!["count", "orders"];
        assert_eq!(processor.detect_intent(&words), QueryIntent::Aggregate);

        // Insert intent
        let words = vec!["add", "user"];
        assert_eq!(processor.detect_intent(&words), QueryIntent::Insert);

        // Delete intent
        let words = vec!["delete", "record"];
        assert_eq!(processor.detect_intent(&words), QueryIntent::Delete);
    }

    #[test]
    fn test_parse_condition() {
        let processor = NLProcessor::new();
        let result = processor.parse_condition("age greater than 18");
        assert!(result.contains(">"));
    }

    #[test]
    fn test_looks_like_table() {
        let processor = NLProcessor::new();
        assert!(processor.looks_like_table("users"));
        assert!(processor.looks_like_table("user_accounts"));
        assert!(!processor.looks_like_table("id"));
    }
}
