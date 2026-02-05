//! Multi-Dialect SQL Support
//!
//! Auto-detects and translates between PostgreSQL, MySQL, MongoDB, and Redis syntax.
//! Converts all dialects to ThunderDB's internal SQL representation.

use std::collections::HashMap;
use regex::Regex;
use serde_json::Value as JsonValue;

use thunder_common::prelude::*;
use thunder_common::error::SqlError;

// ============================================================================
// Dialect Detection
// ============================================================================

/// Supported query dialects
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// PostgreSQL syntax (default)
    PostgreSQL,
    /// MySQL syntax with AUTO_INCREMENT, ENUM, backticks, etc.
    MySQL,
    /// MongoDB query syntax (JSON-based)
    MongoDB,
    /// Redis RESP commands
    Redis,
    /// Unknown/ambiguous
    Unknown,
}

impl std::fmt::Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dialect::PostgreSQL => write!(f, "PostgreSQL"),
            Dialect::MySQL => write!(f, "MySQL"),
            Dialect::MongoDB => write!(f, "MongoDB"),
            Dialect::Redis => write!(f, "Redis"),
            Dialect::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Dialect detector - analyzes query text to determine the dialect
pub struct DialectDetector {
    mysql_patterns: Vec<Regex>,
    mongodb_patterns: Vec<Regex>,
    redis_patterns: Vec<Regex>,
}

impl Default for DialectDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl DialectDetector {
    pub fn new() -> Self {
        Self {
            mysql_patterns: vec![
                Regex::new(r"(?i)\bAUTO_INCREMENT\b").unwrap(),
                Regex::new(r"(?i)\bENUM\s*\(").unwrap(),
                Regex::new(r"(?i)\bON\s+UPDATE\s+CURRENT_TIMESTAMP\b").unwrap(),
                Regex::new(r"(?i)\bUNSIGNED\b").unwrap(),
                Regex::new(r"(?i)\bTINYINT\b").unwrap(),
                Regex::new(r"(?i)\bMEDIUMINT\b").unwrap(),
                Regex::new(r"(?i)\bDOUBLE\b").unwrap(),
                Regex::new(r"(?i)\bIF\s+NOT\s+EXISTS\b").unwrap(),
                Regex::new(r"`[^`]+`").unwrap(), // Backtick identifiers
                Regex::new(r"(?i)\bLIMIT\s+\d+\s*,\s*\d+").unwrap(), // LIMIT offset, count
                Regex::new(r"(?i)\bSTRAIGHT_JOIN\b").unwrap(),
                Regex::new(r"(?i)\bSQL_CALC_FOUND_ROWS\b").unwrap(),
                Regex::new(r#"(?i)\bCOMMENT\s*=?\s*'"#).unwrap(),
                Regex::new(r"(?i)\bENGINE\s*=").unwrap(),
                Regex::new(r"(?i)\bCHARSET\s*=").unwrap(),
                Regex::new(r"(?i)\bCOLLATE\s*=").unwrap(),
            ],
            mongodb_patterns: vec![
                Regex::new(r"^\s*db\.").unwrap(),
                Regex::new(r"(?i)\.find\s*\(").unwrap(),
                Regex::new(r"(?i)\.insertOne\s*\(").unwrap(),
                Regex::new(r"(?i)\.insertMany\s*\(").unwrap(),
                Regex::new(r"(?i)\.updateOne\s*\(").unwrap(),
                Regex::new(r"(?i)\.updateMany\s*\(").unwrap(),
                Regex::new(r"(?i)\.deleteOne\s*\(").unwrap(),
                Regex::new(r"(?i)\.deleteMany\s*\(").unwrap(),
                Regex::new(r"(?i)\.aggregate\s*\(").unwrap(),
                Regex::new(r"(?i)\.createCollection\s*\(").unwrap(),
                Regex::new(r"(?i)\.drop\s*\(").unwrap(),
                Regex::new(r"\$match\b").unwrap(),
                Regex::new(r"\$group\b").unwrap(),
                Regex::new(r"\$project\b").unwrap(),
                Regex::new(r"\$sort\b").unwrap(),
                Regex::new(r"\$limit\b").unwrap(),
                Regex::new(r"\$lookup\b").unwrap(),
            ],
            redis_patterns: vec![
                Regex::new(r"(?i)^(GET|SET|DEL|MGET|MSET|HGET|HSET|HDEL|HGETALL)\s+").unwrap(),
                Regex::new(r"(?i)^(LPUSH|RPUSH|LPOP|RPOP|LRANGE|LLEN)\s+").unwrap(),
                Regex::new(r"(?i)^(SADD|SREM|SMEMBERS|SISMEMBER|SCARD)\s+").unwrap(),
                Regex::new(r"(?i)^(ZADD|ZREM|ZRANGE|ZRANK|ZSCORE|ZCARD)\s+").unwrap(),
                Regex::new(r"(?i)^(EXPIRE|TTL|PTTL|PERSIST|EXISTS|TYPE)\s+").unwrap(),
                Regex::new(r"(?i)^(INCR|DECR|INCRBY|DECRBY|INCRBYFLOAT)\s+").unwrap(),
                Regex::new(r"(?i)^(KEYS|SCAN|FLUSHDB|FLUSHALL|DBSIZE)\b").unwrap(),
                Regex::new(r"(?i)^(SUBSCRIBE|PUBLISH|PSUBSCRIBE)\s+").unwrap(),
                Regex::new(r"(?i)^(MULTI|EXEC|DISCARD|WATCH)\b").unwrap(),
                Regex::new(r"(?i)^(PING|ECHO|INFO|CONFIG)\b").unwrap(),
            ],
        }
    }

    /// Detect the dialect of a query
    pub fn detect(&self, query: &str) -> Dialect {
        let query = query.trim();

        // Check Redis first (simple command format)
        for pattern in &self.redis_patterns {
            if pattern.is_match(query) {
                return Dialect::Redis;
            }
        }

        // Check MongoDB (db.collection.method format)
        for pattern in &self.mongodb_patterns {
            if pattern.is_match(query) {
                return Dialect::MongoDB;
            }
        }

        // Check for MySQL-specific syntax
        let mysql_score: usize = self.mysql_patterns.iter()
            .filter(|p| p.is_match(query))
            .count();

        if mysql_score >= 1 {
            return Dialect::MySQL;
        }

        // Default to PostgreSQL (most compatible)
        Dialect::PostgreSQL
    }
}

// ============================================================================
// MySQL Translator
// ============================================================================

/// Translates MySQL syntax to PostgreSQL/ThunderDB syntax
pub struct MySQLTranslator;

impl MySQLTranslator {
    /// Translate MySQL query to PostgreSQL-compatible syntax
    pub fn translate(query: &str) -> String {
        let mut result = query.to_string();

        // Replace backtick identifiers with double quotes
        result = Self::replace_backticks(&result);

        // Replace AUTO_INCREMENT with SERIAL or BIGSERIAL
        result = Self::replace_auto_increment(&result);

        // Replace ENUM with VARCHAR
        result = Self::replace_enum(&result);

        // Replace MySQL-specific types
        result = Self::replace_types(&result);

        // Remove MySQL-specific clauses
        result = Self::remove_mysql_clauses(&result);

        // Convert LIMIT offset, count to LIMIT count OFFSET offset
        result = Self::convert_limit(&result);

        // Remove ON UPDATE CURRENT_TIMESTAMP
        result = Self::remove_on_update(&result);

        // Replace != with <>
        result = result.replace("!=", "<>");

        // Replace || (or) with OR
        let or_regex = Regex::new(r"\s*\|\|\s*").unwrap();
        result = or_regex.replace_all(&result, " OR ").to_string();

        result
    }

    fn replace_backticks(query: &str) -> String {
        let re = Regex::new(r"`([^`]+)`").unwrap();
        re.replace_all(query, "\"$1\"").to_string()
    }

    fn replace_auto_increment(query: &str) -> String {
        let re = Regex::new(r"(?i)\b(BIGINT|INT|INTEGER)\s+AUTO_INCREMENT\s+PRIMARY\s+KEY").unwrap();
        let result = re.replace_all(query, "$1 PRIMARY KEY").to_string();

        let re2 = Regex::new(r"(?i)\bAUTO_INCREMENT\b").unwrap();
        re2.replace_all(&result, "").to_string()
    }

    fn replace_enum(query: &str) -> String {
        // Replace ENUM('val1', 'val2', ...) with VARCHAR(50)
        let re = Regex::new(r"(?i)\bENUM\s*\([^)]+\)").unwrap();
        re.replace_all(query, "VARCHAR(50)").to_string()
    }

    fn replace_types(query: &str) -> String {
        let mut result = query.to_string();

        // TINYINT -> SMALLINT
        let re = Regex::new(r"(?i)\bTINYINT\b(\(\d+\))?").unwrap();
        result = re.replace_all(&result, "SMALLINT").to_string();

        // MEDIUMINT -> INT
        let re = Regex::new(r"(?i)\bMEDIUMINT\b(\(\d+\))?").unwrap();
        result = re.replace_all(&result, "INT").to_string();

        // DOUBLE -> DOUBLE PRECISION (workaround: replace then fix double replacement)
        let re = Regex::new(r"(?i)\bDOUBLE\b").unwrap();
        result = re.replace_all(&result, "DOUBLE PRECISION").to_string();
        // Fix any accidental double replacement
        let re = Regex::new(r"(?i)DOUBLE PRECISION\s+PRECISION").unwrap();
        result = re.replace_all(&result, "DOUBLE PRECISION").to_string();

        // UNSIGNED -> (remove)
        let re = Regex::new(r"(?i)\s+UNSIGNED\b").unwrap();
        result = re.replace_all(&result, "").to_string();

        // DATETIME -> TIMESTAMP
        let re = Regex::new(r"(?i)\bDATETIME\b").unwrap();
        result = re.replace_all(&result, "TIMESTAMP").to_string();

        // INT(n) -> INT (remove display width)
        let re = Regex::new(r"(?i)\b(INT|INTEGER)\(\d+\)").unwrap();
        result = re.replace_all(&result, "$1").to_string();

        // BIGINT(n) -> BIGINT
        let re = Regex::new(r"(?i)\bBIGINT\(\d+\)").unwrap();
        result = re.replace_all(&result, "BIGINT").to_string();

        result
    }

    fn remove_mysql_clauses(query: &str) -> String {
        let mut result = query.to_string();

        // Remove ENGINE=...
        let re = Regex::new(r"(?i)\s*ENGINE\s*=\s*\w+").unwrap();
        result = re.replace_all(&result, "").to_string();

        // Remove DEFAULT CHARSET=...
        let re = Regex::new(r"(?i)\s*(DEFAULT\s+)?CHARSET\s*=\s*\w+").unwrap();
        result = re.replace_all(&result, "").to_string();

        // Remove COLLATE=...
        let re = Regex::new(r"(?i)\s*COLLATE\s*=?\s*\w+").unwrap();
        result = re.replace_all(&result, "").to_string();

        // Remove COMMENT='...'
        let re = Regex::new(r"(?i)\s*COMMENT\s*=?\s*'[^']*'").unwrap();
        result = re.replace_all(&result, "").to_string();

        // Remove IF NOT EXISTS (keep for compatibility, but note it)
        // Actually, let's keep IF NOT EXISTS as it's useful

        result
    }

    fn convert_limit(query: &str) -> String {
        // Convert LIMIT offset, count -> LIMIT count OFFSET offset
        let re = Regex::new(r"(?i)\bLIMIT\s+(\d+)\s*,\s*(\d+)").unwrap();
        re.replace_all(query, "LIMIT $2 OFFSET $1").to_string()
    }

    fn remove_on_update(query: &str) -> String {
        let re = Regex::new(r"(?i)\s*ON\s+UPDATE\s+CURRENT_TIMESTAMP(\(\))?").unwrap();
        re.replace_all(query, "").to_string()
    }
}

// ============================================================================
// MongoDB Translator
// ============================================================================

/// Translates MongoDB queries to SQL
pub struct MongoDBTranslator;

impl MongoDBTranslator {
    /// Parse and translate MongoDB query to SQL
    pub fn translate(query: &str) -> Result<String> {
        let query = query.trim();

        // Parse: db.collection.method(args)
        let re = Regex::new(r"db\.(\w+)\.(\w+)\(([\s\S]*)\)").unwrap();

        if let Some(caps) = re.captures(query) {
            let collection = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            let method = caps.get(2).map(|m| m.as_str()).unwrap_or("");
            let args_str = caps.get(3).map(|m| m.as_str()).unwrap_or("");

            match method.to_lowercase().as_str() {
                "find" => Self::translate_find(collection, args_str),
                "findone" => Self::translate_find_one(collection, args_str),
                "insertone" => Self::translate_insert_one(collection, args_str),
                "insertmany" => Self::translate_insert_many(collection, args_str),
                "updateone" | "updatemany" => Self::translate_update(collection, args_str),
                "deleteone" | "deletemany" => Self::translate_delete(collection, args_str),
                "count" | "countdocuments" => Self::translate_count(collection, args_str),
                "createcollection" => Ok(format!("CREATE TABLE {} (id BIGINT PRIMARY KEY, data JSON)", collection)),
                "drop" => Ok(format!("DROP TABLE {}", collection)),
                "aggregate" => Self::translate_aggregate(collection, args_str),
                _ => Err(Error::Sql(SqlError::UnsupportedFeature(format!("MongoDB method: {}", method)))),
            }
        } else {
            Err(Error::Sql(SqlError::ParseError("Invalid MongoDB query format".to_string())))
        }
    }

    fn translate_find(collection: &str, args: &str) -> Result<String> {
        let args = args.trim();

        if args.is_empty() || args == "{}" {
            return Ok(format!("SELECT * FROM {}", collection));
        }

        // Parse filter and projection
        let parts: Vec<&str> = Self::split_mongo_args(args);
        let filter = parts.first().unwrap_or(&"{}");
        let projection = parts.get(1);

        let where_clause = Self::mongo_filter_to_sql(filter)?;
        let select_clause = projection
            .map(|p| Self::mongo_projection_to_sql(p))
            .unwrap_or_else(|| Ok("*".to_string()))?;

        if where_clause.is_empty() {
            Ok(format!("SELECT {} FROM {}", select_clause, collection))
        } else {
            Ok(format!("SELECT {} FROM {} WHERE {}", select_clause, collection, where_clause))
        }
    }

    fn translate_find_one(collection: &str, args: &str) -> Result<String> {
        let base_query = Self::translate_find(collection, args)?;
        Ok(format!("{} LIMIT 1", base_query))
    }

    fn translate_insert_one(collection: &str, args: &str) -> Result<String> {
        let doc: JsonValue = serde_json::from_str(args.trim())
            .map_err(|e| Error::Sql(SqlError::ParseError(format!("Invalid JSON: {}", e))))?;

        if let JsonValue::Object(obj) = doc {
            let columns: Vec<String> = obj.keys().cloned().collect();
            let values: Vec<String> = obj.values()
                .map(Self::json_value_to_sql)
                .collect();

            Ok(format!(
                "INSERT INTO {} ({}) VALUES ({})",
                collection,
                columns.join(", "),
                values.join(", ")
            ))
        } else {
            Err(Error::Sql(SqlError::ParseError("Insert requires an object".to_string())))
        }
    }

    fn translate_insert_many(collection: &str, args: &str) -> Result<String> {
        let docs: Vec<JsonValue> = serde_json::from_str(args.trim())
            .map_err(|e| Error::Sql(SqlError::ParseError(format!("Invalid JSON array: {}", e))))?;

        if docs.is_empty() {
            return Err(Error::Sql(SqlError::ParseError("Empty insert array".to_string())));
        }

        // Get columns from first document
        let first = docs.first().unwrap();
        let columns: Vec<String> = if let JsonValue::Object(obj) = first {
            obj.keys().cloned().collect()
        } else {
            return Err(Error::Sql(SqlError::ParseError("Documents must be objects".to_string())));
        };

        let values_rows: Vec<String> = docs.iter()
            .filter_map(|doc| {
                if let JsonValue::Object(obj) = doc {
                    let vals: Vec<String> = columns.iter()
                        .map(|col| obj.get(col)
                            .map(Self::json_value_to_sql)
                            .unwrap_or_else(|| "NULL".to_string()))
                        .collect();
                    Some(format!("({})", vals.join(", ")))
                } else {
                    None
                }
            })
            .collect();

        Ok(format!(
            "INSERT INTO {} ({}) VALUES {}",
            collection,
            columns.join(", "),
            values_rows.join(", ")
        ))
    }

    fn translate_update(collection: &str, args: &str) -> Result<String> {
        let parts: Vec<&str> = Self::split_mongo_args(args);

        if parts.len() < 2 {
            return Err(Error::Sql(SqlError::ParseError("Update requires filter and update document".to_string())));
        }

        let filter = parts[0];
        let update = parts[1];

        let where_clause = Self::mongo_filter_to_sql(filter)?;
        let set_clause = Self::mongo_update_to_sql(update)?;

        if where_clause.is_empty() {
            Ok(format!("UPDATE {} SET {}", collection, set_clause))
        } else {
            Ok(format!("UPDATE {} SET {} WHERE {}", collection, set_clause, where_clause))
        }
    }

    fn translate_delete(collection: &str, args: &str) -> Result<String> {
        let args = args.trim();

        if args.is_empty() || args == "{}" {
            return Ok(format!("DELETE FROM {}", collection));
        }

        let where_clause = Self::mongo_filter_to_sql(args)?;

        if where_clause.is_empty() {
            Ok(format!("DELETE FROM {}", collection))
        } else {
            Ok(format!("DELETE FROM {} WHERE {}", collection, where_clause))
        }
    }

    fn translate_count(collection: &str, args: &str) -> Result<String> {
        let args = args.trim();

        if args.is_empty() || args == "{}" {
            return Ok(format!("SELECT COUNT(*) FROM {}", collection));
        }

        let where_clause = Self::mongo_filter_to_sql(args)?;

        if where_clause.is_empty() {
            Ok(format!("SELECT COUNT(*) FROM {}", collection))
        } else {
            Ok(format!("SELECT COUNT(*) FROM {} WHERE {}", collection, where_clause))
        }
    }

    fn translate_aggregate(collection: &str, args: &str) -> Result<String> {
        let pipeline: Vec<JsonValue> = serde_json::from_str(args.trim())
            .map_err(|e| Error::Sql(SqlError::ParseError(format!("Invalid pipeline: {}", e))))?;

        let mut select_parts = vec!["*".to_string()];
        let mut where_parts = Vec::new();
        let mut group_by = Vec::new();
        let mut order_by = Vec::new();
        let mut limit_val: Option<i64> = None;
        let mut skip_val: Option<i64> = None;

        for stage in pipeline {
            if let JsonValue::Object(obj) = stage {
                for (op, val) in obj {
                    match op.as_str() {
                        "$match" => {
                            if let JsonValue::Object(filter) = val {
                                for (k, v) in filter {
                                    where_parts.push(Self::mongo_condition_to_sql(&k, &v));
                                }
                            }
                        }
                        "$project" => {
                            if let JsonValue::Object(proj) = val {
                                select_parts = proj.keys()
                                    .filter(|k| proj.get(*k).map(|v| v != &JsonValue::from(0)).unwrap_or(true))
                                    .cloned()
                                    .collect();
                            }
                        }
                        "$group" => {
                            if let JsonValue::Object(grp) = val {
                                if let Some(id) = grp.get("_id") {
                                    match id {
                                        JsonValue::String(s) if s.starts_with('$') => {
                                            group_by.push(s[1..].to_string());
                                        }
                                        JsonValue::Object(obj) => {
                                            for (_, v) in obj {
                                                if let JsonValue::String(s) = v {
                                                    if s.starts_with('$') {
                                                        group_by.push(s[1..].to_string());
                                                    }
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                // Handle aggregation functions
                                select_parts.clear();
                                for (key, value) in &grp {
                                    if key == "_id" {
                                        continue;
                                    }
                                    if let JsonValue::Object(agg) = value {
                                        for (func, field) in agg {
                                            let field_name = if let JsonValue::String(s) = field {
                                                if s.starts_with('$') { &s[1..] } else { s.as_str() }
                                            } else {
                                                "*"
                                            };
                                            let sql_func = match func.as_str() {
                                                "$sum" => format!("SUM({})", field_name),
                                                "$avg" => format!("AVG({})", field_name),
                                                "$min" => format!("MIN({})", field_name),
                                                "$max" => format!("MAX({})", field_name),
                                                "$count" => "COUNT(*)".to_string(),
                                                _ => field_name.to_string(),
                                            };
                                            select_parts.push(format!("{} AS {}", sql_func, key));
                                        }
                                    }
                                }
                                for g in &group_by {
                                    select_parts.insert(0, g.clone());
                                }
                            }
                        }
                        "$sort" => {
                            if let JsonValue::Object(sort) = val {
                                for (field, dir) in sort {
                                    let direction = if dir.as_i64().unwrap_or(1) == -1 { "DESC" } else { "ASC" };
                                    order_by.push(format!("{} {}", field, direction));
                                }
                            }
                        }
                        "$limit" => {
                            limit_val = val.as_i64();
                        }
                        "$skip" => {
                            skip_val = val.as_i64();
                        }
                        _ => {}
                    }
                }
            }
        }

        let mut sql = format!("SELECT {} FROM {}", select_parts.join(", "), collection);

        if !where_parts.is_empty() {
            sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
        }

        if !group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_by.join(", ")));
        }

        if !order_by.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", order_by.join(", ")));
        }

        if let Some(limit) = limit_val {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(skip) = skip_val {
            sql.push_str(&format!(" OFFSET {}", skip));
        }

        Ok(sql)
    }

    fn split_mongo_args(args: &str) -> Vec<&str> {
        // Simple split on top-level commas (between {} blocks)
        let mut result = Vec::new();
        let mut depth = 0;
        let mut start = 0;
        let chars: Vec<char> = args.chars().collect();

        for (i, c) in chars.iter().enumerate() {
            match c {
                '{' | '[' => depth += 1,
                '}' | ']' => depth -= 1,
                ',' if depth == 0 => {
                    result.push(args[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            }
        }

        if start < args.len() {
            result.push(args[start..].trim());
        }

        result
    }

    fn mongo_filter_to_sql(filter: &str) -> Result<String> {
        let filter: JsonValue = serde_json::from_str(filter.trim())
            .map_err(|e| Error::Sql(SqlError::ParseError(format!("Invalid filter JSON: {}", e))))?;

        if let JsonValue::Object(obj) = filter {
            if obj.is_empty() {
                return Ok(String::new());
            }

            let conditions: Vec<String> = obj.iter()
                .map(|(k, v)| Self::mongo_condition_to_sql(k, v))
                .collect();

            Ok(conditions.join(" AND "))
        } else {
            Ok(String::new())
        }
    }

    fn mongo_condition_to_sql(field: &str, value: &JsonValue) -> String {
        // Handle special operators
        if field.starts_with('$') {
            return match field {
                "$and" => {
                    if let JsonValue::Array(arr) = value {
                        let conds: Vec<String> = arr.iter()
                            .filter_map(|v| {
                                if let JsonValue::Object(obj) = v {
                                    Some(obj.iter()
                                        .map(|(k, v)| Self::mongo_condition_to_sql(k, v))
                                        .collect::<Vec<_>>()
                                        .join(" AND "))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        format!("({})", conds.join(" AND "))
                    } else {
                        "1=1".to_string()
                    }
                }
                "$or" => {
                    if let JsonValue::Array(arr) = value {
                        let conds: Vec<String> = arr.iter()
                            .filter_map(|v| {
                                if let JsonValue::Object(obj) = v {
                                    Some(obj.iter()
                                        .map(|(k, v)| Self::mongo_condition_to_sql(k, v))
                                        .collect::<Vec<_>>()
                                        .join(" AND "))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        format!("({})", conds.join(" OR "))
                    } else {
                        "1=1".to_string()
                    }
                }
                _ => "1=1".to_string(),
            };
        }

        match value {
            JsonValue::Object(ops) => {
                let mut conditions = Vec::new();
                for (op, val) in ops {
                    let cond = match op.as_str() {
                        "$eq" => format!("{} = {}", field, Self::json_value_to_sql(val)),
                        "$ne" => format!("{} <> {}", field, Self::json_value_to_sql(val)),
                        "$gt" => format!("{} > {}", field, Self::json_value_to_sql(val)),
                        "$gte" => format!("{} >= {}", field, Self::json_value_to_sql(val)),
                        "$lt" => format!("{} < {}", field, Self::json_value_to_sql(val)),
                        "$lte" => format!("{} <= {}", field, Self::json_value_to_sql(val)),
                        "$in" => {
                            if let JsonValue::Array(arr) = val {
                                let vals: Vec<String> = arr.iter().map(Self::json_value_to_sql).collect();
                                format!("{} IN ({})", field, vals.join(", "))
                            } else {
                                format!("{} = {}", field, Self::json_value_to_sql(val))
                            }
                        }
                        "$nin" => {
                            if let JsonValue::Array(arr) = val {
                                let vals: Vec<String> = arr.iter().map(Self::json_value_to_sql).collect();
                                format!("{} NOT IN ({})", field, vals.join(", "))
                            } else {
                                format!("{} <> {}", field, Self::json_value_to_sql(val))
                            }
                        }
                        "$regex" => {
                            if let JsonValue::String(pattern) = val {
                                format!("{} LIKE '%{}%'", field, pattern.replace('%', "\\%"))
                            } else {
                                "1=1".to_string()
                            }
                        }
                        "$exists" => {
                            if val.as_bool().unwrap_or(true) {
                                format!("{} IS NOT NULL", field)
                            } else {
                                format!("{} IS NULL", field)
                            }
                        }
                        _ => "1=1".to_string(),
                    };
                    conditions.push(cond);
                }
                conditions.join(" AND ")
            }
            _ => format!("{} = {}", field, Self::json_value_to_sql(value)),
        }
    }

    fn mongo_update_to_sql(update: &str) -> Result<String> {
        let update: JsonValue = serde_json::from_str(update.trim())
            .map_err(|e| Error::Sql(SqlError::ParseError(format!("Invalid update JSON: {}", e))))?;

        if let JsonValue::Object(obj) = update {
            let mut sets = Vec::new();

            for (op, fields) in obj {
                if let JsonValue::Object(field_vals) = fields {
                    match op.as_str() {
                        "$set" => {
                            for (field, value) in field_vals {
                                sets.push(format!("{} = {}", field, Self::json_value_to_sql(&value)));
                            }
                        }
                        "$inc" => {
                            for (field, value) in field_vals {
                                sets.push(format!("{} = {} + {}", field, field, Self::json_value_to_sql(&value)));
                            }
                        }
                        "$unset" => {
                            for (field, _) in field_vals {
                                sets.push(format!("{} = NULL", field));
                            }
                        }
                        _ => {}
                    }
                }
            }

            if sets.is_empty() {
                Err(Error::Sql(SqlError::ParseError("No valid update operations".to_string())))
            } else {
                Ok(sets.join(", "))
            }
        } else {
            Err(Error::Sql(SqlError::ParseError("Update must be an object".to_string())))
        }
    }

    fn mongo_projection_to_sql(projection: &str) -> Result<String> {
        let proj: JsonValue = serde_json::from_str(projection.trim())
            .map_err(|e| Error::Sql(SqlError::ParseError(format!("Invalid projection JSON: {}", e))))?;

        if let JsonValue::Object(obj) = proj {
            let fields: Vec<String> = obj.iter()
                .filter(|(_, v)| v.as_i64().unwrap_or(1) != 0)
                .map(|(k, _)| k.clone())
                .collect();

            if fields.is_empty() {
                Ok("*".to_string())
            } else {
                Ok(fields.join(", "))
            }
        } else {
            Ok("*".to_string())
        }
    }

    fn json_value_to_sql(value: &JsonValue) -> String {
        match value {
            JsonValue::Null => "NULL".to_string(),
            JsonValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            JsonValue::Array(arr) => {
                let vals: Vec<String> = arr.iter().map(Self::json_value_to_sql).collect();
                format!("ARRAY[{}]", vals.join(", "))
            }
            JsonValue::Object(_) => format!("'{}'", value.to_string().replace('\'', "''")),
        }
    }
}

// ============================================================================
// Redis Translator
// ============================================================================

/// Translates Redis commands to SQL operations on a key-value table
pub struct RedisTranslator;

impl RedisTranslator {
    /// Translate Redis command to SQL
    /// Assumes a table structure: kv_store(key VARCHAR PRIMARY KEY, value TEXT, type VARCHAR, expiry TIMESTAMP)
    pub fn translate(command: &str) -> Result<String> {
        let parts: Vec<&str> = command.split_whitespace().collect();

        if parts.is_empty() {
            return Err(Error::Sql(SqlError::ParseError("Empty Redis command".to_string())));
        }

        let cmd = parts[0].to_uppercase();
        let args = &parts[1..];

        match cmd.as_str() {
            // String operations
            "GET" => {
                Self::require_args(&cmd, args, 1)?;
                Ok(format!("SELECT value FROM kv_store WHERE key = '{}'", args[0]))
            }
            "SET" => {
                Self::require_args(&cmd, args, 2)?;
                let key = args[0];
                let value = args[1..].join(" ");
                Ok(format!(
                    "INSERT INTO kv_store (key, value, type) VALUES ('{}', '{}', 'string') ON CONFLICT (key) DO UPDATE SET value = '{}'",
                    key, value, value
                ))
            }
            "DEL" => {
                Self::require_args(&cmd, args, 1)?;
                let keys: Vec<String> = args.iter().map(|k| format!("'{}'", k)).collect();
                Ok(format!("DELETE FROM kv_store WHERE key IN ({})", keys.join(", ")))
            }
            "MGET" => {
                Self::require_args(&cmd, args, 1)?;
                let keys: Vec<String> = args.iter().map(|k| format!("'{}'", k)).collect();
                Ok(format!("SELECT key, value FROM kv_store WHERE key IN ({})", keys.join(", ")))
            }
            "EXISTS" => {
                Self::require_args(&cmd, args, 1)?;
                let keys: Vec<String> = args.iter().map(|k| format!("'{}'", k)).collect();
                Ok(format!("SELECT COUNT(*) FROM kv_store WHERE key IN ({})", keys.join(", ")))
            }
            "INCR" => {
                Self::require_args(&cmd, args, 1)?;
                Ok(format!(
                    "UPDATE kv_store SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = '{}'",
                    args[0]
                ))
            }
            "DECR" => {
                Self::require_args(&cmd, args, 1)?;
                Ok(format!(
                    "UPDATE kv_store SET value = CAST(CAST(value AS INTEGER) - 1 AS TEXT) WHERE key = '{}'",
                    args[0]
                ))
            }
            "INCRBY" => {
                Self::require_args(&cmd, args, 2)?;
                Ok(format!(
                    "UPDATE kv_store SET value = CAST(CAST(value AS INTEGER) + {} AS TEXT) WHERE key = '{}'",
                    args[1], args[0]
                ))
            }

            // Hash operations
            "HGET" => {
                Self::require_args(&cmd, args, 2)?;
                Ok(format!(
                    "SELECT value->>'{}' FROM kv_store WHERE key = '{}'",
                    args[1], args[0]
                ))
            }
            "HSET" => {
                Self::require_args(&cmd, args, 3)?;
                Ok(format!(
                    "UPDATE kv_store SET value = jsonb_set(COALESCE(value::jsonb, '{{}}'), '{{{}}}', '\"{}\"') WHERE key = '{}'",
                    args[1], args[2], args[0]
                ))
            }
            "HGETALL" => {
                Self::require_args(&cmd, args, 1)?;
                Ok(format!("SELECT value FROM kv_store WHERE key = '{}'", args[0]))
            }

            // List operations (stored as JSON arrays)
            "LPUSH" => {
                Self::require_args(&cmd, args, 2)?;
                let values: Vec<String> = args[1..].iter().map(|v| format!("\"{}\"", v)).collect();
                Ok(format!(
                    "UPDATE kv_store SET value = '[{}]' || COALESCE(value, '[]') WHERE key = '{}'",
                    values.join(","), args[0]
                ))
            }
            "RPUSH" => {
                Self::require_args(&cmd, args, 2)?;
                let values: Vec<String> = args[1..].iter().map(|v| format!("\"{}\"", v)).collect();
                Ok(format!(
                    "UPDATE kv_store SET value = COALESCE(value, '[]') || '[{}]' WHERE key = '{}'",
                    values.join(","), args[0]
                ))
            }
            "LRANGE" => {
                Self::require_args(&cmd, args, 3)?;
                Ok(format!(
                    "SELECT value FROM kv_store WHERE key = '{}'",
                    args[0]
                ))
            }
            "LLEN" => {
                Self::require_args(&cmd, args, 1)?;
                Ok(format!(
                    "SELECT json_array_length(value) FROM kv_store WHERE key = '{}'",
                    args[0]
                ))
            }

            // Set operations (stored as JSON arrays with uniqueness)
            "SADD" => {
                Self::require_args(&cmd, args, 2)?;
                let values: Vec<String> = args[1..].iter().map(|v| format!("\"{}\"", v)).collect();
                Ok(format!(
                    "INSERT INTO kv_store (key, value, type) VALUES ('{}', '[{}]', 'set') ON CONFLICT (key) DO UPDATE SET value = value",
                    args[0], values.join(",")
                ))
            }
            "SMEMBERS" => {
                Self::require_args(&cmd, args, 1)?;
                Ok(format!("SELECT value FROM kv_store WHERE key = '{}'", args[0]))
            }
            "SCARD" => {
                Self::require_args(&cmd, args, 1)?;
                Ok(format!(
                    "SELECT json_array_length(value) FROM kv_store WHERE key = '{}'",
                    args[0]
                ))
            }

            // Key operations
            "KEYS" => {
                let pattern = args.first().unwrap_or(&"*");
                let like_pattern = pattern.replace('*', "%").replace('?', "_");
                Ok(format!("SELECT key FROM kv_store WHERE key LIKE '{}'", like_pattern))
            }
            "TYPE" => {
                Self::require_args(&cmd, args, 1)?;
                Ok(format!("SELECT type FROM kv_store WHERE key = '{}'", args[0]))
            }
            "TTL" => {
                Self::require_args(&cmd, args, 1)?;
                Ok(format!(
                    "SELECT EXTRACT(EPOCH FROM (expiry - CURRENT_TIMESTAMP)) FROM kv_store WHERE key = '{}'",
                    args[0]
                ))
            }
            "EXPIRE" => {
                Self::require_args(&cmd, args, 2)?;
                Ok(format!(
                    "UPDATE kv_store SET expiry = CURRENT_TIMESTAMP + INTERVAL '{} seconds' WHERE key = '{}'",
                    args[1], args[0]
                ))
            }

            // Server commands
            "PING" => Ok("SELECT 'PONG'".to_string()),
            "DBSIZE" => Ok("SELECT COUNT(*) FROM kv_store".to_string()),
            "FLUSHDB" => Ok("DELETE FROM kv_store".to_string()),
            "INFO" => Ok("SELECT 'ThunderDB Redis Compatibility Layer'".to_string()),

            _ => Err(Error::Sql(SqlError::UnsupportedFeature(format!("Redis command: {}", cmd)))),
        }
    }

    fn require_args(cmd: &str, args: &[&str], min: usize) -> Result<()> {
        if args.len() < min {
            Err(Error::Sql(SqlError::ParseError(format!(
                "{} requires at least {} argument(s)", cmd, min
            ))))
        } else {
            Ok(())
        }
    }
}

// ============================================================================
// Unified Query Translator
// ============================================================================

/// Main translator that auto-detects dialect and translates to SQL
pub struct QueryTranslator {
    detector: DialectDetector,
}

impl Default for QueryTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryTranslator {
    pub fn new() -> Self {
        Self {
            detector: DialectDetector::new(),
        }
    }

    /// Translate a query from any supported dialect to ThunderDB SQL
    pub fn translate(&self, query: &str) -> Result<(String, Dialect)> {
        let dialect = self.detector.detect(query);

        let translated = match dialect {
            Dialect::PostgreSQL => query.to_string(),
            Dialect::MySQL => MySQLTranslator::translate(query),
            Dialect::MongoDB => MongoDBTranslator::translate(query)?,
            Dialect::Redis => RedisTranslator::translate(query)?,
            Dialect::Unknown => query.to_string(),
        };

        Ok((translated, dialect))
    }

    /// Translate with explicit dialect
    pub fn translate_with_dialect(&self, query: &str, dialect: Dialect) -> Result<String> {
        match dialect {
            Dialect::PostgreSQL => Ok(query.to_string()),
            Dialect::MySQL => Ok(MySQLTranslator::translate(query)),
            Dialect::MongoDB => MongoDBTranslator::translate(query),
            Dialect::Redis => RedisTranslator::translate(query),
            Dialect::Unknown => Ok(query.to_string()),
        }
    }

    /// Get the detected dialect for a query
    pub fn detect_dialect(&self, query: &str) -> Dialect {
        self.detector.detect(query)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dialect_detection_mysql() {
        let detector = DialectDetector::new();

        assert_eq!(
            detector.detect("CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY)"),
            Dialect::MySQL
        );
        assert_eq!(
            detector.detect("SELECT * FROM `users` WHERE `name` = 'test'"),
            Dialect::MySQL
        );
        assert_eq!(
            detector.detect("CREATE TABLE t (status ENUM('a', 'b'))"),
            Dialect::MySQL
        );
    }

    #[test]
    fn test_dialect_detection_mongodb() {
        let detector = DialectDetector::new();

        assert_eq!(
            detector.detect("db.users.find({})"),
            Dialect::MongoDB
        );
        assert_eq!(
            detector.detect("db.orders.aggregate([{$match: {status: 'active'}}])"),
            Dialect::MongoDB
        );
    }

    #[test]
    fn test_dialect_detection_redis() {
        let detector = DialectDetector::new();

        assert_eq!(detector.detect("GET mykey"), Dialect::Redis);
        assert_eq!(detector.detect("SET foo bar"), Dialect::Redis);
        assert_eq!(detector.detect("HGETALL user:1"), Dialect::Redis);
    }

    #[test]
    fn test_mysql_translation() {
        let result = MySQLTranslator::translate(
            "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))"
        );
        assert!(!result.contains("AUTO_INCREMENT"));
        assert!(result.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_mysql_enum_translation() {
        let result = MySQLTranslator::translate(
            "CREATE TABLE t (status ENUM('active', 'inactive'))"
        );
        assert!(result.contains("VARCHAR(50)"));
        assert!(!result.contains("ENUM"));
    }

    #[test]
    fn test_mysql_backtick_translation() {
        let result = MySQLTranslator::translate("SELECT `name` FROM `users`");
        assert!(result.contains("\"name\""));
        assert!(result.contains("\"users\""));
    }

    #[test]
    fn test_mongodb_find_translation() {
        let result = MongoDBTranslator::translate("db.users.find({})").unwrap();
        assert_eq!(result, "SELECT * FROM users");

        let result = MongoDBTranslator::translate("db.users.find({\"age\": {\"$gt\": 21}})").unwrap();
        assert!(result.contains("WHERE"));
        assert!(result.contains("age > 21"));
    }

    #[test]
    fn test_mongodb_insert_translation() {
        let result = MongoDBTranslator::translate(
            "db.users.insertOne({\"name\": \"John\", \"age\": 30})"
        ).unwrap();
        assert!(result.contains("INSERT INTO users"));
        assert!(result.contains("VALUES"));
    }

    #[test]
    fn test_redis_get_translation() {
        let result = RedisTranslator::translate("GET mykey").unwrap();
        assert!(result.contains("SELECT"));
        assert!(result.contains("kv_store"));
        assert!(result.contains("mykey"));
    }

    #[test]
    fn test_redis_set_translation() {
        let result = RedisTranslator::translate("SET foo bar").unwrap();
        assert!(result.contains("INSERT INTO kv_store"));
        assert!(result.contains("foo"));
        assert!(result.contains("bar"));
    }
}
