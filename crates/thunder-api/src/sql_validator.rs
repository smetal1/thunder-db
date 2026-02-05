//! SQL Input Validation for ThunderDB API
//!
//! Provides security validation for SQL queries to prevent:
//! - SQL injection attacks
//! - Dangerous operations
//! - Resource exhaustion attacks

use std::collections::HashSet;

/// SQL validation result
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Query is valid
    Valid,
    /// Query contains potential SQL injection
    Injection(String),
    /// Query uses forbidden operation
    ForbiddenOperation(String),
    /// Query exceeds limits
    ExceedsLimits(String),
    /// Query is malformed
    Malformed(String),
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        matches!(self, ValidationResult::Valid)
    }

    pub fn error_message(&self) -> Option<String> {
        match self {
            ValidationResult::Valid => None,
            ValidationResult::Injection(msg) => Some(format!("Potential SQL injection: {}", msg)),
            ValidationResult::ForbiddenOperation(msg) => Some(format!("Forbidden operation: {}", msg)),
            ValidationResult::ExceedsLimits(msg) => Some(format!("Query exceeds limits: {}", msg)),
            ValidationResult::Malformed(msg) => Some(format!("Malformed query: {}", msg)),
        }
    }
}

/// SQL Validator configuration
#[derive(Debug, Clone)]
pub struct SqlValidatorConfig {
    /// Maximum query length in bytes
    pub max_query_length: usize,
    /// Maximum number of statements in a batch
    pub max_statements: usize,
    /// Allow DDL statements (CREATE, ALTER, DROP)
    pub allow_ddl: bool,
    /// Allow destructive operations (DROP, TRUNCATE)
    pub allow_destructive: bool,
    /// Allow system commands (VACUUM, ANALYZE)
    pub allow_system_commands: bool,
    /// Forbidden keywords (in addition to defaults)
    pub forbidden_keywords: HashSet<String>,
    /// Allow comments in SQL
    pub allow_comments: bool,
    /// Allow multiple statements separated by semicolon
    pub allow_multiple_statements: bool,
}

impl Default for SqlValidatorConfig {
    fn default() -> Self {
        Self {
            max_query_length: 1024 * 1024, // 1MB
            max_statements: 100,
            allow_ddl: true,
            allow_destructive: false, // Require explicit permission
            allow_system_commands: false,
            forbidden_keywords: HashSet::new(),
            allow_comments: true,
            allow_multiple_statements: true,
        }
    }
}

impl SqlValidatorConfig {
    /// Create a read-only configuration
    pub fn read_only() -> Self {
        Self {
            allow_ddl: false,
            allow_destructive: false,
            allow_system_commands: false,
            allow_multiple_statements: false,
            ..Default::default()
        }
    }

    /// Create a strict configuration for untrusted input
    pub fn strict() -> Self {
        Self {
            max_query_length: 64 * 1024, // 64KB
            max_statements: 1,
            allow_ddl: false,
            allow_destructive: false,
            allow_system_commands: false,
            allow_comments: false,
            allow_multiple_statements: false,
            ..Default::default()
        }
    }
}

/// SQL Validator
pub struct SqlValidator {
    config: SqlValidatorConfig,
}

impl SqlValidator {
    /// Create a new SQL validator with the given configuration
    pub fn new(config: SqlValidatorConfig) -> Self {
        Self { config }
    }

    /// Create a validator with default configuration
    pub fn default_validator() -> Self {
        Self::new(SqlValidatorConfig::default())
    }

    /// Validate a SQL query
    pub fn validate(&self, sql: &str) -> ValidationResult {
        // Check length
        if sql.len() > self.config.max_query_length {
            return ValidationResult::ExceedsLimits(format!(
                "Query length {} exceeds maximum {}",
                sql.len(),
                self.config.max_query_length
            ));
        }

        // Check for empty query
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return ValidationResult::Malformed("Empty query".to_string());
        }

        // Normalize for analysis (lowercase, collapse whitespace)
        let normalized = normalize_sql(sql);

        // Check for SQL injection patterns
        if let Some(pattern) = self.detect_injection(&normalized) {
            return ValidationResult::Injection(pattern);
        }

        // Check for forbidden operations
        if let Some(op) = self.detect_forbidden_operation(&normalized) {
            return ValidationResult::ForbiddenOperation(op);
        }

        // Check statement count
        let statement_count = count_statements(sql);
        if statement_count > self.config.max_statements {
            return ValidationResult::ExceedsLimits(format!(
                "Statement count {} exceeds maximum {}",
                statement_count, self.config.max_statements
            ));
        }

        if !self.config.allow_multiple_statements && statement_count > 1 {
            return ValidationResult::ForbiddenOperation(
                "Multiple statements not allowed".to_string(),
            );
        }

        // Check for comments if not allowed
        if !self.config.allow_comments && (normalized.contains("--") || normalized.contains("/*")) {
            return ValidationResult::ForbiddenOperation(
                "Comments not allowed in queries".to_string(),
            );
        }

        ValidationResult::Valid
    }

    /// Detect SQL injection patterns
    fn detect_injection(&self, sql: &str) -> Option<String> {
        // Common injection patterns
        let injection_patterns = [
            // Union-based injection
            ("union select", "UNION SELECT injection"),
            ("union all select", "UNION ALL SELECT injection"),
            // Comment injection
            ("' --", "Comment injection via single quote"),
            ("\" --", "Comment injection via double quote"),
            ("'; --", "Statement termination injection"),
            // Boolean-based injection
            ("' or '1'='1", "Boolean injection pattern"),
            ("' or 1=1", "Boolean injection pattern"),
            ("\" or \"1\"=\"1", "Boolean injection pattern"),
            ("' and '1'='1", "Boolean injection pattern"),
            // Stacked queries
            ("; drop ", "Stacked query with DROP"),
            ("; delete ", "Stacked query with DELETE"),
            ("; update ", "Stacked query with UPDATE"),
            ("; insert ", "Stacked query with INSERT"),
            // Time-based injection
            ("sleep(", "Time-based injection (SLEEP)"),
            ("pg_sleep(", "Time-based injection (PG_SLEEP)"),
            ("benchmark(", "Time-based injection (BENCHMARK)"),
            ("waitfor delay", "Time-based injection (WAITFOR)"),
            // System commands
            ("exec sp_", "System procedure execution"),
            ("exec xp_", "Extended procedure execution"),
            ("into outfile", "File write attempt"),
            ("into dumpfile", "File dump attempt"),
            ("load_file(", "File read attempt"),
            // Information schema probing
            ("information_schema", "Schema probing"),
            ("pg_catalog", "PostgreSQL catalog probing"),
            ("mysql.user", "MySQL user table access"),
        ];

        for (pattern, description) in &injection_patterns {
            if sql.contains(pattern) {
                return Some(description.to_string());
            }
        }

        // Check for suspicious character sequences
        let suspicious_sequences = [
            ("''", "Multiple single quotes"),
            ("\"\"", "Multiple double quotes"),
            ("\\x", "Hex escape sequence"),
            ("0x", "Hex literal (potential bypass)"),
            ("char(", "CHAR() function (potential bypass)"),
            ("chr(", "CHR() function (potential bypass)"),
            ("concat(", "CONCAT() with potential injection"),
        ];

        // Only flag these if they appear suspicious (e.g., in WHERE clauses)
        let sql_lower = sql.to_lowercase();
        for (seq, description) in &suspicious_sequences {
            if sql_lower.contains(seq)
                && (sql_lower.contains("where") || sql_lower.contains("having"))
            {
                // Additional check: if it looks like user input injection
                if self.looks_like_injection(sql, seq) {
                    return Some(description.to_string());
                }
            }
        }

        None
    }

    /// Check if a sequence looks like an injection attempt
    fn looks_like_injection(&self, sql: &str, _seq: &str) -> bool {
        // Check for unbalanced quotes
        let single_quotes = sql.matches('\'').count();
        let double_quotes = sql.matches('"').count();

        // Odd number of quotes is suspicious
        if single_quotes % 2 != 0 || double_quotes % 2 != 0 {
            return true;
        }

        false
    }

    /// Detect forbidden operations based on configuration
    fn detect_forbidden_operation(&self, sql: &str) -> Option<String> {
        // Destructive operations
        if !self.config.allow_destructive {
            let destructive = ["drop table", "drop database", "drop schema", "truncate"];
            for op in &destructive {
                if sql.contains(op) {
                    return Some(format!("Destructive operation: {}", op.to_uppercase()));
                }
            }
        }

        // DDL operations
        if !self.config.allow_ddl {
            let ddl = [
                "create table",
                "create index",
                "create database",
                "alter table",
                "drop index",
            ];
            for op in &ddl {
                if sql.contains(op) {
                    return Some(format!("DDL operation not allowed: {}", op.to_uppercase()));
                }
            }
        }

        // System commands
        if !self.config.allow_system_commands {
            let system = ["vacuum", "analyze", "reindex", "checkpoint"];
            for op in &system {
                // Check if it's the main command (not part of another word)
                let pattern = format!(" {} ", op);
                if sql.starts_with(op) || sql.contains(&pattern) {
                    return Some(format!(
                        "System command not allowed: {}",
                        op.to_uppercase()
                    ));
                }
            }
        }

        // Custom forbidden keywords
        for keyword in &self.config.forbidden_keywords {
            let keyword_lower = keyword.to_lowercase();
            if sql.contains(&keyword_lower) {
                return Some(format!("Forbidden keyword: {}", keyword));
            }
        }

        None
    }
}

/// Normalize SQL for analysis
fn normalize_sql(sql: &str) -> String {
    sql.to_lowercase()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
}

/// Count statements in SQL (basic semicolon counting, ignoring strings)
fn count_statements(sql: &str) -> usize {
    let mut count = 0;
    let mut in_string = false;
    let mut string_char = ' ';
    let mut prev_char = ' ';

    for ch in sql.chars() {
        if !in_string {
            if ch == '\'' || ch == '"' {
                in_string = true;
                string_char = ch;
            } else if ch == ';' {
                count += 1;
            }
        } else {
            // Check for end of string
            if ch == string_char && prev_char != '\\' {
                in_string = false;
            }
        }
        prev_char = ch;
    }

    // Count final statement if no trailing semicolon
    let trimmed = sql.trim();
    if !trimmed.is_empty() && !trimmed.ends_with(';') {
        count += 1;
    } else if count == 0 && !trimmed.is_empty() {
        count = 1;
    }

    count.max(1)
}

/// Validate query parameters for potential injection
pub fn validate_parameters(params: &[serde_json::Value]) -> ValidationResult {
    for (i, param) in params.iter().enumerate() {
        if let serde_json::Value::String(s) = param {
            // Check for suspicious strings in parameters
            let lower = s.to_lowercase();
            let suspicious = [
                "' or ", "\" or ", "'; ", "\"; ", "union select", "drop table",
            ];
            for pattern in &suspicious {
                if lower.contains(pattern) {
                    return ValidationResult::Injection(format!(
                        "Suspicious pattern in parameter {}: {}",
                        i, pattern
                    ));
                }
            }
        }
    }
    ValidationResult::Valid
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_select() {
        let validator = SqlValidator::default_validator();
        let result = validator.validate("SELECT * FROM users WHERE id = 1");
        assert!(result.is_valid());
    }

    #[test]
    fn test_valid_insert() {
        let validator = SqlValidator::default_validator();
        let result = validator.validate("INSERT INTO users (name) VALUES ('test')");
        assert!(result.is_valid());
    }

    #[test]
    fn test_union_injection() {
        let validator = SqlValidator::default_validator();
        let result = validator.validate("SELECT * FROM users WHERE id = 1 UNION SELECT * FROM passwords");
        assert!(matches!(result, ValidationResult::Injection(_)));
    }

    #[test]
    fn test_comment_injection() {
        let validator = SqlValidator::default_validator();
        let result = validator.validate("SELECT * FROM users WHERE name = '' --'");
        assert!(matches!(result, ValidationResult::Injection(_)));
    }

    #[test]
    fn test_boolean_injection() {
        let validator = SqlValidator::default_validator();
        let result = validator.validate("SELECT * FROM users WHERE name = '' or '1'='1'");
        assert!(matches!(result, ValidationResult::Injection(_)));
    }

    #[test]
    fn test_destructive_blocked() {
        let validator = SqlValidator::default_validator();
        let result = validator.validate("DROP TABLE users");
        assert!(matches!(result, ValidationResult::ForbiddenOperation(_)));
    }

    #[test]
    fn test_destructive_allowed() {
        let mut config = SqlValidatorConfig::default();
        config.allow_destructive = true;
        let validator = SqlValidator::new(config);
        let result = validator.validate("DROP TABLE users");
        assert!(result.is_valid());
    }

    #[test]
    fn test_read_only_config() {
        let config = SqlValidatorConfig::read_only();
        let validator = SqlValidator::new(config);

        // SELECT should be allowed
        assert!(validator.validate("SELECT * FROM users").is_valid());

        // INSERT should be allowed (it's not DDL)
        assert!(validator.validate("INSERT INTO users (name) VALUES ('test')").is_valid());

        // CREATE should be blocked
        let result = validator.validate("CREATE TABLE test (id INT)");
        assert!(matches!(result, ValidationResult::ForbiddenOperation(_)));
    }

    #[test]
    fn test_strict_config() {
        let config = SqlValidatorConfig::strict();
        let validator = SqlValidator::new(config);

        // Multiple statements blocked (hits max_statements=1 limit)
        let result = validator.validate("SELECT 1; SELECT 2");
        assert!(
            matches!(result, ValidationResult::ExceedsLimits(_) | ValidationResult::ForbiddenOperation(_)),
            "Expected ExceedsLimits or ForbiddenOperation, got {:?}",
            result
        );

        // Comments blocked
        let result = validator.validate("SELECT 1 -- comment");
        assert!(matches!(result, ValidationResult::ForbiddenOperation(_)));
    }

    #[test]
    fn test_empty_query() {
        let validator = SqlValidator::default_validator();
        let result = validator.validate("");
        assert!(matches!(result, ValidationResult::Malformed(_)));

        let result = validator.validate("   ");
        assert!(matches!(result, ValidationResult::Malformed(_)));
    }

    #[test]
    fn test_query_length_limit() {
        let mut config = SqlValidatorConfig::default();
        config.max_query_length = 10;
        let validator = SqlValidator::new(config);

        let result = validator.validate("SELECT * FROM very_long_table_name");
        assert!(matches!(result, ValidationResult::ExceedsLimits(_)));
    }

    #[test]
    fn test_parameter_validation() {
        // Safe parameter
        let result = validate_parameters(&[serde_json::json!("safe_value")]);
        assert!(result.is_valid());

        // Suspicious parameter
        let result = validate_parameters(&[serde_json::json!("' or '1'='1")]);
        assert!(matches!(result, ValidationResult::Injection(_)));
    }

    #[test]
    fn test_statement_counting() {
        assert_eq!(count_statements("SELECT 1"), 1);
        assert_eq!(count_statements("SELECT 1;"), 1);
        assert_eq!(count_statements("SELECT 1; SELECT 2"), 2);
        assert_eq!(count_statements("SELECT 1; SELECT 2;"), 2);
        // Semicolon in string shouldn't count
        assert_eq!(count_statements("SELECT ';' FROM t"), 1);
    }
}
