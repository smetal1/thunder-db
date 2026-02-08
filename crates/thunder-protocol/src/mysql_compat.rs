//! MySQL Compatibility Module
//!
//! Provides compatibility layer for MySQL clients and benchmarking tools.
//! Implements:
//! - System variable queries (@@version, @@max_connections, etc.)
//! - SHOW commands (SHOW DATABASES, SHOW TABLES, etc.)
//! - SET commands
//! - Information schema queries

use std::collections::HashMap;

use thunder_common::prelude::*;

/// MySQL compatibility query result
#[derive(Debug, Clone)]
pub struct MySqlQueryResult {
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<Vec<Option<String>>>,
    pub affected_rows: u64,
}

impl MySqlQueryResult {
    pub fn ok(affected_rows: u64) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            affected_rows,
        }
    }

    pub fn single_value(name: &str, value: &str) -> Self {
        Self {
            columns: vec![(name.to_string(), DataType::String)],
            rows: vec![vec![Some(value.to_string())]],
            affected_rows: 0,
        }
    }
}

/// MySQL compatibility handler
pub struct MySqlCompat {
    /// Server version to report
    #[allow(dead_code)]
    server_version: String,
    /// Session variables
    variables: parking_lot::RwLock<HashMap<String, String>>,
    /// Virtual databases
    databases: parking_lot::RwLock<Vec<String>>,
}

impl Default for MySqlCompat {
    fn default() -> Self {
        Self::new()
    }
}

impl MySqlCompat {
    pub fn new() -> Self {
        let mut variables = HashMap::new();
        // Initialize common session variables
        variables.insert("version".to_string(), "8.0.35-ThunderDB".to_string());
        variables.insert("version_comment".to_string(), "ThunderDB HTAP".to_string());
        variables.insert("max_connections".to_string(), "1000".to_string());
        variables.insert("max_allowed_packet".to_string(), "67108864".to_string());
        variables.insert("sql_mode".to_string(), "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES".to_string());
        variables.insert("autocommit".to_string(), "1".to_string());
        variables.insert("character_set_client".to_string(), "utf8mb4".to_string());
        variables.insert("character_set_connection".to_string(), "utf8mb4".to_string());
        variables.insert("character_set_results".to_string(), "utf8mb4".to_string());
        variables.insert("collation_connection".to_string(), "utf8mb4_unicode_ci".to_string());
        variables.insert("tx_isolation".to_string(), "REPEATABLE-READ".to_string());
        variables.insert("transaction_isolation".to_string(), "REPEATABLE-READ".to_string());
        variables.insert("wait_timeout".to_string(), "28800".to_string());
        variables.insert("interactive_timeout".to_string(), "28800".to_string());
        variables.insert("net_write_timeout".to_string(), "60".to_string());
        variables.insert("net_read_timeout".to_string(), "30".to_string());
        variables.insert("lower_case_table_names".to_string(), "0".to_string());
        variables.insert("time_zone".to_string(), "SYSTEM".to_string());
        variables.insert("system_time_zone".to_string(), "UTC".to_string());
        variables.insert("init_connect".to_string(), "".to_string());
        variables.insert("license".to_string(), "Apache-2.0".to_string());

        Self {
            server_version: "8.0.35-ThunderDB".to_string(),
            variables: parking_lot::RwLock::new(variables),
            databases: parking_lot::RwLock::new(vec![
                "information_schema".to_string(),
                "mysql".to_string(),
                "performance_schema".to_string(),
                "thunder".to_string(),
            ]),
        }
    }

    /// Check if this is a system query that we should handle
    pub fn is_system_query(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("SELECT @@")
            || upper.starts_with("SHOW ")
            || upper.starts_with("SET ")
            || upper.contains("INFORMATION_SCHEMA")
            || upper.contains("MYSQL.")
            || upper.contains("PERFORMANCE_SCHEMA")
    }

    /// Handle a system query and return the result
    pub fn handle_query(&self, sql: &str) -> Option<MySqlQueryResult> {
        let upper = sql.trim().to_uppercase();

        // Handle SET commands
        if upper.starts_with("SET ") {
            return Some(self.handle_set_command(sql));
        }

        // Handle SELECT @@variable
        if upper.starts_with("SELECT @@") {
            return Some(self.handle_select_variable(sql));
        }

        // Handle SHOW commands
        if upper.starts_with("SHOW ") {
            return Some(self.handle_show_command(sql));
        }

        // Handle information_schema queries
        if upper.contains("INFORMATION_SCHEMA") {
            return Some(self.handle_information_schema(sql));
        }

        None
    }

    fn handle_set_command(&self, sql: &str) -> MySqlQueryResult {
        // Parse SET variable = value
        let sql_clean = sql.trim().trim_end_matches(';');
        let parts: Vec<&str> = sql_clean[4..].splitn(2, '=').collect();

        if parts.len() == 2 {
            let var_name = parts[0].trim().trim_start_matches('@').trim_start_matches('@').to_lowercase();
            let var_value = parts[1].trim().trim_matches('\'').trim_matches('"').to_string();
            self.variables.write().insert(var_name, var_value);
        }

        MySqlQueryResult::ok(0)
    }

    fn handle_select_variable(&self, sql: &str) -> MySqlQueryResult {
        // Parse SELECT @@variable
        let upper = sql.to_uppercase();

        // Extract variable names
        let vars: Vec<&str> = sql[upper.find("@@").unwrap_or(0)..]
            .split(',')
            .filter(|s| s.contains("@@"))
            .collect();

        if vars.is_empty() {
            return MySqlQueryResult::ok(0);
        }

        // Handle single variable
        if vars.len() == 1 {
            let var_part = vars[0];
            let var_name = var_part
                .trim()
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_start_matches("@@")
                .trim_start_matches("session.")
                .trim_start_matches("global.")
                .to_lowercase();

            let variables = self.variables.read();
            let value = variables.get(&var_name).cloned().unwrap_or_default();

            // Determine column name (with alias if present)
            let col_name = if var_part.to_uppercase().contains(" AS ") {
                var_part
                    .to_uppercase()
                    .split(" AS ")
                    .nth(1)
                    .unwrap_or(&var_name)
                    .trim()
                    .to_string()
            } else {
                format!("@@{}", var_name)
            };

            return MySqlQueryResult {
                columns: vec![(col_name, DataType::String)],
                rows: vec![vec![Some(value)]],
                affected_rows: 0,
            };
        }

        // Multiple variables
        let mut columns = Vec::new();
        let mut values = Vec::new();
        let variables = self.variables.read();

        for var_part in &vars {
            let var_name = var_part
                .trim()
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_start_matches("@@")
                .trim_start_matches("session.")
                .trim_start_matches("global.")
                .to_lowercase();

            let col_name = format!("@@{}", var_name);
            let value = variables.get(&var_name).cloned().unwrap_or_default();

            columns.push((col_name, DataType::String));
            values.push(Some(value));
        }

        MySqlQueryResult {
            columns,
            rows: vec![values],
            affected_rows: 0,
        }
    }

    fn handle_show_command(&self, sql: &str) -> MySqlQueryResult {
        let upper = sql.trim().to_uppercase();

        if upper.starts_with("SHOW DATABASES") || upper.starts_with("SHOW SCHEMAS") {
            return self.show_databases();
        }

        if upper.starts_with("SHOW TABLES") {
            return self.show_tables();
        }

        if upper.starts_with("SHOW VARIABLES") {
            return self.show_variables(&upper);
        }

        if upper.starts_with("SHOW STATUS") {
            return self.show_status();
        }

        if upper.starts_with("SHOW PROCESSLIST") || upper.starts_with("SHOW FULL PROCESSLIST") {
            return self.show_processlist();
        }

        if upper.starts_with("SHOW CREATE TABLE") {
            return self.show_create_table(&upper);
        }

        if upper.starts_with("SHOW COLUMNS") || upper.starts_with("SHOW FIELDS") {
            return self.show_columns(&upper);
        }

        if upper.starts_with("SHOW INDEX") || upper.starts_with("SHOW INDEXES") || upper.starts_with("SHOW KEYS") {
            return MySqlQueryResult {
                columns: vec![
                    ("Table".to_string(), DataType::String),
                    ("Key_name".to_string(), DataType::String),
                    ("Column_name".to_string(), DataType::String),
                ],
                rows: vec![],
                affected_rows: 0,
            };
        }

        if upper.starts_with("SHOW GRANTS") {
            return MySqlQueryResult::single_value(
                "Grants for root@localhost",
                "GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION",
            );
        }

        if upper.starts_with("SHOW WARNINGS") || upper.starts_with("SHOW ERRORS") {
            return MySqlQueryResult {
                columns: vec![
                    ("Level".to_string(), DataType::String),
                    ("Code".to_string(), DataType::Int32),
                    ("Message".to_string(), DataType::String),
                ],
                rows: vec![],
                affected_rows: 0,
            };
        }

        if upper.starts_with("SHOW ENGINES") {
            return MySqlQueryResult {
                columns: vec![
                    ("Engine".to_string(), DataType::String),
                    ("Support".to_string(), DataType::String),
                    ("Comment".to_string(), DataType::String),
                ],
                rows: vec![
                    vec![Some("ThunderDB".to_string()), Some("DEFAULT".to_string()), Some("ThunderDB HTAP Engine".to_string())],
                ],
                affected_rows: 0,
            };
        }

        if upper.starts_with("SHOW COLLATION") {
            return MySqlQueryResult {
                columns: vec![
                    ("Collation".to_string(), DataType::String),
                    ("Charset".to_string(), DataType::String),
                    ("Id".to_string(), DataType::Int32),
                    ("Default".to_string(), DataType::String),
                ],
                rows: vec![
                    vec![Some("utf8mb4_unicode_ci".to_string()), Some("utf8mb4".to_string()), Some("224".to_string()), Some("Yes".to_string())],
                ],
                affected_rows: 0,
            };
        }

        // Default: return empty result
        MySqlQueryResult::ok(0)
    }

    fn show_databases(&self) -> MySqlQueryResult {
        let databases = self.databases.read();
        MySqlQueryResult {
            columns: vec![("Database".to_string(), DataType::String)],
            rows: databases.iter().map(|db| vec![Some(db.clone())]).collect(),
            affected_rows: 0,
        }
    }

    fn show_tables(&self) -> MySqlQueryResult {
        MySqlQueryResult {
            columns: vec![("Tables".to_string(), DataType::String)],
            rows: vec![],
            affected_rows: 0,
        }
    }

    fn show_variables(&self, upper: &str) -> MySqlQueryResult {
        let variables = self.variables.read();

        // Check for LIKE clause
        let like_pattern = if upper.contains("LIKE") {
            upper
                .split("LIKE")
                .nth(1)
                .map(|s| s.trim().trim_matches('\'').trim_matches('"').replace('%', ""))
        } else {
            None
        };

        let rows: Vec<Vec<Option<String>>> = variables
            .iter()
            .filter(|(k, _)| {
                if let Some(ref pattern) = like_pattern {
                    k.contains(&pattern.to_lowercase())
                } else {
                    true
                }
            })
            .map(|(k, v)| vec![Some(k.clone()), Some(v.clone())])
            .collect();

        MySqlQueryResult {
            columns: vec![
                ("Variable_name".to_string(), DataType::String),
                ("Value".to_string(), DataType::String),
            ],
            rows,
            affected_rows: 0,
        }
    }

    fn show_status(&self) -> MySqlQueryResult {
        MySqlQueryResult {
            columns: vec![
                ("Variable_name".to_string(), DataType::String),
                ("Value".to_string(), DataType::String),
            ],
            rows: vec![
                vec![Some("Uptime".to_string()), Some("100".to_string())],
                vec![Some("Threads_connected".to_string()), Some("1".to_string())],
                vec![Some("Queries".to_string()), Some("0".to_string())],
                vec![Some("Slow_queries".to_string()), Some("0".to_string())],
                vec![Some("Open_tables".to_string()), Some("0".to_string())],
            ],
            affected_rows: 0,
        }
    }

    fn show_processlist(&self) -> MySqlQueryResult {
        MySqlQueryResult {
            columns: vec![
                ("Id".to_string(), DataType::Int64),
                ("User".to_string(), DataType::String),
                ("Host".to_string(), DataType::String),
                ("db".to_string(), DataType::String),
                ("Command".to_string(), DataType::String),
                ("Time".to_string(), DataType::Int32),
                ("State".to_string(), DataType::String),
                ("Info".to_string(), DataType::String),
            ],
            rows: vec![
                vec![
                    Some("1".to_string()),
                    Some("root".to_string()),
                    Some("localhost".to_string()),
                    Some("thunder".to_string()),
                    Some("Query".to_string()),
                    Some("0".to_string()),
                    Some("executing".to_string()),
                    Some("SHOW PROCESSLIST".to_string()),
                ],
            ],
            affected_rows: 0,
        }
    }

    fn show_create_table(&self, _upper: &str) -> MySqlQueryResult {
        MySqlQueryResult {
            columns: vec![
                ("Table".to_string(), DataType::String),
                ("Create Table".to_string(), DataType::String),
            ],
            rows: vec![],
            affected_rows: 0,
        }
    }

    fn show_columns(&self, _upper: &str) -> MySqlQueryResult {
        MySqlQueryResult {
            columns: vec![
                ("Field".to_string(), DataType::String),
                ("Type".to_string(), DataType::String),
                ("Null".to_string(), DataType::String),
                ("Key".to_string(), DataType::String),
                ("Default".to_string(), DataType::String),
                ("Extra".to_string(), DataType::String),
            ],
            rows: vec![],
            affected_rows: 0,
        }
    }

    fn handle_information_schema(&self, sql: &str) -> MySqlQueryResult {
        let upper = sql.to_uppercase();

        if upper.contains("SCHEMATA") {
            let databases = self.databases.read();
            return MySqlQueryResult {
                columns: vec![
                    ("CATALOG_NAME".to_string(), DataType::String),
                    ("SCHEMA_NAME".to_string(), DataType::String),
                    ("DEFAULT_CHARACTER_SET_NAME".to_string(), DataType::String),
                    ("DEFAULT_COLLATION_NAME".to_string(), DataType::String),
                ],
                rows: databases.iter().map(|db| {
                    vec![
                        Some("def".to_string()),
                        Some(db.clone()),
                        Some("utf8mb4".to_string()),
                        Some("utf8mb4_unicode_ci".to_string()),
                    ]
                }).collect(),
                affected_rows: 0,
            };
        }

        if upper.contains("TABLES") {
            return MySqlQueryResult {
                columns: vec![
                    ("TABLE_CATALOG".to_string(), DataType::String),
                    ("TABLE_SCHEMA".to_string(), DataType::String),
                    ("TABLE_NAME".to_string(), DataType::String),
                    ("TABLE_TYPE".to_string(), DataType::String),
                ],
                rows: vec![],
                affected_rows: 0,
            };
        }

        if upper.contains("COLUMNS") {
            return MySqlQueryResult {
                columns: vec![
                    ("TABLE_SCHEMA".to_string(), DataType::String),
                    ("TABLE_NAME".to_string(), DataType::String),
                    ("COLUMN_NAME".to_string(), DataType::String),
                    ("DATA_TYPE".to_string(), DataType::String),
                ],
                rows: vec![],
                affected_rows: 0,
            };
        }

        if upper.contains("PROCESSLIST") {
            return self.show_processlist();
        }

        MySqlQueryResult::ok(0)
    }

    /// Set a variable
    pub fn set_variable(&self, name: &str, value: &str) {
        self.variables.write().insert(name.to_lowercase(), value.to_string());
    }

    /// Get a variable
    pub fn get_variable(&self, name: &str) -> Option<String> {
        self.variables.read().get(&name.to_lowercase()).cloned()
    }

    /// Add a database
    pub fn add_database(&self, name: &str) {
        let mut dbs = self.databases.write();
        if !dbs.contains(&name.to_string()) {
            dbs.push(name.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_system_query() {
        assert!(MySqlCompat::is_system_query("SELECT @@version"));
        assert!(MySqlCompat::is_system_query("SHOW DATABASES"));
        assert!(MySqlCompat::is_system_query("SET autocommit=1"));
        assert!(!MySqlCompat::is_system_query("SELECT * FROM users"));
    }

    #[test]
    fn test_select_variable() {
        let compat = MySqlCompat::new();
        let result = compat.handle_query("SELECT @@version").unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0][0].as_ref().unwrap().contains("ThunderDB"));
    }

    #[test]
    fn test_show_databases() {
        let compat = MySqlCompat::new();
        let result = compat.handle_query("SHOW DATABASES").unwrap();
        assert_eq!(result.columns.len(), 1);
        assert!(result.rows.len() >= 4); // At least the default databases
    }

    #[test]
    fn test_set_command() {
        let compat = MySqlCompat::new();
        let result = compat.handle_query("SET autocommit = 0").unwrap();
        assert_eq!(result.affected_rows, 0);
        assert_eq!(compat.get_variable("autocommit"), Some("0".to_string()));
    }
}
