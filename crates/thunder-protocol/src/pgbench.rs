//! pgbench Compatibility Module
//!
//! Provides compatibility layer for PostgreSQL benchmarking tools like pgbench.
//! Implements:
//! - System catalog queries (pg_catalog.*, information_schema.*)
//! - CREATE DATABASE emulation
//! - COPY command for bulk data loading
//! - Batch INSERT optimization

use thunder_common::prelude::*;

/// System catalog response for pgbench compatibility
#[derive(Debug, Clone)]
pub struct CatalogQueryResult {
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tag: String,
}

impl CatalogQueryResult {
    pub fn empty(tag: &str) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            command_tag: tag.to_string(),
        }
    }

    pub fn single_column(name: &str, values: Vec<String>, tag: &str) -> Self {
        Self {
            columns: vec![(name.to_string(), DataType::String)],
            rows: values.into_iter().map(|v| vec![Some(v)]).collect(),
            command_tag: tag.to_string(),
        }
    }
}

/// pgbench compatibility handler
pub struct PgbenchCompat {
    /// Database name (virtual, always returns success)
    databases: parking_lot::RwLock<Vec<String>>,
    /// Server version to report
    server_version: String,
}

impl Default for PgbenchCompat {
    fn default() -> Self {
        Self::new()
    }
}

impl PgbenchCompat {
    pub fn new() -> Self {
        Self {
            databases: parking_lot::RwLock::new(vec!["postgres".to_string(), "thunder".to_string()]),
            server_version: "15.0".to_string(),
        }
    }

    /// Check if this is a system catalog query that we should handle
    pub fn is_catalog_query(sql: &str) -> bool {
        let upper = sql.to_uppercase();
        upper.contains("PG_CATALOG")
            || upper.contains("PG_CLASS")
            || upper.contains("PG_NAMESPACE")
            || upper.contains("PG_ATTRIBUTE")
            || upper.contains("PG_TYPE")
            || upper.contains("PG_INDEX")
            || upper.contains("PG_DATABASE")
            || upper.contains("PG_SETTINGS")
            || upper.contains("PG_USER")
            || upper.contains("PG_ROLES")
            || upper.contains("PG_STAT_")
            || upper.contains("INFORMATION_SCHEMA")
            || upper.contains("CURRENT_SCHEMA")
            || upper.contains("CURRENT_DATABASE")
            || upper.contains("VERSION()")
            || upper.contains("PG_BACKEND_PID")
            || upper.contains("PG_IS_IN_RECOVERY")
            || upper.starts_with("SET ")
            || upper.starts_with("SHOW ")
            || upper.starts_with("RESET ")
    }

    /// Check if this is a CREATE DATABASE command
    pub fn is_create_database(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("CREATE DATABASE")
    }

    /// Check if this is a DROP DATABASE command
    pub fn is_drop_database(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("DROP DATABASE")
    }

    /// Check if this is a COPY command
    pub fn is_copy_command(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("COPY ")
    }

    /// Check if this is a VACUUM command
    pub fn is_vacuum_command(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("VACUUM")
    }

    /// Check if this is a TRUNCATE command
    pub fn is_truncate_command(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("TRUNCATE")
    }

    /// Handle CREATE DATABASE (virtual - always succeeds)
    pub fn handle_create_database(&self, sql: &str) -> CatalogQueryResult {
        // Extract database name
        let upper = sql.to_uppercase();
        if let Some(name_start) = upper.find("CREATE DATABASE") {
            let rest = &sql[name_start + 15..].trim();
            let name = rest.split_whitespace().next().unwrap_or("newdb");
            let clean_name = name.trim_matches(|c| c == '"' || c == '\'' || c == ';');
            self.databases.write().push(clean_name.to_string());
        }
        CatalogQueryResult::empty("CREATE DATABASE")
    }

    /// Handle DROP DATABASE (virtual - always succeeds)
    pub fn handle_drop_database(&self, sql: &str) -> CatalogQueryResult {
        let upper = sql.to_uppercase();
        if let Some(name_start) = upper.find("DROP DATABASE") {
            let rest = &sql[name_start + 13..].trim();
            // Handle IF EXISTS
            let rest = if rest.to_uppercase().starts_with("IF EXISTS") {
                &rest[9..].trim()
            } else {
                rest
            };
            let name = rest.split_whitespace().next().unwrap_or("");
            let clean_name = name.trim_matches(|c| c == '"' || c == '\'' || c == ';');
            self.databases.write().retain(|db| db != clean_name);
        }
        CatalogQueryResult::empty("DROP DATABASE")
    }

    /// Handle VACUUM command (no-op for now)
    pub fn handle_vacuum(&self, _sql: &str) -> CatalogQueryResult {
        CatalogQueryResult::empty("VACUUM")
    }

    /// Handle TRUNCATE command
    pub fn handle_truncate(&self, sql: &str) -> Option<String> {
        // Extract table name and convert to DELETE
        let upper = sql.to_uppercase();
        if let Some(pos) = upper.find("TRUNCATE") {
            let rest = &sql[pos + 8..].trim();
            // Handle TABLE keyword
            let rest = if rest.to_uppercase().starts_with("TABLE") {
                &rest[5..].trim()
            } else {
                rest
            };
            // Get table name
            let table_name = rest
                .split(|c: char| c.is_whitespace() || c == ';' || c == ',')
                .next()
                .unwrap_or("")
                .trim_matches('"');
            if !table_name.is_empty() {
                return Some(format!("DELETE FROM {}", table_name));
            }
        }
        None
    }

    /// Handle system catalog queries
    pub fn handle_catalog_query(&self, sql: &str) -> Option<CatalogQueryResult> {
        let upper = sql.to_uppercase();

        // Handle SET commands
        if upper.starts_with("SET ") {
            return Some(CatalogQueryResult::empty("SET"));
        }

        // Handle SHOW commands
        if upper.starts_with("SHOW ") {
            return Some(self.handle_show_command(sql));
        }

        // Handle RESET commands
        if upper.starts_with("RESET ") {
            return Some(CatalogQueryResult::empty("RESET"));
        }

        // Handle SELECT version()
        if upper.contains("VERSION()") {
            return Some(CatalogQueryResult {
                columns: vec![("version".to_string(), DataType::String)],
                rows: vec![vec![Some(format!("PostgreSQL {} (ThunderDB)", self.server_version))]],
                command_tag: "SELECT 1".to_string(),
            });
        }

        // Handle current_database()
        if upper.contains("CURRENT_DATABASE()") {
            return Some(CatalogQueryResult {
                columns: vec![("current_database".to_string(), DataType::String)],
                rows: vec![vec![Some("thunder".to_string())]],
                command_tag: "SELECT 1".to_string(),
            });
        }

        // Handle current_schema()
        if upper.contains("CURRENT_SCHEMA") {
            return Some(CatalogQueryResult {
                columns: vec![("current_schema".to_string(), DataType::String)],
                rows: vec![vec![Some("public".to_string())]],
                command_tag: "SELECT 1".to_string(),
            });
        }

        // Handle pg_backend_pid()
        if upper.contains("PG_BACKEND_PID") {
            return Some(CatalogQueryResult {
                columns: vec![("pg_backend_pid".to_string(), DataType::Int32)],
                rows: vec![vec![Some("1".to_string())]],
                command_tag: "SELECT 1".to_string(),
            });
        }

        // Handle pg_is_in_recovery()
        if upper.contains("PG_IS_IN_RECOVERY") {
            return Some(CatalogQueryResult {
                columns: vec![("pg_is_in_recovery".to_string(), DataType::Boolean)],
                rows: vec![vec![Some("f".to_string())]],
                command_tag: "SELECT 1".to_string(),
            });
        }

        // Handle pg_database queries
        if upper.contains("PG_DATABASE") {
            return Some(self.handle_pg_database_query(&upper));
        }

        // Handle pg_class queries (table metadata)
        if upper.contains("PG_CLASS") {
            return Some(self.handle_pg_class_query(&upper));
        }

        // Handle pg_namespace queries
        if upper.contains("PG_NAMESPACE") {
            return Some(self.handle_pg_namespace_query(&upper));
        }

        // Handle pg_type queries
        if upper.contains("PG_TYPE") {
            return Some(self.handle_pg_type_query(&upper));
        }

        // Handle pg_attribute queries
        if upper.contains("PG_ATTRIBUTE") {
            return Some(self.handle_pg_attribute_query(&upper));
        }

        // Handle pg_settings queries
        if upper.contains("PG_SETTINGS") {
            return Some(self.handle_pg_settings_query(&upper));
        }

        // Handle pg_stat queries
        if upper.contains("PG_STAT_") {
            return Some(CatalogQueryResult {
                columns: vec![("stat".to_string(), DataType::String)],
                rows: vec![],
                command_tag: "SELECT 0".to_string(),
            });
        }

        // Handle pg_user / pg_roles
        if upper.contains("PG_USER") || upper.contains("PG_ROLES") {
            return Some(self.handle_pg_roles_query(&upper));
        }

        // Handle information_schema queries
        if upper.contains("INFORMATION_SCHEMA") {
            return Some(self.handle_information_schema_query(&upper));
        }

        None
    }

    fn handle_show_command(&self, sql: &str) -> CatalogQueryResult {
        let upper = sql.to_uppercase();
        let param = upper.trim_start_matches("SHOW ").trim().trim_end_matches(';');

        let (col_name, value) = match param {
            "SERVER_VERSION" | "SERVER_VERSION_NUM" => ("server_version", self.server_version.clone()),
            "SERVER_ENCODING" => ("server_encoding", "UTF8".to_string()),
            "CLIENT_ENCODING" => ("client_encoding", "UTF8".to_string()),
            "LC_COLLATE" => ("lc_collate", "en_US.UTF-8".to_string()),
            "LC_CTYPE" => ("lc_ctype", "en_US.UTF-8".to_string()),
            "IS_SUPERUSER" => ("is_superuser", "on".to_string()),
            "SESSION_AUTHORIZATION" => ("session_authorization", "postgres".to_string()),
            "STANDARD_CONFORMING_STRINGS" => ("standard_conforming_strings", "on".to_string()),
            "TIMEZONE" => ("TimeZone", "UTC".to_string()),
            "DATESTYLE" => ("DateStyle", "ISO, MDY".to_string()),
            "INTEGER_DATETIMES" => ("integer_datetimes", "on".to_string()),
            "MAX_CONNECTIONS" => ("max_connections", "1000".to_string()),
            "TRANSACTION_ISOLATION" => ("transaction_isolation", "read committed".to_string()),
            "SEARCH_PATH" => ("search_path", "public".to_string()),
            _ => (param, "".to_string()),
        };

        CatalogQueryResult {
            columns: vec![(col_name.to_lowercase(), DataType::String)],
            rows: vec![vec![Some(value)]],
            command_tag: "SHOW".to_string(),
        }
    }

    fn handle_pg_database_query(&self, _upper: &str) -> CatalogQueryResult {
        let databases = self.databases.read();
        CatalogQueryResult {
            columns: vec![
                ("oid".to_string(), DataType::Int32),
                ("datname".to_string(), DataType::String),
                ("datdba".to_string(), DataType::Int32),
                ("encoding".to_string(), DataType::Int32),
                ("datcollate".to_string(), DataType::String),
                ("datctype".to_string(), DataType::String),
            ],
            rows: databases.iter().enumerate().map(|(i, name)| {
                vec![
                    Some((i + 1).to_string()),
                    Some(name.clone()),
                    Some("1".to_string()),
                    Some("6".to_string()), // UTF8
                    Some("en_US.UTF-8".to_string()),
                    Some("en_US.UTF-8".to_string()),
                ]
            }).collect(),
            command_tag: format!("SELECT {}", databases.len()),
        }
    }

    fn handle_pg_class_query(&self, _upper: &str) -> CatalogQueryResult {
        // Return empty result for pg_class (tables will be handled separately)
        CatalogQueryResult {
            columns: vec![
                ("oid".to_string(), DataType::Int32),
                ("relname".to_string(), DataType::String),
                ("relnamespace".to_string(), DataType::Int32),
                ("relkind".to_string(), DataType::String),
                ("reltuples".to_string(), DataType::Float64),
            ],
            rows: vec![],
            command_tag: "SELECT 0".to_string(),
        }
    }

    fn handle_pg_namespace_query(&self, _upper: &str) -> CatalogQueryResult {
        CatalogQueryResult {
            columns: vec![
                ("oid".to_string(), DataType::Int32),
                ("nspname".to_string(), DataType::String),
                ("nspowner".to_string(), DataType::Int32),
            ],
            rows: vec![
                vec![Some("2200".to_string()), Some("public".to_string()), Some("1".to_string())],
                vec![Some("11".to_string()), Some("pg_catalog".to_string()), Some("1".to_string())],
                vec![Some("99".to_string()), Some("pg_toast".to_string()), Some("1".to_string())],
            ],
            command_tag: "SELECT 3".to_string(),
        }
    }

    fn handle_pg_type_query(&self, _upper: &str) -> CatalogQueryResult {
        // Common PostgreSQL types that pgbench might query
        CatalogQueryResult {
            columns: vec![
                ("oid".to_string(), DataType::Int32),
                ("typname".to_string(), DataType::String),
                ("typnamespace".to_string(), DataType::Int32),
                ("typlen".to_string(), DataType::Int16),
            ],
            rows: vec![
                vec![Some("16".to_string()), Some("bool".to_string()), Some("11".to_string()), Some("1".to_string())],
                vec![Some("20".to_string()), Some("int8".to_string()), Some("11".to_string()), Some("8".to_string())],
                vec![Some("21".to_string()), Some("int2".to_string()), Some("11".to_string()), Some("2".to_string())],
                vec![Some("23".to_string()), Some("int4".to_string()), Some("11".to_string()), Some("4".to_string())],
                vec![Some("25".to_string()), Some("text".to_string()), Some("11".to_string()), Some("-1".to_string())],
                vec![Some("700".to_string()), Some("float4".to_string()), Some("11".to_string()), Some("4".to_string())],
                vec![Some("701".to_string()), Some("float8".to_string()), Some("11".to_string()), Some("8".to_string())],
                vec![Some("1043".to_string()), Some("varchar".to_string()), Some("11".to_string()), Some("-1".to_string())],
                vec![Some("1114".to_string()), Some("timestamp".to_string()), Some("11".to_string()), Some("8".to_string())],
            ],
            command_tag: "SELECT 9".to_string(),
        }
    }

    fn handle_pg_attribute_query(&self, _upper: &str) -> CatalogQueryResult {
        // Return empty for now - table columns are handled by information_schema
        CatalogQueryResult {
            columns: vec![
                ("attrelid".to_string(), DataType::Int32),
                ("attname".to_string(), DataType::String),
                ("atttypid".to_string(), DataType::Int32),
                ("attnum".to_string(), DataType::Int16),
                ("attnotnull".to_string(), DataType::Boolean),
            ],
            rows: vec![],
            command_tag: "SELECT 0".to_string(),
        }
    }

    fn handle_pg_settings_query(&self, _upper: &str) -> CatalogQueryResult {
        CatalogQueryResult {
            columns: vec![
                ("name".to_string(), DataType::String),
                ("setting".to_string(), DataType::String),
                ("unit".to_string(), DataType::String),
                ("category".to_string(), DataType::String),
            ],
            rows: vec![
                vec![Some("max_connections".to_string()), Some("1000".to_string()), Some("".to_string()), Some("Connections".to_string())],
                vec![Some("shared_buffers".to_string()), Some("128MB".to_string()), Some("8kB".to_string()), Some("Memory".to_string())],
                vec![Some("work_mem".to_string()), Some("4MB".to_string()), Some("kB".to_string()), Some("Memory".to_string())],
            ],
            command_tag: "SELECT 3".to_string(),
        }
    }

    fn handle_pg_roles_query(&self, _upper: &str) -> CatalogQueryResult {
        CatalogQueryResult {
            columns: vec![
                ("oid".to_string(), DataType::Int32),
                ("rolname".to_string(), DataType::String),
                ("rolsuper".to_string(), DataType::Boolean),
                ("rolinherit".to_string(), DataType::Boolean),
                ("rolcreaterole".to_string(), DataType::Boolean),
                ("rolcreatedb".to_string(), DataType::Boolean),
                ("rolcanlogin".to_string(), DataType::Boolean),
            ],
            rows: vec![
                vec![
                    Some("1".to_string()),
                    Some("postgres".to_string()),
                    Some("t".to_string()),
                    Some("t".to_string()),
                    Some("t".to_string()),
                    Some("t".to_string()),
                    Some("t".to_string()),
                ],
            ],
            command_tag: "SELECT 1".to_string(),
        }
    }

    fn handle_information_schema_query(&self, upper: &str) -> CatalogQueryResult {
        if upper.contains("TABLES") {
            return CatalogQueryResult {
                columns: vec![
                    ("table_catalog".to_string(), DataType::String),
                    ("table_schema".to_string(), DataType::String),
                    ("table_name".to_string(), DataType::String),
                    ("table_type".to_string(), DataType::String),
                ],
                rows: vec![],
                command_tag: "SELECT 0".to_string(),
            };
        }

        if upper.contains("COLUMNS") {
            return CatalogQueryResult {
                columns: vec![
                    ("table_name".to_string(), DataType::String),
                    ("column_name".to_string(), DataType::String),
                    ("ordinal_position".to_string(), DataType::Int32),
                    ("data_type".to_string(), DataType::String),
                    ("is_nullable".to_string(), DataType::String),
                ],
                rows: vec![],
                command_tag: "SELECT 0".to_string(),
            };
        }

        CatalogQueryResult {
            columns: vec![],
            rows: vec![],
            command_tag: "SELECT 0".to_string(),
        }
    }
}

/// COPY command parser and handler
#[derive(Debug, Clone)]
pub struct CopyCommand {
    /// Target table name
    pub table_name: String,
    /// Column names (optional)
    pub columns: Option<Vec<String>>,
    /// Direction: true = FROM (import), false = TO (export)
    pub is_from: bool,
    /// Source/destination (STDIN/STDOUT or filename)
    pub source: CopySource,
    /// Format options
    pub format: CopyFormat,
    /// Delimiter character
    pub delimiter: char,
    /// NULL string representation
    pub null_string: String,
    /// Whether first row is header
    pub header: bool,
}

#[derive(Debug, Clone)]
pub enum CopySource {
    Stdin,
    Stdout,
    File(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
}

impl CopyCommand {
    /// Parse a COPY command from SQL
    pub fn parse(sql: &str) -> Option<Self> {
        let upper = sql.to_uppercase();
        if !upper.trim().starts_with("COPY ") {
            return None;
        }

        let sql_clean = sql.trim().trim_end_matches(';');
        let parts: Vec<&str> = sql_clean.split_whitespace().collect();
        if parts.len() < 3 {
            return None;
        }

        // Parse table name (and optional columns)
        let mut idx = 1; // Skip "COPY"
        let table_part = parts.get(idx)?;
        let (table_name, columns) = if table_part.contains('(') {
            // COPY table(col1, col2) FROM ...
            let full: String = parts[idx..].join(" ");
            let paren_start = full.find('(')?;
            let paren_end = full.find(')')?;
            let table = full[..paren_start].trim().to_string();
            let cols_str = &full[paren_start + 1..paren_end];
            let cols: Vec<String> = cols_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            // Find next part after closing paren
            let remaining = &full[paren_end + 1..];
            let _next_parts: Vec<&str> = remaining.split_whitespace().collect();
            idx = parts.len(); // We've consumed all parts in this case
            (table, Some(cols))
        } else {
            let table = table_part.trim_matches('"').to_string();
            idx += 1;
            (table, None)
        };

        // Parse FROM/TO
        let direction = parts.get(idx)?.to_uppercase();
        let is_from = direction == "FROM";
        idx += 1;

        // Parse source
        let source_str = parts.get(idx)?;
        let source = match source_str.to_uppercase().as_str() {
            "STDIN" => CopySource::Stdin,
            "STDOUT" => CopySource::Stdout,
            _ => CopySource::File(source_str.trim_matches('\'').to_string()),
        };

        // Parse options
        let mut format = CopyFormat::Text;
        let mut delimiter = '\t';
        let mut null_string = "\\N".to_string();
        let mut header = false;

        // Look for WITH clause and options
        let options_str = sql_clean.to_uppercase();
        if options_str.contains("FORMAT CSV") || options_str.contains("CSV") {
            format = CopyFormat::Csv;
            delimiter = ',';
        }
        if options_str.contains("FORMAT BINARY") || options_str.contains("BINARY") {
            format = CopyFormat::Binary;
        }
        if options_str.contains("HEADER") {
            header = true;
        }
        if let Some(delim_pos) = options_str.find("DELIMITER") {
            // Extract delimiter character
            let rest = &sql_clean[delim_pos..];
            if let Some(quote_start) = rest.find('\'') {
                if let Some(quote_end) = rest[quote_start + 1..].find('\'') {
                    let delim_char = &rest[quote_start + 1..quote_start + 1 + quote_end];
                    if !delim_char.is_empty() {
                        delimiter = delim_char.chars().next().unwrap_or('\t');
                    }
                }
            }
        }
        if let Some(null_pos) = options_str.find("NULL") {
            let rest = &sql_clean[null_pos..];
            if let Some(quote_start) = rest.find('\'') {
                if let Some(quote_end) = rest[quote_start + 1..].find('\'') {
                    null_string = rest[quote_start + 1..quote_start + 1 + quote_end].to_string();
                }
            }
        }

        Some(CopyCommand {
            table_name,
            columns,
            is_from,
            source,
            format,
            delimiter,
            null_string,
            header,
        })
    }

    /// Parse a line of COPY data into values
    pub fn parse_line(&self, line: &str) -> Vec<Option<String>> {
        if line == "\\." {
            return vec![]; // End of data marker
        }

        match self.format {
            CopyFormat::Csv => self.parse_csv_line(line),
            CopyFormat::Text => self.parse_text_line(line),
            CopyFormat::Binary => vec![], // Not supported
        }
    }

    fn parse_text_line(&self, line: &str) -> Vec<Option<String>> {
        line.split(self.delimiter)
            .map(|field| {
                if field == self.null_string {
                    None
                } else {
                    // Unescape text format escapes
                    Some(field.replace("\\n", "\n").replace("\\t", "\t").replace("\\\\", "\\"))
                }
            })
            .collect()
    }

    fn parse_csv_line(&self, line: &str) -> Vec<Option<String>> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            if in_quotes {
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        // Escaped quote
                        current.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else {
                    current.push(c);
                }
            } else if c == '"' {
                in_quotes = true;
            } else if c == self.delimiter {
                let value = if current.is_empty() || current == self.null_string {
                    None
                } else {
                    Some(current.clone())
                };
                result.push(value);
                current.clear();
            } else {
                current.push(c);
            }
        }

        // Push last field
        let value = if current.is_empty() || current == self.null_string {
            None
        } else {
            Some(current)
        };
        result.push(value);

        result
    }
}

/// Batch INSERT optimizer
#[derive(Debug)]
pub struct BatchInsertBuilder {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Option<String>>>,
    batch_size: usize,
}

impl BatchInsertBuilder {
    pub fn new(table_name: &str, columns: Vec<String>, batch_size: usize) -> Self {
        Self {
            table_name: table_name.to_string(),
            columns,
            values: Vec::new(),
            batch_size,
        }
    }

    /// Add a row to the batch
    pub fn add_row(&mut self, values: Vec<Option<String>>) -> Option<String> {
        self.values.push(values);

        if self.values.len() >= self.batch_size {
            Some(self.build_insert())
        } else {
            None
        }
    }

    /// Build INSERT statement for accumulated rows
    pub fn build_insert(&mut self) -> String {
        if self.values.is_empty() {
            return String::new();
        }

        let columns_str = if self.columns.is_empty() {
            String::new()
        } else {
            format!(" ({})", self.columns.join(", "))
        };

        let values_strs: Vec<String> = self.values.iter().map(|row| {
            let vals: Vec<String> = row.iter().map(|v| {
                match v {
                    Some(s) => {
                        // Try to parse as number
                        if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
                            s.clone()
                        } else {
                            format!("'{}'", s.replace('\'', "''"))
                        }
                    }
                    None => "NULL".to_string(),
                }
            }).collect();
            format!("({})", vals.join(", "))
        }).collect();

        let sql = format!(
            "INSERT INTO {}{} VALUES {}",
            self.table_name,
            columns_str,
            values_strs.join(", ")
        );

        self.values.clear();
        sql
    }

    /// Check if there are pending rows
    pub fn has_pending(&self) -> bool {
        !self.values.is_empty()
    }

    /// Get number of pending rows
    pub fn pending_count(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_query_detection() {
        assert!(PgbenchCompat::is_catalog_query("SELECT * FROM pg_catalog.pg_class"));
        assert!(PgbenchCompat::is_catalog_query("SELECT version()"));
        assert!(PgbenchCompat::is_catalog_query("SET client_encoding = 'UTF8'"));
        assert!(PgbenchCompat::is_catalog_query("SHOW server_version"));
        assert!(!PgbenchCompat::is_catalog_query("SELECT * FROM users"));
    }

    #[test]
    fn test_create_database() {
        assert!(PgbenchCompat::is_create_database("CREATE DATABASE testdb"));
        assert!(PgbenchCompat::is_create_database("create database mydb"));
        assert!(!PgbenchCompat::is_create_database("CREATE TABLE test"));
    }

    #[test]
    fn test_copy_command_parse() {
        let cmd = CopyCommand::parse("COPY pgbench_accounts FROM STDIN").unwrap();
        assert_eq!(cmd.table_name, "pgbench_accounts");
        assert!(cmd.is_from);
        assert!(matches!(cmd.source, CopySource::Stdin));

        let cmd = CopyCommand::parse("COPY test(id, name) FROM STDIN WITH (FORMAT CSV, HEADER)").unwrap();
        assert_eq!(cmd.table_name, "test");
        assert_eq!(cmd.columns, Some(vec!["id".to_string(), "name".to_string()]));
        assert_eq!(cmd.format, CopyFormat::Csv);
        assert!(cmd.header);
    }

    #[test]
    fn test_copy_parse_line() {
        let cmd = CopyCommand {
            table_name: "test".to_string(),
            columns: None,
            is_from: true,
            source: CopySource::Stdin,
            format: CopyFormat::Text,
            delimiter: '\t',
            null_string: "\\N".to_string(),
            header: false,
        };

        let values = cmd.parse_line("1\tAlice\t\\N");
        assert_eq!(values, vec![Some("1".to_string()), Some("Alice".to_string()), None]);
    }

    #[test]
    fn test_batch_insert() {
        let mut builder = BatchInsertBuilder::new("test", vec!["id".to_string(), "name".to_string()], 3);

        assert!(builder.add_row(vec![Some("1".to_string()), Some("Alice".to_string())]).is_none());
        assert!(builder.add_row(vec![Some("2".to_string()), Some("Bob".to_string())]).is_none());

        let sql = builder.add_row(vec![Some("3".to_string()), Some("Charlie".to_string())]).unwrap();
        assert!(sql.contains("INSERT INTO test"));
        assert!(sql.contains("VALUES"));
    }
}
