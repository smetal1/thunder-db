//! ThunderDB Interactive Console
//!
//! A command-line interface for interacting with ThunderDB, similar to psql.
//! Supports SQL query execution, meta-commands, and query history.

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use colored::Colorize;
use comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Cell, ContentArrangement, Table};
use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{ColorMode, Editor};

// ============================================================================
// CLI Arguments
// ============================================================================

/// ThunderDB Interactive Console
#[derive(Parser, Debug)]
#[command(name = "thunder-cli")]
#[command(version, about = "ThunderDB interactive SQL console", long_about = None)]
struct Args {
    /// Database server host
    #[arg(short = 'h', long, default_value = "localhost", env = "THUNDER_HOST")]
    host: String,

    /// Database server port
    #[arg(short = 'p', long, default_value = "8080", env = "THUNDER_PORT")]
    port: u16,

    /// Database name
    #[arg(short = 'd', long, default_value = "thunder", env = "THUNDER_DATABASE")]
    database: String,

    /// Username
    #[arg(short = 'U', long, default_value = "thunder", env = "THUNDER_USER")]
    user: String,

    /// Password (use THUNDER_PASSWORD env var for security)
    #[arg(short = 'W', long, env = "THUNDER_PASSWORD")]
    password: Option<String>,

    /// Execute a single command and exit
    #[arg(short = 'c', long)]
    command: Option<String>,

    /// Execute commands from file
    #[arg(short = 'f', long)]
    file: Option<PathBuf>,

    /// Output format: table, csv, json
    #[arg(short = 'o', long, default_value = "table")]
    output: OutputFormat,

    /// Disable welcome message
    #[arg(long)]
    quiet: bool,

    /// Connection string (overrides other connection options)
    #[arg(short = 'C', long)]
    connection: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

// ============================================================================
// Console State
// ============================================================================

struct ConsoleState {
    host: String,
    port: u16,
    database: String,
    user: String,
    output_format: OutputFormat,
    in_transaction: bool,
    #[allow(dead_code)]
    auto_commit: bool,
    timing: bool,
    running: Arc<AtomicBool>,
}

impl ConsoleState {
    fn new(args: &Args) -> Self {
        Self {
            host: args.host.clone(),
            port: args.port,
            database: args.database.clone(),
            user: args.user.clone(),
            output_format: args.output,
            in_transaction: false,
            auto_commit: true,
            timing: false,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    fn prompt(&self) -> String {
        let txn_indicator = if self.in_transaction { "*" } else { "" };
        format!("{}{}{}> ", self.database.cyan(), txn_indicator.red(), "".normal())
    }
}

// ============================================================================
// HTTP Client for REST API
// ============================================================================

struct HttpClient {
    base_url: String,
    client: reqwest::blocking::Client,
}

impl HttpClient {
    fn new(host: &str, port: u16) -> Self {
        Self {
            base_url: format!("http://{}:{}", host, port),
            client: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    fn execute_query(&self, sql: &str) -> Result<QueryResponse, String> {
        let url = format!("{}/api/v1/query", self.base_url);
        let request = QueryRequest {
            sql: sql.to_string(),
            params: vec![],
            timeout_ms: None,
            transaction_id: None,
        };

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .map_err(|e| format!("Connection error: {}", e))?;

        if response.status().is_success() {
            response.json::<QueryResponse>()
                .map_err(|e| format!("Failed to parse response: {}", e))
        } else {
            let error: ApiError = response.json()
                .unwrap_or_else(|_| ApiError {
                    code: "UNKNOWN".to_string(),
                    message: "Unknown error".to_string(),
                    details: None,
                });
            Err(format!("Error {}: {}", error.code, error.message))
        }
    }

    fn health_check(&self) -> Result<HealthResponse, String> {
        let url = format!("{}/api/v1/health", self.base_url);
        let response = self.client
            .get(&url)
            .send()
            .map_err(|e| format!("Connection error: {}", e))?;

        response.json::<HealthResponse>()
            .map_err(|e| format!("Failed to parse response: {}", e))
    }

    fn get_tables(&self) -> Result<Vec<String>, String> {
        let url = format!("{}/api/v1/tables", self.base_url);
        let response = self.client
            .get(&url)
            .send()
            .map_err(|e| format!("Connection error: {}", e))?;

        let tables: Vec<TableInfo> = response.json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        Ok(tables.into_iter().map(|t| t.name).collect())
    }
}

// ============================================================================
// API Types
// ============================================================================

#[derive(serde::Serialize)]
struct QueryRequest {
    sql: String,
    params: Vec<serde_json::Value>,
    timeout_ms: Option<u64>,
    transaction_id: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct QueryResponse {
    query_id: String,
    columns: Vec<ColumnInfo>,
    rows: Vec<Vec<serde_json::Value>>,
    rows_affected: Option<u64>,
    execution_time_ms: u64,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct ColumnInfo {
    name: String,
    data_type: String,
    nullable: bool,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct ApiError {
    code: String,
    message: String,
    details: Option<serde_json::Value>,
}

#[derive(serde::Deserialize, Debug)]
struct HealthResponse {
    status: String,
    version: String,
    uptime_seconds: u64,
}

#[derive(serde::Deserialize, Debug)]
struct TableInfo {
    name: String,
}

// ============================================================================
// Meta Commands
// ============================================================================

enum MetaCommand {
    Help,
    Quit,
    Clear,
    Status,
    Tables,
    Describe(String),
    Format(OutputFormat),
    Timing(bool),
    History,
    Connect(String, u16),
    Unknown(String),
}

fn parse_meta_command(input: &str) -> Option<MetaCommand> {
    let input = input.trim();
    if !input.starts_with('\\') {
        return None;
    }

    let parts: Vec<&str> = input[1..].split_whitespace().collect();
    if parts.is_empty() {
        return Some(MetaCommand::Unknown(input.to_string()));
    }

    Some(match parts[0] {
        "?" | "help" | "h" => MetaCommand::Help,
        "q" | "quit" | "exit" => MetaCommand::Quit,
        "clear" | "cls" => MetaCommand::Clear,
        "status" | "s" => MetaCommand::Status,
        "dt" | "tables" => MetaCommand::Tables,
        "d" | "describe" => {
            if parts.len() > 1 {
                MetaCommand::Describe(parts[1].to_string())
            } else {
                MetaCommand::Unknown("\\d requires a table name".to_string())
            }
        }
        "format" | "f" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "table" => MetaCommand::Format(OutputFormat::Table),
                    "csv" => MetaCommand::Format(OutputFormat::Csv),
                    "json" => MetaCommand::Format(OutputFormat::Json),
                    _ => MetaCommand::Unknown(format!("Unknown format: {}", parts[1])),
                }
            } else {
                MetaCommand::Unknown("\\format requires: table, csv, or json".to_string())
            }
        }
        "timing" => {
            let enabled = parts.get(1).map(|s| s == &"on").unwrap_or(true);
            MetaCommand::Timing(enabled)
        }
        "history" => MetaCommand::History,
        "connect" | "c" => {
            if parts.len() >= 2 {
                let host = parts[1].to_string();
                let port = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(8080);
                MetaCommand::Connect(host, port)
            } else {
                MetaCommand::Unknown("\\connect requires host [port]".to_string())
            }
        }
        _ => MetaCommand::Unknown(format!("Unknown command: {}", input)),
    })
}

// ============================================================================
// Output Formatting
// ============================================================================

fn format_output(response: &QueryResponse, format: OutputFormat) -> String {
    match format {
        OutputFormat::Table => format_as_table(response),
        OutputFormat::Csv => format_as_csv(response),
        OutputFormat::Json => format_as_json(response),
    }
}

fn format_as_table(response: &QueryResponse) -> String {
    if response.columns.is_empty() {
        if let Some(affected) = response.rows_affected {
            return format!("{}", format!("OK, {} rows affected", affected).green());
        }
        return "OK".green().to_string();
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic);

    // Add header
    let headers: Vec<Cell> = response.columns.iter()
        .map(|c| Cell::new(&c.name).fg(comfy_table::Color::Cyan))
        .collect();
    table.set_header(headers);

    // Add rows
    for row in &response.rows {
        let cells: Vec<Cell> = row.iter()
            .map(|v| Cell::new(format_value(v)))
            .collect();
        table.add_row(cells);
    }

    let row_count = response.rows.len();
    format!("{}\n({} row{})", table, row_count, if row_count != 1 { "s" } else { "" })
}

fn format_as_csv(response: &QueryResponse) -> String {
    let mut output = String::new();

    // Header
    let headers: Vec<&str> = response.columns.iter().map(|c| c.name.as_str()).collect();
    output.push_str(&headers.join(","));
    output.push('\n');

    // Rows
    for row in &response.rows {
        let values: Vec<String> = row.iter().map(|v| escape_csv(format_value(v))).collect();
        output.push_str(&values.join(","));
        output.push('\n');
    }

    output
}

fn format_as_json(response: &QueryResponse) -> String {
    let records: Vec<serde_json::Value> = response.rows.iter()
        .map(|row| {
            let obj: serde_json::Map<String, serde_json::Value> = response.columns.iter()
                .zip(row.iter())
                .map(|(col, val)| (col.name.clone(), val.clone()))
                .collect();
            serde_json::Value::Object(obj)
        })
        .collect();

    serde_json::to_string_pretty(&records).unwrap_or_else(|_| "[]".to_string())
}

fn format_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_value).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(obj) => serde_json::to_string(obj).unwrap_or_default(),
    }
}

fn escape_csv(s: String) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s
    }
}

// ============================================================================
// Command Execution
// ============================================================================

fn execute_command(
    client: &HttpClient,
    sql: &str,
    state: &mut ConsoleState,
) -> Result<(), String> {
    let start = Instant::now();

    match client.execute_query(sql) {
        Ok(response) => {
            let output = format_output(&response, state.output_format);
            println!("{}", output);

            if state.timing {
                let elapsed = start.elapsed();
                println!("{}", format!("Time: {:.3}s (server: {}ms)",
                    elapsed.as_secs_f64(),
                    response.execution_time_ms
                ).dimmed());
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("{}", format!("ERROR: {}", e).red());
            Err(e)
        }
    }
}

fn handle_meta_command(
    cmd: MetaCommand,
    client: &mut HttpClient,
    state: &mut ConsoleState,
    editor: &Editor<(), DefaultHistory>,
) -> bool {
    match cmd {
        MetaCommand::Help => {
            print_help();
            true
        }
        MetaCommand::Quit => false,
        MetaCommand::Clear => {
            print!("\x1B[2J\x1B[1;1H");
            std::io::stdout().flush().ok();
            true
        }
        MetaCommand::Status => {
            match client.health_check() {
                Ok(health) => {
                    println!("{}", "Server Status".cyan().bold());
                    println!("  Status:  {}", health.status.green());
                    println!("  Version: {}", health.version);
                    println!("  Uptime:  {} seconds", health.uptime_seconds);
                    println!("\n{}", "Connection".cyan().bold());
                    println!("  Host:     {}:{}", state.host, state.port);
                    println!("  Database: {}", state.database);
                    println!("  User:     {}", state.user);
                }
                Err(e) => {
                    eprintln!("{}", format!("Failed to get status: {}", e).red());
                }
            }
            true
        }
        MetaCommand::Tables => {
            match client.get_tables() {
                Ok(tables) => {
                    println!("{}", "Tables".cyan().bold());
                    if tables.is_empty() {
                        println!("  (no tables)");
                    } else {
                        for table in tables {
                            println!("  {}", table);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{}", format!("Failed to list tables: {}", e).red());
                }
            }
            true
        }
        MetaCommand::Describe(table) => {
            let sql = format!("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{}'", table);
            let _ = execute_command(client, &sql, state);
            true
        }
        MetaCommand::Format(format) => {
            state.output_format = format;
            println!("Output format set to {:?}", format);
            true
        }
        MetaCommand::Timing(enabled) => {
            state.timing = enabled;
            println!("Timing is {}", if enabled { "on" } else { "off" });
            true
        }
        MetaCommand::History => {
            println!("{}", "Query History".cyan().bold());
            for (i, entry) in editor.history().iter().enumerate() {
                println!("{:4}  {}", i + 1, entry);
            }
            true
        }
        MetaCommand::Connect(host, port) => {
            println!("Connecting to {}:{}...", host, port);
            *client = HttpClient::new(&host, port);
            state.host = host;
            state.port = port;

            match client.health_check() {
                Ok(_) => println!("{}", "Connected!".green()),
                Err(e) => eprintln!("{}", format!("Connection failed: {}", e).red()),
            }
            true
        }
        MetaCommand::Unknown(msg) => {
            eprintln!("{}", format!("Unknown command: {}", msg).yellow());
            true
        }
    }
}

fn print_help() {
    println!("{}", "ThunderDB Console Commands".cyan().bold());
    println!();
    println!("  {}  Show this help message", "\\? or \\help".green());
    println!("  {}       Quit the console", "\\q or \\quit".green());
    println!("  {}        Clear the screen", "\\clear".green());
    println!("  {}       Show server status", "\\status".green());
    println!("  {}   List all tables", "\\dt or \\tables".green());
    println!("  {}  Describe table schema", "\\d <table>".green());
    println!("  {} Set output (table/csv/json)", "\\format <fmt>".green());
    println!("  {} Toggle query timing", "\\timing [on|off]".green());
    println!("  {}      Show query history", "\\history".green());
    println!("  {}  Connect to server", "\\connect <host> [port]".green());
    println!();
    println!("{}", "SQL Examples".cyan().bold());
    println!("  SELECT * FROM users WHERE id = 1;");
    println!("  INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');");
    println!("  CREATE TABLE products (id SERIAL PRIMARY KEY, name VARCHAR(100));");
    println!();
}

fn print_welcome() {
    println!("{}", r#"
  _____ _                     _           ____  ____
 |_   _| |__  _   _ _ __   __| | ___ _ __|  _ \| __ )
   | | | '_ \| | | | '_ \ / _` |/ _ \ '__| | | |  _ \
   | | | | | | |_| | | | | (_| |  __/ |  | |_| | |_) |
   |_| |_| |_|\__,_|_| |_|\__,_|\___|_|  |____/|____/
"#.cyan());
    println!("  {} - Interactive SQL Console", "thunder-cli".bold());
    println!("  Type {} for help, {} to exit", "\\?".green(), "\\q".green());
    println!();
}

// ============================================================================
// Main Entry Point
// ============================================================================

fn main() {
    let args = Args::parse();

    // Parse connection string if provided
    let (host, port) = if let Some(ref conn) = args.connection {
        parse_connection_string(conn).unwrap_or((args.host.clone(), args.port))
    } else {
        (args.host.clone(), args.port)
    };

    let mut state = ConsoleState::new(&args);
    state.host = host.clone();
    state.port = port;

    let mut client = HttpClient::new(&state.host, state.port);

    // Setup Ctrl+C handler
    let running = state.running.clone();
    ctrlc::set_handler(move || {
        running.store(false, Ordering::SeqCst);
        println!("\nInterrupted. Press Ctrl+C again or type \\q to quit.");
    }).expect("Error setting Ctrl+C handler");

    // Handle single command mode
    if let Some(ref command) = args.command {
        if let Err(e) = execute_command(&client, command, &mut state) {
            eprintln!("{}", e);
            std::process::exit(1);
        }
        return;
    }

    // Handle file execution mode
    if let Some(ref file_path) = args.file {
        match std::fs::read_to_string(file_path) {
            Ok(content) => {
                for line in content.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with("--") {
                        continue;
                    }
                    println!("{} {}", "=>".dimmed(), line);
                    if let Err(e) = execute_command(&client, line, &mut state) {
                        eprintln!("{}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("{}", format!("Failed to read file: {}", e).red());
                std::process::exit(1);
            }
        }
        return;
    }

    // Interactive mode
    if !args.quiet {
        print_welcome();
    }

    // Test connection
    match client.health_check() {
        Ok(health) => {
            println!("{}", format!("Connected to ThunderDB {} at {}:{}",
                health.version, state.host, state.port).green());
        }
        Err(e) => {
            eprintln!("{}", format!("Warning: Could not connect to server: {}", e).yellow());
            eprintln!("You can try \\connect <host> <port> to reconnect\n");
        }
    }

    // Setup readline
    let history_path = dirs::home_dir()
        .map(|p| p.join(".thunder_history"))
        .unwrap_or_else(|| PathBuf::from(".thunder_history"));

    let mut editor = Editor::<(), DefaultHistory>::new().expect("Failed to create editor");
    editor.set_color_mode(ColorMode::Enabled);
    let _ = editor.load_history(&history_path);

    let mut sql_buffer = String::new();

    loop {
        if !state.running.load(Ordering::SeqCst) {
            state.running.store(true, Ordering::SeqCst);
            continue;
        }

        let prompt = if sql_buffer.is_empty() {
            state.prompt()
        } else {
            format!("{}-> ", " ".repeat(state.database.len()))
        };

        match editor.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                // Check for meta commands
                if sql_buffer.is_empty() {
                    if let Some(cmd) = parse_meta_command(line) {
                        if !handle_meta_command(cmd, &mut client, &mut state, &editor) {
                            break;
                        }
                        continue;
                    }
                }

                // Build up SQL statement
                sql_buffer.push_str(line);
                sql_buffer.push(' ');

                // Check if statement is complete (ends with semicolon)
                if line.ends_with(';') {
                    let sql = sql_buffer.trim().trim_end_matches(';');
                    let _ = editor.add_history_entry(sql);

                    let _ = execute_command(&client, sql, &mut state);
                    sql_buffer.clear();
                }
            }
            Err(ReadlineError::Interrupted) => {
                sql_buffer.clear();
                println!("^C");
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("{}", format!("Error: {:?}", err).red());
                break;
            }
        }
    }

    // Save history
    let _ = editor.save_history(&history_path);
}

fn parse_connection_string(conn_str: &str) -> Option<(String, u16)> {
    // Support formats: host:port or thunderdb://host:port/db
    if conn_str.contains("://") {
        let url = url::Url::parse(conn_str).ok()?;
        let host = url.host_str()?.to_string();
        let port = url.port().unwrap_or(8080);
        Some((host, port))
    } else if conn_str.contains(':') {
        let parts: Vec<&str> = conn_str.split(':').collect();
        if parts.len() == 2 {
            let host = parts[0].to_string();
            let port = parts[1].parse().ok()?;
            Some((host, port))
        } else {
            None
        }
    } else {
        Some((conn_str.to_string(), 8080))
    }
}
