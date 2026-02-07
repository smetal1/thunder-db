# ThunderDB Protocol Compatibility

This document describes the protocol compatibility features implemented for PostgreSQL and MySQL protocols.

## PostgreSQL Protocol Compatibility

### Supported Features

#### pgbench Compatibility
ThunderDB supports standard PostgreSQL benchmarking tools like `pgbench` through the following features:

- **CREATE DATABASE**: Emulated for virtual database creation (always succeeds)
- **DROP DATABASE**: Emulated for virtual database removal
- **VACUUM**: No-op implementation (returns success)
- **TRUNCATE**: Converted to DELETE for compatibility
- **COPY FROM STDIN**: Full support for bulk data loading with CSV and TEXT formats

#### System Catalog Queries
ThunderDB emulates PostgreSQL system catalogs for tool compatibility:

| Catalog | Support Level | Notes |
|---------|--------------|-------|
| `pg_database` | Full | Lists virtual databases |
| `pg_class` | Partial | Returns empty for table queries |
| `pg_namespace` | Full | Returns standard schemas |
| `pg_type` | Full | Common PostgreSQL types |
| `pg_attribute` | Partial | Returns empty |
| `pg_settings` | Partial | Common settings |
| `pg_roles` / `pg_user` | Full | Returns root user |
| `information_schema.tables` | Full | Lists user tables |
| `information_schema.columns` | Full | Lists table columns |

#### System Functions
- `version()`: Returns PostgreSQL-compatible version string
- `current_database()`: Returns current database name
- `current_schema()`: Returns "public"
- `pg_backend_pid()`: Returns process ID
- `pg_is_in_recovery()`: Returns false

#### SET/SHOW/RESET Commands
All standard PostgreSQL session commands are accepted and return appropriate values.

### COPY Command Support

The COPY command is fully supported for bulk data loading:

```sql
COPY table_name FROM STDIN [WITH (FORMAT csv|text, DELIMITER 'char', NULL 'string', HEADER)]
```

Features:
- **Text format**: Tab-delimited by default
- **CSV format**: Comma-delimited with quote handling
- **Batch processing**: Rows are batched (100 rows default) for efficient insertion
- **NULL handling**: Configurable NULL string representation

### Limitations

1. **COPY TO**: Export not yet supported
2. **COPY FROM FILE**: Only STDIN is supported
3. **Binary format**: Not supported for COPY
4. **Cursors**: Basic cursor support only

## MySQL Protocol Compatibility

### Supported Features

#### Authentication
- **mysql_native_password**: Primary authentication method
- **caching_sha2_password**: Supported
- **SSL/TLS**: Supported with certificate hot-reload

#### System Variable Queries
ThunderDB handles `SELECT @@variable` queries for all common MySQL variables:

- `@@version`, `@@version_comment`
- `@@max_connections`, `@@max_allowed_packet`
- `@@character_set_*`, `@@collation_connection`
- `@@autocommit`, `@@sql_mode`
- `@@tx_isolation`, `@@transaction_isolation`
- And many more...

#### SHOW Commands

| Command | Support |
|---------|---------|
| `SHOW DATABASES` | Full |
| `SHOW TABLES` | Full |
| `SHOW VARIABLES [LIKE 'pattern']` | Full |
| `SHOW STATUS` | Basic |
| `SHOW PROCESSLIST` | Basic |
| `SHOW CREATE TABLE` | Partial |
| `SHOW COLUMNS` / `SHOW FIELDS` | Partial |
| `SHOW INDEX` / `SHOW KEYS` | Partial |
| `SHOW GRANTS` | Full |
| `SHOW WARNINGS` / `SHOW ERRORS` | Empty result |
| `SHOW ENGINES` | Full |
| `SHOW COLLATION` | Basic |

#### SET Commands
All SET commands are accepted and session variables are tracked.

### Information Schema
Queries against `information_schema` are handled:

- `SCHEMATA`: Lists databases
- `TABLES`: Lists tables
- `COLUMNS`: Lists columns
- `PROCESSLIST`: Shows active processes

### Prepared Statements
Full support for MySQL prepared statements:
- `COM_STMT_PREPARE`
- `COM_STMT_EXECUTE`
- `COM_STMT_CLOSE`

## Connection Pooling

### Server-Side Pool (`ServerConnectionPool`)

Features:
- Pre-allocated session pool with configurable min/max sizes
- Session reuse for reduced overhead
- Automatic health checks and cleanup
- Idle timeout and max lifetime settings
- Request multiplexing for concurrent handling

Configuration:
```rust
PoolConfig {
    min_size: 10,
    max_size: 100,
    acquire_timeout_ms: 30_000,
    idle_timeout_ms: 300_000,
    max_lifetime_ms: 3600_000,
    health_check_interval_ms: 30_000,
    max_requests_per_connection: 10_000,
}
```

### Concurrent Load Handling

- **Semaphore-based limiting**: Prevents connection overload
- **Background health checks**: Maintains pool health
- **Session cleanup**: Automatic cleanup of idle/expired sessions
- **Peak concurrent tracking**: Statistics for monitoring

## Batch Operations

### Batch INSERT Builder

For bulk data ingestion, ThunderDB provides an optimized batch INSERT builder:

```rust
let mut builder = BatchInsertBuilder::new("table_name", columns, batch_size);

// Add rows individually
if let Some(sql) = builder.add_row(values) {
    // Execute batched INSERT when batch_size is reached
    executor.execute(sql);
}

// Flush remaining rows
if builder.has_pending() {
    executor.execute(builder.build_insert());
}
```

Features:
- Configurable batch size
- Automatic SQL generation
- NULL value handling
- Quote escaping

## Performance Considerations

1. **System Catalog Queries**: Handled in-memory without database access
2. **COPY Data**: Batched for efficient insertion
3. **Connection Pooling**: Reduces connection overhead
4. **Session Variables**: Cached per-connection

## Testing with Standard Tools

### pgbench

```bash
# Initialize (creates tables)
pgbench -i -s 10 -h localhost -p 5432 -U postgres thunder

# Run benchmark
pgbench -c 10 -j 4 -T 60 -h localhost -p 5432 -U postgres thunder
```

### redis-benchmark (via RESP protocol)

```bash
redis-benchmark -h localhost -p 6379 -t set,get -n 100000
```

### MySQL Connector/J

Compatible with standard MySQL connectors that use:
- mysql_native_password authentication
- Protocol version 10
- Standard capability flags
