# ThunderDB Operations Runbook

## Table of Contents

1. [Startup Procedure](#startup-procedure)
2. [Graceful Shutdown](#graceful-shutdown)
3. [Backup and Restore](#backup-and-restore)
4. [Point-in-Time Recovery (PITR)](#point-in-time-recovery-pitr)
5. [Scaling](#scaling)
6. [Monitoring and Alerting](#monitoring-and-alerting)
7. [Troubleshooting Common Issues](#troubleshooting-common-issues)
8. [Configuration Reference for Production Tuning](#configuration-reference-for-production-tuning)
9. [Emergency Procedures](#emergency-procedures)

---

## Startup Procedure

### Bare-Metal / VM

```bash
# Start with a configuration file
thunderdb --config /etc/thunderdb/thunderdb.toml

# Start with overrides for data directory and listen address
thunderdb --config /etc/thunderdb/thunderdb.toml \
  --data-dir /mnt/nvme/thunderdb/data \
  --listen 0.0.0.0

# Start with verbose (debug-level) logging for troubleshooting
thunderdb --config /etc/thunderdb/thunderdb.toml --verbose
```

Environment variable overrides take the highest priority. Commonly used:

```bash
export THUNDERDB_DATA_DIR=/mnt/nvme/thunderdb/data
export THUNDERDB_WAL_DIR=/mnt/nvme/thunderdb/wal
export THUNDERDB_LOG_LEVEL=info
export THUNDERDB_SUPERUSER_PASSWORD_HASH="<bcrypt hash>"
thunderdb --config /etc/thunderdb/thunderdb.toml
```

### Kubernetes

```bash
# Deploy the full stack
kubectl apply -k deploy/k8s/

# Verify the StatefulSet rolls out
kubectl -n thunderdb rollout status statefulset/thunderdb

# Confirm the pod is ready (readiness probe passes)
kubectl -n thunderdb get pods -l app.kubernetes.io/name=thunderdb

# Check startup logs
kubectl -n thunderdb logs statefulset/thunderdb -f
```

### Pre-Flight Checks

Before starting a production node, verify:

1. Data and WAL volumes are mounted and writable.
2. The superuser password hash is set (via `THUNDERDB_SUPERUSER_PASSWORD_HASH` env var or `security.superuser_password_hash` in config).
3. Sufficient disk space exists (warn threshold defaults to 10% free, critical to 5%).
4. Port availability for all protocols (pg: 5432, mysql: 3306, resp: 6379, http: 8080, grpc: 9090).

Generate a password hash:

```bash
thunderdb --hash-password "your-secure-password"
# Output: $2b$12$...   (bcrypt hash)
```

### Verifying Startup

```bash
# Liveness check (basic "I'm alive")
curl -s http://localhost:8080/admin/live | jq .

# Readiness check (database engine initialized, ready for queries)
curl -s http://localhost:8080/admin/ready | jq .

# Full health check (database, storage, disk I/O)
curl -s http://localhost:8080/admin/health | jq .
```

---

## Graceful Shutdown

ThunderDB handles SIGTERM and SIGINT for graceful shutdown. The shutdown sequence:

1. Stop accepting new protocol connections (PostgreSQL, MySQL, RESP).
2. Wait 2 seconds for in-flight requests to complete.
3. Shutdown background workers (checkpoint, vacuum, disk monitor, backup).
4. Final WAL checkpoint is written by the checkpoint worker.
5. Database engine shuts down (flushes buffer pool, closes files).

### Bare-Metal / VM

```bash
# Send SIGTERM (preferred)
kill -TERM $(pidof thunderdb)

# Or press Ctrl+C if running in foreground (sends SIGINT)
```

### Kubernetes

```bash
# Scale down gracefully (respects terminationGracePeriodSeconds: 30)
kubectl -n thunderdb scale statefulset thunderdb --replicas=0

# Or delete with a grace period
kubectl -n thunderdb delete pod thunderdb-0 --grace-period=30
```

The StatefulSet has `terminationGracePeriodSeconds: 30` configured, which gives the server enough time to flush the buffer pool and write a final checkpoint.

### Verifying Clean Shutdown

Check the logs for these messages:

```
INFO ThunderDB server stopped
```

If the server is killed without a clean shutdown (SIGKILL, OOM, power loss), it will replay the WAL on the next startup to recover to a consistent state. This is safe but takes time proportional to the amount of unflushed WAL.

---

## Backup and Restore

### Automatic Backups

Enable automatic backups in the configuration:

```toml
[storage]
backup_enabled = true
backup_interval = "24h"       # Every 24 hours
backup_retention_count = 7    # Keep last 7 backups
backup_dir = "/mnt/backups/thunderdb"  # Optional; defaults to data_dir/backups
```

The backup worker runs inside the server and prunes old backups beyond the retention count.

### Manual Backup via API

```bash
# Trigger an ad-hoc backup
curl -X POST http://localhost:8080/admin/backup \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <admin-token>" \
  -d '{"target_dir": "/mnt/backups/thunderdb/manual"}'

# List available backups
curl -s http://localhost:8080/admin/backups \
  -H "Authorization: Bearer <admin-token>" | jq .
```

### Manual Backup via Kubernetes

```bash
# Trigger backup from inside the pod
kubectl -n thunderdb exec thunderdb-0 -- \
  curl -s -X POST http://localhost:8080/admin/backup \
    -H "Content-Type: application/json" \
    -d '{"target_dir": "/var/lib/thunderdb/data/backups/manual"}'

# Copy backup to local machine
kubectl -n thunderdb cp thunderdb-0:/var/lib/thunderdb/data/backups ./local-backups/

# Or use a CronJob to automate external backup copies
```

### Restore from Backup

Restore requires the server to be stopped. It cannot be done through the REST API while the server is running.

```bash
# 1. Stop the server
kill -TERM $(pidof thunderdb)

# 2. Move/remove existing data (keep as a safety net)
mv /var/lib/thunderdb/data /var/lib/thunderdb/data.old
mv /var/lib/thunderdb/wal /var/lib/thunderdb/wal.old

# 3. Copy backup data into place
cp -a /mnt/backups/thunderdb/2025-01-15T00-00-00Z/data /var/lib/thunderdb/data
cp -a /mnt/backups/thunderdb/2025-01-15T00-00-00Z/wal /var/lib/thunderdb/wal

# 4. Start the server (it will replay any WAL from the backup)
thunderdb --config /etc/thunderdb/thunderdb.toml

# 5. Verify health
curl -s http://localhost:8080/admin/health | jq .

# 6. Remove old data once verified
rm -rf /var/lib/thunderdb/data.old /var/lib/thunderdb/wal.old
```

---

## Point-in-Time Recovery (PITR)

PITR restores the database to a specific timestamp by restoring a base backup and then replaying WAL entries up to the target time.

### Prerequisites

- A base backup taken before the target recovery time.
- WAL files from the backup time through the target recovery time must be available.

### Procedure

```bash
# 1. Stop the server
kill -TERM $(pidof thunderdb)

# 2. Run PITR restore (uses RFC 3339 timestamp format)
thunderdb --config /etc/thunderdb/thunderdb.toml \
  --restore-pitr "2025-01-15T12:30:00Z" \
  --restore-backup /mnt/backups/thunderdb/2025-01-15T00-00-00Z

# Output:
# Restoring from backup "/mnt/backups/thunderdb/2025-01-15T00-00-00Z" to timestamp 2025-01-15T12:30:00Z (1736942200000 ms)...
# PITR restore complete. You can now start the server normally.

# 3. Start the server normally
thunderdb --config /etc/thunderdb/thunderdb.toml

# 4. Verify health and data integrity
curl -s http://localhost:8080/admin/health | jq .
```

The `--restore-pitr` flag requires `--restore-backup` to specify the backup directory. The server restores the backup, replays WAL entries up to the specified timestamp, then exits. You must start the server separately afterward.

### PITR in Kubernetes

```bash
# 1. Scale down
kubectl -n thunderdb scale statefulset thunderdb --replicas=0

# 2. Run a one-off restore job
kubectl -n thunderdb run thunderdb-pitr --rm -it \
  --image=thunderdb/thunderdb:latest \
  --overrides='{
    "spec": {
      "containers": [{
        "name": "thunderdb-pitr",
        "image": "thunderdb/thunderdb:latest",
        "command": ["thunderdb"],
        "args": [
          "--config", "/etc/thunderdb/thunderdb.toml",
          "--restore-pitr", "2025-01-15T12:30:00Z",
          "--restore-backup", "/var/lib/thunderdb/data/backups/2025-01-15T00-00-00Z"
        ],
        "volumeMounts": [
          {"name": "config", "mountPath": "/etc/thunderdb", "readOnly": true},
          {"name": "data", "mountPath": "/var/lib/thunderdb/data"},
          {"name": "wal", "mountPath": "/var/lib/thunderdb/wal"}
        ]
      }],
      "volumes": [
        {"name": "config", "configMap": {"name": "thunderdb-config"}},
        {"name": "data", "persistentVolumeClaim": {"claimName": "data-thunderdb-0"}},
        {"name": "wal", "persistentVolumeClaim": {"claimName": "wal-thunderdb-0"}}
      ],
      "restartPolicy": "Never"
    }
  }'

# 3. Scale back up
kubectl -n thunderdb scale statefulset thunderdb --replicas=1
```

---

## Scaling

### Vertical Scaling

#### Bare-Metal / VM

Update the configuration file and restart the server:

```toml
[storage]
buffer_pool_size = 4294967296    # 4 GB (was 1 GB)
wal_buffer_size = 134217728      # 128 MB (was 64 MB)
compaction_threads = 8            # Match available CPU cores
```

Rule of thumb for `buffer_pool_size`: allocate 50-70% of available RAM. For example, on a 16 GB machine, set `buffer_pool_size` to 8-11 GB.

```bash
# Reload configuration without restart (only mutable settings like log level)
kill -HUP $(pidof thunderdb)

# For storage settings, a full restart is required
kill -TERM $(pidof thunderdb)
thunderdb --config /etc/thunderdb/thunderdb.toml
```

#### Kubernetes

```bash
# Update resource requests/limits
kubectl -n thunderdb patch statefulset thunderdb --type='json' -p='[
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/requests/memory", "value": "4Gi"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/limits/memory", "value": "16Gi"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/requests/cpu", "value": "2"},
  {"op": "replace", "path": "/spec/template/spec/containers/0/resources/limits/cpu", "value": "8"}
]'

# Expand PVCs (if the StorageClass supports volume expansion)
kubectl -n thunderdb patch pvc data-thunderdb-0 -p '{"spec": {"resources": {"requests": {"storage": "200Gi"}}}}'
kubectl -n thunderdb patch pvc wal-thunderdb-0 -p '{"spec": {"resources": {"requests": {"storage": "50Gi"}}}}'

# Update the ConfigMap with tuned buffer pool settings, then restart
kubectl -n thunderdb rollout restart statefulset/thunderdb
```

### Horizontal Scaling (Cluster Mode)

ThunderDB uses Raft consensus for replication. To add nodes:

1. Update the `cluster.peers` list in the configuration on each node.
2. Set a unique `node_id` for each node.

```toml
# Node 1 config
node_id = 1
[cluster]
cluster_name = "thunderdb-prod"
peers = ["node2:9090", "node3:9090"]
replication_factor = 3
auto_balance = true
raft_election_timeout = "1s"
raft_heartbeat_interval = "100ms"
```

#### Adding a Node in Kubernetes

```bash
# Scale from 1 to 3 replicas
kubectl -n thunderdb scale statefulset thunderdb --replicas=3

# Update the ConfigMap to include peer addresses
kubectl -n thunderdb edit configmap thunderdb-config
# Set peers = ["thunderdb-0.thunderdb.thunderdb.svc:9090", "thunderdb-1.thunderdb.thunderdb.svc:9090", "thunderdb-2.thunderdb.thunderdb.svc:9090"]
# Set replication_factor = 3
# Set auto_balance = true

# Restart all pods to pick up peer configuration
kubectl -n thunderdb rollout restart statefulset/thunderdb

# Verify all nodes are running and ready
kubectl -n thunderdb get pods -l app.kubernetes.io/name=thunderdb
```

#### Region Management

ThunderDB automatically shards data into regions. Key settings:

```toml
[cluster]
max_region_size = 268435456   # 256 MB - regions split above this size
min_region_size = 67108864    # 64 MB - regions merge below this size
auto_balance = true            # Automatically rebalance regions across nodes
```

Monitor region distribution:

```bash
curl -s http://localhost:8080/metrics | grep thunderdb_regions_count
```

---

## Monitoring and Alerting

### Prometheus Metrics

ThunderDB exposes Prometheus-format metrics at `/metrics`:

```bash
curl -s http://localhost:8080/metrics
```

Key metrics to monitor:

| Metric | Type | Description |
|--------|------|-------------|
| `thunderdb_queries_total` | Counter | Total queries executed |
| `thunderdb_queries_failed_total` | Counter | Total failed queries |
| `thunderdb_query_duration_seconds` | Histogram | Query latency distribution |
| `thunderdb_transactions_started_total` | Counter | Transactions started |
| `thunderdb_transactions_committed_total` | Counter | Transactions committed |
| `thunderdb_transactions_aborted_total` | Counter | Transactions aborted/rolled back |
| `thunderdb_active_connections` | Gauge | Currently active connections |
| `thunderdb_active_transactions` | Gauge | Currently open transactions |
| `thunderdb_buffer_pool_hit_ratio` | Gauge | Buffer pool cache hit ratio (0.0-1.0) |
| `thunderdb_buffer_pool_hits_total` | Counter | Buffer pool cache hits |
| `thunderdb_buffer_pool_misses_total` | Counter | Buffer pool cache misses |
| `thunderdb_wal_size_bytes` | Gauge | Current WAL size on disk |
| `thunderdb_pages_read_total` | Counter | Total pages read from disk |
| `thunderdb_pages_written_total` | Counter | Total pages written to disk |
| `thunderdb_bytes_read_total` | Counter | Total bytes read |
| `thunderdb_bytes_written_total` | Counter | Total bytes written |
| `thunderdb_raft_proposals_total` | Counter | Raft proposals submitted |
| `thunderdb_raft_commits_total` | Counter | Raft commits applied |
| `thunderdb_regions_count` | Gauge | Number of data regions |
| `thunderdb_transaction_duration_seconds` | Histogram | Transaction duration distribution |

### Admin Metrics API

For JSON-formatted metrics:

```bash
curl -s http://localhost:8080/admin/metrics \
  -H "Authorization: Bearer <admin-token>" | jq .
```

Returns: `queries_total`, `queries_failed`, `active_connections`, `active_transactions`, `buffer_pool_hit_ratio`, `average_query_time_ms`.

### Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: 'thunderdb'
    scrape_interval: 15s
    static_configs:
      - targets: ['thunderdb:8080']
    # For Kubernetes service discovery:
    # kubernetes_sd_configs:
    #   - role: pod
    #     namespaces:
    #       names: ['thunderdb']
    #   relabel_configs:
    #     - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
    #       action: keep
    #       regex: thunderdb
```

### Recommended Alerts

```yaml
groups:
  - name: thunderdb
    rules:
      # SLO: Availability < 99.95%
      - alert: ThunderDBHighErrorRate
        expr: |
          rate(thunderdb_queries_failed_total[5m])
          / rate(thunderdb_queries_total[5m]) > 0.001
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "ThunderDB error rate exceeds 0.1% SLO threshold"

      # SLO: Point query p99 > 100ms
      - alert: ThunderDBHighPointQueryLatency
        expr: |
          histogram_quantile(0.99, rate(thunderdb_query_duration_seconds_bucket[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "ThunderDB p99 query latency exceeds 100ms SLO"

      # Buffer pool hit ratio low
      - alert: ThunderDBLowBufferPoolHitRatio
        expr: thunderdb_buffer_pool_hit_ratio < 0.9
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Buffer pool hit ratio below 90% -- consider increasing buffer_pool_size"

      # WAL growing too large
      - alert: ThunderDBWalTooLarge
        expr: thunderdb_wal_size_bytes > 2147483648
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "WAL size exceeds 2 GB -- check checkpoint worker"

      # No active connections (possible network issue)
      - alert: ThunderDBNoConnections
        expr: thunderdb_active_connections == 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "No active connections to ThunderDB"

      # Health endpoint failing
      - alert: ThunderDBUnhealthy
        expr: up{job="thunderdb"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "ThunderDB instance is down"

      # Long-running transactions
      - alert: ThunderDBLongTransactions
        expr: |
          histogram_quantile(0.99, rate(thunderdb_transaction_duration_seconds_bucket[5m])) > 60
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Transactions taking longer than 60 seconds at p99"
```

### Health Endpoints

| Endpoint | Auth | Purpose | Used By |
|----------|------|---------|---------|
| `GET /admin/live` | None | Liveness probe -- returns 200 if process is alive | Kubernetes livenessProbe |
| `GET /admin/ready` | None | Readiness probe -- returns 200 if engine is initialized | Kubernetes readinessProbe |
| `GET /admin/health` | None | Full health check (database, storage, disk I/O) | Monitoring dashboards |

### Slow Query Log

Enable slow query logging in the configuration:

```toml
[logging]
level = "info"
format = "json"
slow_query_enabled = true
slow_query_threshold = "1s"    # Log queries taking longer than 1 second
```

### OpenTelemetry Integration

```toml
[logging]
otel_endpoint = "http://otel-collector:4317"
otel_service_name = "thunderdb"
```

Build with the `otel` feature flag:

```bash
cargo build --release --features otel
```

---

## Troubleshooting Common Issues

### High Query Latency

**Symptoms:** `thunderdb_query_duration_seconds` p99 exceeds SLO thresholds.

**Diagnosis:**

```bash
# Check buffer pool hit ratio (should be > 0.95 for OLTP workloads)
curl -s http://localhost:8080/metrics | grep buffer_pool_hit_ratio

# Check active connections and transactions
curl -s http://localhost:8080/admin/metrics \
  -H "Authorization: Bearer <admin-token>" | jq .

# Check for slow queries in logs
journalctl -u thunderdb | grep "slow_query"

# Check disk I/O saturation
iostat -xz 1 5
```

**Resolution:**

1. **Low buffer pool hit ratio:** Increase `storage.buffer_pool_size` in the config and restart.
2. **Disk I/O saturation:** Move data/WAL to faster storage (NVMe recommended). Enable `direct_io = true` for predictable I/O patterns.
3. **Lock contention:** Check for long-running transactions holding locks. Kill stale connections via `DELETE /admin/connections/:id`.
4. **Missing indexes:** Use `EXPLAIN` to identify full table scans, then create appropriate indexes.
5. **Large result sets:** Set `max_result_rows` in config to prevent runaway queries. Current default is 1,000,000 rows.

### Disk Full

**Symptoms:** Write errors, server goes read-only, disk space alerts.

ThunderDB monitors disk space automatically:
- **Warning** at `disk_space_warn_percent` (default: 10% free) -- logs a warning.
- **Critical** at `disk_space_critical_percent` (default: 5% free) -- database goes read-only.

**Diagnosis:**

```bash
# Check disk usage
df -h /var/lib/thunderdb/data /var/lib/thunderdb/wal

# Check WAL size
curl -s http://localhost:8080/metrics | grep thunderdb_wal_size_bytes

# List largest files
du -sh /var/lib/thunderdb/data/* | sort -rh | head -20
du -sh /var/lib/thunderdb/wal/* | sort -rh | head -20
```

**Resolution:**

1. **Prune old backups:** Remove backups beyond the retention policy.
   ```bash
   ls -la /var/lib/thunderdb/data/backups/
   rm -rf /var/lib/thunderdb/data/backups/<oldest-backup>
   ```
2. **Force a checkpoint:** This flushes dirty pages and allows WAL truncation.
   ```bash
   # The checkpoint worker runs every checkpoint_interval (default: 5 minutes)
   # Restart the server to force an immediate checkpoint on shutdown
   kill -TERM $(pidof thunderdb)
   thunderdb --config /etc/thunderdb/thunderdb.toml
   ```
3. **Vacuum tables:** Reclaim space from deleted rows.
   ```bash
   curl -X POST http://localhost:8080/admin/vacuum/my_table \
     -H "Authorization: Bearer <admin-token>"
   ```
4. **Expand storage (Kubernetes):**
   ```bash
   kubectl -n thunderdb patch pvc data-thunderdb-0 \
     -p '{"spec": {"resources": {"requests": {"storage": "100Gi"}}}}'
   ```
5. **Move data directory:** If the current volume cannot be expanded, stop the server, copy data to a larger volume, update `data_dir` / `wal_dir` in config, and restart.

### Connection Exhaustion

**Symptoms:** New connections rejected, `thunderdb_active_connections` at limit, clients receiving connection errors.

**Diagnosis:**

```bash
# Check active connection count
curl -s http://localhost:8080/metrics | grep thunderdb_active_connections

# List active connections
curl -s http://localhost:8080/admin/connections \
  -H "Authorization: Bearer <admin-token>" | jq .

# List active queries (look for long-running queries holding connections)
curl -s http://localhost:8080/admin/queries \
  -H "Authorization: Bearer <admin-token>" | jq .
```

**Resolution:**

1. **Kill idle/stale connections:**
   ```bash
   # Kill a specific connection
   curl -X DELETE http://localhost:8080/admin/connections/<connection-id> \
     -H "Authorization: Bearer <admin-token>"
   ```
2. **Increase connection limits:** The server currently allows up to 1,000 connections per protocol. Ensure your connection pool settings (client-side) are reasonable (typically 10-50 per application instance).
3. **Use connection pooling:** Place a connection pooler (e.g., PgBouncer for PostgreSQL protocol) in front of ThunderDB for high-connection-count workloads.
4. **Set query timeouts:** Configure `query_timeout` in the server config to prevent queries from holding connections indefinitely.
   ```toml
   query_timeout = "30s"
   ```

### WAL Growth

**Symptoms:** `thunderdb_wal_size_bytes` growing continuously, WAL directory consuming excessive disk space.

**Diagnosis:**

```bash
# Check current WAL size
curl -s http://localhost:8080/metrics | grep thunderdb_wal_size_bytes

# Check WAL directory
du -sh /var/lib/thunderdb/wal/

# Compare against max_wal_size setting (default: 2 GB for k8s, 1 GB default)
grep max_wal_size /etc/thunderdb/thunderdb.toml
```

**Resolution:**

1. **Verify checkpoint worker is running:** The checkpoint worker flushes dirty pages and truncates WAL at `checkpoint_interval` (default: 5 minutes). Check logs for checkpoint messages.
2. **Reduce checkpoint interval:** For write-heavy workloads, reduce `checkpoint_interval`:
   ```toml
   [storage]
   checkpoint_interval = "1m"
   ```
3. **Increase max_wal_size cautiously:** If WAL growth is expected (burst writes), you can increase the limit, but ensure you have sufficient disk space:
   ```toml
   [storage]
   max_wal_size = 4294967296  # 4 GB
   ```
4. **Check for long-running transactions:** Open transactions prevent WAL from being truncated. Commit or abort stale transactions.
5. **Force a restart:** As a last resort, a clean shutdown triggers a final checkpoint that should truncate the WAL.

---

## Configuration Reference for Production Tuning

### Critical Production Settings

```toml
# thunderdb.toml -- Production template

node_id = 1
listen_addr = "0.0.0.0"
pg_port = 5432
mysql_port = 3306
resp_port = 6379
http_port = 8080
grpc_port = 9090

data_dir = "/var/lib/thunderdb/data"
wal_dir = "/var/lib/thunderdb/wal"     # Use a separate volume for WAL

query_timeout = "30s"
max_result_rows = 1000000

[storage]
# Buffer pool: 50-70% of available RAM
buffer_pool_size = 8589934592          # 8 GB (for a 16 GB machine)
wal_buffer_size = 134217728            # 128 MB
page_size = 16384                      # 16 KB (do not change after init)
checkpoint_interval = "5m"
compaction_threads = 4                 # Match CPU core count
direct_io = true                       # Bypass OS page cache; recommended for dedicated servers
compression = true
compression_algorithm = "Lz4"          # Lz4 for speed, Zstd for ratio
max_wal_size = 2147483648              # 2 GB
sync_commit = true                     # Set to false only if durability is not critical
disk_space_warn_percent = 10.0
disk_space_critical_percent = 5.0
disk_check_interval = "30s"
backup_enabled = true
backup_interval = "24h"
backup_retention_count = 7
backup_dir = "/mnt/backups/thunderdb"

[cluster]
cluster_name = "thunderdb-prod"
peers = []                             # Add peer addresses for multi-node clusters
replication_factor = 1                 # Set to 3 for HA
raft_election_timeout = "1s"
raft_heartbeat_interval = "100ms"
max_region_size = 268435456            # 256 MB
min_region_size = 67108864             # 64 MB
auto_balance = false                   # Enable for multi-node clusters

[security]
authentication_enabled = true
tls_enabled = true
tls_cert_path = "/etc/thunderdb/tls/tls.crt"
tls_key_path = "/etc/thunderdb/tls/tls.key"
superuser = "admin"
# Set via env: THUNDERDB_SUPERUSER_PASSWORD_HASH

[logging]
level = "info"
format = "json"                        # json for structured logging in production
slow_query_enabled = true
slow_query_threshold = "1s"
# file = "/var/log/thunderdb/thunderdb.log"   # Optional: log to file with daily rotation
# otel_endpoint = "http://otel-collector:4317" # Optional: OpenTelemetry export
# otel_service_name = "thunderdb-prod"
```

### Environment Variable Reference

All settings can be overridden by environment variables (highest priority):

| Variable | Description | Example |
|----------|-------------|---------|
| `THUNDERDB_LISTEN_ADDR` | Bind address | `0.0.0.0` |
| `THUNDERDB_HTTP_PORT` | HTTP/admin port | `8080` |
| `THUNDERDB_PG_PORT` | PostgreSQL wire protocol port | `5432` |
| `THUNDERDB_MYSQL_PORT` | MySQL wire protocol port | `3306` |
| `THUNDERDB_RESP_PORT` | RESP (Redis) protocol port | `6379` |
| `THUNDERDB_DATA_DIR` | Data directory path | `/var/lib/thunderdb/data` |
| `THUNDERDB_WAL_DIR` | WAL directory path | `/var/lib/thunderdb/wal` |
| `THUNDERDB_LOG_LEVEL` | Log level | `info`, `debug`, `warn` |
| `THUNDERDB_TLS_ENABLED` | Enable TLS | `true` |
| `THUNDERDB_TLS_CERT` | TLS certificate file path | `/etc/tls/tls.crt` |
| `THUNDERDB_TLS_KEY` | TLS private key file path | `/etc/tls/tls.key` |
| `THUNDERDB_SUPERUSER_PASSWORD_HASH` | Bcrypt hash for superuser password | `$2b$12$...` |
| `THUNDERDB_AUTH_ENABLED` | Enable authentication | `true` |

### Configuration Hot-Reload

Send SIGHUP to reload mutable settings without restarting:

```bash
kill -HUP $(pidof thunderdb)
```

Settings that can be reloaded:
- `logging.level`
- `query_timeout`

Settings that require a full restart:
- All port settings
- `data_dir` and `wal_dir`
- `storage.buffer_pool_size`
- `storage.page_size`
- `cluster.peers`
- TLS certificate paths

---

## Emergency Procedures

### Server Unresponsive (Hung)

```bash
# 1. Attempt graceful shutdown first
kill -TERM $(pidof thunderdb)

# 2. Wait 30 seconds for graceful shutdown
sleep 30

# 3. If still running, check if shutdown is in progress
kill -0 $(pidof thunderdb) 2>/dev/null && echo "Still running"

# 4. As a last resort, force kill (WAL replay will recover on next start)
kill -9 $(pidof thunderdb)

# 5. Restart
thunderdb --config /etc/thunderdb/thunderdb.toml
```

In Kubernetes:

```bash
# Force delete a hung pod (bypasses terminationGracePeriodSeconds)
kubectl -n thunderdb delete pod thunderdb-0 --grace-period=0 --force
```

### Data Corruption Suspected

```bash
# 1. Stop the server immediately
kill -TERM $(pidof thunderdb)

# 2. Make a copy of the current state BEFORE any recovery attempt
cp -a /var/lib/thunderdb/data /var/lib/thunderdb/data.corrupt-backup
cp -a /var/lib/thunderdb/wal /var/lib/thunderdb/wal.corrupt-backup

# 3. Try starting the server -- WAL replay may fix inconsistencies
thunderdb --config /etc/thunderdb/thunderdb.toml --verbose

# 4. Verify health
curl -s http://localhost:8080/admin/health | jq .

# 5. If the server fails to start, restore from the latest backup
thunderdb --config /etc/thunderdb/thunderdb.toml \
  --restore-pitr "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --restore-backup /mnt/backups/thunderdb/latest

# 6. Start after restore
thunderdb --config /etc/thunderdb/thunderdb.toml
```

### Split-Brain in Cluster Mode

If Raft consensus is lost (e.g., network partition resolved with divergent state):

```bash
# 1. Identify the leader node
curl -s http://node1:8080/metrics | grep thunderdb_raft
curl -s http://node2:8080/metrics | grep thunderdb_raft
curl -s http://node3:8080/metrics | grep thunderdb_raft

# 2. Stop all nodes
for host in node1 node2 node3; do
  ssh $host "kill -TERM \$(pidof thunderdb)"
done

# 3. Identify the node with the most recent WAL/commit state
for host in node1 node2 node3; do
  echo "$host:"
  ssh $host "ls -la /var/lib/thunderdb/wal/ | tail -5"
done

# 4. Start the most up-to-date node first as the seed
ssh node1 "thunderdb --config /etc/thunderdb/thunderdb.toml"

# 5. Start remaining nodes -- they will rejoin via Raft
ssh node2 "thunderdb --config /etc/thunderdb/thunderdb.toml"
ssh node3 "thunderdb --config /etc/thunderdb/thunderdb.toml"

# 6. Verify cluster health
for host in node1 node2 node3; do
  echo "$host:"
  curl -s http://$host:8080/admin/health | jq .status
done
```

### Emergency Read-Only Mode

If the database is experiencing write issues (disk failures, corruption) but reads still work:

```bash
# The server automatically enters read-only mode when disk_space_critical_percent
# threshold is breached (default: 5% free space). This protects against
# complete disk exhaustion.

# To manually verify the server state:
curl -s http://localhost:8080/admin/health | jq .

# If disk_ok is false, the server is likely in read-only mode.
# Free disk space or expand the volume, then restart the server to
# re-enable writes.
```

### Recovering from OOM Kill

```bash
# 1. Check if OOM killed the process
dmesg | grep -i "oom.*thunderdb"
journalctl -k | grep -i oom

# 2. Reduce buffer_pool_size to fit within available memory
# Rule: buffer_pool_size + OS overhead + connection memory < total RAM
# Each connection uses approximately 2-10 MB depending on query complexity

# 3. Set memory limits in the OS
# For systemd:
# MemoryMax=12G in the unit file
# For Kubernetes:
# resources.limits.memory in the StatefulSet

# 4. Restart the server (WAL replay will recover state)
thunderdb --config /etc/thunderdb/thunderdb.toml
```

### Rolling Restart (Cluster)

For configuration changes that require a restart in a multi-node cluster:

```bash
# Restart one node at a time, waiting for it to rejoin before proceeding
for i in 0 1 2; do
  echo "Restarting thunderdb-$i..."

  kubectl -n thunderdb delete pod thunderdb-$i
  kubectl -n thunderdb wait --for=condition=Ready pod/thunderdb-$i --timeout=120s

  echo "thunderdb-$i is ready. Waiting 30s for replication catch-up..."
  sleep 30
done
```

For bare-metal clusters, follow the same pattern: stop one node, wait for it to recover and rejoin, then proceed to the next.
