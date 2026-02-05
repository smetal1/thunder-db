# ThunderDB

A high-performance distributed HTAP (Hybrid Transactional/Analytical Processing) database written in Rust. ThunderDB combines OLTP and OLAP capabilities in a single system with PostgreSQL wire protocol compatibility, distributed transactions, vector search, and change data capture.

## Features

- **HTAP Architecture** — Unified row store for transactions and column store for analytics
- **Multi-Protocol** — PostgreSQL, MySQL, and RESP (Redis) wire protocol support
- **Distributed** — Raft consensus, region-based sharding, and automatic rebalancing
- **ACID Transactions** — MVCC with distributed two-phase commit and deadlock detection
- **Vector Search** — HNSW and IVF indexes for approximate nearest neighbor queries
- **Change Data Capture** — Stream changes from PostgreSQL, MySQL, MongoDB, and Redis
- **Foreign Data Wrappers** — Query external PostgreSQL, MySQL, MongoDB, and Redis directly
- **Multiple APIs** — REST, gRPC, GraphQL, and WebSocket interfaces
- **AI-Native** — Optional LLM integration for natural language queries and ML operations
- **Production Ready** — WAL durability, PITR, TLS, RBAC, rate limiting, and observability

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Client Layer                       │
│   PostgreSQL  │  MySQL  │  RESP  │  REST/gRPC/GraphQL   │
├─────────────────────────────────────────────────────────┤
│                     SQL Engine                          │
│       Parser → Analyzer → Planner → Optimizer           │
├─────────────────────────────────────────────────────────┤
│                   Query Executor                        │
│          Vectorized Execution (DataFusion)               │
├──────────────────────┬──────────────────────────────────┤
│   Transaction Mgr    │         Cluster Layer            │
│   MVCC · 2PC · Locks │   Raft · Sharding · Replication  │
├──────────────────────┴──────────────────────────────────┤
│                    Storage Engine                        │
│   Row Store  │  Column Store  │  B+Tree  │  Vector Index │
│                WAL  │  Buffer Pool                       │
└─────────────────────────────────────────────────────────┘
```

### Crates

| Crate | Description |
|-------|-------------|
| `thunder-server` | Main server binary and engine integration |
| `thunder-client` | Native Rust client and interactive CLI (`thunder-cli`) |
| `thunder-sql` | SQL parser, analyzer, planner, and optimizer |
| `thunder-query` | Vectorized query execution with DataFusion |
| `thunder-storage` | Row/column stores, B+Tree indexes, WAL, buffer pool |
| `thunder-txn` | MVCC, lock manager, deadlock detection, 2PC coordinator |
| `thunder-cluster` | Raft consensus, region sharding, replication |
| `thunder-protocol` | PostgreSQL, MySQL, and RESP wire protocols |
| `thunder-api` | REST, gRPC, GraphQL, and WebSocket APIs |
| `thunder-vector` | HNSW/IVF vector indexes and similarity search |
| `thunder-cdc` | Change data capture connectors |
| `thunder-fdw` | Foreign data wrapper connectors |
| `thunder-common` | Shared types, config, errors, and utilities |

## Quick Start

### Prerequisites

- Rust 1.75+ (pinned via `rust-toolchain.toml`)
- System dependencies: `pkg-config`, `libssl-dev` (Linux) or equivalent

### Build from Source

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release
```

The server binary is at `target/release/thunderdb` and the CLI at `target/release/thunder-cli`.

### Run

```bash
# Start the server with default config
./target/release/thunderdb --config config/thunderdb.toml

# Connect with the interactive CLI
./target/release/thunder-cli
```

### Docker

```bash
# Build and run with Docker Compose
docker compose up -d

# Or build the image manually
docker build -t thunderdb .
docker run -p 5432:5432 -p 3306:3306 -p 6379:6379 -p 8080:8080 thunderdb
```

### Connect

ThunderDB is wire-compatible with PostgreSQL, MySQL, and Redis:

```bash
# PostgreSQL protocol (port 5432)
psql -h localhost -p 5432 -U admin

# MySQL protocol (port 3306)
mysql -h 127.0.0.1 -P 3306 -u admin -p

# Redis protocol (port 6379)
redis-cli -p 6379

# HTTP API (port 8080)
curl http://localhost:8080/api/v1/health
```

## Configuration

ThunderDB uses TOML configuration files. See [`config/thunderdb.toml`](config/thunderdb.toml) for the full reference.

Key settings:

```toml
[server]
node_id = 1
listen_addr = "0.0.0.0"
pg_port = 5432
mysql_port = 3306
resp_port = 6379
http_port = 8080
data_dir = "./data"
wal_dir = "./wal"

[storage]
buffer_pool_size = "128MB"
page_size = "16KB"
compression = true
compression_algorithm = "Lz4"

[cluster]
replication_factor = 3
auto_balance = true

[security]
authentication_enabled = true
tls_enabled = false
```

## Cluster Deployment

### Local 3-Node Cluster

```bash
# Start a 3-node cluster locally
./scripts/cluster.sh start

# Check cluster status
./scripts/cluster.sh status

# Stop the cluster
./scripts/cluster.sh stop
```

### Kubernetes

```bash
kubectl apply -k deploy/k8s/
```

See [`deploy/k8s/`](deploy/k8s/) for StatefulSet, Service, ConfigMap, and PDB manifests.

## Development

```bash
# Run all tests
./scripts/run_tests.sh

# Run with coverage
./scripts/run_tests.sh --coverage

# Run benchmarks
cargo bench

# Format and lint
cargo fmt --check
cargo clippy -- -D warnings
```

### Optional Features

```bash
# Enable LLM-powered natural language queries
cargo build --features llm

# Enable ML operations
cargo build --features ml

# Enable OpenTelemetry tracing
cargo build -p thunder-server --features otel

# Enable SIMD-accelerated vector operations
cargo build -p thunder-vector --features simd
```

## Monitoring

- **Health endpoints**: `/admin/live`, `/admin/ready`, `/admin/health`
- **Prometheus metrics**: Exposed for scraping
- **Grafana dashboards**: Pre-built dashboards in [`deploy/grafana/`](deploy/grafana/)
- **Alerting rules**: Prometheus alerts in [`deploy/prometheus/`](deploy/prometheus/)
- **Operations runbook**: [`deploy/runbook.md`](deploy/runbook.md)

## License

Apache-2.0
