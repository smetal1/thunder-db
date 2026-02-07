#!/bin/bash
# ThunderDB Benchmark Report Generator
# Collects and formats benchmark results into a comprehensive report

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-thunderdb}"
REPORT_DIR="${SCRIPT_DIR}/reports"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="${REPORT_DIR}/thunderdb_benchmark_report_${TIMESTAMP}.md"
JSON_OUTPUT="${REPORT_DIR}/thunderdb_benchmark_report_${TIMESTAMP}.json"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"; }
info() { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }

mkdir -p "$REPORT_DIR"

log "Generating ThunderDB Benchmark Report..."

# Collect job logs
log "Collecting benchmark results from Kubernetes jobs..."

PGBENCH_LOG=""
SYSBENCH_LOG=""
REDIS_LOG=""

if kubectl get job thunderdb-pgbench -n "$NAMESPACE" &>/dev/null; then
    PGBENCH_LOG=$(kubectl logs job/thunderdb-pgbench -n "$NAMESPACE" 2>/dev/null || echo "No logs available")
fi

if kubectl get job thunderdb-sysbench -n "$NAMESPACE" &>/dev/null; then
    SYSBENCH_LOG=$(kubectl logs job/thunderdb-sysbench -n "$NAMESPACE" 2>/dev/null || echo "No logs available")
fi

if kubectl get job thunderdb-redis-benchmark -n "$NAMESPACE" &>/dev/null; then
    REDIS_LOG=$(kubectl logs job/thunderdb-redis-benchmark -n "$NAMESPACE" 2>/dev/null || echo "No logs available")
fi

# Get cluster info
CLUSTER_INFO=$(kubectl cluster-info 2>/dev/null | head -1 || echo "Unknown")
NODE_INFO=$(kubectl get nodes -o wide 2>/dev/null || echo "Unknown")
POD_INFO=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=thunderdb -o wide 2>/dev/null || echo "Unknown")

# Extract metrics
extract_pgbench_tps() {
    echo "$PGBENCH_LOG" | grep "tps = " | tail -1 | awk '{print $3}' || echo "N/A"
}

extract_sysbench_tps() {
    local section=$1
    echo "$SYSBENCH_LOG" | grep -A30 "$section" | grep "transactions:" | awk '{print $3}' | tr -d '(' || echo "N/A"
}

extract_redis_rps() {
    local cmd=$1
    echo "$REDIS_LOG" | grep -i "^$cmd:" | awk '{print $2}' || echo "N/A"
}

# Generate Markdown report
cat > "$OUTPUT_FILE" <<EOF
# ThunderDB Benchmark Report

**Generated:** $(date)
**Namespace:** $NAMESPACE

---

## Executive Summary

This report contains performance benchmarks for ThunderDB across all supported protocols:
- PostgreSQL (port 5432)
- MySQL (port 3306)
- Redis/RESP (port 6379)

---

## Environment

### Cluster Information
\`\`\`
$CLUSTER_INFO
\`\`\`

### Node Information
\`\`\`
$NODE_INFO
\`\`\`

### ThunderDB Pod
\`\`\`
$POD_INFO
\`\`\`

---

## PostgreSQL Protocol Benchmark (pgbench)

### Configuration
- **Tool:** pgbench (PostgreSQL)
- **Scale Factor:** 10
- **Clients:** 10
- **Duration:** 60 seconds

### Results

| Test Type | TPS | Description |
|-----------|-----|-------------|
| Read-Only | $(extract_pgbench_tps) | SELECT-only workload |
| Read-Write | $(echo "$PGBENCH_LOG" | grep -B5 "tps = " | head -1 | grep "tps" | awk '{print $3}' || echo "N/A") | TPC-B like mixed workload |

### Raw Output
\`\`\`
$PGBENCH_LOG
\`\`\`

---

## MySQL Protocol Benchmark (sysbench)

### Configuration
- **Tool:** sysbench
- **Tables:** 4
- **Table Size:** 10,000 rows
- **Threads:** 8
- **Duration:** 60 seconds

### Results

| Test Type | TPS | Description |
|-----------|-----|-------------|
| OLTP Read-Only | $(extract_sysbench_tps "OLTP Read-Only") | Read-only OLTP workload |
| OLTP Read-Write | $(extract_sysbench_tps "OLTP Read-Write") | Mixed read/write OLTP |
| OLTP Write-Only | $(extract_sysbench_tps "OLTP Write-Only") | Write-intensive workload |
| Point Select | $(extract_sysbench_tps "Point Select") | Single-row SELECT by primary key |

### Raw Output
\`\`\`
$SYSBENCH_LOG
\`\`\`

---

## Redis Protocol Benchmark (redis-benchmark)

### Configuration
- **Tool:** redis-benchmark
- **Clients:** 50
- **Requests:** 100,000
- **Data Size:** 256 bytes

### Results

| Command | Requests/sec | Description |
|---------|--------------|-------------|
| PING | $(extract_redis_rps "ping") | Connection test |
| SET | $(extract_redis_rps "set") | String write |
| GET | $(extract_redis_rps "get") | String read |
| INCR | $(extract_redis_rps "incr") | Atomic increment |
| LPUSH | $(extract_redis_rps "lpush") | List push left |
| RPUSH | $(extract_redis_rps "rpush") | List push right |
| LPOP | $(extract_redis_rps "lpop") | List pop left |
| SADD | $(extract_redis_rps "sadd") | Set add |
| HSET | $(extract_redis_rps "hset") | Hash set |

### Raw Output
\`\`\`
$REDIS_LOG
\`\`\`

---

## Summary & Recommendations

### Key Findings
1. **PostgreSQL Protocol:** Compatible with standard pgbench workloads
2. **MySQL Protocol:** Supports sysbench OLTP benchmarks
3. **Redis Protocol:** Handles standard redis-benchmark operations

### Performance Notes
- Results may vary based on:
  - Cluster resources (CPU, memory, storage IOPS)
  - Network latency between benchmark client and ThunderDB
  - Concurrent workload on the cluster
  - Storage class performance characteristics

### Next Steps
- Run benchmarks with higher concurrency to test scalability
- Test with larger datasets for durability benchmarks
- Compare with dedicated single-protocol databases

---

*Report generated by ThunderDB Benchmark Suite*
EOF

# Generate JSON report
cat > "$JSON_OUTPUT" <<EOF
{
    "report_metadata": {
        "generated_at": "$(date -Iseconds)",
        "namespace": "$NAMESPACE",
        "tool_version": "1.0.0"
    },
    "postgresql": {
        "tool": "pgbench",
        "config": {
            "scale_factor": 10,
            "clients": 10,
            "duration_seconds": 60
        },
        "results": {
            "read_only_tps": "$(extract_pgbench_tps)",
            "available": $([ -n "$PGBENCH_LOG" ] && echo "true" || echo "false")
        }
    },
    "mysql": {
        "tool": "sysbench",
        "config": {
            "tables": 4,
            "table_size": 10000,
            "threads": 8,
            "duration_seconds": 60
        },
        "results": {
            "oltp_read_only_tps": "$(extract_sysbench_tps 'OLTP Read-Only')",
            "oltp_read_write_tps": "$(extract_sysbench_tps 'OLTP Read-Write')",
            "available": $([ -n "$SYSBENCH_LOG" ] && echo "true" || echo "false")
        }
    },
    "redis": {
        "tool": "redis-benchmark",
        "config": {
            "clients": 50,
            "requests": 100000,
            "data_size_bytes": 256
        },
        "results": {
            "ping_rps": "$(extract_redis_rps 'ping')",
            "set_rps": "$(extract_redis_rps 'set')",
            "get_rps": "$(extract_redis_rps 'get')",
            "available": $([ -n "$REDIS_LOG" ] && echo "true" || echo "false")
        }
    }
}
EOF

log "Report generated successfully!"
echo ""
info "Markdown Report: $OUTPUT_FILE"
info "JSON Report: $JSON_OUTPUT"
echo ""
log "To view the report:"
echo "  cat $OUTPUT_FILE"
echo ""
log "Or open in browser (macOS):"
echo "  open $OUTPUT_FILE"
