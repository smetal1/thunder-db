#!/bin/bash
# MySQL Protocol Benchmark using sysbench
# Benchmarks ThunderDB's MySQL-compatible interface

set -e

# Configuration
HOST="${THUNDERDB_HOST:-thunderdb.thunderdb.svc.cluster.local}"
PORT="${THUNDERDB_MYSQL_PORT:-3306}"
USER="${THUNDERDB_USER:-admin}"
PASSWORD="${THUNDERDB_PASSWORD:-}"
DATABASE="${THUNDERDB_DATABASE:-benchmark}"
REPORT_DIR="${REPORT_DIR:-/reports}"
TABLE_SIZE="${SYSBENCH_TABLE_SIZE:-10000}"
TABLES="${SYSBENCH_TABLES:-4}"
THREADS="${SYSBENCH_THREADS:-8}"
DURATION="${SYSBENCH_DURATION:-60}"
REPORT_INTERVAL="${SYSBENCH_REPORT_INTERVAL:-5}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"; }

REPORT_FILE="${REPORT_DIR}/sysbench_report_$(date +%Y%m%d_%H%M%S).txt"
JSON_REPORT="${REPORT_DIR}/sysbench_report_$(date +%Y%m%d_%H%M%S).json"

SYSBENCH_COMMON="--mysql-host=$HOST --mysql-port=$PORT --mysql-user=$USER --mysql-password=$PASSWORD --mysql-db=$DATABASE"

log "Starting MySQL Protocol Benchmark (sysbench)"
log "Target: $HOST:$PORT"
log "Configuration: tables=$TABLES, table_size=$TABLE_SIZE, threads=$THREADS, duration=${DURATION}s"

# Create report header
{
    echo "=============================================="
    echo "ThunderDB MySQL Protocol Benchmark Report"
    echo "=============================================="
    echo "Date: $(date)"
    echo "Host: $HOST:$PORT"
    echo "Tables: $TABLES"
    echo "Table Size: $TABLE_SIZE rows"
    echo "Threads: $THREADS"
    echo "Duration: ${DURATION}s"
    echo "=============================================="
    echo ""
} > "$REPORT_FILE"

# Create database
log "Setting up benchmark database..."
mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASSWORD" -e "DROP DATABASE IF EXISTS $DATABASE; CREATE DATABASE $DATABASE;" 2>/dev/null || true

# Prepare benchmark tables
log "Preparing sysbench tables..."
PREP_START=$(date +%s.%N)
sysbench oltp_read_write $SYSBENCH_COMMON \
    --tables=$TABLES \
    --table-size=$TABLE_SIZE \
    prepare 2>&1 | tee -a "$REPORT_FILE"
PREP_END=$(date +%s.%N)
PREP_TIME=$(echo "$PREP_END - $PREP_START" | bc)

{
    echo ""
    echo "Preparation Time: ${PREP_TIME}s"
    echo ""
} >> "$REPORT_FILE"

# Function to run a sysbench test
run_sysbench_test() {
    local test_name=$1
    local test_type=$2

    log "Running $test_name benchmark..."
    {
        echo "=============================================="
        echo "$test_name"
        echo "=============================================="
    } >> "$REPORT_FILE"

    sysbench $test_type $SYSBENCH_COMMON \
        --tables=$TABLES \
        --table-size=$TABLE_SIZE \
        --threads=$THREADS \
        --time=$DURATION \
        --report-interval=$REPORT_INTERVAL \
        run 2>&1 | tee -a "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
}

# Run OLTP Read-Only test
run_sysbench_test "OLTP Read-Only" "oltp_read_only"
READONLY_RESULT=$(tail -50 "$REPORT_FILE" | grep -A20 "OLTP Read-Only" || true)
READONLY_TPS=$(echo "$READONLY_RESULT" | grep "transactions:" | awk '{print $3}' | tr -d '(')
READONLY_QPS=$(echo "$READONLY_RESULT" | grep "queries:" | awk '{print $3}' | tr -d '(')
READONLY_LATENCY=$(echo "$READONLY_RESULT" | grep "avg:" | head -1 | awk '{print $2}')

# Run OLTP Read-Write test
run_sysbench_test "OLTP Read-Write" "oltp_read_write"
READWRITE_RESULT=$(tail -50 "$REPORT_FILE" | grep -A20 "OLTP Read-Write" || true)
READWRITE_TPS=$(echo "$READWRITE_RESULT" | grep "transactions:" | awk '{print $3}' | tr -d '(')
READWRITE_QPS=$(echo "$READWRITE_RESULT" | grep "queries:" | awk '{print $3}' | tr -d '(')
READWRITE_LATENCY=$(echo "$READWRITE_RESULT" | grep "avg:" | head -1 | awk '{print $2}')

# Run OLTP Write-Only test
run_sysbench_test "OLTP Write-Only" "oltp_write_only"
WRITEONLY_RESULT=$(tail -50 "$REPORT_FILE" | grep -A20 "OLTP Write-Only" || true)
WRITEONLY_TPS=$(echo "$WRITEONLY_RESULT" | grep "transactions:" | awk '{print $3}' | tr -d '(')
WRITEONLY_QPS=$(echo "$WRITEONLY_RESULT" | grep "queries:" | awk '{print $3}' | tr -d '(')
WRITEONLY_LATENCY=$(echo "$WRITEONLY_RESULT" | grep "avg:" | head -1 | awk '{print $2}')

# Run Point Select test
run_sysbench_test "Point Select" "oltp_point_select"
POINTSELECT_RESULT=$(tail -50 "$REPORT_FILE" | grep -A20 "Point Select" || true)
POINTSELECT_QPS=$(echo "$POINTSELECT_RESULT" | grep "queries:" | awk '{print $3}' | tr -d '(')
POINTSELECT_LATENCY=$(echo "$POINTSELECT_RESULT" | grep "avg:" | head -1 | awk '{print $2}')

# Run Update Index test
run_sysbench_test "Update Index" "oltp_update_index"
UPDATEINDEX_RESULT=$(tail -50 "$REPORT_FILE" | grep -A20 "Update Index" || true)
UPDATEINDEX_QPS=$(echo "$UPDATEINDEX_RESULT" | grep "queries:" | awk '{print $3}' | tr -d '(')
UPDATEINDEX_LATENCY=$(echo "$UPDATEINDEX_RESULT" | grep "avg:" | head -1 | awk '{print $2}')

# Cleanup
log "Cleaning up..."
sysbench oltp_read_write $SYSBENCH_COMMON \
    --tables=$TABLES \
    cleanup 2>&1 || true

# Generate JSON report
cat > "$JSON_REPORT" <<EOF
{
    "benchmark": "sysbench",
    "protocol": "mysql",
    "timestamp": "$(date -Iseconds)",
    "target": {
        "host": "$HOST",
        "port": $PORT
    },
    "configuration": {
        "tables": $TABLES,
        "table_size": $TABLE_SIZE,
        "threads": $THREADS,
        "duration_seconds": $DURATION
    },
    "results": {
        "preparation_time_seconds": $PREP_TIME,
        "oltp_read_only": {
            "tps": ${READONLY_TPS:-0},
            "qps": ${READONLY_QPS:-0},
            "latency_avg_ms": ${READONLY_LATENCY:-0}
        },
        "oltp_read_write": {
            "tps": ${READWRITE_TPS:-0},
            "qps": ${READWRITE_QPS:-0},
            "latency_avg_ms": ${READWRITE_LATENCY:-0}
        },
        "oltp_write_only": {
            "tps": ${WRITEONLY_TPS:-0},
            "qps": ${WRITEONLY_QPS:-0},
            "latency_avg_ms": ${WRITEONLY_LATENCY:-0}
        },
        "point_select": {
            "qps": ${POINTSELECT_QPS:-0},
            "latency_avg_ms": ${POINTSELECT_LATENCY:-0}
        },
        "update_index": {
            "qps": ${UPDATEINDEX_QPS:-0},
            "latency_avg_ms": ${UPDATEINDEX_LATENCY:-0}
        }
    }
}
EOF

# Summary
{
    echo "=============================================="
    echo "SUMMARY"
    echo "=============================================="
    echo "OLTP Read-Only TPS:   ${READONLY_TPS:-N/A}"
    echo "OLTP Read-Write TPS:  ${READWRITE_TPS:-N/A}"
    echo "OLTP Write-Only TPS:  ${WRITEONLY_TPS:-N/A}"
    echo "Point Select QPS:     ${POINTSELECT_QPS:-N/A}"
    echo "Update Index QPS:     ${UPDATEINDEX_QPS:-N/A}"
    echo "=============================================="
} >> "$REPORT_FILE"

log "Benchmark complete!"
log "Text Report: $REPORT_FILE"
log "JSON Report: $JSON_REPORT"

cat "$REPORT_FILE" | tail -12
