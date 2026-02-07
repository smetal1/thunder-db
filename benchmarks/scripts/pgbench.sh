#!/bin/bash
# PostgreSQL Protocol Benchmark using pgbench
# Benchmarks ThunderDB's PostgreSQL-compatible interface

set -e

# Configuration
HOST="${THUNDERDB_HOST:-thunderdb.thunderdb.svc.cluster.local}"
PORT="${THUNDERDB_PG_PORT:-5432}"
USER="${THUNDERDB_USER:-admin}"
PASSWORD="${THUNDERDB_PASSWORD:-}"
DATABASE="${THUNDERDB_DATABASE:-benchmark}"
REPORT_DIR="${REPORT_DIR:-/reports}"
SCALE="${PGBENCH_SCALE:-10}"
CLIENTS="${PGBENCH_CLIENTS:-10}"
THREADS="${PGBENCH_THREADS:-2}"
DURATION="${PGBENCH_DURATION:-60}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"; }

# Export password for pgbench
export PGPASSWORD="$PASSWORD"

REPORT_FILE="${REPORT_DIR}/pgbench_report_$(date +%Y%m%d_%H%M%S).txt"
JSON_REPORT="${REPORT_DIR}/pgbench_report_$(date +%Y%m%d_%H%M%S).json"

log "Starting PostgreSQL Protocol Benchmark"
log "Target: $HOST:$PORT"
log "Configuration: scale=$SCALE, clients=$CLIENTS, threads=$THREADS, duration=${DURATION}s"

# Create report header
{
    echo "=============================================="
    echo "ThunderDB PostgreSQL Protocol Benchmark Report"
    echo "=============================================="
    echo "Date: $(date)"
    echo "Host: $HOST:$PORT"
    echo "Scale Factor: $SCALE"
    echo "Clients: $CLIENTS"
    echo "Threads: $THREADS"
    echo "Duration: ${DURATION}s"
    echo "=============================================="
    echo ""
} > "$REPORT_FILE"

# Check connectivity
log "Checking connectivity..."
if ! pg_isready -h "$HOST" -p "$PORT" -U "$USER" -t 10 > /dev/null 2>&1; then
    error "Cannot connect to ThunderDB at $HOST:$PORT"
    exit 1
fi
log "Connection successful"

# Create benchmark database
log "Setting up benchmark database..."
psql -h "$HOST" -p "$PORT" -U "$USER" -c "DROP DATABASE IF EXISTS $DATABASE;" 2>/dev/null || true
psql -h "$HOST" -p "$PORT" -U "$USER" -c "CREATE DATABASE $DATABASE;" 2>/dev/null || true

# Initialize pgbench tables
log "Initializing pgbench tables (scale factor: $SCALE)..."
INIT_START=$(date +%s.%N)
pgbench -h "$HOST" -p "$PORT" -U "$USER" -i -s "$SCALE" "$DATABASE" 2>&1 | tee -a "$REPORT_FILE"
INIT_END=$(date +%s.%N)
INIT_TIME=$(echo "$INIT_END - $INIT_START" | bc)

{
    echo ""
    echo "Initialization Time: ${INIT_TIME}s"
    echo ""
} >> "$REPORT_FILE"

# Run read-only benchmark
log "Running READ-ONLY benchmark..."
{
    echo "=============================================="
    echo "READ-ONLY Benchmark (SELECT only)"
    echo "=============================================="
} >> "$REPORT_FILE"

READ_RESULT=$(pgbench -h "$HOST" -p "$PORT" -U "$USER" -S -c "$CLIENTS" -j "$THREADS" -T "$DURATION" -P 5 "$DATABASE" 2>&1)
echo "$READ_RESULT" >> "$REPORT_FILE"
READ_TPS=$(echo "$READ_RESULT" | grep "tps = " | tail -1 | awk '{print $3}')
READ_LATENCY=$(echo "$READ_RESULT" | grep "latency average" | awk '{print $4}')

# Run read-write benchmark
log "Running READ-WRITE benchmark..."
{
    echo ""
    echo "=============================================="
    echo "READ-WRITE Benchmark (TPC-B like)"
    echo "=============================================="
} >> "$REPORT_FILE"

RW_RESULT=$(pgbench -h "$HOST" -p "$PORT" -U "$USER" -c "$CLIENTS" -j "$THREADS" -T "$DURATION" -P 5 "$DATABASE" 2>&1)
echo "$RW_RESULT" >> "$REPORT_FILE"
RW_TPS=$(echo "$RW_RESULT" | grep "tps = " | tail -1 | awk '{print $3}')
RW_LATENCY=$(echo "$RW_RESULT" | grep "latency average" | awk '{print $4}')

# Run simple update benchmark
log "Running SIMPLE UPDATE benchmark..."
{
    echo ""
    echo "=============================================="
    echo "SIMPLE UPDATE Benchmark"
    echo "=============================================="
} >> "$REPORT_FILE"

UPDATE_RESULT=$(pgbench -h "$HOST" -p "$PORT" -U "$USER" -N -c "$CLIENTS" -j "$THREADS" -T "$DURATION" -P 5 "$DATABASE" 2>&1)
echo "$UPDATE_RESULT" >> "$REPORT_FILE"
UPDATE_TPS=$(echo "$UPDATE_RESULT" | grep "tps = " | tail -1 | awk '{print $3}')
UPDATE_LATENCY=$(echo "$UPDATE_RESULT" | grep "latency average" | awk '{print $4}')

# Generate JSON report
cat > "$JSON_REPORT" <<EOF
{
    "benchmark": "pgbench",
    "protocol": "postgresql",
    "timestamp": "$(date -Iseconds)",
    "target": {
        "host": "$HOST",
        "port": $PORT
    },
    "configuration": {
        "scale_factor": $SCALE,
        "clients": $CLIENTS,
        "threads": $THREADS,
        "duration_seconds": $DURATION
    },
    "results": {
        "initialization_time_seconds": $INIT_TIME,
        "read_only": {
            "tps": ${READ_TPS:-0},
            "latency_avg_ms": ${READ_LATENCY:-0}
        },
        "read_write": {
            "tps": ${RW_TPS:-0},
            "latency_avg_ms": ${RW_LATENCY:-0}
        },
        "simple_update": {
            "tps": ${UPDATE_TPS:-0},
            "latency_avg_ms": ${UPDATE_LATENCY:-0}
        }
    }
}
EOF

# Summary
{
    echo ""
    echo "=============================================="
    echo "SUMMARY"
    echo "=============================================="
    echo "Read-Only TPS:      ${READ_TPS:-N/A}"
    echo "Read-Write TPS:     ${RW_TPS:-N/A}"
    echo "Simple Update TPS:  ${UPDATE_TPS:-N/A}"
    echo "=============================================="
} >> "$REPORT_FILE"

log "Benchmark complete!"
log "Text Report: $REPORT_FILE"
log "JSON Report: $JSON_REPORT"

# Print summary
cat "$REPORT_FILE" | tail -10
