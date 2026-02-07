#!/bin/bash
# Redis Protocol (RESP) Benchmark using redis-benchmark
# Benchmarks ThunderDB's Redis-compatible interface

set -e

# Configuration
HOST="${THUNDERDB_HOST:-thunderdb.thunderdb.svc.cluster.local}"
PORT="${THUNDERDB_RESP_PORT:-6379}"
PASSWORD="${THUNDERDB_PASSWORD:-}"
REPORT_DIR="${REPORT_DIR:-/reports}"
CLIENTS="${REDIS_CLIENTS:-50}"
REQUESTS="${REDIS_REQUESTS:-100000}"
DATA_SIZE="${REDIS_DATA_SIZE:-256}"
KEYSPACE="${REDIS_KEYSPACE:-100000}"
PIPELINE="${REDIS_PIPELINE:-1}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"; }

REPORT_FILE="${REPORT_DIR}/redis_report_$(date +%Y%m%d_%H%M%S).txt"
JSON_REPORT="${REPORT_DIR}/redis_report_$(date +%Y%m%d_%H%M%S).json"
CSV_REPORT="${REPORT_DIR}/redis_report_$(date +%Y%m%d_%H%M%S).csv"

# Auth flag
AUTH_FLAG=""
if [ -n "$PASSWORD" ]; then
    AUTH_FLAG="-a $PASSWORD"
fi

log "Starting Redis Protocol (RESP) Benchmark"
log "Target: $HOST:$PORT"
log "Configuration: clients=$CLIENTS, requests=$REQUESTS, data_size=${DATA_SIZE}bytes, pipeline=$PIPELINE"

# Create report header
{
    echo "=============================================="
    echo "ThunderDB Redis Protocol (RESP) Benchmark Report"
    echo "=============================================="
    echo "Date: $(date)"
    echo "Host: $HOST:$PORT"
    echo "Clients: $CLIENTS"
    echo "Requests: $REQUESTS"
    echo "Data Size: ${DATA_SIZE} bytes"
    echo "Pipeline: $PIPELINE"
    echo "Keyspace: $KEYSPACE keys"
    echo "=============================================="
    echo ""
} > "$REPORT_FILE"

# Check connectivity
log "Checking connectivity..."
if ! redis-cli -h "$HOST" -p "$PORT" $AUTH_FLAG PING 2>/dev/null | grep -q "PONG"; then
    error "Cannot connect to ThunderDB RESP interface at $HOST:$PORT"
    exit 1
fi
log "Connection successful"

# Run comprehensive benchmark with CSV output
log "Running comprehensive benchmark..."
BENCHMARK_START=$(date +%s.%N)

redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG \
    -c "$CLIENTS" \
    -n "$REQUESTS" \
    -d "$DATA_SIZE" \
    -r "$KEYSPACE" \
    -P "$PIPELINE" \
    --csv 2>/dev/null > "$CSV_REPORT"

BENCHMARK_END=$(date +%s.%N)
BENCHMARK_TIME=$(echo "$BENCHMARK_END - $BENCHMARK_START" | bc)

# Parse CSV results
{
    echo "=============================================="
    echo "Benchmark Results (CSV format)"
    echo "=============================================="
    cat "$CSV_REPORT"
    echo ""
} >> "$REPORT_FILE"

# Run individual tests for detailed output
log "Running individual command benchmarks..."

declare -A RESULTS

# PING benchmark
log "Testing PING..."
PING_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -t ping 2>&1 | grep "requests per second")
RESULTS["PING"]=$(echo "$PING_RESULT" | awk '{print $1}')
echo "PING: $PING_RESULT" >> "$REPORT_FILE"

# SET benchmark
log "Testing SET..."
SET_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -d "$DATA_SIZE" -r "$KEYSPACE" -t set 2>&1 | grep "requests per second")
RESULTS["SET"]=$(echo "$SET_RESULT" | awk '{print $1}')
echo "SET: $SET_RESULT" >> "$REPORT_FILE"

# GET benchmark
log "Testing GET..."
GET_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -r "$KEYSPACE" -t get 2>&1 | grep "requests per second")
RESULTS["GET"]=$(echo "$GET_RESULT" | awk '{print $1}')
echo "GET: $GET_RESULT" >> "$REPORT_FILE"

# INCR benchmark
log "Testing INCR..."
INCR_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -r "$KEYSPACE" -t incr 2>&1 | grep "requests per second")
RESULTS["INCR"]=$(echo "$INCR_RESULT" | awk '{print $1}')
echo "INCR: $INCR_RESULT" >> "$REPORT_FILE"

# LPUSH benchmark
log "Testing LPUSH..."
LPUSH_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -r "$KEYSPACE" -t lpush 2>&1 | grep "requests per second")
RESULTS["LPUSH"]=$(echo "$LPUSH_RESULT" | awk '{print $1}')
echo "LPUSH: $LPUSH_RESULT" >> "$REPORT_FILE"

# RPUSH benchmark
log "Testing RPUSH..."
RPUSH_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -r "$KEYSPACE" -t rpush 2>&1 | grep "requests per second")
RESULTS["RPUSH"]=$(echo "$RPUSH_RESULT" | awk '{print $1}')
echo "RPUSH: $RPUSH_RESULT" >> "$REPORT_FILE"

# LPOP benchmark
log "Testing LPOP..."
LPOP_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -r "$KEYSPACE" -t lpop 2>&1 | grep "requests per second")
RESULTS["LPOP"]=$(echo "$LPOP_RESULT" | awk '{print $1}')
echo "LPOP: $LPOP_RESULT" >> "$REPORT_FILE"

# SADD benchmark
log "Testing SADD..."
SADD_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -r "$KEYSPACE" -t sadd 2>&1 | grep "requests per second")
RESULTS["SADD"]=$(echo "$SADD_RESULT" | awk '{print $1}')
echo "SADD: $SADD_RESULT" >> "$REPORT_FILE"

# HSET benchmark
log "Testing HSET..."
HSET_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -r "$KEYSPACE" -t hset 2>&1 | grep "requests per second")
RESULTS["HSET"]=$(echo "$HSET_RESULT" | awk '{print $1}')
echo "HSET: $HSET_RESULT" >> "$REPORT_FILE"

# MSET benchmark (10 keys)
log "Testing MSET..."
MSET_RESULT=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -r "$KEYSPACE" -t mset 2>&1 | grep "requests per second")
RESULTS["MSET"]=$(echo "$MSET_RESULT" | awk '{print $1}')
echo "MSET: $MSET_RESULT" >> "$REPORT_FILE"

# Pipeline benchmark
if [ "$PIPELINE" -gt 1 ]; then
    log "Testing with pipeline=$PIPELINE..."
    {
        echo ""
        echo "=============================================="
        echo "Pipeline Benchmark (pipeline=$PIPELINE)"
        echo "=============================================="
    } >> "$REPORT_FILE"

    PIPELINE_SET=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -d "$DATA_SIZE" -r "$KEYSPACE" -P "$PIPELINE" -t set 2>&1 | grep "requests per second")
    PIPELINE_GET=$(redis-benchmark -h "$HOST" -p "$PORT" $AUTH_FLAG -c "$CLIENTS" -n "$REQUESTS" -r "$KEYSPACE" -P "$PIPELINE" -t get 2>&1 | grep "requests per second")

    echo "SET (pipeline): $PIPELINE_SET" >> "$REPORT_FILE"
    echo "GET (pipeline): $PIPELINE_GET" >> "$REPORT_FILE"
fi

# Generate JSON report
cat > "$JSON_REPORT" <<EOF
{
    "benchmark": "redis-benchmark",
    "protocol": "resp",
    "timestamp": "$(date -Iseconds)",
    "target": {
        "host": "$HOST",
        "port": $PORT
    },
    "configuration": {
        "clients": $CLIENTS,
        "requests": $REQUESTS,
        "data_size_bytes": $DATA_SIZE,
        "keyspace": $KEYSPACE,
        "pipeline": $PIPELINE
    },
    "results": {
        "total_time_seconds": $BENCHMARK_TIME,
        "commands": {
            "PING": ${RESULTS["PING"]:-0},
            "SET": ${RESULTS["SET"]:-0},
            "GET": ${RESULTS["GET"]:-0},
            "INCR": ${RESULTS["INCR"]:-0},
            "LPUSH": ${RESULTS["LPUSH"]:-0},
            "RPUSH": ${RESULTS["RPUSH"]:-0},
            "LPOP": ${RESULTS["LPOP"]:-0},
            "SADD": ${RESULTS["SADD"]:-0},
            "HSET": ${RESULTS["HSET"]:-0},
            "MSET": ${RESULTS["MSET"]:-0}
        }
    }
}
EOF

# Summary
{
    echo ""
    echo "=============================================="
    echo "SUMMARY (requests/second)"
    echo "=============================================="
    echo "PING:  ${RESULTS["PING"]:-N/A}"
    echo "SET:   ${RESULTS["SET"]:-N/A}"
    echo "GET:   ${RESULTS["GET"]:-N/A}"
    echo "INCR:  ${RESULTS["INCR"]:-N/A}"
    echo "LPUSH: ${RESULTS["LPUSH"]:-N/A}"
    echo "RPUSH: ${RESULTS["RPUSH"]:-N/A}"
    echo "LPOP:  ${RESULTS["LPOP"]:-N/A}"
    echo "SADD:  ${RESULTS["SADD"]:-N/A}"
    echo "HSET:  ${RESULTS["HSET"]:-N/A}"
    echo "MSET:  ${RESULTS["MSET"]:-N/A}"
    echo "=============================================="
    echo "Total benchmark time: ${BENCHMARK_TIME}s"
    echo "=============================================="
} >> "$REPORT_FILE"

log "Benchmark complete!"
log "Text Report: $REPORT_FILE"
log "JSON Report: $JSON_REPORT"
log "CSV Report: $CSV_REPORT"

cat "$REPORT_FILE" | tail -18
