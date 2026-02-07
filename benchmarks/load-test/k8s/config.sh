#!/bin/bash
# Load Test Configuration

export THUNDERDB_HOST="${THUNDERDB_HOST:-thunderdb}"
export THUNDERDB_PG_PORT="${THUNDERDB_PG_PORT:-5432}"
export THUNDERDB_USER="${THUNDERDB_USER:-admin}"
export THUNDERDB_PASSWORD="${THUNDERDB_PASSWORD:-thunder123}"
export THUNDERDB_DATABASE="${THUNDERDB_DATABASE:-default}"

# Test parameters
export CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-1 2 4 8 16 32}"
export DATASET_SIZES="${DATASET_SIZES:-1000 10000 50000}"
export TEST_DURATION="${TEST_DURATION:-60}"
export WARMUP_DURATION="${WARMUP_DURATION:-5}"
export READ_PERCENTAGE="${READ_PERCENTAGE:-80}"
export WRITE_PERCENTAGE="${WRITE_PERCENTAGE:-20}"
export NUM_SAMPLES="${NUM_SAMPLES:-1000}"

# Thresholds
export P99_LATENCY_THRESHOLD_MS="${P99_LATENCY_THRESHOLD_MS:-100}"
export ERROR_RATE_THRESHOLD="${ERROR_RATE_THRESHOLD:-0.01}"

# Output directory
export RUN_DIR="${RUN_DIR:-/tmp/load-test-results}"
mkdir -p "$RUN_DIR/raw" "$RUN_DIR/metrics" 2>/dev/null || true

# Logging functions
log_info() { echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_warn() { echo "[WARN] $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_error() { echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_success() { echo "[SUCCESS] $(date '+%Y-%m-%d %H:%M:%S') $*"; }

# Percentile calculation
calculate_percentile() {
    local file=$1
    local pct=$2
    local count=$(wc -l < "$file")
    local idx=$(echo "scale=0; $count * $pct / 100" | bc)
    [ "$idx" -lt 1 ] && idx=1
    sed -n "${idx}p" "$file"
}

# Connection test
test_connection() {
    PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "SELECT 1" > /dev/null 2>&1
}
