#!/bin/bash
# ThunderDB Load Test Configuration
# ==================================

# Database connection settings
export THUNDERDB_HOST="${THUNDERDB_HOST:-localhost}"
export THUNDERDB_PG_PORT="${THUNDERDB_PG_PORT:-5432}"
export THUNDERDB_MYSQL_PORT="${THUNDERDB_MYSQL_PORT:-3306}"
export THUNDERDB_REDIS_PORT="${THUNDERDB_REDIS_PORT:-6379}"
export THUNDERDB_USER="${THUNDERDB_USER:-postgres}"
export THUNDERDB_PASSWORD="${THUNDERDB_PASSWORD:-thunder}"
export THUNDERDB_DATABASE="${THUNDERDB_DATABASE:-thunder}"

# Load test parameters
export CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-1 4 8 16 32 64 128}"
export DATASET_SIZES="${DATASET_SIZES:-1000 10000 100000 500000}"
export TEST_DURATION="${TEST_DURATION:-300}"  # seconds (5 minutes default)
export SUSTAINED_DURATION="${SUSTAINED_DURATION:-3600}"  # 1 hour for sustained test
export WARMUP_DURATION="${WARMUP_DURATION:-30}"  # seconds

# Workload mix (percentages)
export READ_PERCENTAGE="${READ_PERCENTAGE:-80}"
export WRITE_PERCENTAGE="${WRITE_PERCENTAGE:-20}"

# Batch settings
export BATCH_SIZE="${BATCH_SIZE:-1000}"
export MAX_BATCH_SIZE="${MAX_BATCH_SIZE:-10000}"

# Monitoring settings
export MONITOR_INTERVAL="${MONITOR_INTERVAL:-5}"  # seconds
export COLLECT_FLAMEGRAPH="${COLLECT_FLAMEGRAPH:-false}"

# Output directories
export RESULTS_DIR="${RESULTS_DIR:-$(dirname "$0")/results}"
export TIMESTAMP=$(date +%Y%m%d_%H%M%S)
export RUN_DIR="${RESULTS_DIR}/run_${TIMESTAMP}"

# Thresholds for alerts
export P99_LATENCY_THRESHOLD_MS="${P99_LATENCY_THRESHOLD_MS:-100}"
export ERROR_RATE_THRESHOLD="${ERROR_RATE_THRESHOLD:-0.01}"  # 1%
export MIN_THROUGHPUT_QPS="${MIN_THROUGHPUT_QPS:-1000}"

# Colors for output
export RED='\033[0;31m'
export GREEN='\033[0;32m'
export YELLOW='\033[1;33m'
export BLUE='\033[0;34m'
export NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"
}

# Check if command exists
check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "Required command '$1' not found. Please install it."
        return 1
    fi
    return 0
}

# Create results directory
setup_results_dir() {
    mkdir -p "$RUN_DIR"/{raw,reports,metrics,logs}
    log_info "Results will be saved to: $RUN_DIR"
}

# Calculate percentiles from a sorted array
# Usage: calculate_percentile <sorted_values_file> <percentile>
calculate_percentile() {
    local file=$1
    local percentile=$2
    local count=$(wc -l < "$file")
    local index=$(echo "scale=0; $count * $percentile / 100" | bc)
    [ "$index" -lt 1 ] && index=1
    sed -n "${index}p" "$file"
}
