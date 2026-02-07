#!/bin/bash
# ThunderDB Comprehensive Load Test Suite
# ========================================
#
# This script runs comprehensive load tests against ThunderDB including:
# - High concurrency testing (up to 128 parallel clients)
# - Large dataset tests (100K+ rows)
# - Latency percentile measurements (p50, p95, p99)
# - Sustained load testing (1+ hours)
# - Resource utilization monitoring
#
# Usage: ./run-load-test.sh [OPTIONS]
#
# Options:
#   --quick         Run quick tests only (5 minutes)
#   --sustained     Run sustained load test (1 hour)
#   --full          Run all tests including sustained (default)
#   --concurrency   Test concurrency only
#   --dataset       Test dataset scaling only
#   --help          Show this help message

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

# Parse command line arguments
TEST_MODE="full"
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            TEST_MODE="quick"
            export TEST_DURATION=60
            export CONCURRENCY_LEVELS="1 4 16"
            export DATASET_SIZES="1000 10000"
            shift
            ;;
        --sustained)
            TEST_MODE="sustained"
            shift
            ;;
        --full)
            TEST_MODE="full"
            shift
            ;;
        --concurrency)
            TEST_MODE="concurrency"
            shift
            ;;
        --dataset)
            TEST_MODE="dataset"
            shift
            ;;
        --help)
            head -25 "$0" | tail -20
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Banner
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           ThunderDB Comprehensive Load Test Suite              ║"
echo "╠════════════════════════════════════════════════════════════════╣"
echo "║  Mode: $(printf '%-54s' "$TEST_MODE") ║"
echo "║  Host: $(printf '%-54s' "$THUNDERDB_HOST:$THUNDERDB_PG_PORT") ║"
echo "║  Duration: $(printf '%-50s' "${TEST_DURATION}s per test") ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check dependencies
log_info "Checking dependencies..."
DEPS_OK=true
for cmd in psql bc awk jq; do
    if ! check_command "$cmd"; then
        DEPS_OK=false
    fi
done

# Optional dependencies
for cmd in pgbench gnuplot; do
    if ! command -v "$cmd" &> /dev/null; then
        log_warn "Optional command '$cmd' not found. Some tests may be skipped."
    fi
done

if [ "$DEPS_OK" = false ]; then
    log_error "Missing required dependencies. Exiting."
    exit 1
fi

# Setup
setup_results_dir

# Start resource monitoring in background
log_info "Starting resource monitoring..."
"$SCRIPT_DIR/monitor-resources.sh" &
MONITOR_PID=$!
trap "kill $MONITOR_PID 2>/dev/null || true" EXIT

# Wait for database to be ready
log_info "Checking database connectivity..."
MAX_RETRIES=30
RETRY=0
while ! PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "SELECT 1" > /dev/null 2>&1; do
    RETRY=$((RETRY + 1))
    if [ $RETRY -ge $MAX_RETRIES ]; then
        log_error "Could not connect to database after $MAX_RETRIES attempts"
        exit 1
    fi
    log_warn "Waiting for database... (attempt $RETRY/$MAX_RETRIES)"
    sleep 2
done
log_success "Database connection established"

# Initialize test tables
log_info "Initializing test tables..."
"$SCRIPT_DIR/init-test-tables.sh"

# Run tests based on mode
case $TEST_MODE in
    quick)
        log_info "Running quick tests..."
        "$SCRIPT_DIR/test-concurrency.sh"
        "$SCRIPT_DIR/test-latency.sh"
        ;;
    concurrency)
        log_info "Running concurrency tests..."
        "$SCRIPT_DIR/test-concurrency.sh"
        ;;
    dataset)
        log_info "Running dataset scaling tests..."
        "$SCRIPT_DIR/test-dataset-scaling.sh"
        ;;
    sustained)
        log_info "Running sustained load test..."
        "$SCRIPT_DIR/test-sustained.sh"
        ;;
    full)
        log_info "Running full test suite..."

        # Phase 1: Concurrency tests
        log_info "Phase 1/4: Concurrency scaling tests"
        "$SCRIPT_DIR/test-concurrency.sh"

        # Phase 2: Dataset scaling tests
        log_info "Phase 2/4: Dataset scaling tests"
        "$SCRIPT_DIR/test-dataset-scaling.sh"

        # Phase 3: Latency analysis
        log_info "Phase 3/4: Latency analysis"
        "$SCRIPT_DIR/test-latency.sh"

        # Phase 4: Sustained load (optional, takes 1 hour)
        read -p "Run sustained load test (1 hour)? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            log_info "Phase 4/4: Sustained load test"
            "$SCRIPT_DIR/test-sustained.sh"
        else
            log_info "Skipping sustained load test"
        fi
        ;;
esac

# Stop monitoring
kill $MONITOR_PID 2>/dev/null || true

# Generate reports
log_info "Generating reports..."
"$SCRIPT_DIR/generate-report.sh"

# Summary
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                     Load Test Complete                          ║"
echo "╠════════════════════════════════════════════════════════════════╣"
echo "║  Results saved to: $(printf '%-42s' "$RUN_DIR") ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

log_success "All tests completed successfully!"
