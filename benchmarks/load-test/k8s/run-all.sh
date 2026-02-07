#!/bin/bash
set -euo pipefail
source /scripts/config.sh

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           ThunderDB Load Test Suite (Kubernetes)               ║"
echo "╠════════════════════════════════════════════════════════════════╣"
echo "║  Host: $THUNDERDB_HOST:$THUNDERDB_PG_PORT"
echo "║  User: $THUNDERDB_USER"
echo "║  Database: $THUNDERDB_DATABASE"
echo "║  Duration: ${TEST_DURATION}s per test"
echo "║  Concurrency levels: $CONCURRENCY_LEVELS"
echo "║  Latency samples: $NUM_SAMPLES"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

log_info "Testing connection to ThunderDB..."
retry=0
while ! test_connection; do
    retry=$((retry + 1))
    if [ $retry -gt 30 ]; then
        log_error "Failed to connect to ThunderDB after 30 attempts"
        log_error "Host: $THUNDERDB_HOST:$THUNDERDB_PG_PORT"
        log_error "User: $THUNDERDB_USER"
        exit 1
    fi
    log_warn "Connection failed, retrying in 2s... ($retry/30)"
    sleep 2
done
log_success "Connected to ThunderDB"

log_info "Initializing test tables..."
/scripts/init-tables.sh

echo ""
log_info "Running concurrency tests..."
/scripts/test-concurrency.sh

echo ""
log_info "Running latency tests..."
/scripts/test-latency.sh

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    Load Test Complete                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Results saved to: $RUN_DIR"
ls -la "$RUN_DIR/raw/" 2>/dev/null || true
