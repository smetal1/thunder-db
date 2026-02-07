#!/bin/bash
set -euo pipefail
source /scripts/config.sh

OUTPUT_FILE="$RUN_DIR/raw/concurrency_results.csv"

log_info "Starting concurrency scaling tests..."
log_info "Concurrency levels: $CONCURRENCY_LEVELS"
log_info "Test duration: ${TEST_DURATION}s per level"

echo "concurrency,throughput_qps,avg_latency_ms,p50_latency_ms,p95_latency_ms,p99_latency_ms,errors,error_rate" > "$OUTPUT_FILE"

run_worker() {
    local duration=$1
    local latency_file=$2
    local end_time=$(($(date +%s) + duration))
    local queries=0 errors=0

    while [ $(date +%s) -lt $end_time ]; do
        local start_ms=$(date +%s%3N)
        local account_id=$((RANDOM % 100 + 1))

        if [ $((RANDOM % 100)) -lt $READ_PERCENTAGE ]; then
            if PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -t -c "SELECT balance FROM load_test_accounts WHERE id = $account_id" > /dev/null 2>&1; then
                queries=$((queries + 1))
                local end_ms=$(date +%s%3N)
                echo "$((end_ms - start_ms))" >> "$latency_file"
            else
                errors=$((errors + 1))
            fi
        else
            local delta=$((RANDOM % 1000 - 500))
            if PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "UPDATE load_test_accounts SET balance = balance + $delta WHERE id = $account_id" > /dev/null 2>&1; then
                queries=$((queries + 1))
                local end_ms=$(date +%s%3N)
                echo "$((end_ms - start_ms))" >> "$latency_file"
            else
                errors=$((errors + 1))
            fi
        fi
    done
    echo "$queries,$errors"
}

for concurrency in $CONCURRENCY_LEVELS; do
    log_info "Testing with $concurrency concurrent clients..."

    LATENCY_FILE=$(mktemp)
    > "$LATENCY_FILE"

    # Warmup
    log_info "  Warming up for ${WARMUP_DURATION}s..."
    for i in $(seq 1 $concurrency); do
        run_worker $WARMUP_DURATION /dev/null &
    done
    wait

    # Main test
    log_info "  Running test for ${TEST_DURATION}s..."
    start_time=$(date +%s)

    declare -a outputs
    for i in $(seq 1 $concurrency); do
        output=$(mktemp)
        outputs+=("$output")
        run_worker $TEST_DURATION "$LATENCY_FILE" > "$output" &
    done
    wait

    end_time=$(date +%s)
    duration=$((end_time - start_time))

    # Aggregate
    total_queries=0 total_errors=0
    for output in "${outputs[@]}"; do
        if [ -f "$output" ]; then
            IFS=',' read -r q e < "$output"
            total_queries=$((total_queries + ${q:-0}))
            total_errors=$((total_errors + ${e:-0}))
            rm -f "$output"
        fi
    done

    throughput=$(echo "scale=2; $total_queries / $duration" | bc)
    error_rate=$(echo "scale=4; $total_errors / ($total_queries + $total_errors + 1)" | bc)

    if [ -s "$LATENCY_FILE" ]; then
        sort -n "$LATENCY_FILE" > "${LATENCY_FILE}.sorted"
        avg=$(awk '{sum+=$1} END {printf "%.2f", sum/NR}' "${LATENCY_FILE}.sorted")
        p50=$(calculate_percentile "${LATENCY_FILE}.sorted" 50)
        p95=$(calculate_percentile "${LATENCY_FILE}.sorted" 95)
        p99=$(calculate_percentile "${LATENCY_FILE}.sorted" 99)
        rm -f "${LATENCY_FILE}.sorted"
    else
        avg=0 p50=0 p95=0 p99=0
    fi
    rm -f "$LATENCY_FILE"

    echo "$concurrency,$throughput,$avg,$p50,$p95,$p99,$total_errors,$error_rate" >> "$OUTPUT_FILE"
    log_info "  Results: ${throughput} QPS, p50=${p50}ms, p95=${p95}ms, p99=${p99}ms, errors=${total_errors}"

    unset outputs
done

log_success "Concurrency tests completed"
echo ""
echo "=== Concurrency Test Results ==="
column -t -s',' "$OUTPUT_FILE"
