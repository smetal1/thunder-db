#!/bin/bash
# Concurrency Scaling Test
# =========================
# Tests ThunderDB performance with increasing numbers of parallel clients

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

OUTPUT_FILE="$RUN_DIR/raw/concurrency_results.csv"
LATENCY_DIR="$RUN_DIR/raw/latencies"
mkdir -p "$LATENCY_DIR"

log_info "Starting concurrency scaling tests..."
log_info "Testing concurrency levels: $CONCURRENCY_LEVELS"
log_info "Test duration per level: ${TEST_DURATION}s"

# CSV header
echo "concurrency,throughput_qps,avg_latency_ms,p50_latency_ms,p95_latency_ms,p99_latency_ms,errors,error_rate" > "$OUTPUT_FILE"

# Worker function to execute queries
run_worker() {
    local worker_id=$1
    local duration=$2
    local output_file=$3
    local end_time=$(($(date +%s) + duration))

    local query_count=0
    local error_count=0

    while [ $(date +%s) -lt $end_time ]; do
        local start_ns=$(date +%s%N)

        # Random workload: 80% reads, 20% writes (based on config)
        local rand=$((RANDOM % 100))
        local success=0

        if [ $rand -lt $READ_PERCENTAGE ]; then
            # Read query - select random account
            local account_id=$((RANDOM % 10000 + 1))
            if PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -t -c "SELECT balance FROM load_test_accounts WHERE id = $account_id" > /dev/null 2>&1; then
                success=1
            fi
        else
            # Write query - update random account
            local account_id=$((RANDOM % 10000 + 1))
            local delta=$((RANDOM % 1000 - 500))
            if PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "UPDATE load_test_accounts SET balance = balance + $delta WHERE id = $account_id" > /dev/null 2>&1; then
                success=1
            fi
        fi

        local end_ns=$(date +%s%N)
        local latency_ns=$((end_ns - start_ns))
        local latency_ms=$(echo "scale=3; $latency_ns / 1000000" | bc)

        if [ $success -eq 1 ]; then
            echo "$latency_ms" >> "$output_file"
            query_count=$((query_count + 1))
        else
            error_count=$((error_count + 1))
        fi
    done

    echo "$query_count,$error_count"
}

# Run tests for each concurrency level
for concurrency in $CONCURRENCY_LEVELS; do
    log_info "Testing with $concurrency concurrent clients..."

    LATENCY_FILE="$LATENCY_DIR/concurrency_${concurrency}.txt"
    > "$LATENCY_FILE"

    # Warmup
    log_info "  Warming up for ${WARMUP_DURATION}s..."
    for i in $(seq 1 $concurrency); do
        run_worker $i $WARMUP_DURATION /dev/null &
    done
    wait

    # Main test
    log_info "  Running main test for ${TEST_DURATION}s..."
    start_time=$(date +%s)

    # Start workers
    declare -a worker_pids
    declare -a worker_outputs
    for i in $(seq 1 $concurrency); do
        worker_output=$(mktemp)
        worker_outputs+=("$worker_output")
        run_worker $i $TEST_DURATION "$LATENCY_FILE" > "$worker_output" &
        worker_pids+=($!)
    done

    # Wait for all workers
    for pid in "${worker_pids[@]}"; do
        wait $pid
    done

    end_time=$(date +%s)
    actual_duration=$((end_time - start_time))

    # Aggregate results
    total_queries=0
    total_errors=0
    for output in "${worker_outputs[@]}"; do
        if [ -f "$output" ]; then
            IFS=',' read -r queries errors < "$output"
            total_queries=$((total_queries + queries))
            total_errors=$((total_errors + errors))
            rm -f "$output"
        fi
    done

    # Calculate metrics
    throughput=$(echo "scale=2; $total_queries / $actual_duration" | bc)
    error_rate=$(echo "scale=4; $total_errors / ($total_queries + $total_errors)" | bc 2>/dev/null || echo "0")

    # Calculate latency percentiles
    if [ -s "$LATENCY_FILE" ]; then
        sort -n "$LATENCY_FILE" > "${LATENCY_FILE}.sorted"

        avg_latency=$(awk '{ sum += $1; count++ } END { printf "%.3f", sum/count }' "${LATENCY_FILE}.sorted")
        p50_latency=$(calculate_percentile "${LATENCY_FILE}.sorted" 50)
        p95_latency=$(calculate_percentile "${LATENCY_FILE}.sorted" 95)
        p99_latency=$(calculate_percentile "${LATENCY_FILE}.sorted" 99)

        rm -f "${LATENCY_FILE}.sorted"
    else
        avg_latency="0"
        p50_latency="0"
        p95_latency="0"
        p99_latency="0"
    fi

    # Record results
    echo "$concurrency,$throughput,$avg_latency,$p50_latency,$p95_latency,$p99_latency,$total_errors,$error_rate" >> "$OUTPUT_FILE"

    # Log summary
    log_info "  Results: ${throughput} QPS, p50=${p50_latency}ms, p95=${p95_latency}ms, p99=${p99_latency}ms"

    # Check thresholds
    if (( $(echo "$p99_latency > $P99_LATENCY_THRESHOLD_MS" | bc -l) )); then
        log_warn "  P99 latency exceeds threshold (${p99_latency}ms > ${P99_LATENCY_THRESHOLD_MS}ms)"
    fi
    if (( $(echo "$error_rate > $ERROR_RATE_THRESHOLD" | bc -l) )); then
        log_warn "  Error rate exceeds threshold (${error_rate} > ${ERROR_RATE_THRESHOLD})"
    fi

    unset worker_pids
    unset worker_outputs
done

log_success "Concurrency tests completed. Results saved to $OUTPUT_FILE"

# Generate quick summary
echo ""
echo "Concurrency Test Summary"
echo "========================"
column -t -s',' "$OUTPUT_FILE" | head -20
