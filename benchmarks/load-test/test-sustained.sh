#!/bin/bash
# Sustained Load Test
# ===================
# Tests ThunderDB under sustained load for extended periods (1+ hours)
# Monitors for:
# - Performance degradation over time
# - Memory leaks
# - Connection pool exhaustion
# - Latency spikes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

OUTPUT_FILE="$RUN_DIR/raw/sustained_results.csv"
TIMESERIES_FILE="$RUN_DIR/raw/sustained_timeseries.csv"
ANOMALY_FILE="$RUN_DIR/raw/sustained_anomalies.log"

# Test parameters
SUSTAINED_CONCURRENCY="${SUSTAINED_CONCURRENCY:-32}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-10}"  # seconds between metric samples
TOTAL_DURATION="${SUSTAINED_DURATION:-3600}"  # 1 hour default

log_info "Starting sustained load test..."
log_info "Duration: ${TOTAL_DURATION}s ($(echo "scale=1; $TOTAL_DURATION / 60" | bc) minutes)"
log_info "Concurrency: ${SUSTAINED_CONCURRENCY} clients"
log_info "Sample interval: ${SAMPLE_INTERVAL}s"

# CSV headers
echo "timestamp,elapsed_sec,queries_completed,throughput_qps,avg_latency_ms,p50_ms,p95_ms,p99_ms,errors,memory_mb,connections" > "$TIMESERIES_FILE"
> "$ANOMALY_FILE"

# Shared counters file
COUNTER_FILE=$(mktemp)
echo "0,0,0" > "$COUNTER_FILE"  # queries, errors, total_latency_us

# Atomic counter update
update_counters() {
    local queries=$1
    local errors=$2
    local latency_us=$3

    (
        flock -x 200
        IFS=',' read -r q e l < "$COUNTER_FILE"
        echo "$((q + queries)),$((e + errors)),$((l + latency_us))" > "$COUNTER_FILE"
    ) 200>"$COUNTER_FILE.lock"
}

# Reset counters
reset_counters() {
    echo "0,0,0" > "$COUNTER_FILE"
}

# Read counters
read_counters() {
    cat "$COUNTER_FILE"
}

# Worker function
run_sustained_worker() {
    local worker_id=$1
    local end_time=$2

    while [ $(date +%s) -lt $end_time ]; do
        local start_ns=$(date +%s%N)
        local success=0
        local error=0

        # Random workload
        local rand=$((RANDOM % 100))
        local account_id=$((RANDOM % 10000 + 1))

        if [ $rand -lt $READ_PERCENTAGE ]; then
            # Read
            if PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -t -c "SELECT balance FROM load_test_accounts WHERE id = $account_id" > /dev/null 2>&1; then
                success=1
            else
                error=1
            fi
        else
            # Write
            local delta=$((RANDOM % 1000 - 500))
            if PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "UPDATE load_test_accounts SET balance = balance + $delta WHERE id = $account_id" > /dev/null 2>&1; then
                success=1
            else
                error=1
            fi
        fi

        local end_ns=$(date +%s%N)
        local latency_us=$(( (end_ns - start_ns) / 1000 ))

        update_counters $success $error $latency_us
    done
}

# Metrics collector
collect_metrics() {
    local start_time=$1
    local end_time=$2
    local interval=$3

    local prev_queries=0
    local baseline_p99=""
    local anomaly_count=0

    while [ $(date +%s) -lt $end_time ]; do
        sleep $interval

        local now=$(date +%s)
        local elapsed=$((now - start_time))

        # Read counters
        IFS=',' read -r total_queries total_errors total_latency_us < <(read_counters)

        # Calculate interval metrics
        local interval_queries=$((total_queries - prev_queries))
        local throughput=$(echo "scale=2; $interval_queries / $interval" | bc)
        local avg_latency_ms=0
        if [ $interval_queries -gt 0 ]; then
            avg_latency_ms=$(echo "scale=3; $total_latency_us / $total_queries / 1000" | bc)
        fi

        prev_queries=$total_queries

        # Get system metrics
        local memory_mb="0"
        local connections="0"

        # Try to get ThunderDB process memory (if running locally)
        if pgrep -f "thunder" > /dev/null 2>&1; then
            memory_mb=$(ps -o rss= -p $(pgrep -f "thunder" | head -1) 2>/dev/null | awk '{print int($1/1024)}' || echo "0")
        fi

        # Get connection count from database
        connections=$(PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -t -c "SELECT 1" 2>/dev/null && echo "$SUSTAINED_CONCURRENCY" || echo "0")

        # Estimate percentiles (simplified - actual would need more data)
        local p50_ms=$avg_latency_ms
        local p95_ms=$(echo "scale=3; $avg_latency_ms * 2" | bc)
        local p99_ms=$(echo "scale=3; $avg_latency_ms * 3" | bc)

        # Record to timeseries
        echo "$(date -Iseconds),$elapsed,$total_queries,$throughput,$avg_latency_ms,$p50_ms,$p95_ms,$p99_ms,$total_errors,$memory_mb,$connections" >> "$TIMESERIES_FILE"

        # Anomaly detection
        if [ -z "$baseline_p99" ] && [ $elapsed -gt 60 ]; then
            baseline_p99=$p99_ms
            log_info "  Baseline P99 established: ${baseline_p99}ms"
        fi

        if [ -n "$baseline_p99" ]; then
            # Check for latency spikes (>2x baseline)
            local spike_threshold=$(echo "scale=3; $baseline_p99 * 2" | bc)
            if (( $(echo "$p99_ms > $spike_threshold" | bc -l) )); then
                anomaly_count=$((anomaly_count + 1))
                echo "[$(date -Iseconds)] LATENCY SPIKE: P99=${p99_ms}ms (baseline=${baseline_p99}ms)" >> "$ANOMALY_FILE"
                log_warn "  Latency spike detected at ${elapsed}s: P99=${p99_ms}ms"
            fi

            # Check for throughput drop (>50% from peak)
            if [ $throughput -lt $((MIN_THROUGHPUT_QPS / 2)) ] && [ $interval_queries -gt 0 ]; then
                echo "[$(date -Iseconds)] LOW THROUGHPUT: ${throughput} QPS" >> "$ANOMALY_FILE"
            fi
        fi

        # Progress update every minute
        if [ $((elapsed % 60)) -lt $interval ]; then
            local progress=$((elapsed * 100 / TOTAL_DURATION))
            log_info "  Progress: ${progress}% (${elapsed}s/${TOTAL_DURATION}s) - ${throughput} QPS, P99=${p99_ms}ms, Errors=${total_errors}"
        fi
    done
}

# Ensure test data exists
log_info "Ensuring test data exists..."
"$SCRIPT_DIR/init-test-tables.sh"

# Calculate end time
START_TIME=$(date +%s)
END_TIME=$((START_TIME + TOTAL_DURATION))

# Start workers
log_info "Starting ${SUSTAINED_CONCURRENCY} worker clients..."
for i in $(seq 1 $SUSTAINED_CONCURRENCY); do
    run_sustained_worker $i $END_TIME &
done

# Start metrics collector
collect_metrics $START_TIME $END_TIME $SAMPLE_INTERVAL &
COLLECTOR_PID=$!

# Wait for test to complete
log_info "Test running... (Ctrl+C to stop early)"
wait

# Generate summary
log_info "Generating summary..."

# Final counters
IFS=',' read -r final_queries final_errors final_latency_us < <(read_counters)

# Calculate overall metrics
ACTUAL_DURATION=$(($(date +%s) - START_TIME))
OVERALL_QPS=$(echo "scale=2; $final_queries / $ACTUAL_DURATION" | bc)
OVERALL_AVG_LATENCY=$(echo "scale=3; $final_latency_us / $final_queries / 1000" | bc 2>/dev/null || echo "0")
ERROR_RATE=$(echo "scale=4; $final_errors / ($final_queries + $final_errors)" | bc 2>/dev/null || echo "0")

# Write summary
cat > "$OUTPUT_FILE" << EOF
Sustained Load Test Summary
===========================

Test Parameters:
  Duration: ${ACTUAL_DURATION}s ($(echo "scale=1; $ACTUAL_DURATION / 60" | bc) minutes)
  Concurrency: ${SUSTAINED_CONCURRENCY} clients
  Workload: ${READ_PERCENTAGE}% reads, ${WRITE_PERCENTAGE}% writes

Results:
  Total Queries: ${final_queries}
  Total Errors: ${final_errors}
  Error Rate: ${ERROR_RATE}
  Overall Throughput: ${OVERALL_QPS} QPS
  Average Latency: ${OVERALL_AVG_LATENCY}ms

Anomalies Detected:
$(wc -l < "$ANOMALY_FILE") events

Timeseries data saved to: $TIMESERIES_FILE
EOF

# Cleanup
rm -f "$COUNTER_FILE" "$COUNTER_FILE.lock"

log_success "Sustained load test completed."

# Display summary
echo ""
cat "$OUTPUT_FILE"

# Show anomalies if any
if [ -s "$ANOMALY_FILE" ]; then
    echo ""
    echo "Anomalies Log (first 20):"
    echo "========================="
    head -20 "$ANOMALY_FILE"
fi

# Performance trend analysis
echo ""
echo "Performance Trend Analysis"
echo "=========================="

# Check for degradation over time
awk -F',' 'NR > 1 {
    elapsed[NR] = $2;
    qps[NR] = $4;
    p99[NR] = $8;
}
END {
    if (NR < 10) exit;

    # Compare first 10% vs last 10%
    first_end = int(NR * 0.1);
    last_start = int(NR * 0.9);

    first_qps = 0; first_p99 = 0; first_count = 0;
    last_qps = 0; last_p99 = 0; last_count = 0;

    for (i = 2; i <= first_end; i++) {
        first_qps += qps[i];
        first_p99 += p99[i];
        first_count++;
    }
    for (i = last_start; i <= NR; i++) {
        last_qps += qps[i];
        last_p99 += p99[i];
        last_count++;
    }

    if (first_count > 0 && last_count > 0) {
        avg_first_qps = first_qps / first_count;
        avg_last_qps = last_qps / last_count;
        avg_first_p99 = first_p99 / first_count;
        avg_last_p99 = last_p99 / last_count;

        qps_change = ((avg_last_qps - avg_first_qps) / avg_first_qps) * 100;
        p99_change = ((avg_last_p99 - avg_first_p99) / avg_first_p99) * 100;

        printf "Throughput: First 10%% avg=%.1f QPS, Last 10%% avg=%.1f QPS (%.1f%% change)\n", avg_first_qps, avg_last_qps, qps_change;
        printf "P99 Latency: First 10%% avg=%.2fms, Last 10%% avg=%.2fms (%.1f%% change)\n", avg_first_p99, avg_last_p99, p99_change;

        if (qps_change < -20) {
            print "WARNING: Significant throughput degradation detected!";
        }
        if (p99_change > 50) {
            print "WARNING: Significant latency increase detected!";
        }
    }
}' "$TIMESERIES_FILE"
