#!/bin/bash
# Latency Percentile Analysis Test
# =================================
# Detailed latency measurements with p50, p95, p99, p99.9 percentiles

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

OUTPUT_FILE="$RUN_DIR/raw/latency_results.csv"
HISTOGRAM_FILE="$RUN_DIR/raw/latency_histogram.txt"
DETAILED_DIR="$RUN_DIR/raw/latency_detailed"
mkdir -p "$DETAILED_DIR"

log_info "Starting latency analysis tests..."

# Number of samples to collect
NUM_SAMPLES="${NUM_SAMPLES:-10000}"
CONCURRENT_CLIENTS="${CONCURRENT_CLIENTS:-16}"

# CSV header
echo "query_type,samples,min_ms,max_ms,avg_ms,stddev_ms,p50_ms,p90_ms,p95_ms,p99_ms,p999_ms" > "$OUTPUT_FILE"

# Query types to test
declare -A QUERIES
QUERIES["point_select"]="SELECT balance FROM load_test_accounts WHERE id = \$ID"
QUERIES["range_select"]="SELECT * FROM load_test_accounts WHERE id BETWEEN \$START AND \$END LIMIT 100"
QUERIES["insert"]="INSERT INTO load_test_history (id, account_id, delta, mtime) VALUES (\$ID, \$ACCOUNT, 100, \$TIME)"
QUERIES["update"]="UPDATE load_test_accounts SET balance = balance + 1 WHERE id = \$ID"
QUERIES["aggregate"]="SELECT COUNT(*), SUM(balance), AVG(balance) FROM load_test_accounts WHERE id <= 1000"

# Function to run latency test for a specific query type
run_latency_test() {
    local query_type=$1
    local query_template="${QUERIES[$query_type]}"
    local output_file="$DETAILED_DIR/${query_type}_latencies.txt"
    local samples_per_client=$((NUM_SAMPLES / CONCURRENT_CLIENTS))

    log_info "  Testing $query_type (${NUM_SAMPLES} samples, ${CONCURRENT_CLIENTS} clients)..."

    > "$output_file"

    # Run concurrent clients
    for client_id in $(seq 1 $CONCURRENT_CLIENTS); do
        (
            for i in $(seq 1 $samples_per_client); do
                # Generate query with random parameters
                local id=$((RANDOM % 10000 + 1))
                local start=$((RANDOM % 5000 + 1))
                local end=$((start + 100))
                local account=$((RANDOM % 10000 + 1))
                local time=$(date +%s)
                local history_id=$((client_id * 1000000 + i))

                local query="${query_template}"
                query="${query//\$ID/$id}"
                query="${query//\$START/$start}"
                query="${query//\$END/$end}"
                query="${query//\$ACCOUNT/$account}"
                query="${query//\$TIME/$time}"

                # Measure latency
                local start_ns=$(date +%s%N)
                PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "$query" > /dev/null 2>&1
                local end_ns=$(date +%s%N)

                local latency_us=$(( (end_ns - start_ns) / 1000 ))
                local latency_ms=$(echo "scale=3; $latency_us / 1000" | bc)
                echo "$latency_ms"
            done
        ) >> "$output_file" &
    done

    wait

    # Calculate statistics
    if [ -s "$output_file" ]; then
        sort -n "$output_file" > "${output_file}.sorted"
        local count=$(wc -l < "${output_file}.sorted")

        local min=$(head -1 "${output_file}.sorted")
        local max=$(tail -1 "${output_file}.sorted")
        local avg=$(awk '{ sum += $1 } END { printf "%.3f", sum/NR }' "${output_file}.sorted")
        local stddev=$(awk -v avg="$avg" '{ sum += ($1 - avg)^2 } END { printf "%.3f", sqrt(sum/NR) }' "${output_file}.sorted")

        local p50=$(calculate_percentile "${output_file}.sorted" 50)
        local p90=$(calculate_percentile "${output_file}.sorted" 90)
        local p95=$(calculate_percentile "${output_file}.sorted" 95)
        local p99=$(calculate_percentile "${output_file}.sorted" 99)
        local p999_idx=$(echo "scale=0; $count * 999 / 1000" | bc)
        [ "$p999_idx" -lt 1 ] && p999_idx=1
        local p999=$(sed -n "${p999_idx}p" "${output_file}.sorted")

        echo "$query_type,$count,$min,$max,$avg,$stddev,$p50,$p90,$p95,$p99,$p999" >> "$OUTPUT_FILE"

        log_info "    avg=${avg}ms, p50=${p50}ms, p95=${p95}ms, p99=${p99}ms, p99.9=${p999}ms"

        # Generate histogram data
        echo "# Histogram for $query_type" >> "$HISTOGRAM_FILE"
        awk '
        BEGIN {
            buckets[0] = 0; buckets[1] = 0; buckets[2] = 0; buckets[5] = 0;
            buckets[10] = 0; buckets[20] = 0; buckets[50] = 0; buckets[100] = 0;
            buckets[200] = 0; buckets[500] = 0; buckets[1000] = 0; buckets[9999] = 0;
        }
        {
            if ($1 < 1) buckets[0]++;
            else if ($1 < 2) buckets[1]++;
            else if ($1 < 5) buckets[2]++;
            else if ($1 < 10) buckets[5]++;
            else if ($1 < 20) buckets[10]++;
            else if ($1 < 50) buckets[20]++;
            else if ($1 < 100) buckets[50]++;
            else if ($1 < 200) buckets[100]++;
            else if ($1 < 500) buckets[200]++;
            else if ($1 < 1000) buckets[500]++;
            else buckets[1000]++;
        }
        END {
            print "<1ms:", buckets[0];
            print "1-2ms:", buckets[1];
            print "2-5ms:", buckets[2];
            print "5-10ms:", buckets[5];
            print "10-20ms:", buckets[10];
            print "20-50ms:", buckets[20];
            print "50-100ms:", buckets[50];
            print "100-200ms:", buckets[100];
            print "200-500ms:", buckets[200];
            print "500-1000ms:", buckets[500];
            print ">1000ms:", buckets[1000];
        }
        ' "${output_file}.sorted" >> "$HISTOGRAM_FILE"
        echo "" >> "$HISTOGRAM_FILE"

        rm -f "${output_file}.sorted"
    fi
}

# Ensure we have test data
log_info "Ensuring test data exists..."
row_count=$(PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -t -c "SELECT COUNT(*) FROM load_test_accounts" 2>/dev/null | tr -d ' ')
if [ "$row_count" -lt 10000 ]; then
    log_info "  Inserting test data..."
    "$SCRIPT_DIR/init-test-tables.sh"
fi

# Clear history table for insert tests
PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "DELETE FROM load_test_history" > /dev/null 2>&1 || true

# Initialize histogram file
> "$HISTOGRAM_FILE"

# Run tests for each query type
for query_type in "${!QUERIES[@]}"; do
    run_latency_test "$query_type"
done

log_success "Latency analysis completed. Results saved to $OUTPUT_FILE"

# Generate summary
echo ""
echo "Latency Percentile Summary (all times in milliseconds)"
echo "======================================================="
column -t -s',' "$OUTPUT_FILE"

echo ""
echo "Latency Distribution Histogram"
echo "==============================="
cat "$HISTOGRAM_FILE"

# Check for concerning patterns
echo ""
echo "Analysis"
echo "========"
awk -F',' 'NR > 1 {
    if ($10 > 100) {
        print "WARNING: " $1 " has P99 > 100ms (" $10 "ms)"
    }
    if ($11 > 500) {
        print "WARNING: " $1 " has P99.9 > 500ms (" $11 "ms)"
    }
    tail_ratio = $10 / $7;
    if (tail_ratio > 5) {
        print "WARNING: " $1 " has high tail latency ratio (P99/P50 = " tail_ratio ")"
    }
}' "$OUTPUT_FILE"
