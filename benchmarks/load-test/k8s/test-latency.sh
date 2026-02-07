#!/bin/bash
set -euo pipefail
source /scripts/config.sh

OUTPUT_FILE="$RUN_DIR/raw/latency_results.csv"

log_info "Starting latency analysis..."
log_info "Samples per query type: $NUM_SAMPLES"

echo "query_type,samples,min_ms,max_ms,avg_ms,p50_ms,p95_ms,p99_ms" > "$OUTPUT_FILE"

run_latency_test() {
    local name=$1
    local query_template=$2
    local samples=$NUM_SAMPLES
    local latency_file=$(mktemp)

    log_info "  Testing $name ($samples samples)..."

    for i in $(seq 1 $samples); do
        local id=$((RANDOM % 100 + 1))
        local query="${query_template//\$ID/$id}"

        local start_ms=$(date +%s%3N)
        PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "$query" > /dev/null 2>&1
        local end_ms=$(date +%s%3N)
        echo "$((end_ms - start_ms))" >> "$latency_file"
    done

    sort -n "$latency_file" > "${latency_file}.sorted"
    local count=$(wc -l < "${latency_file}.sorted")
    local min=$(head -1 "${latency_file}.sorted")
    local max=$(tail -1 "${latency_file}.sorted")
    local avg=$(awk '{sum+=$1} END {printf "%.2f", sum/NR}' "${latency_file}.sorted")
    local p50=$(calculate_percentile "${latency_file}.sorted" 50)
    local p95=$(calculate_percentile "${latency_file}.sorted" 95)
    local p99=$(calculate_percentile "${latency_file}.sorted" 99)

    echo "$name,$count,$min,$max,$avg,$p50,$p95,$p99" >> "$OUTPUT_FILE"
    log_info "    avg=${avg}ms, p50=${p50}ms, p95=${p95}ms, p99=${p99}ms"

    rm -f "$latency_file" "${latency_file}.sorted"
}

run_latency_test "point_select" "SELECT balance FROM load_test_accounts WHERE id = \$ID"
run_latency_test "update" "UPDATE load_test_accounts SET balance = balance + 1 WHERE id = \$ID"
run_latency_test "aggregate" "SELECT COUNT(*), SUM(balance) FROM load_test_accounts WHERE id <= 100"

log_success "Latency analysis completed"
echo ""
echo "=== Latency Results ==="
column -t -s',' "$OUTPUT_FILE"
