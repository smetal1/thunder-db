#!/bin/bash
# Load Test Report Generator
# ==========================
# Generates comprehensive reports from load test results

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

REPORT_DIR="${1:-$RUN_DIR}"
OUTPUT_FILE="$REPORT_DIR/load_test_report.md"
JSON_FILE="$REPORT_DIR/load_test_report.json"

log_info "Generating load test report from: $REPORT_DIR"

# Check if results exist
if [ ! -d "$REPORT_DIR/raw" ]; then
    log_error "No raw results found in $REPORT_DIR/raw"
    exit 1
fi

# ========================================
# Generate Markdown Report
# ========================================
cat > "$OUTPUT_FILE" << 'EOF'
# ThunderDB Load Test Report

Generated: $(date -Iseconds)

## Test Environment

EOF

# Add environment info
cat >> "$OUTPUT_FILE" << EOF
- **Host**: $(hostname)
- **ThunderDB Version**: ${THUNDERDB_VERSION:-unknown}
- **Test Date**: $(date)
- **Report Directory**: $REPORT_DIR

## Configuration

| Parameter | Value |
|-----------|-------|
| ThunderDB Host | $THUNDERDB_HOST |
| PostgreSQL Port | $THUNDERDB_PG_PORT |
| MySQL Port | $THUNDERDB_MYSQL_PORT |
| Read/Write Ratio | ${READ_PERCENTAGE}%/${WRITE_PERCENTAGE}% |
| Concurrency Levels | $CONCURRENCY_LEVELS |
| Dataset Sizes | $DATASET_SIZES |
| Test Duration | ${TEST_DURATION}s |

---

EOF

# ========================================
# Concurrency Test Results
# ========================================
if [ -f "$REPORT_DIR/raw/concurrency_results.csv" ]; then
    cat >> "$OUTPUT_FILE" << 'EOF'
## Concurrency Scaling Results

Tests performance under increasing concurrent load.

EOF

    echo '```' >> "$OUTPUT_FILE"
    column -t -s',' "$REPORT_DIR/raw/concurrency_results.csv" >> "$OUTPUT_FILE"
    echo '```' >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"

    # Find optimal concurrency
    optimal=$(awk -F',' 'NR>1 {print $1","$2}' "$REPORT_DIR/raw/concurrency_results.csv" | sort -t',' -k2 -nr | head -1)
    optimal_conc=$(echo "$optimal" | cut -d',' -f1)
    optimal_qps=$(echo "$optimal" | cut -d',' -f2)

    cat >> "$OUTPUT_FILE" << EOF
**Peak Performance**: $optimal_qps QPS at $optimal_conc concurrent clients

### Analysis

EOF

    # Analyze scaling efficiency
    awk -F',' 'NR>1 {
        conc[NR] = $1;
        qps[NR] = $2;
        p99[NR] = $6;
    }
    END {
        if (NR < 3) exit;

        # Check for linear scaling
        first_qps = qps[2];
        first_conc = conc[2];
        last_qps = qps[NR];
        last_conc = conc[NR];

        expected_scaling = last_conc / first_conc;
        actual_scaling = last_qps / first_qps;
        efficiency = (actual_scaling / expected_scaling) * 100;

        printf "- Scaling efficiency: %.1f%%\n", efficiency;

        if (efficiency > 80) {
            print "- ✅ Excellent scaling - near-linear performance increase";
        } else if (efficiency > 50) {
            print "- ⚠️ Good scaling - some contention at higher concurrency";
        } else {
            print "- ❌ Poor scaling - significant bottleneck detected";
        }

        # Check P99 latency trend
        p99_increase = (p99[NR] / p99[2]) * 100 - 100;
        printf "- P99 latency increase from min to max concurrency: %.1f%%\n", p99_increase;
    }' "$REPORT_DIR/raw/concurrency_results.csv" >> "$OUTPUT_FILE"

    echo "" >> "$OUTPUT_FILE"
fi

# ========================================
# Dataset Scaling Results
# ========================================
if [ -f "$REPORT_DIR/raw/dataset_results.csv" ]; then
    cat >> "$OUTPUT_FILE" << 'EOF'
## Dataset Scaling Results

Tests performance with increasing data volumes.

EOF

    echo '```' >> "$OUTPUT_FILE"
    column -t -s',' "$REPORT_DIR/raw/dataset_results.csv" >> "$OUTPUT_FILE"
    echo '```' >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"

    if [ -f "$REPORT_DIR/raw/bulk_insert_results.csv" ]; then
        cat >> "$OUTPUT_FILE" << 'EOF'
### Bulk Insert Performance

EOF
        echo '```' >> "$OUTPUT_FILE"
        column -t -s',' "$REPORT_DIR/raw/bulk_insert_results.csv" | head -20 >> "$OUTPUT_FILE"
        echo '```' >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"

        # Find best batch size
        best=$(sort -t',' -k4 -nr "$REPORT_DIR/raw/bulk_insert_results.csv" | head -1)
        best_batch=$(echo "$best" | cut -d',' -f2)
        best_rate=$(echo "$best" | cut -d',' -f4)

        echo "**Optimal batch size**: $best_batch rows (${best_rate} rows/sec)" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
    fi
fi

# ========================================
# Latency Analysis Results
# ========================================
if [ -f "$REPORT_DIR/raw/latency_results.csv" ]; then
    cat >> "$OUTPUT_FILE" << 'EOF'
## Latency Percentile Analysis

Detailed latency measurements for different query types.

EOF

    echo '```' >> "$OUTPUT_FILE"
    column -t -s',' "$REPORT_DIR/raw/latency_results.csv" >> "$OUTPUT_FILE"
    echo '```' >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"

    # Analyze tail latency
    cat >> "$OUTPUT_FILE" << 'EOF'
### Tail Latency Analysis

EOF

    awk -F',' 'NR>1 {
        query = $1;
        p50 = $7;
        p99 = $10;
        p999 = $11;

        ratio = p99 / p50;

        if (ratio > 10) {
            printf "- ❌ %s: High tail latency ratio (P99/P50 = %.1fx)\n", query, ratio;
        } else if (ratio > 5) {
            printf "- ⚠️ %s: Moderate tail latency ratio (P99/P50 = %.1fx)\n", query, ratio;
        } else {
            printf "- ✅ %s: Good tail latency ratio (P99/P50 = %.1fx)\n", query, ratio;
        }
    }' "$REPORT_DIR/raw/latency_results.csv" >> "$OUTPUT_FILE"

    echo "" >> "$OUTPUT_FILE"

    if [ -f "$REPORT_DIR/raw/latency_histogram.txt" ]; then
        cat >> "$OUTPUT_FILE" << 'EOF'
### Latency Distribution

EOF
        echo '```' >> "$OUTPUT_FILE"
        cat "$REPORT_DIR/raw/latency_histogram.txt" >> "$OUTPUT_FILE"
        echo '```' >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
    fi
fi

# ========================================
# Sustained Load Results
# ========================================
if [ -f "$REPORT_DIR/raw/sustained_results.csv" ]; then
    cat >> "$OUTPUT_FILE" << 'EOF'
## Sustained Load Test Results

Long-running stability test.

EOF

    echo '```' >> "$OUTPUT_FILE"
    cat "$REPORT_DIR/raw/sustained_results.csv" >> "$OUTPUT_FILE"
    echo '```' >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"

    # Anomaly summary
    if [ -f "$REPORT_DIR/raw/sustained_anomalies.log" ]; then
        anomaly_count=$(wc -l < "$REPORT_DIR/raw/sustained_anomalies.log")
        if [ "$anomaly_count" -gt 0 ]; then
            cat >> "$OUTPUT_FILE" << EOF
### Anomalies Detected: $anomaly_count

EOF
            echo '```' >> "$OUTPUT_FILE"
            head -20 "$REPORT_DIR/raw/sustained_anomalies.log" >> "$OUTPUT_FILE"
            echo '```' >> "$OUTPUT_FILE"
        else
            echo "No anomalies detected during sustained load." >> "$OUTPUT_FILE"
        fi
        echo "" >> "$OUTPUT_FILE"
    fi

    # Performance trend from timeseries
    if [ -f "$REPORT_DIR/raw/sustained_timeseries.csv" ]; then
        cat >> "$OUTPUT_FILE" << 'EOF'
### Performance Trend

EOF
        awk -F',' 'NR > 1 {
            elapsed[NR] = $2;
            qps[NR] = $4;
            p99[NR] = $8;
            memory[NR] = $10;
        }
        END {
            if (NR < 10) exit;

            first_end = int(NR * 0.1);
            last_start = int(NR * 0.9);

            first_qps = 0; first_mem = 0; first_count = 0;
            last_qps = 0; last_mem = 0; last_count = 0;

            for (i = 2; i <= first_end; i++) {
                first_qps += qps[i];
                first_mem += memory[i];
                first_count++;
            }
            for (i = last_start; i <= NR; i++) {
                last_qps += qps[i];
                last_mem += memory[i];
                last_count++;
            }

            if (first_count > 0 && last_count > 0) {
                avg_first_qps = first_qps / first_count;
                avg_last_qps = last_qps / last_count;
                avg_first_mem = first_mem / first_count;
                avg_last_mem = last_mem / last_count;

                qps_change = ((avg_last_qps - avg_first_qps) / avg_first_qps) * 100;
                mem_change = avg_last_mem - avg_first_mem;

                printf "| Metric | Start (first 10%%) | End (last 10%%) | Change |\n";
                printf "|--------|-------------------|-----------------|--------|\n";
                printf "| Throughput | %.1f QPS | %.1f QPS | %.1f%% |\n", avg_first_qps, avg_last_qps, qps_change;
                printf "| Memory | %.0f MB | %.0f MB | %+.0f MB |\n", avg_first_mem, avg_last_mem, mem_change;

                print "";

                if (qps_change < -20) {
                    print "⚠️ **Warning**: Significant throughput degradation detected over time.";
                }
                if (mem_change > 100) {
                    print "⚠️ **Warning**: Potential memory leak detected (+" mem_change "MB).";
                }
            }
        }' "$REPORT_DIR/raw/sustained_timeseries.csv" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
    fi
fi

# ========================================
# Resource Utilization
# ========================================
if [ -f "$REPORT_DIR/metrics/system_resources.csv" ]; then
    cat >> "$OUTPUT_FILE" << 'EOF'
## Resource Utilization

System resource usage during tests.

EOF

    awk -F',' 'NR > 1 {
        cpu += $2;
        mem += $3;
        count++;
    }
    END {
        if (count > 0) {
            printf "| Resource | Average | Peak |\n";
            printf "|----------|---------|------|\n";
            printf "| CPU | %.1f%% | - |\n", cpu/count;
            printf "| Memory | %.1f%% | - |\n", mem/count;
        }
    }' "$REPORT_DIR/metrics/system_resources.csv" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
fi

# ========================================
# Summary and Recommendations
# ========================================
cat >> "$OUTPUT_FILE" << 'EOF'
## Summary and Recommendations

### Performance Summary

EOF

# Overall assessment
if [ -f "$REPORT_DIR/raw/concurrency_results.csv" ]; then
    max_qps=$(awk -F',' 'NR>1 {print $2}' "$REPORT_DIR/raw/concurrency_results.csv" | sort -nr | head -1)
    max_p99=$(awk -F',' 'NR>1 {print $6}' "$REPORT_DIR/raw/concurrency_results.csv" | sort -nr | head -1)

    echo "- **Peak Throughput**: $max_qps queries/second" >> "$OUTPUT_FILE"
    echo "- **Max P99 Latency**: ${max_p99}ms" >> "$OUTPUT_FILE"
fi

cat >> "$OUTPUT_FILE" << 'EOF'

### Recommendations

Based on the test results:

1. **Concurrency Settings**: Review the optimal concurrency level from scaling tests
2. **Batch Sizes**: Use the optimal batch size identified for bulk operations
3. **Connection Pooling**: Configure connection pool based on peak concurrency requirements
4. **Monitoring**: Set up alerts for P99 latency thresholds identified in tests

---

*Report generated by ThunderDB Load Test Suite*
EOF

log_success "Markdown report generated: $OUTPUT_FILE"

# ========================================
# Generate JSON Report
# ========================================
log_info "Generating JSON report..."

cat > "$JSON_FILE" << EOF
{
  "metadata": {
    "generated_at": "$(date -Iseconds)",
    "host": "$(hostname)",
    "thunderdb_version": "${THUNDERDB_VERSION:-unknown}",
    "report_directory": "$REPORT_DIR"
  },
  "configuration": {
    "thunderdb_host": "$THUNDERDB_HOST",
    "pg_port": $THUNDERDB_PG_PORT,
    "mysql_port": $THUNDERDB_MYSQL_PORT,
    "read_percentage": $READ_PERCENTAGE,
    "write_percentage": $WRITE_PERCENTAGE,
    "test_duration_sec": $TEST_DURATION
  },
EOF

# Add concurrency results
if [ -f "$REPORT_DIR/raw/concurrency_results.csv" ]; then
    echo '  "concurrency_results": [' >> "$JSON_FILE"
    awk -F',' 'NR>1 {
        if (NR > 2) printf ",\n";
        printf "    {\"concurrency\": %s, \"throughput_qps\": %s, \"avg_latency_ms\": %s, \"p50_ms\": %s, \"p95_ms\": %s, \"p99_ms\": %s, \"errors\": %s}", $1, $2, $3, $4, $5, $6, $7;
    }' "$REPORT_DIR/raw/concurrency_results.csv" >> "$JSON_FILE"
    echo '' >> "$JSON_FILE"
    echo '  ],' >> "$JSON_FILE"
else
    echo '  "concurrency_results": [],' >> "$JSON_FILE"
fi

# Add latency results
if [ -f "$REPORT_DIR/raw/latency_results.csv" ]; then
    echo '  "latency_results": [' >> "$JSON_FILE"
    awk -F',' 'NR>1 {
        if (NR > 2) printf ",\n";
        printf "    {\"query_type\": \"%s\", \"samples\": %s, \"avg_ms\": %s, \"p50_ms\": %s, \"p95_ms\": %s, \"p99_ms\": %s, \"p999_ms\": %s}", $1, $2, $5, $7, $9, $10, $11;
    }' "$REPORT_DIR/raw/latency_results.csv" >> "$JSON_FILE"
    echo '' >> "$JSON_FILE"
    echo '  ],' >> "$JSON_FILE"
else
    echo '  "latency_results": [],' >> "$JSON_FILE"
fi

# Add dataset results
if [ -f "$REPORT_DIR/raw/dataset_results.csv" ]; then
    echo '  "dataset_results": [' >> "$JSON_FILE"
    awk -F',' 'NR>1 {
        if (NR > 2) printf ",\n";
        printf "    {\"dataset_size\": %s, \"insert_time_sec\": %s, \"select_time_ms\": %s, \"update_time_ms\": %s}", $1, $2, $4, $6;
    }' "$REPORT_DIR/raw/dataset_results.csv" >> "$JSON_FILE"
    echo '' >> "$JSON_FILE"
    echo '  ],' >> "$JSON_FILE"
else
    echo '  "dataset_results": [],' >> "$JSON_FILE"
fi

# Close JSON
echo '  "sustained_test": {}' >> "$JSON_FILE"
echo '}' >> "$JSON_FILE"

log_success "JSON report generated: $JSON_FILE"

echo ""
echo "Reports generated:"
echo "  - Markdown: $OUTPUT_FILE"
echo "  - JSON: $JSON_FILE"
