#!/bin/bash
# Dataset Scaling Test
# ====================
# Tests ThunderDB performance with increasing dataset sizes (100K+ rows)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

OUTPUT_FILE="$RUN_DIR/raw/dataset_results.csv"
BULK_OUTPUT_FILE="$RUN_DIR/raw/bulk_insert_results.csv"

log_info "Starting dataset scaling tests..."
log_info "Testing dataset sizes: $DATASET_SIZES"

# CSV headers
echo "dataset_size,insert_time_sec,insert_rate_rows_per_sec,select_all_time_ms,select_range_time_ms,update_time_ms,delete_time_ms" > "$OUTPUT_FILE"
echo "dataset_size,batch_size,total_time_sec,rows_per_sec" > "$BULK_OUTPUT_FILE"

# Run tests for each dataset size
for dataset_size in $DATASET_SIZES; do
    log_info "Testing with dataset size: $dataset_size rows"

    # Clean up from previous test
    log_info "  Cleaning up previous data..."
    PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "TRUNCATE load_test_accounts" > /dev/null 2>&1 || \
    PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "DELETE FROM load_test_accounts" > /dev/null 2>&1

    # ========================================
    # Bulk Insert Test
    # ========================================
    log_info "  Testing bulk insert..."

    # Test different batch sizes
    for batch_size in 100 1000 5000 10000; do
        if [ $batch_size -gt $dataset_size ]; then
            continue
        fi

        log_info "    Batch size: $batch_size"

        # Clean table
        PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "DELETE FROM load_test_accounts" > /dev/null 2>&1

        start_time=$(date +%s.%N)

        # Generate and insert data in batches
        rows_inserted=0
        while [ $rows_inserted -lt $dataset_size ]; do
            remaining=$((dataset_size - rows_inserted))
            current_batch=$((remaining < batch_size ? remaining : batch_size))

            # Generate batch INSERT statement
            values=""
            for i in $(seq 1 $current_batch); do
                id=$((rows_inserted + i))
                if [ -n "$values" ]; then
                    values="$values,"
                fi
                values="${values}($id, 0, '$(printf 'x%.0s' {1..84})')"
            done

            PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "INSERT INTO load_test_accounts (id, balance, filler) VALUES $values" > /dev/null 2>&1

            rows_inserted=$((rows_inserted + current_batch))
        done

        end_time=$(date +%s.%N)
        insert_time=$(echo "$end_time - $start_time" | bc)
        insert_rate=$(echo "scale=2; $dataset_size / $insert_time" | bc)

        echo "$dataset_size,$batch_size,$insert_time,$insert_rate" >> "$BULK_OUTPUT_FILE"
        log_info "      Inserted $dataset_size rows in ${insert_time}s (${insert_rate} rows/sec)"
    done

    # ========================================
    # Query Performance Tests
    # ========================================

    # Ensure we have the full dataset
    row_count=$(PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -t -c "SELECT COUNT(*) FROM load_test_accounts" 2>/dev/null | tr -d ' ')
    if [ "$row_count" != "$dataset_size" ]; then
        log_info "  Reinserting data for query tests..."
        PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "DELETE FROM load_test_accounts" > /dev/null 2>&1

        values=""
        for i in $(seq 1 $dataset_size); do
            if [ -n "$values" ]; then
                values="$values,"
            fi
            values="${values}($i, 0, '$(printf 'x%.0s' {1..84})')"

            # Insert in batches of 1000
            if [ $((i % 1000)) -eq 0 ] || [ $i -eq $dataset_size ]; then
                PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "INSERT INTO load_test_accounts (id, balance, filler) VALUES $values" > /dev/null 2>&1
                values=""
            fi
        done
    fi

    # SELECT all rows (with LIMIT for large datasets)
    log_info "  Testing SELECT performance..."
    limit=$((dataset_size > 10000 ? 10000 : dataset_size))
    start_time=$(date +%s%N)
    PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "SELECT * FROM load_test_accounts LIMIT $limit" > /dev/null 2>&1
    end_time=$(date +%s%N)
    select_all_time=$(echo "scale=3; ($end_time - $start_time) / 1000000" | bc)

    # SELECT with range query
    log_info "  Testing range SELECT..."
    range_start=$((dataset_size / 4))
    range_end=$((dataset_size * 3 / 4))
    start_time=$(date +%s%N)
    PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "SELECT * FROM load_test_accounts WHERE id BETWEEN $range_start AND $range_end LIMIT 1000" > /dev/null 2>&1
    end_time=$(date +%s%N)
    select_range_time=$(echo "scale=3; ($end_time - $start_time) / 1000000" | bc)

    # UPDATE batch
    log_info "  Testing UPDATE performance..."
    update_count=$((dataset_size / 10))
    start_time=$(date +%s%N)
    PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "UPDATE load_test_accounts SET balance = balance + 1 WHERE id <= $update_count" > /dev/null 2>&1
    end_time=$(date +%s%N)
    update_time=$(echo "scale=3; ($end_time - $start_time) / 1000000" | bc)

    # DELETE batch
    log_info "  Testing DELETE performance..."
    delete_count=$((dataset_size / 20))
    start_time=$(date +%s%N)
    PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "DELETE FROM load_test_accounts WHERE id > $((dataset_size - delete_count))" > /dev/null 2>&1
    end_time=$(date +%s%N)
    delete_time=$(echo "scale=3; ($end_time - $start_time) / 1000000" | bc)

    # Get final insert time (best batch size)
    best_insert_rate=$(grep "^$dataset_size," "$BULK_OUTPUT_FILE" | sort -t',' -k4 -nr | head -1 | cut -d',' -f3)

    echo "$dataset_size,$best_insert_rate,$insert_rate,$select_all_time,$select_range_time,$update_time,$delete_time" >> "$OUTPUT_FILE"

    log_info "  Completed: SELECT=${select_all_time}ms, RANGE=${select_range_time}ms, UPDATE=${update_time}ms, DELETE=${delete_time}ms"
done

log_success "Dataset scaling tests completed. Results saved to $OUTPUT_FILE"

# Generate summary
echo ""
echo "Dataset Scaling Test Summary"
echo "============================"
echo ""
echo "Query Performance:"
column -t -s',' "$OUTPUT_FILE" | head -20
echo ""
echo "Bulk Insert Performance (best batch sizes):"
column -t -s',' "$BULK_OUTPUT_FILE" | sort -t',' -k1,1n -k4,4nr | head -20
