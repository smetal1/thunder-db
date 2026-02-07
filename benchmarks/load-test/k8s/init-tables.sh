#!/bin/bash
set -euo pipefail
source /scripts/config.sh

log_info "Initializing test tables..."

PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" << 'EOF'
CREATE TABLE IF NOT EXISTS load_test_accounts (
    id INTEGER PRIMARY KEY,
    balance INTEGER DEFAULT 0,
    filler TEXT
);

CREATE TABLE IF NOT EXISTS load_test_history (
    id INTEGER PRIMARY KEY,
    account_id INTEGER,
    delta INTEGER,
    mtime INTEGER
);
EOF

# Check if data exists
count=$(PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -t -c "SELECT COUNT(*) FROM load_test_accounts" 2>/dev/null | tr -d ' ')

# Insert minimal test data - individual inserts are faster than batch in ThunderDB
if [ "${count:-0}" -lt 10 ]; then
    log_info "Inserting minimal test data (100 rows)..."
    for i in $(seq 1 100); do
        PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "INSERT INTO load_test_accounts (id, balance, filler) VALUES ($i, 0, 'x')" > /dev/null 2>&1 || true
        if [ $((i % 20)) -eq 0 ]; then
            log_info "  Inserted $i/100 rows"
        fi
    done
    log_info "  Data insertion complete"
else
    log_info "Test data already exists ($count rows)"
fi

log_success "Test tables ready"
