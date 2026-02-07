#!/bin/bash
# Initialize test tables for load testing
# ========================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

log_info "Creating test tables..."

# Create tables using psql
PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" << 'EOF'

-- Drop existing test tables if they exist
DROP TABLE IF EXISTS load_test_accounts CASCADE;
DROP TABLE IF EXISTS load_test_transactions CASCADE;
DROP TABLE IF EXISTS load_test_history CASCADE;
DROP TABLE IF EXISTS load_test_kv CASCADE;

-- Main accounts table (similar to pgbench_accounts)
CREATE TABLE IF NOT EXISTS load_test_accounts (
    id BIGINT PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0,
    filler VARCHAR(84) NOT NULL DEFAULT ''
);

-- Transactions table for write tests
CREATE TABLE IF NOT EXISTS load_test_transactions (
    id BIGINT PRIMARY KEY,
    account_id BIGINT NOT NULL,
    amount BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

-- History table for append-only workloads
CREATE TABLE IF NOT EXISTS load_test_history (
    id BIGINT PRIMARY KEY,
    account_id BIGINT NOT NULL,
    delta BIGINT NOT NULL,
    mtime BIGINT NOT NULL
);

-- Key-value table for simple CRUD tests
CREATE TABLE IF NOT EXISTS load_test_kv (
    k VARCHAR(64) PRIMARY KEY,
    v VARCHAR(256) NOT NULL
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_transactions_account ON load_test_transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_history_account ON load_test_history(account_id);

EOF

log_success "Test tables created successfully"

# Seed initial data
log_info "Seeding initial data..."

# Generate seed data script
SEED_SIZE="${SEED_SIZE:-10000}"

PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" << EOF
-- Seed accounts
INSERT INTO load_test_accounts (id, balance, filler)
SELECT
    generate_series(1, $SEED_SIZE),
    0,
    repeat('x', 84)
ON CONFLICT (id) DO NOTHING;

-- Seed some key-value pairs
INSERT INTO load_test_kv (k, v)
SELECT
    'key_' || generate_series(1, 1000),
    'value_' || generate_series(1, 1000)
ON CONFLICT (k) DO NOTHING;
EOF

log_success "Seeded $SEED_SIZE accounts and 1000 key-value pairs"
