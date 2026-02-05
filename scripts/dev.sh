#!/bin/bash
# ThunderDB Development Server Script

set -e

echo "=========================================="
echo "  Starting ThunderDB Development Server"
echo "=========================================="

# Create data directories
mkdir -p data wal

# Set environment variables for development
export RUST_LOG=thunder=debug,info
export RUST_BACKTRACE=1

# Build and run
cargo run --bin thunderdb -- \
    --config config/thunderdb.toml \
    --verbose \
    "$@"
