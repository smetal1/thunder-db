#!/bin/bash
# ThunderDB Test Runner Script

set -e

echo "=========================================="
echo "  ThunderDB Test Suite"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to run tests with timing
run_tests() {
    local name="$1"
    local cmd="$2"

    echo -e "\n${YELLOW}=== Running $name ===${NC}"
    local start=$(date +%s)

    if eval "$cmd"; then
        local end=$(date +%s)
        echo -e "${GREEN}✓ $name passed ($(($end - $start))s)${NC}"
        return 0
    else
        local end=$(date +%s)
        echo -e "${RED}✗ $name failed ($(($end - $start))s)${NC}"
        return 1
    fi
}

# Parse arguments
COVERAGE=false
VERBOSE=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --coverage)
            COVERAGE=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Set verbose flag
VERBOSE_FLAG=""
if [ "$VERBOSE" = true ]; then
    VERBOSE_FLAG="-- --nocapture"
fi

# Run unit tests
run_tests "Unit Tests" "cargo test --workspace --lib $VERBOSE_FLAG"

# Run integration tests
run_tests "Integration Tests" "cargo test --workspace --test '*' $VERBOSE_FLAG"

# Run doc tests
run_tests "Doc Tests" "cargo test --workspace --doc"

# Check formatting
run_tests "Format Check" "cargo fmt --all -- --check"

# Run clippy
run_tests "Clippy Lints" "cargo clippy --workspace --all-targets -- -D warnings"

# Run coverage if requested
if [ "$COVERAGE" = true ]; then
    echo -e "\n${YELLOW}=== Running Coverage Analysis ===${NC}"
    if command -v cargo-tarpaulin &> /dev/null; then
        cargo tarpaulin --workspace --out Html --output-dir coverage/
        echo -e "${GREEN}Coverage report generated in coverage/tarpaulin-report.html${NC}"
    else
        echo -e "${RED}cargo-tarpaulin not installed. Install with: cargo install cargo-tarpaulin${NC}"
    fi
fi

echo -e "\n${GREEN}=========================================="
echo "  All Tests Passed!"
echo "==========================================${NC}"
