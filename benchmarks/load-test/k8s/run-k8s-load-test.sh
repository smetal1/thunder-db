#!/bin/bash
# ThunderDB Kubernetes Load Test Runner
# =====================================
# Launches load tests as Kubernetes jobs and collects results

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-thunderdb}"
JOB_NAME="${JOB_NAME:-thunderdb-load-test-quick}"
REPORT_DIR="${REPORT_DIR:-$SCRIPT_DIR/../reports}"
TIMEOUT="${TIMEOUT:-600}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
    --full          Run full load test (longer duration)
    --quick         Run quick load test (default)
    --namespace NS  Kubernetes namespace (default: thunderdb)
    --timeout SEC   Wait timeout in seconds (default: 600)
    --help          Show this help message

Examples:
    $(basename "$0")                    # Run quick test
    $(basename "$0") --full             # Run full test
    $(basename "$0") --namespace myns   # Run in custom namespace
EOF
    exit 0
}

# Parse arguments
FULL_TEST=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --full)
            FULL_TEST=true
            JOB_NAME="thunderdb-load-test"
            TIMEOUT=1800
            shift
            ;;
        --quick)
            FULL_TEST=false
            JOB_NAME="thunderdb-load-test-quick"
            shift
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║         ThunderDB Kubernetes Load Test Runner                  ║"
echo "╠════════════════════════════════════════════════════════════════╣"
echo "║  Namespace: $NAMESPACE"
echo "║  Job: $JOB_NAME"
echo "║  Timeout: ${TIMEOUT}s"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Create report directory
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/load_test_$(date +%Y%m%d_%H%M%S).log"

# Step 1: Clean up existing jobs and reports
log_info "Cleaning up existing load test jobs..."
kubectl delete job thunderdb-load-test -n "$NAMESPACE" 2>/dev/null || true
kubectl delete job thunderdb-load-test-quick -n "$NAMESPACE" 2>/dev/null || true

# Clean old reports (keep last 5)
log_info "Cleaning old reports (keeping last 5)..."
ls -t "$REPORT_DIR"/load_test_*.log 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null || true

# Step 2: Create the job
log_info "Creating load test job: $JOB_NAME..."

if [ "$FULL_TEST" = true ]; then
    # Extract and apply only the full test job
    kubectl apply -f "$SCRIPT_DIR/load-test-job.yaml" 2>&1 | grep -v "thunderdb-load-test-quick" || true
    # Delete the quick job if it was created
    kubectl delete job thunderdb-load-test-quick -n "$NAMESPACE" 2>/dev/null || true
else
    # Apply both, then we only care about the quick one
    kubectl apply -f "$SCRIPT_DIR/load-test-job.yaml"
    # Delete the full job if it was created
    kubectl delete job thunderdb-load-test -n "$NAMESPACE" 2>/dev/null || true
fi

# Step 4: Wait for pod to start
log_info "Waiting for load test pod to start..."
sleep 3

POD_NAME=""
for i in $(seq 1 60); do
    POD_NAME=$(kubectl get pods -n "$NAMESPACE" -l app=thunderdb-load-test --no-headers 2>/dev/null | grep -v "Completed\|Error" | head -1 | awk '{print $1}')
    if [ -n "$POD_NAME" ]; then
        POD_STATUS=$(kubectl get pod "$POD_NAME" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
        if [ "$POD_STATUS" = "Running" ]; then
            break
        fi
        log_info "  Pod $POD_NAME is $POD_STATUS, waiting..."
    fi
    sleep 2
done

if [ -z "$POD_NAME" ]; then
    log_error "Failed to find load test pod"
    kubectl get pods -n "$NAMESPACE" -l app=thunderdb-load-test
    exit 1
fi

log_info "Load test pod: $POD_NAME (Running)"

# Step 5: Stream logs and wait for completion
log_info "Streaming load test output..."
echo ""
echo "═══════════════════════════════════════════════════════════════════"

# Stream logs to both console and file
# Use gtimeout on macOS (from coreutils) or timeout on Linux, or fallback to no timeout
if command -v gtimeout &> /dev/null; then
    gtimeout "${TIMEOUT}s" kubectl logs -f "$POD_NAME" -n "$NAMESPACE" 2>&1 | tee "$REPORT_FILE" || true
elif command -v timeout &> /dev/null; then
    timeout "${TIMEOUT}s" kubectl logs -f "$POD_NAME" -n "$NAMESPACE" 2>&1 | tee "$REPORT_FILE" || true
else
    # No timeout available, just stream logs until job completes
    kubectl logs -f "$POD_NAME" -n "$NAMESPACE" 2>&1 | tee "$REPORT_FILE" || true
fi

echo "═══════════════════════════════════════════════════════════════════"
echo ""

# Step 6: Check job status
JOB_STATUS=$(kubectl get job "$JOB_NAME" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "Unknown")

if [ "$JOB_STATUS" = "True" ]; then
    log_success "Load test completed successfully!"
else
    FAILED_STATUS=$(kubectl get job "$JOB_NAME" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || echo "")
    if [ "$FAILED_STATUS" = "True" ]; then
        log_error "Load test failed!"
        exit 1
    else
        log_warn "Load test status unknown: $JOB_STATUS"
    fi
fi

# Step 7: Save final report
log_info "Report saved to: $REPORT_FILE"

# Generate summary
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                      Test Summary                              ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Extract key metrics from the report
if [ -f "$REPORT_FILE" ]; then
    echo "Concurrency Results:"
    grep -A 20 "=== Concurrency Test Results ===" "$REPORT_FILE" 2>/dev/null | head -15 || echo "  (not available)"
    echo ""
    echo "Latency Results:"
    grep -A 10 "=== Latency Results ===" "$REPORT_FILE" 2>/dev/null | head -10 || echo "  (not available)"
fi

echo ""
log_success "Full report: $REPORT_FILE"
