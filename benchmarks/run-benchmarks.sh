#!/bin/bash
# ThunderDB Benchmark Runner
# Runs all benchmarks against a deployed ThunderDB instance

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-thunderdb}"
REPORT_DIR="${SCRIPT_DIR}/reports"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $1"; }
error() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $1"; }
info() { echo -e "${BLUE}[$(date '+%H:%M:%S')] INFO:${NC} $1"; }

usage() {
    cat <<EOF
ThunderDB Benchmark Runner

Usage: $0 [OPTIONS] [BENCHMARK...]

Benchmarks:
  pgbench     Run PostgreSQL protocol benchmark (pgbench)
  sysbench    Run MySQL protocol benchmark (sysbench)
  redis       Run Redis protocol benchmark (redis-benchmark)
  all         Run all benchmarks (default)

Options:
  -n, --namespace NAME    Kubernetes namespace (default: thunderdb)
  -m, --mode MODE         Run mode: k8s (in-cluster) or local (port-forward)
  -d, --duration SEC      Benchmark duration in seconds (default: 60)
  -c, --clients NUM       Number of concurrent clients (default: 10)
  -w, --wait              Wait for jobs to complete
  -h, --help              Show this help message

Examples:
  $0                      # Run all benchmarks in k8s mode
  $0 pgbench redis        # Run only pgbench and redis benchmarks
  $0 -m local all         # Run all benchmarks with port-forwarding
  $0 -d 120 -c 20 all     # Run with 120s duration and 20 clients

EOF
    exit 0
}

# Default values
MODE="k8s"
DURATION=60
CLIENTS=10
WAIT=false
BENCHMARKS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace) NAMESPACE="$2"; shift 2 ;;
        -m|--mode) MODE="$2"; shift 2 ;;
        -d|--duration) DURATION="$2"; shift 2 ;;
        -c|--clients) CLIENTS="$2"; shift 2 ;;
        -w|--wait) WAIT=true; shift ;;
        -h|--help) usage ;;
        pgbench|sysbench|redis|all) BENCHMARKS+=("$1"); shift ;;
        *) error "Unknown option: $1"; usage ;;
    esac
done

# Default to all benchmarks
[[ ${#BENCHMARKS[@]} -eq 0 ]] && BENCHMARKS=("all")
[[ " ${BENCHMARKS[*]} " =~ " all " ]] && BENCHMARKS=("pgbench" "sysbench" "redis")

mkdir -p "$REPORT_DIR"

log "ThunderDB Benchmark Suite"
log "========================="
info "Namespace: $NAMESPACE"
info "Mode: $MODE"
info "Duration: ${DURATION}s"
info "Clients: $CLIENTS"
info "Benchmarks: ${BENCHMARKS[*]}"
echo ""

# Check ThunderDB is running
log "Checking ThunderDB deployment..."
if ! kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=thunderdb -o name | grep -q pod; then
    error "ThunderDB pod not found in namespace $NAMESPACE"
    exit 1
fi

POD_STATUS=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=thunderdb -o jsonpath='{.items[0].status.phase}')
if [[ "$POD_STATUS" != "Running" ]]; then
    error "ThunderDB pod is not running (status: $POD_STATUS)"
    exit 1
fi
log "ThunderDB is running"

run_k8s_benchmark() {
    local benchmark=$1
    local job_file="${SCRIPT_DIR}/k8s/${benchmark}-job.yaml"

    if [[ ! -f "$job_file" ]]; then
        # Handle naming differences
        case $benchmark in
            pgbench) job_file="${SCRIPT_DIR}/k8s/pgbench-job.yaml" ;;
            redis) job_file="${SCRIPT_DIR}/k8s/redis-benchmark-job.yaml" ;;
            sysbench) job_file="${SCRIPT_DIR}/k8s/sysbench-job.yaml" ;;
        esac
    fi

    log "Starting $benchmark benchmark job..."

    # Delete existing job if any
    kubectl delete job "thunderdb-${benchmark}" -n "$NAMESPACE" 2>/dev/null || true
    kubectl delete job "thunderdb-${benchmark}-benchmark" -n "$NAMESPACE" 2>/dev/null || true

    # Apply configmap first
    kubectl apply -f "${SCRIPT_DIR}/k8s/benchmark-configmap.yaml"

    # Create job with updated parameters
    cat "$job_file" | \
        sed "s/PGBENCH_DURATION.*/PGBENCH_DURATION\n              value: \"$DURATION\"/" | \
        sed "s/SYSBENCH_DURATION.*/SYSBENCH_DURATION\n              value: \"$DURATION\"/" | \
        sed "s/PGBENCH_CLIENTS.*/PGBENCH_CLIENTS\n              value: \"$CLIENTS\"/" | \
        sed "s/REDIS_CLIENTS.*/REDIS_CLIENTS\n              value: \"$CLIENTS\"/" | \
        kubectl apply -f -

    if $WAIT; then
        local job_name
        case $benchmark in
            redis) job_name="thunderdb-redis-benchmark" ;;
            *) job_name="thunderdb-${benchmark}" ;;
        esac

        log "Waiting for $benchmark job to complete..."
        kubectl wait --for=condition=complete --timeout=600s job/"$job_name" -n "$NAMESPACE" || {
            warn "Job did not complete in time, fetching logs..."
        }

        # Get logs
        log "Fetching $benchmark results..."
        kubectl logs job/"$job_name" -n "$NAMESPACE" > "${REPORT_DIR}/${benchmark}_${TIMESTAMP}.log" 2>&1 || true
        log "Results saved to ${REPORT_DIR}/${benchmark}_${TIMESTAMP}.log"
    fi
}

run_local_benchmark() {
    local benchmark=$1

    log "Starting port-forward for $benchmark..."

    case $benchmark in
        pgbench)
            # Port-forward PostgreSQL
            kubectl port-forward svc/thunderdb 5432:5432 -n "$NAMESPACE" &
            PF_PID=$!
            sleep 2

            export THUNDERDB_HOST=localhost
            export THUNDERDB_PG_PORT=5432
            export THUNDERDB_USER=admin
            export PGBENCH_DURATION=$DURATION
            export PGBENCH_CLIENTS=$CLIENTS
            export REPORT_DIR="$REPORT_DIR"

            "${SCRIPT_DIR}/scripts/pgbench.sh"

            kill $PF_PID 2>/dev/null || true
            ;;

        redis)
            kubectl port-forward svc/thunderdb 6379:6379 -n "$NAMESPACE" &
            PF_PID=$!
            sleep 2

            export THUNDERDB_HOST=localhost
            export THUNDERDB_RESP_PORT=6379
            export REDIS_CLIENTS=$CLIENTS
            export REPORT_DIR="$REPORT_DIR"

            "${SCRIPT_DIR}/scripts/redis-benchmark.sh"

            kill $PF_PID 2>/dev/null || true
            ;;

        sysbench)
            kubectl port-forward svc/thunderdb 3306:3306 -n "$NAMESPACE" &
            PF_PID=$!
            sleep 2

            export THUNDERDB_HOST=localhost
            export THUNDERDB_MYSQL_PORT=3306
            export THUNDERDB_USER=admin
            export SYSBENCH_DURATION=$DURATION
            export SYSBENCH_THREADS=$CLIENTS
            export REPORT_DIR="$REPORT_DIR"

            "${SCRIPT_DIR}/scripts/sysbench-mysql.sh"

            kill $PF_PID 2>/dev/null || true
            ;;
    esac
}

# Run benchmarks
for benchmark in "${BENCHMARKS[@]}"; do
    echo ""
    log "=============================================="
    log "Running $benchmark benchmark"
    log "=============================================="

    if [[ "$MODE" == "k8s" ]]; then
        run_k8s_benchmark "$benchmark"
    else
        run_local_benchmark "$benchmark"
    fi
done

echo ""
log "=============================================="
log "Benchmark suite complete!"
log "=============================================="
log "Reports directory: $REPORT_DIR"

if [[ "$MODE" == "k8s" ]] && ! $WAIT; then
    echo ""
    info "Jobs are running in background. To check status:"
    echo "  kubectl get jobs -n $NAMESPACE -l app=thunderdb-benchmark"
    echo ""
    info "To view logs:"
    echo "  kubectl logs job/thunderdb-pgbench -n $NAMESPACE"
    echo "  kubectl logs job/thunderdb-sysbench -n $NAMESPACE"
    echo "  kubectl logs job/thunderdb-redis-benchmark -n $NAMESPACE"
fi
