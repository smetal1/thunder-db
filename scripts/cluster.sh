#!/bin/bash
# ThunderDB Cluster Management Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_DIR/target/release/thunderdb"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

usage() {
    echo "Usage: $0 {start|stop|status|logs} [node_id]"
    echo ""
    echo "Commands:"
    echo "  start [node_id]  - Start cluster or specific node"
    echo "  stop [node_id]   - Stop cluster or specific node"
    echo "  status           - Show cluster status"
    echo "  logs [node_id]   - Show logs for node"
    exit 1
}

start_node() {
    local node_id=$1
    local config="$PROJECT_DIR/config/node${node_id}.toml"
    local log_file="$PROJECT_DIR/logs/node${node_id}.log"
    local pid_file="$PROJECT_DIR/pids/node${node_id}.pid"

    mkdir -p "$PROJECT_DIR/logs" "$PROJECT_DIR/pids"
    mkdir -p "$PROJECT_DIR/data/node${node_id}" "$PROJECT_DIR/wal/node${node_id}"

    if [ -f "$pid_file" ] && kill -0 $(cat "$pid_file") 2>/dev/null; then
        echo -e "${YELLOW}Node $node_id is already running${NC}"
        return
    fi

    echo -e "${GREEN}Starting node $node_id...${NC}"
    nohup "$BINARY" --config "$config" > "$log_file" 2>&1 &
    echo $! > "$pid_file"
    echo -e "${GREEN}Node $node_id started (PID: $(cat $pid_file))${NC}"
}

stop_node() {
    local node_id=$1
    local pid_file="$PROJECT_DIR/pids/node${node_id}.pid"

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${YELLOW}Stopping node $node_id (PID: $pid)...${NC}"
            kill "$pid"
            rm "$pid_file"
            echo -e "${GREEN}Node $node_id stopped${NC}"
        else
            echo -e "${RED}Node $node_id is not running${NC}"
            rm "$pid_file"
        fi
    else
        echo -e "${RED}No PID file for node $node_id${NC}"
    fi
}

start_cluster() {
    echo "Starting 3-node cluster..."
    for i in 1 2 3; do
        start_node $i
        sleep 1
    done
    echo -e "\n${GREEN}Cluster started!${NC}"
}

stop_cluster() {
    echo "Stopping cluster..."
    for i in 1 2 3; do
        stop_node $i
    done
    echo -e "\n${GREEN}Cluster stopped!${NC}"
}

show_status() {
    echo "Cluster Status:"
    echo "---------------"
    for i in 1 2 3; do
        local pid_file="$PROJECT_DIR/pids/node${i}.pid"
        if [ -f "$pid_file" ] && kill -0 $(cat "$pid_file") 2>/dev/null; then
            echo -e "Node $i: ${GREEN}RUNNING${NC} (PID: $(cat $pid_file))"
        else
            echo -e "Node $i: ${RED}STOPPED${NC}"
        fi
    done
}

show_logs() {
    local node_id=$1
    local log_file="$PROJECT_DIR/logs/node${node_id}.log"

    if [ -f "$log_file" ]; then
        tail -f "$log_file"
    else
        echo -e "${RED}No log file for node $node_id${NC}"
    fi
}

# Check if binary exists
if [ ! -f "$BINARY" ]; then
    echo -e "${YELLOW}Binary not found. Building release...${NC}"
    cd "$PROJECT_DIR" && cargo build --release
fi

# Parse command
case "$1" in
    start)
        if [ -n "$2" ]; then
            start_node "$2"
        else
            start_cluster
        fi
        ;;
    stop)
        if [ -n "$2" ]; then
            stop_node "$2"
        else
            stop_cluster
        fi
        ;;
    status)
        show_status
        ;;
    logs)
        if [ -n "$2" ]; then
            show_logs "$2"
        else
            echo "Please specify node_id"
            usage
        fi
        ;;
    *)
        usage
        ;;
esac
