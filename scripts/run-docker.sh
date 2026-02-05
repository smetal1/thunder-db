#!/bin/bash
# ThunderDB Docker Runner Script
# Builds and runs ThunderDB in an Ubuntu container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== ThunderDB Docker Runner ==="
echo "Project directory: $PROJECT_DIR"

# Check if image exists
if docker images | grep -q "thunderdb"; then
    echo "Using existing thunderdb image..."
else
    echo "Building ThunderDB Docker image (this may take 10-20 minutes)..."
    cd "$PROJECT_DIR"
    docker build -t thunderdb:latest .
fi

# Stop any existing container
docker rm -f thunderdb 2>/dev/null || true

# Run the container
echo "Starting ThunderDB container..."
docker run -d \
    --name thunderdb \
    -p 5432:5432 \
    -p 3306:3306 \
    -p 6379:6379 \
    -p 8080:8080 \
    -p 9090:9090 \
    -v thunderdb-data:/var/lib/thunderdb/data \
    -v thunderdb-wal:/var/lib/thunderdb/wal \
    thunderdb:latest

echo ""
echo "=== ThunderDB is starting ==="
echo "Waiting for server to be ready..."

# Wait for health check
for i in {1..30}; do
    if curl -sf http://localhost:8080/api/v1/health > /dev/null 2>&1; then
        echo "ThunderDB is ready!"
        echo ""
        echo "=== Connection Information ==="
        echo "  Dashboard:   http://localhost:8080/dashboard"
        echo "  HTTP API:    http://localhost:8080/api/v1"
        echo "  PostgreSQL:  localhost:5432"
        echo "  MySQL:       localhost:3306"
        echo "  Redis:       localhost:6379"
        echo "  gRPC:        localhost:9090"
        echo ""
        echo "=== CLI Usage ==="
        echo "  docker exec -it thunderdb thunder-cli"
        echo ""
        exit 0
    fi
    echo "  Waiting... ($i/30)"
    sleep 2
done

echo "ERROR: ThunderDB failed to start. Check logs with: docker logs thunderdb"
exit 1
