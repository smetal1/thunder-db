#!/bin/bash
# Build ThunderDB Debian package
# Usage: ./scripts/build-deb.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "Building ThunderDB .deb package for Linux x86_64..."

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is required to build Linux packages on macOS"
    exit 1
fi

# Build using Docker
docker build --platform linux/amd64 -f Dockerfile.deb -t thunderdb-builder .

# Extract the .deb file from the container
CONTAINER_ID=$(docker create thunderdb-builder)
docker cp "$CONTAINER_ID:/app/target/debian/" ./target/ 2>/dev/null || true
docker rm "$CONTAINER_ID"

# Show results
echo ""
echo "Build complete! Package location:"
ls -la target/debian/*.deb 2>/dev/null || echo "Package not found - check build logs"

echo ""
echo "To install on Ubuntu/Debian:"
echo "  sudo dpkg -i target/debian/thunderdb_*.deb"
echo "  sudo apt-get install -f  # Install any missing dependencies"
