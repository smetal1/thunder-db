#!/bin/bash
# ThunderDB Build Script

set -e

echo "=========================================="
echo "  Building ThunderDB"
echo "=========================================="

# Parse arguments
RELEASE=false
FEATURES=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --release)
            RELEASE=true
            shift
            ;;
        --features)
            FEATURES="--features $2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Determine build type
if [ "$RELEASE" = true ]; then
    BUILD_TYPE="--release"
    echo "Build type: Release"
else
    BUILD_TYPE=""
    echo "Build type: Debug"
fi

# Build
echo "Running cargo build..."
cargo build --workspace $BUILD_TYPE $FEATURES

# Print binary location
if [ "$RELEASE" = true ]; then
    BINARY="target/release/thunderdb"
else
    BINARY="target/debug/thunderdb"
fi

if [ -f "$BINARY" ]; then
    echo ""
    echo "Build successful!"
    echo "Binary: $BINARY"
    echo ""
    echo "Run with: $BINARY --config config/thunderdb.toml"
else
    echo "Build completed but binary not found at expected location"
fi
