# ThunderDB Dockerfile
# Multi-stage build for optimized image size

# Stage 1: Build
FROM rust:1.85-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    clang \
    lld \
    protobuf-compiler \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build release binaries
RUN cargo build --release

# Stage 2: Runtime
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create thunder user
RUN useradd -m -s /bin/bash thunder

# Create data directories
RUN mkdir -p /var/lib/thunderdb/data /var/lib/thunderdb/wal /var/log/thunderdb \
    && chown -R thunder:thunder /var/lib/thunderdb /var/log/thunderdb

# Copy binaries from builder
COPY --from=builder /app/target/release/thunderdb /usr/local/bin/
COPY --from=builder /app/target/release/thunder-cli /usr/local/bin/

# Copy default config
COPY config/docker.toml /etc/thunderdb/thunderdb.toml

# Set permissions
RUN chmod +x /usr/local/bin/thunderdb /usr/local/bin/thunder-cli

# Switch to thunder user
USER thunder

# Expose ports
# 5432 - PostgreSQL protocol
# 3306 - MySQL protocol
# 6379 - RESP (Redis) protocol
# 8080 - HTTP API & Dashboard
# 9090 - gRPC
EXPOSE 5432 3306 6379 8080 9090

# Set environment variables
ENV THUNDER_DATA_DIR=/var/lib/thunderdb/data
ENV THUNDER_WAL_DIR=/var/lib/thunderdb/wal
ENV THUNDER_LOG_DIR=/var/log/thunderdb
ENV RUST_LOG=info

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/api/v1/health || exit 1

# Default command
CMD ["thunderdb", "--config", "/etc/thunderdb/thunderdb.toml"]
