# ThunderDB Benchmark Image
# Contains all dependencies for running load tests

FROM postgres:15-alpine

# Install required dependencies
RUN apk add --no-cache \
    bash \
    bc \
    coreutils \
    curl \
    jq \
    procps \
    util-linux

# Set working directory
WORKDIR /scripts

# Copy test scripts
COPY config.sh init-tables.sh test-concurrency.sh test-latency.sh run-all.sh ./

# Make scripts executable
RUN chmod +x *.sh

# Create results directory
RUN mkdir -p /tmp/load-test-results

# Default command
CMD ["/scripts/run-all.sh"]
