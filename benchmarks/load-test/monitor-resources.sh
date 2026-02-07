#!/bin/bash
# Resource Monitoring Script
# ==========================
# Monitors system resources during load tests:
# - CPU usage
# - Memory usage
# - Disk I/O
# - Network I/O
# - ThunderDB-specific metrics

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

OUTPUT_FILE="$RUN_DIR/metrics/system_resources.csv"
THUNDERDB_METRICS_FILE="$RUN_DIR/metrics/thunderdb_metrics.csv"

log_info "Starting resource monitoring (interval: ${MONITOR_INTERVAL}s)..."

# CSV headers
echo "timestamp,cpu_percent,memory_percent,memory_used_mb,disk_read_mb_s,disk_write_mb_s,net_recv_mb_s,net_send_mb_s,load_avg_1m,load_avg_5m" > "$OUTPUT_FILE"
echo "timestamp,active_connections,queries_per_sec,buffer_pool_hit_ratio,wal_size_mb,active_transactions" > "$THUNDERDB_METRICS_FILE"

# Previous values for rate calculations
prev_disk_read=0
prev_disk_write=0
prev_net_recv=0
prev_net_send=0
prev_time=$(date +%s)

# Get initial disk/network stats
if [ -f /proc/diskstats ]; then
    prev_disk_read=$(awk '/sda/ {print $6}' /proc/diskstats 2>/dev/null | head -1 || echo 0)
    prev_disk_write=$(awk '/sda/ {print $10}' /proc/diskstats 2>/dev/null | head -1 || echo 0)
fi

if [ -f /proc/net/dev ]; then
    prev_net_recv=$(awk '/eth0:|en0:/ {gsub(":", ""); print $2}' /proc/net/dev 2>/dev/null | head -1 || echo 0)
    prev_net_send=$(awk '/eth0:|en0:/ {gsub(":", ""); print $10}' /proc/net/dev 2>/dev/null | head -1 || echo 0)
fi

# Monitoring loop
while true; do
    sleep "$MONITOR_INTERVAL"

    timestamp=$(date -Iseconds)
    current_time=$(date +%s)
    elapsed=$((current_time - prev_time))

    # ========================================
    # System Metrics
    # ========================================

    # CPU usage
    if command -v mpstat &> /dev/null; then
        cpu_percent=$(mpstat 1 1 | awk '/Average/ {print 100 - $NF}' 2>/dev/null || echo "0")
    elif [ -f /proc/stat ]; then
        cpu_percent=$(awk '/^cpu / {
            total = $2 + $3 + $4 + $5 + $6 + $7 + $8;
            idle = $5;
            printf "%.1f", 100 * (1 - idle / total);
        }' /proc/stat 2>/dev/null || echo "0")
    elif command -v top &> /dev/null; then
        # macOS
        cpu_percent=$(top -l 1 | grep "CPU usage" | awk '{print $3}' | tr -d '%' 2>/dev/null || echo "0")
    else
        cpu_percent="0"
    fi

    # Memory usage
    if [ -f /proc/meminfo ]; then
        memory_info=$(awk '/MemTotal|MemAvailable/ {print $2}' /proc/meminfo)
        mem_total=$(echo "$memory_info" | head -1)
        mem_available=$(echo "$memory_info" | tail -1)
        memory_used_mb=$(( (mem_total - mem_available) / 1024 ))
        memory_percent=$(echo "scale=1; 100 * (1 - $mem_available / $mem_total)" | bc)
    elif command -v vm_stat &> /dev/null; then
        # macOS
        page_size=$(vm_stat | head -1 | awk -F'page size of ' '{print $2}' | awk '{print $1}')
        pages_free=$(vm_stat | awk '/Pages free/ {print $3}' | tr -d '.')
        pages_active=$(vm_stat | awk '/Pages active/ {print $3}' | tr -d '.')
        pages_inactive=$(vm_stat | awk '/Pages inactive/ {print $3}' | tr -d '.')
        pages_wired=$(vm_stat | awk '/Pages wired/ {print $4}' | tr -d '.')

        mem_used=$(( (pages_active + pages_wired) * page_size / 1024 / 1024 ))
        mem_total=$(( (pages_free + pages_active + pages_inactive + pages_wired) * page_size / 1024 / 1024 ))
        memory_used_mb=$mem_used
        memory_percent=$(echo "scale=1; 100 * $mem_used / $mem_total" | bc)
    else
        memory_used_mb="0"
        memory_percent="0"
    fi

    # Disk I/O
    disk_read_mb_s="0"
    disk_write_mb_s="0"
    if [ -f /proc/diskstats ] && [ $elapsed -gt 0 ]; then
        curr_read=$(awk '/sda/ {print $6}' /proc/diskstats 2>/dev/null | head -1 || echo 0)
        curr_write=$(awk '/sda/ {print $10}' /proc/diskstats 2>/dev/null | head -1 || echo 0)
        # Sectors are typically 512 bytes
        disk_read_mb_s=$(echo "scale=2; ($curr_read - $prev_disk_read) * 512 / 1024 / 1024 / $elapsed" | bc 2>/dev/null || echo "0")
        disk_write_mb_s=$(echo "scale=2; ($curr_write - $prev_disk_write) * 512 / 1024 / 1024 / $elapsed" | bc 2>/dev/null || echo "0")
        prev_disk_read=$curr_read
        prev_disk_write=$curr_write
    elif command -v iostat &> /dev/null; then
        # macOS
        io=$(iostat -d -c 1 2>/dev/null | tail -1)
        disk_read_mb_s=$(echo "$io" | awk '{print $3/1024}' 2>/dev/null || echo "0")
        disk_write_mb_s=$(echo "$io" | awk '{print $4/1024}' 2>/dev/null || echo "0")
    fi

    # Network I/O
    net_recv_mb_s="0"
    net_send_mb_s="0"
    if [ -f /proc/net/dev ] && [ $elapsed -gt 0 ]; then
        curr_recv=$(awk '/eth0:|ens/ {gsub(":", ""); print $2}' /proc/net/dev 2>/dev/null | head -1 || echo 0)
        curr_send=$(awk '/eth0:|ens/ {gsub(":", ""); print $10}' /proc/net/dev 2>/dev/null | head -1 || echo 0)
        net_recv_mb_s=$(echo "scale=2; ($curr_recv - $prev_net_recv) / 1024 / 1024 / $elapsed" | bc 2>/dev/null || echo "0")
        net_send_mb_s=$(echo "scale=2; ($curr_send - $prev_net_send) / 1024 / 1024 / $elapsed" | bc 2>/dev/null || echo "0")
        prev_net_recv=$curr_recv
        prev_net_send=$curr_send
    elif command -v netstat &> /dev/null; then
        # macOS - simplified
        net_recv_mb_s="0"
        net_send_mb_s="0"
    fi

    # Load average
    if [ -f /proc/loadavg ]; then
        load_info=$(cat /proc/loadavg)
        load_avg_1m=$(echo "$load_info" | awk '{print $1}')
        load_avg_5m=$(echo "$load_info" | awk '{print $2}')
    elif command -v uptime &> /dev/null; then
        load_avg_1m=$(uptime | awk -F'load average:' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
        load_avg_5m=$(uptime | awk -F'load average:' '{print $2}' | awk -F',' '{print $2}' | tr -d ' ')
    else
        load_avg_1m="0"
        load_avg_5m="0"
    fi

    # Record system metrics
    echo "$timestamp,$cpu_percent,$memory_percent,$memory_used_mb,$disk_read_mb_s,$disk_write_mb_s,$net_recv_mb_s,$net_send_mb_s,$load_avg_1m,$load_avg_5m" >> "$OUTPUT_FILE"

    # ========================================
    # ThunderDB-specific Metrics
    # ========================================

    # Try to get ThunderDB metrics (if available)
    active_connections="0"
    queries_per_sec="0"
    buffer_pool_hit_ratio="0"
    wal_size_mb="0"
    active_transactions="0"

    # Attempt to query ThunderDB for internal metrics
    # This assumes ThunderDB exposes some introspection queries
    if PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -c "SELECT 1" > /dev/null 2>&1; then
        # Get connection count (if available through session table)
        active_connections=$(PGPASSWORD="$THUNDERDB_PASSWORD" psql -h "$THUNDERDB_HOST" -p "$THUNDERDB_PG_PORT" -U "$THUNDERDB_USER" -d "$THUNDERDB_DATABASE" -t -c "SELECT COUNT(*) FROM thunderdb.sessions" 2>/dev/null | tr -d ' ' || echo "0")

        # Get WAL size if data dir is accessible
        if [ -d "${DATA_DIR:-./data}/wal" ]; then
            wal_size_mb=$(du -sm "${DATA_DIR:-./data}/wal" 2>/dev/null | awk '{print $1}' || echo "0")
        fi
    fi

    # Record ThunderDB metrics
    echo "$timestamp,$active_connections,$queries_per_sec,$buffer_pool_hit_ratio,$wal_size_mb,$active_transactions" >> "$THUNDERDB_METRICS_FILE"

    prev_time=$current_time
done
