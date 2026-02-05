//! Metrics collection and reporting

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Thread-safe counter
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn reset(&self) -> u64 {
        self.value.swap(0, Ordering::Relaxed)
    }
}

/// Thread-safe gauge
#[derive(Debug, Default)]
pub struct Gauge {
    value: AtomicU64,
}

impl Gauge {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn set(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Histogram for tracking distributions
#[derive(Debug)]
pub struct Histogram {
    buckets: Vec<AtomicU64>,
    boundaries: Vec<f64>,
    sum: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    /// Create a new histogram with default buckets
    pub fn new() -> Self {
        Self::with_buckets(vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ])
    }

    /// Create a histogram with custom bucket boundaries
    pub fn with_buckets(boundaries: Vec<f64>) -> Self {
        let buckets = (0..=boundaries.len())
            .map(|_| AtomicU64::new(0))
            .collect();

        Self {
            buckets,
            boundaries,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record a value
    pub fn observe(&self, value: f64) {
        let bucket_idx = self
            .boundaries
            .iter()
            .position(|&b| value <= b)
            .unwrap_or(self.boundaries.len());

        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add((value * 1_000_000.0) as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the count of observations
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the sum of observations
    pub fn sum(&self) -> f64 {
        self.sum.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    /// Get bucket counts
    pub fn buckets(&self) -> Vec<(f64, u64)> {
        self.boundaries
            .iter()
            .zip(self.buckets.iter())
            .map(|(&boundary, count)| (boundary, count.load(Ordering::Relaxed)))
            .collect()
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Timer for measuring operation duration
pub struct Timer {
    start: Instant,
    histogram: Arc<Histogram>,
}

impl Timer {
    pub fn new(histogram: Arc<Histogram>) -> Self {
        Self {
            start: Instant::now(),
            histogram,
        }
    }

    pub fn observe(self) -> Duration {
        let elapsed = self.start.elapsed();
        self.histogram.observe(elapsed.as_secs_f64());
        elapsed
    }
}

/// Collection of database metrics
#[derive(Debug, Default)]
pub struct DatabaseMetrics {
    // Query metrics
    pub queries_total: Counter,
    pub queries_failed: Counter,
    pub query_duration: Histogram,

    // Transaction metrics
    pub transactions_started: Counter,
    pub transactions_committed: Counter,
    pub transactions_aborted: Counter,
    pub transaction_duration: Histogram,

    // Storage metrics
    pub pages_read: Counter,
    pub pages_written: Counter,
    pub bytes_read: Counter,
    pub bytes_written: Counter,
    pub buffer_pool_hits: Counter,
    pub buffer_pool_misses: Counter,

    // Current state gauges
    pub active_connections: Gauge,
    pub active_transactions: Gauge,
    pub buffer_pool_usage: Gauge,
    pub wal_size: Gauge,

    // Cluster metrics
    pub raft_proposals: Counter,
    pub raft_commits: Counter,
    pub regions_count: Gauge,
}

impl DatabaseMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start a query timer
    pub fn start_query_timer(&self) -> Timer {
        Timer::new(Arc::new(Histogram::new()))
    }

    /// Record a successful query
    pub fn record_query_success(&self, duration: Duration) {
        self.queries_total.inc();
        self.query_duration.observe(duration.as_secs_f64());
    }

    /// Record a failed query
    pub fn record_query_failure(&self) {
        self.queries_total.inc();
        self.queries_failed.inc();
    }

    /// Get buffer pool hit ratio
    pub fn buffer_pool_hit_ratio(&self) -> f64 {
        let hits = self.buffer_pool_hits.get();
        let misses = self.buffer_pool_misses.get();
        let total = hits + misses;

        if total == 0 {
            1.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Export metrics in Prometheus text exposition format
    pub fn export_prometheus(&self) -> String {
        let mut out = String::with_capacity(4096);

        // --- Counters ---
        Self::write_counter(&mut out, "thunderdb_queries_total",
            "Total number of queries executed", self.queries_total.get());
        Self::write_counter(&mut out, "thunderdb_queries_failed_total",
            "Total number of failed queries", self.queries_failed.get());
        Self::write_counter(&mut out, "thunderdb_transactions_started_total",
            "Total transactions started", self.transactions_started.get());
        Self::write_counter(&mut out, "thunderdb_transactions_committed_total",
            "Total committed transactions", self.transactions_committed.get());
        Self::write_counter(&mut out, "thunderdb_transactions_aborted_total",
            "Total aborted transactions", self.transactions_aborted.get());
        Self::write_counter(&mut out, "thunderdb_pages_read_total",
            "Total pages read from disk", self.pages_read.get());
        Self::write_counter(&mut out, "thunderdb_pages_written_total",
            "Total pages written to disk", self.pages_written.get());
        Self::write_counter(&mut out, "thunderdb_bytes_read_total",
            "Total bytes read", self.bytes_read.get());
        Self::write_counter(&mut out, "thunderdb_bytes_written_total",
            "Total bytes written", self.bytes_written.get());
        Self::write_counter(&mut out, "thunderdb_buffer_pool_hits_total",
            "Buffer pool cache hits", self.buffer_pool_hits.get());
        Self::write_counter(&mut out, "thunderdb_buffer_pool_misses_total",
            "Buffer pool cache misses", self.buffer_pool_misses.get());
        Self::write_counter(&mut out, "thunderdb_raft_proposals_total",
            "Total Raft proposals", self.raft_proposals.get());
        Self::write_counter(&mut out, "thunderdb_raft_commits_total",
            "Total Raft commits", self.raft_commits.get());

        // --- Gauges ---
        Self::write_gauge(&mut out, "thunderdb_active_connections",
            "Current active connections", self.active_connections.get());
        Self::write_gauge(&mut out, "thunderdb_active_transactions",
            "Current active transactions", self.active_transactions.get());
        Self::write_gauge(&mut out, "thunderdb_buffer_pool_usage_pages",
            "Buffer pool pages in use", self.buffer_pool_usage.get());
        Self::write_gauge(&mut out, "thunderdb_wal_size_bytes",
            "Current WAL size in bytes", self.wal_size.get());
        Self::write_gauge(&mut out, "thunderdb_regions_count",
            "Number of regions on this node", self.regions_count.get());

        // --- Derived gauges ---
        out.push_str(&format!(
            "# HELP thunderdb_buffer_pool_hit_ratio Buffer pool hit ratio\n\
             # TYPE thunderdb_buffer_pool_hit_ratio gauge\n\
             thunderdb_buffer_pool_hit_ratio {:.6}\n\n",
            self.buffer_pool_hit_ratio()
        ));

        // --- Histograms ---
        Self::write_histogram(&mut out, "thunderdb_query_duration_seconds",
            "Query execution duration in seconds", &self.query_duration);
        Self::write_histogram(&mut out, "thunderdb_transaction_duration_seconds",
            "Transaction duration in seconds", &self.transaction_duration);

        out
    }

    fn write_counter(out: &mut String, name: &str, help: &str, value: u64) {
        out.push_str(&format!(
            "# HELP {} {}\n# TYPE {} counter\n{} {}\n\n",
            name, help, name, name, value
        ));
    }

    fn write_gauge(out: &mut String, name: &str, help: &str, value: u64) {
        out.push_str(&format!(
            "# HELP {} {}\n# TYPE {} gauge\n{} {}\n\n",
            name, help, name, name, value
        ));
    }

    fn write_histogram(out: &mut String, name: &str, help: &str, hist: &Histogram) {
        out.push_str(&format!("# HELP {} {}\n# TYPE {} histogram\n", name, help, name));
        let mut cumulative = 0u64;
        for (boundary, count) in hist.buckets() {
            cumulative += count;
            out.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", name, boundary, cumulative));
        }
        out.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", name, hist.count()));
        out.push_str(&format!("{}_sum {}\n", name, hist.sum()));
        out.push_str(&format!("{}_count {}\n\n", name, hist.count()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::new();
        assert_eq!(counter.get(), 0);

        counter.inc();
        assert_eq!(counter.get(), 1);

        counter.add(5);
        assert_eq!(counter.get(), 6);

        let old = counter.reset();
        assert_eq!(old, 6);
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_gauge() {
        let gauge = Gauge::new();
        assert_eq!(gauge.get(), 0);

        gauge.set(10);
        assert_eq!(gauge.get(), 10);

        gauge.inc();
        assert_eq!(gauge.get(), 11);

        gauge.dec();
        assert_eq!(gauge.get(), 10);
    }

    #[test]
    fn test_histogram() {
        let hist = Histogram::new();

        hist.observe(0.05);
        hist.observe(0.1);
        hist.observe(0.5);

        assert_eq!(hist.count(), 3);
        assert!((hist.sum() - 0.65).abs() < 0.001);
    }

    #[test]
    fn test_database_metrics() {
        let metrics = DatabaseMetrics::new();

        metrics.queries_total.inc();
        metrics.buffer_pool_hits.add(90);
        metrics.buffer_pool_misses.add(10);

        assert_eq!(metrics.queries_total.get(), 1);
        assert!((metrics.buffer_pool_hit_ratio() - 0.9).abs() < 0.001);
    }
}
