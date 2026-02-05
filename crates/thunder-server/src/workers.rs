//! Background Workers for ThunderDB
//!
//! Provides background maintenance tasks:
//! - Vacuum worker: Periodically cleans up dead tuples
//! - Checkpoint worker: Periodically writes checkpoints for durability

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::DatabaseEngine;

/// Configuration for background workers
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Interval between vacuum cycles (default: 30 seconds)
    pub vacuum_interval: Duration,
    /// Interval between checkpoints (default: 5 minutes)
    pub checkpoint_interval: Duration,
    /// Whether vacuum is enabled
    pub vacuum_enabled: bool,
    /// Whether checkpointing is enabled
    pub checkpoint_enabled: bool,
    /// Disk space check interval
    pub disk_check_interval: Duration,
    /// Disk space warning threshold (percent free)
    pub disk_warn_percent: f64,
    /// Disk space critical threshold (percent free) — triggers read-only mode
    pub disk_critical_percent: f64,
    /// Enable automatic backups
    pub backup_enabled: bool,
    /// Interval between automatic backups
    pub backup_interval: Duration,
    /// Number of backups to retain (oldest are pruned)
    pub backup_retention_count: usize,
    /// Custom backup directory (None = data_dir/backups)
    pub backup_dir: Option<std::path::PathBuf>,
    /// Enable periodic data scrubbing (checksum verification)
    pub scrub_enabled: bool,
    /// Interval between scrub passes
    pub scrub_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            vacuum_interval: Duration::from_secs(30),
            checkpoint_interval: Duration::from_secs(300), // 5 minutes
            vacuum_enabled: true,
            checkpoint_enabled: true,
            disk_check_interval: Duration::from_secs(30),
            disk_warn_percent: 10.0,
            disk_critical_percent: 5.0,
            backup_enabled: false,
            backup_interval: Duration::from_secs(86400), // 24 hours
            backup_retention_count: 7,
            backup_dir: None,
            scrub_enabled: false,
            scrub_interval: Duration::from_secs(604800), // 1 week
        }
    }
}

/// Background worker manager
pub struct WorkerManager {
    engine: Arc<DatabaseEngine>,
    config: WorkerConfig,
    cancel_token: CancellationToken,
}

impl WorkerManager {
    /// Create a new worker manager
    pub fn new(engine: Arc<DatabaseEngine>, config: WorkerConfig) -> Self {
        Self {
            engine,
            config,
            cancel_token: CancellationToken::new(),
        }
    }

    /// Get a cancellation token for shutdown signaling
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Start all background workers
    ///
    /// Returns handles to the spawned tasks
    pub fn start(&self) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();

        if self.config.vacuum_enabled {
            handles.push(self.start_vacuum_worker());
        }

        if self.config.checkpoint_enabled {
            handles.push(self.start_checkpoint_worker());
        }

        handles.push(self.start_disk_monitor());

        if self.config.backup_enabled {
            handles.push(self.start_backup_worker());
        }

        if self.config.scrub_enabled {
            handles.push(self.start_scrub_worker());
        }

        handles
    }

    /// Start the vacuum worker
    fn start_vacuum_worker(&self) -> tokio::task::JoinHandle<()> {
        let engine = self.engine.clone();
        let interval = self.config.vacuum_interval;
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            info!("Vacuum worker started (interval: {:?})", interval);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        if let Err(e) = run_vacuum_cycle(&engine).await {
                            error!("Vacuum cycle failed: {}", e);
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        info!("Vacuum worker shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start the checkpoint worker
    fn start_checkpoint_worker(&self) -> tokio::task::JoinHandle<()> {
        let engine = self.engine.clone();
        let interval = self.config.checkpoint_interval;
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            info!("Checkpoint worker started (interval: {:?})", interval);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        debug!("Running scheduled checkpoint");
                        if let Err(e) = engine.checkpoint().await {
                            error!("Checkpoint failed: {}", e);
                        } else {
                            debug!("Checkpoint completed successfully");
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        info!("Checkpoint worker shutting down");
                        // Write a final checkpoint on shutdown
                        info!("Writing final checkpoint before shutdown...");
                        if let Err(e) = engine.checkpoint().await {
                            error!("Final checkpoint failed: {}", e);
                        }
                        break;
                    }
                }
            }
        })
    }

    /// Start the disk space monitor
    fn start_disk_monitor(&self) -> tokio::task::JoinHandle<()> {
        let engine = self.engine.clone();
        let interval = self.config.disk_check_interval;
        let warn_pct = self.config.disk_warn_percent;
        let critical_pct = self.config.disk_critical_percent;
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            info!(
                "Disk monitor started (interval: {:?}, warn: {:.1}%, critical: {:.1}%)",
                interval, warn_pct, critical_pct
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        check_disk_space(&engine, warn_pct, critical_pct);
                    }
                    _ = cancel_token.cancelled() => {
                        info!("Disk monitor shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start the automatic backup worker
    fn start_backup_worker(&self) -> tokio::task::JoinHandle<()> {
        let engine = self.engine.clone();
        let interval = self.config.backup_interval;
        let retention = self.config.backup_retention_count;
        let backup_dir = self.config.backup_dir.clone();
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            info!(
                "Backup worker started (interval: {:?}, retention: {})",
                interval, retention
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        let start = std::time::Instant::now();
                        let target = backup_dir.as_ref().and_then(|p| p.to_str());
                        info!("Starting scheduled backup...");

                        match engine.backup(target).await {
                            Ok(result) => {
                                let elapsed = start.elapsed();
                                info!(
                                    "Backup completed: id={}, path={}, size={} bytes, duration={:?}",
                                    result.backup_id, result.backup_path, result.size_bytes, elapsed
                                );

                                // Prune old backups
                                prune_old_backups(&engine, retention);
                            }
                            Err(e) => {
                                error!("Scheduled backup failed: {}", e);
                            }
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        info!("Backup worker shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start the data scrubbing worker (checksum verification)
    fn start_scrub_worker(&self) -> tokio::task::JoinHandle<()> {
        let engine = self.engine.clone();
        let interval = self.config.scrub_interval;
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            info!("Scrub worker started (interval: {:?})", interval);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        info!("Starting data scrub (checksum verification)...");
                        let start = std::time::Instant::now();

                        // Scrub verifies page checksums in the buffer pool
                        let (pages_checked, corruptions) = engine.scrub_pages().await;
                        let elapsed = start.elapsed();

                        if corruptions > 0 {
                            error!(
                                "Data scrub completed: {} pages checked, {} CORRUPTIONS FOUND in {:?}",
                                pages_checked, corruptions, elapsed
                            );
                        } else {
                            info!(
                                "Data scrub completed: {} pages checked, 0 corruptions in {:?}",
                                pages_checked, elapsed
                            );
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        info!("Scrub worker shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Shutdown all workers gracefully
    pub async fn shutdown(&self) {
        info!("Shutting down background workers...");
        self.cancel_token.cancel();
    }
}

/// Run a vacuum cycle on all tables
async fn run_vacuum_cycle(engine: &DatabaseEngine) -> thunder_common::prelude::Result<()> {
    debug!("Starting vacuum cycle");

    // Get list of tables from the catalog
    let tables = engine.list_tables();

    if tables.is_empty() {
        debug!("No tables to vacuum");
        return Ok(());
    }

    let mut total_tuples_removed = 0u64;
    let mut total_bytes_freed = 0u64;

    for table_name in tables {
        match engine.vacuum_table(&table_name).await {
            Ok(result) => {
                if result.tuples_removed > 0 {
                    info!(
                        "Vacuumed table '{}': {} tuples removed, {} bytes freed",
                        table_name, result.tuples_removed, result.bytes_freed
                    );
                } else {
                    debug!("Vacuumed table '{}': no dead tuples", table_name);
                }
                total_tuples_removed += result.tuples_removed;
                total_bytes_freed += result.bytes_freed;
            }
            Err(e) => {
                warn!("Failed to vacuum table '{}': {}", table_name, e);
            }
        }
    }

    if total_tuples_removed > 0 {
        info!(
            "Vacuum cycle completed: {} total tuples removed, {} total bytes freed",
            total_tuples_removed, total_bytes_freed
        );
    } else {
        debug!("Vacuum cycle completed: no dead tuples found");
    }

    Ok(())
}

/// Prune old backups, keeping only the `retention` most recent.
fn prune_old_backups(engine: &DatabaseEngine, retention: usize) {
    match engine.list_backups() {
        Ok(mut backups) => {
            if backups.len() > retention {
                // Sort by timestamp (newest first)
                backups.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

                for old in backups.iter().skip(retention) {
                    let path = std::path::Path::new(&old.backup_path);
                    if path.exists() {
                        match std::fs::remove_dir_all(path) {
                            Ok(()) => info!("Pruned old backup: {}", old.backup_id),
                            Err(e) => warn!("Failed to prune backup {}: {}", old.backup_id, e),
                        }
                    }
                }
            }
        }
        Err(e) => warn!("Failed to list backups for pruning: {}", e),
    }
}

/// Check disk space on data and WAL directories
fn check_disk_space(engine: &DatabaseEngine, warn_pct: f64, critical_pct: f64) {
    use fs2::available_space;
    use fs2::total_space;

    let config = engine.engine_config();
    for (label, path) in [("data", &config.data_dir), ("WAL", &config.wal_dir)] {
        let total = match total_space(path) {
            Ok(t) => t,
            Err(e) => {
                warn!("Failed to check total disk space for {} dir ({:?}): {}", label, path, e);
                continue;
            }
        };
        let avail = match available_space(path) {
            Ok(a) => a,
            Err(e) => {
                warn!("Failed to check available disk space for {} dir ({:?}): {}", label, path, e);
                continue;
            }
        };
        if total == 0 {
            continue;
        }
        let free_pct = (avail as f64 / total as f64) * 100.0;

        if free_pct < critical_pct {
            if !engine.is_read_only() {
                error!(
                    "{} disk critically low: {:.1}% free ({} / {} bytes). \
                     Switching to READ-ONLY mode.",
                    label, free_pct, avail, total
                );
                engine.set_read_only(true);
            }
        } else if free_pct < warn_pct {
            warn!(
                "{} disk space low: {:.1}% free ({} / {} bytes)",
                label, free_pct, avail, total
            );
            // Recover from read-only if above critical threshold
            if engine.is_read_only() {
                info!("{} disk space recovered above critical threshold, disabling read-only mode", label);
                engine.set_read_only(false);
            }
        } else if engine.is_read_only() {
            // Well above thresholds, recover from read-only
            info!("{} disk space recovered ({:.1}% free), disabling read-only mode", label, free_pct);
            engine.set_read_only(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = WorkerConfig::default();
        assert_eq!(config.vacuum_interval, Duration::from_secs(30));
        assert_eq!(config.checkpoint_interval, Duration::from_secs(300));
        assert!(config.vacuum_enabled);
        assert!(config.checkpoint_enabled);
    }
}
