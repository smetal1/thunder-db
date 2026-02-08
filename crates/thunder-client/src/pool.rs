//! Connection Pool
//!
//! Provides a connection pool for efficient connection reuse.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tokio::time::{interval, timeout};
use tracing::{debug, info, trace};

use thunder_common::prelude::*;

use crate::connection::{Connection, ConnectionState};
use crate::ClientConfig;

// ============================================================================
// Pool Configuration
// ============================================================================

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub min_size: usize,
    pub max_size: usize,
    pub acquire_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub max_lifetime_ms: u64,
    pub health_check_interval_ms: u64,
    pub test_on_checkout: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_size: 1,
            max_size: 10,
            acquire_timeout_ms: 30000,
            idle_timeout_ms: 600000,
            max_lifetime_ms: 3600000,
            health_check_interval_ms: 30000,
            test_on_checkout: true,
        }
    }
}

// ============================================================================
// Pooled Connection
// ============================================================================

/// A connection wrapper that returns to pool on drop
pub struct PooledConnection {
    connection: Option<Connection>,
    pool: Arc<ConnectionPoolInner>,
}

impl PooledConnection {
    pub fn connection(&self) -> &Connection {
        self.connection.as_ref().expect("Connection taken")
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        self.connection.as_mut().expect("Connection taken")
    }

    pub async fn query(&mut self, sql: &str) -> Result<crate::QueryResult> {
        self.connection_mut().query(sql).await
    }

    pub async fn execute(&mut self, sql: &str) -> Result<u64> {
        self.connection_mut().execute(sql).await
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            self.pool.return_connection(conn);
        }
    }
}

// ============================================================================
// Connection Pool Inner
// ============================================================================

struct ConnectionPoolInner {
    pool_config: PoolConfig,
    client_config: ClientConfig,
    available: Mutex<VecDeque<Connection>>,
    semaphore: Semaphore,
    total_created: AtomicU64,
    active_count: AtomicUsize,
    total_count: AtomicUsize,
    next_id: AtomicU64,
    closed: parking_lot::RwLock<bool>,
    #[allow(dead_code)]
    created_at: Instant,
    stats: parking_lot::RwLock<PoolStats>,
}

impl ConnectionPoolInner {
    fn new(pool_config: PoolConfig, client_config: ClientConfig) -> Self {
        let max_size = pool_config.max_size;
        Self {
            pool_config,
            client_config,
            available: Mutex::new(VecDeque::new()),
            semaphore: Semaphore::new(max_size),
            total_created: AtomicU64::new(0),
            active_count: AtomicUsize::new(0),
            total_count: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            closed: parking_lot::RwLock::new(false),
            created_at: Instant::now(),
            stats: parking_lot::RwLock::new(PoolStats::default()),
        }
    }

    fn return_connection(&self, conn: Connection) {
        if *self.closed.read() {
            self.total_count.fetch_sub(1, Ordering::Relaxed);
            return;
        }

        if conn.state() == ConnectionState::Ready {
            self.available.lock().push_back(conn);
        } else {
            self.total_count.fetch_sub(1, Ordering::Relaxed);
            debug!("Discarding unhealthy connection");
        }

        self.active_count.fetch_sub(1, Ordering::Relaxed);
        self.semaphore.add_permits(1);
        self.stats.write().connections_returned += 1;
    }
}

// ============================================================================
// Connection Pool
// ============================================================================

/// Connection pool for ThunderDB
pub struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

impl ConnectionPool {
    pub async fn new(pool_config: PoolConfig, client_config: ClientConfig) -> Result<Self> {
        let min_size = pool_config.min_size;
        let inner = Arc::new(ConnectionPoolInner::new(pool_config, client_config));
        let pool = Self { inner };

        for _ in 0..min_size {
            if let Ok(conn) = pool.create_connection().await {
                pool.inner.available.lock().push_back(conn);
                pool.inner.total_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        info!(min = min_size, max = pool.inner.pool_config.max_size, "Connection pool created");

        let pool_clone = pool.clone();
        tokio::spawn(async move {
            pool_clone.health_check_loop().await;
        });

        Ok(pool)
    }

    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }

    pub async fn acquire(&self) -> Result<PooledConnection> {
        if *self.inner.closed.read() {
            return Err(Error::ResourceExhausted("Pool is closed".to_string()));
        }

        self.inner.stats.write().acquire_attempts += 1;

        let acquire_timeout = Duration::from_millis(self.inner.pool_config.acquire_timeout_ms);

        let _permit = timeout(acquire_timeout, self.inner.semaphore.acquire())
            .await
            .map_err(|_| Error::Timeout("Acquire timeout".to_string()))?
            .map_err(|_| Error::Internal("Semaphore closed".to_string()))?;

        loop {
            let conn = self.inner.available.lock().pop_front();

            if let Some(mut conn) = conn {
                if self.inner.pool_config.test_on_checkout {
                    if conn.ping().await.is_err() {
                        self.inner.total_count.fetch_sub(1, Ordering::Relaxed);
                        debug!("Discarding dead connection on checkout");
                        continue;
                    }
                }

                self.inner.active_count.fetch_add(1, Ordering::Relaxed);
                self.inner.stats.write().connections_acquired += 1;
                std::mem::forget(_permit);

                return Ok(PooledConnection {
                    connection: Some(conn),
                    pool: Arc::clone(&self.inner),
                });
            }

            let conn = self.create_connection().await?;
            self.inner.total_count.fetch_add(1, Ordering::Relaxed);
            self.inner.active_count.fetch_add(1, Ordering::Relaxed);
            self.inner.stats.write().connections_acquired += 1;
            self.inner.stats.write().connections_created += 1;
            std::mem::forget(_permit);

            return Ok(PooledConnection {
                connection: Some(conn),
                pool: Arc::clone(&self.inner),
            });
        }
    }

    async fn create_connection(&self) -> Result<Connection> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let mut conn = Connection::new(id, self.inner.client_config.clone());
        conn.connect().await?;
        self.inner.total_created.fetch_add(1, Ordering::Relaxed);
        debug!(id = id, "Created new connection");
        Ok(conn)
    }

    async fn health_check_loop(&self) {
        let mut check_interval = interval(Duration::from_millis(
            self.inner.pool_config.health_check_interval_ms,
        ));

        loop {
            check_interval.tick().await;
            if *self.inner.closed.read() {
                break;
            }
            self.run_health_check().await;
        }
    }

    async fn run_health_check(&self) {
        let now = Instant::now();
        let idle_timeout = Duration::from_millis(self.inner.pool_config.idle_timeout_ms);

        {
            let mut available = self.inner.available.lock();
            let mut retained = VecDeque::new();

            while let Some(conn) = available.pop_front() {
                let idle_time = now.duration_since(conn.last_activity());
                if idle_time <= idle_timeout {
                    retained.push_back(conn);
                } else {
                    self.inner.total_count.fetch_sub(1, Ordering::Relaxed);
                    debug!(id = conn.id(), "Connection exceeded idle timeout");
                }
            }

            *available = retained;
        }

        let total = self.inner.total_count.load(Ordering::Relaxed);
        let min_size = self.inner.pool_config.min_size;
        if total < min_size {
            for _ in 0..(min_size - total) {
                if let Ok(conn) = self.create_connection().await {
                    self.inner.available.lock().push_back(conn);
                    self.inner.total_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        trace!(
            total = self.inner.total_count.load(Ordering::Relaxed),
            active = self.inner.active_count.load(Ordering::Relaxed),
            "Health check complete"
        );
    }

    pub fn stats(&self) -> PoolStats {
        let stats = self.inner.stats.read().clone();
        PoolStats {
            total_connections: self.inner.total_count.load(Ordering::Relaxed),
            active_connections: self.inner.active_count.load(Ordering::Relaxed),
            available_connections: self.inner.available.lock().len(),
            ..stats
        }
    }

    pub async fn close(&self) {
        *self.inner.closed.write() = true;
        let mut available = self.inner.available.lock();
        for mut conn in available.drain(..) {
            let _ = conn.close().await;
        }
        info!("Connection pool closed");
    }

    pub fn size(&self) -> usize {
        self.inner.total_count.load(Ordering::Relaxed)
    }

    pub fn active(&self) -> usize {
        self.inner.active_count.load(Ordering::Relaxed)
    }

    pub fn available(&self) -> usize {
        self.inner.available.lock().len()
    }
}

// ============================================================================
// Pool Statistics
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    pub total_connections: usize,
    pub active_connections: usize,
    pub available_connections: usize,
    pub connections_created: u64,
    pub acquire_attempts: u64,
    pub connections_acquired: u64,
    pub connections_returned: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.min_size, 1);
        assert_eq!(config.max_size, 10);
    }
}
