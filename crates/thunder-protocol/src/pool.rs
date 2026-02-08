//! Server-Side Connection Pool
//!
//! Provides connection pooling for handling concurrent client requests.
//! Features:
//! - Pre-allocated session pools for fast connection handling
//! - Concurrent request handling with work-stealing
//! - Connection reuse across requests
//! - Health monitoring and cleanup

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

// Session imports not currently used but kept for future session management
#[allow(unused_imports)]
use crate::session::{SessionContext, SessionManager, SessionConfig};

/// Server-side connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of pooled sessions
    pub min_size: usize,
    /// Maximum number of pooled sessions
    pub max_size: usize,
    /// Maximum time to wait for a session (ms)
    pub acquire_timeout_ms: u64,
    /// Time before idle session is cleaned up (ms)
    pub idle_timeout_ms: u64,
    /// Maximum session lifetime (ms)
    pub max_lifetime_ms: u64,
    /// Health check interval (ms)
    pub health_check_interval_ms: u64,
    /// Maximum concurrent requests per connection
    pub max_requests_per_connection: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_size: 10,
            max_size: 100,
            acquire_timeout_ms: 30_000,
            idle_timeout_ms: 300_000, // 5 minutes
            max_lifetime_ms: 3600_000, // 1 hour
            health_check_interval_ms: 30_000,
            max_requests_per_connection: 10_000,
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total sessions created
    pub sessions_created: u64,
    /// Total sessions acquired
    pub sessions_acquired: u64,
    /// Total sessions returned
    pub sessions_returned: u64,
    /// Current active sessions
    pub active_sessions: usize,
    /// Current idle sessions
    pub idle_sessions: usize,
    /// Total requests processed
    pub requests_processed: u64,
    /// Average acquire time (ms)
    pub avg_acquire_time_ms: f64,
    /// Peak concurrent sessions
    pub peak_concurrent: usize,
}

/// A pooled session wrapper that returns to pool on drop
pub struct PooledConnection {
    session: Option<PooledSession>,
    pool: Arc<ServerConnectionPoolInner>,
    acquired_at: Instant,
}

impl PooledConnection {
    /// Get a reference to the underlying session
    pub fn session(&self) -> &PooledSession {
        self.session.as_ref().unwrap()
    }

    /// Get a mutable reference to the underlying session
    pub fn session_mut(&mut self) -> &mut PooledSession {
        self.session.as_mut().unwrap()
    }

    /// Get the session ID
    pub fn session_id(&self) -> uuid::Uuid {
        self.session.as_ref().unwrap().id
    }

    /// Mark session as dirty (needs cleanup before reuse)
    pub fn mark_dirty(&mut self) {
        if let Some(ref mut session) = self.session {
            session.is_dirty = true;
        }
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let acquire_time = self.acquired_at.elapsed().as_millis() as u64;
            self.pool.return_session(session, acquire_time);
        }
    }
}

/// Internal session state
#[derive(Debug)]
pub struct PooledSession {
    /// Unique session ID
    pub id: uuid::Uuid,
    /// When the session was created
    pub created_at: Instant,
    /// Last time session was used
    pub last_used: Instant,
    /// Number of requests processed
    pub request_count: u64,
    /// Whether session needs cleanup before reuse
    pub is_dirty: bool,
    /// Associated user
    pub user: Option<String>,
    /// Associated database
    pub database: Option<String>,
}

impl PooledSession {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            id: uuid::Uuid::new_v4(),
            created_at: now,
            last_used: now,
            request_count: 0,
            is_dirty: false,
            user: None,
            database: None,
        }
    }

    fn reset(&mut self) {
        self.last_used = Instant::now();
        self.is_dirty = false;
    }

    fn is_expired(&self, max_lifetime: Duration) -> bool {
        self.created_at.elapsed() > max_lifetime
    }

    fn is_idle_too_long(&self, idle_timeout: Duration) -> bool {
        self.last_used.elapsed() > idle_timeout
    }
}

/// Internal pool state
struct ServerConnectionPoolInner {
    config: PoolConfig,
    available: Mutex<VecDeque<PooledSession>>,
    semaphore: Semaphore,
    #[allow(dead_code)]
    stats: RwLock<PoolStats>,
    active_count: AtomicUsize,
    total_created: AtomicU64,
    total_acquired: AtomicU64,
    total_returned: AtomicU64,
    total_requests: AtomicU64,
    total_acquire_time_ms: AtomicU64,
    peak_concurrent: AtomicUsize,
}

impl ServerConnectionPoolInner {
    fn new(config: PoolConfig) -> Self {
        Self {
            semaphore: Semaphore::new(config.max_size),
            available: Mutex::new(VecDeque::with_capacity(config.max_size)),
            stats: RwLock::new(PoolStats::default()),
            active_count: AtomicUsize::new(0),
            total_created: AtomicU64::new(0),
            total_acquired: AtomicU64::new(0),
            total_returned: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            total_acquire_time_ms: AtomicU64::new(0),
            peak_concurrent: AtomicUsize::new(0),
            config,
        }
    }

    fn return_session(&self, mut session: PooledSession, acquire_time_ms: u64) {
        self.total_returned.fetch_add(1, Ordering::Relaxed);
        self.total_acquire_time_ms.fetch_add(acquire_time_ms, Ordering::Relaxed);
        self.active_count.fetch_sub(1, Ordering::Relaxed);

        let max_lifetime = Duration::from_millis(self.config.max_lifetime_ms);

        // Check if session should be discarded
        let should_discard = session.is_dirty
            || session.is_expired(max_lifetime)
            || session.request_count >= self.config.max_requests_per_connection as u64;

        if should_discard {
            debug!(session_id = %session.id, "Discarding session (dirty or expired)");
            self.semaphore.add_permits(1);
        } else {
            // Reset and return to pool
            session.reset();
            let mut available = self.available.lock();
            if available.len() < self.config.max_size {
                available.push_back(session);
            } else {
                // Pool is full, discard
                self.semaphore.add_permits(1);
            }
        }
    }
}

/// Server-side connection pool
pub struct ServerConnectionPool {
    inner: Arc<ServerConnectionPoolInner>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl ServerConnectionPool {
    /// Create a new connection pool
    pub fn new(config: PoolConfig) -> Self {
        let min_size = config.min_size;
        let inner = Arc::new(ServerConnectionPoolInner::new(config));

        // Pre-create minimum sessions
        {
            let mut available = inner.available.lock();
            for _ in 0..min_size {
                available.push_back(PooledSession::new());
                inner.total_created.fetch_add(1, Ordering::Relaxed);
            }
        }

        let pool = Self {
            inner,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        };

        // Start background health check
        pool.start_health_check();

        pool
    }

    /// Acquire a session from the pool
    pub async fn acquire(&self) -> Option<PooledConnection> {
        let timeout = Duration::from_millis(self.inner.config.acquire_timeout_ms);
        let start = Instant::now();

        // Try to acquire semaphore permit with timeout
        let permit = match tokio::time::timeout(timeout, self.inner.semaphore.acquire()).await {
            Ok(Ok(permit)) => permit,
            _ => {
                warn!("Failed to acquire session from pool (timeout)");
                return None;
            }
        };

        // Don't use the permit (we're tracking with active_count)
        permit.forget();

        // Try to get existing session from pool
        let session = {
            let mut available = self.inner.available.lock();
            available.pop_front()
        };

        let session = session.unwrap_or_else(|| {
            // Create new session
            self.inner.total_created.fetch_add(1, Ordering::Relaxed);
            PooledSession::new()
        });

        // Update stats
        self.inner.total_acquired.fetch_add(1, Ordering::Relaxed);
        let active = self.inner.active_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Update peak concurrent
        loop {
            let peak = self.inner.peak_concurrent.load(Ordering::Relaxed);
            if active <= peak {
                break;
            }
            if self.inner.peak_concurrent.compare_exchange(
                peak, active, Ordering::Relaxed, Ordering::Relaxed
            ).is_ok() {
                break;
            }
        }

        Some(PooledConnection {
            session: Some(session),
            pool: self.inner.clone(),
            acquired_at: start,
        })
    }

    /// Try to acquire without waiting
    pub fn try_acquire(&self) -> Option<PooledConnection> {
        let permit = self.inner.semaphore.try_acquire().ok()?;
        permit.forget();

        let session = {
            let mut available = self.inner.available.lock();
            available.pop_front()
        };

        let session = session.unwrap_or_else(|| {
            self.inner.total_created.fetch_add(1, Ordering::Relaxed);
            PooledSession::new()
        });

        self.inner.total_acquired.fetch_add(1, Ordering::Relaxed);
        let active = self.inner.active_count.fetch_add(1, Ordering::Relaxed) + 1;

        loop {
            let peak = self.inner.peak_concurrent.load(Ordering::Relaxed);
            if active <= peak {
                break;
            }
            if self.inner.peak_concurrent.compare_exchange(
                peak, active, Ordering::Relaxed, Ordering::Relaxed
            ).is_ok() {
                break;
            }
        }

        Some(PooledConnection {
            session: Some(session),
            pool: self.inner.clone(),
            acquired_at: Instant::now(),
        })
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let total_acquired = self.inner.total_acquired.load(Ordering::Relaxed);
        let total_acquire_time = self.inner.total_acquire_time_ms.load(Ordering::Relaxed);
        let avg_acquire = if total_acquired > 0 {
            total_acquire_time as f64 / total_acquired as f64
        } else {
            0.0
        };

        PoolStats {
            sessions_created: self.inner.total_created.load(Ordering::Relaxed),
            sessions_acquired: total_acquired,
            sessions_returned: self.inner.total_returned.load(Ordering::Relaxed),
            active_sessions: self.inner.active_count.load(Ordering::Relaxed),
            idle_sessions: self.inner.available.lock().len(),
            requests_processed: self.inner.total_requests.load(Ordering::Relaxed),
            avg_acquire_time_ms: avg_acquire,
            peak_concurrent: self.inner.peak_concurrent.load(Ordering::Relaxed),
        }
    }

    /// Get current pool size (active + idle)
    pub fn size(&self) -> usize {
        self.inner.active_count.load(Ordering::Relaxed) + self.inner.available.lock().len()
    }

    /// Get number of active sessions
    pub fn active(&self) -> usize {
        self.inner.active_count.load(Ordering::Relaxed)
    }

    /// Get number of idle sessions
    pub fn idle(&self) -> usize {
        self.inner.available.lock().len()
    }

    /// Shutdown the pool
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    fn start_health_check(&self) {
        let inner = self.inner.clone();
        let shutdown = self.shutdown.clone();
        let interval = Duration::from_millis(inner.config.health_check_interval_ms);
        let idle_timeout = Duration::from_millis(inner.config.idle_timeout_ms);
        let max_lifetime = Duration::from_millis(inner.config.max_lifetime_ms);
        let min_size = inner.config.min_size;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        // Cleanup expired/idle sessions
                        let mut removed = 0;
                        {
                            let mut available = inner.available.lock();
                            available.retain(|session| {
                                let keep = !session.is_expired(max_lifetime)
                                    && !session.is_idle_too_long(idle_timeout);
                                if !keep {
                                    removed += 1;
                                }
                                keep
                            });
                        }

                        if removed > 0 {
                            debug!(removed, "Cleaned up idle/expired sessions");
                            inner.semaphore.add_permits(removed);
                        }

                        // Ensure minimum pool size
                        let current_idle = inner.available.lock().len();
                        if current_idle < min_size {
                            let to_create = min_size - current_idle;
                            let mut available = inner.available.lock();
                            for _ in 0..to_create {
                                available.push_back(PooledSession::new());
                                inner.total_created.fetch_add(1, Ordering::Relaxed);
                            }
                            debug!(created = to_create, "Replenished pool to min_size");
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Connection pool health check shutting down");
                        break;
                    }
                }
            }
        });
    }
}

impl Drop for ServerConnectionPool {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

/// Request multiplexer for handling concurrent requests on a single connection
pub struct RequestMultiplexer {
    /// Maximum concurrent requests
    max_concurrent: usize,
    /// Current in-flight requests
    in_flight: AtomicUsize,
    /// Semaphore for limiting concurrency
    semaphore: Semaphore,
}

impl RequestMultiplexer {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            in_flight: AtomicUsize::new(0),
            semaphore: Semaphore::new(max_concurrent),
        }
    }

    /// Try to acquire a request slot
    pub async fn acquire(&self) -> Option<RequestGuard> {
        let permit = self.semaphore.acquire().await.ok()?;
        permit.forget();
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Some(RequestGuard { multiplexer: self })
    }

    /// Try to acquire without waiting
    pub fn try_acquire(&self) -> Option<RequestGuard> {
        let permit = self.semaphore.try_acquire().ok()?;
        permit.forget();
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Some(RequestGuard { multiplexer: self })
    }

    /// Get number of in-flight requests
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Check if at capacity
    pub fn is_full(&self) -> bool {
        self.in_flight.load(Ordering::Relaxed) >= self.max_concurrent
    }
}

/// Guard for a request slot
pub struct RequestGuard<'a> {
    multiplexer: &'a RequestMultiplexer,
}

impl Drop for RequestGuard<'_> {
    fn drop(&mut self) {
        self.multiplexer.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.multiplexer.semaphore.add_permits(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pool_acquire_release() {
        let config = PoolConfig {
            min_size: 2,
            max_size: 5,
            ..Default::default()
        };
        let pool = ServerConnectionPool::new(config);

        // Should have min_size sessions pre-created
        assert_eq!(pool.idle(), 2);

        // Acquire a session
        let session1 = pool.acquire().await.unwrap();
        assert_eq!(pool.active(), 1);
        assert_eq!(pool.idle(), 1);

        // Acquire another
        let session2 = pool.acquire().await.unwrap();
        assert_eq!(pool.active(), 2);
        assert_eq!(pool.idle(), 0);

        // Return sessions
        drop(session1);
        assert_eq!(pool.active(), 1);
        assert_eq!(pool.idle(), 1);

        drop(session2);
        assert_eq!(pool.active(), 0);
        assert_eq!(pool.idle(), 2);
    }

    #[tokio::test]
    async fn test_pool_max_connections() {
        let config = PoolConfig {
            min_size: 1,
            max_size: 2,
            acquire_timeout_ms: 100,
            ..Default::default()
        };
        let pool = ServerConnectionPool::new(config);

        let _s1 = pool.acquire().await.unwrap();
        let _s2 = pool.acquire().await.unwrap();

        // Third should timeout
        let result = pool.acquire().await;
        assert!(result.is_none());
    }

    #[test]
    fn test_request_multiplexer() {
        let mux = RequestMultiplexer::new(3);

        let _g1 = mux.try_acquire().unwrap();
        assert_eq!(mux.in_flight(), 1);

        let _g2 = mux.try_acquire().unwrap();
        let _g3 = mux.try_acquire().unwrap();
        assert!(mux.is_full());

        // Fourth should fail
        assert!(mux.try_acquire().is_none());

        // Drop one and try again
        drop(_g1);
        assert!(!mux.is_full());
        let _g4 = mux.try_acquire().unwrap();
    }
}
