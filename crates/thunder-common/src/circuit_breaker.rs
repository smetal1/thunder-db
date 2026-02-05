//! Circuit Breaker Pattern for ThunderDB
//!
//! Implements a classic three-state circuit breaker to protect external
//! service calls (FDW connectors, CDC connectors, cluster transport):
//!
//! - **Closed** — requests flow through; failures are counted.
//! - **Open** — requests are rejected immediately; after `reset_timeout`
//!   the breaker transitions to HalfOpen.
//! - **HalfOpen** — a limited number of probe requests are allowed;
//!   if they succeed the breaker closes, otherwise it re-opens.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for a circuit breaker instance.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// How long the circuit stays open before allowing probe requests.
    pub reset_timeout: Duration,
    /// Maximum concurrent probe requests in HalfOpen state.
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(30),
            half_open_max_calls: 3,
        }
    }
}

// ============================================================================
// State
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitState {
    Closed,
    Open { since: Instant },
    HalfOpen,
}

// ============================================================================
// Circuit Breaker
// ============================================================================

/// A thread-safe circuit breaker.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<CircuitState>,
    failure_count: AtomicU32,
    half_open_calls: AtomicU32,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with default configuration.
    pub fn new() -> Self {
        Self::with_config(CircuitBreakerConfig::default())
    }

    /// Create a new circuit breaker with custom configuration.
    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Mutex::new(CircuitState::Closed),
            failure_count: AtomicU32::new(0),
            half_open_calls: AtomicU32::new(0),
        }
    }

    /// Check whether a request should be allowed.
    ///
    /// Returns `true` if the request may proceed, `false` if the circuit
    /// is open and the caller should fail fast.
    pub fn allow_request(&self) -> bool {
        let mut state = self.state.lock();

        match *state {
            CircuitState::Closed => true,
            CircuitState::Open { since } => {
                if since.elapsed() >= self.config.reset_timeout {
                    // Transition to HalfOpen
                    *state = CircuitState::HalfOpen;
                    self.half_open_calls.store(0, Ordering::SeqCst);
                    // Allow the first probe
                    self.half_open_calls.fetch_add(1, Ordering::SeqCst);
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                let current = self.half_open_calls.fetch_add(1, Ordering::SeqCst);
                current < self.config.half_open_max_calls
            }
        }
    }

    /// Record a successful request.
    pub fn record_success(&self) {
        let mut state = self.state.lock();
        self.failure_count.store(0, Ordering::SeqCst);

        if *state == CircuitState::HalfOpen {
            *state = CircuitState::Closed;
            self.half_open_calls.store(0, Ordering::SeqCst);
        }
    }

    /// Record a failed request.
    pub fn record_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        let mut state = self.state.lock();

        match *state {
            CircuitState::Closed => {
                if failures >= self.config.failure_threshold {
                    *state = CircuitState::Open {
                        since: Instant::now(),
                    };
                    tracing::warn!(
                        failures = failures,
                        "Circuit breaker opened after {} consecutive failures",
                        failures
                    );
                }
            }
            CircuitState::HalfOpen => {
                // Probe failed — re-open
                *state = CircuitState::Open {
                    since: Instant::now(),
                };
                self.half_open_calls.store(0, Ordering::SeqCst);
                tracing::warn!("Circuit breaker re-opened after probe failure");
            }
            CircuitState::Open { .. } => {
                // Already open, nothing to do
            }
        }
    }

    /// Return the current state name (for metrics / logging).
    pub fn state_name(&self) -> &'static str {
        let state = self.state.lock();
        match *state {
            CircuitState::Closed => "closed",
            CircuitState::Open { .. } => "open",
            CircuitState::HalfOpen => "half_open",
        }
    }

    /// Return the consecutive failure count.
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::SeqCst)
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("state", &self.state_name())
            .field("failure_count", &self.failure_count())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_closed_allows_requests() {
        let cb = CircuitBreaker::new();
        assert!(cb.allow_request());
        assert_eq!(cb.state_name(), "closed");
    }

    #[test]
    fn test_opens_after_threshold() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout: Duration::from_secs(60),
            half_open_max_calls: 1,
        });

        // 3 failures should open the circuit
        cb.record_failure();
        cb.record_failure();
        assert!(cb.allow_request()); // still closed
        cb.record_failure();

        assert_eq!(cb.state_name(), "open");
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_success_resets_failure_count() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        });

        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // reset
        cb.record_failure();
        cb.record_failure();

        // Should still be closed (only 2 consecutive failures after reset)
        assert_eq!(cb.state_name(), "closed");
    }

    #[test]
    fn test_half_open_on_timeout() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout: Duration::from_millis(0), // immediate
            half_open_max_calls: 1,
        });

        cb.record_failure();
        assert_eq!(cb.state_name(), "open");

        // Wait for "timeout" (0ms)
        std::thread::sleep(Duration::from_millis(1));

        // Should transition to half-open and allow one probe
        assert!(cb.allow_request());
        assert_eq!(cb.state_name(), "half_open");

        // Second request should be denied
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_half_open_closes_on_success() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout: Duration::from_millis(0),
            half_open_max_calls: 1,
        });

        cb.record_failure();
        std::thread::sleep(Duration::from_millis(1));
        assert!(cb.allow_request()); // half-open

        cb.record_success();
        assert_eq!(cb.state_name(), "closed");
        assert!(cb.allow_request());
    }

    #[test]
    fn test_half_open_reopens_on_failure() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout: Duration::from_millis(0),
            half_open_max_calls: 1,
        });

        cb.record_failure();
        std::thread::sleep(Duration::from_millis(1));
        assert!(cb.allow_request()); // half-open

        cb.record_failure();
        assert_eq!(cb.state_name(), "open");
    }
}
