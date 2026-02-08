//! Token-bucket rate limiter for the ThunderDB REST API.
//!
//! Limits requests per client IP address using a token bucket algorithm:
//! - Each IP gets a bucket with `capacity` tokens.
//! - Tokens refill at `rate` tokens/second.
//! - Each request consumes 1 token.
//! - When the bucket is empty, requests get 429 Too Many Requests.
//!
//! Stale buckets (no activity for 10 minutes) are evicted periodically.

use std::net::IpAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    http::{Request, Response, StatusCode},
};
use dashmap::DashMap;
use futures::future::BoxFuture;
use tower::{Layer, Service};

use crate::ApiError;

// ============================================================================
// Token Bucket
// ============================================================================

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    rate: f64,     // tokens per second
    capacity: f64,
}

impl TokenBucket {
    fn new(rate: f64, capacity: f64) -> Self {
        Self {
            tokens: capacity,
            last_refill: Instant::now(),
            rate,
            capacity,
        }
    }

    /// Try to consume one token. Returns `true` if allowed.
    fn try_consume(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Estimated seconds until the next token is available.
    fn retry_after(&self) -> f64 {
        if self.rate > 0.0 {
            (1.0 - self.tokens) / self.rate
        } else {
            60.0
        }
    }
}

// ============================================================================
// Rate Limiter State
// ============================================================================

/// Shared state for the rate limiter.
#[derive(Clone)]
pub struct RateLimiterState {
    buckets: Arc<DashMap<IpAddr, TokenBucket>>,
    rate: f64,
    capacity: f64,
}

impl RateLimiterState {
    pub fn new(requests_per_second: f64, burst: usize) -> Self {
        let state = Self {
            buckets: Arc::new(DashMap::new()),
            rate: requests_per_second,
            capacity: burst as f64,
        };

        // Spawn background cleanup task
        let buckets = state.buckets.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let cutoff = Instant::now() - Duration::from_secs(600);
                buckets.retain(|_, bucket| bucket.last_refill > cutoff);
            }
        });

        state
    }

    /// Check if a request from the given IP is allowed.
    pub fn check(&self, ip: IpAddr) -> RateLimitResult {
        let mut entry = self.buckets.entry(ip).or_insert_with(|| {
            TokenBucket::new(self.rate, self.capacity)
        });

        if entry.try_consume() {
            RateLimitResult::Allowed
        } else {
            RateLimitResult::Limited {
                retry_after: entry.retry_after().ceil() as u64,
            }
        }
    }
}

pub enum RateLimitResult {
    Allowed,
    Limited { retry_after: u64 },
}

// ============================================================================
// Tower Layer / Service
// ============================================================================

/// Tower `Layer` that wraps services with rate limiting.
#[derive(Clone)]
pub struct RateLimitLayer {
    state: RateLimiterState,
}

impl RateLimitLayer {
    pub fn new(requests_per_second: f64, burst: usize) -> Self {
        Self {
            state: RateLimiterState::new(requests_per_second, burst),
        }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            state: self.state.clone(),
        }
    }
}

/// Tower `Service` that applies rate limiting per client IP.
#[derive(Clone)]
pub struct RateLimitService<S> {
    inner: S,
    state: RateLimiterState,
}

impl<S> Service<Request<Body>> for RateLimitService<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // Extract client IP from ConnectInfo or forwarded headers
        let client_ip = extract_client_ip(&req);
        let result = self.state.check(client_ip);

        match result {
            RateLimitResult::Allowed => {
                let future = self.inner.call(req);
                Box::pin(future)
            }
            RateLimitResult::Limited { retry_after } => {
                Box::pin(async move {
                    let body = serde_json::to_string(&ApiError::new(
                        "RATE_LIMITED",
                        "Too many requests. Please retry later.",
                    ))
                    .unwrap_or_default();

                    let response = Response::builder()
                        .status(StatusCode::TOO_MANY_REQUESTS)
                        .header("Retry-After", retry_after.to_string())
                        .header("Content-Type", "application/json")
                        .body(Body::from(body))
                        .unwrap();

                    Ok(response)
                })
            }
        }
    }
}

/// Extract the client IP from the request.
///
/// Checks `X-Forwarded-For` first (for reverse proxy setups),
/// then falls back to `ConnectInfo`.
fn extract_client_ip(req: &Request<Body>) -> IpAddr {
    // Try X-Forwarded-For header (first IP in the chain)
    if let Some(forwarded) = req
        .headers()
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(first_ip) = forwarded.split(',').next() {
            if let Ok(ip) = first_ip.trim().parse::<IpAddr>() {
                return ip;
            }
        }
    }

    // Fall back to ConnectInfo
    if let Some(connect_info) = req.extensions().get::<axum::extract::ConnectInfo<std::net::SocketAddr>>() {
        return connect_info.0.ip();
    }

    // Ultimate fallback
    "127.0.0.1".parse().unwrap()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_allows_burst() {
        let mut bucket = TokenBucket::new(10.0, 5.0);

        // Should allow burst of 5
        for _ in 0..5 {
            assert!(bucket.try_consume());
        }
        // 6th should fail
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_token_bucket_refills() {
        let mut bucket = TokenBucket::new(1000.0, 5.0);

        // Consume all tokens
        for _ in 0..5 {
            assert!(bucket.try_consume());
        }
        assert!(!bucket.try_consume());

        // Simulate time passing (manually set last_refill)
        bucket.last_refill = Instant::now() - Duration::from_millis(10);

        // Should have refilled ~10 tokens (capped at capacity=5)
        assert!(bucket.try_consume());
    }

    #[test]
    fn test_retry_after() {
        let mut bucket = TokenBucket::new(1.0, 1.0);
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume());

        let retry = bucket.retry_after();
        assert!(retry > 0.0 && retry <= 1.0);
    }
}
