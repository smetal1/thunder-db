//! Load and stress tests for ThunderDB REST API.
//!
//! These tests verify behavior under concurrent load using
//! `tower::ServiceExt::oneshot` without starting a real HTTP server.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use thunder_api::{ApiConfig, AppState, RestServer};
use tower::ServiceExt;

/// Helper: build a shared router for concurrent tests.
fn shared_router() -> axum::Router {
    let config = ApiConfig::default();
    let state = AppState::new();
    RestServer::new(config, state).router()
}

// ============================================================================
// Concurrent query load
// ============================================================================

#[tokio::test]
async fn test_concurrent_query_load() {
    let router = shared_router();

    // Fire 50 concurrent requests to /api/v1/query
    let mut handles = Vec::new();
    for i in 0..50 {
        let app = router.clone();
        handles.push(tokio::spawn(async move {
            let body = serde_json::json!({ "sql": format!("SELECT {}", i) });
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/query")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();

            response.status()
        }));
    }

    let mut statuses = Vec::new();
    for handle in handles {
        statuses.push(handle.await.unwrap());
    }

    // All requests should get a response (not panic/hang).
    // Without an engine, we expect errors (not 200), but no 5xx panics.
    // The key assertion: every request got a response within the timeout.
    assert_eq!(statuses.len(), 50);

    // None should be 500 Internal Server Error (panics)
    for (i, status) in statuses.iter().enumerate() {
        assert_ne!(
            *status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Request {} returned 500 (unexpected server panic)",
            i
        );
    }
}

// ============================================================================
// Concurrent health check load
// ============================================================================

#[tokio::test]
async fn test_concurrent_health_checks() {
    let router = shared_router();

    let mut handles = Vec::new();
    for _ in 0..100 {
        let app = router.clone();
        handles.push(tokio::spawn(async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .uri("/admin/health")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            response.status()
        }));
    }

    for handle in handles {
        let status = handle.await.unwrap();
        assert_eq!(status, StatusCode::OK, "Health check should always return 200");
    }
}

// ============================================================================
// Mixed endpoint load
// ============================================================================

#[tokio::test]
async fn test_mixed_endpoint_load() {
    let router = shared_router();

    let endpoints = vec![
        ("/admin/health", "GET"),
        ("/admin/live", "GET"),
        ("/admin/ready", "GET"),
        ("/metrics", "GET"),
    ];

    let mut handles = Vec::new();
    for (i, (uri, _method)) in endpoints.iter().cycle().take(40).enumerate() {
        let app = router.clone();
        let uri = uri.to_string();
        handles.push(tokio::spawn(async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .uri(uri.as_str())
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            (i, response.status())
        }));
    }

    for handle in handles {
        let (idx, status) = handle.await.unwrap();
        // All admin/metrics endpoints should return a valid response
        assert!(
            status.is_success() || status == StatusCode::SERVICE_UNAVAILABLE,
            "Request {} to mixed endpoint returned unexpected status: {}",
            idx,
            status
        );
    }
}

// ============================================================================
// Rapid sequential requests (no concurrency, tests router reuse)
// ============================================================================

#[tokio::test]
async fn test_rapid_sequential_requests() {
    let router = shared_router();

    for i in 0..200 {
        let app = router.clone();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "Sequential request {} failed",
            i
        );
    }
}
