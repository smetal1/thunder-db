//! Integration tests for ThunderDB REST API endpoints.
//!
//! Tests use `tower::ServiceExt::oneshot` to send requests directly to the
//! Axum router without starting a real HTTP server.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use thunder_api::{ApiConfig, AppState, RestServer};
use tower::ServiceExt;

/// Helper: build a router backed by an `AppState` with no engine attached.
fn router_without_engine() -> axum::Router {
    let config = ApiConfig::default();
    let state = AppState::new();
    RestServer::new(config, state).router()
}

// ============================================================================
// Health check
// ============================================================================

#[tokio::test]
async fn test_health_check_without_engine() {
    let router = router_without_engine();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/admin/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Without an engine the status should be "degraded"
    assert_eq!(json["status"], "degraded");
    assert_eq!(json["database_ok"], false);
}

// ============================================================================
// Liveness probe
// ============================================================================

#[tokio::test]
async fn test_liveness_probe() {
    let router = router_without_engine();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/admin/live")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["alive"], true);
    assert!(json["timestamp"].is_string());
}

// ============================================================================
// Readiness probe
// ============================================================================

#[tokio::test]
async fn test_readiness_without_engine() {
    let router = router_without_engine();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/admin/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // No engine means the server is not ready → 503
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// ============================================================================
// Prometheus metrics
// ============================================================================

#[tokio::test]
async fn test_prometheus_metrics() {
    let router = router_without_engine();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = std::str::from_utf8(&body).unwrap();

    // Prometheus metrics should contain ThunderDB-prefixed metric names
    assert!(
        text.contains("thunderdb_"),
        "Expected 'thunderdb_' prefix in metrics output, got: {}",
        &text[..text.len().min(500)]
    );
}

// ============================================================================
// Request body size limit
// ============================================================================

#[tokio::test]
async fn test_request_size_limit() {
    let config = ApiConfig::default(); // max_request_size = 10MB
    let state = AppState::new();
    let router = RestServer::new(config, state).router();

    // Create a body larger than 10MB
    let oversized = vec![b'x'; 11 * 1024 * 1024];

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(oversized))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should be rejected with 413 Payload Too Large
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
