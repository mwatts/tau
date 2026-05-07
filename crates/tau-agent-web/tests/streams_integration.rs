//! Integration tests for the durable streams HTTP routes.
//!
//! Uses `tower::ServiceExt::oneshot` to drive the axum router without
//! binding a real TCP listener.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tau_agent_web::routes::build_router;
use tau_streams::{DurableStream, LiveDeliveryHub, SqliteStore};
use tower::ServiceExt; // for `oneshot`

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn test_streams() -> Arc<DurableStream<SqliteStore>> {
    let store = SqliteStore::open_in_memory().expect("in-memory store");
    let hub = Arc::new(LiveDeliveryHub::default());
    Arc::new(DurableStream::new(store, hub))
}

fn test_app() -> axum::Router {
    let streams = test_streams();
    build_router("test-token".to_owned(), Some(streams))
}

async fn oneshot(app: axum::Router, req: Request<Body>) -> axum::response::Response {
    app.oneshot(req).await.expect("oneshot failed")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_and_read_stream() {
    let app = test_app();

    // PUT /v1/streams/my-stream → 201
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/v1/streams/my-stream")
        .body(Body::empty())
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::CREATED, "expected 201 on create");
    assert_eq!(
        resp.headers().get("stream-id").and_then(|v| v.to_str().ok()),
        Some("my-stream"),
        "stream-id header missing"
    );
    assert_eq!(
        resp.headers().get("stream-state").and_then(|v| v.to_str().ok()),
        Some("open"),
        "stream-state should be open"
    );

    // POST /v1/streams/my-stream  — append an event
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/streams/my-stream")
        .body(Body::from(b"hello world".to_vec()))
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::OK, "expected 200 on append");
    assert!(
        resp.headers().contains_key("stream-offset"),
        "stream-offset header missing"
    );

    // GET /v1/streams/my-stream — read events
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/streams/my-stream")
        .body(Body::empty())
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::OK, "expected 200 on read");
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/x-ndjson"),
    );
}

#[tokio::test]
async fn head_and_delete() {
    let app = test_app();

    // Create a stream.
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/v1/streams/hd-stream")
        .body(Body::empty())
        .unwrap();
    oneshot(app.clone(), req).await;

    // HEAD → 200 with headers.
    let req = Request::builder()
        .method(Method::HEAD)
        .uri("/v1/streams/hd-stream")
        .body(Body::empty())
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("stream-id").and_then(|v| v.to_str().ok()),
        Some("hd-stream"),
    );

    // DELETE → 204.
    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/v1/streams/hd-stream")
        .body(Body::empty())
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT, "DELETE should return 204");

    // HEAD after delete → 410 (Deleted maps to GONE).
    let req = Request::builder()
        .method(Method::HEAD)
        .uri("/v1/streams/hd-stream")
        .body(Body::empty())
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::GONE);
}

#[tokio::test]
async fn close_then_append_fails() {
    let app = test_app();

    // Create stream.
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/v1/streams/close-stream")
        .body(Body::empty())
        .unwrap();
    oneshot(app.clone(), req).await;

    // Close it.
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/streams/close-stream")
        .header("stream-closed", "true")
        .body(Body::empty())
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::OK, "expected 200 on close");
    assert_eq!(
        resp.headers()
            .get("stream-state")
            .and_then(|v| v.to_str().ok()),
        Some("closed"),
    );

    // Append after close → 409.
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/streams/close-stream")
        .body(Body::from(b"too late".to_vec()))
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn producer_dedup_returns_204() {
    let app = test_app();

    // Create stream.
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/v1/streams/dedup-stream")
        .body(Body::empty())
        .unwrap();
    oneshot(app.clone(), req).await;

    // First append with producer id/epoch/seq.
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/streams/dedup-stream?producer_id=prod1&epoch=1&seq=0")
        .body(Body::from(b"payload".to_vec()))
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::OK, "first append should be 200");

    // Duplicate append → 204.
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/streams/dedup-stream?producer_id=prod1&epoch=1&seq=0")
        .body(Body::from(b"payload".to_vec()))
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT, "dedup should be 204");
}

#[tokio::test]
async fn list_streams() {
    let app = test_app();

    // Create two streams.
    for name in &["list-s1", "list-s2"] {
        let req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/v1/streams/{name}"))
            .body(Body::empty())
            .unwrap();
        let resp = oneshot(app.clone(), req).await;
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // GET /v1/streams
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/streams")
        .body(Body::empty())
        .unwrap();
    let resp = oneshot(app.clone(), req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/json"),
    );

    let body_bytes = axum::body::to_bytes(resp.into_body(), 65536)
        .await
        .unwrap();
    let items: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    let arr = items.as_array().expect("expected JSON array");
    let ids: Vec<&str> = arr
        .iter()
        .filter_map(|v| v["id"].as_str())
        .collect();
    assert!(ids.contains(&"list-s1"), "list-s1 not in list");
    assert!(ids.contains(&"list-s2"), "list-s2 not in list");
}
