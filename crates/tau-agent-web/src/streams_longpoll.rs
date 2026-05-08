//! Long-poll handler for durable streams.
//!
//! If events are immediately available they are returned at once.  Otherwise
//! the handler subscribes to the [`tau_streams::LiveDeliveryHub`] and waits up
//! to 30 seconds for new data.  On timeout it responds with `204 No Content`.

use std::sync::Arc;
use std::time::Duration;

use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use tau_streams::{LiveEvent, Offset, StreamId};
use tokio::time::timeout;

use crate::routes::AppState;
use crate::streams::stream_error_response;

const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Long-poll: return events immediately if available, else wait up to 30 s.
pub async fn handle_long_poll(
    stream_id: StreamId,
    offset: Offset,
    limit: usize,
    state: Arc<AppState>,
) -> Response {
    let ds = match &state.streams {
        Some(ds) => ds.clone(),
        None => {
            return (StatusCode::SERVICE_UNAVAILABLE, "streams not configured").into_response();
        }
    };

    // First attempt: read immediately.
    let result = match ds.read(&stream_id, &offset, limit) {
        Ok(r) => r,
        Err(e) => return stream_error_response(e),
    };

    if !result.events.is_empty() {
        return build_read_response(result);
    }

    if result.stream_closed {
        // Stream is done; return the empty result immediately with the closed header.
        return build_read_response(result);
    }

    // No data yet — subscribe and wait.
    let mut rx = ds.hub().subscribe(&stream_id);

    match timeout(LONG_POLL_TIMEOUT, rx.recv()).await {
        Ok(Ok(LiveEvent::Data { .. })) | Ok(Ok(LiveEvent::Closed)) => {
            // New event (or close): re-read from the store.
            match ds.read(&stream_id, &offset, limit) {
                Ok(r) => build_read_response(r),
                Err(e) => stream_error_response(e),
            }
        }
        Ok(Err(_lagged)) => {
            // Broadcast lagged — re-read anyway.
            match ds.read(&stream_id, &offset, limit) {
                Ok(r) => build_read_response(r),
                Err(e) => stream_error_response(e),
            }
        }
        Err(_timeout) => StatusCode::NO_CONTENT.into_response(),
    }
}

fn build_read_response(result: tau_streams::ReadResult) -> Response {
    use axum::body::Body;
    use axum::http::Response as HttpResponse;

    let mut lines = String::new();
    for ev in &result.events {
        // Each event serialised as a JSON line.
        let line = serde_json::json!({
            "offset": ev.offset.0,
            "data": ev.data,
        });
        lines.push_str(&line.to_string());
        lines.push('\n');
    }

    let mut builder = HttpResponse::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .header(
            "stream-next-offset",
            HeaderValue::from_str(&result.next_offset.0).unwrap_or_else(|_| HeaderValue::from_static("")),
        )
    ;

    // Presence headers: only insert when true (§5.6)
    if result.up_to_date {
        builder = builder.header("stream-up-to-date", "true");
    }
    if result.stream_closed && result.up_to_date {
        builder = builder.header("stream-closed", "true");
    }

    if result.up_to_date {
        builder = builder.header(
            "cache-control",
            "public, max-age=31536000, immutable",
        );
    }

    builder
        .body(Body::from(lines))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}
