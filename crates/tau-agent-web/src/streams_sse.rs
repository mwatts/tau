//! Server-Sent Events (SSE) handler for live stream delivery.
//!
//! Two-phase delivery:
//! 1. Catch-up: historical events read in batches of 100.
//! 2. Live: events from the [`tau_streams::LiveDeliveryHub`].
//!
//! A control SSE frame with `event: control` signals phase transitions and
//! stream closure.  The server closes the connection after 60 s to allow
//! clients to reconnect cleanly.

use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use tau_streams::{LiveEvent, Offset, StreamId};
use tokio::time::timeout;

use crate::routes::AppState;
use crate::streams::stream_error_response;

const BATCH_SIZE: usize = 100;
const SSE_TIMEOUT: Duration = Duration::from_secs(60);

/// Serve a stream as SSE, starting catch-up from `offset`.
pub async fn handle_sse(
    stream_id: StreamId,
    offset: Offset,
    state: Arc<AppState>,
) -> Response {
    let ds = match &state.streams {
        Some(ds) => ds.clone(),
        None => {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "streams not configured",
            )
                .into_response();
        }
    };

    // Determine encoding from content type
    let meta = match ds.head(&stream_id) {
        Ok(m) => m,
        Err(e) => return stream_error_response(e),
    };
    let mime = meta.content_type.as_mime().to_owned();
    let use_base64 = !mime.starts_with("text/") && mime != "application/json";

    let event_stream = stream! {
        let mut cursor = offset;

        // -----------------------------------------------------------------------
        // Phase 1: catch-up — drain historical events
        // -----------------------------------------------------------------------
        loop {
            let result = match ds.read(&stream_id, &cursor, BATCH_SIZE) {
                Ok(r) => r,
                Err(e) => {
                    yield Err(format!("read error: {e}"));
                    return;
                }
            };

            for ev in &result.events {
                let data_payload = if use_base64 {
                    base64_encode(&ev.data)
                } else {
                    String::from_utf8_lossy(&ev.data).into_owned()
                };
                yield Ok(Event::default().event("data").data(data_payload));
            }

            if let Some(last) = result.events.last() {
                cursor = last.offset.clone();
            }

            if result.up_to_date {
                let mut ctrl = serde_json::json!({
                    "streamNextOffset": result.next_offset.0,
                    "upToDate": true,
                });
                if result.stream_closed {
                    ctrl["streamClosed"] = serde_json::json!(true);
                } else {
                    let cursor_val = tau_streams::generate_cursor(None);
                    ctrl["streamCursor"] = serde_json::json!(cursor_val);
                }
                yield Ok(Event::default().event("control").data(ctrl.to_string()));
                if result.stream_closed {
                    return;
                }
                break;
            }
        }

        // -----------------------------------------------------------------------
        // Phase 2: live delivery via hub
        // -----------------------------------------------------------------------
        let mut rx = ds.hub().subscribe(&stream_id);

        loop {
            let recv_fut = rx.recv();
            match timeout(SSE_TIMEOUT, recv_fut).await {
                Ok(Ok(LiveEvent::Data { offset, data })) => {
                    let data_payload = if use_base64 {
                        base64_encode(&data)
                    } else {
                        String::from_utf8_lossy(&data).into_owned()
                    };
                    yield Ok(Event::default().event("data").data(data_payload));

                    let cursor_val = tau_streams::generate_cursor(None);
                    let ctrl = serde_json::json!({
                        "streamNextOffset": offset.0,
                        "streamCursor": cursor_val,
                    });
                    yield Ok(Event::default().event("control").data(ctrl.to_string()));
                }
                Ok(Ok(LiveEvent::Closed)) => {
                    let ctrl = serde_json::json!({
                        "streamClosed": true,
                        "upToDate": true,
                    });
                    yield Ok(
                        Event::default()
                            .event("control")
                            .data(ctrl.to_string()),
                    );
                    return;
                }
                Ok(Err(_lagged)) => {
                    // Broadcast lagged: re-read from cursor to recover.
                    // We just end the SSE — the client will reconnect.
                    return;
                }
                Err(_timeout) => {
                    // 60 s timeout: end stream so client reconnects.
                    return;
                }
            }
        }
    };

    let mut response = Sse::new(event_stream)
        .keep_alive(KeepAlive::default())
        .into_response();
    if use_base64 {
        response.headers_mut().insert(
            axum::http::HeaderName::from_static("stream-sse-data-encoding"),
            axum::http::HeaderValue::from_static("base64"),
        );
    }
    response
}

fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    // Simple base64 using the standard alphabet — avoids pulling in a new dep.
    // tau-agent-web already has the `base64` crate via workspace? Not listed.
    // Fall back to a minimal inline encoder.
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = if chunk.len() > 1 { chunk[1] as usize } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as usize } else { 0 };
        let _ = write!(out, "{}", ALPHABET[(b0 >> 2) & 0x3f] as char);
        let _ = write!(out, "{}", ALPHABET[((b0 << 4) | (b1 >> 4)) & 0x3f] as char);
        if chunk.len() > 1 {
            let _ = write!(out, "{}", ALPHABET[((b1 << 2) | (b2 >> 6)) & 0x3f] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            let _ = write!(out, "{}", ALPHABET[b2 & 0x3f] as char);
        } else {
            out.push('=');
        }
    }
    out
}

