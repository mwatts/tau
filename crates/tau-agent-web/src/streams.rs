//! HTTP route handlers for the durable streams protocol.
//!
//! Routes:
//! ```text
//! PUT    /v1/streams/{id}  → create stream
//! POST   /v1/streams/{id}  → append or close (Stream-Closed: true)
//! GET    /v1/streams/{id}  → read / long-poll / SSE
//! HEAD   /v1/streams/{id}  → metadata
//! DELETE /v1/streams/{id}  → delete
//! GET    /v1/streams       → list streams
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::Router;
use tau_streams::{
    AppendRequest, ContentType, CreateOptions, DurableStream, Offset, ProducerEpoch, ProducerId,
    ProducerSeq, SqliteStore, StreamError, StreamId, StreamMeta, StreamSeq, StreamState,
};

use crate::routes::AppState;

// ---------------------------------------------------------------------------
// Router builder
// ---------------------------------------------------------------------------

pub fn stream_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/streams", get(list_streams))
        .route(
            "/v1/streams/{id}",
            put(create_stream)
                .post(append_or_close)
                .get(read_stream)
                .head(head_stream)
                .delete(delete_stream),
        )
        .layer(axum::middleware::from_fn(security_headers_middleware))
}

async fn security_headers_middleware(
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("cross-origin"),
    );
    response
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

pub(crate) fn stream_error_response(err: StreamError) -> Response {
    match err {
        StreamError::NotFound(_) => {
            (StatusCode::NOT_FOUND, err.to_string()).into_response()
        }
        StreamError::AlreadyExists(_) => {
            (StatusCode::CONFLICT, err.to_string()).into_response()
        }
        StreamError::AlreadyClosed(_, ref next_offset) => {
            let mut headers = HeaderMap::new();
            headers.insert("stream-closed", HeaderValue::from_static("true"));
            if let Some(off) = next_offset {
                if let Ok(v) = HeaderValue::from_str(&off.0) {
                    headers.insert("stream-next-offset", v);
                }
            }
            (StatusCode::CONFLICT, headers, err.to_string()).into_response()
        }
        StreamError::Deleted(_) => {
            (StatusCode::GONE, err.to_string()).into_response()
        }
        StreamError::OffsetExpired(_) => {
            (StatusCode::GONE, err.to_string()).into_response()
        }
        StreamError::ProducerFenced { expected, .. } => {
            let mut headers = HeaderMap::new();
            if let Ok(v) = HeaderValue::from_str(&expected.to_string()) {
                headers.insert("producer-epoch", v);
            }
            (StatusCode::FORBIDDEN, headers, err.to_string()).into_response()
        }
        StreamError::SequenceRegression { .. } => {
            (StatusCode::CONFLICT, err.to_string()).into_response()
        }
        StreamError::ProducerSequenceGap { expected, received } => {
            let mut headers = HeaderMap::new();
            if let Ok(v) = HeaderValue::from_str(&expected.to_string()) {
                headers.insert("producer-expected-seq", v);
            }
            if let Ok(v) = HeaderValue::from_str(&received.to_string()) {
                headers.insert("producer-received-seq", v);
            }
            (StatusCode::CONFLICT, headers, err.to_string()).into_response()
        }
        StreamError::InvalidInput(_) => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        }
        StreamError::Storage(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: require streams to be configured
// ---------------------------------------------------------------------------

fn require_streams(state: &AppState) -> Result<Arc<DurableStream<SqliteStore>>, Response> {
    state
        .streams
        .clone()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "streams not configured").into_response())
}

// ---------------------------------------------------------------------------
// Helper: build meta response headers
// ---------------------------------------------------------------------------

fn meta_headers(meta: &StreamMeta) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Ok(v) = HeaderValue::from_str(&meta.id.0) {
        headers.insert("stream-id", v);
    }
    let state_str = match meta.state {
        StreamState::Open => "open",
        StreamState::Closed => "closed",
        StreamState::Deleted => "deleted",
    };
    if let Ok(v) = HeaderValue::from_str(state_str) {
        headers.insert("stream-state", v);
    }
    headers
}

// ---------------------------------------------------------------------------
// PUT /v1/streams/{id}  — create stream
// ---------------------------------------------------------------------------

pub async fn create_stream(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(_query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let ds = match require_streams(&state) {
        Ok(d) => d,
        Err(r) => return r,
    };

    let stream_id = StreamId(id);

    // Parse tags from body JSON or query params.
    let tags: Option<HashMap<String, String>> = if !body.is_empty() {
        match serde_json::from_slice::<HashMap<String, String>>(&body) {
            Ok(m) => Some(m),
            Err(_) => None,
        }
    } else {
        // Tags can also be passed as ?tag=key=value pairs.
        let tag_map: HashMap<String, String> = headers
            .get("stream-tags")
            .and_then(|v| v.to_str().ok())
            .map(|s| {
                s.split(',')
                    .filter_map(|pair| {
                        let mut kv = pair.splitn(2, '=');
                        Some((kv.next()?.trim().to_owned(), kv.next()?.trim().to_owned()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        if tag_map.is_empty() { None } else { Some(tag_map) }
    };

    // Parse Content-Type from request headers; default to octet-stream.
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map_or(ContentType::OctetStream, ContentType::from_mime);

    let ttl = headers
        .get("stream-ttl")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs);

    let expires_at = headers
        .get("stream-expires-at")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok());

    if ttl.is_some() && expires_at.is_some() {
        return (StatusCode::BAD_REQUEST, "cannot set both Stream-TTL and Stream-Expires-At").into_response();
    }

    let closed = headers
        .get("stream-closed")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let opts = CreateOptions { ttl, expires_at, closed };

    let forked_from = headers
        .get("stream-forked-from")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    if let Some(source_path) = forked_from {
        let source_id = StreamId(source_path);
        let fork_offset = headers
            .get("stream-fork-offset")
            .and_then(|v| v.to_str().ok())
            .map(|s| Offset(s.to_owned()))
            .unwrap_or_else(Offset::now);

        // Check if dest stream already exists to determine 200 vs 201.
        let fork_already_exists = ds.head(&stream_id).is_ok();

        return match ds.fork(&source_id, &fork_offset, &stream_id, Some(&content_type), tags, &opts) {
            Ok(meta) => {
                let status = if fork_already_exists { StatusCode::OK } else { StatusCode::CREATED };
                let mut resp_headers = meta_headers(&meta);
                resp_headers.insert(
                    "content-type",
                    HeaderValue::from_str(meta.content_type.as_mime())
                        .unwrap_or(HeaderValue::from_static("application/octet-stream")),
                );
                if !fork_already_exists {
                    resp_headers.insert(
                        "location",
                        HeaderValue::from_str(&format!("/v1/streams/{}", meta.id.0))
                            .unwrap_or(HeaderValue::from_static("/")),
                    );
                }
                if let Some(ref next) = meta.next_offset {
                    if let Ok(v) = HeaderValue::from_str(&next.0) {
                        resp_headers.insert("stream-next-offset", v);
                    }
                }
                if meta.state == StreamState::Closed {
                    resp_headers.insert("stream-closed", HeaderValue::from_static("true"));
                }
                let body = serde_json::json!({
                    "id": meta.id.0,
                    "state": format!("{:?}", meta.state).to_lowercase(),
                    "created_at": meta.created_at,
                });
                (status, resp_headers, body.to_string()).into_response()
            }
            Err(StreamError::Deleted(_)) => {
                (StatusCode::CONFLICT, "source stream is deleted or soft-deleted").into_response()
            }
            Err(e) => stream_error_response(e),
        };
    }

    // Check if stream already exists to determine 200 vs 201.
    let already_exists = ds.head(&stream_id).is_ok();

    match ds.create(&stream_id, &content_type, tags, &opts) {
        Ok(meta) => {
            let status = if already_exists { StatusCode::OK } else { StatusCode::CREATED };
            let mut resp_headers = meta_headers(&meta);
            resp_headers.insert(
                "content-type",
                HeaderValue::from_str(meta.content_type.as_mime())
                    .unwrap_or(HeaderValue::from_static("application/octet-stream")),
            );
            if !already_exists {
                // Location header per spec: point to the newly created stream resource.
                resp_headers.insert(
                    "location",
                    HeaderValue::from_str(&format!("/v1/streams/{}", meta.id.0))
                        .unwrap_or(HeaderValue::from_static("/")),
                );
            }
            // Return the tail offset so clients know where to start reading.
            if let Some(ref next) = meta.next_offset {
                if let Ok(v) = HeaderValue::from_str(&next.0) {
                    resp_headers.insert("stream-next-offset", v);
                }
            }
            if meta.state == StreamState::Closed {
                resp_headers.insert("stream-closed", HeaderValue::from_static("true"));
            }
            let body = serde_json::json!({
                "id": meta.id.0,
                "state": format!("{:?}", meta.state).to_lowercase(),
                "created_at": meta.created_at,
            });
            (status, resp_headers, body.to_string()).into_response()
        }
        Err(e) => stream_error_response(e),
    }
}

// ---------------------------------------------------------------------------
// POST /v1/streams/{id}  — append or close
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Default)]
pub struct AppendQuery {}

pub async fn append_or_close(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(_query): Query<AppendQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let ds = match require_streams(&state) {
        Ok(d) => d,
        Err(r) => return r,
    };

    let stream_id = StreamId(id);

    // Check for close request.
    let is_close = headers
        .get("stream-closed")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if is_close && body.is_empty() {
        // §5.3: close-only — return 204 No Content with Stream-Closed: true
        // and Stream-Next-Offset so clients can resume after the final event.
        return match ds.close(&stream_id) {
            Ok(meta) => {
                let mut resp_headers = HeaderMap::new();
                resp_headers.insert("stream-closed", HeaderValue::from_static("true"));
                if let Some(ref next) = meta.next_offset {
                    if let Ok(v) = HeaderValue::from_str(&next.0) {
                        resp_headers.insert("stream-next-offset", v);
                    }
                }
                (StatusCode::NO_CONTENT, resp_headers).into_response()
            }
            Err(e) => stream_error_response(e),
        };
    }

    // §5.2: Reject empty body unless Stream-Closed: true is present.
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty body requires Stream-Closed: true").into_response();
    }

    // §5.2: Validate Content-Type: if provided, must match stream's type.
    // If absent and body present, require the Content-Type header.
    let req_ct = headers.get("content-type").and_then(|v| v.to_str().ok());
    if let Some(ct_str) = req_ct {
        let req_content_type = tau_streams::ContentType::from_mime(ct_str);
        match ds.head(&stream_id) {
            Ok(meta) => {
                if req_content_type != meta.content_type {
                    return (StatusCode::CONFLICT, "content-type mismatch with stream")
                        .into_response();
                }
            }
            Err(e) => return stream_error_response(e),
        }
    } else {
        return (StatusCode::BAD_REQUEST, "Content-Type header required when body is present")
            .into_response();
    }

    // §5.2.1: Parse producer identity from HTTP headers (not query params).
    let producer_id = headers
        .get("producer-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());
    if producer_id.as_ref().map_or(false, |s| s.is_empty()) {
        return (StatusCode::BAD_REQUEST, "Producer-Id must not be empty").into_response();
    }
    let producer_epoch = headers
        .get("producer-epoch")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    let producer_seq = headers
        .get("producer-seq")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let stream_seq = headers
        .get("stream-seq")
        .and_then(|v| v.to_str().ok())
        .map(|s| StreamSeq(s.to_owned()));

    // All three must be present together or none.
    let has_any = producer_id.is_some() || producer_epoch.is_some() || producer_seq.is_some();
    let has_all = producer_id.is_some() && producer_epoch.is_some() && producer_seq.is_some();
    if has_any && !has_all {
        return (
            StatusCode::BAD_REQUEST,
            "Producer-Id, Producer-Epoch, and Producer-Seq must all be provided together",
        )
            .into_response();
    }

    let has_producer = producer_id.is_some();

    // Build the append request (used for both append-only and append-and-close).
    let req = AppendRequest {
        data: body.to_vec(),
        producer_id: producer_id.map(ProducerId),
        epoch: producer_epoch.map(ProducerEpoch),
        seq: producer_seq.map(ProducerSeq),
        stream_seq,
    };

    // §5.2: Stream-Closed: true with a non-empty body → atomic append-and-close.
    let append_result = if is_close {
        ds.append_and_close(&stream_id, req)
    } else {
        ds.append(&stream_id, req)
    };

    match append_result {
        Ok(result) => {
            if result.deduplicated {
                return StatusCode::NO_CONTENT.into_response();
            }
            let mut resp_headers = HeaderMap::new();
            if let Ok(v) = HeaderValue::from_str(&result.offset.0) {
                resp_headers.insert("stream-offset", v);
            }
            if let Ok(v) = HeaderValue::from_str(&result.next_offset.0) {
                resp_headers.insert("stream-next-offset", v);
            }
            if is_close {
                resp_headers.insert("stream-closed", HeaderValue::from_static("true"));
            }
            // §5.2.1: Echo producer headers on success.
            if has_producer {
                if let Some(epoch) = producer_epoch {
                    if let Ok(v) = HeaderValue::from_str(&epoch.to_string()) {
                        resp_headers.insert("producer-epoch", v);
                    }
                }
                if let Some(seq) = producer_seq {
                    if let Ok(v) = HeaderValue::from_str(&seq.to_string()) {
                        resp_headers.insert("producer-seq", v);
                    }
                }
            }
            // §5.2: with producer headers → 200 OK (idempotent producer, new data)
            //       without producer headers → 204 No Content
            let status = if has_producer {
                StatusCode::OK
            } else {
                StatusCode::NO_CONTENT
            };
            (status, resp_headers).into_response()
        }
        Err(e) => stream_error_response(e),
    }
}

// ---------------------------------------------------------------------------
// GET /v1/streams/{id}  — read / long-poll / SSE
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Default)]
pub struct ReadQuery {
    pub offset: Option<String>,
    pub limit: Option<usize>,
    /// `live=sse` → SSE; `live=long-poll` → long-poll; absent → immediate read
    pub live: Option<String>,
    pub cursor: Option<String>,
}

pub async fn read_stream(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ReadQuery>,
    headers: HeaderMap,
) -> Response {
    let stream_id = StreamId(id);
    let offset = query
        .offset
        .map(Offset)
        .unwrap_or_else(Offset::beginning);
    let limit = query.limit.unwrap_or(100);

    match query.live.as_deref() {
        Some("sse") => {
            crate::streams_sse::handle_sse(stream_id, offset, state).await
        }
        Some("long-poll") => {
            crate::streams_longpoll::handle_long_poll(stream_id, offset, limit, query.cursor, state).await
        }
        _ => {
            // Immediate read.
            let ds = match require_streams(&state) {
                Ok(d) => d,
                Err(r) => return r,
            };

            match ds.read(&stream_id, &offset, limit) {
                Ok(result) => {
                    // Check if this is a JSON-mode stream and return a JSON array.
                    let meta = ds.head(&stream_id).ok();
                    let is_json = meta
                        .as_ref()
                        .map_or(false, |m| m.content_type == ContentType::Json);

                    if is_json {
                        let messages: Vec<serde_json::Value> = result
                            .events
                            .iter()
                            .filter_map(|ev| serde_json::from_slice(&ev.data).ok())
                            .collect();
                        let body =
                            serde_json::to_string(&messages).unwrap_or_else(|_| "[]".to_owned());

                        let mut resp_headers = HeaderMap::new();
                        resp_headers
                            .insert("content-type", HeaderValue::from_static("application/json"));
                        if let Ok(v) = HeaderValue::from_str(&result.next_offset.0) {
                            resp_headers.insert("stream-next-offset", v);
                        }
                        if result.up_to_date {
                            resp_headers.insert(
                                "stream-up-to-date",
                                HeaderValue::from_static("true"),
                            );
                        }
                        if result.stream_closed && result.up_to_date {
                            resp_headers
                                .insert("stream-closed", HeaderValue::from_static("true"));
                        }
                        if result.up_to_date {
                            resp_headers.insert(
                                "cache-control",
                                HeaderValue::from_static("public, max-age=31536000, immutable"),
                            );
                        }

                        let closed_suffix = if result.stream_closed && result.up_to_date { ":c" } else { "" };
                        let etag = format!("\"{}:{}:{}{}\"", stream_id.0, offset.0, result.next_offset.0, closed_suffix);
                        if let Ok(v) = HeaderValue::from_str(&etag) {
                            resp_headers.insert("etag", v);
                        }

                        if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
                            if inm == etag {
                                return StatusCode::NOT_MODIFIED.into_response();
                            }
                        }

                        return (StatusCode::OK, resp_headers, body).into_response();
                    }

                    let mut lines = String::new();
                    for ev in &result.events {
                        let line = serde_json::json!({
                            "offset": ev.offset.0,
                            "data": ev.data,
                        });
                        lines.push_str(&line.to_string());
                        lines.push('\n');
                    }

                    let mut resp_headers = HeaderMap::new();
                    if let Ok(v) = HeaderValue::from_str(&result.next_offset.0) {
                        resp_headers.insert("stream-next-offset", v);
                    }
                    // Presence headers: only insert when true (§5.6)
                    if result.up_to_date {
                        resp_headers.insert("stream-up-to-date", HeaderValue::from_static("true"));
                    }
                    if result.stream_closed && result.up_to_date {
                        resp_headers.insert("stream-closed", HeaderValue::from_static("true"));
                    }
                    resp_headers.insert(
                        "content-type",
                        HeaderValue::from_static("application/x-ndjson"),
                    );

                    if result.up_to_date {
                        resp_headers.insert(
                            "cache-control",
                            HeaderValue::from_static("public, max-age=31536000, immutable"),
                        );
                    }

                    let closed_suffix = if result.stream_closed && result.up_to_date { ":c" } else { "" };
                    let etag = format!("\"{}:{}:{}{}\"", stream_id.0, offset.0, result.next_offset.0, closed_suffix);
                    if let Ok(v) = HeaderValue::from_str(&etag) {
                        resp_headers.insert("etag", v);
                    }

                    if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
                        if inm == etag {
                            return StatusCode::NOT_MODIFIED.into_response();
                        }
                    }

                    (StatusCode::OK, resp_headers, lines).into_response()
                }
                Err(e) => stream_error_response(e),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// HEAD /v1/streams/{id}  — metadata
// ---------------------------------------------------------------------------

pub async fn head_stream(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let ds = match require_streams(&state) {
        Ok(d) => d,
        Err(r) => return r,
    };

    let stream_id = StreamId(id);
    match ds.head(&stream_id) {
        Ok(meta) if meta.state == StreamState::Deleted => {
            (StatusCode::GONE, "stream deleted").into_response()
        }
        Ok(meta) => {
            let mut headers = HeaderMap::new();
            // Content-Type
            headers.insert(
                "content-type",
                HeaderValue::from_str(meta.content_type.as_mime())
                    .unwrap_or(HeaderValue::from_static("application/octet-stream")),
            );
            // Stream-Next-Offset
            if let Some(ref next) = meta.next_offset {
                if let Ok(v) = HeaderValue::from_str(&next.0) {
                    headers.insert("stream-next-offset", v);
                }
            }
            // Stream-Closed (presence header — only when true)
            if meta.state == StreamState::Closed {
                headers.insert("stream-closed", HeaderValue::from_static("true"));
            }
            // Stream-TTL
            if let Some(ttl) = meta.ttl {
                if let Ok(v) = HeaderValue::from_str(&ttl.as_secs().to_string()) {
                    headers.insert("stream-ttl", v);
                }
            }
            // Stream-Expires-At
            if let Some(expires) = meta.expires_at {
                if let Ok(v) = HeaderValue::from_str(&expires.to_string()) {
                    headers.insert("stream-expires-at", v);
                }
            }
            // Cache-Control
            headers.insert("cache-control", HeaderValue::from_static("no-store"));
            (StatusCode::OK, headers).into_response()
        }
        Err(e) => stream_error_response(e),
    }
}

// ---------------------------------------------------------------------------
// DELETE /v1/streams/{id}  — delete
// ---------------------------------------------------------------------------

pub async fn delete_stream(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let ds = match require_streams(&state) {
        Ok(d) => d,
        Err(r) => return r,
    };

    let stream_id = StreamId(id);
    match ds.delete(&stream_id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => stream_error_response(e),
    }
}

// ---------------------------------------------------------------------------
// GET /v1/streams  — list
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Default)]
pub struct ListQuery {
    pub tag_key: Option<String>,
    pub tag_value: Option<String>,
}

pub async fn list_streams(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListQuery>,
) -> Response {
    let ds = match require_streams(&state) {
        Ok(d) => d,
        Err(r) => return r,
    };

    let tag_filter = match (&query.tag_key, &query.tag_value) {
        (Some(k), Some(v)) => Some((k.as_str(), v.as_str())),
        _ => None,
    };

    match ds.list(tag_filter) {
        Ok(streams) => {
            let items: Vec<_> = streams
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "id": m.id.0,
                        "state": format!("{:?}", m.state).to_lowercase(),
                        "created_at": m.created_at,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                [("content-type", "application/json")],
                serde_json::to_string(&items).unwrap_or_default(),
            )
                .into_response()
        }
        Err(e) => stream_error_response(e),
    }
}

