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

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::Router;
use tau_streams::{
    AppendRequest, DurableStream, Offset, ProducerEpoch, ProducerId, ProducerSeq, SqliteStore,
    StreamError, StreamId, StreamMeta, StreamState,
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
        .route("/v1/streams/{id}/fork", axum::routing::post(fork_stream))
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
        StreamError::AlreadyClosed(_) => {
            (StatusCode::CONFLICT, err.to_string()).into_response()
        }
        StreamError::Deleted(_) => {
            (StatusCode::GONE, err.to_string()).into_response()
        }
        StreamError::OffsetExpired(_) => {
            (StatusCode::GONE, err.to_string()).into_response()
        }
        StreamError::ProducerFenced { .. } => {
            (StatusCode::FORBIDDEN, err.to_string()).into_response()
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

    match ds.create(&stream_id, tags) {
        Ok(meta) => {
            let mut resp_headers = meta_headers(&meta);
            resp_headers.insert(
                "content-type",
                HeaderValue::from_static("application/json"),
            );
            let body = serde_json::json!({
                "id": meta.id.0,
                "state": format!("{:?}", meta.state).to_lowercase(),
                "created_at": meta.created_at,
            });
            (StatusCode::CREATED, resp_headers, body.to_string()).into_response()
        }
        Err(e) => stream_error_response(e),
    }
}

// ---------------------------------------------------------------------------
// POST /v1/streams/{id}  — append or close
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Default)]
pub struct AppendQuery {
    pub producer_id: Option<String>,
    pub epoch: Option<u64>,
    pub seq: Option<u64>,
}

pub async fn append_or_close(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<AppendQuery>,
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

    if is_close {
        return match ds.close(&stream_id) {
            Ok(meta) => {
                let resp_headers = meta_headers(&meta);
                (StatusCode::OK, resp_headers).into_response()
            }
            Err(e) => stream_error_response(e),
        };
    }

    // Append request.
    let req = AppendRequest {
        data: body.to_vec(),
        producer_id: query.producer_id.map(|s| ProducerId(s)),
        epoch: query.epoch.map(ProducerEpoch),
        seq: query.seq.map(ProducerSeq),
    };

    match ds.append(&stream_id, req) {
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
            (StatusCode::OK, resp_headers).into_response()
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
    /// `live=sse` → SSE; `live=poll` → long-poll; absent → immediate read
    pub live: Option<String>,
}

pub async fn read_stream(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ReadQuery>,
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
        Some("poll") => {
            crate::streams_longpoll::handle_long_poll(stream_id, offset, limit, state).await
        }
        _ => {
            // Immediate read.
            let ds = match require_streams(&state) {
                Ok(d) => d,
                Err(r) => return r,
            };

            match ds.read(&stream_id, &offset, limit) {
                Ok(result) => {
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
                    resp_headers.insert(
                        "stream-up-to-date",
                        HeaderValue::from_static(if result.up_to_date { "true" } else { "false" }),
                    );
                    resp_headers.insert(
                        "stream-closed",
                        HeaderValue::from_static(
                            if result.stream_closed { "true" } else { "false" },
                        ),
                    );
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
            let resp_headers = meta_headers(&meta);
            (StatusCode::OK, resp_headers).into_response()
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

// ---------------------------------------------------------------------------
// POST /v1/streams/{id}/fork  — fork stream
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct ForkRequest {
    dest_id: String,
    up_to_offset: String,
    #[serde(default)]
    tags: Option<HashMap<String, String>>,
}

async fn fork_stream(
    Path(source_id): Path<String>,
    State(state): State<Arc<AppState>>,
    axum::Json(body): axum::Json<ForkRequest>,
) -> Response {
    let ds = match require_streams(&state) {
        Ok(d) => d,
        Err(r) => return r,
    };

    let source = StreamId(source_id);
    let up_to = Offset(body.up_to_offset);
    let dest = StreamId(body.dest_id.clone());

    match ds.fork(&source, &up_to, &dest, body.tags) {
        Ok(meta) => {
            let mut resp_headers = meta_headers(&meta);
            resp_headers.insert(
                "content-type",
                HeaderValue::from_static("application/json"),
            );
            let body_json = serde_json::json!({
                "id": meta.id.0,
                "state": format!("{:?}", meta.state).to_lowercase(),
                "created_at": meta.created_at,
            });
            (StatusCode::CREATED, resp_headers, body_json.to_string()).into_response()
        }
        Err(e) => stream_error_response(e),
    }
}
