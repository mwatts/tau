# Durable Streams Integration Design

**Date**: 2026-05-06
**Status**: Approved
**Branch**: TBD (new branch off impl-mcp)

## Overview

Integrate the [Durable Streams](https://durablestreams.com/) protocol natively into tau as the foundational data primitive for agent loops. Every prompt, tool call, and generation becomes an event on a persistent, offset-addressable, replayable stream. Multiple agents and humans attach to the same stream with independent cursors over plain HTTP.

This is **not** an integration of Electric-the-product. It is a native Rust implementation of the durable streams protocol concepts: append-only event logs, offset-based resumption, idempotent producers, live delivery (long-poll + SSE), and multiplayer read fan-out.

**References**:
- [Durable Streams Concepts](https://durablestreams.com/concepts)
- [Electric Streams Docs](https://electric.ax/docs/streams/)
- [The Data Primitive for the Agent Loop](https://electric.ax/blog/2026/04/08/data-primitive-agent-loop)

## Approach

**Approach A: New `tau-streams` crate + gradual migration** (selected).

Build a standalone `tau-streams` crate implementing the full durable streams protocol. Migrate tau's session storage to produce/consume stream events incrementally. Existing UDS protocol continues working; HTTP long-poll/SSE added for external consumers.

> **Migration path to Approach B**: The long-term architectural target is Approach B — streams replace the storage layer wholesale. Every piece of mutable state becomes a stream with a materialized projection. See [Approach B Migration Path](#approach-b-migration-path) for details. Do not rush this transition; it follows naturally once Phase A is stable and CQRS patterns are validated.

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Protocol depth | Full durable streams protocol | Native Rust implementation with proper offsets, idempotent producers, live delivery |
| Transport | HTTP (long-poll + SSE) + Unix domain socket | HTTP for external consumers (web UI, agents, tools); UDS for local agent-to-daemon (existing, low overhead) |
| Multiplayer | Phased — read fan-out first, multi-writer second | Observation is the 80% use case; multi-writer needs careful producer coordination |
| Storage | SQLite (evolve existing) | WAL mode for concurrent reads, ~50k writes/sec ceiling is sufficient |
| Session mapping | Clean break, streams-first | Streams are the universal primitive; sessions are one stream type among several |
| HTTP stack | axum 0.8 | Already used by `tau-agent-web`, tokio ecosystem, good SSE support |

## Architecture

### Crate Structure

```
tau-streams (new crate)
  ├── Core types: StreamId, Offset, ProducerId, ProducerEpoch, ProducerSeq
  ├── StreamStore trait + SQLite implementation
  ├── LiveDeliveryHub (broadcast channels for real-time fan-out)
  ├── DurableStream (combines store + hub)
  └── No tau domain knowledge — standalone, reusable

tau-agent-lib
  ├── Depends on tau-streams
  ├── Session storage migrates from messages table → session streams
  ├── SessionEvent, TaskEvent, SystemEvent enums
  ├── session_index / task_index materialized projection tables
  └── Existing UDS protocol gets stream-native variants + backward compat shims

tau-agent-web
  ├── Mounts tau-streams HTTP routes alongside existing WS bridge
  ├── SSE + long-poll handlers
  └── CDN-friendly headers
```

### Data Flow

```
Producer (agent_runner / task plugin / daemon)
    │
    ▼
DurableStream::append()
    ├── StreamStore::append()     → SQLite write (WAL mode)
    └── LiveDeliveryHub::notify() → broadcast to all subscribers
                                        │
                          ┌─────────────┼─────────────┐
                          ▼             ▼             ▼
                     SSE client    Long-poll      UDS subscriber
                     (web UI)      (external)     (local agent)
```

## Core Types & Storage

### Stream Model

```rust
pub struct StreamId(String);
pub struct Offset(String);          // opaque, lexicographically sortable
pub struct ProducerId(String);
pub struct ProducerEpoch(u64);      // incremented on restart, fences zombies
pub struct ProducerSeq(u64);        // monotonic per epoch, enables dedup

pub enum ContentType {
    NdJson,
    Json,
    OctetStream,
    Custom(String),
}

pub enum StreamState { Open, Closed, Deleted }

pub struct StreamMeta {
    pub id: StreamId,
    pub content_type: ContentType,
    pub state: StreamState,
    pub created_at: i64,
    pub closed_at: Option<i64>,
    pub ttl: Option<Duration>,
    pub expires_at: Option<i64>,
    pub tags: HashMap<String, String>,
}
```

### Versioned Event Envelope

All stream content uses this envelope from day one to support schema evolution:

```rust
#[derive(Serialize, Deserialize)]
pub struct StreamEnvelope<T> {
    pub v: u32,
    pub ts: i64,
    pub source: String,
    #[serde(flatten)]
    pub event: T,
}
```

When event shapes change, bump `v` and add a deserialization migration. Old events remain readable forever.

### Offset Generation

Format: `{timestamp_micros}_{sequence}`. Lexicographically sortable, monotonic. Sentinels: `"-1"` (beginning of stream), `"now"` (tail/subscribe to future only). Opaque to consumers — never parse or construct, only use values received from the server.

### SQLite Schema

```sql
CREATE TABLE streams (
    id TEXT PRIMARY KEY,
    content_type TEXT NOT NULL DEFAULT 'application/x-ndjson',
    state TEXT NOT NULL DEFAULT 'open',
    created_at INTEGER NOT NULL,
    closed_at INTEGER,
    ttl_seconds INTEGER,
    expires_at INTEGER,
    tags_json TEXT
);

CREATE TABLE stream_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id TEXT NOT NULL REFERENCES streams(id),
    offset TEXT NOT NULL,
    producer_id TEXT,
    producer_epoch INTEGER,
    producer_seq INTEGER,
    data BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(stream_id, offset)
);

CREATE INDEX idx_stream_events_lookup ON stream_events(stream_id, offset);
CREATE UNIQUE INDEX idx_producer_dedup
    ON stream_events(stream_id, producer_id, producer_epoch, producer_seq);
```

### StreamStore Trait

```rust
pub trait StreamStore: Send + Sync {
    fn create(&self, meta: StreamMeta) -> Result<StreamMeta>;
    fn append(&self, id: &StreamId, event: AppendRequest) -> Result<AppendResult>;
    fn read(&self, id: &StreamId, from: &Offset, limit: usize) -> Result<ReadResult>;
    fn head(&self, id: &StreamId) -> Result<StreamMeta>;
    fn close(&self, id: &StreamId) -> Result<()>;
    fn delete(&self, id: &StreamId) -> Result<()>;
}

pub struct AppendRequest {
    pub data: Vec<u8>,
    pub producer_id: Option<ProducerId>,
    pub producer_epoch: Option<ProducerEpoch>,
    pub producer_seq: Option<ProducerSeq>,
}

pub struct AppendResult {
    pub offset: Offset,
    pub next_offset: Offset,
    pub deduplicated: bool,
}

pub struct ReadResult {
    pub events: Vec<StreamEvent>,
    pub next_offset: Offset,
    pub up_to_date: bool,
    pub stream_closed: bool,
}
```

## Live Delivery Engine

### LiveDeliveryHub

```rust
pub struct LiveDeliveryHub {
    streams: DashMap<StreamId, broadcast::Sender<LiveEvent>>,
}

pub enum LiveEvent {
    Data { offset: Offset, data: Vec<u8> },
    Closed,
}
```

Lazy channel creation — broadcast channel allocated on first subscribe. Capacity 256; slow consumers get `RecvError::Lagged` and fall back to catch-up from storage.

### Consumer Flow

1. **Catch-up**: Read historical events from `StreamStore::read()` until `up_to_date: true`.
2. **Live**: Switch to broadcast channel. New appends arrive via `LiveEvent::Data`.

### DurableStream (combined API)

```rust
impl DurableStream {
    pub fn append(&self, id: &StreamId, req: AppendRequest) -> Result<AppendResult> {
        let result = self.store.append(id, req)?;
        if !result.deduplicated {
            self.hub.notify(id, result.offset.clone(), req.data);
        }
        Ok(result)
    }
}
```

### Long-Poll

Request: `GET /v1/streams/{id}?offset=X&live=long-poll`
- Data available → return immediately with events + `Stream-Next-Offset`
- Up-to-date → hold open, wait on broadcast (30s timeout)
- Timeout → `204 No Content`

### SSE

Request: `GET /v1/streams/{id}?offset=X&live=sse` with `Accept: text/event-stream`
- Catch-up: emit historical events as `data:` frames
- Emit `event: control` with `{"streamUpToDate": true}` after catch-up
- Live: emit new events as `data:` frames
- Server closes after ~60s; client reconnects with last offset
- On stream close: emit `event: control` with `{"streamClosed": true}`

### Runtime Bridge

Tau daemon runs `smol`; axum runs `tokio`. `LiveDeliveryHub` uses `tokio::sync::broadcast`. UDS subscribers bridge via `smol::unblock` adapter wrapping `blocking_recv()`.

## HTTP Protocol

### Routes

```
PUT    /v1/streams/{id}     → create stream (idempotent)
POST   /v1/streams/{id}     → append or close
GET    /v1/streams/{id}     → read / long-poll / SSE
HEAD   /v1/streams/{id}     → metadata
DELETE /v1/streams/{id}     → delete
GET    /v1/streams          → list streams (tau extension)
```

### Headers

**Producer headers** (on POST append):
- `Producer-Id`, `Producer-Epoch`, `Producer-Seq` — enable idempotent writes and zombie fencing

**Response headers**:
- `Stream-Offset` — offset of appended event
- `Stream-Next-Offset` — use for next read
- `Stream-Up-To-Date: true` — caught up to tail
- `Stream-Closed: true` — stream permanently finished
- `Stream-Cursor` — CDN edge collapsing (future)
- `Cache-Control` — `public, immutable` for historical reads; `no-cache` for live
- `ETag` — offset-range based, enables conditional requests

### CDN Compatibility

Historical reads are cacheable — data at a given offset never changes. Live reads use `no-cache`. Conditional requests via `If-None-Match` + `ETag` return `304 Not Modified`.

## Session-to-Stream Mapping

### Stream Types

```rust
pub enum StreamType { Session, Task, System, Coordination }
```

Tag stored in `streams.tags_json`. The stream primitive is type-agnostic; tau-agent-lib applies semantics.

### Session Events

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    UserMessage { content: String, attachments: Vec<Attachment> },
    AssistantMessage { content: String, model: String },
    ToolCall { id: String, name: String, input: serde_json::Value },
    ToolResult { call_id: String, output: serde_json::Value, is_error: bool },
    SystemMessage { content: String },
    SessionMeta { key: String, value: serde_json::Value },
}
```

Granular — a single turn with two tool calls becomes 5 events, not one blob.

### Session Index

Mutable metadata stays in a lightweight table:

```sql
CREATE TABLE session_index (
    session_id TEXT PRIMARY KEY,
    stream_id TEXT NOT NULL REFERENCES streams(id),
    parent_id TEXT,
    successor_id TEXT,
    agent_id TEXT,
    is_agent INTEGER NOT NULL DEFAULT 0,
    model TEXT,
    system_prompt TEXT,
    cwd TEXT,
    project_name TEXT,
    tagline TEXT,
    archived INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);
```

### Lifecycle Mapping

| Current tau | Durable streams |
|-------------|----------------|
| `CreateSession` | `PUT /v1/streams/{id}` + INSERT `session_index` |
| `append_message()` | `POST /v1/streams/{id}` (SessionEvent as NDJSON) |
| `get_messages()` | `GET /v1/streams/{id}?offset=-1` |
| `Subscribe { session_id }` | `GET /v1/streams/{id}?offset=now&live=sse` (or internal hub subscribe for UDS) |
| Session succeeds/archives | `POST` with `Stream-Closed: true` + UPDATE `session_index` |

### Branching (Phase 2)

Create new stream, copy events from source up to a given offset, then diverge. Parent-child session relationships become source-branch stream relationships.

### Migration Tool

1. For each `sessions` row → create stream + insert `session_index`
2. For each `messages` row ordered by `id` → decompose `message_json` into granular `SessionEvent`s → append to stream
3. Old tables remain as backup. Drop after confidence period.

## Task & System Streams

### Task Events

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskEvent {
    Created { title: String, project: Option<String>, priority: i64, tags: Vec<String> },
    Updated { fields: serde_json::Value },
    Assigned { session_id: String, agent_name: Option<String> },
    Dispatched { session_id: String },
    ProgressNote { note: String },
    Completed { result: Option<String> },
    Failed { error: String },
    Blocked { reason: String },
    Unblocked,
    Cancelled { reason: Option<String> },
}
```

Current state via event sourcing: fold events to materialize `Task`. `task_index` table is a read optimization rebuilt from replay if corrupted.

```sql
CREATE TABLE task_index (
    task_id INTEGER PRIMARY KEY,
    stream_id TEXT NOT NULL REFERENCES streams(id),
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'todo',
    priority INTEGER NOT NULL DEFAULT 0,
    project TEXT,
    session_id TEXT,
    tags_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
```

### System Events

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SystemEvent {
    DaemonStarted { version: String, pid: u32 },
    DaemonShutdown { reason: String },
    AgentStarted { agent_name: String, session_id: String },
    AgentStopped { agent_name: String, reason: String },
    ScheduleFired { schedule_id: String, session_id: String },
    SessionCreated { session_id: String, stream_id: String },
    SessionClosed { session_id: String, reason: String },
    Error { context: String, message: String },
}
```

One singleton stream per daemon: `system-{daemon-id}`.

### Stream Naming Convention

```
session-{uuid}       → session stream
task-{id}            → task stream
system-{daemon-id}   → system stream
coord-{group-id}     → coordination stream (Phase 2)
```

## UDS Bridge

### New Protocol Variants

```rust
// Requests
StreamCreate { id, content_type, tags },
StreamAppend { id, data, producer_id, producer_epoch, producer_seq },
StreamRead { id, offset, limit },
StreamSubscribe { id, offset },
StreamClose { id },
StreamList { type_filter },

// Responses
StreamCreated { id },
StreamAppended { offset, next_offset },
StreamEvents { events, next_offset, up_to_date, closed },
StreamEvent { offset, data },  // live push
StreamClosed { id },
StreamListing { streams },
```

### Backward Compatibility

Existing `Subscribe { session_id }` translates internally to `hub.subscribe(StreamId("session-{session_id}"), Offset::now())` and pushes events wrapped as `Response::Stream { event }`. Old clients unchanged.

### Operations That Stay UDS-Only

- `Chat` — starts agent loop (the loop *produces* stream events)
- `CancelChat` — imperative command
- `ListSessions` / `GetSessionInfo` — reads from `session_index`
- Agent/schedule CRUD — config operations

## Implementation Phases

### Phase 1: Stream Primitive
New `tau-streams` crate. Core types, `StreamStore` trait, SQLite implementation, offset generation, idempotent append with producer dedup, lifecycle management. Standalone, no tau integration.

### Phase 2: Live Delivery
`LiveDeliveryHub`, `DurableStream` combined API, catch-up → live transition, backpressure handling. Still within `tau-streams`.

### Phase 3: HTTP Protocol
axum routes in `tau-agent-web`. PUT/POST/GET/HEAD/DELETE + SSE + long-poll. CDN headers. Durable streams spec headers.

### Phase 4: Session Migration
`SessionEvent` enum, `session_index` table. `agent_runner` produces stream events. Existing `Subscribe` → hub adapter. `notifications.rs` converges into hub. Migration tool for existing data.

### Phase 5: Task & System Streams
`TaskEvent`, `task_index`. `SystemEvent`, singleton system stream. Task plugin appends to task streams.

### Phase 6: Multiplayer
Multi-writer with epoch fencing. Coordination streams. Branch operation (fork at offset).

### Phase 7: Stream-native UDS
New `StreamCreate/Append/Read/Subscribe` protocol variants. Old variants remain as shims.

## Approach B Migration Path

> **This section documents the long-term target.** Approach A is the current implementation path. Approach B follows when Phase A is stable and CQRS patterns are validated.

### What Approach B Looks Like

Every piece of mutable state becomes a stream with a materialized projection:

| Current table | Becomes | Projection table |
|--------------|---------|-----------------|
| `messages` | Session streams | *(eliminated)* |
| `sessions` | Session streams + `session_index` | Done in Phase A |
| `tasks` | Task streams + `task_index` | Done in Phase A |
| `agents` | Agent config stream | `agent_index` |
| `schedules` | Schedule config stream | `schedule_index` |
| `projects` | Project stream | `project_index` |
| `queued_messages` | Per-session inbox stream | Consumer ack model |

### When to Transition

1. Phase A stable in production
2. CQRS projection pattern validated (task streams → task_index works reliably)
3. A concrete pain point emerges that A handles awkwardly but B handles naturally
4. Not before Phase 6 — multiplayer and branching benefit most

### Transition Mechanics

- `db.rs` shrinks to thin wrapper around stream operations + index management
- `Db` struct holds `DurableStream` instead of (or alongside) raw `rusqlite::Connection`
- Index tables become derivable: `cargo run --bin rebuild-indexes` replays all streams
- Periodic snapshots prevent replay overhead for long-lived streams

### Risks

- **Event sourcing overhead**: mitigate with periodic snapshot events in streams
- **Schema evolution**: handled by versioned `StreamEnvelope` (added in Phase 1)
- **Mutable state resistance**: agent config, project paths are naturally CRUD. Some tables may stay as plain SQLite even in "full B" — pragmatism over purity.

## Runtime Bridge Detail

The `smol` ↔ `tokio` bridge deserves specifics. Three options, in preference order:

1. **Shared tokio runtime**: `tau-agent-web` already runs tokio. Expose a `tokio::runtime::Handle` that `tau-agent-lib` can use to spawn stream subscription tasks. The hub's broadcast channels live on this handle. UDS dispatch spawns a tokio task for `StreamSubscribe` that writes back to the smol socket via a `futures::channel::mpsc`.

2. **Dedicated stream thread**: A single background thread runs a tokio runtime owning the `LiveDeliveryHub`. Smol tasks communicate via `flume` (sync/async compatible) channels.

3. **Replace smol with tokio**: Long-term simplification. Out of scope for this design but noted as a possibility during Approach B.

Option 1 is recommended — `tau-agent-web` already provides the tokio handle.

## Retention & TTL Cleanup

A background task (registered in `bg_jobs`) runs periodically (default: every 5 minutes):

1. Query `streams WHERE expires_at < now() OR (ttl_seconds IS NOT NULL AND created_at + ttl_seconds < now())`
2. Delete matching `stream_events` rows
3. Set `streams.state = 'deleted'`
4. If a consumer requests a discarded offset: return `410 Gone`

Closed streams without TTL/expiry persist indefinitely (archival). Active streams are never cleaned.

## Error Handling

`tau-streams` is a library crate. Per Rust guidelines, canonical error structs:

```rust
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("stream not found: {0}")]
    NotFound(StreamId),
    #[error("stream already closed: {0}")]
    AlreadyClosed(StreamId),
    #[error("offset expired (410 Gone): {0}")]
    OffsetExpired(Offset),
    #[error("producer fenced: epoch {actual} < {expected}")]
    ProducerFenced { actual: u64, expected: u64 },
    #[error("storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}
```

HTTP layer maps these to status codes: `NotFound` → 404, `AlreadyClosed` → 409, `OffsetExpired` → 410, `ProducerFenced` → 403, `Storage` → 500.

## What's NOT a Stream (Phase A)

These stay as regular SQLite tables:

| Table | Reason |
|-------|--------|
| `session_index` | Mutable metadata (archived, successor updates) |
| `task_index` | Materialized projection, updated in place |
| `agents` | Config CRUD |
| `projects` | Lookup table |
| `schedules` | Config CRUD |
