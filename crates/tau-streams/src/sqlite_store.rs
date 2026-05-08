//! `SQLite`-backed implementation of [`StreamStore`].
//!
//! All reads and writes go through a single `rusqlite::Connection` wrapped in
//! a `Mutex`, giving serialised access suitable for the shared-library use
//! case.  For higher-throughput needs the connection can be replaced with a
//! connection pool in a future iteration.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, params};

use crate::{
    error::{Result, StreamError},
    offset::OffsetGenerator,
    store::StreamStore,
    types::{
        AppendRequest, AppendResult, ContentType, CreateOptions, Offset, ProducerEpoch, ProducerId,
        ProducerInfo, ReadResult, StreamEvent, StreamId, StreamMeta, StreamState,
    },
};

// ---------------------------------------------------------------------------
// SqliteStore
// ---------------------------------------------------------------------------

/// `SQLite`-backed stream store.
///
/// Wrap in [`std::sync::Arc`] for shared ownership across threads.
#[derive(Debug)]
pub struct SqliteStore {
    conn: Mutex<Connection>,
    offset_gen: OffsetGenerator,
}

impl SqliteStore {
    /// Opens a store backed by the given database file path.
    ///
    /// # Errors
    ///
    /// Returns a storage error if the connection or schema migration fails.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Mutex::new(conn),
            offset_gen: OffsetGenerator::new(),
        };
        store.init()?;
        Ok(store)
    }

    /// Opens an in-memory store — useful for tests.
    ///
    /// # Errors
    ///
    /// Returns a storage error if the connection or schema migration fails.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Mutex::new(conn),
            offset_gen: OffsetGenerator::new(),
        };
        store.init()?;
        Ok(store)
    }

    /// Applies pragmas and runs the DDL migration.
    fn init(&self) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let result = conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS streams (
                id           TEXT NOT NULL PRIMARY KEY,
                content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                state        TEXT NOT NULL DEFAULT 'open',
                created_at   INTEGER NOT NULL,
                closed_at    INTEGER,
                ttl_seconds  INTEGER,
                expires_at   INTEGER,
                tags_json    TEXT NOT NULL DEFAULT '{}',
                last_seq     TEXT
            );

            CREATE TABLE IF NOT EXISTS stream_events (
                offset      TEXT NOT NULL,
                stream_id   TEXT NOT NULL REFERENCES streams(id),
                data        BLOB NOT NULL,
                created_at  INTEGER NOT NULL,
                producer_id TEXT,
                epoch       INTEGER,
                seq         INTEGER,
                PRIMARY KEY (stream_id, offset)
            );

            CREATE INDEX IF NOT EXISTS idx_stream_events_stream_id
                ON stream_events (stream_id, offset);

            -- Partial unique index for producer dedup (only when producer_id IS NOT NULL).
            CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_events_producer_dedup
                ON stream_events (stream_id, producer_id, epoch, seq)
                WHERE producer_id IS NOT NULL;

            CREATE TABLE IF NOT EXISTS stream_producers (
                stream_id      TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
                producer_id    TEXT NOT NULL,
                epoch          INTEGER NOT NULL DEFAULT 1,
                registered_at  INTEGER NOT NULL,
                last_append_at INTEGER,
                PRIMARY KEY (stream_id, producer_id)
            );
            ",
        );
        drop(conn);
        Ok(result?)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .try_into()
        .unwrap_or(i64::MAX)
}

/// Deserialise a `StreamMeta` from a row of the `streams` table.
fn row_to_meta(
    id: String,
    content_type_str: &str,
    state_str: &str,
    created_at: i64,
    closed_at: Option<i64>,
    ttl_seconds: Option<i64>,
    expires_at: Option<i64>,
    tags_json: &str,
) -> rusqlite::Result<StreamMeta> {
    let content_type = ContentType::from_mime(content_type_str);
    let state = match state_str {
        "open" => StreamState::Open,
        "closed" => StreamState::Closed,
        "deleted" => StreamState::Deleted,
        other => {
            return Err(rusqlite::Error::InvalidParameterName(format!(
                "unknown stream state: {other}"
            )));
        }
    };
    let tags: HashMap<String, String> = serde_json::from_str(tags_json).unwrap_or_default();

    Ok(StreamMeta {
        id: StreamId(id),
        content_type,
        state,
        created_at,
        closed_at,
        ttl: ttl_seconds.map(|s| {
            #[expect(clippy::cast_sign_loss, reason = "TTL is always positive")]
            std::time::Duration::from_secs(s as u64)
        }),
        expires_at,
        tags,
        next_offset: None,
    })
}

// ---------------------------------------------------------------------------
// StreamStore impl
// ---------------------------------------------------------------------------

impl StreamStore for SqliteStore {
    fn create(
        &self,
        id: &StreamId,
        content_type: &ContentType,
        tags: Option<HashMap<String, String>>,
        opts: &CreateOptions,
    ) -> Result<StreamMeta> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let now = now_micros();
        let tags_json = serde_json::to_string(&tags.unwrap_or_default())
            .unwrap_or_else(|_| "{}".to_owned());
        let mime = content_type.as_mime();
        let state_str = if opts.closed { "closed" } else { "open" };
        let closed_at: Option<i64> = if opts.closed { Some(now) } else { None };
        #[expect(clippy::cast_possible_wrap, reason = "TTL seconds fit in i64 in practice")]
        let ttl_secs: Option<i64> = opts.ttl.map(|d| d.as_secs() as i64);

        // INSERT OR IGNORE for idempotent create.
        let rows_changed = conn.execute(
            "INSERT OR IGNORE INTO streams (id, content_type, state, created_at, closed_at, ttl_seconds, expires_at, tags_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![id.0, mime, state_str, now, closed_at, ttl_secs, opts.expires_at, tags_json],
        )?;
        drop(conn);

        if rows_changed == 0 {
            // Idempotent: return existing metadata.
            return self.head(id);
        }

        let state = if opts.closed { StreamState::Closed } else { StreamState::Open };
        Ok(StreamMeta {
            id: id.clone(),
            content_type: content_type.clone(),
            state,
            created_at: now,
            closed_at,
            ttl: opts.ttl,
            expires_at: opts.expires_at,
            tags: serde_json::from_str(&tags_json).unwrap_or_default(),
            next_offset: Some(Offset::now()),
        })
    }

    fn append(&self, id: &StreamId, req: AppendRequest) -> Result<AppendResult> {
        let conn = self.conn.lock().expect("mutex poisoned");

        // Load stream state.
        let state_row: Option<String> = conn
            .query_row(
                "SELECT state FROM streams WHERE id = ?1",
                params![id.0],
                |r| r.get(0),
            )
            .optional()?;

        let state_str = state_row.ok_or_else(|| StreamError::NotFound(id.clone()))?;

        match state_str.as_str() {
            "closed" => {
                let tail: Option<String> = conn
                    .query_row(
                        "SELECT MAX(offset) FROM stream_events WHERE stream_id = ?1",
                        params![id.0],
                        |r| r.get(0),
                    )
                    .optional()?
                    .flatten();
                return Err(StreamError::AlreadyClosed(id.clone(), tail.map(Offset)));
            }
            "deleted" => return Err(StreamError::Deleted(id.clone())),
            _ => {}
        }

        // Producer fencing & dedup.
        //
        // SQLite stores integers as signed i64; epochs and sequence numbers
        // are expected to be far below i64::MAX in practice, so we cast.
        if let Some(ref pid) = req.producer_id {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "epoch/seq are far below i64::MAX in practice"
            )]
            let epoch_val: i64 = req.epoch.map_or(0, |e| e.0 as i64);
            #[expect(
                clippy::cast_possible_wrap,
                reason = "epoch/seq are far below i64::MAX in practice"
            )]
            let seq_val: i64 = req.seq.map_or(0, |s| s.0 as i64);

            // Check registered epoch in stream_producers (authoritative fence).
            let registered_epoch: Option<i64> = conn
                .query_row(
                    "SELECT epoch FROM stream_producers
                     WHERE stream_id = ?1 AND producer_id = ?2",
                    params![id.0, pid.0],
                    |r| r.get(0),
                )
                .optional()?;

            // Also check the max epoch observed in stream_events (legacy/unregistered).
            let events_epoch: Option<i64> = conn
                .query_row(
                    "SELECT MAX(epoch) FROM stream_events
                     WHERE stream_id = ?1 AND producer_id = ?2",
                    params![id.0, pid.0],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();

            // Fencing is enforced against the highest known epoch from either source.
            let current_epoch = match (registered_epoch, events_epoch) {
                (Some(r), Some(e)) => Some(r.max(e)),
                (Some(r), None) => Some(r),
                (None, Some(e)) => Some(e),
                (None, None) => None,
            };

            if let Some(current) = current_epoch {
                if epoch_val < current {
                    #[expect(
                        clippy::cast_sign_loss,
                        reason = "values came from u64; sign loss is not possible"
                    )]
                    return Err(StreamError::ProducerFenced {
                        actual: epoch_val as u64,
                        expected: current as u64,
                    });
                }
            }

            // Check for existing duplicate (same producer_id + epoch + seq).
            let existing: Option<String> = conn
                .query_row(
                    "SELECT offset FROM stream_events
                     WHERE stream_id = ?1 AND producer_id = ?2 AND epoch = ?3 AND seq = ?4",
                    params![id.0, pid.0, epoch_val, seq_val],
                    |r| r.get(0),
                )
                .optional()?;

            if let Some(existing_offset) = existing {
                let offset = Offset(existing_offset);
                return Ok(AppendResult {
                    next_offset: offset.clone(),
                    offset,
                    deduplicated: true,
                });
            }

            // Check for sequence gap (seq must be exactly lastSeq + 1 for same epoch)
            let last_producer_seq: Option<i64> = conn
                .query_row(
                    "SELECT MAX(seq) FROM stream_events
                     WHERE stream_id = ?1 AND producer_id = ?2 AND epoch = ?3",
                    params![id.0, pid.0, epoch_val],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();

            if let Some(last) = last_producer_seq {
                let expected = last + 1;
                if seq_val != expected {
                    if seq_val > expected {
                        #[expect(
                            clippy::cast_sign_loss,
                            reason = "values came from u64; sign loss is not possible"
                        )]
                        return Err(StreamError::ProducerSequenceGap {
                            expected: expected as u64,
                            received: seq_val as u64,
                        });
                    }
                }
            }
        }

        // Stream-Seq validation (§5.2): reject if received <= last accepted.
        if let Some(ref seq) = req.stream_seq {
            let last: Option<String> = conn
                .query_row(
                    "SELECT last_seq FROM streams WHERE id = ?1",
                    params![id.0],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();

            if let Some(ref last_val) = last {
                if seq.0 <= *last_val {
                    return Err(StreamError::SequenceRegression {
                        received: seq.0.clone(),
                        last: last_val.clone(),
                    });
                }
            }
        }

        let offset = self.offset_gen.next();
        let now = now_micros();

        let producer_id = req.producer_id.as_ref().map(|p| p.0.as_str());
        // SQLite only supports i64; cast u64 epoch/seq (they fit in practice).
        #[expect(
            clippy::cast_possible_wrap,
            reason = "epoch/seq are far below i64::MAX in practice"
        )]
        let epoch: Option<i64> = req.epoch.map(|e| e.0 as i64);
        #[expect(
            clippy::cast_possible_wrap,
            reason = "epoch/seq are far below i64::MAX in practice"
        )]
        let seq: Option<i64> = req.seq.map(|s| s.0 as i64);

        conn.execute(
            "INSERT INTO stream_events (offset, stream_id, data, created_at, producer_id, epoch, seq)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![offset.0, id.0, req.data, now, producer_id, epoch, seq],
        )?;

        if let Some(ref seq) = req.stream_seq {
            conn.execute(
                "UPDATE streams SET last_seq = ?1 WHERE id = ?2",
                params![seq.0, id.0],
            )?;
        }
        drop(conn);

        let next = self.offset_gen.next();
        Ok(AppendResult {
            next_offset: next,
            offset,
            deduplicated: false,
        })
    }

    fn read(&self, id: &StreamId, offset: &Offset, limit: usize) -> Result<ReadResult> {
        let conn = self.conn.lock().expect("mutex poisoned");

        let state_row: Option<String> = conn
            .query_row(
                "SELECT state FROM streams WHERE id = ?1",
                params![id.0],
                |r| r.get(0),
            )
            .optional()?;

        let state_str = state_row.ok_or_else(|| StreamError::NotFound(id.clone()))?;

        if state_str == "deleted" {
            return Err(StreamError::Deleted(id.clone()));
        }

        let stream_closed = state_str == "closed";

        // "now" sentinel — return empty result immediately.
        if offset.is_now() {
            // Return the latest stored offset so the caller can resume from here.
            let latest: Option<String> = conn
                .query_row(
                    "SELECT MAX(offset) FROM stream_events WHERE stream_id = ?1",
                    params![id.0],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();

            let next_offset = latest.map_or_else(Offset::now, Offset);

            return Ok(ReadResult {
                events: vec![],
                next_offset,
                up_to_date: true,
                stream_closed,
            });
        }

        // "beginning" sentinel reads from offset "" (everything), otherwise
        // we read strictly after the provided offset string.
        let after_offset = if offset.is_beginning() {
            String::new() // Empty string is lexicographically less than any real offset.
        } else {
            offset.0.clone()
        };

        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);

        let mut stmt = conn.prepare(
            "SELECT offset, data, created_at
             FROM stream_events
             WHERE stream_id = ?1 AND offset > ?2
             ORDER BY offset ASC
             LIMIT ?3",
        )?;

        let events: Vec<StreamEvent> = stmt
            .query_map(params![id.0, after_offset, limit_i64], |r| {
                let offset_str: String = r.get(0)?;
                let data: Vec<u8> = r.get(1)?;
                let created_at: i64 = r.get(2)?;
                Ok(StreamEvent {
                    offset: Offset(offset_str),
                    data,
                    created_at,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        drop(stmt);

        let next_offset = events
            .last()
            .map_or_else(|| offset.clone(), |e| e.offset.clone());

        // Are we at the tail?
        let has_more: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM stream_events
                 WHERE stream_id = ?1 AND offset > ?2
                 LIMIT 1",
                params![id.0, next_offset.0],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;
        drop(conn);

        Ok(ReadResult {
            events,
            next_offset,
            up_to_date: !has_more,
            stream_closed,
        })
    }

    fn head(&self, id: &StreamId) -> Result<StreamMeta> {
        let conn = self.conn.lock().expect("mutex poisoned");

        let row: Option<(String, String, String, i64, Option<i64>, Option<i64>, Option<i64>, String)> = conn
            .query_row(
                "SELECT id, content_type, state, created_at, closed_at, ttl_seconds, expires_at, tags_json
                 FROM streams WHERE id = ?1",
                params![id.0],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?)),
            )
            .optional()?;

        let (sid, ct, state, created, closed, ttl_secs, expires, tags_json) =
            row.ok_or_else(|| StreamError::NotFound(id.clone()))?;

        // Query the tail offset for the stream.
        let tail: Option<String> = conn
            .query_row(
                "SELECT MAX(offset) FROM stream_events WHERE stream_id = ?1",
                params![id.0],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        drop(conn);

        let mut meta = row_to_meta(sid, &ct, &state, created, closed, ttl_secs, expires, &tags_json)
            .map_err(StreamError::Storage)?;
        meta.next_offset = Some(tail.map_or_else(Offset::now, Offset));
        Ok(meta)
    }

    fn close(&self, id: &StreamId) -> Result<StreamMeta> {
        let conn = self.conn.lock().expect("mutex poisoned");

        let state_row: Option<String> = conn
            .query_row(
                "SELECT state FROM streams WHERE id = ?1",
                params![id.0],
                |r| r.get(0),
            )
            .optional()?;

        let state_str = state_row.ok_or_else(|| StreamError::NotFound(id.clone()))?;

        match state_str.as_str() {
            "closed" => {
                // Idempotent: return current metadata without error.
                drop(conn);
                return self.head(id);
            }
            "deleted" => return Err(StreamError::Deleted(id.clone())),
            _ => {}
        }

        let now = now_micros();
        conn.execute(
            "UPDATE streams SET state = 'closed', closed_at = ?1 WHERE id = ?2",
            params![now, id.0],
        )?;

        let row: (String, String, String, i64, Option<i64>, Option<i64>, Option<i64>, String) = conn.query_row(
            "SELECT id, content_type, state, created_at, closed_at, ttl_seconds, expires_at, tags_json
             FROM streams WHERE id = ?1",
            params![id.0],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?)),
        )?;
        drop(conn);

        row_to_meta(row.0, &row.1, &row.2, row.3, row.4, row.5, row.6, &row.7).map_err(StreamError::Storage)
    }

    fn append_and_close(&self, id: &StreamId, req: AppendRequest) -> Result<AppendResult> {
        // Run the append first (which validates state, fencing, and dedup).
        // If the stream is already closed we may still return a dedup result.
        let state_row: Option<String> = {
            let conn = self.conn.lock().expect("mutex poisoned");
            conn.query_row(
                "SELECT state FROM streams WHERE id = ?1",
                params![id.0],
                |r| r.get(0),
            )
            .optional()?
        };

        let state_str = state_row.ok_or_else(|| StreamError::NotFound(id.clone()))?;

        match state_str.as_str() {
            "deleted" => return Err(StreamError::Deleted(id.clone())),
            "closed" => {
                // Only allow dedup match on an already-closed stream.
                if let Some(ref pid) = req.producer_id {
                    #[expect(
                        clippy::cast_possible_wrap,
                        reason = "epoch/seq are far below i64::MAX in practice"
                    )]
                    let epoch_val: i64 = req.epoch.map_or(0, |e| e.0 as i64);
                    #[expect(
                        clippy::cast_possible_wrap,
                        reason = "epoch/seq are far below i64::MAX in practice"
                    )]
                    let seq_val: i64 = req.seq.map_or(0, |s| s.0 as i64);

                    let conn = self.conn.lock().expect("mutex poisoned");
                    let existing: Option<String> = conn
                        .query_row(
                            "SELECT offset FROM stream_events
                             WHERE stream_id = ?1 AND producer_id = ?2 AND epoch = ?3 AND seq = ?4",
                            params![id.0, pid.0, epoch_val, seq_val],
                            |r| r.get(0),
                        )
                        .optional()?;

                    if let Some(existing_offset) = existing {
                        let offset = Offset(existing_offset);
                        return Ok(AppendResult {
                            next_offset: offset.clone(),
                            offset,
                            deduplicated: true,
                        });
                    }
                }
                return Err(StreamError::AlreadyClosed(id.clone(), None));
            }
            _ => {}
        }

        // Append to the open stream.
        let result = self.append(id, req)?;

        // Close the stream.
        let now = now_micros();
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute(
            "UPDATE streams SET state = 'closed', closed_at = ?1 WHERE id = ?2",
            params![now, id.0],
        )?;
        drop(conn);

        Ok(result)
    }

    fn delete(&self, id: &StreamId) -> Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");

        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM streams WHERE id = ?1",
                params![id.0],
                |_| Ok(true),
            )
            .optional()?
            .is_some();

        if !exists {
            return Err(StreamError::NotFound(id.clone()));
        }

        // Remove events first (foreign key constraint), then mark stream deleted.
        conn.execute(
            "DELETE FROM stream_events WHERE stream_id = ?1",
            params![id.0],
        )?;
        conn.execute(
            "UPDATE streams SET state = 'deleted' WHERE id = ?1",
            params![id.0],
        )?;
        drop(conn);

        Ok(())
    }

    fn list(&self, tag_filter: Option<(&str, &str)>) -> Result<Vec<StreamMeta>> {
        type Row = (String, String, String, i64, Option<i64>, Option<i64>, Option<i64>, String);

        let conn = self.conn.lock().expect("mutex poisoned");

        let metas: Vec<Row> = if let Some((key, value)) = tag_filter {
            let pattern = format!("%\"{key}\":\"{value}\"%");
            let mut stmt = conn.prepare(
                "SELECT id, content_type, state, created_at, closed_at, ttl_seconds, expires_at, tags_json
                 FROM streams
                 WHERE state != 'deleted' AND tags_json LIKE ?1",
            )?;
            stmt.query_map(params![pattern], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?))
            })?
            .collect::<rusqlite::Result<Vec<Row>>>()?
        } else {
            let mut stmt = conn.prepare(
                "SELECT id, content_type, state, created_at, closed_at, ttl_seconds, expires_at, tags_json
                 FROM streams WHERE state != 'deleted'",
            )?;
            stmt.query_map(params![], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?))
            })?
            .collect::<rusqlite::Result<Vec<Row>>>()?
        };
        drop(conn);

        metas
            .into_iter()
            .map(|(sid, ct, state, created, closed, ttl_secs, expires, tags_json)| {
                row_to_meta(sid, &ct, &state, created, closed, ttl_secs, expires, &tags_json)
                    .map_err(StreamError::Storage)
            })
            .collect()
    }

    fn fork(
        &self,
        source: &StreamId,
        up_to: &Offset,
        dest: &StreamId,
        tags: Option<HashMap<String, String>>,
    ) -> Result<StreamMeta> {
        // Verify the source exists by calling head — returns NotFound if absent.
        let source_meta = self.head(source)?;
        self.create(dest, &source_meta.content_type, tags, &CreateOptions::default())?;
        let read = self.read(source, &Offset::beginning(), usize::MAX)?;
        for event in read.events {
            if event.offset > *up_to {
                break;
            }
            self.append(
                dest,
                AppendRequest {
                    data: event.data,
                    producer_id: None,
                    epoch: None,
                    seq: None,
                    stream_seq: None,
                },
            )?;
        }
        self.head(dest)
    }

    fn register_producer(&self, id: &StreamId, producer_id: &ProducerId) -> Result<ProducerEpoch> {
        self.head(id)?; // verify stream exists
        let conn = self.conn.lock().expect("mutex poisoned");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        conn.execute(
            "INSERT INTO stream_producers (stream_id, producer_id, epoch, registered_at)
             VALUES (?1, ?2, 1, ?3)
             ON CONFLICT(stream_id, producer_id) DO UPDATE SET
                epoch = stream_producers.epoch + 1,
                registered_at = excluded.registered_at",
            params![id.0, producer_id.0, now],
        )?;
        #[expect(
            clippy::cast_sign_loss,
            reason = "epoch is always a positive integer stored as i64"
        )]
        let epoch: u64 = conn.query_row(
            "SELECT epoch FROM stream_producers WHERE stream_id = ?1 AND producer_id = ?2",
            params![id.0, producer_id.0],
            |r| r.get::<_, i64>(0),
        )? as u64;
        Ok(ProducerEpoch(epoch))
    }

    fn list_producers(&self, id: &StreamId) -> Result<Vec<ProducerInfo>> {
        self.head(id)?;
        let conn = self.conn.lock().expect("mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT producer_id, epoch, registered_at, last_append_at
             FROM stream_producers WHERE stream_id = ?1
             ORDER BY registered_at",
        )?;
        let rows = stmt.query_map(params![id.0], |row| {
            #[expect(
                clippy::cast_sign_loss,
                reason = "epoch is always a positive integer stored as i64"
            )]
            let epoch = ProducerEpoch(row.get::<_, i64>(1)? as u64);
            Ok(ProducerInfo {
                producer_id: ProducerId(row.get(0)?),
                epoch,
                registered_at: row.get(2)?,
                last_append_at: row.get(3)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::types::{CreateOptions, ProducerEpoch, ProducerId, ProducerSeq, StreamSeq};

    fn store() -> SqliteStore {
        SqliteStore::open_in_memory().expect("in-memory store")
    }

    fn stream_id(s: &str) -> StreamId {
        StreamId(s.to_owned())
    }

    fn append(store: &SqliteStore, id: &StreamId, data: &[u8]) -> AppendResult {
        store
            .append(
                id,
                AppendRequest {
                    data: data.to_vec(),
                    producer_id: None,
                    epoch: None,
                    seq: None,
                    stream_seq: None,
                },
            )
            .expect("append failed")
    }

    // -----------------------------------------------------------------------

    #[test]
    fn create_and_head() {
        let s = store();
        let id = stream_id("my-stream");
        let meta = s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");
        assert_eq!(meta.id, id);
        assert_eq!(meta.state, StreamState::Open);

        let head = s.head(&id).expect("head");
        assert_eq!(head.id, id);
    }

    #[test]
    fn create_is_idempotent() {
        let s = store();
        let id = stream_id("dup");
        let meta1 = s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("first create");
        let meta2 = s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("second create should succeed");
        assert_eq!(meta1.id, meta2.id);
        assert_eq!(meta2.state, StreamState::Open);
    }

    #[test]
    fn head_not_found() {
        let s = store();
        let err = s.head(&stream_id("ghost")).expect_err("should not exist");
        assert!(matches!(err, StreamError::NotFound(_)));
    }

    #[test]
    fn close_and_reclose() {
        let s = store();
        let id = stream_id("closeable");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        let meta = s.close(&id).expect("close");
        assert_eq!(meta.state, StreamState::Closed);

        // §5.3: closing an already-closed stream is idempotent.
        let meta2 = s.close(&id).expect("re-close should succeed");
        assert_eq!(meta2.state, StreamState::Closed);
    }

    #[test]
    fn close_already_closed_is_idempotent() {
        let s = store();
        let id = stream_id("close-idemp");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        s.close(&id).unwrap();
        let meta = s.close(&id).expect("second close should succeed");
        assert_eq!(meta.state, StreamState::Closed);
    }

    #[test]
    fn append_and_close_atomic() {
        let s = store();
        let id = stream_id("append-close");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        let result = s
            .append_and_close(
                &id,
                AppendRequest {
                    data: b"final".to_vec(),
                    producer_id: None,
                    epoch: None,
                    seq: None,
                    stream_seq: None,
                },
            )
            .expect("append_and_close");
        assert!(!result.deduplicated);
        let head = s.head(&id).unwrap();
        assert_eq!(head.state, StreamState::Closed);
        let read = s.read(&id, &Offset::beginning(), 100).unwrap();
        assert_eq!(read.events.len(), 1);
        assert!(read.stream_closed);
    }

    #[test]
    fn append_and_close_on_already_closed_errors() {
        let s = store();
        let id = stream_id("append-close-fail");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        s.close(&id).unwrap();
        let err = s
            .append_and_close(
                &id,
                AppendRequest {
                    data: b"too late".to_vec(),
                    producer_id: None,
                    epoch: None,
                    seq: None,
                    stream_seq: None,
                },
            )
            .expect_err("should fail");
        assert!(matches!(err, StreamError::AlreadyClosed(..)));
    }

    #[test]
    fn delete_removes_events() {
        let s = store();
        let id = stream_id("deletable");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        append(&s, &id, b"event1");
        append(&s, &id, b"event2");

        s.delete(&id).expect("delete");

        // Read should fail with Deleted.
        let err = s
            .read(&id, &Offset::beginning(), 100)
            .expect_err("read deleted");
        assert!(matches!(err, StreamError::Deleted(_)));
    }

    #[test]
    fn append_and_read() {
        let s = store();
        let id = stream_id("appendable");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();

        append(&s, &id, b"hello");
        append(&s, &id, b"world");

        let result = s.read(&id, &Offset::beginning(), 100).expect("read");
        assert_eq!(result.events.len(), 2);
        assert_eq!(result.events[0].data, b"hello");
        assert_eq!(result.events[1].data, b"world");
        assert!(result.up_to_date);
        assert!(!result.stream_closed);
    }

    #[test]
    fn read_with_offset_resumes() {
        let s = store();
        let id = stream_id("resume");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();

        append(&s, &id, b"a");
        let r1 = append(&s, &id, b"b");
        append(&s, &id, b"c");

        // Read after the offset of "b" — should get only "c".
        let result = s.read(&id, &r1.offset, 100).expect("read");
        assert_eq!(result.events.len(), 1);
        assert_eq!(result.events[0].data, b"c");
    }

    #[test]
    fn read_now_returns_empty() {
        let s = store();
        let id = stream_id("now-test");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        append(&s, &id, b"pre-existing");

        let result = s.read(&id, &Offset::now(), 100).expect("read");
        assert_eq!(result.events.len(), 0);
        assert!(result.up_to_date);
    }

    #[test]
    fn append_to_closed_fails() {
        let s = store();
        let id = stream_id("closed-append");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        s.close(&id).unwrap();

        let err = s
            .append(
                &id,
                AppendRequest {
                    data: b"nope".to_vec(),
                    producer_id: None,
                    epoch: None,
                    seq: None,
                    stream_seq: None,
                },
            )
            .expect_err("append to closed");
        assert!(matches!(err, StreamError::AlreadyClosed(..)));
    }

    #[test]
    fn read_closed_returns_data() {
        let s = store();
        let id = stream_id("closed-readable");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        append(&s, &id, b"event");
        s.close(&id).unwrap();

        let result = s.read(&id, &Offset::beginning(), 100).expect("read closed");
        assert_eq!(result.events.len(), 1);
        assert!(result.stream_closed);
    }

    #[test]
    fn producer_dedup() {
        let s = store();
        let id = stream_id("dedup");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();

        let pid = ProducerId("p1".to_owned());
        let req = || AppendRequest {
            data: b"once".to_vec(),
            producer_id: Some(pid.clone()),
            epoch: Some(ProducerEpoch(1)),
            seq: Some(ProducerSeq(0)),
            stream_seq: None,
        };

        let r1 = s.append(&id, req()).expect("first append");
        let r2 = s.append(&id, req()).expect("duplicate append");

        assert!(!r1.deduplicated);
        assert!(r2.deduplicated);
        assert_eq!(r1.offset, r2.offset);

        // Only one event stored.
        let result = s.read(&id, &Offset::beginning(), 100).unwrap();
        assert_eq!(result.events.len(), 1);
    }

    #[test]
    fn producer_epoch_fencing() {
        let s = store();
        let id = stream_id("fencing");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();

        let pid = ProducerId("p2".to_owned());

        // Append with epoch 5.
        s.append(
            &id,
            AppendRequest {
                data: b"epoch5".to_vec(),
                producer_id: Some(pid.clone()),
                epoch: Some(ProducerEpoch(5)),
                seq: Some(ProducerSeq(0)),
                stream_seq: None,
            },
        )
        .expect("epoch 5 append");

        // Append with epoch 3 (older) — should be fenced.
        let err = s
            .append(
                &id,
                AppendRequest {
                    data: b"epoch3".to_vec(),
                    producer_id: Some(pid.clone()),
                    epoch: Some(ProducerEpoch(3)),
                    seq: Some(ProducerSeq(0)),
                    stream_seq: None,
                },
            )
            .expect_err("should be fenced");

        assert!(
            matches!(err, StreamError::ProducerFenced { actual: 3, expected: 5 }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn list_streams() {
        let s = store();
        let mut tags = HashMap::new();
        tags.insert("env".to_owned(), "prod".to_owned());

        s.create(&stream_id("s1"), &ContentType::OctetStream, Some(tags.clone()), &CreateOptions::default()).unwrap();
        s.create(&stream_id("s2"), &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();

        let all = s.list(None).expect("list all");
        assert_eq!(all.len(), 2);

        let prod = s.list(Some(("env", "prod"))).expect("list prod");
        assert_eq!(prod.len(), 1);
        assert_eq!(prod[0].id, stream_id("s1"));
    }

    #[test]
    fn list_excludes_deleted() {
        let s = store();
        s.create(&stream_id("keep"), &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        s.create(&stream_id("gone"), &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        s.delete(&stream_id("gone")).unwrap();

        let all = s.list(None).expect("list");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, stream_id("keep"));
    }

    #[test]
    fn fork_copies_events_up_to_offset() {
        let store = SqliteStore::open_in_memory().expect("store");
        let src = StreamId("src".to_owned());
        store.create(&src, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create src");

        let r1 = store.append(&src, AppendRequest {
            data: b"event-1".to_vec(), producer_id: None, epoch: None, seq: None, stream_seq: None,
        }).expect("append 1");
        let r2 = store.append(&src, AppendRequest {
            data: b"event-2".to_vec(), producer_id: None, epoch: None, seq: None, stream_seq: None,
        }).expect("append 2");
        let _r3 = store.append(&src, AppendRequest {
            data: b"event-3".to_vec(), producer_id: None, epoch: None, seq: None, stream_seq: None,
        }).expect("append 3");

        let dest = StreamId("fork-dest".to_owned());
        let meta = store.fork(&src, &r2.offset, &dest, None).expect("fork");
        assert_eq!(meta.id.0, "fork-dest");

        let read = store.read(&dest, &Offset::beginning(), 100).expect("read fork");
        assert_eq!(read.events.len(), 2, "fork should have 2 events (up to r2)");
        assert_eq!(read.events[0].data, b"event-1");
        assert_eq!(read.events[1].data, b"event-2");

        // Suppress unused warning
        let _ = r1;
    }

    #[test]
    fn fork_nonexistent_source_errors() {
        let store = SqliteStore::open_in_memory().expect("store");
        let src = StreamId("nope".to_owned());
        let dest = StreamId("fork-dest".to_owned());
        let result = store.fork(&src, &Offset::beginning(), &dest, None);
        assert!(result.is_err(), "fork of nonexistent source should fail");
    }

    #[test]
    fn fork_empty_source_creates_empty_dest() {
        let store = SqliteStore::open_in_memory().expect("store");
        let src = StreamId("empty-src".to_owned());
        store.create(&src, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");
        let dest = StreamId("fork-empty".to_owned());
        let meta = store.fork(&src, &Offset::now(), &dest, None).expect("fork");
        assert_eq!(meta.id.0, "fork-empty");
        let read = store.read(&dest, &Offset::beginning(), 100).expect("read");
        assert_eq!(read.events.len(), 0);
    }

    // -----------------------------------------------------------------------
    // Producer registration tests
    // -----------------------------------------------------------------------

    #[test]
    fn register_producer_assigns_epoch() {
        let store = SqliteStore::open_in_memory().expect("store");
        let id = StreamId("multi".to_owned());
        store.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");

        let reg1 = store.register_producer(&id, &ProducerId("writer-a".to_owned())).expect("reg1");
        assert_eq!(reg1.0, 1);

        let reg2 = store.register_producer(&id, &ProducerId("writer-b".to_owned())).expect("reg2");
        assert_eq!(reg2.0, 1);

        let reg3 = store.register_producer(&id, &ProducerId("writer-a".to_owned())).expect("reg3");
        assert_eq!(reg3.0, 2);
    }

    #[test]
    fn list_producers_returns_registered() {
        let store = SqliteStore::open_in_memory().expect("store");
        let id = StreamId("multi-list".to_owned());
        store.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");

        store.register_producer(&id, &ProducerId("a".to_owned())).expect("reg");
        store.register_producer(&id, &ProducerId("b".to_owned())).expect("reg");

        let producers = store.list_producers(&id).expect("list");
        assert_eq!(producers.len(), 2);
        assert_eq!(producers[0].producer_id.0, "a");
        assert_eq!(producers[1].producer_id.0, "b");
    }

    #[test]
    fn multi_writer_epoch_fencing() {
        let store = SqliteStore::open_in_memory().expect("store");
        let id = StreamId("fenced".to_owned());
        store.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");

        let pid_a = ProducerId("writer-a".to_owned());
        let pid_b = ProducerId("writer-b".to_owned());
        let epoch_a = store.register_producer(&id, &pid_a).expect("reg a");
        let epoch_b = store.register_producer(&id, &pid_b).expect("reg b");

        // Both write successfully
        store.append(&id, AppendRequest {
            data: b"from-a".to_vec(),
            producer_id: Some(pid_a.clone()),
            epoch: Some(epoch_a),
            seq: Some(ProducerSeq(0)),
            stream_seq: None,
        }).expect("a writes");

        store.append(&id, AppendRequest {
            data: b"from-b".to_vec(),
            producer_id: Some(pid_b.clone()),
            epoch: Some(epoch_b),
            seq: Some(ProducerSeq(0)),
            stream_seq: None,
        }).expect("b writes");

        // Re-register writer-a bumps epoch, fencing old
        let epoch_a2 = store.register_producer(&id, &pid_a).expect("re-reg a");
        assert!(epoch_a2.0 > epoch_a.0);

        // Old epoch fenced
        let fenced = store.append(&id, AppendRequest {
            data: b"stale-a".to_vec(),
            producer_id: Some(pid_a.clone()),
            epoch: Some(epoch_a),
            seq: Some(ProducerSeq(1)),
            stream_seq: None,
        });
        assert!(matches!(fenced, Err(crate::error::StreamError::ProducerFenced { .. })));

        // New epoch works
        store.append(&id, AppendRequest {
            data: b"fresh-a".to_vec(),
            producer_id: Some(pid_a),
            epoch: Some(epoch_a2),
            seq: Some(ProducerSeq(0)),
            stream_seq: None,
        }).expect("new epoch");

        // writer-b unaffected
        store.append(&id, AppendRequest {
            data: b"from-b-2".to_vec(),
            producer_id: Some(pid_b),
            epoch: Some(epoch_b),
            seq: Some(ProducerSeq(1)),
            stream_seq: None,
        }).expect("b still works");

        let read = store.read(&id, &Offset::beginning(), 100).expect("read");
        assert_eq!(read.events.len(), 4);
    }

    #[test]
    fn register_producer_nonexistent_stream_errors() {
        let store = SqliteStore::open_in_memory().expect("store");
        let id = StreamId("nope".to_owned());
        let result = store.register_producer(&id, &ProducerId("x".to_owned()));
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Content-type and next_offset tests (spec compliance)
    // -----------------------------------------------------------------------

    #[test]
    fn create_stores_explicit_content_type() {
        let s = store();
        let id = stream_id("ct-ndjson");

        let meta = s
            .create(&id, &ContentType::NdJson, None, &CreateOptions::default())
            .expect("create with NdJson");
        assert_eq!(meta.content_type, ContentType::NdJson);

        // Idempotent second call — returns existing row with correct type.
        let meta2 = s
            .create(&id, &ContentType::OctetStream, None, &CreateOptions::default())
            .expect("idempotent create");
        assert_eq!(meta2.content_type, ContentType::NdJson, "idempotent create must not overwrite content_type");
    }

    #[test]
    fn create_stores_custom_content_type() {
        let s = store();
        let id = stream_id("ct-custom");
        let ct = ContentType::Custom("text/plain".to_owned());
        let meta = s.create(&id, &ct, None, &CreateOptions::default()).expect("create with custom type");
        assert_eq!(meta.content_type, ct);

        // Verify it round-trips through the database via head().
        let head = s.head(&id).expect("head");
        assert_eq!(head.content_type, ct);
    }

    #[test]
    fn head_returns_next_offset_none_when_empty() {
        let s = store();
        let id = stream_id("empty-next-offset");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");

        let meta = s.head(&id).expect("head");
        // Empty stream → next_offset is the "now" sentinel.
        assert_eq!(
            meta.next_offset,
            Some(Offset::now()),
            "empty stream next_offset should be now sentinel"
        );
    }

    #[test]
    fn head_returns_next_offset_after_appends() {
        let s = store();
        let id = stream_id("next-offset-after-appends");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");

        append(&s, &id, b"first");
        let last = append(&s, &id, b"second");

        let meta = s.head(&id).expect("head");
        assert_eq!(
            meta.next_offset,
            Some(last.offset),
            "next_offset should equal the last appended offset"
        );
    }

    #[test]
    fn stream_seq_rejects_regression() {
        let s = store();
        let id = stream_id("seq-test");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        s.append(&id, AppendRequest {
            data: b"a".to_vec(),
            producer_id: None, epoch: None, seq: None,
            stream_seq: Some(StreamSeq("002".to_owned())),
        }).unwrap();

        let err = s.append(&id, AppendRequest {
            data: b"b".to_vec(),
            producer_id: None, epoch: None, seq: None,
            stream_seq: Some(StreamSeq("001".to_owned())),
        }).expect_err("should reject");
        assert!(matches!(err, StreamError::SequenceRegression { .. }));
    }

    #[test]
    fn producer_sequence_gap_detected() {
        let s = store();
        let id = stream_id("gap-test");
        s.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).unwrap();
        let pid = ProducerId("p".to_owned());
        s.append(&id, AppendRequest {
            data: b"seq0".to_vec(),
            producer_id: Some(pid.clone()),
            epoch: Some(ProducerEpoch(0)),
            seq: Some(ProducerSeq(0)),
            stream_seq: None,
        }).unwrap();

        let err = s.append(&id, AppendRequest {
            data: b"seq5".to_vec(),
            producer_id: Some(pid),
            epoch: Some(ProducerEpoch(0)),
            seq: Some(ProducerSeq(5)),
            stream_seq: None,
        }).expect_err("should detect gap");
        assert!(matches!(err, StreamError::ProducerSequenceGap { expected: 1, received: 5 }));
    }

    #[test]
    fn create_with_ttl() {
        let s = store();
        let id = stream_id("ttl-stream");
        let opts = CreateOptions { ttl: Some(Duration::from_secs(3600)), ..Default::default() };
        let meta = s.create(&id, &ContentType::OctetStream, None, &opts).unwrap();
        assert_eq!(meta.ttl, Some(Duration::from_secs(3600)));
    }

    #[test]
    fn create_and_close() {
        let s = store();
        let id = stream_id("pre-closed");
        let opts = CreateOptions { closed: true, ..Default::default() };
        let meta = s.create(&id, &ContentType::OctetStream, None, &opts).unwrap();
        assert_eq!(meta.state, StreamState::Closed);
    }
}
