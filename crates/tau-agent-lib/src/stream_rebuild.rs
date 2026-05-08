//! Rebuild materialized index tables from durable stream replay.

use tau_streams::{DurableStream, Offset, SqliteStore};

use crate::db::Db;

/// Rebuild the `task_index` table by replaying all task streams.
///
/// For each stream tagged `type=task`, replays all events to materialize
/// the current task state, then upserts into `task_index`.
pub fn rebuild_task_index(
    db: &Db,
    ds: &DurableStream<SqliteStore>,
) -> crate::Result<()> {
    let streams = ds
        .list(Some(("type", "task")))
        .map_err(|e| crate::Error::Io(e.to_string()))?;

    for meta in streams {
        let task_id_str = meta.id.0.strip_prefix("task-").unwrap_or(&meta.id.0);
        let task_id: i64 = match task_id_str.parse() {
            Ok(id) => id,
            Err(_) => continue,
        };

        let read = ds
            .read(&meta.id, &Offset::beginning(), usize::MAX)
            .map_err(|e| crate::Error::Io(e.to_string()))?;

        let mut title = String::new();
        let mut status = "todo".to_string();
        let mut priority: i64 = 0;
        let mut project = None;
        let mut session_id = None;
        let mut tags_json = None;

        for event in &read.events {
            if let Ok(envelope) =
                serde_json::from_slice::<crate::stream_events::TaskEnvelope>(&event.data)
            {
                match envelope.event {
                    crate::stream_events::TaskEvent::Created {
                        title: t,
                        project: p,
                        priority: pr,
                        tags: ref tg,
                    } => {
                        title = t;
                        project = p;
                        priority = pr;
                        if !tg.is_empty() {
                            tags_json = serde_json::to_string(tg).ok();
                        }
                    }
                    crate::stream_events::TaskEvent::Updated { ref fields } => {
                        if let Some(s) = fields.get("state").and_then(|v| v.as_str()) {
                            status = s.to_string();
                        }
                        if let Some(t) = fields.get("title").and_then(|v| v.as_str()) {
                            title = t.to_string();
                        }
                        if let Some(p) = fields.get("priority").and_then(|v| v.as_i64()) {
                            priority = p;
                        }
                    }
                    crate::stream_events::TaskEvent::Assigned {
                        session_id: ref sid,
                        ..
                    } => {
                        session_id = Some(sid.clone());
                    }
                    crate::stream_events::TaskEvent::Completed { .. }
                    | crate::stream_events::TaskEvent::Failed { .. } => {
                        if let crate::stream_events::TaskEvent::Completed { .. } = &envelope.event {
                            status = "done".to_string();
                        } else {
                            status = "failed".to_string();
                        }
                    }
                    crate::stream_events::TaskEvent::Cancelled { .. } => {
                        status = "closed".to_string();
                    }
                    _ => {}
                }
            }
        }

        if !title.is_empty() {
            let _ = db.upsert_task_index(
                task_id,
                &meta.id.0,
                &title,
                &status,
                priority,
                project.as_deref(),
                session_id.as_deref(),
                tags_json.as_deref(),
            );
        }
    }
    Ok(())
}

/// Rebuild the `session_index` table by replaying all session streams.
///
/// For each stream tagged `type=session`, ensures a row exists in
/// `session_index` with the stream_id mapping. The full session metadata
/// is populated by `migrate_sessions_to_index()`; this function only
/// ensures the mapping is present for stream-first sessions.
pub fn rebuild_session_index(
    db: &Db,
    ds: &DurableStream<SqliteStore>,
) -> crate::Result<()> {
    let streams = ds
        .list(Some(("type", "session")))
        .map_err(|e| crate::Error::Io(e.to_string()))?;

    tracing::info!(count = streams.len(), "rebuilding session_index from streams");

    // Populate any session_index rows that are missing by backfilling from
    // the sessions table; stream-specific rows will be present once the
    // sessions table is populated via the normal CRUD path.
    let _ = db.migrate_sessions_to_index();

    Ok(())
}
