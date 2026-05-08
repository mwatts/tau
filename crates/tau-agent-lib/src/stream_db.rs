//! Stream-aware database wrapper.
//!
//! `StreamDb` wraps the legacy [`Db`] and an optional [`DurableStream`],
//! providing the same API surface via [`Deref`]. Methods that need
//! dual-write behaviour (legacy table + stream) are overridden as
//! inherent methods which shadow the [`Deref`]'d ones.

use std::sync::Arc;

use crate::db::Db;

type ServerDurableStream = tau_streams::DurableStream<tau_streams::SqliteStore>;

pub struct StreamDb {
    db: Db,
    streams: Option<Arc<ServerDurableStream>>,
}

impl StreamDb {
    pub fn new(db: Db, streams: Option<Arc<ServerDurableStream>>) -> Self {
        Self { db, streams }
    }

    pub fn streams(&self) -> Option<&Arc<ServerDurableStream>> {
        self.streams.as_ref()
    }

    /// Append a message to the legacy messages table and, when a durable
    /// stream backend is configured, also emit the corresponding
    /// [`SessionEvent`] to the session's stream.
    pub fn append_message(&self, session_id: &str, msg: &crate::types::Message) -> crate::Result<()> {
        self.db.append_message(session_id, msg)?;

        if let Some(ref ds) = self.streams {
            let stream_id = tau_streams::StreamId(format!("session-{session_id}"));
            if let Some(event) = message_to_session_event(msg) {
                let data = event.to_ndjson_bytes("server");
                let req = tau_streams::AppendRequest {
                    data,
                    producer_id: None,
                    epoch: None,
                    seq: None,
                };
                if let Err(e) = ds.append(&stream_id, req) {
                    tracing::warn!(session_id, %e, "stream append failed (legacy write succeeded)");
                }
            }
        }
        Ok(())
    }
}

impl std::ops::Deref for StreamDb {
    type Target = Db;
    fn deref(&self) -> &Db {
        &self.db
    }
}

fn message_to_session_event(msg: &crate::types::Message) -> Option<crate::stream_events::SessionEvent> {
    match msg {
        crate::types::Message::User(u) => {
            let content = u.content.iter().filter_map(|c| {
                if let crate::types::UserContent::Text(t) = c {
                    Some(t.text.clone())
                } else {
                    None
                }
            }).collect::<Vec<_>>().join("\n");
            Some(crate::stream_events::SessionEvent::UserMessage {
                content,
                attachments: Vec::new(),
            })
        }
        crate::types::Message::Assistant(a) => {
            let content = a.content.iter().filter_map(|c| {
                if let crate::types::AssistantContent::Text(t) = c {
                    Some(t.text.clone())
                } else {
                    None
                }
            }).collect::<Vec<_>>().join("\n");
            if content.is_empty() { return None; }
            let model = a.model.clone();
            Some(crate::stream_events::SessionEvent::AssistantMessage { content, model })
        }
        _ => None,
    }
}
