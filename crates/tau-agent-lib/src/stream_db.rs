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
                    stream_seq: None,
                };
                if let Err(e) = ds.append(&stream_id, req) {
                    tracing::warn!(session_id, %e, "stream append failed (legacy write succeeded)");
                }
            }
        }
        Ok(())
    }

    /// Create a session in the legacy sessions table and, when a durable
    /// stream backend is configured, also create the session stream and emit
    /// a [`SessionMeta`] event with the session's initial metadata.
    pub fn create_session(&self, session: &crate::db::StoredSession) -> crate::Result<()> {
        self.db.create_session(session)?;

        if let Some(ref ds) = self.streams {
            let stream_id = tau_streams::StreamId(format!("session-{}", session.id));
            let mut tags = std::collections::HashMap::new();
            tags.insert("type".to_string(), "session".to_string());
            tags.insert("session_id".to_string(), session.id.clone());
            if let Some(ref project) = session.project_name {
                tags.insert("project".to_string(), project.clone());
            }
            let _ = ds.create(&stream_id, &tau_streams::ContentType::NdJson, Some(tags), &tau_streams::CreateOptions::default());

            let meta_event = crate::stream_events::SessionEvent::SessionMeta {
                key: "created".to_string(),
                value: serde_json::json!({
                    "model": session.model.id,
                    "cwd": session.cwd,
                    "project_name": session.project_name,
                    "parent_id": session.parent_id,
                }),
            };
            let data = meta_event.to_ndjson_bytes("server");
            let _ = ds.append(&stream_id, tau_streams::AppendRequest {
                data, producer_id: None, epoch: None, seq: None, stream_seq: None,
            });
        }
        Ok(())
    }

    /// Queue a message in the legacy queued_messages table and, when a durable
    /// stream backend is configured, also emit an [`InboxEvent`] to the
    /// target session's inbox stream.
    pub fn queue_message(&self, target: &str, content: &str, sender_info: &str) -> crate::Result<()> {
        self.db.queue_message(target, content, sender_info)?;

        if let Some(ref ds) = self.streams {
            let stream_id = tau_streams::StreamId(format!("inbox-{target}"));
            let _ = ds.create(&stream_id, &tau_streams::ContentType::NdJson, {
                let mut tags = std::collections::HashMap::new();
                tags.insert("type".to_string(), "inbox".to_string());
                tags.insert("session_id".to_string(), target.to_string());
                Some(tags)
            }, &tau_streams::CreateOptions::default());
            let event = crate::stream_events::InboxEvent::Message {
                content: content.to_string(),
                sender_info: sender_info.to_string(),
            };
            let data = event.to_ndjson_bytes("server");
            let _ = ds.append(&stream_id, tau_streams::AppendRequest {
                data, producer_id: None, epoch: None, seq: None, stream_seq: None,
            });
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
