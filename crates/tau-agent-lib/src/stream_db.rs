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
}

impl std::ops::Deref for StreamDb {
    type Target = Db;
    fn deref(&self) -> &Db {
        &self.db
    }
}
