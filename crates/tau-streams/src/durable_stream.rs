//! [`DurableStream`] — a stream store combined with live delivery.
//!
//! All mutating operations (append, close) are forwarded to both the backing
//! [`StreamStore`] and the [`LiveDeliveryHub`], keeping durable storage and
//! live subscribers in sync.

use std::{collections::HashMap, sync::Arc};

use crate::{
    error::Result,
    hub::LiveDeliveryHub,
    store::StreamStore,
    types::{AppendRequest, AppendResult, Offset, ReadResult, StreamId, StreamMeta},
};

// ---------------------------------------------------------------------------
// DurableStream
// ---------------------------------------------------------------------------

/// Combines a [`StreamStore`] with a [`LiveDeliveryHub`] to provide both
/// durable persistence and real-time event delivery.
///
/// Wrap in [`Arc`] to share across tasks.
#[derive(Debug)]
pub struct DurableStream<S: StreamStore> {
    store: S,
    hub: Arc<LiveDeliveryHub>,
}

impl<S: StreamStore> DurableStream<S> {
    /// Creates a new `DurableStream` backed by `store` and notifying `hub`.
    pub const fn new(store: S, hub: Arc<LiveDeliveryHub>) -> Self {
        Self { store, hub }
    }

    /// Returns a reference to the underlying store.
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Returns a reference to the hub.
    pub const fn hub(&self) -> &Arc<LiveDeliveryHub> {
        &self.hub
    }

    /// Creates a new stream.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying store.
    pub fn create(
        &self,
        id: &StreamId,
        tags: Option<HashMap<String, String>>,
    ) -> Result<StreamMeta> {
        self.store.create(id, tags)
    }

    /// Appends an event to a stream and notifies live subscribers.
    ///
    /// If the append is a duplicate (deduplicated = true) the hub is **not**
    /// notified — the subscriber already received the original event.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying store.
    pub fn append(&self, id: &StreamId, req: AppendRequest) -> Result<AppendResult> {
        // Clone data before moving req into the store so we can forward it to
        // the hub on a successful, non-deduplicated append.
        let data_for_hub = req.data.clone();

        let result = self.store.append(id, req)?;

        if !result.deduplicated {
            self.hub.notify(id, result.offset.clone(), data_for_hub);
        }

        Ok(result)
    }

    /// Reads events from the stream.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying store.
    pub fn read(&self, id: &StreamId, offset: &Offset, limit: usize) -> Result<ReadResult> {
        self.store.read(id, offset, limit)
    }

    /// Returns the current metadata for a stream.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying store.
    pub fn head(&self, id: &StreamId) -> Result<StreamMeta> {
        self.store.head(id)
    }

    /// Closes the stream in the store and sends a [`crate::hub::LiveEvent::Closed`]
    /// event to all live subscribers.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying store.
    pub fn close(&self, id: &StreamId) -> Result<StreamMeta> {
        let meta = self.store.close(id)?;
        self.hub.close_stream(id);
        Ok(meta)
    }

    /// Deletes the stream.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying store.
    pub fn delete(&self, id: &StreamId) -> Result<()> {
        self.store.delete(id)
    }

    /// Lists streams, optionally filtering by a tag key/value pair.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying store.
    pub fn list(&self, tag_filter: Option<(&str, &str)>) -> Result<Vec<StreamMeta>> {
        self.store.list(tag_filter)
    }

    /// Creates a new stream by copying events from `source` up to and
    /// including `up_to` offset into `dest`.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying store, including
    /// [`crate::error::StreamError::NotFound`] if the source stream does not exist.
    pub fn fork(
        &self,
        source: &StreamId,
        up_to: &Offset,
        dest: &StreamId,
        tags: Option<HashMap<String, String>>,
    ) -> Result<StreamMeta> {
        self.store.fork(source, up_to, dest, tags)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::{hub::LiveEvent, sqlite_store::SqliteStore};

    fn make_durable() -> DurableStream<SqliteStore> {
        let store = SqliteStore::open_in_memory().expect("in-memory store");
        let hub = Arc::new(LiveDeliveryHub::default());
        DurableStream::new(store, hub)
    }

    fn sid(s: &str) -> StreamId {
        StreamId(s.to_owned())
    }

    #[tokio::test]
    async fn append_notifies_subscribers() {
        let ds = make_durable();
        let id = sid("stream-notify");
        ds.create(&id, None).expect("create");

        let mut rx = ds.hub().subscribe(&id);

        ds.append(
            &id,
            AppendRequest {
                data: b"payload".to_vec(),
                producer_id: None,
                epoch: None,
                seq: None,
            },
        )
        .expect("append");

        let ev = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout")
            .expect("recv");

        assert!(
            matches!(ev, LiveEvent::Data { .. }),
            "expected Data event, got {ev:?}"
        );
        if let LiveEvent::Data { data, .. } = ev {
            assert_eq!(data, b"payload");
        }
    }

    #[tokio::test]
    async fn dedup_does_not_notify() {
        use crate::types::{ProducerEpoch, ProducerId, ProducerSeq};

        let ds = make_durable();
        let id = sid("stream-dedup");
        ds.create(&id, None).expect("create");

        let mut rx = ds.hub().subscribe(&id);

        let req = || AppendRequest {
            data: b"once".to_vec(),
            producer_id: Some(ProducerId("p".to_owned())),
            epoch: Some(ProducerEpoch(1)),
            seq: Some(ProducerSeq(0)),
        };

        // First append — real; should notify.
        ds.append(&id, req()).expect("first append");
        let ev = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        assert!(matches!(ev, LiveEvent::Data { .. }));

        // Second append — duplicate; should NOT produce a second notification.
        let result = ds.append(&id, req()).expect("dedup append");
        assert!(result.deduplicated);

        // Wait briefly; no second event should arrive.
        let second = timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(
            second.is_err(),
            "unexpected second notification for deduplicated append"
        );
    }

    #[tokio::test]
    async fn close_notifies_subscribers() {
        let ds = make_durable();
        let id = sid("stream-close");
        ds.create(&id, None).expect("create");

        let mut rx = ds.hub().subscribe(&id);

        ds.close(&id).expect("close");

        let ev = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout")
            .expect("recv");

        assert!(
            matches!(ev, LiveEvent::Closed),
            "expected Closed event, got {ev:?}"
        );
    }
}
