//! [`StreamConsumer`] — catch-up + live event consumer for a single stream.
//!
//! A consumer first replays historical events from the [`crate::store::StreamStore`]
//! in batches until it reaches the tail, then switches to receiving live events
//! forwarded by the [`crate::hub::LiveDeliveryHub`].

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::{
    error::Result,
    hub::{LiveDeliveryHub, LiveEvent},
    store::StreamStore,
    types::{Offset, StreamId},
};

// ---------------------------------------------------------------------------
// ConsumerEvent
// ---------------------------------------------------------------------------

/// Events produced by a [`StreamConsumer`].
#[derive(Debug)]
pub enum ConsumerEvent {
    /// A stream event at the given offset.
    Data {
        /// Offset of the event.
        offset: Offset,
        /// Raw event bytes.
        data: Vec<u8>,
    },
    /// The consumer has replayed all historical events and is now at the tail.
    UpToDate,
    /// The stream has been closed; no further events will arrive.
    Closed,
    /// The live broadcast channel fell behind; some events may have been
    /// dropped.  The consumer sets `caught_up = false` and the caller should
    /// re-drive catch-up.
    Lagged,
}

// ---------------------------------------------------------------------------
// StreamConsumer
// ---------------------------------------------------------------------------

/// Combines historical catch-up (via the store) with live delivery (via the
/// hub) for a single stream.
///
/// ## Usage
///
/// 1. Call [`StreamConsumer::catch_up_batch`] in a loop until [`ConsumerEvent::UpToDate`]
///    appears in the returned batch.
/// 2. Switch to [`StreamConsumer::next_live`] for subsequent events.
/// 3. If [`ConsumerEvent::Lagged`] is returned, go back to step 1.
pub struct StreamConsumer<S: StreamStore> {
    stream_id: StreamId,
    store: Arc<S>,
    rx: broadcast::Receiver<LiveEvent>,
    current_offset: Offset,
    caught_up: bool,
    batch_size: usize,
}

impl<S: StreamStore + std::fmt::Debug> std::fmt::Debug for StreamConsumer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamConsumer")
            .field("stream_id", &self.stream_id)
            .field("store", &self.store)
            .field("current_offset", &self.current_offset)
            .field("caught_up", &self.caught_up)
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

impl<S: StreamStore> StreamConsumer<S> {
    /// Creates a new consumer.
    ///
    /// # Parameters
    ///
    /// - `stream_id` — the stream to consume.
    /// - `store` — shared store for catch-up reads.
    /// - `hub` — hub to subscribe to for live events.
    /// - `from` — start offset; pass [`Offset::beginning`] to replay all
    ///   events, or a specific offset to resume from that position.
    /// - `batch_size` — maximum number of events to return per
    ///   [`Self::catch_up_batch`] call.
    pub fn new(
        stream_id: StreamId,
        store: Arc<S>,
        hub: &LiveDeliveryHub,
        from: Offset,
        batch_size: usize,
    ) -> Self {
        let rx = hub.subscribe(&stream_id);
        Self {
            stream_id,
            store,
            rx,
            current_offset: from,
            caught_up: false,
            batch_size,
        }
    }

    /// Returns `true` if the consumer has reached the tail of the store.
    #[must_use]
    pub const fn is_caught_up(&self) -> bool {
        self.caught_up
    }

    /// Returns the most recently processed offset.
    #[must_use]
    pub const fn current_offset(&self) -> &Offset {
        &self.current_offset
    }

    /// Reads the next batch of historical events from the store.
    ///
    /// The returned `Vec` contains zero or more [`ConsumerEvent::Data`] entries
    /// followed by at most one of [`ConsumerEvent::UpToDate`] or
    /// [`ConsumerEvent::Closed`].
    ///
    /// When the batch is empty and `up_to_date` is set this method appends
    /// `UpToDate` and sets `caught_up = true`.  Subsequent calls will keep
    /// returning `[UpToDate]` until a lag event resets the flag.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying store read.
    pub fn catch_up_batch(&mut self) -> Result<Vec<ConsumerEvent>> {
        let result = self
            .store
            .read(&self.stream_id, &self.current_offset, self.batch_size)?;

        let mut events: Vec<ConsumerEvent> = result
            .events
            .into_iter()
            .map(|e| {
                ConsumerEvent::Data {
                    offset: e.offset,
                    data: e.data,
                }
            })
            .collect();

        // Advance the cursor.
        self.current_offset = result.next_offset;

        if result.up_to_date {
            self.caught_up = true;
            events.push(ConsumerEvent::UpToDate);
            if result.stream_closed {
                events.push(ConsumerEvent::Closed);
            }
        } else if result.stream_closed {
            events.push(ConsumerEvent::Closed);
        }

        Ok(events)
    }

    /// Waits for the next live event from the broadcast channel.
    ///
    /// Skips events whose offset is at or before `current_offset` (they were
    /// already delivered via catch-up).  Updates `current_offset` on each
    /// accepted `Data` event.
    ///
    /// Returns [`ConsumerEvent::Lagged`] and resets `caught_up` to `false`
    /// when the broadcast channel reports that messages were missed.  The
    /// caller should then resume catch-up via [`Self::catch_up_batch`].
    ///
    /// Returns [`ConsumerEvent::Closed`] when the channel is closed (stream
    /// teardown) or when a [`LiveEvent::Closed`] is received.
    #[expect(
        clippy::future_not_send,
        reason = "StreamConsumer is intentionally single-task; S need not be Send+Sync"
    )]
    pub async fn next_live(&mut self) -> ConsumerEvent {
        loop {
            match self.rx.recv().await {
                Ok(LiveEvent::Data { offset, data }) => {
                    // Skip events we already saw during catch-up.
                    if offset <= self.current_offset {
                        continue;
                    }
                    self.current_offset = offset.clone();
                    return ConsumerEvent::Data { offset, data };
                }
                Ok(LiveEvent::Closed) | Err(broadcast::error::RecvError::Closed) => {
                    return ConsumerEvent::Closed;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    self.caught_up = false;
                    return ConsumerEvent::Lagged;
                }
            }
        }
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
    use crate::{
        hub::LiveDeliveryHub,
        sqlite_store::SqliteStore,
        types::{AppendRequest, ContentType, CreateOptions, StreamId},
    };

    fn sid(s: &str) -> StreamId {
        StreamId(s.to_owned())
    }

    fn make_store() -> Arc<SqliteStore> {
        Arc::new(SqliteStore::open_in_memory().expect("in-memory store"))
    }

    /// A separate store for live-only tests so the consumer's catch-up is
    /// immediately up-to-date.
    fn make_consumer(
        hub: &LiveDeliveryHub,
        stream_id: &StreamId,
    ) -> StreamConsumer<SqliteStore> {
        let store = make_store();
        // Create the stream in the consumer's own store so reads don't error.
        store.create(stream_id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");
        StreamConsumer::new(
            stream_id.clone(),
            store,
            hub,
            Offset::beginning(),
            64,
        )
    }

    // -----------------------------------------------------------------------
    // Catch-up
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn catch_up_then_live_delivery() {
        let hub = Arc::new(LiveDeliveryHub::default());
        let id = sid("catch-up-live");

        // Populate a shared store with two historical events.
        let shared_store = make_store();
        shared_store.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");
        shared_store
            .append(
                &id,
                AppendRequest {
                    data: b"hist1".to_vec(),
                    producer_id: None,
                    epoch: None,
                    seq: None,
                    stream_seq: None,
                },
            )
            .expect("append hist1");
        shared_store
            .append(
                &id,
                AppendRequest {
                    data: b"hist2".to_vec(),
                    producer_id: None,
                    epoch: None,
                    seq: None,
                    stream_seq: None,
                },
            )
            .expect("append hist2");

        let mut consumer = StreamConsumer::new(
            id.clone(),
            Arc::clone(&shared_store),
            &hub,
            Offset::beginning(),
            64,
        );

        // Catch-up: should deliver hist1, hist2, then UpToDate.
        let batch = consumer.catch_up_batch().expect("catch_up_batch");
        assert!(consumer.is_caught_up());
        let data_events: Vec<_> = batch
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Data { .. }))
            .collect();
        assert_eq!(data_events.len(), 2);
        assert!(batch.iter().any(|e| matches!(e, ConsumerEvent::UpToDate)));

        // Now test live delivery via the hub.
        let live_data = b"live1".to_vec();
        let live_offset = Offset("live-offset-1".into());
        hub.notify(&id, live_offset.clone(), live_data.clone());

        let ev = timeout(Duration::from_secs(1), consumer.next_live())
            .await
            .expect("timeout");
        assert!(
            matches!(ev, ConsumerEvent::Data { .. }),
            "expected Data, got {ev:?}"
        );
        if let ConsumerEvent::Data { data, .. } = ev {
            assert_eq!(data, live_data);
        }
    }

    // -----------------------------------------------------------------------
    // Close propagation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn close_propagation() {
        let hub = Arc::new(LiveDeliveryHub::default());
        let id = sid("close-prop");

        let mut consumer = make_consumer(&hub, &id);

        // Drain catch-up (empty store → immediate UpToDate).
        let batch = consumer.catch_up_batch().expect("catch_up_batch");
        assert!(batch.iter().any(|e| matches!(e, ConsumerEvent::UpToDate)));

        // Close the stream via the hub.
        hub.close_stream(&id);

        let ev = timeout(Duration::from_secs(1), consumer.next_live())
            .await
            .expect("timeout");
        assert!(
            matches!(ev, ConsumerEvent::Closed),
            "expected Closed, got {ev:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Empty store — immediate UpToDate
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn empty_store_is_immediately_up_to_date() {
        let hub = Arc::new(LiveDeliveryHub::default());
        let id = sid("empty");

        let mut consumer = make_consumer(&hub, &id);
        let batch = consumer.catch_up_batch().expect("catch_up_batch");

        assert!(consumer.is_caught_up());
        assert!(batch.iter().any(|e| matches!(e, ConsumerEvent::UpToDate)));
    }

    // -----------------------------------------------------------------------
    // Skips already-seen offsets from the live channel
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn live_skips_stale_offsets() {
        let hub = Arc::new(LiveDeliveryHub::default());
        let id = sid("stale-skip");

        // Populate store with one event and catch up fully.
        let shared_store = make_store();
        shared_store.create(&id, &ContentType::OctetStream, None, &CreateOptions::default()).expect("create");
        let res = shared_store
            .append(
                &id,
                AppendRequest {
                    data: b"first".to_vec(),
                    producer_id: None,
                    epoch: None,
                    seq: None,
                    stream_seq: None,
                },
            )
            .expect("append");
        let seen_offset = res.offset.clone();

        let mut consumer = StreamConsumer::new(
            id.clone(),
            Arc::clone(&shared_store),
            &hub,
            Offset::beginning(),
            64,
        );
        let _batch = consumer.catch_up_batch().expect("catch_up_batch");
        assert!(consumer.is_caught_up());

        // Replay the same offset via the hub — consumer must skip it.
        hub.notify(&id, seen_offset, b"first".to_vec());

        // Then send a genuinely new event.
        hub.notify(&id, Offset("new-offset".into()), b"second".to_vec());

        let ev = timeout(Duration::from_secs(1), consumer.next_live())
            .await
            .expect("timeout");
        if let ConsumerEvent::Data { data, .. } = ev {
            assert_eq!(data, b"second");
        } else {
            panic!("expected Data, got {ev:?}");
        }
    }
}
