//! Live delivery hub for real-time stream event fanout.
//!
//! [`LiveDeliveryHub`] manages a set of broadcast channels — one per stream —
//! and provides lazy creation, notification, and teardown.

use dashmap::DashMap;
use tokio::sync::broadcast;

use crate::types::{Offset, StreamId};

// ---------------------------------------------------------------------------
// LiveEvent
// ---------------------------------------------------------------------------

/// Events dispatched to live subscribers of a stream.
#[derive(Debug, Clone)]
pub enum LiveEvent {
    /// A new event was appended to the stream.
    Data {
        /// The offset assigned to the appended event.
        offset: Offset,
        /// The raw event bytes.
        data: Vec<u8>,
    },
    /// The stream has been closed; no further events will arrive.
    Closed,
}

// ---------------------------------------------------------------------------
// LiveDeliveryHub
// ---------------------------------------------------------------------------

/// Hub for broadcasting live stream events to multiple subscribers.
///
/// Channels are created lazily on first subscribe and removed when a stream is
/// closed.  Wrap in [`std::sync::Arc`] to share across tasks.
#[derive(Debug)]
pub struct LiveDeliveryHub {
    channels: DashMap<StreamId, broadcast::Sender<LiveEvent>>,
    capacity: usize,
}

impl Default for LiveDeliveryHub {
    fn default() -> Self {
        Self::new(256)
    }
}

impl LiveDeliveryHub {
    /// Creates a hub with the given per-channel broadcast capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            channels: DashMap::new(),
            capacity,
        }
    }

    /// Sends a [`LiveEvent::Data`] event to all current subscribers of `stream_id`.
    ///
    /// Does nothing if the stream has no channel (i.e. no active subscribers).
    pub fn notify(&self, stream_id: &StreamId, offset: Offset, data: Vec<u8>) {
        if let Some(tx) = self.channels.get(stream_id) {
            // send() returns Err only when there are no receivers; that is fine.
            let _ = tx.send(LiveEvent::Data { offset, data });
        }
    }

    /// Sends a [`LiveEvent::Closed`] event to all current subscribers and
    /// removes the channel from the hub.
    pub fn close_stream(&self, stream_id: &StreamId) {
        if let Some((_, tx)) = self.channels.remove(stream_id) {
            let _ = tx.send(LiveEvent::Closed);
        }
    }

    /// Returns a receiver for live events on `stream_id`.
    ///
    /// Creates the broadcast channel if it does not yet exist.
    #[must_use]
    pub fn subscribe(&self, stream_id: &StreamId) -> broadcast::Receiver<LiveEvent> {
        // Use entry API for lazy creation.
        let tx = self
            .channels
            .entry(stream_id.clone())
            .or_insert_with(|| broadcast::channel(self.capacity).0);
        tx.subscribe()
    }

    /// Returns the number of active receivers for `stream_id`.
    ///
    /// Returns `0` if no channel exists for the stream.
    #[must_use]
    pub fn subscriber_count(&self, stream_id: &StreamId) -> usize {
        self.channels
            .get(stream_id)
            .map_or(0, |tx| tx.receiver_count())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::TryRecvError;

    fn sid(s: &str) -> StreamId {
        StreamId(s.to_owned())
    }

    #[tokio::test]
    async fn subscribe_and_receive() {
        let hub = LiveDeliveryHub::default();
        let id = sid("s1");
        let mut rx = hub.subscribe(&id);

        hub.notify(&id, Offset("off1".into()), b"hello".to_vec());

        let ev = rx.recv().await.expect("event");
        assert!(matches!(ev, LiveEvent::Data { .. }));
        if let LiveEvent::Data { offset, data } = ev {
            assert_eq!(offset.0, "off1");
            assert_eq!(data, b"hello");
        }
    }

    #[tokio::test]
    async fn close_sends_event() {
        let hub = LiveDeliveryHub::default();
        let id = sid("s2");
        let mut rx = hub.subscribe(&id);

        hub.close_stream(&id);

        let ev = rx.recv().await.expect("closed event");
        assert!(matches!(ev, LiveEvent::Closed));
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let hub = LiveDeliveryHub::default();
        let id = sid("s3");
        let mut rx1 = hub.subscribe(&id);
        let mut rx2 = hub.subscribe(&id);

        assert_eq!(hub.subscriber_count(&id), 2);

        hub.notify(&id, Offset("o".into()), b"data".to_vec());

        let ev1 = rx1.recv().await.expect("rx1 event");
        let ev2 = rx2.recv().await.expect("rx2 event");
        assert!(matches!(ev1, LiveEvent::Data { .. }));
        assert!(matches!(ev2, LiveEvent::Data { .. }));
    }

    #[tokio::test]
    async fn notify_without_subscribers_is_noop() {
        let hub = LiveDeliveryHub::default();
        let id = sid("s4");
        // Should not panic; no channel exists.
        hub.notify(&id, Offset("o".into()), b"data".to_vec());
    }

    #[tokio::test]
    async fn subscriber_count_zero_for_unknown() {
        let hub = LiveDeliveryHub::default();
        let id = sid("unknown");
        assert_eq!(hub.subscriber_count(&id), 0);
    }

    #[tokio::test]
    async fn close_removes_channel() {
        let hub = LiveDeliveryHub::default();
        let id = sid("s5");
        let mut rx = hub.subscribe(&id);
        hub.close_stream(&id);

        // Channel removed; subsequent notify is a noop.
        hub.notify(&id, Offset("o2".into()), b"after-close".to_vec());

        // We should receive Closed, not the post-close notification.
        let ev = rx.recv().await.expect("event");
        assert!(matches!(ev, LiveEvent::Closed));

        // No further events.
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty | TryRecvError::Closed)));
    }
}
