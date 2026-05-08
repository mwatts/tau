//! Core domain types for durable streams.

use std::{collections::HashMap, fmt, time::Duration};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// StreamId
// ---------------------------------------------------------------------------

/// Opaque identifier for a stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamId(pub String);

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<S: Into<String>> From<S> for StreamId {
    fn from(s: S) -> Self {
        Self(s.into())
    }
}

// ---------------------------------------------------------------------------
// Offset
// ---------------------------------------------------------------------------

/// Cursor into a stream.
///
/// Sentinel values:
/// - `"-1"` — beginning of the stream (read all events).
/// - `"now"` — tail of the stream (read no historical events).
///
/// Lexicographic ordering is valid for non-sentinel offsets produced by
/// [`crate::offset::OffsetGenerator`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Offset(pub String);

impl Offset {
    /// Returns the sentinel offset that represents the beginning of a stream.
    #[must_use]
    pub fn beginning() -> Self {
        Self("-1".to_owned())
    }

    /// Returns the sentinel offset that represents the current tail (no history).
    #[must_use]
    pub fn now() -> Self {
        Self("now".to_owned())
    }

    /// Returns `true` if this offset is the beginning sentinel (`"-1"`).
    #[must_use]
    pub fn is_beginning(&self) -> bool {
        self.0 == "-1"
    }

    /// Returns `true` if this offset is the `"now"` sentinel.
    #[must_use]
    pub fn is_now(&self) -> bool {
        self.0 == "now"
    }

    /// Returns `true` if this offset is either [`Self::beginning`] or [`Self::now`].
    #[must_use]
    pub fn is_sentinel(&self) -> bool {
        self.is_beginning() || self.is_now()
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<S: Into<String>> From<S> for Offset {
    fn from(s: S) -> Self {
        Self(s.into())
    }
}

// ---------------------------------------------------------------------------
// Producer identity
// ---------------------------------------------------------------------------

/// Identifies a specific producer process.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProducerId(pub String);

impl fmt::Display for ProducerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Monotonically increasing producer epoch, used for fencing stale writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProducerEpoch(pub u64);

/// Per-epoch sequence number for exactly-once deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProducerSeq(pub u64);

/// Information about a registered producer on a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerInfo {
    /// The producer identifier.
    pub producer_id: ProducerId,
    /// Current epoch for this producer (monotonically increasing).
    pub epoch: ProducerEpoch,
    /// Unix timestamp (seconds) when the producer was first (or last) registered.
    pub registered_at: i64,
    /// Unix timestamp (seconds) of the last append by this producer, if any.
    pub last_append_at: Option<i64>,
}

// ---------------------------------------------------------------------------
// ContentType
// ---------------------------------------------------------------------------

/// MIME content type for stream data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentType {
    /// `application/x-ndjson`
    NdJson,
    /// `application/json`
    Json,
    /// `application/octet-stream`
    OctetStream,
    /// Any other MIME type.
    Custom(String),
}

impl ContentType {
    /// Returns the canonical MIME string for this content type.
    #[must_use]
    pub fn as_mime(&self) -> &str {
        match self {
            Self::NdJson => "application/x-ndjson",
            Self::Json => "application/json",
            Self::OctetStream => "application/octet-stream",
            Self::Custom(s) => s.as_str(),
        }
    }

    /// Parses a MIME string into a `ContentType`.
    #[must_use]
    pub fn from_mime(mime: &str) -> Self {
        match mime {
            "application/x-ndjson" => Self::NdJson,
            "application/json" => Self::Json,
            "application/octet-stream" => Self::OctetStream,
            other => Self::Custom(other.to_owned()),
        }
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_mime())
    }
}

// ---------------------------------------------------------------------------
// StreamState
// ---------------------------------------------------------------------------

/// Lifecycle state of a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamState {
    /// Events can be appended.
    Open,
    /// No more events can be appended; existing events are still readable.
    Closed,
    /// Stream has been deleted; events have been removed.
    Deleted,
}

// ---------------------------------------------------------------------------
// StreamMeta
// ---------------------------------------------------------------------------

/// Metadata describing a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMeta {
    /// Unique stream identifier.
    pub id: StreamId,
    /// MIME type of the stream payload.
    pub content_type: ContentType,
    /// Current lifecycle state.
    pub state: StreamState,
    /// Unix timestamp (microseconds) when the stream was created.
    pub created_at: i64,
    /// Unix timestamp (microseconds) when the stream was closed, if ever.
    pub closed_at: Option<i64>,
    /// Optional time-to-live for events in the stream.
    pub ttl: Option<Duration>,
    /// Unix timestamp (microseconds) after which the stream expires, if set.
    pub expires_at: Option<i64>,
    /// Arbitrary key/value tags for discovery and filtering.
    pub tags: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// StreamEnvelope
// ---------------------------------------------------------------------------

/// Versioned, source-stamped wrapper around a stream event payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEnvelope<T> {
    /// Schema/protocol version.
    pub v: u32,
    /// Unix timestamp (microseconds) when the envelope was created.
    pub ts: i64,
    /// Logical source identifier (e.g. service name or agent id).
    pub source: String,
    /// The wrapped event payload.
    #[serde(flatten)]
    pub event: T,
}

// ---------------------------------------------------------------------------
// AppendRequest / AppendResult
// ---------------------------------------------------------------------------

/// Request to append a single event to a stream.
#[derive(Debug, Clone)]
pub struct AppendRequest {
    /// Raw event bytes.
    pub data: Vec<u8>,
    /// Optional producer identifier for exactly-once semantics.
    pub producer_id: Option<ProducerId>,
    /// Producer epoch; required when `producer_id` is set.
    pub epoch: Option<ProducerEpoch>,
    /// Per-epoch sequence number; required when `producer_id` is set.
    pub seq: Option<ProducerSeq>,
}

/// Result of a successful append operation.
#[derive(Debug, Clone)]
pub struct AppendResult {
    /// Offset assigned to the newly appended event.
    pub offset: Offset,
    /// Offset to use for the next append (equals `offset` when deduplicated).
    pub next_offset: Offset,
    /// `true` if this append was a duplicate and the event was not stored again.
    pub deduplicated: bool,
}

// ---------------------------------------------------------------------------
// StreamEvent / ReadResult
// ---------------------------------------------------------------------------

/// A single event read from a stream.
#[derive(Debug, Clone)]
pub struct StreamEvent {
    /// Offset of this event within the stream.
    pub offset: Offset,
    /// Raw event bytes.
    pub data: Vec<u8>,
    /// Unix timestamp (microseconds) when the event was stored.
    pub created_at: i64,
}

/// Result of a read operation on a stream.
#[derive(Debug, Clone)]
pub struct ReadResult {
    /// The events returned by this read.
    pub events: Vec<StreamEvent>,
    /// Offset to supply on the next read to continue from here.
    pub next_offset: Offset,
    /// `true` if there are currently no newer events (caller is at the tail).
    pub up_to_date: bool,
    /// `true` if the stream has been closed (no further events will appear).
    pub stream_closed: bool,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_sentinels() {
        let beg = Offset::beginning();
        let now = Offset::now();
        assert!(beg.is_beginning());
        assert!(beg.is_sentinel());
        assert!(!beg.is_now());
        assert!(now.is_now());
        assert!(now.is_sentinel());
        assert!(!now.is_beginning());
    }

    #[test]
    fn offset_lexicographic_ordering() {
        let a = Offset("0000000001000000_0000".to_owned());
        let b = Offset("0000000001000001_0000".to_owned());
        let c = Offset("0000000001000001_0001".to_owned());
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn content_type_round_trip() {
        for ct in [
            ContentType::NdJson,
            ContentType::Json,
            ContentType::OctetStream,
            ContentType::Custom("text/plain".to_owned()),
        ] {
            assert_eq!(ContentType::from_mime(ct.as_mime()), ct);
        }
    }

    #[test]
    fn envelope_serialization() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Ping {
            msg: String,
        }

        let env = StreamEnvelope {
            v: 1,
            ts: 1_700_000_000_000_000,
            source: "test".to_owned(),
            event: Ping { msg: "hello".to_owned() },
        };

        let json = serde_json::to_string(&env).unwrap();
        let decoded: StreamEnvelope<Ping> = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.v, 1);
        assert_eq!(decoded.event.msg, "hello");
        // flatten should put "msg" at the top level
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["msg"], "hello");
        assert_eq!(val["source"], "test");
    }
}
