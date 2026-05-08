//! Error types for the tau-streams crate.

use crate::types::{Offset, StreamId};

/// All errors that can be produced by stream operations.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// The requested stream does not exist.
    #[error("stream not found: {0}")]
    NotFound(StreamId),

    /// A stream with this id already exists.
    #[error("stream already exists: {0}")]
    AlreadyExists(StreamId),

    /// The stream has already been closed and cannot be closed again.
    #[error("stream already closed: {0}")]
    AlreadyClosed(StreamId, Option<Offset>),

    /// The stream has been deleted and can no longer be accessed.
    #[error("stream deleted: {0}")]
    Deleted(StreamId),

    /// The requested offset has expired (e.g. TTL-based compaction).
    #[error("offset expired: {0}")]
    OffsetExpired(Offset),

    /// The producer's epoch is lower than the current epoch; the producer has
    /// been fenced out by a newer writer.
    #[error("producer fenced: epoch {actual} < current {expected}")]
    ProducerFenced {
        /// The epoch supplied by the fenced producer.
        actual: u64,
        /// The epoch already registered for this producer in the stream.
        expected: u64,
    },

    /// Stream-Seq regression: received value is <= the last accepted value.
    #[error("sequence regression: received {received} <= last {last}")]
    SequenceRegression {
        received: String,
        last: String,
    },

    /// Producer sequence gap: expected contiguous sequence but got a gap.
    #[error("producer sequence gap: expected {expected}, received {received}")]
    ProducerSequenceGap {
        expected: u64,
        received: u64,
    },

    /// An underlying `SQLite` storage error.
    #[error("storage error: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Convenience `Result` alias for stream operations.
pub type Result<T> = std::result::Result<T, StreamError>;
