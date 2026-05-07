//! Durable streams protocol implementation for tau.
//!
//! This crate provides a SQLite-backed, append-only event stream store with:
//! - Lexicographically ordered, fixed-width offsets
//! - Exactly-once producer semantics (dedup + epoch fencing)
//! - Stream lifecycle management (open → closed → deleted)
//! - Tag-based stream discovery

pub mod error;
pub mod offset;
pub mod sqlite_store;
pub mod store;
pub mod types;

pub use error::{Result, StreamError};
pub use offset::OffsetGenerator;
pub use sqlite_store::SqliteStore;
pub use store::StreamStore;
pub use types::{
    AppendRequest, AppendResult, ContentType, Offset, ProducerEpoch, ProducerId, ProducerSeq,
    ReadResult, StreamEnvelope, StreamEvent, StreamId, StreamMeta, StreamState,
};
