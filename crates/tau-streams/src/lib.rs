//! Durable streams protocol implementation for tau.
//!
//! This crate provides a SQLite-backed, append-only event stream store with:
//! - Lexicographically ordered, fixed-width offsets
//! - Exactly-once producer semantics (dedup + epoch fencing)
//! - Stream lifecycle management (open → closed → deleted)
//! - Tag-based stream discovery
//! - Real-time live delivery via [`LiveDeliveryHub`]
//! - Catch-up + live consumer via [`consumer::StreamConsumer`]

pub mod consumer;
pub mod cursor;
pub mod durable_stream;
pub mod error;
pub mod hub;
pub mod offset;
pub mod sqlite_store;
pub mod store;
pub mod types;

pub use cursor::generate_cursor;
pub use durable_stream::DurableStream;
pub use error::{Result, StreamError};
pub use hub::{LiveDeliveryHub, LiveEvent};
pub use offset::OffsetGenerator;
pub use sqlite_store::SqliteStore;
pub use store::StreamStore;
pub use types::{
    AppendRequest, AppendResult, ContentType, CreateOptions, Offset, ProducerEpoch, ProducerId,
    ProducerInfo, ProducerSeq, ReadResult, StreamEnvelope, StreamEvent, StreamId, StreamMeta,
    StreamState, StreamSeq,
};
