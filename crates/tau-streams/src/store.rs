//! Abstract store trait for durable streams.

use std::collections::HashMap;

use crate::{
    error::Result,
    types::{AppendRequest, AppendResult, Offset, ReadResult, StreamId, StreamMeta},
};

/// Backend storage for durable streams.
///
/// Implementors provide the persistence layer; callers interact with streams
/// exclusively through this interface.
pub trait StreamStore {
    /// Creates a new stream with the given id and optional metadata tags.
    ///
    /// Idempotent: if a stream with the same id already exists, returns its
    /// current metadata without error.
    fn create(
        &self,
        id: &StreamId,
        tags: Option<HashMap<String, String>>,
    ) -> Result<StreamMeta>;

    /// Appends an event to an open stream.
    ///
    /// # Errors
    ///
    /// - [`crate::error::StreamError::NotFound`] — stream does not exist.
    /// - [`crate::error::StreamError::AlreadyClosed`] — stream is closed.
    /// - [`crate::error::StreamError::Deleted`] — stream has been deleted.
    /// - [`crate::error::StreamError::ProducerFenced`] — producer epoch is stale.
    fn append(&self, id: &StreamId, req: AppendRequest) -> Result<AppendResult>;

    /// Reads events from the stream starting after `offset`.
    ///
    /// Pass [`Offset::beginning`] to read from the start, [`Offset::now`] to
    /// subscribe without receiving historical events.
    ///
    /// # Errors
    ///
    /// - [`crate::error::StreamError::NotFound`] — stream does not exist.
    /// - [`crate::error::StreamError::Deleted`] — stream has been deleted.
    fn read(&self, id: &StreamId, offset: &Offset, limit: usize) -> Result<ReadResult>;

    /// Returns the current metadata for a stream.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::StreamError::NotFound`] if the stream does not exist.
    fn head(&self, id: &StreamId) -> Result<StreamMeta>;

    /// Closes a stream, preventing further appends.
    ///
    /// # Errors
    ///
    /// - [`crate::error::StreamError::NotFound`] — stream does not exist.
    /// - [`crate::error::StreamError::AlreadyClosed`] — stream is already closed.
    fn close(&self, id: &StreamId) -> Result<StreamMeta>;

    /// Deletes a stream and removes all its events.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::StreamError::NotFound`] if the stream does not exist.
    fn delete(&self, id: &StreamId) -> Result<()>;

    /// Lists streams, optionally filtering by a tag key/value pair.
    ///
    /// When `tag_filter` is `Some((key, value))` only streams whose `tags` map
    /// contains that exact key/value entry are returned. Deleted streams are
    /// excluded.
    ///
    /// # Errors
    ///
    /// Returns a storage error if the underlying query fails.
    fn list(&self, tag_filter: Option<(&str, &str)>) -> Result<Vec<StreamMeta>>;
}
