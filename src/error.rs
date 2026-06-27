//! Error types for timslite.

use std::fmt;
use std::io;

/// All possible errors that can occur in timslite.
#[derive(Debug)]
pub enum TmslError {
    /// Standard I/O error wrapping.
    Io(io::Error),
    /// File magic bytes do not match expected "TMSL".
    InvalidMagic,
    /// Unsupported file format version.
    InvalidVersion(u16),
    /// Memory-mapping failed.
    MmapError(String),
    /// Compression failed.
    CompressionError(String),
    /// Decompression failed.
    DecompressionError(String),
    /// Data is corrupt or malformed.
    InvalidData(String),
    /// Requested resource not found.
    NotFound(String),
    /// Requested timestamp is outside the retention window.
    Expired(String),
    /// Resource already exists (e.g., creating an existing dataset).
    AlreadyExists(String),
    /// Segment file is full (no more space for data, expansion needed or seal+new).
    SegmentFull,
    /// Queue already opened for this dataset.
    QueueAlreadyOpen(String),
    /// Queue not opened for this dataset yet.
    QueueNotOpen(String),
    /// Consumer group not found.
    ConsumerGroupNotFound(String),
    /// Consumer group already exists.
    ConsumerGroupExists(String),
    /// Queue has been closed; no further operations allowed.
    QueueClosed(String),
    /// Pending entries limit reached (max 239).
    PendingFull(String),
}

impl fmt::Display for TmslError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TmslError::Io(e) => write!(f, "I/O error: {e}"),
            TmslError::InvalidMagic => write!(f, "invalid file magic: expected b\"TMSL\""),
            TmslError::InvalidVersion(v) => write!(f, "unsupported version: {v}"),
            TmslError::MmapError(msg) => write!(f, "mmap error: {msg}"),
            TmslError::CompressionError(msg) => write!(f, "compression error: {msg}"),
            TmslError::DecompressionError(msg) => write!(f, "decompression error: {msg}"),
            TmslError::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            TmslError::NotFound(msg) => write!(f, "not found: {msg}"),
            TmslError::Expired(msg) => write!(f, "expired: {msg}"),
            TmslError::AlreadyExists(msg) => write!(f, "already exists: {msg}"),
            TmslError::SegmentFull => write!(f, "segment full (expansion needed)"),
            TmslError::QueueAlreadyOpen(msg) => write!(f, "queue already open: {msg}"),
            TmslError::QueueNotOpen(msg) => write!(f, "queue not open: {msg}"),
            TmslError::ConsumerGroupNotFound(msg) => write!(f, "consumer group not found: {msg}"),
            TmslError::ConsumerGroupExists(msg) => {
                write!(f, "consumer group already exists: {msg}")
            }
            TmslError::QueueClosed(msg) => write!(f, "queue closed: {msg}"),
            TmslError::PendingFull(msg) => write!(f, "pending entries full: {msg}"),
        }
    }
}

impl std::error::Error for TmslError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TmslError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for TmslError {
    fn from(e: io::Error) -> Self {
        TmslError::Io(e)
    }
}

/// Convenience Result alias.
pub type Result<T> = std::result::Result<T, TmslError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file missing");
        let err: TmslError = io_err.into();
        match err {
            TmslError::Io(_) => {}
            _ => panic!("expected TmslError::Io"),
        }
    }

    #[test]
    fn test_display_messages() {
        assert!(TmslError::InvalidMagic.to_string().contains("TMSL"));
        assert!(TmslError::InvalidVersion(5).to_string().contains("5"));
        assert!(TmslError::MmapError("test".to_string())
            .to_string()
            .contains("test"));
    }

    #[test]
    fn test_error_source() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "no access");
        let err: TmslError = TmslError::Io(io_err);
        assert!(err.source().is_some());

        assert!(TmslError::InvalidMagic.source().is_none());
        assert!(TmslError::InvalidVersion(1).source().is_none());
    }

    #[test]
    fn test_not_found() {
        let err = TmslError::NotFound("/data/foo".to_string());
        assert!(err.to_string().contains("/data/foo"));
    }

    #[test]
    fn test_expired() {
        let err = TmslError::Expired("timestamp 100 is expired".to_string());
        let msg = err.to_string();
        assert!(msg.contains("expired"));
        assert!(msg.contains("100"));
    }

    #[test]
    fn test_already_exists() {
        let err = TmslError::AlreadyExists("dataset sensor_001/events".to_string());
        let msg = err.to_string();
        assert!(msg.contains("already exists"));
        assert!(msg.contains("sensor_001"));
    }

    #[test]
    fn test_segment_full_display() {
        let err = TmslError::SegmentFull;
        let msg = err.to_string();
        assert!(msg.contains("segment full"));
    }

    #[test]
    fn test_queue_already_open_display() {
        let err = TmslError::QueueAlreadyOpen("sensor_001".to_string());
        let msg = err.to_string();
        assert!(msg.contains("queue already open"));
        assert!(msg.contains("sensor_001"));
    }

    #[test]
    fn test_queue_not_open_display() {
        let err = TmslError::QueueNotOpen("sensor_001".to_string());
        let msg = err.to_string();
        assert!(msg.contains("queue not open"));
        assert!(msg.contains("sensor_001"));
    }

    #[test]
    fn test_consumer_group_not_found_display() {
        let err = TmslError::ConsumerGroupNotFound("group_01".to_string());
        let msg = err.to_string();
        assert!(msg.contains("consumer group not found"));
        assert!(msg.contains("group_01"));
    }

    #[test]
    fn test_consumer_group_exists_display() {
        let err = TmslError::ConsumerGroupExists("group_01".to_string());
        let msg = err.to_string();
        assert!(msg.contains("consumer group already exists"));
        assert!(msg.contains("group_01"));
    }

    #[test]
    fn test_queue_closed_display() {
        let err = TmslError::QueueClosed("sensor_001".to_string());
        let msg = err.to_string();
        assert!(msg.contains("queue closed"));
        assert!(msg.contains("sensor_001"));
    }

    #[test]
    fn test_pending_full_display() {
        let err = TmslError::PendingFull("queue sensor_001".to_string());
        let msg = err.to_string();
        assert!(msg.contains("pending entries full"));
        assert!(msg.contains("sensor_001"));
    }

    #[test]
    fn test_all_variants_have_no_source() {
        // Only Io has a source; all others return None.
        assert!(TmslError::SegmentFull.source().is_none());
        assert!(TmslError::QueueAlreadyOpen("x".to_string()).source().is_none());
        assert!(TmslError::QueueNotOpen("x".to_string()).source().is_none());
        assert!(TmslError::ConsumerGroupNotFound("x".to_string()).source().is_none());
        assert!(TmslError::ConsumerGroupExists("x".to_string()).source().is_none());
        assert!(TmslError::QueueClosed("x".to_string()).source().is_none());
        assert!(TmslError::PendingFull("x".to_string()).source().is_none());
        assert!(TmslError::NotFound("x".to_string()).source().is_none());
        assert!(TmslError::Expired("x".to_string()).source().is_none());
        assert!(TmslError::AlreadyExists("x".to_string()).source().is_none());
        assert!(TmslError::InvalidData("x".to_string()).source().is_none());
        assert!(TmslError::CompressionError("x".to_string()).source().is_none());
        assert!(TmslError::DecompressionError("x".to_string()).source().is_none());
        assert!(TmslError::MmapError("x".to_string()).source().is_none());
        assert!(TmslError::InvalidVersion(1).source().is_none());
        assert!(TmslError::InvalidMagic.source().is_none());
    }
}
