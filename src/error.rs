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
    /// Resource already exists (e.g., creating an existing dataset).
    AlreadyExists(String),
    /// Segment file is full (no more space for data, expansion needed or seal+new).
    SegmentFull,
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
            TmslError::AlreadyExists(msg) => write!(f, "already exists: {msg}"),
            TmslError::SegmentFull => write!(f, "segment full (expansion needed)"),
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
    fn test_already_exists() {
        let err = TmslError::AlreadyExists("dataset sensor_001/events".to_string());
        let msg = err.to_string();
        assert!(msg.contains("already exists"));
        assert!(msg.contains("sensor_001"));
    }
}
