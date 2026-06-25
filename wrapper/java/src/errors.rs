use std::fmt;

#[derive(Debug)]
pub enum TmslError {
    Io { message: String },
    InvalidMagic,
    InvalidVersion { message: String },
    MmapError { message: String },
    CompressionError { message: String },
    DecompressionError { message: String },
    InvalidData { message: String },
    NotFound { message: String },
    Expired { message: String },
    AlreadyExists { message: String },
    SegmentFull,
    QueueAlreadyOpen { message: String },
    QueueNotOpen { message: String },
    ConsumerGroupNotFound { message: String },
    ConsumerGroupExists { message: String },
    QueueClosed { message: String },
    PendingFull { message: String },
    StoreClosed { message: String },
    DatasetClosed { message: String },
    QueueBridgeClosed { message: String },
    IteratorExhausted { message: String },
}

impl fmt::Display for TmslError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TmslError::Io { message } => write!(f, "I/O error: {message}"),
            TmslError::InvalidMagic => write!(f, "invalid file magic: expected b\"TMSL\""),
            TmslError::InvalidVersion { message } => write!(f, "unsupported version: {message}"),
            TmslError::MmapError { message } => write!(f, "mmap error: {message}"),
            TmslError::CompressionError { message } => write!(f, "compression error: {message}"),
            TmslError::DecompressionError { message } => {
                write!(f, "decompression error: {message}")
            }
            TmslError::InvalidData { message } => write!(f, "invalid data: {message}"),
            TmslError::NotFound { message } => write!(f, "not found: {message}"),
            TmslError::Expired { message } => write!(f, "expired: {message}"),
            TmslError::AlreadyExists { message } => write!(f, "already exists: {message}"),
            TmslError::SegmentFull => write!(f, "segment full (expansion needed)"),
            TmslError::QueueAlreadyOpen { message } => write!(f, "queue already open: {message}"),
            TmslError::QueueNotOpen { message } => write!(f, "queue not open: {message}"),
            TmslError::ConsumerGroupNotFound { message } => {
                write!(f, "consumer group not found: {message}")
            }
            TmslError::ConsumerGroupExists { message } => {
                write!(f, "consumer group already exists: {message}")
            }
            TmslError::QueueClosed { message } => write!(f, "queue closed: {message}"),
            TmslError::PendingFull { message } => write!(f, "pending entries full: {message}"),
            TmslError::StoreClosed { message } => write!(f, "store closed: {message}"),
            TmslError::DatasetClosed { message } => write!(f, "dataset closed: {message}"),
            TmslError::QueueBridgeClosed { message } => {
                write!(f, "queue bridge closed: {message}")
            }
            TmslError::IteratorExhausted { message } => {
                write!(f, "iterator exhausted: {message}")
            }
        }
    }
}

impl std::error::Error for TmslError {}

impl From<timslite::TmslError> for TmslError {
    fn from(err: timslite::TmslError) -> Self {
        match err {
            timslite::TmslError::Io(e) => TmslError::Io {
                message: e.to_string(),
            },
            timslite::TmslError::InvalidMagic => TmslError::InvalidMagic,
            timslite::TmslError::InvalidVersion(v) => TmslError::InvalidVersion {
                message: v.to_string(),
            },
            timslite::TmslError::MmapError(msg) => TmslError::MmapError { message: msg },
            timslite::TmslError::CompressionError(msg) => {
                TmslError::CompressionError { message: msg }
            }
            timslite::TmslError::DecompressionError(msg) => {
                TmslError::DecompressionError { message: msg }
            }
            timslite::TmslError::InvalidData(msg) => TmslError::InvalidData { message: msg },
            timslite::TmslError::NotFound(msg) => TmslError::NotFound { message: msg },
            timslite::TmslError::Expired(msg) => TmslError::Expired { message: msg },
            timslite::TmslError::AlreadyExists(msg) => TmslError::AlreadyExists { message: msg },
            timslite::TmslError::SegmentFull => TmslError::SegmentFull,
            timslite::TmslError::QueueAlreadyOpen(msg) => {
                TmslError::QueueAlreadyOpen { message: msg }
            }
            timslite::TmslError::QueueNotOpen(msg) => TmslError::QueueNotOpen { message: msg },
            timslite::TmslError::ConsumerGroupNotFound(msg) => {
                TmslError::ConsumerGroupNotFound { message: msg }
            }
            timslite::TmslError::ConsumerGroupExists(msg) => {
                TmslError::ConsumerGroupExists { message: msg }
            }
            timslite::TmslError::QueueClosed(msg) => TmslError::QueueClosed { message: msg },
            timslite::TmslError::PendingFull(msg) => TmslError::PendingFull { message: msg },
        }
    }
}

impl From<std::io::Error> for TmslError {
    fn from(err: std::io::Error) -> Self {
        TmslError::Io {
            message: err.to_string(),
        }
    }
}
