use std::fmt;
use timslite::TmslError as RustTmslError;

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
            TmslError::QueueBridgeClosed { message } => write!(f, "queue bridge closed: {message}"),
            TmslError::IteratorExhausted { message } => {
                write!(f, "iterator exhausted: {message}")
            }
        }
    }
}

impl From<RustTmslError> for TmslError {
    fn from(err: RustTmslError) -> Self {
        match err {
            RustTmslError::Io(e) => TmslError::Io { message: e.to_string() },
            RustTmslError::InvalidMagic => TmslError::InvalidMagic,
            RustTmslError::InvalidVersion(v) => TmslError::InvalidVersion { message: format!("unsupported version: {v}") },
            RustTmslError::MmapError(msg) => TmslError::MmapError { message: msg },
            RustTmslError::CompressionError(msg) => TmslError::CompressionError { message: msg },
            RustTmslError::DecompressionError(msg) => TmslError::DecompressionError { message: msg },
            RustTmslError::InvalidData(msg) => TmslError::InvalidData { message: msg },
            RustTmslError::NotFound(msg) => TmslError::NotFound { message: msg },
            RustTmslError::Expired(msg) => TmslError::Expired { message: msg },
            RustTmslError::AlreadyExists(msg) => TmslError::AlreadyExists { message: msg },
            RustTmslError::SegmentFull => TmslError::SegmentFull,
            RustTmslError::QueueAlreadyOpen(msg) => TmslError::QueueAlreadyOpen { message: msg },
            RustTmslError::QueueNotOpen(msg) => TmslError::QueueNotOpen { message: msg },
            RustTmslError::ConsumerGroupNotFound(msg) => TmslError::ConsumerGroupNotFound { message: msg },
            RustTmslError::ConsumerGroupExists(msg) => TmslError::ConsumerGroupExists { message: msg },
            RustTmslError::QueueClosed(msg) => TmslError::QueueClosed { message: msg },
            RustTmslError::PendingFull(msg) => TmslError::PendingFull { message: msg },
        }
    }
}
