use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Dataset not found: {0}")]
    DatasetNotFound(String),

    #[error("Dataset already exists: {0}")]
    DatasetExists(String),

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64),

    #[error("Invalid data type: {0}")]
    InvalidDataType(i32),

    #[error("Storage not initialized")]
    NotInitialized,

    #[error("Storage already closed")]
    AlreadyClosed,

    #[error("Index error: {0}")]
    IndexError(String),

    #[error("Compression error: {0}")]
    CompressionError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("File mapping error: {0}")]
    MappingError(String),

    #[error("Invalid offset: {0}")]
    InvalidOffset(i64),

    #[error("Data corrupted: {0}")]
    DataCorrupted(String),

    #[error("Write failed: {0}")]
    WriteFailed(String),

    #[error("Read failed: {0}")]
    ReadFailed(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Lock error: {0}")]
    LockError(String),

    #[error("Dataset is full")]
    DatasetFull,
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Error::SerializationError(err.to_string())
    }
}

impl From<flate2::CompressError> for Error {
    fn from(err: flate2::CompressError) -> Self {
        Error::CompressionError(err.to_string())
    }
}

impl From<flate2::DecompressError> for Error {
    fn from(err: flate2::DecompressError) -> Self {
        Error::CompressionError(err.to_string())
    }
}
