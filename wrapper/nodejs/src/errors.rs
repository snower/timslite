use napi::bindgen_prelude::Status;
use napi::Error;

pub fn from_tmsl(err: timslite::TmslError) -> Error {
    let (code, msg) = match &err {
        timslite::TmslError::Io(_) => ("TMSL_IO", err.to_string()),
        timslite::TmslError::InvalidMagic => ("TMSL_INVALID_DATA", err.to_string()),
        timslite::TmslError::InvalidVersion(_) => ("TMSL_INVALID_DATA", err.to_string()),
        timslite::TmslError::MmapError(_) => ("TMSL_MMAP", err.to_string()),
        timslite::TmslError::CompressionError(_) => ("TMSL_COMPRESSION", err.to_string()),
        timslite::TmslError::DecompressionError(_) => ("TMSL_DECOMPRESSION", err.to_string()),
        timslite::TmslError::InvalidData(_) => ("TMSL_INVALID_DATA", err.to_string()),
        timslite::TmslError::NotFound(_) => ("TMSL_NOT_FOUND", err.to_string()),
        timslite::TmslError::Expired(_) => ("TMSL_EXPIRED", err.to_string()),
        timslite::TmslError::AlreadyExists(_) => ("TMSL_ALREADY_EXISTS", err.to_string()),
        timslite::TmslError::SegmentFull => ("TMSL_SEGMENT_FULL", err.to_string()),
        timslite::TmslError::QueueAlreadyOpen(_) => ("TMSL_QUEUE_ALREADY_OPEN", err.to_string()),
        timslite::TmslError::QueueNotOpen(_) => ("TMSL_QUEUE_NOT_OPEN", err.to_string()),
        timslite::TmslError::ConsumerGroupNotFound(_) => ("TMSL_CONSUMER_GROUP_NOT_FOUND", err.to_string()),
        timslite::TmslError::ConsumerGroupExists(_) => ("TMSL_CONSUMER_GROUP_EXISTS", err.to_string()),
        timslite::TmslError::QueueClosed(_) => ("TMSL_QUEUE_CLOSED", err.to_string()),
        timslite::TmslError::PendingFull(_) => ("TMSL_PENDING_FULL", err.to_string()),
    };
    Error::new(Status::GenericFailure, format!("[{code}] {msg}"))
}

pub fn store_closed() -> Error {
    Error::new(Status::GenericFailure, "[TMSL_STORE_CLOSED] Store is closed")
}

pub fn invalid_data(msg: &str) -> Error {
    Error::new(Status::InvalidArg, format!("[TMSL_INVALID_DATA] {msg}"))
}

pub fn wrap<T>(result: timslite::error::Result<T>) -> napi::Result<T> {
    result.map_err(from_tmsl)
}
