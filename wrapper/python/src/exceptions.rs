//! Python exception types for timslite errors.
//!
//! Creates a hierarchy of Python exceptions inheriting from TmslError.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModuleMethods;
use pyo3::PyTypeInfo;

// 鈹€鈹€ Exception hierarchy 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
// TmslError (base) inherits PyException
// All specific error types inherit TmslError

create_exception!(
    timslite,
    TmslError,
    PyException,
    "Base exception for all timslite errors."
);
create_exception!(
    timslite,
    TmslIoError,
    TmslError,
    "I/O error (file not found, permission denied, etc.)."
);
create_exception!(
    timslite,
    TmslNotFoundError,
    TmslError,
    "Dataset, segment, or handle not found."
);
create_exception!(
    timslite,
    TmslAlreadyExistsError,
    TmslError,
    "Dataset already exists."
);
create_exception!(
    timslite,
    TmslInvalidDataError,
    TmslError,
    "Invalid data: bad timestamp, out-of-order, duplicate, corrupt block."
);
create_exception!(
    timslite,
    TmslSegmentFullError,
    TmslError,
    "Segment file is full (expansion needed)."
);
create_exception!(timslite, TmslMmapError, TmslError, "Memory-mapping error.");
create_exception!(
    timslite,
    TmslCompressionError,
    TmslError,
    "Compression failure."
);
create_exception!(
    timslite,
    TmslDecompressionError,
    TmslError,
    "Decompression failure."
);
create_exception!(
    timslite,
    TmslExpiredError,
    TmslError,
    "Timestamp is outside the retention window."
);
create_exception!(
    timslite,
    TmslQueueAlreadyOpenError,
    TmslError,
    "Queue is already open for this dataset."
);
create_exception!(
    timslite,
    TmslQueueNotOpenError,
    TmslError,
    "Queue is not open for this dataset."
);
create_exception!(
    timslite,
    TmslConsumerGroupNotFoundError,
    TmslError,
    "Consumer group not found."
);
create_exception!(
    timslite,
    TmslConsumerGroupExistsError,
    TmslError,
    "Consumer group already exists."
);
create_exception!(
    timslite,
    TmslQueueClosedError,
    TmslError,
    "Queue has been closed."
);
create_exception!(
    timslite,
    TmslPendingFullError,
    TmslError,
    "Pending entries limit reached (max 239)."
);

/// Register all exception types on the Python module.
///
/// Must be called from the `#[pymodule]` function body.
/// Uses `m.add()` 鈥?`#[pymodule_export]` does NOT work for `create_exception!` types.
pub fn register(m: &Bound<'_, pyo3::types::PyModule>) -> PyResult<()> {
    m.add("TmslError", TmslError::type_object(m.py()))?;
    m.add("TmslIoError", TmslIoError::type_object(m.py()))?;
    m.add("TmslNotFoundError", TmslNotFoundError::type_object(m.py()))?;
    m.add(
        "TmslAlreadyExistsError",
        TmslAlreadyExistsError::type_object(m.py()),
    )?;
    m.add(
        "TmslInvalidDataError",
        TmslInvalidDataError::type_object(m.py()),
    )?;
    m.add(
        "TmslSegmentFullError",
        TmslSegmentFullError::type_object(m.py()),
    )?;
    m.add("TmslMmapError", TmslMmapError::type_object(m.py()))?;
    m.add(
        "TmslCompressionError",
        TmslCompressionError::type_object(m.py()),
    )?;
    m.add(
        "TmslDecompressionError",
        TmslDecompressionError::type_object(m.py()),
    )?;
    m.add("TmslExpiredError", TmslExpiredError::type_object(m.py()))?;
    m.add(
        "TmslQueueAlreadyOpenError",
        TmslQueueAlreadyOpenError::type_object(m.py()),
    )?;
    m.add(
        "TmslQueueNotOpenError",
        TmslQueueNotOpenError::type_object(m.py()),
    )?;
    m.add(
        "TmslConsumerGroupNotFoundError",
        TmslConsumerGroupNotFoundError::type_object(m.py()),
    )?;
    m.add(
        "TmslConsumerGroupExistsError",
        TmslConsumerGroupExistsError::type_object(m.py()),
    )?;
    m.add(
        "TmslQueueClosedError",
        TmslQueueClosedError::type_object(m.py()),
    )?;
    m.add(
        "TmslPendingFullError",
        TmslPendingFullError::type_object(m.py()),
    )?;
    Ok(())
}

/// Map a Rust `timslite::TmslError` to the corresponding Python exception.
pub fn map_error(err: timslite::TmslError) -> pyo3::PyErr {
    use timslite::TmslError as E;
    let msg = err.to_string();
    match err {
        E::Io(_) => TmslIoError::new_err(msg),
        E::NotFound(_) => TmslNotFoundError::new_err(msg),
        E::AlreadyExists(_) => TmslAlreadyExistsError::new_err(msg),
        E::InvalidData(_) | E::InvalidMagic | E::InvalidVersion(_) => {
            TmslInvalidDataError::new_err(msg)
        }
        E::SegmentFull => TmslSegmentFullError::new_err(msg),
        E::MmapError(_) => TmslMmapError::new_err(msg),
        E::CompressionError(_) => TmslCompressionError::new_err(msg),
        E::DecompressionError(_) => TmslDecompressionError::new_err(msg),
        E::Expired(_) => TmslExpiredError::new_err(msg),
        E::QueueAlreadyOpen(_) => TmslQueueAlreadyOpenError::new_err(msg),
        E::QueueNotOpen(_) => TmslQueueNotOpenError::new_err(msg),
        E::ConsumerGroupNotFound(_) => TmslConsumerGroupNotFoundError::new_err(msg),
        E::ConsumerGroupExists(_) => TmslConsumerGroupExistsError::new_err(msg),
        E::QueueClosed(_) => TmslQueueClosedError::new_err(msg),
        E::PendingFull(_) => TmslPendingFullError::new_err(msg),
    }
}

/// Convenience: wrap a `timslite::Result<T>` into a `PyResult<T>`.
pub fn wrap<T>(result: timslite::Result<T>) -> PyResult<T> {
    result.map_err(map_error)
}
