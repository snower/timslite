//! Python exception types for timslite errors.
//!
//! Creates a hierarchy of Python exceptions inheriting from TmslError.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModuleMethods;
use pyo3::PyTypeInfo;

// ── Exception hierarchy ─────────────────────────────────────────────────────
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

/// Register all exception types on the Python module.
///
/// Must be called from the `#[pymodule]` function body.
/// Uses `m.add()` — `#[pymodule_export]` does NOT work for `create_exception!` types.
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
    }
}

/// Convenience: wrap a `timslite::Result<T>` into a `PyResult<T>`.
pub fn wrap<T>(result: timslite::Result<T>) -> PyResult<T> {
    result.map_err(map_error)
}
