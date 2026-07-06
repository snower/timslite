//! PyDataset 鈥?wrapped dataset with hidden lock.
//!
//! PyDataset holds an Arc<DataSet> so it can outlive the Store.
//! All operations automatically acquire the lock.

use pyo3::prelude::*;
use std::sync::Arc;

use crate::exceptions::wrap;
use crate::query::{PyQueryIterator, PyQueryLengthIterator};
use crate::store::PyDataSetInspectResult;

#[pyclass(name = "Dataset")]
pub struct PyDataset {
    inner: Arc<timslite::DataSet>,
    id: u64,
    base_dir: String,
    read_only: bool,
}

impl PyDataset {
    /// Create a new PyDataset from an Arc<DataSet>.
    pub fn new(
        inner: Arc<timslite::DataSet>,
        id: u64,
        base_dir: String,
        read_only: bool,
    ) -> Self {
        Self {
            inner,
            id,
            base_dir,
            read_only,
        }
    }

    /// Clone the Arc reference for sharing.
    pub fn inner_arc(&self) -> Arc<timslite::DataSet> {
        Arc::clone(&self.inner)
    }
}

#[pymethods]
impl PyDataset {
    fn __repr__(&self) -> String {
        format!("Dataset(id={}, dir={:?})", self.id, self.base_dir)
    }

    /// Write a record (timestamp, data).
    ///
    /// Args:
    ///     timestamp: Signed i64 business timestamp. Negative values and 0 are valid.
    ///     data: Payload bytes.
    ///
    /// Raises:
    ///     TmslInvalidDataError: out-of-order missing timestamp or oversized record.
    fn write(&mut self, timestamp: i64, data: Vec<u8>) -> PyResult<()> {
        if self.read_only {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Dataset is read-only",
            ));
        }
        wrap(self.inner.write(timestamp, &data))
    }

    /// Append bytes to a record.
    fn append(&mut self, timestamp: i64, data: Vec<u8>) -> PyResult<()> {
        if self.read_only {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Dataset is read-only",
            ));
        }
        wrap(self.inner.append(timestamp, &data))
    }

    /// Write a record using the current Unix timestamp (seconds).
    ///
    /// Args:
    ///     data: Payload bytes.
    ///
    /// Raises:
    ///     TmslInvalidDataError: oversized record.
    fn write_now(&mut self, data: Vec<u8>) -> PyResult<()> {
        if self.read_only {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Dataset is read-only",
            ));
        }
        wrap(self.inner.write_now(&data))
    }

    /// Append data to a record using the current Unix timestamp (seconds).
    ///
    /// Args:
    ///     data: Payload bytes to append.
    ///
    /// Raises:
    ///     TmslInvalidDataError: if append would exceed max record size.
    fn append_now(&mut self, data: Vec<u8>) -> PyResult<()> {
        if self.read_only {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Dataset is read-only",
            ));
        }
        wrap(self.inner.append_now(&data))
    }

    /// Read a single record by timestamp.
    ///
    /// Args:
    ///     timestamp: Exact signed business timestamp.
    ///
    /// Returns:
    ///     Optional[tuple[int, bytes]]: (timestamp, data) if found, or None
    ///         when the timestamp does not exist, or the entry is deleted/filler.
    fn read(&mut self, timestamp: i64) -> PyResult<Option<(i64, Vec<u8>)>> {
        wrap(self.inner.read(timestamp))
    }

    /// Read the latest written timestamp's record without falling back.
    fn read_latest(&mut self) -> PyResult<Option<(i64, Vec<u8>)>> {
        wrap(self.inner.read_latest())
    }

    /// Inspect dataset configuration and runtime state.
    ///
    /// Returns:
    ///     DataSetInspectResult: Contains `info` (DataSetInfo with config)
    ///         and `state` (DataSetState with runtime stats).
    fn inspect(&self) -> PyResult<PyDataSetInspectResult> {
        let result = wrap(self.inner.inspect())?;
        Ok(PyDataSetInspectResult::from(result))
    }

    /// Delete the record at the given timestamp.
    ///
    /// Marks the index entry as sentinel (block_offset = 0xFFFF..., in_block = 0xFFFF)
    /// and increments the data segment's invalid_record_count. The physical data
    /// remains on disk until retention-based reclamation or future compaction.
    ///
    /// Args:
    ///     timestamp: Exact signed business timestamp of the record to delete.
    ///
    /// Raises:
    ///     TmslNotFoundError: no real data exists at that timestamp.
    ///     TmslInvalidDataError: dataset is empty or timestamp is expired.
    fn delete(&mut self, timestamp: i64) -> PyResult<()> {
        if self.read_only {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Dataset is read-only",
            ));
        }
        wrap(self.inner.delete(timestamp))
    }

    /// Query records in [start_ts, end_ts], returns a lazy iterator.
    ///
    /// Yields (timestamp: int, data: bytes) tuples.
    /// Implements Python iterator protocol (__iter__ / __next__).
    fn query(&mut self, start_ts: i64, end_ts: i64) -> PyResult<PyQueryIterator> {
        let iter = wrap(self.inner.query_iter(start_ts, end_ts))?;
        Ok(PyQueryIterator::new(iter))
    }

    /// Query and collect all results into a list.
    /// Convenience wrapper: equivalent to list(dataset.query(...)).
    fn query_all(&mut self, start_ts: i64, end_ts: i64) -> PyResult<Vec<(i64, Vec<u8>)>> {
        wrap(self.inner.query(start_ts, end_ts))
    }

    /// Flush pending data to disk.
    fn flush(&mut self) -> PyResult<()> {
        wrap(self.inner.flush())
    }

    /// Base directory of this dataset.
    #[getter]
    fn data_dir(&self) -> &str {
        &self.base_dir
    }

    /// Internal dataset ID (used for queue operations).
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    /// Store-assigned numeric dataset identifier.
    #[getter]
    fn identifier(&self) -> u64 {
        self.inner.identifier()
    }

    /// Latest successfully written timestamp (None if the dataset is empty).
    #[getter]
    fn latest_timestamp(&self) -> Option<i64> {
        self.inner.latest_written_timestamp()
    }

    /// Check if index entry exists for a timestamp.
    ///
    /// Args:
    ///     timestamp: Exact signed business timestamp.
    ///
    /// Returns:
    ///     bool: True if index entry exists (including filler entries), False otherwise.
    fn read_exist(&mut self, timestamp: i64) -> PyResult<bool> {
        wrap(self.inner.read_exist(timestamp))
    }

    /// Check existence of index entries in [start_ts, end_ts].
    ///
    /// Args:
    ///     start_ts: Start timestamp (inclusive).
    ///     end_ts: End timestamp (inclusive).
    ///
    /// Returns:
    ///     bytes: Bitmap as bytes; bit i corresponds to start_ts + i.
    fn query_exist(&mut self, start_ts: i64, end_ts: i64) -> PyResult<Vec<u8>> {
        wrap(self.inner.query_exist(start_ts, end_ts))
    }

    /// Read the logical data length for a timestamp.
    ///
    /// Args:
    ///     timestamp: Exact signed business timestamp.
    ///
    /// Returns:
    ///     Optional[int]: Data length if record exists, None if not found, filler, or expired.
    fn read_length(&mut self, timestamp: i64) -> PyResult<Option<u32>> {
        wrap(self.inner.read_length(timestamp))
    }

    /// Query data lengths in [start_ts, end_ts], returns a lazy iterator.
    ///
    /// Yields (timestamp: int, data_len: int) tuples.
    /// Implements Python iterator protocol (__iter__ / __next__).
    fn query_length(&mut self, start_ts: i64, end_ts: i64) -> PyResult<PyQueryLengthIterator> {
        let iter = wrap(self.inner.query_length_iter(start_ts, end_ts))?;
        Ok(PyQueryLengthIterator::new(iter))
    }

    /// Query data lengths and collect all results into a list.
    /// Convenience wrapper: equivalent to list(dataset.query_length(...)).
    fn query_length_all(&mut self, start_ts: i64, end_ts: i64) -> PyResult<Vec<(i64, u32)>> {
        wrap(self.inner.query_length(start_ts, end_ts))
    }
}
