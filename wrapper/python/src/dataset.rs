//! PyDataset — wrapped dataset with hidden lock.
//!
//! PyDataset holds an Arc<Mutex<DataSet>> so it can outlive the Store.
//! All operations automatically acquire the lock.

use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

use crate::exceptions::wrap;
use crate::query::{PyQueryIterator, PyQueryLengthIterator};

#[pyclass(name = "Dataset")]
pub struct PyDataset {
    inner: Arc<Mutex<timslite::DataSet>>,
    id: u64,
    base_dir: String,
    read_only: bool,
}

impl PyDataset {
    /// Create a new PyDataset from an Arc<Mutex<DataSet>>.
    pub fn new(
        inner: Arc<Mutex<timslite::DataSet>>,
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
    pub fn inner_arc(&self) -> Arc<Mutex<timslite::DataSet>> {
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
    ///     timestamp: POSIX timestamp (> 0). Must be strictly increasing
    ///                in non-continuous mode.
    ///     data: Payload bytes.
    ///
    /// Raises:
    ///     TmslInvalidDataError: timestamp <= 0, out-of-order, or duplicate.
    fn write(&mut self, timestamp: i64, data: Vec<u8>) -> PyResult<()> {
        if self.read_only {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Dataset is read-only",
            ));
        }
        let mut ds = self.inner.lock().unwrap();
        wrap(ds.write(timestamp, &data))
    }

    /// Append bytes to a record.
    fn append(&mut self, timestamp: i64, data: Vec<u8>) -> PyResult<()> {
        if self.read_only {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Dataset is read-only",
            ));
        }
        let mut ds = self.inner.lock().unwrap();
        wrap(ds.append(timestamp, &data))
    }

    /// Read a single record by exact timestamp.
    ///
    /// Args:
    ///     timestamp: Timestamp of the record to read. Pass `-1` to fetch
    ///         the latest written record.
    ///
    /// Returns:
    ///     Optional[tuple[int, bytes]]: (timestamp, data) if found, or None
    ///         when the timestamp does not exist, the entry is deleted/filler,
    ///         or `-1` is passed on an empty dataset.
    fn read(&mut self, timestamp: i64) -> PyResult<Option<(i64, Vec<u8>)>> {
        let mut ds = self.inner.lock().unwrap();
        wrap(ds.read(timestamp))
    }

    /// Delete the record at the given timestamp.
    ///
    /// Marks the index entry as sentinel (block_offset = 0xFFFF..., in_block = 0xFFFF)
    /// and increments the data segment's invalid_record_count. The physical data
    /// remains on disk until retention-based reclamation or future compaction.
    ///
    /// Args:
    ///     timestamp: Timestamp of the record to delete (> 0).
    ///
    /// Raises:
    ///     TmslNotFoundError: no real data exists at that timestamp.
    ///     TmslInvalidDataError: timestamp <= 0 or dataset is empty.
    fn delete(&mut self, timestamp: i64) -> PyResult<()> {
        if self.read_only {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Dataset is read-only",
            ));
        }
        let mut ds = self.inner.lock().unwrap();
        wrap(ds.delete(timestamp))
    }

    /// Query records in [start_ts, end_ts], returns a lazy iterator.
    ///
    /// Yields (timestamp: int, data: bytes) tuples.
    /// Implements Python iterator protocol (__iter__ / __next__).
    fn query(&mut self, start_ts: i64, end_ts: i64) -> PyResult<PyQueryIterator> {
        let ds_arc = self.inner_arc();
        let entries = {
            let mut ds = ds_arc.lock().unwrap();
            wrap(ds.query_index_entries(start_ts, end_ts))?
        };
        Ok(PyQueryIterator::new(ds_arc, entries))
    }

    /// Query and collect all results into a list.
    /// Convenience wrapper: equivalent to list(dataset.query(...)).
    fn query_all(&mut self, start_ts: i64, end_ts: i64) -> PyResult<Vec<(i64, Vec<u8>)>> {
        let ds_arc = self.inner_arc();
        let entries = {
            let mut ds = ds_arc.lock().unwrap();
            wrap(ds.query_index_entries(start_ts, end_ts))?
        };
        let entries_len = entries.len();

        let mut results = Vec::with_capacity(entries_len);
        for entry in entries {
            if entry.block_offset == timslite::BLOCK_OFFSET_FILLER {
                continue;
            }
            let mut ds = ds_arc.lock().unwrap();
            let (ts, data) = wrap(ds.read_entry_at_index(&entry))?;
            results.push((ts, data));
        }
        Ok(results)
    }

    /// Flush pending data to disk.
    fn flush(&mut self) -> PyResult<()> {
        let mut ds = self.inner.lock().unwrap();
        wrap(ds.flush())
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

    /// Latest successfully written timestamp (0 if the dataset is empty).
    #[getter]
    fn latest_timestamp(&self) -> i64 {
        let ds = self.inner.lock().unwrap();
        ds.latest_written_timestamp()
    }

    /// Check if index entry exists for a timestamp.
    ///
    /// Args:
    ///     timestamp: Timestamp to check. Pass `-1` to check latest written timestamp.
    ///
    /// Returns:
    ///     bool: True if index entry exists (including filler entries), False otherwise.
    fn read_exist(&mut self, timestamp: i64) -> PyResult<bool> {
        let mut ds = self.inner.lock().unwrap();
        wrap(ds.read_exist(timestamp))
    }

    /// Check existence of index entries in [start_ts, end_ts].
    ///
    /// Args:
    ///     start_ts: Start timestamp (inclusive).
    ///     end_ts: End timestamp (inclusive).
    ///
    /// Returns:
    ///     bytes: Bitmap as bytes. Bit i represents (start_ts + i): 1=exists, 0=not found.
    fn query_exist(&mut self, start_ts: i64, end_ts: i64) -> PyResult<Vec<u8>> {
        let mut ds = self.inner.lock().unwrap();
        wrap(ds.query_exist(start_ts, end_ts))
    }

    /// Read the logical data length for a timestamp.
    ///
    /// Args:
    ///     timestamp: Timestamp to read. Pass `-1` to read latest written timestamp.
    ///
    /// Returns:
    ///     Optional[int]: Data length if record exists, None if not found, filler, or expired.
    fn read_length(&mut self, timestamp: i64) -> PyResult<Option<u32>> {
        let mut ds = self.inner.lock().unwrap();
        wrap(ds.read_length(timestamp))
    }

    /// Query data lengths in [start_ts, end_ts], returns a lazy iterator.
    ///
    /// Yields (timestamp: int, data_len: int) tuples.
    /// Implements Python iterator protocol (__iter__ / __next__).
    fn query_length(&mut self, start_ts: i64, end_ts: i64) -> PyResult<PyQueryLengthIterator> {
        let ds_arc = self.inner_arc();
        let entries = {
            let mut ds = ds_arc.lock().unwrap();
            wrap(ds.query_index_entries(start_ts, end_ts))?
        };
        Ok(PyQueryLengthIterator::new(ds_arc, entries))
    }

    /// Query data lengths and collect all results into a list.
    /// Convenience wrapper: equivalent to list(dataset.query_length(...)).
    fn query_length_all(&mut self, start_ts: i64, end_ts: i64) -> PyResult<Vec<(i64, u32)>> {
        let ds_arc = self.inner_arc();
        let entries = {
            let mut ds = ds_arc.lock().unwrap();
            wrap(ds.query_index_entries(start_ts, end_ts))?
        };
        let entries_len = entries.len();

        let mut results = Vec::with_capacity(entries_len);
        for entry in entries {
            if entry.block_offset == timslite::BLOCK_OFFSET_FILLER {
                continue;
            }
            let mut ds = ds_arc.lock().unwrap();
            let re = timslite::ReadIndexEntry {
                timestamp: entry.timestamp,
                block_offset: entry.block_offset,
                in_block_offset: entry.in_block_offset,
            };
            let cache = ds.cache_ref().cloned();
            let data_len = wrap(ds.segments_mut().read_record_data_len(&re, cache.as_deref()))?;
            results.push((entry.timestamp, data_len));
        }
        Ok(results)
    }
}
