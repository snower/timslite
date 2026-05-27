//! PyDataset — wrapped dataset with hidden lock.
//!
//! PyDataset holds an Arc<Mutex<DataSet>> so it can outlive the Store.
//! All operations automatically acquire the lock.

use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

use crate::exceptions::wrap;
use crate::query::PyQueryIterator;

#[pyclass(name = "Dataset")]
pub struct PyDataset {
    inner: Arc<Mutex<timslite::DataSet>>,
    id: u64,
    base_dir: String,
}

impl PyDataset {
    /// Create a new PyDataset from an Arc<Mutex<DataSet>>.
    pub fn new(inner: Arc<Mutex<timslite::DataSet>>, id: u64, base_dir: String) -> Self {
        Self {
            inner,
            id,
            base_dir,
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
        let mut ds = self.inner.lock().unwrap();
        wrap(ds.write(timestamp, &data))
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
            let (ts, data) = wrap(ds.read_entry_at_index(&entry, None))?;
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
}
