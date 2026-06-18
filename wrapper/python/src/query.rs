//! PyQueryIterator / PyQueryLengthIterator — lazy iterators implementing Python iterator protocol.
//!
//! Pattern: Pre-fetch Vec<IndexEntry> (18B each, cheap), store Arc<DataSet>
//! for lazy data fetching. Each __next__ locks → reads one record → releases lock.

use pyo3::prelude::*;
use std::sync::Arc;

use crate::exceptions::wrap;

#[pyclass(name = "QueryIterator")]
pub struct PyQueryIterator {
    /// Pre-collected index entries (cheap: 18 bytes each)
    entries: Vec<timslite::IndexEntry>,
    /// Shared reference to dataset for data fetching
    dataset_arc: Arc<timslite::DataSet>,
    /// Current position in entries
    index: usize,
}

impl PyQueryIterator {
    pub fn new(
        dataset_arc: Arc<timslite::DataSet>,
        entries: Vec<timslite::IndexEntry>,
    ) -> Self {
        Self {
            dataset_arc,
            entries,
            index: 0,
        }
    }
}

#[pymethods]
impl PyQueryIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<(i64, Vec<u8>)>> {
        // Skip filler entries and find the next real entry
        while self.index < self.entries.len() {
            let entry = self.entries[self.index];
            self.index += 1;
            if entry.block_offset == timslite::BLOCK_OFFSET_FILLER {
                continue;
            }
            // Lock → read → release
            let (ts, data) = wrap(self.dataset_arc.read_entry_at_index(&entry))?;
            return Ok(Some((ts, data)));
        }
        Ok(None) // Triggers StopIteration in Python
    }

    /// Number of entries remaining (including fillers).
    fn __len__(&self) -> usize {
        self.entries.len().saturating_sub(self.index)
    }

    /// Release iterator resources.
    /// Normally not needed — resources are released on GC.
    fn close(&mut self) {
        self.entries.clear();
        self.index = 0;
    }
}

#[pyclass(name = "QueryLengthIterator")]
pub struct PyQueryLengthIterator {
    /// Pre-collected index entries (cheap: 18 bytes each)
    entries: Vec<timslite::IndexEntry>,
    /// Shared reference to dataset for data fetching
    dataset_arc: Arc<timslite::DataSet>,
    /// Current position in entries
    index: usize,
}

impl PyQueryLengthIterator {
    pub fn new(
        dataset_arc: Arc<timslite::DataSet>,
        entries: Vec<timslite::IndexEntry>,
    ) -> Self {
        Self {
            dataset_arc,
            entries,
            index: 0,
        }
    }
}

#[pymethods]
impl PyQueryLengthIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<(i64, u32)>> {
        // Skip filler entries and find the next real entry
        while self.index < self.entries.len() {
            let entry = self.entries[self.index];
            self.index += 1;
            if entry.block_offset == timslite::BLOCK_OFFSET_FILLER {
                continue;
            }
            // Lock → read data_len → release
            let data_len = wrap(self.dataset_arc.read_length_at_index(&entry))?;
            return Ok(Some((entry.timestamp, data_len)));
        }
        Ok(None) // Triggers StopIteration in Python
    }

    /// Number of entries remaining (including fillers).
    fn __len__(&self) -> usize {
        self.entries.len().saturating_sub(self.index)
    }

    /// Release iterator resources.
    /// Normally not needed — resources are released on GC.
    fn close(&mut self) {
        self.entries.clear();
        self.index = 0;
    }
}
