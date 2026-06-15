//! PyStore — main entry point, context manager.
//!
//! PyStore wraps Option<Store>. close() uses Option::take() to prevent use-after-close.
//! Dataset management tracks Arc<Mutex<DataSet>> for sharing with PyDataset.

use pyo3::prelude::*;
use pyo3::types::PyType;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::config::PyStoreConfig;
use crate::dataset::PyDataset;
use crate::exceptions::wrap;
use crate::queue::{PyDatasetQueue, PyJournalQueue};

/// Immutable dataset configuration info.
#[pyclass(name = "DataSetInfo")]
#[derive(Clone, Debug)]
pub struct PyDataSetInfo {
    /// Dataset name
    #[pyo3(get)]
    pub name: String,
    /// Dataset type
    #[pyo3(get)]
    pub dataset_type: String,
    /// Dataset directory path
    #[pyo3(get)]
    pub base_dir: String,
    /// Store-assigned numeric dataset identifier
    #[pyo3(get)]
    pub identifier: u64,
    /// Data segment file size limit (bytes)
    #[pyo3(get)]
    pub data_segment_size: u64,
    /// Index segment file size limit (bytes)
    #[pyo3(get)]
    pub index_segment_size: u64,
    /// Initial data segment file size (bytes)
    #[pyo3(get)]
    pub initial_data_segment_size: u64,
    /// Initial index segment file size (bytes)
    #[pyo3(get)]
    pub initial_index_segment_size: u64,
    /// Compression algorithm type (0=zstd, 1=deflate)
    #[pyo3(get)]
    pub compress_type: u8,
    /// Compression level (0-9)
    #[pyo3(get)]
    pub compress_level: u8,
    /// Index mode: 0=sparse, 1=continuous
    #[pyo3(get)]
    pub index_continuous: u8,
    /// Data retention window (same unit as timestamp, 0=no limit)
    #[pyo3(get)]
    pub retention_window: u64,
    /// Whether this dataset records journal entries when Store journal is enabled
    #[pyo3(get)]
    pub enable_journal: bool,
    /// Dataset creation time (Unix milliseconds)
    #[pyo3(get)]
    pub create_time: i64,
}

/// Mutable dataset state info.
#[pyclass(name = "DataSetState")]
#[derive(Clone, Debug)]
pub struct PyDataSetState {
    /// Highest written timestamp
    #[pyo3(get)]
    pub latest_written_timestamp: i64,
    /// Number of currently open data segments
    #[pyo3(get)]
    pub open_data_segments: u32,
    /// Total number of data segments
    #[pyo3(get)]
    pub data_segments: u32,
    /// Total record count across all data segments
    #[pyo3(get)]
    pub total_record_count: u64,
    /// Total used space across all data segments (bytes)
    #[pyo3(get)]
    pub total_data_size: u64,
    /// Total uncompressed size across all data segments (bytes)
    #[pyo3(get)]
    pub total_uncompressed_size: u64,
    /// Total invalid record count across all data segments
    #[pyo3(get)]
    pub total_invalid_record_count: u64,
    /// Global minimum timestamp from the index-visible range
    #[pyo3(get)]
    pub min_timestamp: i64,
    /// Global maximum timestamp from the index-visible range
    #[pyo3(get)]
    pub max_timestamp: i64,
    /// Number of currently open index segments
    #[pyo3(get)]
    pub open_index_segments: u32,
    /// Total number of index segments
    #[pyo3(get)]
    pub index_segments: u32,
    /// Number of in-memory buffered index entries
    #[pyo3(get)]
    pub pending_index_entries: u32,
    /// Index base timestamp (None if no data)
    #[pyo3(get)]
    pub base_timestamp: Option<i64>,
    /// Whether the dataset is in read-only mode
    #[pyo3(get)]
    pub read_only: bool,
    /// Whether BlockCache is enabled
    #[pyo3(get)]
    pub has_block_cache: bool,
    /// Whether Journal is enabled
    #[pyo3(get)]
    pub has_journal: bool,
    /// Whether the dataset has an associated Queue
    #[pyo3(get)]
    pub has_queue: bool,
    /// Number of queue consumer groups
    #[pyo3(get)]
    pub queue_consumer_groups: u32,
}

/// Result of `Store.inspect_dataset()`.
#[pyclass(name = "DataSetInspectResult")]
#[derive(Clone, Debug)]
pub struct PyDataSetInspectResult {
    /// Immutable configuration info
    #[pyo3(get)]
    pub info: PyDataSetInfo,
    /// Mutable current state
    #[pyo3(get)]
    pub state: PyDataSetState,
}

impl From<timslite::DataSetInfo> for PyDataSetInfo {
    fn from(info: timslite::DataSetInfo) -> Self {
        Self {
            name: info.name,
            dataset_type: info.dataset_type,
            base_dir: info.base_dir,
            identifier: info.identifier,
            data_segment_size: info.data_segment_size,
            index_segment_size: info.index_segment_size,
            initial_data_segment_size: info.initial_data_segment_size,
            initial_index_segment_size: info.initial_index_segment_size,
            compress_type: info.compress_type,
            compress_level: info.compress_level,
            index_continuous: info.index_continuous,
            retention_window: info.retention_window,
            enable_journal: info.enable_journal,
            create_time: info.create_time,
        }
    }
}

impl From<timslite::DataSetState> for PyDataSetState {
    fn from(state: timslite::DataSetState) -> Self {
        Self {
            latest_written_timestamp: state.latest_written_timestamp,
            open_data_segments: state.open_data_segments,
            data_segments: state.data_segments,
            total_record_count: state.total_record_count,
            total_data_size: state.total_data_size,
            total_uncompressed_size: state.total_uncompressed_size,
            total_invalid_record_count: state.total_invalid_record_count,
            min_timestamp: state.min_timestamp,
            max_timestamp: state.max_timestamp,
            open_index_segments: state.open_index_segments,
            index_segments: state.index_segments,
            pending_index_entries: state.pending_index_entries,
            base_timestamp: state.base_timestamp,
            read_only: state.read_only,
            has_block_cache: state.has_block_cache,
            has_journal: state.has_journal,
            has_queue: state.has_queue,
            queue_consumer_groups: state.queue_consumer_groups,
        }
    }
}

impl From<timslite::DataSetInspectResult> for PyDataSetInspectResult {
    fn from(result: timslite::DataSetInspectResult) -> Self {
        Self {
            info: PyDataSetInfo::from(result.info),
            state: PyDataSetState::from(result.state),
        }
    }
}

#[pyclass(name = "Store")]
pub struct PyStore {
    inner: Option<timslite::Store>,
    /// Track open datasets: dataset_id -> Arc<Mutex<DataSet>>
    /// This runs parallel to Store's internal registry.
    datasets: std::collections::HashMap<u64, Arc<Mutex<timslite::DataSet>>>,
    next_id: u64,
}

#[pymethods]
impl PyStore {
    #[new]
    fn new() -> Self {
        Self {
            inner: None,
            datasets: std::collections::HashMap::new(),
            next_id: 1,
        }
    }

    /// Open or create a store at `data_dir`.
    ///
    /// Directories are created automatically if they don't exist.
    #[classmethod]
    #[pyo3(signature = (data_dir, config=None))]
    fn open(
        _cls: &Bound<'_, PyType>,
        data_dir: &str,
        config: Option<Py<PyStoreConfig>>,
        py: Python<'_>,
    ) -> PyResult<Self> {
        let store_config = match config {
            Some(cfg) => cfg.borrow(py).inner().clone(),
            None => timslite::StoreConfig::default(),
        };
        let store = wrap(timslite::Store::open(PathBuf::from(data_dir), store_config))?;
        Ok(Self {
            inner: Some(store),
            datasets: std::collections::HashMap::new(),
            next_id: 1,
        })
    }

    /// Context manager entry.
    fn __enter__(slf: Py<Self>, _py: Python<'_>) -> PyResult<Py<Self>> {
        Ok(slf)
    }

    /// Context manager exit — calls close().
    fn __exit__(
        &mut self,
        _py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.close()
    }

    /// Close the store completely.
    ///
    /// Closes all tracked datasets (release mmap handles) then drops the store.
    fn close(&mut self) -> PyResult<()> {
        // First close all tracked datasets to release mmap handles
        for ds_arc in self.datasets.values() {
            if let Ok(mut ds) = ds_arc.lock() {
                let _ = ds.flush();
                let _ = ds.close();
            }
        }
        self.datasets.clear();

        let store = self
            .inner
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is already closed"))?;

        drop(store);
        Ok(())
    }

    /// Create a new dataset.
    ///
    /// Only `name` and `dataset_type` are required. All other parameters
    /// inherit from StoreConfig defaults unless overridden.
    #[pyo3(signature = (name, dataset_type, *, data_segment_size=None, index_segment_size=None, compress_level=None, index_continuous=false, initial_data_segment_size=None, initial_index_segment_size=None, enable_journal=true))]
    #[allow(clippy::too_many_arguments)]
    fn create_dataset(
        &mut self,
        name: &str,
        dataset_type: &str,
        data_segment_size: Option<u64>,
        index_segment_size: Option<u64>,
        compress_level: Option<u8>,
        index_continuous: bool,
        initial_data_segment_size: Option<u64>,
        initial_index_segment_size: Option<u64>,
        enable_journal: bool,
    ) -> PyResult<()> {
        let store = self
            .inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;

        let store_config = store.config().clone();
        let mut builder = timslite::DataSetConfigBuilder::from_store(&store_config);

        if let Some(v) = data_segment_size {
            builder = builder.data_segment_size(v);
        }
        if let Some(v) = index_segment_size {
            builder = builder.index_segment_size(v);
        }
        if let Some(v) = compress_level {
            builder = builder.compress_level(v);
        }
        builder = builder.index_continuous(if index_continuous { 1 } else { 0 });
        if let Some(v) = initial_data_segment_size {
            builder = builder.initial_data_segment_size(v);
        }
        if let Some(v) = initial_index_segment_size {
            builder = builder.initial_index_segment_size(v);
        }
        builder = builder.enable_journal(enable_journal);

        wrap(store.create_dataset_with_config(name, dataset_type, Some(builder)))?;
        Ok(())
    }

    /// Open an existing dataset.
    ///
    /// Returns a Dataset object for read/write operations.
    fn open_dataset(&mut self, name: &str, dataset_type: &str) -> PyResult<PyDataset> {
        let store = self
            .inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;

        let handle = wrap(store.open_dataset(name, dataset_type))?;
        let ds_arc = wrap(store.get_dataset(&handle))?;

        let id = self.next_id;
        self.next_id += 1;
        let base_dir = {
            let ds = ds_arc.lock().unwrap();
            ds.base_dir().to_string_lossy().to_string()
        };

        let py_ds = PyDataset::new(ds_arc, id, base_dir, false);
        self.datasets.insert(id, py_ds.inner_arc());
        Ok(py_ds)
    }

    /// Open an existing dataset by its Store-assigned numeric identifier.
    fn open_dataset_by_identifier(&mut self, identifier: u64) -> PyResult<PyDataset> {
        let store = self
            .inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;

        let handle = wrap(store.open_dataset_by_identifier(identifier))?;
        let ds_arc = wrap(store.get_dataset(&handle))?;

        let id = self.next_id;
        self.next_id += 1;
        let base_dir = {
            let ds = ds_arc.lock().unwrap();
            ds.base_dir().to_string_lossy().to_string()
        };

        let py_ds = PyDataset::new(ds_arc, id, base_dir, false);
        self.datasets.insert(id, py_ds.inner_arc());
        Ok(py_ds)
    }

    /// Open the queue subsystem for a dataset.
    ///
    /// Args:
    ///     dataset_id: The ID of the dataset (returned by `open_dataset().id`).
    ///
    /// Returns a DatasetQueue that supports push and consumer group operations.
    ///
    /// Raises:
    ///     TmslQueueAlreadyOpenError: Queue is already open for this dataset.
    fn open_queue(&mut self, dataset_id: u64) -> PyResult<PyDatasetQueue> {
        let ds_arc = self
            .datasets
            .get(&dataset_id)
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "dataset_id {dataset_id} not found"
                ))
            })?
            .clone();

        let (inner, notify) = {
            let mut ds = ds_arc.lock().unwrap();
            wrap(ds.open_queue())?
        };

        Ok(PyDatasetQueue::new(timslite::DatasetQueue::new(
            ds_arc, inner, notify,
        )))
    }

    /// Return the latest journal sequence, or None when journal is empty.
    fn journal_latest_sequence(&self) -> PyResult<Option<i64>> {
        let store = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        wrap(store.journal_latest_sequence())
    }

    /// Read one encoded journal record by sequence.
    fn journal_read(&self, sequence: i64) -> PyResult<Option<(i64, Vec<u8>)>> {
        let store = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        wrap(store.journal_read(sequence))
    }

    /// Query encoded journal records by inclusive sequence range.
    fn journal_query(&self, start_sequence: i64, end_sequence: i64) -> PyResult<Vec<(i64, Vec<u8>)>> {
        let store = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        wrap(store.journal_query(start_sequence, end_sequence))
    }

    /// Open the built-in journal queue.
    fn open_journal_queue(&mut self) -> PyResult<PyJournalQueue> {
        let store = self
            .inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        Ok(PyJournalQueue::new(wrap(store.open_journal_queue())?))
    }

    /// Delete an entire dataset.
    ///
    /// WARNING: Irreversible. All data is permanently deleted.
    fn drop_dataset(&mut self, name: &str, dataset_type: &str) -> PyResult<()> {
        let store = self
            .inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        wrap(store.drop_dataset_by_name(name, dataset_type))
    }

    /// Execute one tick of background tasks synchronously.
    ///
    /// Checks if flush, idle-close, cache eviction, or retention reclaim are
    /// due and runs them immediately.  Returns `(executed, next_delay_ms)`.
    ///
    /// Can be called regardless of whether the background thread is enabled.
    /// When `enable_background_thread=False`, call this periodically to drive
    /// background logic (e.g. in an event loop).
    fn tick_background_tasks(&self) -> PyResult<(usize, u64)> {
        let store = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        let result = wrap(store.tick_background_tasks())?;
        Ok((result.executed_tasks, result.next_delay.as_millis() as u64))
    }

    /// Return the delay in milliseconds until the next background task is due.
    ///
    /// Does NOT execute any tasks — reads a snapshot of the executor state.
    /// Useful for scheduling the next `tick_background_tasks()` call.
    fn next_background_delay(&self) -> PyResult<u64> {
        let store = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        let delay = wrap(store.next_background_delay())?;
        Ok(delay.as_millis() as u64)
    }

    /// Get all unique dataset names in the store.
    fn get_dataset_names(&self) -> PyResult<Vec<String>> {
        let store = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        wrap(store.get_dataset_names())
    }

    /// Get all dataset types for a given name.
    fn get_dataset_types(&self, name: &str) -> PyResult<Vec<String>> {
        let store = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        wrap(store.get_dataset_types(name))
    }

    /// Get detailed info and state of a dataset.
    ///
    /// Returns a DataSetInspectResult containing immutable config (DataSetInfo)
    /// and mutable state (DataSetState).
    fn inspect_dataset(&self, name: &str, dataset_type: &str) -> PyResult<PyDataSetInspectResult> {
        let store = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
        let result = wrap(store.inspect_dataset(name, dataset_type))?;
        Ok(PyDataSetInspectResult::from(result))
    }
}

impl Drop for PyStore {
    fn drop(&mut self) {
        // best-effort: flush tracked datasets then drop store.
        // Note: background thread is detached by Store::Drop.
        // On Windows, files may not be immediately deletable if the
        // background thread still holds references. Tests should allow
        // for delayed cleanup (use ignore_errors=True for tempfile).
        if let Ok(store) = self.inner.take().ok_or(()) {
            for ds_arc in self.datasets.values() {
                if let Ok(mut ds) = ds_arc.lock() {
                    let _ = ds.flush();
                }
            }
            let _ = self.datasets.drain();
            drop(store);
        }
    }
}
