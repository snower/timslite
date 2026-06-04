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
use crate::queue::PyDatasetQueue;

#[pyclass(name = "Store")]
pub struct PyStore {
    inner: Option<timslite::Store>,
    /// Track open datasets: dataset_id -> Arc<Mutex<DataSet>>
    /// This runs parallel to Store's internal registry.
    datasets: std::collections::HashMap<u64, Arc<Mutex<timslite::DataSet>>>,
    read_only_dataset_ids: std::collections::HashSet<u64>,
    next_id: u64,
}

#[pymethods]
impl PyStore {
    #[new]
    fn new() -> Self {
        Self {
            inner: None,
            datasets: std::collections::HashMap::new(),
            read_only_dataset_ids: std::collections::HashSet::new(),
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
            read_only_dataset_ids: std::collections::HashSet::new(),
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
        self.read_only_dataset_ids.clear();

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
    #[pyo3(signature = (name, dataset_type, *, data_segment_size=None, index_segment_size=None, compress_level=None, index_continuous=false, initial_data_segment_size=None, initial_index_segment_size=None))]
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
        let read_only =
            name == timslite::JOURNAL_DATASET_NAME && dataset_type == timslite::JOURNAL_DATASET_TYPE;

        let id = self.next_id;
        self.next_id += 1;
        let base_dir = {
            let ds = ds_arc.lock().unwrap();
            ds.base_dir().to_string_lossy().to_string()
        };

        let py_ds = PyDataset::new(ds_arc, id, base_dir, read_only);
        self.datasets.insert(id, py_ds.inner_arc());
        if read_only {
            self.read_only_dataset_ids.insert(id);
        }
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
        if self.read_only_dataset_ids.contains(&dataset_id) {
            let store = self
                .inner
                .as_mut()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Store is closed"))?;
            return Ok(PyDatasetQueue::new(wrap(store.open_journal_queue())?));
        }

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
