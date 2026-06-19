//! PyStoreConfig — kwargs-based configuration (no builder pattern in Python).

use pyo3::prelude::*;
use pyo3::types::PyType;

#[pyclass(name = "StoreConfig")]
pub struct PyStoreConfig {
    inner: timslite::StoreConfig,
}

#[pymethods]
impl PyStoreConfig {
    #[new]
    #[pyo3(signature = (*, flush_interval=15, idle_timeout=1800, data_segment_size=67108864, index_segment_size=4194304, initial_data_segment_size=262144, initial_index_segment_size=4096, compress_level=6, cache_max_memory=268435456, cache_idle_timeout=1800, retention_check_hour=0, enable_background_thread=true, enable_journal=true))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        flush_interval: u64,
        idle_timeout: u64,
        data_segment_size: u64,
        index_segment_size: u64,
        initial_data_segment_size: u64,
        initial_index_segment_size: u64,
        compress_level: u8,
        cache_max_memory: usize,
        cache_idle_timeout: u64,
        retention_check_hour: u8,
        enable_background_thread: bool,
        enable_journal: bool,
    ) -> Self {
        Self {
            inner: timslite::StoreConfig::builder()
                .flush_interval(std::time::Duration::from_secs(flush_interval))
                .idle_timeout(std::time::Duration::from_secs(idle_timeout))
                .data_segment_size(data_segment_size)
                .index_segment_size(index_segment_size)
                .initial_data_segment_size(initial_data_segment_size)
                .initial_index_segment_size(initial_index_segment_size)
                .compress_level(compress_level)
                .cache_max_memory(cache_max_memory)
                .cache_idle_timeout(std::time::Duration::from_secs(cache_idle_timeout))
                .retention_check_hour(retention_check_hour)
                .enable_background_thread(enable_background_thread)
                .enable_journal(enable_journal)
                .build(),
        }
    }

    #[classmethod]
    fn default(_cls: &Bound<'_, PyType>) -> Self {
        Self {
            inner: timslite::StoreConfig::default(),
        }
    }

    #[getter]
    fn flush_interval(&self) -> u64 {
        self.inner.flush_interval().as_secs()
    }

    #[getter]
    fn idle_timeout(&self) -> u64 {
        self.inner.idle_timeout().as_secs()
    }

    #[getter]
    fn data_segment_size(&self) -> u64 {
        self.inner.data_segment_size()
    }

    #[getter]
    fn index_segment_size(&self) -> u64 {
        self.inner.index_segment_size()
    }

    #[getter]
    fn initial_data_segment_size(&self) -> u64 {
        self.inner.initial_data_segment_size()
    }

    #[getter]
    fn initial_index_segment_size(&self) -> u64 {
        self.inner.initial_index_segment_size()
    }

    #[getter]
    fn compress_level(&self) -> u8 {
        self.inner.compress_level()
    }

    #[getter]
    fn cache_max_memory(&self) -> usize {
        self.inner.cache_max_memory()
    }

    #[getter]
    fn cache_idle_timeout(&self) -> u64 {
        self.inner.cache_idle_timeout().as_secs()
    }

    #[getter]
    fn retention_check_hour(&self) -> u8 {
        self.inner.retention_check_hour()
    }

    #[getter]
    fn enable_background_thread(&self) -> bool {
        self.inner.enable_background_thread()
    }

    #[getter]
    fn enable_journal(&self) -> bool {
        self.inner.enable_journal()
    }
}

impl PyStoreConfig {
    /// Access the inner StoreConfig.
    pub fn inner(&self) -> &timslite::StoreConfig {
        &self.inner
    }
}
