//! Store: facade that manages all datasets and background tasks.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::bg::BackgroundTasks;
use crate::bg::TickResult;
use crate::cache::BlockCache;
use crate::config::{DataSetConfigBuilder, StoreConfig};
use crate::dataset::{DataSet, DataSetKey};
use crate::error::{Result, TmslError};
use crate::journal::{
    meta_values_from_file, JournalManager, JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE,
};
use crate::queue::{DatasetQueue, DatasetQueueConsumer};

fn is_valid_dataset_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn validate_dataset_component(label: &str, value: &str) -> Result<()> {
    if is_valid_dataset_component(value) {
        Ok(())
    } else {
        Err(TmslError::InvalidData(format!(
            "{label} must match ^[0-9A-Za-z_-]+$"
        )))
    }
}

fn validate_dataset_path_components(name: &str, dataset_type: &str) -> Result<()> {
    validate_dataset_component("dataset name", name)?;
    validate_dataset_component("dataset type", dataset_type)
}

/// Opaque handle for FFI consumers.
#[derive(Clone, Copy)]
pub struct DataSetHandle(pub u64);

/// The Store: top-level facade for managing datasets.
pub struct Store {
    data_dir: PathBuf,
    datasets: Arc<RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>>,
    config: StoreConfig,
    block_cache: Arc<BlockCache>,
    journal: JournalManager,
    bg_tasks: Option<BackgroundTasks>,
    next_handle_id: u64,
    handles: HashMap<u64, DataSetKey>,
    read_only_handles: HashSet<u64>,
}

impl Store {
    /// Open a store at the given directory.
    pub fn open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;
        let journal = JournalManager::open_or_create(&data_dir, &config)?;

        let mut datasets = HashMap::new();

        // Scan for existing datasets
        for entry in std::fs::read_dir(&data_dir)? {
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if name == JOURNAL_DATASET_NAME {
                continue;
            }
            if !is_valid_dataset_component(&name) {
                log::warn!(
                    "[store] skipping dataset directory with invalid name: {}",
                    name
                );
                continue;
            }
            // Scan types (skip internal `data/` and `index/` directories)
            for type_entry in std::fs::read_dir(&path)? {
                let type_path = type_entry?.path();
                if !type_path.is_dir() {
                    continue;
                }
                let type_name = type_path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if type_name == "data" || type_name == "index" {
                    continue; // Internal subdirectory, not a dataset type
                }
                if !is_valid_dataset_component(type_name) {
                    log::warn!(
                        "[store] skipping dataset type directory with invalid name: {}/{}",
                        name,
                        type_name
                    );
                    continue;
                }
                let dataset_type = type_name.to_string();

                // Only open datasets that have a meta file (explicitly created)
                if !type_path.join("meta").exists() {
                    continue;
                }

                let key = DataSetKey {
                    name: name.clone(),
                    dataset_type: dataset_type.clone(),
                };
                let ds = DataSet::open(key.clone(), type_path.clone())?;
                log::info!("[store] loaded existing dataset: {}/{}", name, dataset_type);
                datasets.insert(key, Arc::new(Mutex::new(ds)));
            }
        }

        let datasets = Arc::new(RwLock::new(datasets));
        let block_cache = Arc::new(BlockCache::new(config.cache_max_memory));

        let mut store = Self {
            data_dir,
            datasets: Arc::clone(&datasets),
            config: config.clone(),
            block_cache: Arc::clone(&block_cache),
            journal,
            bg_tasks: None,
            next_handle_id: 0,
            handles: HashMap::new(),
            read_only_handles: HashSet::new(),
        };

        // Start background tasks (or just the executor)
        if config.enable_background_thread {
            store.bg_tasks = Some(BackgroundTasks::start(
                datasets,
                block_cache,
                config.flush_interval,
                config.idle_timeout,
                config.cache_idle_timeout,
                config.retention_check_hour,
            ));
        } else {
            store.bg_tasks = Some(BackgroundTasks::new(
                datasets,
                block_cache,
                config.flush_interval,
                config.idle_timeout,
                config.cache_idle_timeout,
                config.retention_check_hour,
            ));
            log::info!("[store] background thread disabled, manual tick required");
        }

        Ok(store)
    }

    /// Create a new dataset using a builder with store-level defaults.
    ///
    /// When `config_builder` is `None`, all parameters inherit store defaults
    /// and `index_continuous` defaults to 0.
    pub fn create_dataset_with_config(
        &mut self,
        name: &str,
        dataset_type: &str,
        config_builder: Option<DataSetConfigBuilder>,
    ) -> Result<DataSetHandle> {
        if name == JOURNAL_DATASET_NAME && dataset_type == JOURNAL_DATASET_TYPE {
            return Err(TmslError::InvalidData("journal dataset is reserved".into()));
        }
        validate_dataset_path_components(name, dataset_type)?;

        let key = DataSetKey {
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
        };

        // Check if already open
        {
            let guard = self.datasets.read().unwrap();
            if guard.contains_key(&key) {
                return Err(TmslError::AlreadyExists(format!(
                    "dataset {}/{} is already open",
                    name, dataset_type
                )));
            }
        }

        let config = config_builder
            .unwrap_or_else(|| DataSetConfigBuilder::from_store(&self.config))
            .build();

        // Create new dataset
        let dir = self.data_dir.join(name).join(dataset_type);
        let ds = DataSet::create(
            key.clone(),
            dir,
            config.data_segment_size,
            config.index_segment_size,
            config.compress_level,
            config.index_continuous,
            config.initial_data_segment_size,
            config.initial_index_segment_size,
            config.retention_ms,
        )?;

        let ds = Arc::new(Mutex::new(ds));
        {
            let mut guard = self.datasets.write().unwrap();
            guard.insert(key.clone(), ds);
        }

        let metadata =
            meta_values_from_file(&self.data_dir.join(name).join(dataset_type).join("meta"))?;
        self.journal.append_create(&key, &metadata)?;

        let id = self.next_handle_id;
        self.next_handle_id += 1;
        self.handles.insert(id, key);
        Ok(DataSetHandle(id))
    }

    /// Create a new dataset (explicit, errors if already exists).
    ///
    /// Backward-compatible wrapper around `create_dataset_with_config`.
    pub fn create_dataset(
        &mut self,
        name: &str,
        dataset_type: &str,
        data_segment_size: u64,
        index_segment_size: u64,
        compress_level: u8,
        index_continuous: u8,
        retention_ms: u64,
    ) -> Result<DataSetHandle> {
        self.create_dataset_with_config(
            name,
            dataset_type,
            Some(
                DataSetConfigBuilder::from_store(&self.config)
                    .data_segment_size(data_segment_size)
                    .index_segment_size(index_segment_size)
                    .compress_level(compress_level)
                    .index_continuous(index_continuous)
                    .retention_ms(retention_ms),
            ),
        )
    }

    /// Open an existing dataset (errors if not found).
    pub fn open_dataset(&mut self, name: &str, dataset_type: &str) -> Result<DataSetHandle> {
        if name == JOURNAL_DATASET_NAME && dataset_type == JOURNAL_DATASET_TYPE {
            if !self.journal.is_enabled() {
                return Err(TmslError::NotFound("journal is disabled".into()));
            }
            let id = self.next_handle_id;
            self.next_handle_id += 1;
            self.handles.insert(id, JournalManager::key());
            self.read_only_handles.insert(id);
            return Ok(DataSetHandle(id));
        }
        validate_dataset_path_components(name, dataset_type)?;

        let key = DataSetKey {
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
        };

        // Check if already open
        {
            let guard = self.datasets.read().unwrap();
            if guard.contains_key(&key) {
                let id = self.next_handle_id;
                self.next_handle_id += 1;
                self.handles.insert(id, key.clone());
                return Ok(DataSetHandle(id));
            }
        }

        // Open existing dataset
        let dir = self.data_dir.join(name).join(dataset_type);
        let ds = DataSet::open(key.clone(), dir)?;

        let ds = Arc::new(Mutex::new(ds));
        {
            let mut guard = self.datasets.write().unwrap();
            guard.insert(key.clone(), ds);
        }

        let id = self.next_handle_id;
        self.next_handle_id += 1;
        self.handles.insert(id, key);
        Ok(DataSetHandle(id))
    }

    /// Close a dataset by handle.
    pub fn close_dataset(&mut self, handle: DataSetHandle) -> Result<()> {
        if self.read_only_handles.remove(&handle.0) {
            self.handles.remove(&handle.0);
            return Ok(());
        }
        if let Some(key) = self.handles.remove(&handle.0) {
            if self.handles.values().any(|existing| *existing == key) {
                return Ok(());
            }
            let mut guard = self.datasets.write().unwrap();
            if let Some(ds_arc) = guard.remove(&key) {
                let mut ds = ds_arc.lock().unwrap();
                ds.close()?;
            }
        }
        Ok(())
    }

    /// Drop (delete) an entire dataset by handle.
    pub fn drop_dataset(&mut self, handle: DataSetHandle) -> Result<()> {
        if self.read_only_handles.contains(&handle.0) {
            return Err(TmslError::InvalidData(
                "read-only internal dataset cannot be dropped".into(),
            ));
        }
        let key = self
            .handles
            .remove(&handle.0)
            .ok_or_else(|| TmslError::NotFound("dataset handle not found".into()))?;

        let (base_dir, metadata) = {
            let mut guard = self.datasets.write().unwrap();
            let ds_arc = guard
                .remove(&key)
                .ok_or_else(|| TmslError::NotFound(format!("dataset {:?} not found", key)))?;
            let mut ds = ds_arc.lock().unwrap();
            let metadata = meta_values_from_file(&ds.base_dir.join("meta"))?;
            let base_dir = ds.base_dir.clone();
            ds.close()?;
            (base_dir, metadata)
        };

        DataSet::drop_dataset(&base_dir)?;
        self.journal.append_drop(&key, &metadata)?;
        log::info!("[store] dropped dataset: {}/{}", key.name, key.dataset_type);
        Ok(())
    }

    /// Drop (delete) an entire dataset by name and type.
    pub fn drop_dataset_by_name(&mut self, name: &str, dataset_type: &str) -> Result<()> {
        if name == JOURNAL_DATASET_NAME && dataset_type == JOURNAL_DATASET_TYPE {
            return Err(TmslError::InvalidData("journal dataset is reserved".into()));
        }
        validate_dataset_path_components(name, dataset_type)?;

        let key = DataSetKey {
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
        };

        // Remove from handles
        self.handles.retain(|_id, k| *k != key);

        let base_dir = self.data_dir.join(name).join(dataset_type);
        let metadata = meta_values_from_file(&base_dir.join("meta"))?;

        // Remove from open datasets if present
        {
            let mut guard = self.datasets.write().unwrap();
            if let Some(ds_arc) = guard.remove(&key) {
                let mut ds = ds_arc.lock().unwrap();
                let _ = ds.close(); // best-effort close
            }
        }

        DataSet::drop_dataset(&base_dir)?;
        self.journal.append_drop(&key, &metadata)?;
        log::info!("[store] dropped dataset: {}/{}", name, dataset_type);
        Ok(())
    }

    /// Get a dataset handle for internal use.
    pub fn get_dataset(&self, handle: &DataSetHandle) -> Result<Arc<Mutex<DataSet>>> {
        let key = self
            .handles
            .get(&handle.0)
            .ok_or_else(|| crate::error::TmslError::NotFound("dataset handle not found".into()))?;
        if JournalManager::is_journal_key(key) {
            return self.journal.dataset();
        }
        let guard = self.datasets.read().unwrap();
        let ds = guard.get(key).ok_or_else(|| {
            crate::error::TmslError::NotFound(format!("dataset {:?} not found", key))
        })?;
        Ok(Arc::clone(ds))
    }

    /// Write through the Store so journal and global cache hooks are applied.
    pub fn write_dataset(
        &mut self,
        handle: DataSetHandle,
        timestamp: i64,
        data: &[u8],
    ) -> Result<()> {
        let key = self
            .handles
            .get(&handle.0)
            .ok_or_else(|| TmslError::NotFound("dataset handle not found".into()))?
            .clone();
        if JournalManager::is_journal_key(&key) || self.read_only_handles.contains(&handle.0) {
            return Err(TmslError::InvalidData(
                "read-only internal dataset cannot be written".into(),
            ));
        }
        let ds_arc = {
            let guard = self.datasets.read().unwrap();
            guard
                .get(&key)
                .ok_or_else(|| TmslError::NotFound(format!("dataset {:?} not found", key)))?
                .clone()
        };
        let outcome = {
            let mut ds = ds_arc
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            ds.write_with_cache_outcome(timestamp, data, Some(self.block_cache()))?
        };
        self.journal.append_data_write(&key, outcome.index_entry)?;
        Ok(())
    }

    /// Append through the Store so journal and global cache hooks are applied.
    pub fn append_dataset(
        &mut self,
        handle: DataSetHandle,
        timestamp: i64,
        data: &[u8],
    ) -> Result<()> {
        let key = self
            .handles
            .get(&handle.0)
            .ok_or_else(|| TmslError::NotFound("dataset handle not found".into()))?
            .clone();
        if JournalManager::is_journal_key(&key) || self.read_only_handles.contains(&handle.0) {
            return Err(TmslError::InvalidData(
                "read-only internal dataset cannot be appended".into(),
            ));
        }
        let ds_arc = {
            let guard = self.datasets.read().unwrap();
            guard
                .get(&key)
                .ok_or_else(|| TmslError::NotFound(format!("dataset {:?} not found", key)))?
                .clone()
        };
        let outcome = {
            let mut ds = ds_arc
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            ds.append_with_cache_outcome(timestamp, data, Some(self.block_cache()))?
        };
        if let Some(outcome) = outcome {
            self.journal.append_data_append(
                &key,
                outcome.index_entry,
                outcome.data_offset,
                outcome.data_len,
            )?;
        }
        Ok(())
    }

    /// Delete through the Store so journal and global cache hooks are applied.
    pub fn delete_dataset_record(&mut self, handle: DataSetHandle, timestamp: i64) -> Result<()> {
        let key = self
            .handles
            .get(&handle.0)
            .ok_or_else(|| TmslError::NotFound("dataset handle not found".into()))?
            .clone();
        if JournalManager::is_journal_key(&key) || self.read_only_handles.contains(&handle.0) {
            return Err(TmslError::InvalidData(
                "read-only internal dataset cannot be deleted".into(),
            ));
        }
        let ds_arc = {
            let guard = self.datasets.read().unwrap();
            guard
                .get(&key)
                .ok_or_else(|| TmslError::NotFound(format!("dataset {:?} not found", key)))?
                .clone()
        };
        let outcome = {
            let mut ds = ds_arc
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            ds.delete_with_cache_outcome(timestamp, Some(self.block_cache()))?
        };
        self.journal
            .append_data_delete(&key, outcome.old_index_entry)?;
        Ok(())
    }

    /// Get a reference to the global block cache.
    pub fn block_cache(&self) -> &Arc<BlockCache> {
        &self.block_cache
    }

    /// Get a reference to the store config.
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    /// Execute one tick of all background tasks synchronously.
    ///
    /// Checks if flush, idle-close, cache eviction, or retention reclaim are
    /// due and runs them immediately.  Returns the number of executed tasks
    /// and the delay until the next one is due.
    ///
    /// Safe to call even when the background thread is enabled — it will
    /// be serialised with the thread via the internal `Mutex`.
    pub fn tick_background_tasks(&self) -> Result<TickResult> {
        let bg = self
            .bg_tasks
            .as_ref()
            .ok_or_else(|| TmslError::InvalidData("bg_tasks not initialised".into()))?;
        Ok(bg.tick())
    }

    /// Return the duration until the next background task is due.
    ///
    /// Reads a snapshot of the executor state without running any tasks.
    /// Blocks briefly if another thread is currently executing `tick`.
    pub fn next_background_delay(&self) -> Result<Duration> {
        let bg = self
            .bg_tasks
            .as_ref()
            .ok_or_else(|| TmslError::InvalidData("bg_tasks not initialised".into()))?;
        Ok(bg.next_delay())
    }

    /// Close the store completely.
    pub fn close(mut self) -> Result<()> {
        // 1. Stop background tasks
        if let Some(mut bg) = self.bg_tasks.take() {
            bg.stop();
        }

        // 2. Flush and close all datasets (close queues first)
        let mut guard = self.datasets.write().unwrap();
        for (_key, ds_arc) in guard.drain() {
            let mut ds = ds_arc.lock().unwrap();
            let _ = ds.close_queue(); // best-effort close queue
            if let Err(e) = ds.close() {
                log::error!("[store] close failed: {}", e);
            }
        }
        self.journal.close()?;

        Ok(())
    }

    // ─── Queue operations ────────────────────────────────────────────────────

    /// Open the queue subsystem for a dataset.
    ///
    /// Must be called before any consumer or push operations on the dataset.
    /// Returns a `DatasetQueue` handle that can be cloned and shared.
    pub fn open_queue(&mut self, handle: DataSetHandle) -> Result<DatasetQueue> {
        let key = self
            .handles
            .get(&handle.0)
            .ok_or_else(|| TmslError::NotFound("dataset handle not found".into()))?
            .clone();
        if JournalManager::is_journal_key(&key) {
            return self.journal.open_queue();
        }
        let ds_arc = self
            .datasets
            .read()
            .unwrap()
            .get(&key)
            .ok_or_else(|| TmslError::NotFound(format!("dataset {:?} not found", key)))?
            .clone();

        // Initialize queue on the dataset
        let (inner, notify) = {
            let mut ds = ds_arc
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            ds.open_queue()?
        };

        Ok(DatasetQueue {
            dataset: ds_arc,
            inner,
            notify,
            allow_push: true,
        })
    }

    /// Close the queue subsystem for a dataset.
    pub fn close_queue(&mut self, handle: DataSetHandle) -> Result<()> {
        let key = self
            .handles
            .get(&handle.0)
            .ok_or_else(|| TmslError::NotFound("dataset handle not found".into()))?
            .clone();
        if JournalManager::is_journal_key(&key) {
            return Err(TmslError::InvalidData(
                "journal queue is managed by JournalManager".into(),
            ));
        }
        let ds_arc = self
            .datasets
            .read()
            .unwrap()
            .get(&key)
            .ok_or_else(|| TmslError::NotFound(format!("dataset {:?} not found", key)))?
            .clone();

        let mut ds = ds_arc
            .lock()
            .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
        ds.close_queue()
    }

    /// Open or create a consumer group for a dataset queue.
    ///
    /// The queue must have been opened via `open_queue` first.
    /// Multiple consumers in the same group share progress.
    pub fn open_consumer(
        &mut self,
        queue: &DatasetQueue,
        group_name: &str,
    ) -> Result<DatasetQueueConsumer> {
        queue.open_consumer(group_name)
    }

    /// Drop a consumer group for a dataset queue.
    pub fn drop_consumer(&mut self, queue: &DatasetQueue, group_name: &str) -> Result<()> {
        queue.drop_consumer(group_name)
    }

    /// Push data into a dataset queue.
    ///
    /// Auto-increments timestamp and notifies waiting consumers.
    pub fn queue_push(&mut self, queue: &DatasetQueue, data: &[u8]) -> Result<i64> {
        queue.push(data)
    }

    /// Poll for the next record from a consumer.
    pub fn queue_poll(
        &self,
        consumer: &DatasetQueueConsumer,
        timeout: Duration,
    ) -> Result<Option<(i64, Vec<u8>)>> {
        consumer.poll(timeout)
    }

    /// Ack a previously polled record.
    pub fn queue_ack(&self, consumer: &DatasetQueueConsumer, timestamp: i64) -> Result<()> {
        consumer.ack(timestamp)
    }

    /// Open the built-in journal queue.
    pub fn open_journal_queue(&mut self) -> Result<DatasetQueue> {
        self.journal.open_queue()
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort cleanup if close() wasn't called
        if let Some(mut bg) = self.bg_tasks.take() {
            bg.stop();
        }
        let _ = self.journal.flush();
    }
}
