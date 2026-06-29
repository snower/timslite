//! Store: facade that manages all datasets and background tasks.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::bg::BackgroundTasks;
use crate::bg::TickResult;
use crate::cache::BlockCache;
use crate::config::{DataSetConfigBuilder, StoreConfig};
use crate::dataset::{
    DataSet, DataSetInspectResult, DataSetJournalSink, DataSetKey, DataSetLifecycleSink,
    DataSetRuntimeContext, SegmentFlushQueue,
};
use crate::error::{Result, TmslError};
use crate::index::segment::IndexEntry;
use crate::journal::{
    meta_values_from_file, validate_create_drop_record_inputs, JournalIndexInfo, JournalManager,
    JournalQueue, JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE,
};
use crate::meta::META_VALUES_LEN_V1;
use crate::util::{is_path_safe_component, PATH_COMPONENT_MAX_LEN};

const MAX_IDENTIFIER_FILE: &str = "max_identifier";
const DATASET_IDENTIFIER_FILE: &str = "identifier";
const STORE_LOCK_FILE: &str = ".lock";

enum StoreLockAttempt {
    Acquired(File),
    Locked,
}

fn validate_dataset_component(label: &str, value: &str) -> Result<()> {
    if is_path_safe_component(value) {
        Ok(())
    } else {
        Err(TmslError::InvalidData(format!(
            "{label} must match ^[0-9A-Za-z_-]+$ and be at most {PATH_COMPONENT_MAX_LEN} bytes"
        )))
    }
}

fn validate_dataset_path_components(name: &str, dataset_type: &str) -> Result<()> {
    validate_dataset_component("dataset name", name)?;
    validate_dataset_component("dataset type", dataset_type)
}

fn parse_identifier_value(label: &str, content: &str, allow_zero: bool) -> Result<u64> {
    let value = content.trim();
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(TmslError::InvalidData(format!(
            "{label} must be a decimal u64 string"
        )));
    }
    let identifier = value.parse::<u64>().map_err(|_| {
        TmslError::InvalidData(format!("{label} exceeds maximum u64 identifier value"))
    })?;
    if !allow_zero && identifier == 0 {
        return Err(TmslError::InvalidData(format!("{label} must be > 0")));
    }
    Ok(identifier)
}

fn read_identifier_file(path: &Path, label: &str, allow_zero: bool) -> Result<u64> {
    let content = std::fs::read_to_string(path)?;
    parse_identifier_value(label, &content, allow_zero)
}

fn read_max_identifier(data_dir: &Path) -> Result<u64> {
    let path = data_dir.join(MAX_IDENTIFIER_FILE);
    match std::fs::read_to_string(&path) {
        Ok(content) => parse_identifier_value(MAX_IDENTIFIER_FILE, &content, true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err.into()),
    }
}

fn write_identifier_file(path: &Path, identifier: u64) -> Result<()> {
    std::fs::write(path, identifier.to_string())?;
    Ok(())
}

fn dataset_identifier_path(dataset_dir: &Path) -> PathBuf {
    dataset_dir.join(DATASET_IDENTIFIER_FILE)
}

fn read_dataset_identifier(dataset_dir: &Path) -> Result<u64> {
    let path = dataset_identifier_path(dataset_dir);
    match read_identifier_file(&path, DATASET_IDENTIFIER_FILE, false) {
        Err(TmslError::Io(err)) if err.kind() == ErrorKind::NotFound => Err(TmslError::NotFound(
            format!("dataset identifier not found at {:?}", path),
        )),
        other => other,
    }
}

fn try_acquire_store_lock(data_dir: &Path) -> Result<StoreLockAttempt> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(data_dir.join(STORE_LOCK_FILE))?;
    match file.try_lock() {
        Ok(()) => Ok(StoreLockAttempt::Acquired(file)),
        Err(fs::TryLockError::WouldBlock) => Ok(StoreLockAttempt::Locked),
        Err(fs::TryLockError::Error(err)) => Err(err.into()),
    }
}

struct StoreDatasetLifecycle {
    datasets: Arc<RwLock<HashMap<DataSetKey, Arc<DataSet>>>>,
}

impl StoreDatasetLifecycle {
    fn new(datasets: Arc<RwLock<HashMap<DataSetKey, Arc<DataSet>>>>) -> Self {
        Self { datasets }
    }

    fn invalidate(&self, key: &DataSetKey) {
        if let Ok(mut datasets) = self.datasets.write() {
            datasets.remove(key);
        }
    }
}

impl DataSetLifecycleSink for StoreDatasetLifecycle {
    fn dataset_closed(&self, key: &DataSetKey) {
        self.invalidate(key);
    }
}

/// The Store: top-level facade for managing datasets.
pub struct Store {
    data_dir: PathBuf,
    datasets: Arc<RwLock<HashMap<DataSetKey, Arc<DataSet>>>>,
    lifecycle: Arc<StoreDatasetLifecycle>,
    config: StoreConfig,
    block_cache: Arc<BlockCache>,
    journal: Arc<JournalManager>,
    bg_tasks: Option<BackgroundTasks>,
    max_identifier: u64,
    identifier_to_key: HashMap<u64, DataSetKey>,
    read_only: bool,
    store_lock: Option<File>,
}

impl Store {
    fn dataset_runtime_context(
        block_cache: &Arc<BlockCache>,
        journal: &Arc<JournalManager>,
        flush_queue: Option<&SegmentFlushQueue>,
        lifecycle: &Arc<StoreDatasetLifecycle>,
        enable_journal: bool,
        read_only: bool,
    ) -> Result<DataSetRuntimeContext> {
        let journal_sink: Option<Arc<dyn DataSetJournalSink>> =
            if !read_only && enable_journal && journal.is_enabled() {
                let sink: Arc<dyn DataSetJournalSink> = journal.clone();
                Some(sink)
            } else {
                None
            };
        let flush_queue = if read_only {
            None
        } else {
            Some(Arc::clone(flush_queue.ok_or_else(|| {
                TmslError::InvalidData("bg_tasks not initialised".into())
            })?))
        };
        let lifecycle_sink: Arc<dyn DataSetLifecycleSink> = lifecycle.clone();
        let context =
            DataSetRuntimeContext::new(Some(Arc::clone(block_cache)), journal_sink, flush_queue)
                .with_lifecycle(lifecycle_sink);
        Ok(if read_only {
            context.with_read_only()
        } else {
            context
        })
    }

    fn ensure_writable(&self, action: &str) -> Result<()> {
        if self.read_only {
            return Err(TmslError::InvalidData(format!(
                "read-only store cannot {action}"
            )));
        }
        Ok(())
    }

    fn dataset_dir(&self, key: &DataSetKey) -> PathBuf {
        self.data_dir.join(&key.name).join(&key.dataset_type)
    }

    fn validate_identifier_within_max(&self, identifier: u64, key: &DataSetKey) -> Result<()> {
        if identifier > self.max_identifier {
            return Err(TmslError::InvalidData(format!(
                "dataset {:?} identifier {identifier} exceeds authoritative max_identifier {}",
                key, self.max_identifier
            )));
        }
        Ok(())
    }

    fn scan_dataset_dirs(&self) -> Result<Vec<(DataSetKey, PathBuf)>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&self.data_dir)? {
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name == JOURNAL_DATASET_NAME || !is_path_safe_component(name) {
                continue;
            }
            for type_entry in std::fs::read_dir(&path)? {
                let type_path = type_entry?.path();
                if !type_path.is_dir() {
                    continue;
                }
                let Some(type_name) = type_path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !is_path_safe_component(type_name) {
                    continue;
                }
                out.push((
                    DataSetKey {
                        name: name.to_string(),
                        dataset_type: type_name.to_string(),
                    },
                    type_path,
                ));
            }
        }
        out.sort_by(|(a, _), (b, _)| {
            a.name
                .cmp(&b.name)
                .then_with(|| a.dataset_type.cmp(&b.dataset_type))
        });
        Ok(out)
    }

    fn find_key_by_identifier(&mut self, identifier: u64) -> Result<DataSetKey> {
        if identifier == 0 {
            return Err(TmslError::InvalidData(
                "dataset identifier must be > 0".into(),
            ));
        }
        if let Some(key) = self.identifier_to_key.get(&identifier).cloned() {
            return Ok(key);
        }

        let mut found: Option<DataSetKey> = None;
        for (key, dir) in self.scan_dataset_dirs()? {
            if !dir.join("meta").exists() || !dataset_identifier_path(&dir).exists() {
                continue;
            }
            let current = read_dataset_identifier(&dir)?;
            self.validate_identifier_within_max(current, &key)?;
            if current != identifier {
                continue;
            }
            if let Some(existing) = found.as_ref() {
                if existing != &key {
                    return Err(TmslError::InvalidData(format!(
                        "duplicate dataset identifier {identifier}: {:?} and {:?}",
                        existing, key
                    )));
                }
            }
            found = Some(key);
        }

        let key =
            found.ok_or_else(|| TmslError::NotFound(format!("dataset identifier {identifier}")))?;
        self.identifier_to_key.insert(identifier, key.clone());
        Ok(key)
    }

    /// Open a store at the given directory.
    pub fn open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self> {
        config.validate()?;
        let data_dir = data_dir.as_ref().to_path_buf();
        let (read_only, store_lock) = match config.read_only {
            Some(true) => (true, None),
            Some(false) => {
                std::fs::create_dir_all(&data_dir)?;
                match try_acquire_store_lock(&data_dir)? {
                    StoreLockAttempt::Acquired(file) => (false, Some(file)),
                    StoreLockAttempt::Locked => {
                        return Err(TmslError::InvalidData(
                            "store is already locked for writing".into(),
                        ));
                    }
                }
            }
            None => {
                std::fs::create_dir_all(&data_dir)?;
                match try_acquire_store_lock(&data_dir)? {
                    StoreLockAttempt::Acquired(file) => (false, Some(file)),
                    StoreLockAttempt::Locked => (true, None),
                }
            }
        };
        let block_cache = Arc::new(BlockCache::new(config.cache_max_memory));
        let flush_queue = DataSetRuntimeContext::new_flush_queue();
        let journal = Arc::new(if read_only {
            JournalManager::open_read_only(&data_dir, &config)?
        } else {
            JournalManager::open_or_create(&data_dir, &config, Some(Arc::clone(&flush_queue)))?
        });
        let max_identifier = read_max_identifier(&data_dir)?;
        let identifier_to_key: HashMap<u64, DataSetKey> = HashMap::new();
        let datasets = Arc::new(RwLock::new(HashMap::new()));
        let lifecycle = Arc::new(StoreDatasetLifecycle::new(Arc::clone(&datasets)));

        let mut store = Self {
            data_dir,
            datasets: Arc::clone(&datasets),
            lifecycle,
            config: config.clone(),
            block_cache: Arc::clone(&block_cache),
            journal: Arc::clone(&journal),
            bg_tasks: None,
            max_identifier,
            identifier_to_key,
            read_only,
            store_lock,
        };

        // Start background tasks (or just the executor)
        if read_only {
            log::info!("[store] opened in read-only mode, background tasks disabled");
        } else if config.enable_background_thread {
            store.bg_tasks = Some(BackgroundTasks::start(
                datasets,
                Arc::clone(&flush_queue),
                Arc::clone(&journal),
                block_cache,
                config.flush_interval,
                config.idle_timeout,
                config.cache_idle_timeout,
                config.retention_check_hour,
            ));
        } else {
            store.bg_tasks = Some(BackgroundTasks::new(
                datasets,
                Arc::clone(&flush_queue),
                Arc::clone(&journal),
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
    ) -> Result<DataSet> {
        self.ensure_writable("create dataset")?;
        if name == JOURNAL_DATASET_NAME && dataset_type == JOURNAL_DATASET_TYPE {
            return Err(TmslError::InvalidData("journal dataset is reserved".into()));
        }
        validate_dataset_path_components(name, dataset_type)?;

        let key = DataSetKey {
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
        };

        let dir = self.dataset_dir(&key);

        // Check if already open or already present on disk.
        {
            let guard = self.datasets.read().unwrap();
            if guard.contains_key(&key) {
                return Err(TmslError::AlreadyExists(format!(
                    "dataset {}/{} is already open",
                    name, dataset_type
                )));
            }
        }
        if dir.join("meta").exists() {
            return Err(TmslError::AlreadyExists(format!(
                "dataset already exists at {:?}",
                dir
            )));
        }

        let config = config_builder
            .unwrap_or_else(|| DataSetConfigBuilder::from_store(&self.config))
            .build()?;
        let effective_journal = self.journal.is_enabled() && config.enable_journal;
        let identifier = self
            .max_identifier
            .checked_add(1)
            .ok_or_else(|| TmslError::InvalidData("dataset identifier overflow".into()))?;
        if effective_journal {
            validate_create_drop_record_inputs(identifier, &key, META_VALUES_LEN_V1)?;
        }
        write_identifier_file(&self.data_dir.join(MAX_IDENTIFIER_FILE), identifier)?;
        self.max_identifier = identifier;

        // Create new dataset
        let ds = DataSet::create_with_compression(
            key.clone(),
            dir.clone(),
            config.data_segment_size,
            config.index_segment_size,
            config.compress_level,
            config.compress_type,
            config.index_continuous,
            config.initial_data_segment_size,
            config.initial_index_segment_size,
            config.retention_window,
            config.enable_journal,
        )?;
        write_identifier_file(&dataset_identifier_path(&dir), identifier)?;
        ds.set_identifier(identifier)?;
        ds.set_runtime_context(Self::dataset_runtime_context(
            &self.block_cache,
            &self.journal,
            self.bg_tasks.as_ref().map(|bg| bg.flush_queue()),
            &self.lifecycle,
            config.enable_journal,
            self.read_only,
        )?)?;

        let ds = Arc::new(ds);
        {
            let mut guard = self.datasets.write().unwrap();
            guard.insert(key.clone(), Arc::clone(&ds));
        }
        self.identifier_to_key.insert(identifier, key.clone());

        let metadata =
            meta_values_from_file(&self.data_dir.join(name).join(dataset_type).join("meta"))?;
        if effective_journal {
            self.journal.append_create(identifier, &key, &metadata)?;
        }

        Ok(ds.as_ref().clone())
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
        retention_window: u64,
    ) -> Result<DataSet> {
        self.create_dataset_with_config(
            name,
            dataset_type,
            Some(
                DataSetConfigBuilder::from_store(&self.config)
                    .data_segment_size(data_segment_size)
                    .index_segment_size(index_segment_size)
                    .initial_data_segment_size(
                        self.config
                            .initial_data_segment_size()
                            .min(data_segment_size),
                    )
                    .initial_index_segment_size(
                        self.config
                            .initial_index_segment_size()
                            .min(index_segment_size),
                    )
                    .compress_level(compress_level)
                    .index_continuous(index_continuous)
                    .retention_window(retention_window),
            ),
        )
    }

    /// Open an existing dataset (errors if not found).
    pub fn open_dataset(&mut self, name: &str, dataset_type: &str) -> Result<DataSet> {
        if name == JOURNAL_DATASET_NAME && dataset_type == JOURNAL_DATASET_TYPE {
            return Err(TmslError::NotFound(
                "journal is not exposed as a dataset handle".into(),
            ));
        }
        validate_dataset_path_components(name, dataset_type)?;

        let key = DataSetKey {
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
        };

        // Check if already open
        let already_open = {
            let guard = self.datasets.read().unwrap();
            guard.get(&key).cloned()
        };
        if let Some(ds) = already_open {
            return Ok(ds.as_ref().clone());
        }

        // Open existing dataset
        let dir = self.dataset_dir(&key);
        let identifier = read_dataset_identifier(&dir)?;
        self.validate_identifier_within_max(identifier, &key)?;
        if let Some(existing) = self.identifier_to_key.get(&identifier) {
            if existing != &key {
                return Err(TmslError::InvalidData(format!(
                    "duplicate dataset identifier {identifier}: {:?} and {:?}",
                    existing, key
                )));
            }
        }
        let ds = if self.read_only {
            DataSet::open_read_only(key.clone(), dir)?
        } else {
            DataSet::open(key.clone(), dir)?
        };
        ds.set_identifier(identifier)?;
        ds.set_runtime_context(Self::dataset_runtime_context(
            &self.block_cache,
            &self.journal,
            self.bg_tasks.as_ref().map(|bg| bg.flush_queue()),
            &self.lifecycle,
            ds.enable_journal(),
            self.read_only,
        )?)?;

        let ds = Arc::new(ds);
        {
            let mut guard = self.datasets.write().unwrap();
            guard.insert(key.clone(), Arc::clone(&ds));
        }
        self.identifier_to_key.insert(identifier, key.clone());
        Ok(ds.as_ref().clone())
    }

    /// Open an existing dataset by its Store-assigned numeric identifier.
    pub fn open_dataset_by_identifier(&mut self, identifier: u64) -> Result<DataSet> {
        let key = self.find_key_by_identifier(identifier)?;
        self.open_dataset(&key.name, &key.dataset_type)
    }

    /// Drop (delete) an entire dataset by name and type.
    pub fn drop_dataset(&mut self, name: &str, dataset_type: &str) -> Result<()> {
        self.ensure_writable("drop dataset")?;
        if name == JOURNAL_DATASET_NAME && dataset_type == JOURNAL_DATASET_TYPE {
            return Err(TmslError::InvalidData("journal dataset is reserved".into()));
        }
        validate_dataset_path_components(name, dataset_type)?;

        let key = DataSetKey {
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
        };

        let ds = self.open_dataset(name, dataset_type)?;
        let base_dir = ds.base_dir();
        let identifier = ds.identifier();
        let metadata = meta_values_from_file(&base_dir.join("meta"))?;
        let effective_journal = self.journal.is_enabled() && ds.enable_journal();
        if effective_journal {
            validate_create_drop_record_inputs(identifier, &key, metadata.len())?;
        }

        ds.close()?;

        DataSet::drop_dataset(&base_dir)?;
        self.identifier_to_key
            .retain(|_, existing| existing != &key);
        if effective_journal {
            self.journal.append_drop(identifier, &key, &metadata)?;
        }
        log::info!("[store] dropped dataset: {}/{}", name, dataset_type);
        Ok(())
    }

    /// Whether this Store instance resolved to read-only mode at open time.
    pub fn is_read_only(&self) -> bool {
        self.read_only
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
    /// Safe to call even when the background thread is enabled; it will
    /// be serialised with the thread via the internal `Mutex`.
    pub fn tick_background_tasks(&self) -> Result<TickResult> {
        if self.read_only {
            return Err(TmslError::InvalidData(
                "read-only store has no background tasks".into(),
            ));
        }
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
        if self.read_only {
            return Err(TmslError::InvalidData(
                "read-only store has no background tasks".into(),
            ));
        }
        let bg = self
            .bg_tasks
            .as_ref()
            .ok_or_else(|| TmslError::InvalidData("bg_tasks not initialised".into()))?;
        Ok(bg.next_delay())
    }

    /// Get all unique dataset names in the store.
    pub fn get_dataset_names(&self) -> Result<Vec<String>> {
        let mut names: Vec<String> = self
            .scan_dataset_dirs()?
            .into_iter()
            .map(|(k, _)| k.name)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        names.sort();
        Ok(names)
    }

    /// Get all dataset types for a given name.
    pub fn get_dataset_types(&self, name: &str) -> Result<Vec<String>> {
        validate_dataset_component("dataset name", name)?;
        let mut types: Vec<String> = self
            .scan_dataset_dirs()?
            .into_iter()
            .filter(|(k, _)| k.name == name)
            .map(|(k, _)| k.dataset_type)
            .collect();
        types.sort();
        Ok(types)
    }

    /// Get detailed info and state of a dataset.
    ///
    /// Returns `DataSetInspectResult` containing immutable config (`DataSetInfo`)
    /// and mutable state (`DataSetState`).
    pub fn inspect_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetInspectResult> {
        validate_dataset_path_components(name, dataset_type)?;
        let key = DataSetKey {
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
        };
        if let Some(ds_arc) = self.datasets.read().unwrap().get(&key).cloned() {
            return ds_arc.inspect();
        }

        let dir = self.dataset_dir(&key);
        let identifier = read_dataset_identifier(&dir)?;
        self.validate_identifier_within_max(identifier, &key)?;
        let ds = if self.read_only {
            DataSet::open_read_only(key.clone(), dir)?
        } else {
            DataSet::open(key.clone(), dir)?
        };
        ds.set_identifier(identifier)?;
        ds.set_runtime_context(Self::dataset_runtime_context(
            &self.block_cache,
            &self.journal,
            self.bg_tasks.as_ref().map(|bg| bg.flush_queue()),
            &self.lifecycle,
            ds.enable_journal(),
            self.read_only,
        )?)?;
        let ds_arc = Arc::new(ds);
        {
            let mut guard = self.datasets.write().unwrap();
            guard.insert(key, Arc::clone(&ds_arc));
        }
        ds_arc.inspect()
    }

    /// Close the store completely.
    pub fn close(mut self) -> Result<()> {
        // 1. Stop background tasks
        if let Some(mut bg) = self.bg_tasks.take() {
            bg.stop();
        }

        // 2. Flush and close all datasets.
        let datasets = {
            let mut guard = self.datasets.write().unwrap();
            guard.drain().map(|(_, ds_arc)| ds_arc).collect::<Vec<_>>()
        };
        for ds_arc in datasets {
            if let Err(e) = ds_arc.close() {
                log::error!("[store] close failed: {}", e);
            }
        }
        self.journal.close()?;

        Ok(())
    }

    /// Return the latest journal sequence, or None when the journal is empty.
    pub fn journal_latest_sequence(&self) -> Result<Option<i64>> {
        self.journal.latest_sequence()
    }

    /// Read one encoded journal record by sequence.
    pub fn journal_read(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>> {
        self.journal.read(sequence)
    }

    /// Query encoded journal records by inclusive sequence range.
    pub fn journal_query(
        &self,
        start_sequence: i64,
        end_sequence: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>> {
        self.journal.query(start_sequence, end_sequence)
    }

    /// Read the source dataset record referenced by a journal write/delete/append record.
    pub fn read_journal_source_record(
        &mut self,
        dataset_identifier: u64,
        index_info: JournalIndexInfo,
    ) -> Result<(i64, Vec<u8>)> {
        let dataset = self.open_dataset_by_identifier(dataset_identifier)?;
        let entry = IndexEntry::new(
            index_info.timestamp,
            index_info.block_offset,
            index_info.in_block_offset,
        );
        dataset.read_entry_at_index(&entry)
    }

    /// Open the built-in journal queue.
    pub fn open_journal_queue(&mut self) -> Result<JournalQueue> {
        self.ensure_writable("open journal queue")?;
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
