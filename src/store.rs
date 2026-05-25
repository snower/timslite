//! Store: facade that manages all datasets and background tasks.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::bg::BackgroundTasks;
use crate::config::StoreConfig;
use crate::dataset::{DataSet, DataSetKey};
use crate::error::Result;

/// Opaque handle for FFI consumers.
#[derive(Clone, Copy)]
pub struct DataSetHandle(pub u64);

/// The Store: top-level facade for managing datasets.
pub struct Store {
    data_dir: PathBuf,
    datasets: Arc<RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>>,
    config: StoreConfig,
    bg_tasks: Option<BackgroundTasks>,
    next_handle_id: u64,
    handles: HashMap<u64, DataSetKey>,
}

impl Store {
    /// Open a store at the given directory.
    pub fn open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

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
                let dataset_type = type_name.to_string();

                let key = DataSetKey {
                    name: name.clone(),
                    dataset_type: dataset_type.clone(),
                };
                let ds = DataSet::new(key.clone(), type_path.clone(), &config)?;
                log::info!("[store] loaded existing dataset: {}/{}", name, dataset_type);
                datasets.insert(key, Arc::new(Mutex::new(ds)));
            }
        }

        let datasets = Arc::new(RwLock::new(datasets));

        let mut store = Self {
            data_dir,
            datasets: Arc::clone(&datasets),
            config: config.clone(),
            bg_tasks: None,
            next_handle_id: 0,
            handles: HashMap::new(),
        };

        // Start background tasks
        store.bg_tasks = Some(BackgroundTasks::start(
            datasets,
            config.flush_interval,
            config.idle_timeout,
        ));

        Ok(store)
    }

    /// Open or create a dataset.
    pub fn open_dataset(&mut self, name: &str, dataset_type: &str) -> Result<DataSetHandle> {
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

        // Create new dataset
        let dir = self.data_dir.join(name).join(dataset_type);
        std::fs::create_dir_all(&dir)?;
        let ds = DataSet::new(key.clone(), dir, &self.config)?;

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
        if let Some(key) = self.handles.remove(&handle.0) {
            let mut guard = self.datasets.write().unwrap();
            if let Some(ds_arc) = guard.remove(&key) {
                let mut ds = ds_arc.lock().unwrap();
                ds.close()?;
            }
        }
        Ok(())
    }

    /// Get a dataset handle for internal use.
    pub fn get_dataset(&self, handle: &DataSetHandle) -> Result<Arc<Mutex<DataSet>>> {
        let key = self
            .handles
            .get(&handle.0)
            .ok_or_else(|| crate::error::TmslError::NotFound("dataset handle not found".into()))?;
        let guard = self.datasets.read().unwrap();
        let ds = guard.get(key).ok_or_else(|| {
            crate::error::TmslError::NotFound(format!("dataset {:?} not found", key))
        })?;
        Ok(Arc::clone(ds))
    }

    /// Close the store completely.
    pub fn close(mut self) -> Result<()> {
        // 1. Stop background tasks
        if let Some(mut bg) = self.bg_tasks.take() {
            bg.stop();
        }

        // 2. Flush and close all datasets
        let mut guard = self.datasets.write().unwrap();
        for (_key, ds_arc) in guard.drain() {
            let mut ds = ds_arc.lock().unwrap();
            if let Err(e) = ds.close() {
                log::error!("[store] close failed: {}", e);
            }
        }

        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort cleanup if close() wasn't called
        if let Some(mut bg) = self.bg_tasks.take() {
            bg.stop();
        }
    }
}
