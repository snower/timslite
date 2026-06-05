//! FFI interface (extern "C" API) for C and other language bindings.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong, c_uchar, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::bg::TickResult;
use crate::config::{DataSetConfigBuilder, StoreConfig};
use crate::error::TmslError;
use crate::index::segment::IndexEntry;
use crate::query::iter::QuerySource;
use crate::queue::{DatasetQueue, DatasetQueueConsumer};
use crate::store::{DataSetHandle, Store};

use std::sync::LazyLock;

// 鈹€鈹€鈹€ Queue handle registry (FFI-safe opaque handles) 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

static NEXT_QUEUE_ID: AtomicUsize = AtomicUsize::new(1);
static QUEUE_REGISTRY: LazyLock<Mutex<HashMap<usize, DatasetQueue>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static NEXT_CONSUMER_ID: AtomicUsize = AtomicUsize::new(1);
static CONSUMER_REGISTRY: LazyLock<Mutex<HashMap<usize, DatasetQueueConsumer>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// 鈹€鈹€鈹€ FFI error handling helpers 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Write an error message to a C string buffer.
pub fn write_error(buf: *mut c_char, len: usize, msg: &str) {
    if buf.is_null() || len == 0 {
        return;
    }
    let c_str = match CString::new(msg) {
        Ok(s) => s,
        Err(_) => CString::new("unknown error").unwrap(),
    };
    let bytes = c_str.as_bytes_with_nul();
    let copy_len = bytes.len().min(len - 1);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, copy_len);
        *(buf.add(copy_len)) = 0;
    }
}

/// Execute a closure and catch panics. Returns the result or -1/null on error.
macro_rules! ffi_catch_int {
    ($err_buf:expr, $err_len:expr, $body:expr) => {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| $body)) {
            Ok(Ok(val)) => val,
            Ok(Err(e)) => {
                write_error($err_buf, $err_len, &format!("{}", e));
                return -1;
            }
            Err(_) => {
                write_error($err_buf, $err_len, "internal panic");
                return -1;
            }
        }
    };
}

macro_rules! ffi_catch_ptr {
    ($err_buf:expr, $err_len:expr, $body:expr) => {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| $body)) {
            Ok(Ok(val)) => val,
            Ok(Err(e)) => {
                write_error($err_buf, $err_len, &format!("{}", e));
                return std::ptr::null_mut();
            }
            Err(_) => {
                write_error($err_buf, $err_len, "internal panic");
                return std::ptr::null_mut();
            }
        }
    };
}

/// Like ffi_catch_int but returns 0 (instead of -1) on error 鈥?for usize handles.
macro_rules! ffi_catch_usize {
    ($err_buf:expr, $err_len:expr, $body:expr) => {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| $body)) {
            Ok(Ok(val)) => val,
            Ok(Err(e)) => {
                write_error($err_buf, $err_len, &format!("{}", e));
                return 0;
            }
            Err(_) => {
                write_error($err_buf, $err_len, "internal panic");
                return 0;
            }
        }
    };
}

// 鈹€鈹€鈹€ Opaque handle types 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

pub const TMSL_STORE_CONFIG_FFI_VERSION: u32 = 3;
pub const TMSL_DATASET_CONFIG_FFI_VERSION: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TmslStoreConfigFFI {
    pub version: u32,
    pub flush_interval_ms: u64,
    pub idle_timeout_ms: u64,
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    pub cache_max_memory: u64,
    pub cache_idle_timeout_ms: u64,
    pub compress_level: u8,
    pub retention_check_hour: u8,
    pub enable_background_thread: u8,
    pub enable_journal: u8,
}

impl Default for TmslStoreConfigFFI {
    fn default() -> Self {
        store_config_to_ffi(&StoreConfig::default())
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TmslDatasetConfigFFI {
    pub version: u32,
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    pub retention_window: u64,
    pub compress_level: u8,
    pub index_continuous: u8,
}

struct FfiStoreState {
    child_handles: AtomicUsize,
}

impl FfiStoreState {
    fn new() -> Self {
        Self {
            child_handles: AtomicUsize::new(0),
        }
    }
}

struct FfiStore {
    inner: *mut Store,
    state: Arc<FfiStoreState>,
}
unsafe impl Send for FfiStore {}
unsafe impl Sync for FfiStore {}

struct FfiDataset {
    store_ptr: *mut Store,
    handle: DataSetHandle,
    state: Arc<FfiStoreState>,
    iterator_count: Arc<AtomicUsize>,
}
unsafe impl Send for FfiDataset {}
unsafe impl Sync for FfiDataset {}

struct FfiIterator {
    store_ptr: *mut Store,
    handle: DataSetHandle,
    state: Arc<FfiStoreState>,
    dataset_iterator_count: Arc<AtomicUsize>,
    sources: Vec<QuerySource>,
    current_source: usize,
}
unsafe impl Send for FfiIterator {}
unsafe impl Sync for FfiIterator {}

fn store_config_to_ffi(config: &StoreConfig) -> TmslStoreConfigFFI {
    TmslStoreConfigFFI {
        version: TMSL_STORE_CONFIG_FFI_VERSION,
        flush_interval_ms: config.flush_interval.as_millis() as u64,
        idle_timeout_ms: config.idle_timeout.as_millis() as u64,
        data_segment_size: config.data_segment_size,
        index_segment_size: config.index_segment_size,
        initial_data_segment_size: config.initial_data_segment_size,
        initial_index_segment_size: config.initial_index_segment_size,
        cache_max_memory: config.cache_max_memory as u64,
        cache_idle_timeout_ms: config.cache_idle_timeout.as_millis() as u64,
        compress_level: config.compress_level,
        retention_check_hour: config.retention_check_hour,
        enable_background_thread: u8::from(config.enable_background_thread),
        enable_journal: u8::from(config.enable_journal),
    }
}

fn store_config_from_ffi(
    config_ptr: *const TmslStoreConfigFFI,
) -> crate::error::Result<StoreConfig> {
    if config_ptr.is_null() {
        return Ok(StoreConfig::default());
    }
    let raw = unsafe { *config_ptr };
    if raw.version != TMSL_STORE_CONFIG_FFI_VERSION {
        return Err(TmslError::InvalidData(format!(
            "unsupported store config version {}",
            raw.version
        )));
    }
    let cache_max_memory = usize::try_from(raw.cache_max_memory)
        .map_err(|_| TmslError::InvalidData("cache_max_memory is too large".into()))?;
    Ok(StoreConfig::builder()
        .flush_interval(Duration::from_millis(raw.flush_interval_ms))
        .idle_timeout(Duration::from_millis(raw.idle_timeout_ms))
        .data_segment_size(raw.data_segment_size)
        .index_segment_size(raw.index_segment_size)
        .initial_data_segment_size(raw.initial_data_segment_size)
        .initial_index_segment_size(raw.initial_index_segment_size)
        .compress_level(raw.compress_level)
        .cache_max_memory(cache_max_memory)
        .cache_idle_timeout(Duration::from_millis(raw.cache_idle_timeout_ms))
        .retention_check_hour(raw.retention_check_hour)
        .enable_background_thread(raw.enable_background_thread != 0)
        .enable_journal(raw.enable_journal != 0)
        .build())
}

fn dataset_config_from_ffi(
    store_config: &StoreConfig,
    config_ptr: *const TmslDatasetConfigFFI,
) -> crate::error::Result<DataSetConfigBuilder> {
    if config_ptr.is_null() {
        return Err(TmslError::InvalidData("dataset config is null".into()));
    }
    let raw = unsafe { *config_ptr };
    if raw.version != TMSL_DATASET_CONFIG_FFI_VERSION {
        return Err(TmslError::InvalidData(format!(
            "unsupported dataset config version {}",
            raw.version
        )));
    }
    Ok(DataSetConfigBuilder::from_store(store_config)
        .data_segment_size(raw.data_segment_size)
        .index_segment_size(raw.index_segment_size)
        .initial_data_segment_size(raw.initial_data_segment_size)
        .initial_index_segment_size(raw.initial_index_segment_size)
        .compress_level(raw.compress_level)
        .index_continuous(raw.index_continuous)
        .retention_window(raw.retention_window))
}

fn register_dataset_child(ffi_store: &FfiStore, handle: DataSetHandle) -> Box<FfiDataset> {
    ffi_store.state.child_handles.fetch_add(1, Ordering::SeqCst);
    Box::new(FfiDataset {
        store_ptr: ffi_store.inner,
        handle,
        state: Arc::clone(&ffi_store.state),
        iterator_count: Arc::new(AtomicUsize::new(0)),
    })
}

fn next_iter_index_entry(iter: &mut FfiIterator) -> crate::error::Result<Option<IndexEntry>> {
    while iter.current_source < iter.sources.len() {
        match iter.sources[iter.current_source].next_entry()? {
            Some(entry) if entry.block_offset == crate::index::segment::BLOCK_OFFSET_FILLER => {
                continue;
            }
            Some(entry) => return Ok(Some(entry)),
            None => iter.current_source += 1,
        }
    }
    Ok(None)
}

// 鈹€鈹€鈹€ Store Management 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Write default store config to `out_config`.
#[no_mangle]
pub extern "C" fn tmsl_store_config_default(
    out_config: *mut TmslStoreConfigFFI,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if out_config.is_null() {
            return Err(TmslError::InvalidData("out_config is null".into()));
        }
        unsafe {
            *out_config = TmslStoreConfigFFI::default();
        }
        Ok(0)
    })
}

/// Open a store. Returns opaque pointer or NULL on error.
#[no_mangle]
pub extern "C" fn tmsl_store_open(
    data_dir: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    tmsl_store_open_with_config(data_dir, std::ptr::null(), err_buf, err_buf_len)
}

/// Open a store with explicit FFI config.
#[no_mangle]
pub extern "C" fn tmsl_store_open_with_config(
    data_dir: *const c_char,
    config_ptr: *const TmslStoreConfigFFI,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    ffi_catch_ptr!(err_buf, err_buf_len, {
        if data_dir.is_null() {
            return Err(TmslError::InvalidData("data_dir is null".into()));
        }
        let dir = unsafe { CStr::from_ptr(data_dir) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid UTF-8: {}", e)))?;
        let config = store_config_from_ffi(config_ptr)?;
        let store = Store::open(dir, config)
            .map_err(|e| TmslError::Io(std::io::Error::other(e.to_string())))?;
        let boxed = Box::new(FfiStore {
            inner: Box::into_raw(Box::new(store)),
            state: Arc::new(FfiStoreState::new()),
        });
        Ok(Box::into_raw(boxed) as *mut c_void)
    })
}

/// Close a store.
#[no_mangle]
pub extern "C" fn tmsl_store_close(
    store: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if store.is_null() {
            return Err(TmslError::InvalidData("store is null".into()));
        }
        let ffi_store_ref = unsafe { &*(store as *const FfiStore) };
        let child_handles = ffi_store_ref.state.child_handles.load(Ordering::SeqCst);
        if child_handles != 0 {
            return Err(TmslError::InvalidData(format!(
                "store has {} outstanding child handle(s)",
                child_handles
            )));
        }
        let ffi_store = unsafe { Box::from_raw(store as *mut FfiStore) };
        let inner = unsafe { Box::from_raw(ffi_store.inner) };
        inner
            .close()
            .map_err(|e| TmslError::Io(std::io::Error::other(e.to_string())))?;
        Ok(0)
    })
}

/// Execute one tick of background tasks synchronously.
///
/// Writes the number of executed tasks (0-4) to `out_executed` and the delay
/// in milliseconds until the next task is due to `out_next_delay_ms`.
/// Returns 0 on success, -1 on error.
///
/// Can be called regardless of whether the background thread is enabled.
#[no_mangle]
pub extern "C" fn tmsl_store_tick_background_tasks(
    store: *mut c_void,
    out_executed: *mut u32,
    out_next_delay_ms: *mut u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if store.is_null() || out_executed.is_null() || out_next_delay_ms.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let store_inner = unsafe { &mut *(ffi_store.inner) };
        let TickResult {
            executed_tasks,
            next_delay,
        } = store_inner
            .tick_background_tasks()
            .map_err(|e| TmslError::Io(std::io::Error::other(e.to_string())))?;
        unsafe {
            *out_executed = executed_tasks as u32;
            *out_next_delay_ms = next_delay.as_millis() as u64;
        }
        Ok(0)
    })
}

/// Query the delay until the next background task is due, without executing.
///
/// Writes the delay in milliseconds to `out_next_delay_ms`.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn tmsl_store_next_background_delay(
    store: *mut c_void,
    out_next_delay_ms: *mut u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if store.is_null() || out_next_delay_ms.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let store_inner = unsafe { &mut *(ffi_store.inner) };
        let delay = store_inner
            .next_background_delay()
            .map_err(|e| TmslError::Io(std::io::Error::other(e.to_string())))?;
        unsafe {
            *out_next_delay_ms = delay.as_millis() as u64;
        }
        Ok(0)
    })
}

// 鈹€鈹€鈹€ Dataset Management 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Create a new dataset (explicit, errors if already exists).
#[no_mangle]
pub extern "C" fn tmsl_dataset_create(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    data_segment_size: u64,
    index_segment_size: u64,
    compress_level: c_uchar,
    index_continuous: c_uchar,
    retention_window: u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    ffi_catch_ptr!(err_buf, err_buf_len, {
        if store.is_null() || name.is_null() || dataset_type.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &mut *(store as *mut FfiStore) };
        let store_inner = unsafe { &mut *(ffi_store.inner) };
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid name: {}", e)))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid type: {}", e)))?;

        let handle = store_inner.create_dataset(
            name_str,
            type_str,
            data_segment_size,
            index_segment_size,
            compress_level,
            index_continuous,
            retention_window,
        )?;
        let boxed = register_dataset_child(ffi_store, handle);
        Ok(Box::into_raw(boxed) as *mut c_void)
    })
}

/// Create a new dataset with explicit FFI dataset config.
#[no_mangle]
pub extern "C" fn tmsl_dataset_create_with_config(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    config_ptr: *const TmslDatasetConfigFFI,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    ffi_catch_ptr!(err_buf, err_buf_len, {
        if store.is_null() || name.is_null() || dataset_type.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &mut *(store as *mut FfiStore) };
        let store_inner = unsafe { &mut *(ffi_store.inner) };
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid name: {}", e)))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid type: {}", e)))?;
        let dataset_config = dataset_config_from_ffi(store_inner.config(), config_ptr)?;

        let handle =
            store_inner.create_dataset_with_config(name_str, type_str, Some(dataset_config))?;
        let boxed = register_dataset_child(ffi_store, handle);
        Ok(Box::into_raw(boxed) as *mut c_void)
    })
}

/// Open a dataset (reads config from meta file).
#[no_mangle]
pub extern "C" fn tmsl_dataset_open(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    ffi_catch_ptr!(err_buf, err_buf_len, {
        if store.is_null() || name.is_null() || dataset_type.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &mut *(store as *mut FfiStore) };
        let store_inner = unsafe { &mut *(ffi_store.inner) };
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid name: {}", e)))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid type: {}", e)))?;

        let handle = store_inner.open_dataset(name_str, type_str)?;
        let boxed = register_dataset_child(ffi_store, handle);
        Ok(Box::into_raw(boxed) as *mut c_void)
    })
}

/// Close a dataset.
#[no_mangle]
pub extern "C" fn tmsl_dataset_close(
    dataset: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() {
            return Err(TmslError::InvalidData("dataset is null".into()));
        }
        let ffi_ds_ref = unsafe { &*(dataset as *const FfiDataset) };
        let iterator_count = ffi_ds_ref.iterator_count.load(Ordering::SeqCst);
        if iterator_count != 0 {
            return Err(TmslError::InvalidData(format!(
                "dataset has {} outstanding iterator handle(s)",
                iterator_count
            )));
        }
        let store_inner = unsafe { &mut *(ffi_ds_ref.store_ptr) };
        store_inner
            .close_dataset(ffi_ds_ref.handle)
            .map_err(|e| TmslError::Io(std::io::Error::other(e.to_string())))?;
        let ffi_ds = unsafe { Box::from_raw(dataset as *mut FfiDataset) };
        ffi_ds.state.child_handles.fetch_sub(1, Ordering::SeqCst);
        Ok(0)
    })
}

/// Drop (delete) an entire dataset. Destroys all data.
#[no_mangle]
pub extern "C" fn tmsl_dataset_drop(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if store.is_null() || name.is_null() || dataset_type.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &mut *(store as *mut FfiStore) };
        let child_handles = ffi_store.state.child_handles.load(Ordering::SeqCst);
        if child_handles != 0 {
            return Err(TmslError::InvalidData(format!(
                "cannot drop dataset with {} outstanding child handle(s)",
                child_handles
            )));
        }
        let store_inner = unsafe { &mut *(ffi_store.inner) };
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid name: {}", e)))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid type: {}", e)))?;

        store_inner.drop_dataset_by_name(name_str, type_str)?;
        Ok(0)
    })
}

/// Flush a dataset.
#[no_mangle]
pub extern "C" fn tmsl_dataset_flush(
    dataset: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() {
            return Err(TmslError::InvalidData("dataset is null".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let store_inner = unsafe { &mut *(ffi_ds.store_ptr) };
        let ds_arc = store_inner.get_dataset(&ffi_ds.handle)?;
        let mut ds = ds_arc.lock().unwrap();
        ds.flush()?;
        Ok(0)
    })
}

/// Get the latest successfully written timestamp of a dataset.
///
/// Writes the timestamp to `out_ts`. Returns 0. When the dataset is empty,
/// `out_ts` is set to 0. Returns -1 on error.
#[no_mangle]
pub extern "C" fn tmsl_dataset_latest_timestamp(
    dataset: *mut c_void,
    out_ts: *mut c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() || out_ts.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let store_inner = unsafe { &mut *(ffi_ds.store_ptr) };
        let ds_arc = store_inner.get_dataset(&ffi_ds.handle)?;
        let ds = ds_arc.lock().unwrap();
        unsafe { *out_ts = ds.latest_written_timestamp() as c_longlong };
        Ok(0)
    })
}

/// Write a record to the dataset.
#[no_mangle]
pub extern "C" fn tmsl_dataset_write(
    dataset: *mut c_void,
    timestamp: c_longlong,
    data: *const c_uchar,
    data_len: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() || (data.is_null() && data_len > 0) {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let data_slice = if data_len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(data, data_len) }
        };
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let store_inner = unsafe { &mut *(ffi_ds.store_ptr) };
        store_inner.write_dataset(ffi_ds.handle, timestamp, data_slice)?;
        Ok(0)
    })
}

/// Append bytes to the dataset record.
#[no_mangle]
pub extern "C" fn tmsl_dataset_append(
    dataset: *mut c_void,
    timestamp: c_longlong,
    data: *const c_uchar,
    data_len: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() || (data.is_null() && data_len > 0) {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let data_slice = if data_len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(data, data_len) }
        };
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let store_inner = unsafe { &mut *(ffi_ds.store_ptr) };
        store_inner.append_dataset(ffi_ds.handle, timestamp, data_slice)?;
        Ok(0)
    })
}

/// Delete the record at the given timestamp.
///
/// Marks the index entry as sentinel and increments the old data segment's
/// invalid_record_count. Returns -1 if no real data exists at that timestamp.
#[no_mangle]
pub extern "C" fn tmsl_dataset_delete(
    dataset: *mut c_void,
    timestamp: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() {
            return Err(TmslError::InvalidData("dataset is null".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let store_inner = unsafe { &mut *(ffi_ds.store_ptr) };
        store_inner.delete_dataset_record(ffi_ds.handle, timestamp)?;
        Ok(0)
    })
}

/// Read a single record by exact timestamp.
///
/// On success (record found): allocates `out_data` via `libc::malloc`, sets
/// `out_ts` and `out_data_len`, returns 0. Caller must free via `tmsl_data_free`.
///
/// Returns: 0 = success, 1 = not found (or filler/deleted), -1 = error.
#[no_mangle]
pub extern "C" fn tmsl_dataset_read(
    dataset: *mut c_void,
    timestamp: c_longlong,
    out_ts: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() || out_ts.is_null() || out_data.is_null() || out_data_len.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let store_inner = unsafe { &mut *(ffi_ds.store_ptr) };
        let ds_arc = store_inner.get_dataset(&ffi_ds.handle)?;
        let mut ds = ds_arc.lock().unwrap();
        match ds.read(timestamp)? {
            Some((ts, data)) => {
                unsafe { *out_ts = ts as c_longlong };
                let ptr = unsafe { libc::malloc(data.len()) as *mut c_uchar };
                if ptr.is_null() {
                    return Err(TmslError::InvalidData("malloc failed".into()));
                }
                unsafe {
                    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
                    *out_data = ptr;
                    *out_data_len = data.len();
                }
                Ok(0)
            }
            None => Ok(1),
        }
    })
}

/// Close and free the iterator.
#[no_mangle]
pub extern "C" fn tmsl_iter_close(iter: *mut c_void) {
    if !iter.is_null() {
        let ffi_iter = unsafe { Box::from_raw(iter as *mut FfiIterator) };
        ffi_iter
            .dataset_iterator_count
            .fetch_sub(1, Ordering::SeqCst);
        ffi_iter.state.child_handles.fetch_sub(1, Ordering::SeqCst);
    }
}

// 鈹€鈹€鈹€ Query Iterator 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Query a time range. Returns iterator pointer or NULL on error.
#[no_mangle]
pub extern "C" fn tmsl_dataset_query(
    dataset: *mut c_void,
    start_ts: c_longlong,
    end_ts: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    ffi_catch_ptr!(err_buf, err_buf_len, {
        if dataset.is_null() {
            return Err(TmslError::InvalidData("dataset is null".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let store_inner = unsafe { &mut *(ffi_ds.store_ptr) };
        let ds_arc = store_inner.get_dataset(&ffi_ds.handle)?;
        let mut ds = ds_arc.lock().unwrap();
        let sources = ds.query_sources(start_ts, end_ts)?;

        ffi_ds.iterator_count.fetch_add(1, Ordering::SeqCst);
        ffi_ds.state.child_handles.fetch_add(1, Ordering::SeqCst);

        let iter = Box::new(FfiIterator {
            store_ptr: ffi_ds.store_ptr,
            handle: ffi_ds.handle,
            state: Arc::clone(&ffi_ds.state),
            dataset_iterator_count: Arc::clone(&ffi_ds.iterator_count),
            sources,
            current_source: 0,
        });
        Ok(Box::into_raw(iter) as *mut c_void)
    })
}

/// Get the next record from the iterator.
/// Returns: 0 = success, 1 = no more data, -1 = error.
#[no_mangle]
pub extern "C" fn tmsl_iter_next(
    iter: *mut c_void,
    out_ts: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if iter.is_null() || out_ts.is_null() || out_data.is_null() || out_data_len.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_iter = unsafe { &mut *(iter as *mut FfiIterator) };
        let entry = match next_iter_index_entry(ffi_iter)? {
            Some(entry) => entry,
            None => return Ok(1),
        };

        // Lazy read: get dataset, read single entry
        let store_inner = unsafe { &mut *(ffi_iter.store_ptr) };
        let ds_arc = store_inner.get_dataset(&ffi_iter.handle)?;
        let mut ds = ds_arc.lock().unwrap();
        let (ts, data) = ds.read_entry_at_index(&entry)?;

        unsafe { *out_ts = ts as c_longlong };

        let ptr = unsafe { libc::malloc(data.len()) as *mut c_uchar };
        if ptr.is_null() {
            return Err(TmslError::InvalidData("malloc failed".into()));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
            *out_data = ptr;
            *out_data_len = data.len();
        }

        Ok(0)
    })
}

/// Free data allocated by FFI read/query APIs.
#[no_mangle]
pub extern "C" fn tmsl_data_free(data: *mut c_void) {
    if !data.is_null() {
        unsafe { libc::free(data) };
    }
}

/// Free data allocated by tmsl_iter_next.
#[no_mangle]
pub extern "C" fn tmsl_iter_free_data(data: *mut c_uchar) {
    tmsl_data_free(data as *mut c_void);
}

// 鈹€鈹€鈹€ Queue FFI functions 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Open the queue subsystem for a dataset.
///
/// Returns an opaque queue handle (usize) that must be passed to other
/// queue functions. Returns 0 on failure (error written to err_buf).
#[no_mangle]
pub extern "C" fn tmsl_queue_open(
    store: *mut Store,
    dataset_handle: u64,
    err_buf: *mut c_char,
    err_len: usize,
) -> usize {
    ffi_catch_usize! { err_buf, err_len, {
        let store = unsafe { &mut *store };
        let handle = DataSetHandle(dataset_handle);
        let queue = store.open_queue(handle)?;
        let id = NEXT_QUEUE_ID.fetch_add(1, Ordering::Relaxed);
        QUEUE_REGISTRY.lock().unwrap().insert(id, queue);
        Ok::<usize, TmslError>(id)
    }}
}

/// Close the queue subsystem for a dataset.
///
/// Invalidates all associated consumer handles.
#[no_mangle]
pub extern "C" fn tmsl_queue_close(
    store: *mut Store,
    queue_handle: usize,
    err_buf: *mut c_char,
    err_len: usize,
) -> c_int {
    ffi_catch_int! { err_buf, err_len, {
        let store = unsafe { &mut *store };
        let registry = QUEUE_REGISTRY.lock().unwrap();
        let _queue = registry.get(&queue_handle).ok_or_else(|| {
            TmslError::NotFound("queue handle not found".into())
        })?;
        drop(registry);

        let handle = DataSetHandle(0);
        store.close_queue(handle).map_err(|_| {
            TmslError::NotFound("queue close failed".into())
        })?;
        QUEUE_REGISTRY.lock().unwrap().remove(&queue_handle);
        Ok::<c_int, TmslError>(0)
    }}
}

/// Open or create a consumer group for a queue.
///
/// Returns an opaque consumer handle (usize).
#[no_mangle]
pub extern "C" fn tmsl_queue_consumer_open(
    queue_handle: usize,
    group_name: *const c_char,
    err_buf: *mut c_char,
    err_len: usize,
) -> usize {
    ffi_catch_usize! { err_buf, err_len, {
        let group_name = unsafe { CStr::from_ptr(group_name) }
            .to_str()
            .map_err(|_| TmslError::InvalidData("invalid group name encoding".into()))?;
        let queue = QUEUE_REGISTRY
            .lock()
            .unwrap()
            .get(&queue_handle)
            .cloned()
            .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?;
        let consumer = queue.open_consumer(group_name)?;
        let id = NEXT_CONSUMER_ID.fetch_add(1, Ordering::Relaxed);
        CONSUMER_REGISTRY.lock().unwrap().insert(id, consumer);
        Ok::<usize, TmslError>(id)
    }}
}

/// Drop (close and remove) a consumer group.
#[no_mangle]
pub extern "C" fn tmsl_queue_consumer_drop(
    queue_handle: usize,
    consumer_handle: usize,
    err_buf: *mut c_char,
    err_len: usize,
) -> c_int {
    ffi_catch_int! { err_buf, err_len, {
        let _queue = QUEUE_REGISTRY
            .lock()
            .unwrap()
            .get(&queue_handle)
            .cloned()
            .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?;
        CONSUMER_REGISTRY.lock().unwrap().remove(&consumer_handle);
        Ok::<c_int, TmslError>(0)
    }}
}

/// Push data into the queue.
///
/// Auto-increments timestamp and notifies waiting consumers.
/// Returns the assigned timestamp (> 0) on success, 0 on failure.
#[no_mangle]
pub extern "C" fn tmsl_queue_push(
    queue_handle: usize,
    data: *const c_uchar,
    data_len: usize,
    err_buf: *mut c_char,
    err_len: usize,
) -> c_longlong {
    ffi_catch_int! { err_buf, err_len, {
        if data.is_null() || data_len == 0 {
            return Err(TmslError::InvalidData("data pointer is null or empty".into()));
        }
        let queue = QUEUE_REGISTRY
            .lock()
            .unwrap()
            .get(&queue_handle)
            .cloned()
            .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?;
        let slice = unsafe { std::slice::from_raw_parts(data, data_len) };
        let ts = queue.push(slice)?;
        Ok::<c_longlong, TmslError>(ts as c_longlong)
    }}
}

/// Poll for the next record from a consumer.
///
/// Returns 0 on success (with data), -1 on error, -2 on timeout.
/// On success: *out_timestamp and *out_data are set.
#[no_mangle]
pub extern "C" fn tmsl_queue_poll(
    consumer_handle: usize,
    timeout_ms: c_longlong,
    out_timestamp: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_len: *mut usize,
    err_buf: *mut c_char,
    err_len: usize,
) -> c_int {
    ffi_catch_int! { err_buf, err_len, {
        let consumer = CONSUMER_REGISTRY
            .lock()
            .unwrap()
            .get(&consumer_handle)
            .cloned()
            .ok_or_else(|| TmslError::NotFound("consumer handle not found".into()))?;
        let timeout = Duration::from_millis(std::cmp::max(0, timeout_ms) as u64);

        match consumer.poll(timeout)? {
            Some((ts, data)) => {
                unsafe {
                    *out_timestamp = ts as c_longlong;
                    let ptr = libc::malloc(data.len()) as *mut c_uchar;
                    if ptr.is_null() {
                        return Err(TmslError::Io(std::io::Error::other(
                            "malloc failed",
                        )));
                    }
                    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
                    *out_data = ptr;
                    *out_len = data.len();
                }
                Ok(0)
            }
            None => Ok(-2), // Timeout
        }
    }}
}

/// Ack a previously polled record.
#[no_mangle]
pub extern "C" fn tmsl_queue_ack(
    consumer_handle: usize,
    timestamp: c_longlong,
    err_buf: *mut c_char,
    err_len: usize,
) -> c_int {
    ffi_catch_int! { err_buf, err_len, {
        let consumer = CONSUMER_REGISTRY
            .lock()
            .unwrap()
            .get(&consumer_handle)
            .cloned()
            .ok_or_else(|| TmslError::NotFound("consumer handle not found".into()))?;
        consumer.ack(timestamp)?;
        Ok::<c_int, TmslError>(0)
    }}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn temp_store_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn err_buf() -> ([c_char; 256], usize) {
        ([0; 256], 256)
    }

    #[test]
    fn test_store_open_with_config_and_child_lifecycle() {
        let dir = temp_store_dir("timslite_ffi_store_config_lifecycle");
        let dir_c = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
        let (mut err, err_len) = err_buf();
        let mut config = TmslStoreConfigFFI::default();
        assert_eq!(
            tmsl_store_config_default(&mut config, err.as_mut_ptr(), err_len),
            0
        );
        config.enable_background_thread = 0;

        let store = tmsl_store_open_with_config(dir_c.as_ptr(), &config, err.as_mut_ptr(), err_len);
        assert!(!store.is_null());

        let name = CString::new("sensor").unwrap();
        let ty = CString::new("wave").unwrap();
        let dataset = tmsl_dataset_create(
            store,
            name.as_ptr(),
            ty.as_ptr(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
            err.as_mut_ptr(),
            err_len,
        );
        assert!(!dataset.is_null());

        assert_eq!(
            tmsl_store_close(store, err.as_mut_ptr(), err_len),
            -1,
            "store close must reject outstanding dataset handles"
        );
        assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err_len), 0);
        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
    }

    #[test]
    fn test_dataset_create_with_config_and_data_free() {
        let dir = temp_store_dir("timslite_ffi_dataset_config");
        let dir_c = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
        let (mut err, err_len) = err_buf();
        let mut store_config = TmslStoreConfigFFI::default();
        assert_eq!(
            tmsl_store_config_default(&mut store_config, err.as_mut_ptr(), err_len),
            0
        );
        store_config.enable_background_thread = 0;

        let store =
            tmsl_store_open_with_config(dir_c.as_ptr(), &store_config, err.as_mut_ptr(), err_len);
        assert!(!store.is_null());

        let dataset_config = TmslDatasetConfigFFI {
            version: TMSL_DATASET_CONFIG_FFI_VERSION,
            data_segment_size: 1024 * 1024,
            index_segment_size: 64 * 1024,
            initial_data_segment_size: 8 * 1024,
            initial_index_segment_size: 4 * 1024,
            retention_window: 0,
            compress_level: 6,
            index_continuous: 0,
        };
        let name = CString::new("sensor").unwrap();
        let ty = CString::new("wave").unwrap();
        let dataset = tmsl_dataset_create_with_config(
            store,
            name.as_ptr(),
            ty.as_ptr(),
            &dataset_config,
            err.as_mut_ptr(),
            err_len,
        );
        assert!(!dataset.is_null());

        let payload = [1u8, 2, 3, 4];
        assert_eq!(
            tmsl_dataset_write(
                dataset,
                100,
                payload.as_ptr(),
                payload.len(),
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );
        let appended = [5u8, 6];
        assert_eq!(
            tmsl_dataset_append(
                dataset,
                100,
                appended.as_ptr(),
                appended.len(),
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );

        let mut out_ts: c_longlong = 0;
        let mut out_data: *mut c_uchar = std::ptr::null_mut();
        let mut out_len: usize = 0;
        assert_eq!(
            tmsl_dataset_read(
                dataset,
                100,
                &mut out_ts,
                &mut out_data,
                &mut out_len,
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );
        assert_eq!(out_ts, 100);
        assert_eq!(out_len, payload.len() + appended.len());
        assert!(!out_data.is_null());
        let out_slice = unsafe { std::slice::from_raw_parts(out_data, out_len) };
        assert_eq!(out_slice, &[1, 2, 3, 4, 5, 6]);
        tmsl_data_free(out_data as *mut c_void);

        assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err_len), 0);
        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
    }
}
