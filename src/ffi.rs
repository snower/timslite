//! FFI interface (extern "C" API) for C and other language bindings.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong, c_uchar, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::bg::TickResult;
use crate::compress::validate_compress_type;
use crate::config::{validate_retention_window, DataSetConfigBuilder, StoreConfig};
use crate::error::TmslError;
use crate::index::segment::IndexEntry;
use crate::journal::{JournalQueue, JournalQueueConsumer};
use crate::queue::{DatasetQueue, DatasetQueueConsumer};
use crate::store::{DataSetHandle, Store};

use std::sync::LazyLock;

static NEXT_QUEUE_ID: AtomicUsize = AtomicUsize::new(1);
static QUEUE_REGISTRY: LazyLock<Mutex<HashMap<usize, FfiQueueEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static NEXT_CONSUMER_ID: AtomicUsize = AtomicUsize::new(1);
static CONSUMER_REGISTRY: LazyLock<Mutex<HashMap<usize, FfiConsumerEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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

pub const TMSL_STORE_CONFIG_FFI_VERSION: u32 = 4;
pub const TMSL_DATASET_CONFIG_FFI_VERSION: u32 = 3;

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
    pub compress_type: u8,
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
    pub compress_type: u8,
    pub index_continuous: u8,
    pub enable_journal: u8,
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
    inner: Arc<Mutex<Store>>,
    state: Arc<FfiStoreState>,
}

struct FfiDataset {
    store: Arc<Mutex<Store>>,
    handle: DataSetHandle,
    state: Arc<FfiStoreState>,
    iterator_count: Arc<AtomicUsize>,
}

struct FfiIterator {
    store: Arc<Mutex<Store>>,
    handle: DataSetHandle,
    state: Arc<FfiStoreState>,
    dataset_iterator_count: Arc<AtomicUsize>,
    entries: Vec<IndexEntry>,
    position: usize,
}

struct FfiJournalIterator {
    state: Arc<FfiStoreState>,
    entries: Vec<(i64, Vec<u8>)>,
    position: usize,
}

#[derive(Clone)]
enum FfiQueueKind {
    Dataset {
        store: Arc<Mutex<Store>>,
        handle: DataSetHandle,
        queue: DatasetQueue,
    },
    Journal {
        queue: JournalQueue,
    },
}

struct FfiQueueEntry {
    kind: FfiQueueKind,
    state: Arc<FfiStoreState>,
}

#[derive(Clone)]
enum FfiConsumerKind {
    Dataset(DatasetQueueConsumer),
    Journal(JournalQueueConsumer),
}

struct FfiConsumerEntry {
    queue_handle: usize,
    group_name: String,
    consumer: FfiConsumerKind,
    state: Arc<FfiStoreState>,
}

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
        compress_type: config.compress_type,
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
    validate_compress_type(raw.compress_type)?;
    Ok(StoreConfig::builder()
        .flush_interval(Duration::from_millis(raw.flush_interval_ms))
        .idle_timeout(Duration::from_millis(raw.idle_timeout_ms))
        .data_segment_size(raw.data_segment_size)
        .index_segment_size(raw.index_segment_size)
        .initial_data_segment_size(raw.initial_data_segment_size)
        .initial_index_segment_size(raw.initial_index_segment_size)
        .compress_level(raw.compress_level)
        .compress_type(raw.compress_type)
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
    validate_compress_type(raw.compress_type)?;
    validate_retention_window(raw.retention_window)?;
    Ok(DataSetConfigBuilder::from_store(store_config)
        .data_segment_size(raw.data_segment_size)
        .index_segment_size(raw.index_segment_size)
        .initial_data_segment_size(raw.initial_data_segment_size)
        .initial_index_segment_size(raw.initial_index_segment_size)
        .compress_level(raw.compress_level)
        .compress_type(raw.compress_type)
        .index_continuous(raw.index_continuous)
        .enable_journal(raw.enable_journal != 0)
        .retention_window(raw.retention_window))
}

fn register_dataset_child(ffi_store: &FfiStore, handle: DataSetHandle) -> Box<FfiDataset> {
    ffi_store.state.child_handles.fetch_add(1, Ordering::SeqCst);
    Box::new(FfiDataset {
        store: Arc::clone(&ffi_store.inner),
        handle,
        state: Arc::clone(&ffi_store.state),
        iterator_count: Arc::new(AtomicUsize::new(0)),
    })
}

fn dataset_has_queue_handle(handle: DataSetHandle) -> crate::error::Result<bool> {
    let registry = QUEUE_REGISTRY
        .lock()
        .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?;
    Ok(registry.values().any(|entry| {
        matches!(
            &entry.kind,
            FfiQueueKind::Dataset { handle: queue_handle, .. } if queue_handle.0 == handle.0
        )
    }))
}

fn alloc_bytes(data: &[u8]) -> crate::error::Result<*mut c_uchar> {
    let len = data.len().max(1);
    let ptr = unsafe { libc::malloc(len) as *mut c_uchar };
    if ptr.is_null() {
        return Err(TmslError::InvalidData("malloc failed".into()));
    }
    if !data.is_empty() {
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
        }
    }
    Ok(ptr)
}

fn remove_consumers_for_queue(queue_handle: usize) -> crate::error::Result<usize> {
    let mut consumers = CONSUMER_REGISTRY
        .lock()
        .map_err(|_| TmslError::InvalidData("consumer registry mutex poisoned".into()))?;
    let ids: Vec<usize> = consumers
        .iter()
        .filter_map(|(id, entry)| (entry.queue_handle == queue_handle).then_some(*id))
        .collect();
    let count = ids.len();
    for id in ids {
        consumers.remove(&id);
    }
    Ok(count)
}

fn remove_consumers_for_group(
    queue_handle: usize,
    group_name: &str,
) -> crate::error::Result<usize> {
    let mut consumers = CONSUMER_REGISTRY
        .lock()
        .map_err(|_| TmslError::InvalidData("consumer registry mutex poisoned".into()))?;
    let ids: Vec<usize> = consumers
        .iter()
        .filter_map(|(id, entry)| {
            (entry.queue_handle == queue_handle && entry.group_name == group_name).then_some(*id)
        })
        .collect();
    let count = ids.len();
    for id in ids {
        consumers.remove(&id);
    }
    Ok(count)
}

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
            inner: Arc::new(Mutex::new(store)),
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
        let inner = Arc::try_unwrap(ffi_store.inner)
            .map_err(|_| TmslError::InvalidData("store has outstanding references".into()))?
            .into_inner()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
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
        let store_inner = ffi_store
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
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
        let store_inner = ffi_store
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
        let delay = store_inner
            .next_background_delay()
            .map_err(|e| TmslError::Io(std::io::Error::other(e.to_string())))?;
        unsafe {
            *out_next_delay_ms = delay.as_millis() as u64;
        }
        Ok(0)
    })
}

// ─── Store dataset enumeration ────────────────────────────────────────

/// Free a malloc'd array of C strings (from tmsl_store_get_dataset_names/types).
///
/// # Safety
/// - `arr` must have been allocated by `libc::malloc` (or equivalent).
/// - Each `arr[i]` must have been allocated by `libc::malloc`.
/// - `count` must match the count returned by the function that produced `arr`.
/// - After this call, `arr` and all its contents are invalid.
#[no_mangle]
pub extern "C" fn tmsl_free_string_array(arr: *mut *mut c_char, count: u32) {
    if arr.is_null() {
        return;
    }
    let count = count as usize;
    unsafe {
        for i in 0..count {
            let s = *arr.add(i);
            if !s.is_null() {
                drop(CString::from_raw(s));
            }
        }
        drop(Vec::from_raw_parts(arr, count, count));
    }
}

/// Get all unique dataset names in the store.
///
/// On success writes a malloc'd array of malloc'd C strings to `out_names` and
/// the count to `out_count`. Caller must free with `tmsl_free_string_array`.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn tmsl_store_get_dataset_names(
    store: *mut c_void,
    out_names: *mut *mut *mut c_char,
    out_count: *mut u32,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if store.is_null() || out_names.is_null() || out_count.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let store_inner = ffi_store
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
        let names = store_inner.get_dataset_names()?;
        let count = names.len();

        // Allocate Vec<*mut c_char> and convert to raw parts
        let mut c_strings: Vec<*mut c_char> = Vec::with_capacity(count);
        for name in names {
            let c_str = CString::new(name)
                .map_err(|_| TmslError::InvalidData("dataset name contains null byte".into()))?;
            c_strings.push(c_str.into_raw());
        }
        let (ptr, _, _) = c_strings.into_raw_parts();

        unsafe {
            *out_names = ptr;
            *out_count = count as u32;
        }
        Ok(0)
    })
}

/// Get all dataset types for a given name.
///
/// On success writes a malloc'd array of malloc'd C strings to `out_types` and
/// the count to `out_count`. Caller must free with `tmsl_free_string_array`.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn tmsl_store_get_dataset_types(
    store: *mut c_void,
    name: *const c_char,
    out_types: *mut *mut *mut c_char,
    out_count: *mut u32,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if store.is_null() || name.is_null() || out_types.is_null() || out_count.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|_| TmslError::InvalidData("invalid UTF-8 in name".into()))?;
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let store_inner = ffi_store
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
        let types = store_inner.get_dataset_types(name_str)?;
        let count = types.len();

        // Allocate Vec<*mut c_char> and convert to raw parts
        let mut c_strings: Vec<*mut c_char> = Vec::with_capacity(count);
        for t in types {
            let c_str = CString::new(t)
                .map_err(|_| TmslError::InvalidData("dataset type contains null byte".into()))?;
            c_strings.push(c_str.into_raw());
        }
        let (ptr, _, _) = c_strings.into_raw_parts();

        unsafe {
            *out_types = ptr;
            *out_count = count as u32;
        }
        Ok(0)
    })
}

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
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid name: {}", e)))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid type: {}", e)))?;

        let handle = {
            let mut store_inner = ffi_store
                .inner
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.create_dataset(
                name_str,
                type_str,
                data_segment_size,
                index_segment_size,
                compress_level,
                index_continuous,
                retention_window,
            )?
        };
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
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid name: {}", e)))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid type: {}", e)))?;

        let handle = {
            let mut store_inner = ffi_store
                .inner
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            let dataset_config = dataset_config_from_ffi(store_inner.config(), config_ptr)?;
            store_inner.create_dataset_with_config(name_str, type_str, Some(dataset_config))?
        };
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
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid name: {}", e)))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid type: {}", e)))?;

        let handle = {
            let mut store_inner = ffi_store
                .inner
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.open_dataset(name_str, type_str)?
        };
        let boxed = register_dataset_child(ffi_store, handle);
        Ok(Box::into_raw(boxed) as *mut c_void)
    })
}

/// Open a dataset by its Store-assigned numeric identifier.
#[no_mangle]
pub extern "C" fn tmsl_dataset_open_by_identifier(
    store: *mut c_void,
    identifier: u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    ffi_catch_ptr!(err_buf, err_buf_len, {
        if store.is_null() {
            return Err(TmslError::InvalidData("store is null".into()));
        }
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let handle = {
            let mut store_inner = ffi_store
                .inner
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.open_dataset_by_identifier(identifier)?
        };
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
        let mut store_inner = ffi_ds_ref
            .store
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
        if dataset_has_queue_handle(ffi_ds_ref.handle)? {
            return Err(TmslError::InvalidData(
                "dataset has outstanding queue handle(s)".into(),
            ));
        }
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
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let child_handles = ffi_store.state.child_handles.load(Ordering::SeqCst);
        if child_handles != 0 {
            return Err(TmslError::InvalidData(format!(
                "cannot drop dataset with {} outstanding child handle(s)",
                child_handles
            )));
        }
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid name: {}", e)))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid type: {}", e)))?;

        let mut store_inner = ffi_store
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
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
        let ds_arc = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_ds.handle)?
        };
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
        let ds_arc = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_ds.handle)?
        };
        let ds = ds_arc.lock().unwrap();
        unsafe { *out_ts = ds.latest_written_timestamp() as c_longlong };
        Ok(0)
    })
}

/// Get the Store-assigned numeric identifier of a dataset.
#[no_mangle]
pub extern "C" fn tmsl_dataset_identifier(
    dataset: *mut c_void,
    out_identifier: *mut u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() || out_identifier.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let identifier = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.dataset_identifier(ffi_ds.handle)?
        };
        unsafe { *out_identifier = identifier };
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
        let mut store_inner = ffi_ds
            .store
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
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
        let mut store_inner = ffi_ds
            .store
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
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
        let mut store_inner = ffi_ds
            .store
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
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
        let ds_arc = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_ds.handle)?
        };
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
        let ds_arc = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_ds.handle)?
        };
        let mut ds = ds_arc.lock().unwrap();
        let entries = ds.query_index_entries(start_ts, end_ts)?;

        ffi_ds.iterator_count.fetch_add(1, Ordering::SeqCst);
        ffi_ds.state.child_handles.fetch_add(1, Ordering::SeqCst);

        let iter = Box::new(FfiIterator {
            store: Arc::clone(&ffi_ds.store),
            handle: ffi_ds.handle,
            state: Arc::clone(&ffi_ds.state),
            dataset_iterator_count: Arc::clone(&ffi_ds.iterator_count),
            entries,
            position: 0,
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
        let entry = loop {
            if ffi_iter.position >= ffi_iter.entries.len() {
                return Ok(1);
            }
            let entry = ffi_iter.entries[ffi_iter.position];
            ffi_iter.position += 1;
            if !entry.is_filler() {
                break entry;
            }
        };

        // Lazy read: get dataset, read single snapshot entry
        let ds_arc = {
            let store_inner = ffi_iter
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_iter.handle)?
        };
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

// ─── Lightweight read operations FFI ──────────────────────────────────────────

/// Check if index entry exists for a timestamp.
/// timestamp=-1 checks latest_written_timestamp.
/// Returns 0=false, 1=true, -1=error.
#[no_mangle]
pub extern "C" fn tmsl_dataset_read_exist(
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
        let ds_arc = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_ds.handle)?
        };
        let mut ds = ds_arc.lock().unwrap();
        let exists = ds.read_exist(timestamp)?;
        Ok(if exists { 1 } else { 0 })
    })
}

/// Check existence of index entries in [start_ts, end_ts].
/// Returns bitmap via out_bitmap (allocated with libc::malloc, caller frees with tmsl_data_free).
/// out_bitmap_len receives the byte count. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn tmsl_dataset_query_exist(
    dataset: *mut c_void,
    start_ts: c_longlong,
    end_ts: c_longlong,
    out_bitmap: *mut *mut c_uchar,
    out_bitmap_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() || out_bitmap.is_null() || out_bitmap_len.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let ds_arc = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_ds.handle)?
        };
        let mut ds = ds_arc.lock().unwrap();
        let bitmap = ds.query_exist(start_ts, end_ts)?;
        if bitmap.is_empty() {
            unsafe {
                *out_bitmap = std::ptr::null_mut();
                *out_bitmap_len = 0;
            }
        } else {
            let ptr = unsafe { libc::malloc(bitmap.len()) as *mut c_uchar };
            if ptr.is_null() {
                return Err(TmslError::InvalidData("malloc failed".into()));
            }
            unsafe {
                std::ptr::copy_nonoverlapping(bitmap.as_ptr(), ptr, bitmap.len());
                *out_bitmap = ptr;
                *out_bitmap_len = bitmap.len();
            }
        }
        Ok(0)
    })
}

/// Read the logical data length for a timestamp.
/// timestamp=-1 reads latest_written_timestamp.
/// Returns 0=success (out_len valid), 1=not found, -1=error.
#[no_mangle]
pub extern "C" fn tmsl_dataset_read_length(
    dataset: *mut c_void,
    timestamp: c_longlong,
    out_len: *mut u32,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() || out_len.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let ds_arc = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_ds.handle)?
        };
        let mut ds = ds_arc.lock().unwrap();
        match ds.read_length(timestamp)? {
            Some(len) => {
                unsafe { *out_len = len };
                Ok(0)
            }
            None => Ok(1),
        }
    })
}

/// Query data lengths for timestamps in [start_ts, end_ts].
/// Returns array of (timestamp: i64, data_len: u32) pairs via out_array (allocated with libc::malloc).
/// Each element is 12 bytes (8 + 4). Caller frees with tmsl_data_free.
/// out_array_len receives the element count. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn tmsl_dataset_query_length(
    dataset: *mut c_void,
    start_ts: c_longlong,
    end_ts: c_longlong,
    out_array: *mut *mut c_void,
    out_array_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if dataset.is_null() || out_array.is_null() || out_array_len.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let ds_arc = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_ds.handle)?
        };
        let mut ds = ds_arc.lock().unwrap();
        let pairs = ds.query_length(start_ts, end_ts)?;
        if pairs.is_empty() {
            unsafe {
                *out_array = std::ptr::null_mut();
                *out_array_len = 0;
            }
        } else {
            // Each element: timestamp (i64) + data_len (u32) = 12 bytes
            let elem_size = std::mem::size_of::<i64>() + std::mem::size_of::<u32>();
            let total_size = pairs.len() * elem_size;
            let ptr = unsafe { libc::malloc(total_size) as *mut u8 };
            if ptr.is_null() {
                return Err(TmslError::InvalidData("malloc failed".into()));
            }
            for (i, (ts, len)) in pairs.iter().enumerate() {
                let offset = i * elem_size;
                unsafe {
                    std::ptr::copy_nonoverlapping(ts.to_le_bytes().as_ptr(), ptr.add(offset), 8);
                    std::ptr::copy_nonoverlapping(
                        len.to_le_bytes().as_ptr(),
                        ptr.add(offset + 8),
                        4,
                    );
                }
            }
            unsafe {
                *out_array = ptr as *mut c_void;
                *out_array_len = pairs.len();
            }
        }
        Ok(0)
    })
}

/// Create a query length iterator. Returns iterator handle or NULL on error.
/// Caller must free with tmsl_iter_close.
#[no_mangle]
pub extern "C" fn tmsl_dataset_query_length_iter(
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
        let ds_arc = {
            let store_inner = ffi_ds
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store_inner.get_dataset(&ffi_ds.handle)?
        };
        let mut ds = ds_arc.lock().unwrap();
        let entries = ds.query_index_entries(start_ts, end_ts)?;
        let ffi_iter = Box::new(FfiIterator {
            store: Arc::clone(&ffi_ds.store),
            handle: ffi_ds.handle,
            state: Arc::clone(&ffi_ds.state),
            dataset_iterator_count: Arc::clone(&ffi_ds.iterator_count),
            entries,
            position: 0,
        });
        ffi_ds.iterator_count.fetch_add(1, Ordering::SeqCst);
        ffi_ds.state.child_handles.fetch_add(1, Ordering::SeqCst);
        Ok(Box::into_raw(ffi_iter) as *mut c_void)
    })
}

/// Get next data length from query length iterator.
/// Returns 0=success (out_ts, out_len valid), 1=done, -1=error.
#[no_mangle]
pub extern "C" fn tmsl_length_iter_next(
    iter: *mut c_void,
    out_ts: *mut c_longlong,
    out_len: *mut u32,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if iter.is_null() || out_ts.is_null() || out_len.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_iter = unsafe { &mut *(iter as *mut FfiIterator) };
        loop {
            if ffi_iter.position >= ffi_iter.entries.len() {
                return Ok(1);
            }
            let entry = &ffi_iter.entries[ffi_iter.position];
            ffi_iter.position += 1;
            if entry.block_offset == crate::index::segment::BLOCK_OFFSET_FILLER {
                continue;
            }
            let store = ffi_iter
                .store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            let ds_arc = store.get_dataset(&ffi_iter.handle)?;
            let mut ds = ds_arc.lock().unwrap();
            let re = crate::segment::ReadIndexEntry {
                timestamp: entry.timestamp,
                block_offset: entry.block_offset,
                in_block_offset: entry.in_block_offset,
            };
            let cache = ds.cache_ref().cloned();
            let data_len = ds
                .segments_mut()
                .read_record_data_len(&re, cache.as_deref())?;
            unsafe {
                *out_ts = entry.timestamp as c_longlong;
                *out_len = data_len;
            }
            return Ok(0);
        }
    })
}

// ─── Queue FFI functions ────────────────────────────────────────────────────

/// Open the queue subsystem for a dataset.
///
/// Returns an opaque queue handle (usize) that must be passed to other
/// queue functions. Returns 0 on failure (error written to err_buf).
#[no_mangle]
pub extern "C" fn tmsl_queue_open(
    dataset: *mut c_void,
    err_buf: *mut c_char,
    err_len: usize,
) -> usize {
    ffi_catch_usize! { err_buf, err_len, {
        if dataset.is_null() {
            return Err(TmslError::InvalidData("dataset is null".into()));
        }
        let ffi_ds = unsafe { &*(dataset as *const FfiDataset) };
        let id = NEXT_QUEUE_ID.fetch_add(1, Ordering::Relaxed);
        {
            let mut store = ffi_ds
                    .store
                    .lock()
                    .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            let queue = store.open_queue(ffi_ds.handle)?;
            ffi_ds.state.child_handles.fetch_add(1, Ordering::SeqCst);
            QUEUE_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?
                .insert(
                    id,
                    FfiQueueEntry {
                        kind: FfiQueueKind::Dataset {
                            store: Arc::clone(&ffi_ds.store),
                            handle: ffi_ds.handle,
                            queue,
                        },
                        state: Arc::clone(&ffi_ds.state),
                    },
                );
        }
        Ok::<usize, TmslError>(id)
    }}
}

/// Close the queue subsystem for a dataset.
///
/// Invalidates all associated consumer handles.
#[no_mangle]
pub extern "C" fn tmsl_queue_close(
    queue_handle: usize,
    err_buf: *mut c_char,
    err_len: usize,
) -> c_int {
    ffi_catch_int! { err_buf, err_len, {
        let (kind, state) = {
            let registry = QUEUE_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?;
            let entry = registry.get(&queue_handle).ok_or_else(|| {
                TmslError::NotFound("queue handle not found".into())
            })?;
            (
                entry.kind.clone(),
                Arc::clone(&entry.state),
            )
        };

        match &kind {
            FfiQueueKind::Dataset { store, handle, .. } => {
                let mut store = store
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
                store.close_queue(*handle)?;
            }
            FfiQueueKind::Journal { queue } => {
                queue.close()?;
            }
        }

        let removed_consumers = remove_consumers_for_queue(queue_handle)?;
        let removed_queue = QUEUE_REGISTRY
            .lock()
            .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?
            .remove(&queue_handle)
            .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?;
        drop(removed_queue);
        state
            .child_handles
            .fetch_sub(removed_consumers + 1, Ordering::SeqCst);
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
        if group_name.is_null() {
            return Err(TmslError::InvalidData("group_name is null".into()));
        }
        let group_name = unsafe { CStr::from_ptr(group_name) }
            .to_str()
            .map_err(|_| TmslError::InvalidData("invalid group name encoding".into()))?;
        let (kind, state) = {
            let registry = QUEUE_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?;
            let entry = registry
                .get(&queue_handle)
                .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?;
            (entry.kind.clone(), Arc::clone(&entry.state))
        };
        let consumer = match kind {
            FfiQueueKind::Dataset { queue, .. } => {
                FfiConsumerKind::Dataset(queue.open_consumer(group_name)?)
            }
            FfiQueueKind::Journal { queue } => {
                FfiConsumerKind::Journal(queue.open_consumer(group_name)?)
            }
        };
        let id = NEXT_CONSUMER_ID.fetch_add(1, Ordering::Relaxed);
        state.child_handles.fetch_add(1, Ordering::SeqCst);
        CONSUMER_REGISTRY
            .lock()
            .map_err(|_| TmslError::InvalidData("consumer registry mutex poisoned".into()))?
            .insert(
                id,
                FfiConsumerEntry {
                    queue_handle,
                    group_name: group_name.to_string(),
                    consumer,
                    state,
                },
            );
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
        let (kind, group_name, state) = {
            let consumers = CONSUMER_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("consumer registry mutex poisoned".into()))?;
            let consumer = consumers
                .get(&consumer_handle)
                .ok_or_else(|| TmslError::NotFound("consumer handle not found".into()))?;
            if consumer.queue_handle != queue_handle {
                return Err(TmslError::InvalidData(
                    "consumer does not belong to queue handle".into(),
                ));
            }
            let registry = QUEUE_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?;
            let kind = registry
                .get(&queue_handle)
                .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?
                .kind
                .clone();
            (
                kind,
                consumer.group_name.clone(),
                Arc::clone(&consumer.state),
            )
        };
        match kind {
            FfiQueueKind::Dataset { queue, .. } => queue.drop_consumer(&group_name)?,
            FfiQueueKind::Journal { .. } => {
                return Err(TmslError::InvalidData(
                    "journal queue consumer groups cannot be dropped through dataset queue API"
                        .into(),
                ));
            }
        }
        let removed = remove_consumers_for_group(queue_handle, &group_name)?;
        state.child_handles.fetch_sub(removed, Ordering::SeqCst);
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
        let kind = {
            let registry = QUEUE_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?;
            registry
                .get(&queue_handle)
                .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?
                .kind
                .clone()
        };
        let slice = unsafe { std::slice::from_raw_parts(data, data_len) };
        let ts = match kind {
            FfiQueueKind::Dataset { queue, .. } => queue.push(slice)?,
            FfiQueueKind::Journal { .. } => {
                return Err(TmslError::InvalidData("journal queue is read-only".into()));
            }
        };
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
        if out_timestamp.is_null() || out_data.is_null() || out_len.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let consumer = {
            let registry = CONSUMER_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("consumer registry mutex poisoned".into()))?;
            registry
                .get(&consumer_handle)
                .ok_or_else(|| TmslError::NotFound("consumer handle not found".into()))?
                .consumer
                .clone()
        };
        let timeout = if timeout_ms <= 0 {
            Duration::from_millis(0)
        } else {
            Duration::from_millis(timeout_ms as u64)
        };

        let polled = match consumer {
            FfiConsumerKind::Dataset(consumer) => consumer.poll(timeout)?,
            FfiConsumerKind::Journal(consumer) => consumer.poll(timeout)?,
        };

        match polled {
            Some((ts, data)) => {
                unsafe {
                    *out_timestamp = ts as c_longlong;
                    let ptr = alloc_bytes(&data)?;
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
        let consumer = {
            let registry = CONSUMER_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("consumer registry mutex poisoned".into()))?;
            registry
                .get(&consumer_handle)
                .ok_or_else(|| TmslError::NotFound("consumer handle not found".into()))?
                .consumer
                .clone()
        };
        match consumer {
            FfiConsumerKind::Dataset(consumer) => consumer.ack(timestamp)?,
            FfiConsumerKind::Journal(consumer) => consumer.ack(timestamp)?,
        }
        Ok::<c_int, TmslError>(0)
    }}
}

// ─── Journal FFI functions ─────────────────────────────────────────────────

/// Return the latest journal sequence.
///
/// Returns 0 on success and writes 0 when the journal is empty.
#[no_mangle]
pub extern "C" fn tmsl_journal_latest_sequence(
    store: *mut c_void,
    out_sequence: *mut c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if store.is_null() || out_sequence.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let store = ffi_store
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
        let sequence = store.journal_latest_sequence()?.unwrap_or(0);
        unsafe {
            *out_sequence = sequence as c_longlong;
        }
        Ok(0)
    })
}

/// Read one encoded journal record by sequence.
///
/// Returns 0=found, 1=not found, -1=error.
#[no_mangle]
pub extern "C" fn tmsl_journal_read(
    store: *mut c_void,
    sequence: c_longlong,
    out_sequence: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if store.is_null() || out_sequence.is_null() || out_data.is_null() || out_data_len.is_null()
        {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let store = ffi_store
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
        match store.journal_read(sequence)? {
            Some((seq, data)) => {
                let ptr = alloc_bytes(&data)?;
                unsafe {
                    *out_sequence = seq as c_longlong;
                    *out_data = ptr;
                    *out_data_len = data.len();
                }
                Ok(0)
            }
            None => {
                unsafe {
                    *out_data = std::ptr::null_mut();
                    *out_data_len = 0;
                }
                Ok(1)
            }
        }
    })
}

/// Query encoded journal records by inclusive sequence range.
#[no_mangle]
pub extern "C" fn tmsl_journal_query(
    store: *mut c_void,
    start_sequence: c_longlong,
    end_sequence: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    ffi_catch_ptr!(err_buf, err_buf_len, {
        if store.is_null() {
            return Err(TmslError::InvalidData("store is null".into()));
        }
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let entries = {
            let store = ffi_store
                .inner
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store.journal_query(start_sequence, end_sequence)?
        };
        ffi_store.state.child_handles.fetch_add(1, Ordering::SeqCst);
        let iter = Box::new(FfiJournalIterator {
            state: Arc::clone(&ffi_store.state),
            entries,
            position: 0,
        });
        Ok(Box::into_raw(iter) as *mut c_void)
    })
}

/// Get the next encoded journal record from a journal iterator.
///
/// Returns 0=success, 1=done, -1=error.
#[no_mangle]
pub extern "C" fn tmsl_journal_iter_next(
    iter: *mut c_void,
    out_sequence: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if iter.is_null() || out_sequence.is_null() || out_data.is_null() || out_data_len.is_null()
        {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_iter = unsafe { &mut *(iter as *mut FfiJournalIterator) };
        if ffi_iter.position >= ffi_iter.entries.len() {
            return Ok(1);
        }
        let (sequence, data) = &ffi_iter.entries[ffi_iter.position];
        ffi_iter.position += 1;
        let ptr = alloc_bytes(data)?;
        unsafe {
            *out_sequence = *sequence as c_longlong;
            *out_data = ptr;
            *out_data_len = data.len();
        }
        Ok(0)
    })
}

/// Close and free a journal iterator.
#[no_mangle]
pub extern "C" fn tmsl_journal_iter_close(iter: *mut c_void) {
    if !iter.is_null() {
        let ffi_iter = unsafe { Box::from_raw(iter as *mut FfiJournalIterator) };
        ffi_iter.state.child_handles.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Open the built-in journal queue.
#[no_mangle]
pub extern "C" fn tmsl_journal_queue_open(
    store: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> usize {
    ffi_catch_usize!(err_buf, err_buf_len, {
        if store.is_null() {
            return Err(TmslError::InvalidData("store is null".into()));
        }
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let queue = {
            let mut store = ffi_store
                .inner
                .lock()
                .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
            store.open_journal_queue()?
        };
        let id = NEXT_QUEUE_ID.fetch_add(1, Ordering::Relaxed);
        ffi_store.state.child_handles.fetch_add(1, Ordering::SeqCst);
        QUEUE_REGISTRY
            .lock()
            .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?
            .insert(
                id,
                FfiQueueEntry {
                    kind: FfiQueueKind::Journal { queue },
                    state: Arc::clone(&ffi_store.state),
                },
            );
        Ok(id)
    })
}

/// Close a journal queue handle and invalidate its consumers.
#[no_mangle]
pub extern "C" fn tmsl_journal_queue_close(
    queue_handle: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        let (queue, state) = {
            let registry = QUEUE_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?;
            let entry = registry
                .get(&queue_handle)
                .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?;
            match &entry.kind {
                FfiQueueKind::Journal { queue } => (queue.clone(), Arc::clone(&entry.state)),
                FfiQueueKind::Dataset { .. } => {
                    return Err(TmslError::InvalidData("queue handle is not journal".into()));
                }
            }
        };
        queue.close()?;
        let removed_consumers = remove_consumers_for_queue(queue_handle)?;
        QUEUE_REGISTRY
            .lock()
            .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?
            .remove(&queue_handle)
            .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?;
        state
            .child_handles
            .fetch_sub(removed_consumers + 1, Ordering::SeqCst);
        Ok(0)
    })
}

/// Open a consumer group on the built-in journal queue.
#[no_mangle]
pub extern "C" fn tmsl_journal_queue_consumer_open(
    queue_handle: usize,
    group_name: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> usize {
    ffi_catch_usize!(err_buf, err_buf_len, {
        {
            let registry = QUEUE_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("queue registry mutex poisoned".into()))?;
            let entry = registry
                .get(&queue_handle)
                .ok_or_else(|| TmslError::NotFound("queue handle not found".into()))?;
            if !matches!(entry.kind, FfiQueueKind::Journal { .. }) {
                return Err(TmslError::InvalidData("queue handle is not journal".into()));
            }
        }
        let consumer = tmsl_queue_consumer_open(queue_handle, group_name, err_buf, err_buf_len);
        if consumer == 0 {
            return Err(TmslError::InvalidData(
                "failed to open journal queue consumer".into(),
            ));
        }
        Ok(consumer)
    })
}

/// Poll data from a journal queue consumer.
#[no_mangle]
pub extern "C" fn tmsl_journal_queue_poll(
    consumer_handle: usize,
    timeout_ms: c_longlong,
    out_sequence: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if out_sequence.is_null() || out_data.is_null() || out_data_len.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let consumer = {
            let registry = CONSUMER_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("consumer registry mutex poisoned".into()))?;
            match &registry
                .get(&consumer_handle)
                .ok_or_else(|| TmslError::NotFound("consumer handle not found".into()))?
                .consumer
            {
                FfiConsumerKind::Journal(consumer) => consumer.clone(),
                FfiConsumerKind::Dataset(_) => {
                    return Err(TmslError::InvalidData(
                        "consumer handle is not journal".into(),
                    ));
                }
            }
        };
        let timeout = if timeout_ms <= 0 {
            Duration::from_millis(0)
        } else {
            Duration::from_millis(timeout_ms as u64)
        };
        match consumer.poll(timeout)? {
            Some((sequence, data)) => {
                let ptr = alloc_bytes(&data)?;
                unsafe {
                    *out_sequence = sequence as c_longlong;
                    *out_data = ptr;
                    *out_data_len = data.len();
                }
                Ok(0)
            }
            None => Ok(-2),
        }
    })
}

/// Ack a previously polled journal queue record.
#[no_mangle]
pub extern "C" fn tmsl_journal_queue_ack(
    consumer_handle: usize,
    sequence: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        let consumer = {
            let registry = CONSUMER_REGISTRY
                .lock()
                .map_err(|_| TmslError::InvalidData("consumer registry mutex poisoned".into()))?;
            match &registry
                .get(&consumer_handle)
                .ok_or_else(|| TmslError::NotFound("consumer handle not found".into()))?
                .consumer
            {
                FfiConsumerKind::Journal(consumer) => consumer.clone(),
                FfiConsumerKind::Dataset(_) => {
                    return Err(TmslError::InvalidData(
                        "consumer handle is not journal".into(),
                    ));
                }
            }
        };
        consumer.ack(sequence)?;
        Ok(0)
    })
}

// ─── Dataset Inspect FFI ──────────────────────────────────────────────────

/// Dataset immutable configuration info (C representation).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TmslDataSetInfo {
    /// Dataset name (caller must free with tmsl_free_inspect_result)
    pub name: *mut c_char,
    /// Dataset type (caller must free with tmsl_free_inspect_result)
    pub dataset_type: *mut c_char,
    /// Dataset directory path (caller must free with tmsl_free_inspect_result)
    pub base_dir: *mut c_char,
    /// Store-assigned numeric dataset identifier
    pub identifier: u64,
    /// Data segment file size limit (bytes)
    pub data_segment_size: u64,
    /// Index segment file size limit (bytes)
    pub index_segment_size: u64,
    /// Initial data segment file size (bytes)
    pub initial_data_segment_size: u64,
    /// Initial index segment file size (bytes)
    pub initial_index_segment_size: u64,
    /// Compression algorithm type (0=zstd, 1=deflate)
    pub compress_type: u8,
    /// Compression level (0-9)
    pub compress_level: u8,
    /// Index mode: 0=sparse, 1=continuous
    pub index_continuous: u8,
    /// Data retention window (same unit as timestamp, 0=no limit)
    pub retention_window: u64,
    /// Whether this dataset records journal entries when Store journal is enabled.
    pub enable_journal: u8,
    /// Dataset creation time (Unix milliseconds)
    pub create_time: i64,
}

/// Dataset mutable state info (C representation).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TmslDataSetState {
    /// Highest written timestamp
    pub latest_written_timestamp: i64,
    /// Number of currently open data segments
    pub open_data_segments: u32,
    /// Number of closed data segments
    pub closed_data_segments: u32,
    /// Total record count across all data segments
    pub total_record_count: u64,
    /// Total used space across all data segments (bytes)
    pub total_data_size: u64,
    /// Total uncompressed size across all data segments (bytes)
    pub total_uncompressed_size: u64,
    /// Total invalid record count across all data segments
    pub total_invalid_record_count: u64,
    /// Global minimum timestamp
    pub min_timestamp: i64,
    /// Global maximum timestamp
    pub max_timestamp: i64,
    /// Number of currently open index segments
    pub open_index_segments: u32,
    /// Number of closed index segments
    pub closed_index_segments: u32,
    /// Number of in-memory buffered index entries
    pub pending_index_entries: u32,
    /// Index base timestamp (0 if no data)
    pub base_timestamp: i64,
    /// Whether the dataset is in read-only mode
    pub read_only: u8,
    /// Whether BlockCache is enabled
    pub has_block_cache: u8,
    /// Whether Journal is enabled
    pub has_journal: u8,
    /// Whether the dataset has an associated Queue
    pub has_queue: u8,
    /// Number of queue consumer groups
    pub queue_consumer_groups: u32,
}

/// Dataset inspect result (C representation).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TmslInspectResult {
    /// Immutable configuration info
    pub info: TmslDataSetInfo,
    /// Mutable current state
    pub state: TmslDataSetState,
}

/// Get detailed info and state of a dataset.
///
/// On success writes the inspect result to `out_result`. Caller must free with
/// `tmsl_free_inspect_result`.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn tmsl_store_inspect_dataset(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    out_result: *mut TmslInspectResult,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    ffi_catch_int!(err_buf, err_buf_len, {
        if store.is_null() || name.is_null() || dataset_type.is_null() || out_result.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|_| TmslError::InvalidData("invalid UTF-8 in name".into()))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|_| TmslError::InvalidData("invalid UTF-8 in dataset_type".into()))?;
        let ffi_store = unsafe { &*(store as *const FfiStore) };
        let store_inner = ffi_store
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("store mutex poisoned".into()))?;
        let result = store_inner.inspect_dataset(name_str, type_str)?;

        // Convert to C struct
        let name_cstr = CString::new(result.info.name)
            .map_err(|_| TmslError::InvalidData("name contains null byte".into()))?;
        let type_cstr = CString::new(result.info.dataset_type)
            .map_err(|_| TmslError::InvalidData("dataset_type contains null byte".into()))?;
        let base_dir_cstr = CString::new(result.info.base_dir)
            .map_err(|_| TmslError::InvalidData("base_dir contains null byte".into()))?;

        let info = TmslDataSetInfo {
            name: name_cstr.into_raw(),
            dataset_type: type_cstr.into_raw(),
            base_dir: base_dir_cstr.into_raw(),
            identifier: result.info.identifier,
            data_segment_size: result.info.data_segment_size,
            index_segment_size: result.info.index_segment_size,
            initial_data_segment_size: result.info.initial_data_segment_size,
            initial_index_segment_size: result.info.initial_index_segment_size,
            compress_type: result.info.compress_type,
            compress_level: result.info.compress_level,
            index_continuous: result.info.index_continuous,
            retention_window: result.info.retention_window,
            enable_journal: u8::from(result.info.enable_journal),
            create_time: result.info.create_time,
        };

        let state = TmslDataSetState {
            latest_written_timestamp: result.state.latest_written_timestamp,
            open_data_segments: result.state.open_data_segments,
            closed_data_segments: result.state.closed_data_segments,
            total_record_count: result.state.total_record_count,
            total_data_size: result.state.total_data_size,
            total_uncompressed_size: result.state.total_uncompressed_size,
            total_invalid_record_count: result.state.total_invalid_record_count,
            min_timestamp: result.state.min_timestamp,
            max_timestamp: result.state.max_timestamp,
            open_index_segments: result.state.open_index_segments,
            closed_index_segments: result.state.closed_index_segments,
            pending_index_entries: result.state.pending_index_entries,
            base_timestamp: result.state.base_timestamp.unwrap_or(0),
            read_only: u8::from(result.state.read_only),
            has_block_cache: u8::from(result.state.has_block_cache),
            has_journal: u8::from(result.state.has_journal),
            has_queue: u8::from(result.state.has_queue),
            queue_consumer_groups: result.state.queue_consumer_groups,
        };

        unsafe {
            *out_result = TmslInspectResult { info, state };
        }
        Ok(0)
    })
}

/// Free the memory allocated by `tmsl_store_inspect_dataset`.
///
/// This frees the strings in `info` and the result struct itself.
#[no_mangle]
pub extern "C" fn tmsl_free_inspect_result(result: *mut TmslInspectResult) {
    if result.is_null() {
        return;
    }
    unsafe {
        let result_ref = &*result;
        // Free strings
        if !result_ref.info.name.is_null() {
            let _ = CString::from_raw(result_ref.info.name);
        }
        if !result_ref.info.dataset_type.is_null() {
            let _ = CString::from_raw(result_ref.info.dataset_type);
        }
        if !result_ref.info.base_dir.is_null() {
            let _ = CString::from_raw(result_ref.info.base_dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn temp_store_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("timslite_ffi_test").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cleanup_store_dir(dir: &std::path::Path) {
        let _ = std::fs::remove_dir_all(dir);
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
        cleanup_store_dir(&dir);
    }

    #[test]
    fn test_dataset_identifier_ffi_api() {
        let dir = temp_store_dir("timslite_ffi_dataset_identifier");
        let dir_c = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
        let (mut err, err_len) = err_buf();
        let mut config = TmslStoreConfigFFI::default();
        assert_eq!(
            tmsl_store_config_default(&mut config, err.as_mut_ptr(), err_len),
            0
        );
        config.enable_background_thread = 0;
        config.enable_journal = 0;

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

        let mut identifier = 0u64;
        assert_eq!(
            tmsl_dataset_identifier(dataset, &mut identifier, err.as_mut_ptr(), err_len),
            0
        );
        assert_eq!(identifier, 1);
        assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err_len), 0);

        let dataset_by_id =
            tmsl_dataset_open_by_identifier(store, identifier, err.as_mut_ptr(), err_len);
        assert!(!dataset_by_id.is_null());
        assert_eq!(
            tmsl_dataset_close(dataset_by_id, err.as_mut_ptr(), err_len),
            0
        );
        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
        cleanup_store_dir(&dir);
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
            compress_type: crate::compress::COMPRESS_TYPE_ZSTD,
            index_continuous: 0,
            enable_journal: 0,
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

        let mut inspect = std::mem::MaybeUninit::<TmslInspectResult>::uninit();
        assert_eq!(
            tmsl_store_inspect_dataset(
                store,
                name.as_ptr(),
                ty.as_ptr(),
                inspect.as_mut_ptr(),
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );
        let mut inspect = unsafe { inspect.assume_init() };
        assert_eq!(inspect.info.enable_journal, 0);
        assert_eq!(inspect.state.has_journal, 0);
        tmsl_free_inspect_result(&mut inspect);

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
        cleanup_store_dir(&dir);
    }

    #[test]
    fn test_store_open_no_config() {
        let dir = temp_store_dir("ffi_store_open_no_config");
        let dir_c = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
        let (mut err, err_len) = err_buf();

        let store = tmsl_store_open(dir_c.as_ptr(), err.as_mut_ptr(), err_len);
        assert!(
            !store.is_null(),
            "tmsl_store_open with null config should succeed"
        );

        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
        cleanup_store_dir(&dir);
    }

    #[test]
    fn test_ffi_query_iterator_and_delete() {
        let dir = temp_store_dir("ffi_query_iter_delete");
        let dir_c = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
        let (mut err, err_len) = err_buf();
        let mut config = TmslStoreConfigFFI::default();
        assert_eq!(
            tmsl_store_config_default(&mut config, err.as_mut_ptr(), err_len),
            0
        );
        config.enable_background_thread = 0;
        config.enable_journal = 0;

        let store = tmsl_store_open_with_config(dir_c.as_ptr(), &config, err.as_mut_ptr(), err_len);
        assert!(!store.is_null());

        let name = CString::new("qid").unwrap();
        let ty = CString::new("data").unwrap();
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

        // Write 5 records
        for i in 1i64..=5 {
            let data = format!("rec_{}", i);
            assert_eq!(
                tmsl_dataset_write(
                    dataset,
                    i,
                    data.as_ptr(),
                    data.len(),
                    err.as_mut_ptr(),
                    err_len,
                ),
                0
            );
        }

        // Flush
        assert_eq!(tmsl_dataset_flush(dataset, err.as_mut_ptr(), err_len), 0);

        // Delete record at ts=3
        assert_eq!(
            tmsl_dataset_delete(dataset, 3, err.as_mut_ptr(), err_len),
            0
        );

        // Query ts 1..5 via iterator
        let iter = tmsl_dataset_query(dataset, 1, 5, err.as_mut_ptr(), err_len);
        assert!(!iter.is_null());

        let mut collected_ts: Vec<i64> = Vec::new();
        loop {
            let mut ts: c_longlong = 0;
            let mut data: *mut c_uchar = std::ptr::null_mut();
            let mut data_len: usize = 0;
            let rc = tmsl_iter_next(
                iter,
                &mut ts,
                &mut data,
                &mut data_len,
                err.as_mut_ptr(),
                err_len,
            );
            if rc == 1 {
                break; // done
            }
            assert_eq!(rc, 0, "iter_next should return 0 or 1");
            collected_ts.push(ts);
            tmsl_iter_free_data(data);
        }
        // ts=3 deleted, should get 1,2,4,5
        assert_eq!(collected_ts, vec![1, 2, 4, 5]);

        tmsl_iter_close(iter);
        assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err_len), 0);
        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
        cleanup_store_dir(&dir);
    }

    #[test]
    fn test_ffi_queue_c_abi_push_poll_ack_lifecycle() {
        let dir = temp_store_dir("ffi_queue_c_abi");
        let dir_c = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
        let (mut err, err_len) = err_buf();
        let mut config = TmslStoreConfigFFI::default();
        assert_eq!(
            tmsl_store_config_default(&mut config, err.as_mut_ptr(), err_len),
            0
        );
        config.enable_background_thread = 0;
        config.enable_journal = 0;

        let store = tmsl_store_open_with_config(dir_c.as_ptr(), &config, err.as_mut_ptr(), err_len);
        assert!(!store.is_null());

        let name = CString::new("queueffi").unwrap();
        let ty = CString::new("data").unwrap();
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

        let queue = tmsl_queue_open(dataset, err.as_mut_ptr(), err_len);
        assert_ne!(queue, 0, "queue open should return a queue handle");
        assert_eq!(
            tmsl_store_close(store, err.as_mut_ptr(), err_len),
            -1,
            "store close must reject outstanding queue handles"
        );

        let group = CString::new("group_a").unwrap();
        let consumer = tmsl_queue_consumer_open(queue, group.as_ptr(), err.as_mut_ptr(), err_len);
        assert_ne!(consumer, 0, "consumer open should return a handle");

        let payload = b"queued-payload";
        let assigned_ts = tmsl_queue_push(
            queue,
            payload.as_ptr(),
            payload.len(),
            err.as_mut_ptr(),
            err_len,
        );
        assert_eq!(assigned_ts, 1);

        let mut out_ts: c_longlong = 0;
        let mut out_data: *mut c_uchar = std::ptr::null_mut();
        let mut out_len: usize = 0;
        assert_eq!(
            tmsl_queue_poll(
                consumer,
                0,
                &mut out_ts,
                &mut out_data,
                &mut out_len,
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );
        assert_eq!(out_ts, assigned_ts);
        let out_slice = unsafe { std::slice::from_raw_parts(out_data, out_len) };
        assert_eq!(out_slice, payload);
        tmsl_data_free(out_data as *mut c_void);

        assert_eq!(
            tmsl_queue_ack(consumer, out_ts, err.as_mut_ptr(), err_len),
            0
        );
        assert_eq!(
            tmsl_queue_consumer_drop(queue, consumer, err.as_mut_ptr(), err_len),
            0
        );
        assert_eq!(tmsl_queue_close(queue, err.as_mut_ptr(), err_len), 0);
        assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err_len), 0);
        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
        cleanup_store_dir(&dir);
    }

    #[test]
    fn test_ffi_journal_read_and_query_store_api() {
        let dir = temp_store_dir("ffi_journal_read_query");
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

        let name = CString::new("journalffi").unwrap();
        let ty = CString::new("data").unwrap();
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

        let mut latest: c_longlong = 0;
        assert_eq!(
            tmsl_journal_latest_sequence(store, &mut latest, err.as_mut_ptr(), err_len),
            0
        );
        assert!(latest >= 1);

        let mut out_seq: c_longlong = 0;
        let mut out_data: *mut c_uchar = std::ptr::null_mut();
        let mut out_len: usize = 0;
        assert_eq!(
            tmsl_journal_read(
                store,
                1,
                &mut out_seq,
                &mut out_data,
                &mut out_len,
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );
        assert_eq!(out_seq, 1);
        assert!(!out_data.is_null());
        let bytes = unsafe { std::slice::from_raw_parts(out_data, out_len) };
        let record = crate::journal::JournalRecord::decode(bytes).unwrap();
        assert_eq!(
            record.kind,
            crate::journal::JournalRecordKind::CreateDataset
        );
        tmsl_data_free(out_data as *mut c_void);

        let iter = tmsl_journal_query(store, 1, latest, err.as_mut_ptr(), err_len);
        assert!(!iter.is_null());
        assert_eq!(
            tmsl_store_close(store, err.as_mut_ptr(), err_len),
            -1,
            "store close must reject outstanding journal iterator"
        );
        assert_eq!(
            tmsl_journal_iter_next(
                iter,
                &mut out_seq,
                &mut out_data,
                &mut out_len,
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );
        assert_eq!(out_seq, 1);
        tmsl_data_free(out_data as *mut c_void);
        tmsl_journal_iter_close(iter);

        assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err_len), 0);
        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
        cleanup_store_dir(&dir);
    }

    #[test]
    fn test_ffi_journal_queue_poll_ack() {
        let dir = temp_store_dir("ffi_journal_queue");
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

        let name = CString::new("journalqueueffi").unwrap();
        let ty = CString::new("data").unwrap();
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

        let queue = tmsl_journal_queue_open(store, err.as_mut_ptr(), err_len);
        assert_ne!(queue, 0);
        assert_eq!(
            tmsl_store_close(store, err.as_mut_ptr(), err_len),
            -1,
            "store close must reject outstanding journal queue"
        );

        let group = CString::new("journal_group").unwrap();
        let consumer =
            tmsl_journal_queue_consumer_open(queue, group.as_ptr(), err.as_mut_ptr(), err_len);
        assert_ne!(consumer, 0);

        let payload = b"journal-write";
        assert_eq!(
            tmsl_dataset_write(
                dataset,
                10,
                payload.as_ptr(),
                payload.len(),
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );

        let mut out_seq: c_longlong = 0;
        let mut out_data: *mut c_uchar = std::ptr::null_mut();
        let mut out_len: usize = 0;
        assert_eq!(
            tmsl_journal_queue_poll(
                consumer,
                100,
                &mut out_seq,
                &mut out_data,
                &mut out_len,
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );
        assert!(out_seq > 0);
        let bytes = unsafe { std::slice::from_raw_parts(out_data, out_len) };
        let record = crate::journal::JournalRecord::decode(bytes).unwrap();
        assert_eq!(record.kind, crate::journal::JournalRecordKind::DataWrite);
        tmsl_data_free(out_data as *mut c_void);
        assert_eq!(
            tmsl_journal_queue_ack(consumer, out_seq, err.as_mut_ptr(), err_len),
            0
        );
        assert_eq!(
            tmsl_journal_queue_close(queue, err.as_mut_ptr(), err_len),
            0
        );

        assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err_len), 0);
        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
        cleanup_store_dir(&dir);
    }

    #[test]
    fn test_ffi_query_iterator_uses_index_entry_snapshot() {
        let dir = temp_store_dir("ffi_query_iter_snapshot");
        let dir_c = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
        let (mut err, err_len) = err_buf();
        let mut config = TmslStoreConfigFFI::default();
        assert_eq!(
            tmsl_store_config_default(&mut config, err.as_mut_ptr(), err_len),
            0
        );
        config.enable_background_thread = 0;
        config.enable_journal = 0;

        let store = tmsl_store_open_with_config(dir_c.as_ptr(), &config, err.as_mut_ptr(), err_len);
        assert!(!store.is_null());

        let name = CString::new("snapffi").unwrap();
        let ty = CString::new("data").unwrap();
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

        for ts in 1i64..=1100 {
            let data = format!("rec_{ts}");
            assert_eq!(
                tmsl_dataset_write(
                    dataset,
                    ts,
                    data.as_ptr(),
                    data.len(),
                    err.as_mut_ptr(),
                    err_len,
                ),
                0
            );
        }

        let iter = tmsl_dataset_query(dataset, 500, 501, err.as_mut_ptr(), err_len);
        assert!(!iter.is_null());
        assert_eq!(
            tmsl_dataset_delete(dataset, 500, err.as_mut_ptr(), err_len),
            0
        );

        let mut ts: c_longlong = 0;
        let mut data: *mut c_uchar = std::ptr::null_mut();
        let mut data_len: usize = 0;
        assert_eq!(
            tmsl_iter_next(
                iter,
                &mut ts,
                &mut data,
                &mut data_len,
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );
        assert_eq!(ts, 500);
        let out_slice = unsafe { std::slice::from_raw_parts(data, data_len) };
        assert_eq!(out_slice, b"rec_500");
        tmsl_iter_free_data(data);

        tmsl_iter_close(iter);
        assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err_len), 0);
        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
        cleanup_store_dir(&dir);
    }

    #[test]
    fn test_ffi_tick_and_next_delay() {
        let dir = temp_store_dir("ffi_tick_delay");
        let dir_c = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
        let (mut err, err_len) = err_buf();
        let mut config = TmslStoreConfigFFI::default();
        assert_eq!(
            tmsl_store_config_default(&mut config, err.as_mut_ptr(), err_len),
            0
        );
        config.enable_background_thread = 0;
        config.enable_journal = 0;

        let store = tmsl_store_open_with_config(dir_c.as_ptr(), &config, err.as_mut_ptr(), err_len);
        assert!(!store.is_null());

        let mut executed: u32 = 99;
        let mut delay_ms: u64 = 99;
        assert_eq!(
            tmsl_store_tick_background_tasks(
                store,
                &mut executed,
                &mut delay_ms,
                err.as_mut_ptr(),
                err_len,
            ),
            0
        );
        assert!(executed <= 4, "executed tasks bounded by 4");

        assert_eq!(
            tmsl_store_next_background_delay(store, &mut delay_ms, err.as_mut_ptr(), err_len),
            0
        );
        assert!(
            delay_ms <= 15_000,
            "delay bounded by default flush_interval (15s)"
        );

        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
        cleanup_store_dir(&dir);
    }

    #[test]
    fn test_ffi_error_path_err_buf_output() {
        let dir = temp_store_dir("ffi_error_path");
        let dir_c = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
        let (mut err, err_len) = err_buf();
        let mut config = TmslStoreConfigFFI::default();
        assert_eq!(
            tmsl_store_config_default(&mut config, err.as_mut_ptr(), err_len),
            0
        );
        config.enable_background_thread = 0;
        config.enable_journal = 0;

        let store = tmsl_store_open_with_config(dir_c.as_ptr(), &config, err.as_mut_ptr(), err_len);
        assert!(!store.is_null());

        // Try to open non-existent dataset → should fail and write err_buf
        let bad_name = CString::new("nonexistent").unwrap();
        let bad_ty = CString::new("data").unwrap();
        let (mut err2, err_len2) = err_buf();
        let null_ds = tmsl_dataset_open(
            store,
            bad_name.as_ptr(),
            bad_ty.as_ptr(),
            err2.as_mut_ptr(),
            err_len2,
        );
        assert!(
            null_ds.is_null(),
            "opening non-existent dataset should return null"
        );
        // err_buf should contain a non-empty error message
        let err_str = unsafe { std::ffi::CStr::from_ptr(err2.as_ptr()) }
            .to_str()
            .unwrap_or("");
        assert!(
            !err_str.is_empty(),
            "err_buf should contain an error message"
        );

        // Try to drop with outstanding child handle → should fail
        let name = CString::new("errds").unwrap();
        let ty = CString::new("data").unwrap();
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
        // store close with outstanding handle → error
        let (mut err3, err_len3) = err_buf();
        assert_eq!(
            tmsl_store_close(store, err3.as_mut_ptr(), err_len3),
            -1,
            "store close must reject outstanding dataset handles"
        );
        let close_err = unsafe { std::ffi::CStr::from_ptr(err3.as_ptr()) }
            .to_str()
            .unwrap_or("");
        assert!(
            close_err.contains("outstanding"),
            "err_buf should mention outstanding handles, got: {}",
            close_err
        );

        assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err_len), 0);
        assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err_len), 0);
        cleanup_store_dir(&dir);
    }
}
