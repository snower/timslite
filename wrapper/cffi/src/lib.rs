//! C ABI wrapper for the `timslite` Rust library.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong, c_uchar, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use libc::{free, malloc};
use timslite::{
    DataSet, DataSetConfigBuilder, DataSetInspectResult, DatasetQueue, DatasetQueueConsumer,
    JournalQueue, JournalQueueConsumer, QueueConsumerConfig, QueuePollCallback, Store,
    StoreConfig, TmslError,
};

const TMSL_STORE_CONFIG_FFI_VERSION: u32 = 5;
const TMSL_DATASET_CONFIG_FFI_VERSION: u32 = 3;
const TMSL_QUEUE_CONSUMER_CONFIG_FFI_VERSION: u32 = 1;

const TMSL_STORE_READ_ONLY_AUTO: u8 = 0;
const TMSL_STORE_READ_ONLY_FALSE: u8 = 1;
const TMSL_STORE_READ_ONLY_TRUE: u8 = 2;

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
    pub read_only_mode: u8,
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

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TmslQueueConsumerConfigFFI {
    pub version: u32,
    pub running_expired_seconds: u32,
    pub max_retry_count: u32,
}

pub type TmslQueuePollCallback = Option<extern "C" fn(*mut c_void)>;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TmslLengthEntry {
    pub timestamp: i64,
    pub data_len: u32,
}

#[repr(C)]
pub struct TmslDataSetInfo {
    pub name: *mut c_char,
    pub dataset_type: *mut c_char,
    pub base_dir: *mut c_char,
    pub identifier: u64,
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    pub compress_type: u8,
    pub compress_level: u8,
    pub index_continuous: u8,
    pub retention_window: u64,
    pub enable_journal: u8,
    pub create_time: i64,
}

#[repr(C)]
pub struct TmslDataSetState {
    pub has_latest_written_timestamp: u8,
    pub latest_written_timestamp: i64,
    pub open_data_segments: u32,
    pub data_segments: u32,
    pub total_record_count: u64,
    pub total_data_size: u64,
    pub total_uncompressed_size: u64,
    pub total_invalid_record_count: u64,
    pub has_min_timestamp: u8,
    pub min_timestamp: i64,
    pub has_max_timestamp: u8,
    pub max_timestamp: i64,
    pub open_index_segments: u32,
    pub index_segments: u32,
    pub pending_index_entries: u32,
    pub has_base_timestamp: u8,
    pub base_timestamp: i64,
    pub read_only: u8,
    pub has_block_cache: u8,
    pub has_journal: u8,
    pub has_queue: u8,
    pub queue_consumer_groups: u32,
}

#[repr(C)]
pub struct TmslInspectResult {
    pub info: TmslDataSetInfo,
    pub state: TmslDataSetState,
}

struct FfiStore {
    inner: Mutex<Option<Store>>,
}

struct FfiDataset {
    dataset: DataSet,
}

struct FfiDataIter {
    rows: Vec<(i64, Vec<u8>)>,
    position: usize,
}

struct FfiLengthIter {
    iter: timslite::QueryLengthIterator,
}

struct FfiQueue {
    kind: QueueKind,
}

#[derive(Clone)]
enum QueueKind {
    Dataset(DatasetQueue),
    Journal(JournalQueue),
}

struct FfiConsumer {
    queue_id: usize,
    group_name: String,
    kind: ConsumerKind,
}

#[derive(Clone)]
enum ConsumerKind {
    Dataset(DatasetQueueConsumer),
    Journal(JournalQueueConsumer),
}

static NEXT_QUEUE_ID: AtomicUsize = AtomicUsize::new(1);
static NEXT_CONSUMER_ID: AtomicUsize = AtomicUsize::new(1);
static QUEUES: LazyLock<Mutex<HashMap<usize, FfiQueue>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static CONSUMERS: LazyLock<Mutex<HashMap<usize, FfiConsumer>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn invalid_data(message: impl Into<String>) -> TmslError {
    TmslError::InvalidData(message.into())
}

fn write_error(err_buf: *mut c_char, err_buf_len: usize, message: &str) {
    if err_buf.is_null() || err_buf_len == 0 {
        return;
    }
    let bytes = message.as_bytes();
    let copy_len = bytes.len().min(err_buf_len.saturating_sub(1));
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), err_buf.cast::<u8>(), copy_len);
        *err_buf.add(copy_len) = 0;
    }
}

fn clear_error(err_buf: *mut c_char, err_buf_len: usize) {
    if !err_buf.is_null() && err_buf_len > 0 {
        unsafe {
            *err_buf = 0;
        }
    }
}

fn run_int<F>(err_buf: *mut c_char, err_buf_len: usize, f: F) -> c_int
where
    F: FnOnce() -> timslite::Result<c_int>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => {
            clear_error(err_buf, err_buf_len);
            value
        }
        Ok(Err(err)) => {
            write_error(err_buf, err_buf_len, &err.to_string());
            -1
        }
        Err(_) => {
            write_error(err_buf, err_buf_len, "panic across timslite C ABI");
            -1
        }
    }
}

fn run_ptr<F>(err_buf: *mut c_char, err_buf_len: usize, f: F) -> *mut c_void
where
    F: FnOnce() -> timslite::Result<*mut c_void>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => {
            clear_error(err_buf, err_buf_len);
            value
        }
        Ok(Err(err)) => {
            write_error(err_buf, err_buf_len, &err.to_string());
            ptr::null_mut()
        }
        Err(_) => {
            write_error(err_buf, err_buf_len, "panic across timslite C ABI");
            ptr::null_mut()
        }
    }
}

fn run_usize<F>(err_buf: *mut c_char, err_buf_len: usize, f: F) -> usize
where
    F: FnOnce() -> timslite::Result<usize>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => {
            clear_error(err_buf, err_buf_len);
            value
        }
        Ok(Err(err)) => {
            write_error(err_buf, err_buf_len, &err.to_string());
            0
        }
        Err(_) => {
            write_error(err_buf, err_buf_len, "panic across timslite C ABI");
            0
        }
    }
}

fn run_i64<F>(err_buf: *mut c_char, err_buf_len: usize, f: F) -> c_longlong
where
    F: FnOnce() -> timslite::Result<c_longlong>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => {
            clear_error(err_buf, err_buf_len);
            value
        }
        Ok(Err(err)) => {
            write_error(err_buf, err_buf_len, &err.to_string());
            -1
        }
        Err(_) => {
            write_error(err_buf, err_buf_len, "panic across timslite C ABI");
            -1
        }
    }
}

fn cstr_to_str<'a>(ptr: *const c_char, field: &str) -> timslite::Result<&'a str> {
    if ptr.is_null() {
        return Err(invalid_data(format!("{field} is null")));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|err| invalid_data(format!("invalid UTF-8 in {field}: {err}")))
}

fn checked_input_slice<'a>(data: *const c_uchar, len: usize) -> timslite::Result<&'a [u8]> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(invalid_data("data is null"));
    }
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

fn alloc_bytes(data: &[u8]) -> timslite::Result<*mut c_uchar> {
    if data.is_empty() {
        return Ok(ptr::null_mut());
    }
    let out = unsafe { malloc(data.len()) }.cast::<c_uchar>();
    if out.is_null() {
        return Err(invalid_data("malloc failed"));
    }
    unsafe {
        ptr::copy_nonoverlapping(data.as_ptr(), out, data.len());
    }
    Ok(out)
}

fn write_alloc_bytes(
    data: &[u8],
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
) -> timslite::Result<()> {
    if out_data.is_null() || out_data_len.is_null() {
        return Err(invalid_data("output pointer is null"));
    }
    let allocated = alloc_bytes(data)?;
    unsafe {
        *out_data = allocated;
        *out_data_len = data.len();
    }
    Ok(())
}

fn cstring_ptr(value: String) -> timslite::Result<*mut c_char> {
    CString::new(value)
        .map(CString::into_raw)
        .map_err(|_| invalid_data("string contains interior NUL"))
}

fn write_string_array(
    values: Vec<String>,
    out_values: *mut *mut *mut c_char,
    out_count: *mut u32,
) -> timslite::Result<()> {
    if out_values.is_null() || out_count.is_null() {
        return Err(invalid_data("output pointer is null"));
    }
    let count = u32::try_from(values.len()).map_err(|_| invalid_data("too many strings"))?;
    if values.is_empty() {
        unsafe {
            *out_values = ptr::null_mut();
            *out_count = 0;
        }
        return Ok(());
    }
    let bytes = values.len() * std::mem::size_of::<*mut c_char>();
    let array = unsafe { malloc(bytes) }.cast::<*mut c_char>();
    if array.is_null() {
        return Err(invalid_data("malloc failed"));
    }
    let mut written = 0usize;
    for value in values {
        match cstring_ptr(value) {
            Ok(ptr) => {
                unsafe {
                    *array.add(written) = ptr;
                }
                written += 1;
            }
            Err(err) => {
                for idx in 0..written {
                    unsafe {
                        let ptr = *array.add(idx);
                        if !ptr.is_null() {
                            drop(CString::from_raw(ptr));
                        }
                    }
                }
                unsafe {
                    free(array.cast::<c_void>());
                }
                return Err(err);
            }
        }
    }
    unsafe {
        *out_values = array;
        *out_count = count;
    }
    Ok(())
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn store_config_to_ffi(config: &StoreConfig) -> TmslStoreConfigFFI {
    let read_only_mode = match config.read_only() {
        None => TMSL_STORE_READ_ONLY_AUTO,
        Some(false) => TMSL_STORE_READ_ONLY_FALSE,
        Some(true) => TMSL_STORE_READ_ONLY_TRUE,
    };
    TmslStoreConfigFFI {
        version: TMSL_STORE_CONFIG_FFI_VERSION,
        flush_interval_ms: duration_millis(config.flush_interval()),
        idle_timeout_ms: duration_millis(config.idle_timeout()),
        data_segment_size: config.data_segment_size(),
        index_segment_size: config.index_segment_size(),
        initial_data_segment_size: config.initial_data_segment_size(),
        initial_index_segment_size: config.initial_index_segment_size(),
        cache_max_memory: config.cache_max_memory() as u64,
        cache_idle_timeout_ms: duration_millis(config.cache_idle_timeout()),
        compress_level: config.compress_level(),
        compress_type: config.compress_type(),
        retention_check_hour: config.retention_check_hour(),
        enable_background_thread: config.enable_background_thread() as u8,
        enable_journal: config.enable_journal() as u8,
        read_only_mode,
    }
}

fn store_config_from_ffi(ptr: *const TmslStoreConfigFFI) -> timslite::Result<StoreConfig> {
    if ptr.is_null() {
        return Ok(StoreConfig::default());
    }
    let config = unsafe { *ptr };
    if config.version != TMSL_STORE_CONFIG_FFI_VERSION {
        return Err(invalid_data(format!(
            "unsupported store config version {}",
            config.version
        )));
    }
    let read_only = match config.read_only_mode {
        TMSL_STORE_READ_ONLY_AUTO => None,
        TMSL_STORE_READ_ONLY_FALSE => Some(false),
        TMSL_STORE_READ_ONLY_TRUE => Some(true),
        other => return Err(invalid_data(format!("invalid read_only_mode {other}"))),
    };
    Ok(StoreConfig::builder()
        .flush_interval(Duration::from_millis(config.flush_interval_ms))
        .idle_timeout(Duration::from_millis(config.idle_timeout_ms))
        .data_segment_size(config.data_segment_size)
        .index_segment_size(config.index_segment_size)
        .initial_data_segment_size(config.initial_data_segment_size)
        .initial_index_segment_size(config.initial_index_segment_size)
        .cache_max_memory(config.cache_max_memory as usize)
        .cache_idle_timeout(Duration::from_millis(config.cache_idle_timeout_ms))
        .compress_level(config.compress_level)
        .compress_type(config.compress_type)
        .retention_check_hour(config.retention_check_hour)
        .enable_background_thread(config.enable_background_thread != 0)
        .enable_journal(config.enable_journal != 0)
        .read_only(read_only)
        .build())
}

fn dataset_config_from_ffi(
    ptr: *const TmslDatasetConfigFFI,
) -> timslite::Result<DataSetConfigBuilder> {
    if ptr.is_null() {
        return Err(invalid_data("dataset config is null"));
    }
    let config = unsafe { *ptr };
    if config.version != TMSL_DATASET_CONFIG_FFI_VERSION {
        return Err(invalid_data(format!(
            "unsupported dataset config version {}",
            config.version
        )));
    }
    Ok(DataSetConfigBuilder::default()
        .data_segment_size(config.data_segment_size)
        .index_segment_size(config.index_segment_size)
        .initial_data_segment_size(config.initial_data_segment_size)
        .initial_index_segment_size(config.initial_index_segment_size)
        .compress_level(config.compress_level)
        .compress_type(config.compress_type)
        .index_continuous(config.index_continuous)
        .retention_window(config.retention_window)
        .enable_journal(config.enable_journal != 0))
}

fn consumer_config_from_ffi(
    ptr: *const TmslQueueConsumerConfigFFI,
) -> timslite::Result<QueueConsumerConfig> {
    if ptr.is_null() {
        return Ok(QueueConsumerConfig::default());
    }
    let config = unsafe { *ptr };
    if config.version != TMSL_QUEUE_CONSUMER_CONFIG_FFI_VERSION {
        return Err(invalid_data(format!(
            "unsupported queue consumer config version {}",
            config.version
        )));
    }
    let max_retry_count = u16::try_from(config.max_retry_count)
        .map_err(|_| invalid_data("max_retry_count exceeds u16"))?;
    QueueConsumerConfig::builder()
        .running_expired_seconds(config.running_expired_seconds as u64)
        .max_retry_count(max_retry_count)
        .build()
}

fn with_store_mut<T>(
    store: *mut c_void,
    f: impl FnOnce(&mut Store) -> timslite::Result<T>,
) -> timslite::Result<T> {
    if store.is_null() {
        return Err(invalid_data("store is null"));
    }
    let ffi_store = unsafe { &*(store as *const FfiStore) };
    let mut guard = ffi_store
        .inner
        .lock()
        .map_err(|_| invalid_data("store mutex poisoned"))?;
    let store = guard
        .as_mut()
        .ok_or_else(|| invalid_data("store is closed"))?;
    f(store)
}

fn with_dataset<T>(
    dataset: *mut c_void,
    f: impl FnOnce(&DataSet) -> timslite::Result<T>,
) -> timslite::Result<T> {
    if dataset.is_null() {
        return Err(invalid_data("dataset is null"));
    }
    let ffi_dataset = unsafe { &*(dataset as *const FfiDataset) };
    f(&ffi_dataset.dataset)
}

fn next_queue_id() -> timslite::Result<usize> {
    let id = NEXT_QUEUE_ID.fetch_add(1, Ordering::Relaxed);
    if id == 0 {
        return Err(invalid_data("queue handle overflow"));
    }
    Ok(id)
}

fn next_consumer_id() -> timslite::Result<usize> {
    let id = NEXT_CONSUMER_ID.fetch_add(1, Ordering::Relaxed);
    if id == 0 {
        return Err(invalid_data("consumer handle overflow"));
    }
    Ok(id)
}

fn register_queue(kind: QueueKind) -> timslite::Result<usize> {
    let id = next_queue_id()?;
    QUEUES
        .lock()
        .map_err(|_| invalid_data("queue registry mutex poisoned"))?
        .insert(id, FfiQueue { kind });
    Ok(id)
}

fn queue_kind(queue_handle: usize) -> timslite::Result<QueueKind> {
    QUEUES
        .lock()
        .map_err(|_| invalid_data("queue registry mutex poisoned"))?
        .get(&queue_handle)
        .map(|queue| queue.kind.clone())
        .ok_or_else(|| invalid_data("invalid queue handle"))
}

fn register_consumer(
    queue_id: usize,
    group_name: String,
    kind: ConsumerKind,
) -> timslite::Result<usize> {
    let id = next_consumer_id()?;
    CONSUMERS
        .lock()
        .map_err(|_| invalid_data("consumer registry mutex poisoned"))?
        .insert(
            id,
            FfiConsumer {
                queue_id,
                group_name,
                kind,
            },
        );
    Ok(id)
}

fn consumer_kind(consumer_handle: usize) -> timslite::Result<ConsumerKind> {
    CONSUMERS
        .lock()
        .map_err(|_| invalid_data("consumer registry mutex poisoned"))?
        .get(&consumer_handle)
        .map(|consumer| consumer.kind.clone())
        .ok_or_else(|| invalid_data("invalid consumer handle"))
}

fn remove_consumers_for_queue(queue_id: usize) -> timslite::Result<()> {
    CONSUMERS
        .lock()
        .map_err(|_| invalid_data("consumer registry mutex poisoned"))?
        .retain(|_, consumer| consumer.queue_id != queue_id);
    Ok(())
}

#[no_mangle]
pub extern "C" fn tmsl_store_config_default(
    out_config: *mut TmslStoreConfigFFI,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_config.is_null() {
            return Err(invalid_data("out_config is null"));
        }
        unsafe {
            *out_config = store_config_to_ffi(&StoreConfig::default());
        }
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn tmsl_store_open(
    data_dir: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    tmsl_store_open_with_config(data_dir, ptr::null(), err_buf, err_buf_len)
}

#[no_mangle]
pub extern "C" fn tmsl_store_open_with_config(
    data_dir: *const c_char,
    config: *const TmslStoreConfigFFI,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    run_ptr(err_buf, err_buf_len, || {
        let data_dir = cstr_to_str(data_dir, "data_dir")?;
        let store = Store::open(data_dir, store_config_from_ffi(config)?)?;
        Ok(Box::into_raw(Box::new(FfiStore {
            inner: Mutex::new(Some(store)),
        })) as *mut c_void)
    })
}

#[no_mangle]
pub extern "C" fn tmsl_store_close(
    store: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if store.is_null() {
            return Err(invalid_data("store is null"));
        }
        let boxed = unsafe { Box::from_raw(store as *mut FfiStore) };
        let store = boxed
            .inner
            .into_inner()
            .map_err(|_| invalid_data("store mutex poisoned"))?
            .ok_or_else(|| invalid_data("store is closed"))?;
        store.close()?;
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn tmsl_store_tick_background_tasks(
    store: *mut c_void,
    out_executed: *mut u32,
    out_next_delay_ms: *mut u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_executed.is_null() || out_next_delay_ms.is_null() {
            return Err(invalid_data("output pointer is null"));
        }
        with_store_mut(store, |store| {
            let result = store.tick_background_tasks()?;
            unsafe {
                *out_executed = result.executed_tasks as u32;
                *out_next_delay_ms = duration_millis(result.next_delay);
            }
            Ok(0)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_store_next_background_delay(
    store: *mut c_void,
    out_next_delay_ms: *mut u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_next_delay_ms.is_null() {
            return Err(invalid_data("out_next_delay_ms is null"));
        }
        with_store_mut(store, |store| {
            let delay = store.next_background_delay()?;
            unsafe {
                *out_next_delay_ms = duration_millis(delay);
            }
            Ok(0)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_free_string_array(arr: *mut *mut c_char, count: u32) {
    if arr.is_null() {
        return;
    }
    unsafe {
        for idx in 0..count as usize {
            let ptr = *arr.add(idx);
            if !ptr.is_null() {
                drop(CString::from_raw(ptr));
            }
        }
        free(arr.cast::<c_void>());
    }
}

#[no_mangle]
pub extern "C" fn tmsl_store_get_dataset_names(
    store: *mut c_void,
    out_names: *mut *mut *mut c_char,
    out_count: *mut u32,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        with_store_mut(store, |store| {
            write_string_array(store.get_dataset_names()?, out_names, out_count)?;
            Ok(0)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_store_get_dataset_types(
    store: *mut c_void,
    name: *const c_char,
    out_types: *mut *mut *mut c_char,
    out_count: *mut u32,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let name = cstr_to_str(name, "name")?;
        with_store_mut(store, |store| {
            write_string_array(store.get_dataset_types(name)?, out_types, out_count)?;
            Ok(0)
        })
    })
}

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
    run_ptr(err_buf, err_buf_len, || {
        let name = cstr_to_str(name, "name")?;
        let dataset_type = cstr_to_str(dataset_type, "dataset_type")?;
        with_store_mut(store, |store| {
            let dataset = store.create_dataset(
                name,
                dataset_type,
                data_segment_size,
                index_segment_size,
                compress_level,
                index_continuous,
                retention_window,
            )?;
            Ok(Box::into_raw(Box::new(FfiDataset { dataset })) as *mut c_void)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_create_with_config(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    config: *const TmslDatasetConfigFFI,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    run_ptr(err_buf, err_buf_len, || {
        let name = cstr_to_str(name, "name")?;
        let dataset_type = cstr_to_str(dataset_type, "dataset_type")?;
        let config = dataset_config_from_ffi(config)?;
        with_store_mut(store, |store| {
            let dataset = store.create_dataset_with_config(name, dataset_type, Some(config))?;
            Ok(Box::into_raw(Box::new(FfiDataset { dataset })) as *mut c_void)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_open(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    run_ptr(err_buf, err_buf_len, || {
        let name = cstr_to_str(name, "name")?;
        let dataset_type = cstr_to_str(dataset_type, "dataset_type")?;
        with_store_mut(store, |store| {
            let dataset = store.open_dataset(name, dataset_type)?;
            Ok(Box::into_raw(Box::new(FfiDataset { dataset })) as *mut c_void)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_open_by_identifier(
    store: *mut c_void,
    identifier: u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    run_ptr(err_buf, err_buf_len, || {
        with_store_mut(store, |store| {
            let dataset = store.open_dataset_by_identifier(identifier)?;
            Ok(Box::into_raw(Box::new(FfiDataset { dataset })) as *mut c_void)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_close(
    dataset: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if dataset.is_null() {
            return Err(invalid_data("dataset is null"));
        }
        let boxed = unsafe { Box::from_raw(dataset as *mut FfiDataset) };
        boxed.dataset.close()?;
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_drop(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let name = cstr_to_str(name, "name")?;
        let dataset_type = cstr_to_str(dataset_type, "dataset_type")?;
        with_store_mut(store, |store| {
            store.drop_dataset(name, dataset_type)?;
            Ok(0)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_flush(
    dataset: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || with_dataset(dataset, |dataset| dataset.flush().map(|_| 0)))
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_latest_timestamp(
    dataset: *mut c_void,
    out_ts: *mut c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_ts.is_null() {
            return Err(invalid_data("out_ts is null"));
        }
        with_dataset(dataset, |dataset| match dataset.latest_written_timestamp() {
            Some(ts) => {
                unsafe {
                    *out_ts = ts;
                }
                Ok(0)
            }
            None => Ok(1),
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_identifier(
    dataset: *mut c_void,
    out_identifier: *mut u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_identifier.is_null() {
            return Err(invalid_data("out_identifier is null"));
        }
        with_dataset(dataset, |dataset| {
            unsafe {
                *out_identifier = dataset.identifier();
            }
            Ok(0)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_write(
    dataset: *mut c_void,
    timestamp: c_longlong,
    data: *const c_uchar,
    data_len: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let data = checked_input_slice(data, data_len)?;
        with_dataset(dataset, |dataset| dataset.write(timestamp, data).map(|_| 0))
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_append(
    dataset: *mut c_void,
    timestamp: c_longlong,
    data: *const c_uchar,
    data_len: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let data = checked_input_slice(data, data_len)?;
        with_dataset(dataset, |dataset| dataset.append(timestamp, data).map(|_| 0))
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_write_now(
    dataset: *mut c_void,
    data: *const c_uchar,
    data_len: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let data = checked_input_slice(data, data_len)?;
        with_dataset(dataset, |dataset| dataset.write_now(data).map(|_| 0))
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_append_now(
    dataset: *mut c_void,
    data: *const c_uchar,
    data_len: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let data = checked_input_slice(data, data_len)?;
        with_dataset(dataset, |dataset| dataset.append_now(data).map(|_| 0))
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_delete(
    dataset: *mut c_void,
    timestamp: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || with_dataset(dataset, |dataset| dataset.delete(timestamp).map(|_| 0)))
}

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
    run_int(err_buf, err_buf_len, || {
        if out_ts.is_null() {
            return Err(invalid_data("out_ts is null"));
        }
        with_dataset(dataset, |dataset| match dataset.read(timestamp)? {
            Some((ts, data)) => {
                unsafe {
                    *out_ts = ts;
                }
                write_alloc_bytes(&data, out_data, out_data_len)?;
                Ok(0)
            }
            None => Ok(1),
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_read_latest(
    dataset: *mut c_void,
    out_ts: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_ts.is_null() {
            return Err(invalid_data("out_ts is null"));
        }
        with_dataset(dataset, |dataset| match dataset.read_latest()? {
            Some((ts, data)) => {
                unsafe {
                    *out_ts = ts;
                }
                write_alloc_bytes(&data, out_data, out_data_len)?;
                Ok(0)
            }
            None => Ok(1),
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_query(
    dataset: *mut c_void,
    start_ts: c_longlong,
    end_ts: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    run_ptr(err_buf, err_buf_len, || {
        with_dataset(dataset, |dataset| {
            let rows = dataset.query(start_ts, end_ts)?;
            Ok(Box::into_raw(Box::new(FfiDataIter { rows, position: 0 })) as *mut c_void)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_iter_next(
    iter: *mut c_void,
    out_ts: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if iter.is_null() || out_ts.is_null() {
            return Err(invalid_data("null pointer"));
        }
        let iter = unsafe { &mut *(iter as *mut FfiDataIter) };
        if iter.position >= iter.rows.len() {
            return Ok(1);
        }
        let (ts, data) = &iter.rows[iter.position];
        iter.position += 1;
        unsafe {
            *out_ts = *ts;
        }
        write_alloc_bytes(data, out_data, out_data_len)?;
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn tmsl_iter_close(iter: *mut c_void) {
    if !iter.is_null() {
        unsafe {
            drop(Box::from_raw(iter as *mut FfiDataIter));
        }
    }
}

#[no_mangle]
pub extern "C" fn tmsl_data_free(data: *mut c_void) {
    if !data.is_null() {
        unsafe {
            free(data);
        }
    }
}

#[no_mangle]
pub extern "C" fn tmsl_iter_free_data(data: *mut c_uchar) {
    tmsl_data_free(data.cast::<c_void>());
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_read_exist(
    dataset: *mut c_void,
    timestamp: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        with_dataset(dataset, |dataset| Ok(dataset.read_exist(timestamp)? as c_int))
    })
}

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
    run_int(err_buf, err_buf_len, || {
        with_dataset(dataset, |dataset| {
            let bitmap = dataset.query_exist(start_ts, end_ts)?;
            write_alloc_bytes(&bitmap, out_bitmap, out_bitmap_len)?;
            Ok(0)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_read_length(
    dataset: *mut c_void,
    timestamp: c_longlong,
    out_len: *mut u32,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_len.is_null() {
            return Err(invalid_data("out_len is null"));
        }
        with_dataset(dataset, |dataset| match dataset.read_length(timestamp)? {
            Some(len) => {
                unsafe {
                    *out_len = len;
                }
                Ok(0)
            }
            None => Ok(1),
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_query_length(
    dataset: *mut c_void,
    start_ts: c_longlong,
    end_ts: c_longlong,
    out_array: *mut *mut TmslLengthEntry,
    out_array_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_array.is_null() || out_array_len.is_null() {
            return Err(invalid_data("output pointer is null"));
        }
        with_dataset(dataset, |dataset| {
            let rows = dataset.query_length(start_ts, end_ts)?;
            if rows.is_empty() {
                unsafe {
                    *out_array = ptr::null_mut();
                    *out_array_len = 0;
                }
                return Ok(0);
            }
            let bytes = rows.len() * std::mem::size_of::<TmslLengthEntry>();
            let array = unsafe { malloc(bytes) }.cast::<TmslLengthEntry>();
            if array.is_null() {
                return Err(invalid_data("malloc failed"));
            }
            for (idx, (timestamp, data_len)) in rows.into_iter().enumerate() {
                unsafe {
                    *array.add(idx) = TmslLengthEntry {
                        timestamp,
                        data_len,
                    };
                }
            }
            unsafe {
                *out_array = array;
                *out_array_len = bytes / std::mem::size_of::<TmslLengthEntry>();
            }
            Ok(0)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_dataset_query_length_iter(
    dataset: *mut c_void,
    start_ts: c_longlong,
    end_ts: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    run_ptr(err_buf, err_buf_len, || {
        with_dataset(dataset, |dataset| {
            let iter = dataset.query_length_iter(start_ts, end_ts)?;
            Ok(Box::into_raw(Box::new(FfiLengthIter { iter })) as *mut c_void)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_length_iter_next(
    iter: *mut c_void,
    out_ts: *mut c_longlong,
    out_len: *mut u32,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if iter.is_null() || out_ts.is_null() || out_len.is_null() {
            return Err(invalid_data("null pointer"));
        }
        let iter = unsafe { &mut *(iter as *mut FfiLengthIter) };
        match iter.iter.next_entry()? {
            Some((ts, len)) => {
                unsafe {
                    *out_ts = ts;
                    *out_len = len;
                }
                Ok(0)
            }
            None => Ok(1),
        }
    })
}

#[no_mangle]
pub extern "C" fn tmsl_length_iter_close(iter: *mut c_void) {
    if !iter.is_null() {
        unsafe {
            drop(Box::from_raw(iter as *mut FfiLengthIter));
        }
    }
}

#[no_mangle]
pub extern "C" fn tmsl_queue_open(
    dataset: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> usize {
    run_usize(err_buf, err_buf_len, || {
        with_dataset(dataset, |dataset| register_queue(QueueKind::Dataset(dataset.open_queue()?)))
    })
}

#[no_mangle]
pub extern "C" fn tmsl_queue_close(
    queue_handle: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let entry = QUEUES
            .lock()
            .map_err(|_| invalid_data("queue registry mutex poisoned"))?
            .remove(&queue_handle)
            .ok_or_else(|| invalid_data("invalid queue handle"))?;
        remove_consumers_for_queue(queue_handle)?;
        match entry.kind {
            QueueKind::Dataset(queue) => queue.close()?,
            QueueKind::Journal(queue) => queue.close()?,
        }
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn tmsl_queue_consumer_open(
    queue_handle: usize,
    group_name: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> usize {
    tmsl_queue_consumer_open_with_config(
        queue_handle,
        group_name,
        ptr::null(),
        err_buf,
        err_buf_len,
    )
}

#[no_mangle]
pub extern "C" fn tmsl_queue_consumer_open_with_config(
    queue_handle: usize,
    group_name: *const c_char,
    config: *const TmslQueueConsumerConfigFFI,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> usize {
    run_usize(err_buf, err_buf_len, || {
        let group_name = cstr_to_str(group_name, "group_name")?.to_string();
        let config = consumer_config_from_ffi(config)?;
        match queue_kind(queue_handle)? {
            QueueKind::Dataset(queue) => {
                let consumer = queue.open_consumer_with_config(&group_name, config)?;
                register_consumer(queue_handle, group_name, ConsumerKind::Dataset(consumer))
            }
            QueueKind::Journal(_) => Err(invalid_data(
                "journal queue requires tmsl_journal_queue_consumer_open",
            )),
        }
    })
}

#[no_mangle]
pub extern "C" fn tmsl_queue_consumer_drop(
    queue_handle: usize,
    consumer_handle: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let mut consumers = CONSUMERS
            .lock()
            .map_err(|_| invalid_data("consumer registry mutex poisoned"))?;
        let consumer = consumers
            .remove(&consumer_handle)
            .ok_or_else(|| invalid_data("invalid consumer handle"))?;
        if consumer.queue_id != queue_handle {
            return Err(invalid_data("consumer does not belong to queue"));
        }
        drop(consumers);
        match queue_kind(queue_handle)? {
            QueueKind::Dataset(queue) => queue.drop_consumer(&consumer.group_name)?,
            QueueKind::Journal(_) => {
                return Err(invalid_data("journal consumer drop is not supported"))
            }
        }
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn tmsl_queue_push(
    queue_handle: usize,
    data: *const c_uchar,
    data_len: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_longlong {
    run_i64(err_buf, err_buf_len, || {
        let data = checked_input_slice(data, data_len)?;
        match queue_kind(queue_handle)? {
            QueueKind::Dataset(queue) => Ok(queue.push(data)?),
            QueueKind::Journal(_) => Err(invalid_data("cannot push to journal queue")),
        }
    })
}

#[no_mangle]
pub extern "C" fn tmsl_queue_poll(
    consumer_handle: usize,
    timeout_ms: c_longlong,
    out_timestamp: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_timestamp.is_null() {
            return Err(invalid_data("out_timestamp is null"));
        }
        let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
        match consumer_kind(consumer_handle)? {
            ConsumerKind::Dataset(consumer) => match consumer.poll(timeout)? {
                Some((ts, data)) => {
                    unsafe {
                        *out_timestamp = ts;
                    }
                    write_alloc_bytes(&data, out_data, out_data_len)?;
                    Ok(0)
                }
                None => Ok(-2),
            },
            ConsumerKind::Journal(_) => Err(invalid_data(
                "journal consumer requires tmsl_journal_queue_poll",
            )),
        }
    })
}

#[no_mangle]
pub extern "C" fn tmsl_queue_ack(
    consumer_handle: usize,
    timestamp: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || match consumer_kind(consumer_handle)? {
        ConsumerKind::Dataset(consumer) => consumer.ack(timestamp).map(|_| 0),
        ConsumerKind::Journal(_) => Err(invalid_data(
            "journal consumer requires tmsl_journal_queue_ack",
        )),
    })
}

#[no_mangle]
pub extern "C" fn tmsl_queue_consumer_poll_callback(
    consumer_handle: usize,
    callback: TmslQueuePollCallback,
    userdata: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let callback: Option<QueuePollCallback> = callback.map(|cb| {
            let userdata = userdata as usize;
            Arc::new(move || cb(userdata as *mut c_void)) as QueuePollCallback
        });
        match consumer_kind(consumer_handle)? {
            ConsumerKind::Dataset(consumer) => consumer.poll_callback(callback).map(|_| 0),
            ConsumerKind::Journal(_) => Err(invalid_data(
                "journal consumer requires tmsl_journal_queue_consumer_poll_callback",
            )),
        }
    })
}

#[no_mangle]
pub extern "C" fn tmsl_journal_latest_sequence(
    store: *mut c_void,
    out_sequence: *mut c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_sequence.is_null() {
            return Err(invalid_data("out_sequence is null"));
        }
        with_store_mut(store, |store| {
            unsafe {
                *out_sequence = store.journal_latest_sequence()?.unwrap_or(0);
            }
            Ok(0)
        })
    })
}

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
    run_int(err_buf, err_buf_len, || {
        if out_sequence.is_null() {
            return Err(invalid_data("out_sequence is null"));
        }
        with_store_mut(store, |store| match store.journal_read(sequence)? {
            Some((seq, data)) => {
                unsafe {
                    *out_sequence = seq;
                }
                write_alloc_bytes(&data, out_data, out_data_len)?;
                Ok(0)
            }
            None => Ok(1),
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_journal_query(
    store: *mut c_void,
    start_sequence: c_longlong,
    end_sequence: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    run_ptr(err_buf, err_buf_len, || {
        with_store_mut(store, |store| {
            let rows = store.journal_query(start_sequence, end_sequence)?;
            Ok(Box::into_raw(Box::new(FfiDataIter { rows, position: 0 })) as *mut c_void)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_journal_iter_next(
    iter: *mut c_void,
    out_sequence: *mut c_longlong,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    tmsl_iter_next(
        iter,
        out_sequence,
        out_data,
        out_data_len,
        err_buf,
        err_buf_len,
    )
}

#[no_mangle]
pub extern "C" fn tmsl_journal_iter_close(iter: *mut c_void) {
    tmsl_iter_close(iter);
}

#[no_mangle]
pub extern "C" fn tmsl_journal_queue_open(
    store: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> usize {
    run_usize(err_buf, err_buf_len, || {
        with_store_mut(store, |store| register_queue(QueueKind::Journal(store.open_journal_queue()?)))
    })
}

#[no_mangle]
pub extern "C" fn tmsl_journal_queue_close(
    queue_handle: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    tmsl_queue_close(queue_handle, err_buf, err_buf_len)
}

#[no_mangle]
pub extern "C" fn tmsl_journal_queue_consumer_open(
    queue_handle: usize,
    group_name: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> usize {
    tmsl_journal_queue_consumer_open_with_config(
        queue_handle,
        group_name,
        ptr::null(),
        err_buf,
        err_buf_len,
    )
}

#[no_mangle]
pub extern "C" fn tmsl_journal_queue_consumer_open_with_config(
    queue_handle: usize,
    group_name: *const c_char,
    config: *const TmslQueueConsumerConfigFFI,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> usize {
    run_usize(err_buf, err_buf_len, || {
        let group_name = cstr_to_str(group_name, "group_name")?.to_string();
        let config = consumer_config_from_ffi(config)?;
        match queue_kind(queue_handle)? {
            QueueKind::Journal(queue) => {
                let consumer = queue.open_consumer_with_config(&group_name, config)?;
                register_consumer(queue_handle, group_name, ConsumerKind::Journal(consumer))
            }
            QueueKind::Dataset(_) => Err(invalid_data(
                "dataset queue requires tmsl_queue_consumer_open",
            )),
        }
    })
}

#[no_mangle]
pub extern "C" fn tmsl_journal_queue_consumer_poll_callback(
    consumer_handle: usize,
    callback: TmslQueuePollCallback,
    userdata: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        let callback: Option<QueuePollCallback> = callback.map(|cb| {
            let userdata = userdata as usize;
            Arc::new(move || cb(userdata as *mut c_void)) as QueuePollCallback
        });
        match consumer_kind(consumer_handle)? {
            ConsumerKind::Journal(consumer) => consumer.poll_callback(callback).map(|_| 0),
            ConsumerKind::Dataset(_) => Err(invalid_data(
                "dataset consumer requires tmsl_queue_consumer_poll_callback",
            )),
        }
    })
}

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
    run_int(err_buf, err_buf_len, || {
        if out_sequence.is_null() {
            return Err(invalid_data("out_sequence is null"));
        }
        let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
        match consumer_kind(consumer_handle)? {
            ConsumerKind::Journal(consumer) => match consumer.poll(timeout)? {
                Some((seq, data)) => {
                    unsafe {
                        *out_sequence = seq;
                    }
                    write_alloc_bytes(&data, out_data, out_data_len)?;
                    Ok(0)
                }
                None => Ok(-2),
            },
            ConsumerKind::Dataset(_) => {
                Err(invalid_data("dataset consumer requires tmsl_queue_poll"))
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn tmsl_journal_queue_ack(
    consumer_handle: usize,
    sequence: c_longlong,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || match consumer_kind(consumer_handle)? {
        ConsumerKind::Journal(consumer) => consumer.ack(sequence).map(|_| 0),
        ConsumerKind::Dataset(_) => Err(invalid_data("dataset consumer requires tmsl_queue_ack")),
    })
}

fn fill_inspect_result(
    inspect: DataSetInspectResult,
    out_result: *mut TmslInspectResult,
) -> timslite::Result<()> {
    let latest = inspect.state.latest_written_timestamp.unwrap_or(0);
    let min_ts = inspect.state.min_timestamp.unwrap_or(0);
    let max_ts = inspect.state.max_timestamp.unwrap_or(0);
    let base_ts = inspect.state.base_timestamp.unwrap_or(0);
    unsafe {
        *out_result = TmslInspectResult {
            info: TmslDataSetInfo {
                name: cstring_ptr(inspect.info.name)?,
                dataset_type: cstring_ptr(inspect.info.dataset_type)?,
                base_dir: cstring_ptr(inspect.info.base_dir)?,
                identifier: inspect.info.identifier,
                data_segment_size: inspect.info.data_segment_size,
                index_segment_size: inspect.info.index_segment_size,
                initial_data_segment_size: inspect.info.initial_data_segment_size,
                initial_index_segment_size: inspect.info.initial_index_segment_size,
                compress_type: inspect.info.compress_type,
                compress_level: inspect.info.compress_level,
                index_continuous: inspect.info.index_continuous,
                retention_window: inspect.info.retention_window,
                enable_journal: inspect.info.enable_journal as u8,
                create_time: inspect.info.create_time,
            },
            state: TmslDataSetState {
                has_latest_written_timestamp: inspect.state.latest_written_timestamp.is_some() as u8,
                latest_written_timestamp: latest,
                open_data_segments: inspect.state.open_data_segments,
                data_segments: inspect.state.data_segments,
                total_record_count: inspect.state.total_record_count,
                total_data_size: inspect.state.total_data_size,
                total_uncompressed_size: inspect.state.total_uncompressed_size,
                total_invalid_record_count: inspect.state.total_invalid_record_count,
                has_min_timestamp: inspect.state.min_timestamp.is_some() as u8,
                min_timestamp: min_ts,
                has_max_timestamp: inspect.state.max_timestamp.is_some() as u8,
                max_timestamp: max_ts,
                open_index_segments: inspect.state.open_index_segments,
                index_segments: inspect.state.index_segments,
                pending_index_entries: inspect.state.pending_index_entries,
                has_base_timestamp: inspect.state.base_timestamp.is_some() as u8,
                base_timestamp: base_ts,
                read_only: inspect.state.read_only as u8,
                has_block_cache: inspect.state.has_block_cache as u8,
                has_journal: inspect.state.has_journal as u8,
                has_queue: inspect.state.has_queue as u8,
                queue_consumer_groups: inspect.state.queue_consumer_groups,
            },
        };
    }
    Ok(())
}

#[no_mangle]
pub extern "C" fn tmsl_store_inspect_dataset(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    out_result: *mut TmslInspectResult,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    run_int(err_buf, err_buf_len, || {
        if out_result.is_null() {
            return Err(invalid_data("out_result is null"));
        }
        let name = cstr_to_str(name, "name")?;
        let dataset_type = cstr_to_str(dataset_type, "dataset_type")?;
        with_store_mut(store, |store| {
            let inspect = store.inspect_dataset(name, dataset_type)?;
            fill_inspect_result(inspect, out_result)?;
            Ok(0)
        })
    })
}

#[no_mangle]
pub extern "C" fn tmsl_free_inspect_result(result: *mut TmslInspectResult) {
    if result.is_null() {
        return;
    }
    unsafe {
        let info = &mut (*result).info;
        if !info.name.is_null() {
            drop(CString::from_raw(info.name));
            info.name = ptr::null_mut();
        }
        if !info.dataset_type.is_null() {
            drop(CString::from_raw(info.dataset_type));
            info.dataset_type = ptr::null_mut();
        }
        if !info.base_dir.is_null() {
            drop(CString::from_raw(info.base_dir));
            info.base_dir = ptr::null_mut();
        }
    }
}
