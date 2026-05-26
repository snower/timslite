//! FFI interface (extern "C" API) for C and other language bindings.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong, c_uchar, c_void};

use crate::config::StoreConfig;
use crate::error::TmslError;
use crate::store::{DataSetHandle, Store};

// ─── FFI error handling helpers ─────────────────────────────────────────────

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

// ─── Opaque handle types ────────────────────────────────────────────────────

struct FfiStore(*mut Store);
unsafe impl Send for FfiStore {}
unsafe impl Sync for FfiStore {}

struct FfiDataset {
    store_ptr: *mut Store,
    handle: DataSetHandle,
}
unsafe impl Send for FfiDataset {}
unsafe impl Sync for FfiDataset {}

struct FfiIterator {
    store_ptr: *mut Store,
    handle: DataSetHandle,
    entries: Vec<(i64, Vec<u8>)>,
    index: usize,
}
unsafe impl Send for FfiIterator {}
unsafe impl Sync for FfiIterator {}

// ─── Store Management ───────────────────────────────────────────────────────

/// Open a store. Returns opaque pointer or NULL on error.
#[no_mangle]
pub extern "C" fn tmsl_store_open(
    data_dir: *const c_char,
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
        let config = StoreConfig::default();
        let store = Store::open(dir, config).map_err(|e| {
            TmslError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        let boxed = Box::new(FfiStore(Box::into_raw(Box::new(store))));
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
        let ffi_store = unsafe { Box::from_raw(store as *mut FfiStore) };
        let inner = unsafe { Box::from_raw(ffi_store.0) };
        inner.close().map_err(|e| {
            TmslError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        Ok(0)
    })
}

// ─── Dataset Management ────────────────────────────────────────────────────

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
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    ffi_catch_ptr!(err_buf, err_buf_len, {
        if store.is_null() || name.is_null() || dataset_type.is_null() {
            return Err(TmslError::InvalidData("null pointer".into()));
        }
        let ffi_store = unsafe { &mut *(store as *mut FfiStore) };
        let store_inner = unsafe { &mut *(ffi_store.0) };
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
        )?;
        let boxed = Box::new(FfiDataset {
            store_ptr: ffi_store.0,
            handle,
        });
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
        let store_inner = unsafe { &mut *(ffi_store.0) };
        let name_str = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid name: {}", e)))?;
        let type_str = unsafe { CStr::from_ptr(dataset_type) }
            .to_str()
            .map_err(|e| TmslError::InvalidData(format!("invalid type: {}", e)))?;

        let handle = store_inner.open_dataset(name_str, type_str)?;
        let boxed = Box::new(FfiDataset {
            store_ptr: ffi_store.0,
            handle,
        });
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
        let ffi_ds = unsafe { Box::from_raw(dataset as *mut FfiDataset) };
        let store_inner = unsafe { &mut *(ffi_ds.store_ptr) };
        store_inner.close_dataset(ffi_ds.handle).map_err(|e| {
            TmslError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
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
        let store_inner = unsafe { &mut *(ffi_store.0) };
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

/// Close and free the iterator.
#[no_mangle]
pub extern "C" fn tmsl_iter_close(iter: *mut c_void) {
    if !iter.is_null() {
        let _ = unsafe { Box::from_raw(iter as *mut FfiIterator) };
    }
}

// ─── Query Iterator ────────────────────────────────────────────────────────

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
        let entries = ds.query(start_ts, end_ts, Some(&*store_inner.block_cache()))?;

        let iter = Box::new(FfiIterator {
            store_ptr: ffi_ds.store_ptr,
            handle: ffi_ds.handle.clone(),
            entries,
            index: 0,
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

        if ffi_iter.index >= ffi_iter.entries.len() {
            return Ok(1); // exhausted
        }

        let (ts, data) = &ffi_iter.entries[ffi_iter.index];
        ffi_iter.index += 1;

        unsafe { *out_ts = *ts as c_longlong };

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

/// Free data allocated by tmsl_iter_next.
#[no_mangle]
pub extern "C" fn tmsl_iter_free_data(data: *mut c_uchar) {
    if !data.is_null() {
        unsafe { libc::free(data as *mut _) };
    }
}
