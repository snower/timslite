//! C FFI bindings for timslite
//!
//! This module provides C-compatible function interfaces for using timslite
//! from other programming languages.

use std::ffi::{CStr, CString};
use std::ptr;
use std::slice;

use crate::{DataType, Dataset, Error, Result, TimeStore};
use std::sync::Arc;

/// Opaque handle to TimeStore
pub struct TimeStoreHandle {
    inner: Arc<TimeStore>,
}

/// Opaque handle to Dataset
pub struct DatasetHandle {
    inner: Arc<Dataset>,
}

/// Open a time-series store
#[no_mangle]
pub extern "C" fn timslite_open(data_dir: *const i8) -> *mut TimeStoreHandle {
    if data_dir.is_null() {
        return ptr::null_mut();
    }

    let data_dir = unsafe { CStr::from_ptr(data_dir) };
    let data_dir = match data_dir.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    match TimeStore::open(data_dir) {
        Ok(store) => Box::into_raw(Box::new(TimeStoreHandle {
            inner: Arc::new(store),
        })),
        Err(_) => ptr::null_mut(),
    }
}

/// Close a time-series store
#[no_mangle]
pub extern "C" fn timslite_close(handle: *mut TimeStoreHandle) {
    if !handle.is_null() {
        unsafe {
            let handle = Box::from_raw(handle);
            let _ = handle.inner.close();
        }
    }
}

/// Open a dataset
#[no_mangle]
pub extern "C" fn timslite_open_dataset(
    handle: *mut TimeStoreHandle,
    name: *const i8,
    data_type: i32,
) -> *mut DatasetHandle {
    if handle.is_null() || name.is_null() {
        return ptr::null_mut();
    }

    let handle = unsafe { &*handle };
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let data_type = match DataType::from_i32(data_type) {
        Some(dt) => dt,
        None => return ptr::null_mut(),
    };

    match handle.inner.open_dataset(name, data_type) {
        Ok(dataset) => Box::into_raw(Box::new(DatasetHandle { inner: dataset })),
        Err(_) => ptr::null_mut(),
    }
}

/// Close a dataset
#[no_mangle]
pub extern "C" fn timslite_close_dataset(handle: *mut DatasetHandle) {
    if !handle.is_null() {
        unsafe {
            let handle = Box::from_raw(handle);
            let _ = handle.inner.close();
        }
    }
}

/// Write data to dataset
#[no_mangle]
pub extern "C" fn timslite_write(
    handle: *mut DatasetHandle,
    timestamp: i64,
    data: *const u8,
    data_len: usize,
) -> i64 {
    if handle.is_null() || data.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    let data = unsafe { slice::from_raw_parts(data, data_len) };

    match handle.inner.write(timestamp, data) {
        Ok(offset) => offset,
        Err(_) => -1,
    }
}

/// Read data from dataset
#[no_mangle]
pub extern "C" fn timslite_read(
    handle: *mut DatasetHandle,
    start_timestamp: i64,
    end_timestamp: i64,
    out_data: *mut u8,
    out_data_len: *mut usize,
) -> i32 {
    if handle.is_null() || out_data.is_null() || out_data_len.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    let options = crate::types::ReadOptions {
        start_timestamp,
        end_timestamp,
        ..Default::default()
    };

    match handle.inner.read(&options) {
        Ok(records) => {
            // For simplicity, just return the count
            unsafe {
                *out_data_len = records.len();
            }
            0
        }
        Err(_) => -1,
    }
}

/// Flush dataset to disk
#[no_mangle]
pub extern "C" fn timslite_flush(handle: *mut DatasetHandle) -> i32 {
    if handle.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    match handle.inner.flush() {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

/// Get error message for last error
#[no_mangle]
pub extern "C" fn timslite_error_message(error_code: i32) -> *mut i8 {
    let msg = match error_code {
        -1 => "Unknown error",
        0 => "Success",
        _ => "Invalid error code",
    };

    match CString::new(msg) {
        Ok(cstr) => cstr.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Free error message
#[no_mangle]
pub extern "C" fn timslite_free_string(s: *mut i8) {
    if !s.is_null() {
        unsafe {
            let _ = CString::from_raw(s);
        }
    }
}
