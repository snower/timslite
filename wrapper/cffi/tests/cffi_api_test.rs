use std::ffi::CString;
use std::os::raw::{c_char, c_uchar, c_void};
use std::ptr;

use timslitecffi::*;

fn err_buf() -> Vec<c_char> {
    vec![0; 512]
}

#[test]
fn cffi_dataset_read_and_queue_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let dir = CString::new(dir.path().to_string_lossy().as_bytes()).unwrap();
    let name = CString::new("sensor").unwrap();
    let kind = CString::new("temperature").unwrap();
    let group = CString::new("g1").unwrap();
    let mut err = err_buf();

    let store = tmsl_store_open(dir.as_ptr(), err.as_mut_ptr(), err.len());
    assert!(!store.is_null());

    let dataset = tmsl_dataset_create(
        store,
        name.as_ptr(),
        kind.as_ptr(),
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
        err.as_mut_ptr(),
        err.len(),
    );
    assert!(!dataset.is_null());

    let payload = b"21.5";
    assert_eq!(
        tmsl_dataset_write(
            dataset,
            100,
            payload.as_ptr(),
            payload.len(),
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );

    let mut ts = 0i64;
    let mut data: *mut c_uchar = ptr::null_mut();
    let mut data_len = 0usize;
    assert_eq!(
        tmsl_dataset_read(
            dataset,
            100,
            &mut ts,
            &mut data,
            &mut data_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    assert_eq!(ts, 100);
    let read = unsafe { std::slice::from_raw_parts(data, data_len) };
    assert_eq!(read, payload);
    tmsl_data_free(data.cast::<c_void>());

    let queue = tmsl_queue_open(dataset, err.as_mut_ptr(), err.len());
    assert_ne!(queue, 0);
    let consumer = tmsl_queue_consumer_open(queue, group.as_ptr(), err.as_mut_ptr(), err.len());
    assert_ne!(consumer, 0);
    let queued = b"queued";
    let queued_ts = tmsl_queue_push(
        queue,
        queued.as_ptr(),
        queued.len(),
        err.as_mut_ptr(),
        err.len(),
    );
    assert!(queued_ts > 0);

    let mut poll_ts = 0i64;
    let mut poll_data: *mut c_uchar = ptr::null_mut();
    let mut poll_len = 0usize;
    assert_eq!(
        tmsl_queue_poll(
            consumer,
            100,
            &mut poll_ts,
            &mut poll_data,
            &mut poll_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    assert_eq!(poll_ts, queued_ts);
    let polled = unsafe { std::slice::from_raw_parts(poll_data, poll_len) };
    assert_eq!(polled, queued);
    tmsl_data_free(poll_data.cast::<c_void>());
    assert_eq!(tmsl_queue_ack(consumer, poll_ts, err.as_mut_ptr(), err.len()), 0);

    assert_eq!(tmsl_queue_close(queue, err.as_mut_ptr(), err.len()), 0);
    assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()), 0);
    assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err.len()), 0);
}

#[test]
fn cffi_dataset_write_now_and_append_now() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let dir = tempfile::tempdir().unwrap();
    let dir = CString::new(dir.path().to_string_lossy().as_bytes()).unwrap();
    let name = CString::new("nowapi").unwrap();
    let kind = CString::new("test").unwrap();
    let mut err = err_buf();

    let store = tmsl_store_open(dir.as_ptr(), err.as_mut_ptr(), err.len());
    assert!(!store.is_null());

    let dataset = tmsl_dataset_create(
        store,
        name.as_ptr(),
        kind.as_ptr(),
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
        err.as_mut_ptr(),
        err.len(),
    );
    assert!(!dataset.is_null());

    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let payload = b"hello_now";
    assert_eq!(
        tmsl_dataset_write_now(
            dataset,
            payload.as_ptr(),
            payload.len(),
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );

    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Read latest to verify timestamp is in expected range
    let mut ts = 0i64;
    let mut data: *mut c_uchar = ptr::null_mut();
    let mut data_len = 0usize;
    assert_eq!(
        tmsl_dataset_read_latest(
            dataset,
            &mut ts,
            &mut data,
            &mut data_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    assert!(
        ts >= before && ts <= after,
        "write_now timestamp {ts} should be in [{before}, {after}]"
    );
    let read = unsafe { std::slice::from_raw_parts(data, data_len) };
    assert_eq!(read, payload);
    tmsl_data_free(data.cast::<c_void>());

    // Test append_now
    let append_payload = b"-appended";
    assert_eq!(
        tmsl_dataset_append_now(
            dataset,
            append_payload.as_ptr(),
            append_payload.len(),
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );

    tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len());
    tmsl_store_close(store, err.as_mut_ptr(), err.len());
}
