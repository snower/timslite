use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_uchar, c_void};
use std::ptr;

use timslitecffi::*;

fn err_buf() -> Vec<c_char> {
    vec![0; 512]
}

unsafe fn c_string_array_to_vec(names: *mut *mut c_char, count: u32) -> Vec<String> {
    (0..count as usize)
        .map(|idx| CStr::from_ptr(*names.add(idx)).to_string_lossy().into_owned())
        .collect()
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
fn cffi_queue_consumer_group_names_inspect_flush_and_close() {
    let dir = tempfile::tempdir().unwrap();
    let dir = CString::new(dir.path().to_string_lossy().as_bytes()).unwrap();
    let name = CString::new("queueapi").unwrap();
    let kind = CString::new("events").unwrap();
    let group = CString::new("g1").unwrap();
    let group_b = CString::new("g2").unwrap();
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
    let queue = tmsl_queue_open(dataset, err.as_mut_ptr(), err.len());
    assert_ne!(queue, 0);

    let c1 = tmsl_queue_consumer_open(queue, group.as_ptr(), err.as_mut_ptr(), err.len());
    assert_ne!(c1, 0);
    let c1_alias = tmsl_queue_consumer_open(queue, group.as_ptr(), err.as_mut_ptr(), err.len());
    assert_ne!(c1_alias, 0);
    let c2 = tmsl_queue_consumer_open(queue, group_b.as_ptr(), err.as_mut_ptr(), err.len());
    assert_ne!(c2, 0);

    let mut names: *mut *mut c_char = ptr::null_mut();
    let mut names_count = 0u32;
    assert_eq!(
        tmsl_queue_get_consumer_group_names(
            queue,
            &mut names,
            &mut names_count,
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    let listed = unsafe { c_string_array_to_vec(names, names_count) };
    assert_eq!(listed, vec!["g1".to_string(), "g2".to_string()]);
    tmsl_free_string_array(names, names_count);

    let payload = b"pending";
    let queued_ts = tmsl_queue_push(
        queue,
        payload.as_ptr(),
        payload.len(),
        err.as_mut_ptr(),
        err.len(),
    );
    assert!(queued_ts > 0);

    let mut poll_ts = 0i64;
    let mut poll_data: *mut c_uchar = ptr::null_mut();
    let mut poll_len = 0usize;
    assert_eq!(
        tmsl_queue_poll(
            c1,
            100,
            &mut poll_ts,
            &mut poll_data,
            &mut poll_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    tmsl_data_free(poll_data.cast::<c_void>());
    assert_eq!(poll_ts, queued_ts);

    let mut inspect: TmslQueueConsumerInspectResultFFI = unsafe { std::mem::zeroed() };
    assert_eq!(
        tmsl_queue_consumer_inspect(c1, &mut inspect, err.as_mut_ptr(), err.len()),
        0
    );
    let inspected_group = unsafe { CStr::from_ptr(inspect.info.group_name) }
        .to_string_lossy()
        .into_owned();
    assert_eq!(inspected_group, "g1");
    assert_eq!(inspect.state.pending_entries_count, 1);
    assert_eq!(unsafe { (*inspect.state.pending_entries).timestamp }, queued_ts);
    tmsl_queue_consumer_inspect_result_free(&mut inspect);
    assert_eq!(tmsl_queue_consumer_flush(c1, err.as_mut_ptr(), err.len()), 0);

    assert_eq!(
        tmsl_queue_consumer_close(queue, c1, err.as_mut_ptr(), err.len()),
        0
    );
    assert_eq!(
        tmsl_queue_poll(
            c1_alias,
            0,
            &mut poll_ts,
            &mut poll_data,
            &mut poll_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        -1
    );

    let reopened = tmsl_queue_consumer_open(queue, group.as_ptr(), err.as_mut_ptr(), err.len());
    assert_ne!(reopened, 0);
    assert_eq!(
        tmsl_queue_poll(
            reopened,
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
    tmsl_data_free(poll_data.cast::<c_void>());
    assert_eq!(
        tmsl_queue_ack(reopened, poll_ts, err.as_mut_ptr(), err.len()),
        0
    );

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
