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

    let payload_minus_two = b"minus-two";
    assert_eq!(
        tmsl_dataset_write(
            dataset,
            -2,
            payload_minus_two.as_ptr(),
            payload_minus_two.len(),
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    let payload_minus_one = b"minus-one";
    assert_eq!(
        tmsl_dataset_write(
            dataset,
            -1,
            payload_minus_one.as_ptr(),
            payload_minus_one.len(),
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );

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
    let latest_payload = b"22.0";
    assert_eq!(
        tmsl_dataset_write(
            dataset,
            101,
            latest_payload.as_ptr(),
            latest_payload.len(),
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

    data = ptr::null_mut();
    data_len = 0;
    assert_eq!(
        tmsl_dataset_read(
            dataset,
            -1,
            &mut ts,
            &mut data,
            &mut data_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    assert_eq!(ts, -1);
    let read = unsafe { std::slice::from_raw_parts(data, data_len) };
    assert_eq!(read, payload_minus_one);
    tmsl_data_free(data.cast::<c_void>());

    data = ptr::null_mut();
    data_len = 0;
    assert_eq!(
        tmsl_dataset_read(
            dataset,
            -2,
            &mut ts,
            &mut data,
            &mut data_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    assert_eq!(ts, -2);
    let read = unsafe { std::slice::from_raw_parts(data, data_len) };
    assert_eq!(read, payload_minus_two);
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

unsafe fn setup_iter_dataset(dir: &std::path::Path) -> (*mut c_void, *mut c_void) {
    let dir = CString::new(dir.to_string_lossy().as_bytes()).unwrap();
    let name = CString::new("iterds").unwrap();
    let kind = CString::new("sensor").unwrap();
    let mut err = err_buf();

    let store = tmsl_store_open(dir.as_ptr(), err.as_mut_ptr(), err.len());
    assert!(!store.is_null(), "store open failed");

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
    assert!(!dataset.is_null(), "dataset create failed");

    for ts in [100i64, 200, 300, 400, 500] {
        let payload = format!("val_{ts}");
        assert_eq!(
            tmsl_dataset_write(
                dataset,
                ts,
                payload.as_ptr(),
                payload.len(),
                err.as_mut_ptr(),
                err.len(),
            ),
            0,
            "write at {ts} failed"
        );
    }
    (store, dataset)
}

#[test]
fn cffi_query_iter_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let (store, dataset) = unsafe { setup_iter_dataset(dir.path()) };
    let mut err = err_buf();

    let iter = tmsl_dataset_query(dataset, 100, 500, err.as_mut_ptr(), err.len());
    assert!(!iter.is_null());
    assert_eq!(unsafe { tmsl_iter_reverse(iter, err.as_mut_ptr(), err.len()) }, 0);

    let mut collected = Vec::new();
    loop {
        let mut ts: i64 = 0;
        let mut data: *mut c_uchar = ptr::null_mut();
        let mut data_len: usize = 0;
        let rc = unsafe {
            tmsl_iter_next(iter, &mut ts, &mut data, &mut data_len, err.as_mut_ptr(), err.len())
        };
        if rc == 1 {
            break;
        }
        assert_eq!(rc, 0);
        let slice = unsafe { std::slice::from_raw_parts(data, data_len) };
        collected.push((ts, String::from_utf8_lossy(slice).into_owned()));
        unsafe { tmsl_data_free(data.cast::<c_void>()) };
    }
    unsafe { tmsl_iter_close(iter) };

    assert_eq!(collected.len(), 5);
    assert_eq!(collected[0].0, 500);
    assert_eq!(collected[4].0, 100);
    assert_eq!(collected[0].1, "val_500");
    assert_eq!(collected[4].1, "val_100");

    unsafe { tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()) };
    unsafe { tmsl_store_close(store, err.as_mut_ptr(), err.len()) };
}

#[test]
fn cffi_query_iter_skip() {
    let dir = tempfile::tempdir().unwrap();
    let (store, dataset) = unsafe { setup_iter_dataset(dir.path()) };
    let mut err = err_buf();

    let iter = tmsl_dataset_query(dataset, 100, 500, err.as_mut_ptr(), err.len());
    assert!(!iter.is_null());
    assert_eq!(unsafe { tmsl_iter_skip(iter, 2, err.as_mut_ptr(), err.len()) }, 0);

    let mut collected = Vec::new();
    loop {
        let mut ts: i64 = 0;
        let mut data: *mut c_uchar = ptr::null_mut();
        let mut data_len: usize = 0;
        let rc = unsafe {
            tmsl_iter_next(iter, &mut ts, &mut data, &mut data_len, err.as_mut_ptr(), err.len())
        };
        if rc == 1 {
            break;
        }
        assert_eq!(rc, 0);
        let slice = unsafe { std::slice::from_raw_parts(data, data_len) };
        collected.push((ts, String::from_utf8_lossy(slice).into_owned()));
        unsafe { tmsl_data_free(data.cast::<c_void>()) };
    }
    unsafe { tmsl_iter_close(iter) };

    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0].0, 300);
    assert_eq!(collected[1].0, 400);
    assert_eq!(collected[2].0, 500);
    assert_eq!(collected[0].1, "val_300");

    unsafe { tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()) };
    unsafe { tmsl_store_close(store, err.as_mut_ptr(), err.len()) };
}

#[test]
fn cffi_query_iter_collect_all() {
    let dir = tempfile::tempdir().unwrap();
    let (store, dataset) = unsafe { setup_iter_dataset(dir.path()) };
    let mut err = err_buf();

    let iter = tmsl_dataset_query(dataset, 100, 500, err.as_mut_ptr(), err.len());
    assert!(!iter.is_null());

    let mut entries: *mut TmslDataEntry = ptr::null_mut();
    let mut count: usize = 0;
    assert_eq!(
        unsafe { tmsl_iter_collect_all(iter, &mut entries, &mut count, err.as_mut_ptr(), err.len()) },
        0
    );
    assert_eq!(count, 5);
    assert!(!entries.is_null());

    for i in 0..count {
        let entry = unsafe { &*entries.add(i) };
        let expected_ts = 100 + (i as i64) * 100;
        assert_eq!(entry.timestamp, expected_ts);
        let slice = unsafe { std::slice::from_raw_parts(entry.data, entry.data_len) };
        let expected = format!("val_{expected_ts}");
        assert_eq!(slice, expected.as_bytes());
    }

    unsafe { tmsl_data_entry_array_free(entries, count) };

    unsafe { tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()) };
    unsafe { tmsl_store_close(store, err.as_mut_ptr(), err.len()) };
}

#[test]
fn cffi_query_iter_collect_take() {
    let dir = tempfile::tempdir().unwrap();
    let (store, dataset) = unsafe { setup_iter_dataset(dir.path()) };
    let mut err = err_buf();

    let iter = tmsl_dataset_query(dataset, 100, 500, err.as_mut_ptr(), err.len());
    assert!(!iter.is_null());

    let mut entries: *mut TmslDataEntry = ptr::null_mut();
    let mut count: usize = 0;
    assert_eq!(
        unsafe { tmsl_iter_collect_take(iter, 3, &mut entries, &mut count, err.as_mut_ptr(), err.len()) },
        0
    );
    assert_eq!(count, 3);
    assert!(!entries.is_null());

    for i in 0..count {
        let entry = unsafe { &*entries.add(i) };
        let expected_ts = 100 + (i as i64) * 100;
        assert_eq!(entry.timestamp, expected_ts);
    }

    unsafe { tmsl_data_entry_array_free(entries, count) };

    unsafe { tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()) };
    unsafe { tmsl_store_close(store, err.as_mut_ptr(), err.len()) };
}

#[test]
fn cffi_query_iter_skip_and_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let (store, dataset) = unsafe { setup_iter_dataset(dir.path()) };
    let mut err = err_buf();

    let iter = tmsl_dataset_query(dataset, 100, 500, err.as_mut_ptr(), err.len());
    assert!(!iter.is_null());
    assert_eq!(unsafe { tmsl_iter_skip(iter, 2, err.as_mut_ptr(), err.len()) }, 0);
    assert_eq!(unsafe { tmsl_iter_reverse(iter, err.as_mut_ptr(), err.len()) }, 0);

    let mut collected_ts = Vec::new();
    loop {
        let mut ts: i64 = 0;
        let mut data: *mut c_uchar = ptr::null_mut();
        let mut data_len: usize = 0;
        let rc = unsafe {
            tmsl_iter_next(iter, &mut ts, &mut data, &mut data_len, err.as_mut_ptr(), err.len())
        };
        if rc == 1 {
            break;
        }
        assert_eq!(rc, 0);
        collected_ts.push(ts);
        unsafe { tmsl_data_free(data.cast::<c_void>()) };
    }
    unsafe { tmsl_iter_close(iter) };

    assert_eq!(collected_ts, vec![500, 400, 300]);

    unsafe { tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()) };
    unsafe { tmsl_store_close(store, err.as_mut_ptr(), err.len()) };
}

#[test]
fn cffi_length_iter_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let (store, dataset) = unsafe { setup_iter_dataset(dir.path()) };
    let mut err = err_buf();

    let iter = tmsl_dataset_query_length_iter(dataset, 100, 500, err.as_mut_ptr(), err.len());
    assert!(!iter.is_null());
    assert_eq!(unsafe { tmsl_length_iter_reverse(iter, err.as_mut_ptr(), err.len()) }, 0);

    let mut collected = Vec::new();
    loop {
        let mut ts: i64 = 0;
        let mut data_len: u32 = 0;
        let rc = unsafe {
            tmsl_length_iter_next(iter, &mut ts, &mut data_len, err.as_mut_ptr(), err.len())
        };
        if rc == 1 {
            break;
        }
        assert_eq!(rc, 0);
        collected.push((ts, data_len));
    }
    unsafe { tmsl_length_iter_close(iter) };

    assert_eq!(collected.len(), 5);
    assert_eq!(collected[0].0, 500);
    assert_eq!(collected[4].0, 100);
    for (_, len) in &collected {
        assert!(*len > 0);
    }

    unsafe { tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()) };
    unsafe { tmsl_store_close(store, err.as_mut_ptr(), err.len()) };
}

#[test]
fn cffi_length_iter_skip() {
    let dir = tempfile::tempdir().unwrap();
    let (store, dataset) = unsafe { setup_iter_dataset(dir.path()) };
    let mut err = err_buf();

    let iter = tmsl_dataset_query_length_iter(dataset, 100, 500, err.as_mut_ptr(), err.len());
    assert!(!iter.is_null());
    assert_eq!(unsafe { tmsl_length_iter_skip(iter, 2, err.as_mut_ptr(), err.len()) }, 0);

    let mut collected_ts = Vec::new();
    loop {
        let mut ts: i64 = 0;
        let mut data_len: u32 = 0;
        let rc = unsafe {
            tmsl_length_iter_next(iter, &mut ts, &mut data_len, err.as_mut_ptr(), err.len())
        };
        if rc == 1 {
            break;
        }
        assert_eq!(rc, 0);
        collected_ts.push(ts);
    }
    unsafe { tmsl_length_iter_close(iter) };

    assert_eq!(collected_ts, vec![300, 400, 500]);

    unsafe { tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()) };
    unsafe { tmsl_store_close(store, err.as_mut_ptr(), err.len()) };
}

#[test]
fn cffi_length_iter_collect_all() {
    let dir = tempfile::tempdir().unwrap();
    let (store, dataset) = unsafe { setup_iter_dataset(dir.path()) };
    let mut err = err_buf();

    let iter = tmsl_dataset_query_length_iter(dataset, 100, 500, err.as_mut_ptr(), err.len());
    assert!(!iter.is_null());

    let mut entries: *mut TmslLengthEntry = ptr::null_mut();
    let mut count: usize = 0;
    assert_eq!(
        unsafe {
            tmsl_length_iter_collect_all(iter, &mut entries, &mut count, err.as_mut_ptr(), err.len())
        },
        0
    );
    assert_eq!(count, 5);
    assert!(!entries.is_null());

    for i in 0..count {
        let entry = unsafe { &*entries.add(i) };
        let expected_ts = 100 + (i as i64) * 100;
        assert_eq!(entry.timestamp, expected_ts);
        assert!(entry.data_len > 0);
    }

    // TmslLengthEntry has no inner heap allocs — free with tmsl_data_free
    unsafe { tmsl_data_free(entries.cast::<c_void>()) };

    unsafe { tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()) };
    unsafe { tmsl_store_close(store, err.as_mut_ptr(), err.len()) };
}

#[test]
fn cffi_length_iter_collect_take() {
    let dir = tempfile::tempdir().unwrap();
    let (store, dataset) = unsafe { setup_iter_dataset(dir.path()) };
    let mut err = err_buf();

    let iter = tmsl_dataset_query_length_iter(dataset, 100, 500, err.as_mut_ptr(), err.len());
    assert!(!iter.is_null());

    let mut entries: *mut TmslLengthEntry = ptr::null_mut();
    let mut count: usize = 0;
    assert_eq!(
        unsafe {
            tmsl_length_iter_collect_take(iter, 3, &mut entries, &mut count, err.as_mut_ptr(), err.len())
        },
        0
    );
    assert_eq!(count, 3);
    assert!(!entries.is_null());

    for i in 0..count {
        let entry = unsafe { &*entries.add(i) };
        let expected_ts = 100 + (i as i64) * 100;
        assert_eq!(entry.timestamp, expected_ts);
    }

    unsafe { tmsl_data_free(entries.cast::<c_void>()) };

    unsafe { tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()) };
    unsafe { tmsl_store_close(store, err.as_mut_ptr(), err.len()) };
}

#[test]
fn cffi_dataset_negative_timestamp_operations() {
    let dir = tempfile::tempdir().unwrap();
    let dir = CString::new(dir.path().to_string_lossy().as_bytes()).unwrap();
    let name = CString::new("neg_ts").unwrap();
    let kind = CString::new("sensor").unwrap();
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

    let payload_a = b"alpha";
    assert_eq!(
        tmsl_dataset_write(
            dataset,
            -2,
            payload_a.as_ptr(),
            payload_a.len(),
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    let payload_b = b"bravo";
    assert_eq!(
        tmsl_dataset_write(
            dataset,
            -1,
            payload_b.as_ptr(),
            payload_b.len(),
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );

    // readExist: -1 is an exact timestamp.
    assert_eq!(
        tmsl_dataset_read_exist(dataset, -1, err.as_mut_ptr(), err.len()),
        1
    );
    // readExist: -2 is an exact timestamp.
    assert_eq!(
        tmsl_dataset_read_exist(dataset, -2, err.as_mut_ptr(), err.len()),
        1
    );
    // readExist: -3 was not written.
    assert_eq!(
        tmsl_dataset_read_exist(dataset, -3, err.as_mut_ptr(), err.len()),
        0
    );

    // readLength: -1 is an exact timestamp.
    let mut out_len = 0u32;
    assert_eq!(
        tmsl_dataset_read_length(
            dataset,
            -1,
            &mut out_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    assert_eq!(out_len as usize, payload_b.len());

    // readLength: -2 is an exact timestamp.
    out_len = 0;
    assert_eq!(
        tmsl_dataset_read_length(
            dataset,
            -2,
            &mut out_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        0
    );
    assert_eq!(out_len as usize, payload_a.len());

    // readLength: -3 was not written.
    out_len = 0;
    assert_eq!(
        tmsl_dataset_read_length(
            dataset,
            -3,
            &mut out_len,
            err.as_mut_ptr(),
            err.len(),
        ),
        1
    );

    assert_eq!(tmsl_dataset_close(dataset, err.as_mut_ptr(), err.len()), 0);
    assert_eq!(tmsl_store_close(store, err.as_mut_ptr(), err.len()), 0);
}
