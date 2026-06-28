//! Public Rust API contract tests.
//!
//! C ABI coverage lives in `wrapper/cffi/tests`; this file validates the
//! standard Rust library surface directly.
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let d = std::env::temp_dir().join("timslite_integration");
    fs::create_dir_all(&d).unwrap();
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    d.join(format!(
        "test_{:?}_{id}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// P2-X-1: Public API basic operations
///
/// Verify that the core API functions work correctly.
#[test]
fn t34_1_api_basic_operations() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    // Test dataset create
    store
        .create_dataset(
            "conv_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    // Test dataset open
    let ds = store.open_dataset("conv_test", "data").unwrap();

    // Test dataset write
    let arc = ds.clone();
    {
        let lock = arc.clone();
        lock.write(100, b"hello api").unwrap();
    }

    // Test dataset read
    {
        let lock = arc.clone();
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 100);
        assert_eq!(data, b"hello api");
    }

    // Test latest timestamp
    {
        let lock = arc.clone();
        assert_eq!(lock.latest_written_timestamp(), Some(100));
    }

    // Test flush
    {
        let lock = arc.clone();
        lock.flush().unwrap();
    }

    store.close().unwrap();
}

/// P2-X-2: Error code verification
///
/// Verify that errors are properly returned.
#[test]
fn t34_2_api_error_handling() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    // Test: open non-existent dataset should fail
    let result = store.open_dataset("nonexistent", "data");
    assert!(result.is_err(), "open non-existent dataset should fail");

    // Test: create dataset with invalid name should fail
    let result = store.create_dataset(
        "invalid/name",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
    );
    assert!(result.is_err(), "invalid name should fail");

    // Test: create dataset with empty name should fail
    let result = store.create_dataset("", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0);
    assert!(result.is_err(), "empty name should fail");

    store.close().unwrap();
}

/// P2-X-3: Memory safety verification
///
/// Verify no memory leaks or dangling pointers.
#[test]
fn t34_3_api_memory_safety() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "mem_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("mem_test", "data").unwrap();
    let arc = ds.clone();

    // Write and read multiple times to test memory management
    for i in 0..100 {
        let data = format!("data_{}", i);
        let data_bytes = data.as_bytes();

        {
            let lock = arc.clone();
            lock.write(i + 1, data_bytes).unwrap();
        }

        {
            let lock = arc.clone();
            let result = lock.read(i + 1).unwrap();
            assert!(result.is_some());
            let (ts, read_data) = result.unwrap();
            assert_eq!(ts, i + 1);
            assert_eq!(read_data, data_bytes);
        }
    }

    store.close().unwrap();
}

/// P2-X-4: Concurrent safety verification
///
/// Verify that API calls from multiple threads are safe.
#[test]
fn t34_4_api_concurrent_reads() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "concurrent",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("concurrent", "data").unwrap();
    let arc = ds.clone();

    // Write initial data
    {
        let lock = arc.clone();
        for i in 0..50 {
            let data = format!("data_{}", i);
            lock.write(i + 1, data.as_bytes()).unwrap();
        }
    }

    // Spawn multiple reader threads
    let mut handles = vec![];
    for _thread_id in 0..4 {
        let arc_clone = arc.clone();
        let handle = thread::spawn(move || {
            for i in 0..50 {
                let lock = arc_clone.clone();
                let result = lock.read(i + 1).unwrap();
                assert!(result.is_some());
                let (ts, data) = result.unwrap();
                assert_eq!(ts, i + 1);
                let expected = format!("data_{}", i);
                assert_eq!(data, expected.as_bytes());
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    store.close().unwrap();
}

/// P2-X-5: Query operations verification
///
/// Verify query operations work correctly.
#[test]
fn t34_5_api_query_operations() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "query_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("query_test", "data").unwrap();
    let arc = ds.clone();

    // Write data
    {
        let lock = arc.clone();
        for i in 0..20 {
            let data = format!("record_{}", i);
            lock.write(i * 10 + 1, data.as_bytes()).unwrap();
        }
    }

    // Query range
    {
        let lock = arc.clone();
        let results = lock.query(1, 200).unwrap();
        assert_eq!(results.len(), 20);
        for (i, (ts, data)) in results.iter().enumerate() {
            assert_eq!(*ts, i as i64 * 10 + 1);
            let expected = format!("record_{}", i);
            assert_eq!(data, expected.as_bytes());
        }
    }

    // Query subset
    {
        let lock = arc.clone();
        let results = lock.query(51, 100).unwrap();
        assert_eq!(results.len(), 5);
    }

    store.close().unwrap();
}

/// P2-X-6: Delete and correction write verification
///
/// Verify delete and correction write operations.
#[test]
fn t34_6_api_delete_and_correction() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "del_corr",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("del_corr", "data").unwrap();
    let arc = ds.clone();

    // Write data
    {
        let lock = arc.clone();
        lock.write(100, b"original").unwrap();
        lock.write(200, b"to_delete").unwrap();
        lock.write(300, b"keep").unwrap();
    }

    // Delete record
    {
        let lock = arc.clone();
        lock.delete(200).unwrap();
    }

    // Verify delete
    {
        let lock = arc.clone();
        let result = lock.read(200).unwrap();
        assert!(result.is_none(), "deleted record should return None");
    }

    // Correction write
    {
        let lock = arc.clone();
        lock.write(100, b"corrected").unwrap();
    }

    // Verify correction
    {
        let lock = arc.clone();
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"corrected");
    }

    // Verify other record unchanged
    {
        let lock = arc.clone();
        let result = lock.read(300).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"keep");
    }

    store.close().unwrap();
}

/// P2-X-7: Create dataset with explicit config builder
///
/// Verify `create_dataset_with_config` with `DataSetConfigBuilder` sets
/// custom segment sizes, compression, and index mode.
#[test]
fn t34_7_create_dataset_with_config() {
    use timslite::{DataSetConfigBuilder, Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    let builder = DataSetConfigBuilder::from_store(store.config())
        .data_segment_size(32 * 1024 * 1024)
        .index_segment_size(2 * 1024 * 1024)
        .compress_level(3)
        .index_continuous(1)
        .initial_data_segment_size(128 * 1024)
        .initial_index_segment_size(4 * 1024)
        .retention_window(0);

    let handle = store
        .create_dataset_with_config("cfg_ds", "metrics", Some(builder))
        .unwrap();

    let ds = handle.clone();
    ds.write(1, b"config_test").unwrap();

    let result = ds.read(1).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().1, b"config_test");

    // Verify inspect reflects our custom config
    let info = store.inspect_dataset("cfg_ds", "metrics").unwrap();
    assert_eq!(info.info.data_segment_size, 32 * 1024 * 1024);
    assert_eq!(info.info.index_segment_size, 2 * 1024 * 1024);
    assert_eq!(info.info.compress_level, 3);
    assert_eq!(info.info.index_continuous, 1);

    store.close().unwrap();
}

/// P2-X-8: Open dataset by numeric identifier
///
/// Verify `open_dataset_by_identifier` opens the same dataset as
/// `open_dataset` and returns consistent data.
#[test]
fn t34_8_open_dataset_by_identifier() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset("id_ds", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    let handle = store.open_dataset("id_ds", "data").unwrap();
    let identifier = handle.identifier();
    assert!(identifier > 0, "identifier should be positive");

    // Write via original handle
    let ds = handle.clone();
    ds.write(100, b"via_handle").unwrap();

    // Open by identifier and read
    let handle2 = store.open_dataset_by_identifier(identifier).unwrap();
    let ds2 = handle2.clone();
    let result = ds2.read(100).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().1, b"via_handle");

    // Both handles should refer to the same dataset
    assert_eq!(
        ds.latest_written_timestamp(),
        ds2.latest_written_timestamp()
    );

    store.close().unwrap();
}

/// P2-X-9: Read latest written record
///
/// Verify `read_latest` returns the most recently written record
/// and returns None on an empty dataset.
#[test]
fn t34_9_read_latest() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "latest_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let handle = store.open_dataset("latest_ds", "data").unwrap();
    let ds = handle.clone();

    // Empty dataset: read_latest should return None
    let result = ds.read_latest().unwrap();
    assert!(
        result.is_none(),
        "empty dataset should return None for read_latest"
    );

    // Write records in ascending order
    ds.write(100, b"first").unwrap();
    ds.write(200, b"second").unwrap();
    ds.write(300, b"third").unwrap();

    // read_latest should return the record at latest_written_timestamp (300)
    let result = ds.read_latest().unwrap();
    assert!(result.is_some());
    let (ts, data) = result.unwrap();
    assert_eq!(ts, 300);
    assert_eq!(data, b"third");

    // latest_written_timestamp should be 300
    assert_eq!(ds.latest_written_timestamp(), Some(300));

    store.close().unwrap();
}

/// P2-X-10: Read with existence check (read_exist)
///
/// Verify `read_exist` returns true for existing records and false
/// for missing or deleted records.
#[test]
fn t34_10_read_exist() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "exist_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let handle = store.open_dataset("exist_ds", "data").unwrap();
    let ds = handle.clone();

    ds.write(100, b"exists").unwrap();
    ds.write(200, b"to_delete").unwrap();
    ds.delete(200).unwrap();

    // Existing record
    assert!(ds.read_exist(100).unwrap(), "record 100 should exist");

    // Non-existent record
    assert!(!ds.read_exist(999).unwrap(), "record 999 should not exist");

    // Deleted record
    assert!(
        !ds.read_exist(200).unwrap(),
        "deleted record 200 should not exist"
    );

    store.close().unwrap();
}

/// P2-X-11: Query with existence check (query_exist)
///
/// Verify `query_exist` returns a byte vector indicating which
/// timestamps in the range have data.
#[test]
fn t34_11_query_exist() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "qexist_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let handle = store.open_dataset("qexist_ds", "data").unwrap();
    let ds = handle.clone();

    // Write records at specific timestamps
    ds.write(100, b"a").unwrap();
    ds.write(200, b"b").unwrap();
    ds.write(300, b"c").unwrap();

    // query_exist should return a non-empty vector
    let exist_vec = ds.query_exist(100, 300).unwrap();
    assert!(
        !exist_vec.is_empty(),
        "query_exist should return data for range with records"
    );

    // query for a range with no records
    let empty_vec = ds.query_exist(500, 600).unwrap();
    assert!(
        empty_vec.iter().all(|&b| b == 0),
        "query_exist should return all-zero bitmap for range with no records"
    );

    store.close().unwrap();
}

/// P2-X-12: Read and get data length (read_length)
///
/// Verify `read_length` returns the correct data length without
/// reading the full data payload.
#[test]
fn t34_12_read_length() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "rlen_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let handle = store.open_dataset("rlen_ds", "data").unwrap();
    let ds = handle.clone();

    let payload = b"hello world, this is a test payload";
    ds.write(100, payload).unwrap();
    ds.write(200, b"short").unwrap();

    // read_length should return the data length
    let len = ds.read_length(100).unwrap();
    assert!(len.is_some());
    assert_eq!(len.unwrap(), payload.len() as u32);

    let len2 = ds.read_length(200).unwrap();
    assert!(len2.is_some());
    assert_eq!(len2.unwrap(), 5); // "short" = 5 bytes

    // Non-existent record
    let len3 = ds.read_length(999).unwrap();
    assert!(len3.is_none(), "non-existent record should return None");

    store.close().unwrap();
}

/// P2-X-13: Query and get data lengths (query_length)
///
/// Verify `query_length` returns timestamp-length pairs for all
/// records in the range without reading full data.
#[test]
fn t34_13_query_length() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "qlen_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let handle = store.open_dataset("qlen_ds", "data").unwrap();
    let ds = handle.clone();

    ds.write(100, b"aaa").unwrap(); // 3 bytes
    ds.write(200, b"bbbbb").unwrap(); // 5 bytes
    ds.write(300, b"c").unwrap(); // 1 byte

    let lengths = ds.query_length(100, 300).unwrap();
    assert_eq!(lengths.len(), 3);
    assert_eq!(lengths[0], (100, 3));
    assert_eq!(lengths[1], (200, 5));
    assert_eq!(lengths[2], (300, 1));

    // Subset query
    let subset = ds.query_length(150, 250).unwrap();
    assert_eq!(subset.len(), 1);
    assert_eq!(subset[0], (200, 5));

    // Empty range
    let empty = ds.query_length(500, 600).unwrap();
    assert!(empty.is_empty());

    store.close().unwrap();
}

/// P2-X-14: Inspect dataset stats
///
/// Verify `inspect_dataset` returns correct info and state fields
/// including segment counts, record counts, and timestamps.
#[test]
fn t34_14_inspect_dataset() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "insp_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let handle = store.open_dataset("insp_ds", "data").unwrap();
    let ds = handle.clone();

    ds.write(100, b"inspect_test").unwrap();
    ds.write(200, b"inspect_test_2").unwrap();
    ds.flush().unwrap();

    let result = store.inspect_dataset("insp_ds", "data").unwrap();

    // Info fields
    assert_eq!(result.info.name, "insp_ds");
    assert_eq!(result.info.dataset_type, "data");
    assert_eq!(result.info.compress_level, 6);
    assert!(result.info.identifier > 0);

    // State fields
    assert_eq!(result.state.latest_written_timestamp, Some(200));
    assert!(result.state.data_segments > 0);
    assert!(result.state.index_segments > 0);

    // Inspect non-existent dataset should fail
    let err = store.inspect_dataset("no_such", "data");
    assert!(err.is_err());

    store.close().unwrap();
}

/// P2-X-15: List dataset names and types
///
/// Verify `get_dataset_names` returns all unique names and
/// `get_dataset_types` returns types for a given name.
#[test]
fn t34_15_list_datasets() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    // Create multiple datasets with different names and types
    store
        .create_dataset("alpha", "raw", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    store
        .create_dataset("alpha", "agg", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    store
        .create_dataset("beta", "raw", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    let names = store.get_dataset_names().unwrap();
    assert!(names.contains(&"alpha".to_string()));
    assert!(names.contains(&"beta".to_string()));

    let types_alpha = store.get_dataset_types("alpha").unwrap();
    assert!(types_alpha.contains(&"raw".to_string()));
    assert!(types_alpha.contains(&"agg".to_string()));

    let types_beta = store.get_dataset_types("beta").unwrap();
    assert!(types_beta.contains(&"raw".to_string()));
    assert_eq!(types_beta.len(), 1);

    // Non-existent name returns empty
    let types_none = store.get_dataset_types("gamma").unwrap();
    assert!(types_none.is_empty());

    store.close().unwrap();
}

/// P2-X-16: Queue push/poll/ack flow
///
/// Verify the queue subsystem: push data, poll from consumer, and ack.
#[test]
fn t34_16_queue_push_poll_ack() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "queue_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let handle = store.open_dataset("queue_ds", "data").unwrap();
    let queue = handle.open_queue().unwrap();
    let consumer = queue.open_consumer("group_a").unwrap();

    // Push records
    let ts1 = queue.push(b"msg1").unwrap();
    let ts2 = queue.push(b"msg2").unwrap();
    let ts3 = queue.push(b"msg3").unwrap();
    assert!(ts1 > 0);
    assert!(ts2 > ts1);
    assert!(ts3 > ts2);

    // Poll should return records in order
    let r1 = consumer.poll(Duration::from_secs(1)).unwrap();
    assert!(r1.is_some());
    let (poll_ts1, poll_data1) = r1.unwrap();
    assert_eq!(poll_ts1, ts1);
    assert_eq!(poll_data1, b"msg1");

    // Ack
    consumer.ack(poll_ts1).unwrap();

    // Poll next
    let r2 = consumer.poll(Duration::from_secs(1)).unwrap();
    assert!(r2.is_some());
    let (poll_ts2, poll_data2) = r2.unwrap();
    assert_eq!(poll_ts2, ts2);
    assert_eq!(poll_data2, b"msg2");

    consumer.ack(poll_ts2).unwrap();

    let r3 = consumer.poll(Duration::from_secs(1)).unwrap();
    assert!(r3.is_some());
    let (poll_ts3, poll_data3) = r3.unwrap();
    assert_eq!(poll_ts3, ts3);
    assert_eq!(poll_data3, b"msg3");

    consumer.ack(poll_ts3).unwrap();

    // Poll with short timeout after all consumed should return None
    let r4 = consumer.poll(Duration::from_millis(50)).unwrap();
    assert!(
        r4.is_none(),
        "should return None after all records consumed"
    );

    store.close().unwrap();
}

/// P2-X-17: Queue with multiple consumer groups
///
/// Verify that two consumer groups track progress independently.
#[test]
fn t34_17_queue_multiple_consumers() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "multi_q",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let handle = store.open_dataset("multi_q", "data").unwrap();
    let queue = handle.open_queue().unwrap();

    let consumer_a = queue.open_consumer("group_a").unwrap();
    let consumer_b = queue.open_consumer("group_b").unwrap();

    // Push two records
    let ts1 = queue.push(b"data1").unwrap();
    let _ts2 = queue.push(b"data2").unwrap();

    // Consumer A polls and acks first record
    let r_a = consumer_a.poll(Duration::from_secs(1)).unwrap();
    assert!(r_a.is_some());
    let (ts_a, _) = r_a.unwrap();
    assert_eq!(ts_a, ts1);
    consumer_a.ack(ts_a).unwrap();

    // Consumer B should also get the first record (independent progress)
    let r_b = consumer_b.poll(Duration::from_secs(1)).unwrap();
    assert!(r_b.is_some());
    let (ts_b, data_b) = r_b.unwrap();
    assert_eq!(ts_b, ts1);
    assert_eq!(data_b, b"data1");

    store.close().unwrap();
}

/// P2-X-18: Query range edge cases
///
/// Verify query behavior with empty datasets, single records,
/// and non-overlapping ranges.
#[test]
fn t34_18_query_range_edge_cases() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "range_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let handle = store.open_dataset("range_ds", "data").unwrap();
    let ds = handle.clone();

    // Query empty dataset
    let results = ds.query(0, 1000).unwrap();
    assert!(
        results.is_empty(),
        "empty dataset query should return empty"
    );

    ds.write(100, b"early").unwrap();
    ds.write(500, b"only").unwrap();
    ds.write(900, b"late").unwrap();

    // Query exact match
    let results = ds.query(500, 500).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 500);
    assert_eq!(results[0].1, b"only");

    // Query range that doesn't include any record
    let results = ds.query(200, 400).unwrap();
    assert!(results.is_empty());

    let results = ds.query(600, 800).unwrap();
    assert!(results.is_empty());

    // Query wide range
    let results = ds.query(0, 1000).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, 100);
    assert_eq!(results[1].0, 500);
    assert_eq!(results[2].0, 900);

    store.close().unwrap();
}

/// P2-X-19: FFI error path verification
///
/// Verify that error conditions produce proper errors, matching
/// what the FFI layer would translate to error codes and err_buf.
#[test]
fn t34_19_error_paths() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    // Error: open non-existent dataset
    let err = store.open_dataset("no_such", "data");
    assert!(err.is_err(), "opening non-existent dataset should fail");

    // Error: create dataset with invalid name (contains '/')
    let err = store.create_dataset(
        "bad/name",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
    );
    assert!(err.is_err(), "dataset name with '/' should fail");

    // Error: create dataset with empty name
    let err = store.create_dataset("", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0);
    assert!(err.is_err(), "empty dataset name should fail");

    // Error: duplicate dataset creation
    store
        .create_dataset("dup_ds", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let err = store.create_dataset("dup_ds", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0);
    assert!(err.is_err(), "duplicate dataset creation should fail");

    // Error: dataset_identifier on valid handle should succeed
    let handle = store.open_dataset("dup_ds", "data").unwrap();
    assert_ne!(handle.identifier(), 0);

    // Error: open_dataset_by_identifier with non-existent id
    let err = store.open_dataset_by_identifier(u64::MAX);
    assert!(err.is_err(), "non-existent identifier should fail");

    // Error: inspect non-existent dataset
    let err = store.inspect_dataset("ghost", "data");
    assert!(err.is_err(), "inspect non-existent dataset should fail");

    // Error: get_dataset_types with invalid name
    let err = store.get_dataset_types("bad/name");
    assert!(
        err.is_err(),
        "get_dataset_types with invalid name should fail"
    );

    store.close().unwrap();
}
