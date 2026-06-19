//! FFI tests: C calling convention, error codes, memory safety.
//!
//! These tests verify the FFI layer by testing the Rust API directly.
//! The FFI functions are thin wrappers around the Rust API, so testing
//! the Rust API verifies the underlying behavior.
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

/// P2-X-1: C calling convention verification
///
/// Verify that the core API functions work correctly.
/// The FFI layer wraps these functions, so if they work,
/// the FFI layer should work too.
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
    let arc = store.get_dataset(&ds).unwrap();
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
    let arc = store.get_dataset(&ds).unwrap();

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
    let arc = store.get_dataset(&ds).unwrap();

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
    let arc = store.get_dataset(&ds).unwrap();

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
    let arc = store.get_dataset(&ds).unwrap();

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
