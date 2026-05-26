//! Integration tests for timslite.

use std::fs;
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    let d = std::env::temp_dir().join("timslite_integration");
    fs::create_dir_all(&d).unwrap();
    d.join(format!(
        "test_{:?}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn t8_1_1_basic_lifecycle() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .flush_interval(std::time::Duration::from_secs(30))
        .idle_timeout(std::time::Duration::from_secs(60))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    // Create (not open) — explicit creation with parameters
    store
        .create_dataset("sensor_001", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6)
        .unwrap();

    let ds_handle = store.open_dataset("sensor_001", "events").unwrap();

    for i in 0..100i64 {
        let data: Vec<u8> = format!("event_{}", i).into_bytes();
        let ds_arc = store.get_dataset(&ds_handle).unwrap();
        ds_arc.lock().unwrap().write(i, &data).unwrap();
    }

    let ds_arc = store.get_dataset(&ds_handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.flush().unwrap();

    let entries = ds.query(0, 99, None).unwrap();
    assert_eq!(entries.len(), 100);
    for (i, (ts, data)) in entries.iter().enumerate() {
        assert_eq!(*ts, i as i64);
        assert_eq!(*data, format!("event_{}", i).as_bytes());
    }

    drop(ds);
    store.close().unwrap();
}

#[test]
fn t8_1_2_multi_dataset_isolation() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("dataset_a", "type_x", 64 * 1024 * 1024, 4 * 1024 * 1024, 6)
        .unwrap();
    store
        .create_dataset("dataset_b", "type_y", 64 * 1024 * 1024, 4 * 1024 * 1024, 6)
        .unwrap();

    let ds1 = store.open_dataset("dataset_a", "type_x").unwrap();
    let ds2 = store.open_dataset("dataset_b", "type_y").unwrap();

    for i in 0..50i64 {
        let data = format!("a_{}", i).into_bytes();
        store
            .get_dataset(&ds1)
            .unwrap()
            .lock()
            .unwrap()
            .write(i, &data)
            .unwrap();
    }
    for i in 0..60i64 {
        let data = format!("b_{}", i).into_bytes();
        store
            .get_dataset(&ds2)
            .unwrap()
            .lock()
            .unwrap()
            .write(i + 100, &data)
            .unwrap();
    }

    let entries1 = store
        .get_dataset(&ds1)
        .unwrap()
        .lock()
        .unwrap()
        .query(0, 1000, None)
        .unwrap();
    assert_eq!(entries1.len(), 50);

    let entries2 = store
        .get_dataset(&ds2)
        .unwrap()
        .lock()
        .unwrap()
        .query(0, 1000, None)
        .unwrap();
    assert_eq!(entries2.len(), 60);

    store.close().unwrap();
}

#[test]
fn t8_1_3_block_aggregation() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().block_max_size(256).build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset("test", "block_test", 64 * 1024 * 1024, 4 * 1024 * 1024, 6)
        .unwrap();

    let ds = store.open_dataset("test", "block_test").unwrap();
    for i in 0..200i64 {
        let data = vec![i as u8; 32];
        store
            .get_dataset(&ds)
            .unwrap()
            .lock()
            .unwrap()
            .write(i, &data)
            .unwrap();
    }

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(0, 199, None).unwrap();
    assert_eq!(entries.len(), 200);

    store.close().unwrap();
}

#[test]
fn t8_1_6_persistence() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();

    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        store
            .create_dataset("persist", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6)
            .unwrap();
        let ds = store.open_dataset("persist", "data").unwrap();
        for i in 0..50i64 {
            let data = format!("persisted_{}", i).into_bytes();
            let arc = store.get_dataset(&ds).unwrap();
            let mut ds_inner = arc.lock().unwrap();
            ds_inner.write(i, &data).unwrap();
        }
        store.close().unwrap();
    }

    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        let ds = store.open_dataset("persist", "data").unwrap();
        let arc = store.get_dataset(&ds).unwrap();
        let entries = arc.lock().unwrap().query(0, 49, None).unwrap();
        assert_eq!(entries.len(), 50);
        assert_eq!(entries[0].0, 0);
        assert_eq!(entries[49].0, 49);
        store.close().unwrap();
    }
}

#[test]
fn t8_1_7_flush_does_not_seal() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .flush_interval(std::time::Duration::from_millis(500))
        .idle_timeout(std::time::Duration::from_secs(60))
        .build();
    let mut store = Store::open(&dir, config.clone()).unwrap();

    store
        .create_dataset("flush_test", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6)
        .unwrap();

    let ds = store.open_dataset("flush_test", "data").unwrap();
    let data = b"pending_data".to_vec();
    {
        let arc = store.get_dataset(&ds).unwrap();
        arc.lock().unwrap().write(1000, &data).unwrap();
    }

    // Wait for background flush
    std::thread::sleep(std::time::Duration::from_secs(2));

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(999, 1001, None).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, data);

    store.close().unwrap();
}

// ─── New lifecycle tests: create/open/drop ─────────────────────────────────

#[test]
fn t8_2_1_create_returns_error_if_exists() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("dup_test", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6)
        .unwrap();

    // Second create of same dataset should fail
    let result = store.create_dataset("dup_test", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6);
    assert!(result.is_err());
    if let Err(err) = result {
        assert!(err.to_string().contains("already exists"));
    }

    store.close().unwrap();
}

#[test]
fn t8_2_2_open_returns_error_if_not_exists() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    // Open non-existent dataset
    let result = store.open_dataset("no_such", "data");
    assert!(result.is_err());
    if let Err(err) = result {
        assert!(err.to_string().contains("not found"));
    }

    store.close().unwrap();
}

#[test]
fn t8_2_3_drop_deletes_dataset() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("drop_test", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6)
        .unwrap();

    let ds_handle = store.open_dataset("drop_test", "data").unwrap();

    // Write some data
    let arc = store.get_dataset(&ds_handle).unwrap();
    arc.lock().unwrap().write(100, b"test").unwrap();
    store.close_dataset(ds_handle).unwrap();

    // Drop the dataset
    store.drop_dataset_by_name("drop_test", "data").unwrap();

    // Verify directory is gone
    let dataset_dir = dir.join("drop_test").join("data");
    assert!(!dataset_dir.exists());

    store.close().unwrap();
}

#[test]
fn t8_2_4_create_after_drop() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("recreate", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6)
        .unwrap();
    let ds = store.open_dataset("recreate", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    arc.lock().unwrap().write(1, b"first").unwrap();
    store.close().unwrap();

    // Re-open store, drop, recreate
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store.drop_dataset_by_name("recreate", "data").unwrap();

    // Now create should succeed (different params are fine since old data is gone)
    store
        .create_dataset("recreate", "data", 32 * 1024 * 1024, 2 * 1024 * 1024, 9)
        .unwrap();
    let ds = store.open_dataset("recreate", "data").unwrap();

    // Data from first creation should be gone
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(0, 10, None).unwrap();
    assert_eq!(entries.len(), 0);

    store.close().unwrap();
}
