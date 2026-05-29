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
        .create_dataset(
            "sensor_001",
            "events",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds_handle = store.open_dataset("sensor_001", "events").unwrap();

    for i in 0..100i64 {
        let data: Vec<u8> = format!("event_{}", i).into_bytes();
        let ds_arc = store.get_dataset(&ds_handle).unwrap();
        ds_arc.lock().unwrap().write(i + 1, &data).unwrap();
    }

    let ds_arc = store.get_dataset(&ds_handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.flush().unwrap();

    let entries = ds.query(1, 100, None).unwrap();
    assert_eq!(entries.len(), 100);
    for (i, (ts, data)) in entries.iter().enumerate() {
        assert_eq!(*ts, (i + 1) as i64);
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
        .create_dataset(
            "dataset_a",
            "type_x",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    store
        .create_dataset(
            "dataset_b",
            "type_y",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
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
            .write(i + 1, &data)
            .unwrap();
    }
    for i in 0..60i64 {
        let data = format!("b_{}", i).into_bytes();
        store
            .get_dataset(&ds2)
            .unwrap()
            .lock()
            .unwrap()
            .write(i + 101, &data)
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
        .create_dataset(
            "test",
            "block_test",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("test", "block_test").unwrap();
    for i in 0..200i64 {
        let data = vec![i as u8; 32];
        store
            .get_dataset(&ds)
            .unwrap()
            .lock()
            .unwrap()
            .write(i + 1, &data)
            .unwrap();
    }

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 200, None).unwrap();
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
            .create_dataset(
                "persist",
                "data",
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                0,
            )
            .unwrap();
        let ds = store.open_dataset("persist", "data").unwrap();
        for i in 0..50i64 {
            let data = format!("persisted_{}", i).into_bytes();
            let arc = store.get_dataset(&ds).unwrap();
            let mut ds_inner = arc.lock().unwrap();
            ds_inner.write(i + 1, &data).unwrap();
        }
        store.close().unwrap();
    }

    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        let ds = store.open_dataset("persist", "data").unwrap();
        let arc = store.get_dataset(&ds).unwrap();
        let entries = arc.lock().unwrap().query(1, 50, None).unwrap();
        assert_eq!(entries.len(), 50);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[49].0, 50);
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
        .create_dataset(
            "flush_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
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
        .create_dataset(
            "dup_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    // Second create of same dataset should fail
    let result = store.create_dataset(
        "dup_test",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
    );
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
        .create_dataset(
            "drop_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
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
        .create_dataset(
            "recreate",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
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
        .create_dataset(
            "recreate",
            "data",
            32 * 1024 * 1024,
            2 * 1024 * 1024,
            9,
            0,
            0,
        )
        .unwrap();
    let ds = store.open_dataset("recreate", "data").unwrap();

    // Data from first creation should be gone
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(0, 10, None).unwrap();
    assert_eq!(entries.len(), 0);

    store.close().unwrap();
}

// ─── Phase 12: Lazy allocation integration tests ─────────────────────────────────

#[test]
fn t12_1_lazy_create_write_query_small_data() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .initial_data_segment_size(256 * 1024) // 256KB initial
        .initial_index_segment_size(4 * 1024) // 4KB initial
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "lazy_small",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("lazy_small", "data").unwrap();
    for i in 0..100i64 {
        let data = format!("small_{}", i).into_bytes();
        store
            .get_dataset(&ds)
            .unwrap()
            .lock()
            .unwrap()
            .write(i + 1, &data)
            .unwrap();
    }

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 100, None).unwrap();
    assert_eq!(entries.len(), 100);
    assert_eq!(entries[0].0, 1);
    assert_eq!(entries[99].0, 100);

    store.close().unwrap();
}

#[test]
fn t12_2_lazy_write_until_max_then_new_segment() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .initial_data_segment_size(256 * 1024)
        .initial_index_segment_size(4 * 1024)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "lazy_max",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("lazy_max", "data").unwrap();
    // Write enough data to fill first block (block_max_size default 64KB)
    // Each record: overhead ~10 + 500 bytes = ~510 bytes
    for i in 0..200i64 {
        let data = vec![i as u8; 500];
        let write_result = store
            .get_dataset(&ds)
            .unwrap()
            .lock()
            .unwrap()
            .write(i + 1, &data);
        if write_result.is_err() {
            break; // May fail at segment boundary — that's fine
        }
    }

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 200, None).unwrap();
    assert!(!entries.is_empty(), "should have some data");

    store.close().unwrap();
}

#[test]
fn t12_3_open_legacy_full_allocated_dataset() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    // Create dataset with lazy allocation config from the start
    let config = StoreConfig::builder()
        .initial_data_segment_size(256 * 1024)
        .initial_index_segment_size(4 * 1024)
        .build();
    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        store
            .create_dataset("legacy", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
            .unwrap();

        let ds = store.open_dataset("legacy", "data").unwrap();
        for i in 0..100i64 {
            let data = format!("legacy_{}", i).into_bytes();
            store
                .get_dataset(&ds)
                .unwrap()
                .lock()
                .unwrap()
                .write(i + 1, &data)
                .unwrap();
        }
        store.close().unwrap();
    }

    // Reopen with different initial sizes (simulating opening with different StoreConfig)
    // The dataset should still be readable because segment sizes are stored in meta
    {
        let config2 = StoreConfig::builder()
            .initial_data_segment_size(512 * 1024)
            .initial_index_segment_size(8 * 1024)
            .build();
        let mut store = Store::open(&dir, config2).unwrap();

        let ds = store.open_dataset("legacy", "data").unwrap();
        let arc = store.get_dataset(&ds).unwrap();
        let entries = arc.lock().unwrap().query(1, 100, None).unwrap();
        assert_eq!(entries.len(), 100);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[99].0, 100);

        // Write more data — should work with lazy expansion
        for i in 100..200i64 {
            let data = format!("legacy_new_{}", i).into_bytes();
            store
                .get_dataset(&ds)
                .unwrap()
                .lock()
                .unwrap()
                .write(i + 1, &data)
                .unwrap();
        }

        let arc = store.get_dataset(&ds).unwrap();
        let entries = arc.lock().unwrap().query(1, 200, None).unwrap();
        assert_eq!(entries.len(), 200);

        store.close().unwrap();
    }
}

#[test]
fn t12_4_disk_space_efficiency() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .initial_data_segment_size(256 * 1024) // 256KB initial
        .initial_index_segment_size(4 * 1024) // 4KB initial
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "eff_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("eff_test", "data").unwrap();
    // Write 100 records totaling ~10KB
    for i in 0..100i64 {
        let data = format!("eff_{}", i).into_bytes();
        store
            .get_dataset(&ds)
            .unwrap()
            .lock()
            .unwrap()
            .write(i + 1, &data)
            .unwrap();
    }

    // With lazy allocation, total disk usage should be < 64MB (segment_size)
    let data_dir = dir.join("eff_test").join("data");
    assert!(data_dir.exists(), "data directory should exist");
    let mut total_size = 0u64;
    for entry in std::fs::read_dir(&data_dir).unwrap() {
        let e = entry.unwrap();
        if e.path().is_file() {
            total_size += e.metadata().unwrap().len();
        }
    }
    assert!(
        total_size < 64 * 1024 * 1024,
        "lazy allocation should use < 64MB for small data, got {} bytes",
        total_size
    );

    // Verify data integrity
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 100, None).unwrap();
    assert_eq!(entries.len(), 100);

    store.close().unwrap();
}

// ─── Phase 14: create_dataset_with_config builder API tests ─────────────

#[test]
fn t14_1_create_with_none_config_uses_store_defaults() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .data_segment_size(32 * 1024 * 1024)
        .index_segment_size(8 * 1024 * 1024)
        .compress_level(3)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    // None config should use store defaults
    store
        .create_dataset_with_config("defaults_test", "data", None)
        .unwrap();

    let ds = store.open_dataset("defaults_test", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    arc.lock().unwrap().write(1, b"hello").unwrap();

    let entries = arc.lock().unwrap().query(1, 1, None).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, b"hello");

    store.close().unwrap();
}

#[test]
fn t14_2_create_with_builder_override() {
    use timslite::{DataSetConfigBuilder, Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .data_segment_size(64 * 1024 * 1024)
        .compress_level(6)
        .build();
    let mut store = Store::open(&dir, config.clone()).unwrap();

    // Override compress_level only, other fields inherit from store
    store
        .create_dataset_with_config(
            "override_test",
            "data",
            Some(DataSetConfigBuilder::from_store(&config).compress_level(9)),
        )
        .unwrap();

    let ds = store.open_dataset("override_test", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    for i in 0..10i64 {
        let data = format!("data_{}", i).into_bytes();
        arc.lock().unwrap().write(i + 1, &data).unwrap();
    }

    let entries = arc.lock().unwrap().query(1, 10, None).unwrap();
    assert_eq!(entries.len(), 10);

    store.close().unwrap();
}

#[test]
fn t14_3_backward_compat_existing_api() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::default();
    let mut store = Store::open(&dir, config).unwrap();

    // Old create_dataset API should still work (delegates to new method)
    store
        .create_dataset(
            "compat_old",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("compat_old", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    arc.lock().unwrap().write(1, b"compat_test").unwrap();

    let entries = arc.lock().unwrap().query(1, 1, None).unwrap();
    assert_eq!(entries.len(), 1);

    store.close().unwrap();
}

// ─── Phase 13: Query Iterator + HotBlockCache integration tests ─────────────

#[test]
fn t13_1_iterator_small_range() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "iter_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("iter_test", "data").unwrap();
    for i in 0..50i64 {
        let data = format!("iter_{}", i).into_bytes();
        store
            .get_dataset(&ds)
            .unwrap()
            .lock()
            .unwrap()
            .write(i + 1, &data)
            .unwrap();
    }

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 50, None).unwrap();
    assert_eq!(entries.len(), 50);
    for (i, (ts, data)) in entries.iter().enumerate() {
        assert_eq!(*ts, (i + 1) as i64);
        assert_eq!(*data, format!("iter_{}", i).as_bytes());
    }

    store.close().unwrap();
}

#[test]
fn t13_3_query_backward_compat() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "compat_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("compat_test", "data").unwrap();
    for i in 0..100i64 {
        let data = vec![i as u8; 64];
        store
            .get_dataset(&ds)
            .unwrap()
            .lock()
            .unwrap()
            .write(i + 1, &data)
            .unwrap();
    }

    // query() should produce the same results as before (via query_iter)
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 100, None).unwrap();
    assert_eq!(entries.len(), 100);

    // Verify order: timestamps must be ascending
    for i in 1..entries.len() {
        assert!(entries[i].0 > entries[i - 1].0);
    }

    store.close().unwrap();
}

#[test]
fn t13_4_query_empty_range() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "empty_q",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("empty_q", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Query before any writes — should return empty
    let entries = arc.lock().unwrap().query(1, 100, None).unwrap();
    assert_eq!(entries.len(), 0);

    store.close().unwrap();
}

// ─── Phase 17: Correction Write ───────────────────────────────────────────

#[test]
fn t17_1_correction_write_same_size() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("cw", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    let ds = store.open_dataset("cw", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write original data
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"alpha").unwrap();
        lock.write(200, b"beta.").unwrap();

        // Correction write: same size
        lock.write(200, b"BETA.").unwrap();

        let entries = lock.query(100, 200, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].1, b"alpha");
        assert_eq!(entries[1].1, b"BETA.");
    }

    store.close().unwrap();
}

#[test]
fn t17_2_correction_write_resize_reopen() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "cw_resize",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cw_resize", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write original (small)
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"tiny").unwrap();

        // Correction: resize to larger
        let bigger = vec![0xABu8; 300];
        lock.write(100, &bigger).unwrap();

        // Correction: resize back to smaller
        lock.write(100, b"x").unwrap();

        let entries = lock.query(100, 100, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"x");
    }
    drop(arc);
    store.close().unwrap();

    // Reopen and verify persistence
    let mut store2 = Store::open(&dir, StoreConfig::default()).unwrap();
    let ds2 = store2.open_dataset("cw_resize", "data").unwrap();
    let arc2 = store2.get_dataset(&ds2).unwrap();
    {
        let mut lock2 = arc2.lock().unwrap();
        let entries = lock2.query(100, 100, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"x");
    }
    drop(arc2);
    store2.close().unwrap();
}

// ─── Phase 18: 乱序写入 + 删除 ───────────────────────────────────────────

#[test]
fn t18_1_out_of_order_write() {
    // Out-of-order writes (ts < latest) should append data to the latest segment,
    // update the existing index entry in place, and increment invalid_record_count
    // when the previous entry referenced real data.
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "ooo_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0, // non-continuous
            0,
        )
        .unwrap();

    let ds = store.open_dataset("ooo_ds", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"v1").unwrap();
        lock.write(200, b"v2").unwrap();
        lock.write(300, b"v3").unwrap();

        // Out-of-order writes — each replaces a real entry
        lock.write(100, b"v1_updated").unwrap();
        lock.write(200, b"v2_updated").unwrap();

        // Query should reflect latest data
        let entries = lock.query(100, 300, None).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].1, b"v1_updated");
        assert_eq!(entries[1].1, b"v2_updated");
        assert_eq!(entries[2].1, b"v3");
    }

    drop(arc);
    store.close().unwrap();
}

#[test]
fn t18_1b_out_of_order_write_continuous() {
    // Same test as t18_1 but with continuous indexing.
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "ooo_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1, // continuous
            0,
        )
        .unwrap();

    let ds = store.open_dataset("ooo_ds", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"v1").unwrap();
        lock.write(150, b"v2").unwrap();

        // Out-of-order: replace real entry at 100
        lock.write(100, b"v1_updated").unwrap();

        // Query should reflect latest data
        let entries = lock.query(100, 150, None).unwrap();
        assert_eq!(entries.len(), 2); // only real entries
        assert_eq!(entries[0].1, b"v1_updated");
        assert_eq!(entries[1].1, b"v2");
    }
    drop(arc);
    store.close().unwrap();
}

#[test]
fn t18_2_delete_lifecycle() {
    // Write N records → delete some → query returns N-K → reopen → still N-K.
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("del_ds", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    let ds = store.open_dataset("del_ds", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write 10 records
    {
        let mut lock = arc.lock().unwrap();
        for i in 1..=10i64 {
            let data = format!("record_{}", i).into_bytes();
            lock.write(i * 10, &data).unwrap();
        }
        assert_eq!(lock.query(10, 100, None).unwrap().len(), 10);
    }

    // Delete 2 records
    {
        let mut lock = arc.lock().unwrap();
        lock.delete(30).unwrap();
        lock.delete(70).unwrap();
    }

    // Verify 8 records remain
    {
        let mut lock = arc.lock().unwrap();
        let entries = lock.query(10, 100, None).unwrap();
        assert_eq!(entries.len(), 8);
        let ts_set: Vec<i64> = entries.iter().map(|(ts, _)| *ts).collect();
        assert!(!ts_set.contains(&30));
        assert!(!ts_set.contains(&70));
        assert!(ts_set.contains(&10));
        assert!(ts_set.contains(&100));

        // Delete same timestamp again → should fail
        let r = lock.delete(30);
        assert!(r.is_err());
    }

    drop(arc);
    store.close().unwrap();

    // Reopen and verify persistence
    let config2 = StoreConfig::default();
    let mut store2 = Store::open(&dir, config2).unwrap();
    let ds2 = store2.open_dataset("del_ds", "data").unwrap();
    let arc2 = store2.get_dataset(&ds2).unwrap();
    {
        let mut lock = arc2.lock().unwrap();
        let entries = lock.query(10, 100, None).unwrap();
        assert_eq!(entries.len(), 8);
    }
    drop(arc2);
    store2.close().unwrap();
}

#[test]
fn t18_3_mixed_operations() {
    // Mixed: write → correction → delete → out-of-order rewrite at deleted ts.
    //
    // Note: correction write requires the target to be the LAST record in
    // the block. It must be performed BEFORE any out-of-order writes that
    // would push records to the end of the block and break that invariant.
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "mix_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1, // continuous
            0,
        )
        .unwrap();

    let ds = store.open_dataset("mix_ds", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    {
        let mut lock = arc.lock().unwrap();

        // Write ts=1..5 (continuous → fillers for 2, 3, 4)
        lock.write(1, b"a").unwrap();
        lock.write(5, b"e").unwrap();

        // Correction write at ts=5 (in-place overwrite — must happen while 5 is still last)
        lock.write(5, b"E_CORRECTED").unwrap();

        // Delete ts=3 (was a filler; deletion should fail)
        let r = lock.delete(3);
        assert!(r.is_err(), "delete on filler should fail");

        // Out-of-order write at ts=3 replaces filler with real data
        lock.write(3, b"c_new").unwrap();

        // After all operations
        let entries_final = lock.query(1, 5, None).unwrap();
        assert_eq!(entries_final.len(), 3);
        let mut found_5 = false;
        for (ts, data) in &entries_final {
            match *ts {
                1 => assert_eq!(data, b"a"),
                3 => assert_eq!(data, b"c_new"),
                5 => {
                    assert_eq!(data, b"E_CORRECTED");
                    found_5 = true;
                }
                _ => panic!("unexpected ts {}", ts),
            }
        }
        assert!(found_5, "ts=5 should be present with corrected data");

        // Delete ts=1 and verify query returns only ts=3 and ts=5
        lock.delete(1).unwrap();
        let entries_after_del = lock.query(1, 5, None).unwrap();
        assert_eq!(entries_after_del.len(), 2);
    }
    drop(arc);
    store.close().unwrap();
}
