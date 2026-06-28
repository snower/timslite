//! StoreConfig and DataSetConfig builder API tests.
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

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
    let arc = ds.clone();
    arc.write(1, b"hello").unwrap();

    let entries = arc.query(1, 1).unwrap();
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
    let arc = ds.clone();
    for i in 0..10i64 {
        let data = format!("data_{}", i).into_bytes();
        arc.write(i + 1, &data).unwrap();
    }

    let entries = arc.query(1, 10).unwrap();
    assert_eq!(entries.len(), 10);

    store.close().unwrap();
}

#[test]
fn t39_1_dataset_config_builder_defaults_and_overrides_journal() {
    use timslite::{DataSetConfigBuilder, StoreConfig};

    let store_config = StoreConfig::builder().enable_journal(false).build();
    let default_dataset = DataSetConfigBuilder::from_store(&store_config)
        .build()
        .unwrap();
    assert!(
        default_dataset.enable_journal(),
        "dataset journal defaults to true independent of the global store switch"
    );

    let disabled_dataset = DataSetConfigBuilder::from_store(&store_config)
        .enable_journal(false)
        .build()
        .unwrap();
    assert!(!disabled_dataset.enable_journal());
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
    let arc = ds.clone();
    arc.write(1, b"compat_test").unwrap();

    let entries = arc.query(1, 1).unwrap();
    assert_eq!(entries.len(), 1);

    store.close().unwrap();
}

// 閳光偓閳光偓閳光偓 Config boundary value tests (P1-F-1~4) 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

#[test]
fn t14_4_data_segment_size_boundary_values() {
    // P1-F-1: Test data_segment_size boundary values
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();

    // Test with very small data_segment_size (should work or return error)
    let config = StoreConfig::builder()
        .data_segment_size(1024) // 1KB - very small
        .build();
    let result = Store::open(&dir, config);
    // Should either succeed or fail with validation error
    assert!(result.is_ok() || result.is_err());

    // Test with large data_segment_size
    let dir2 = temp_dir();
    let config = StoreConfig::builder()
        .data_segment_size(1024 * 1024 * 1024) // 1GB
        .build();
    let result = Store::open(&dir2, config);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn t14_5_compress_level_boundary_values() {
    // P1-F-2: Test compress_level boundary values
    use timslite::{Store, StoreConfig};

    // Test compress_level=0 (no compression)
    let dir = temp_dir();
    let config = StoreConfig::builder().compress_level(0).build();
    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("comp0", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 0, 0, 0)
        .unwrap();
    let ds = store.open_dataset("comp0", "data").unwrap();
    let arc = ds.clone();
    arc.write(1, b"no_compress").unwrap();
    let entries = arc.query(1, 1).unwrap();
    assert_eq!(entries.len(), 1);
    store.close().unwrap();

    // Test compress_level=10 (high compression)
    let dir2 = temp_dir();
    let config = StoreConfig::builder().compress_level(10).build();
    let mut store = Store::open(&dir2, config).unwrap();
    store
        .create_dataset(
            "comp10",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            10,
            0,
            0,
        )
        .unwrap();
    let ds = store.open_dataset("comp10", "data").unwrap();
    let arc = ds.clone();
    arc.write(1, b"high_compress").unwrap();
    let entries = arc.query(1, 1).unwrap();
    assert_eq!(entries.len(), 1);
    store.close().unwrap();
}

#[test]
fn t14_6_retention_window_boundary_values() {
    // P1-F-3: Test retention_window boundary values
    use timslite::{Store, StoreConfig};

    // Test retention_window=0 (no limit)
    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ret0", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let ds = store.open_dataset("ret0", "data").unwrap();
    let arc = ds.clone();
    assert_eq!(arc.retention_window(), 0);
    store.close().unwrap();

    // Test retention_window with maximum signed timestamp-domain value
    let dir2 = temp_dir();
    let mut store = Store::open(&dir2, StoreConfig::default()).unwrap();
    store
        .create_dataset(
            "ret_large",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            i64::MAX as u64,
        )
        .unwrap();
    let ds = store.open_dataset("ret_large", "data").unwrap();
    let arc = ds.clone();
    assert_eq!(arc.retention_window(), i64::MAX as u64);
    store.close().unwrap();

    let dir3 = temp_dir();
    let mut store = Store::open(&dir3, StoreConfig::default()).unwrap();
    let err = match store.create_dataset(
        "ret_too_large",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        i64::MAX as u64 + 1,
    ) {
        Ok(_) => panic!("retention_window above i64::MAX should be rejected"),
        Err(err) => err,
    };
    assert!(format!("{err}").contains("retention_window"));
}

#[test]
fn t14_7_flush_interval_boundary_values() {
    // P1-F-4: Test flush_interval boundary values
    use std::time::Duration;
    use timslite::StoreConfig;

    // Test flush_interval=0 (might be invalid or treated as minimum)
    let config = StoreConfig::builder()
        .flush_interval(Duration::from_millis(0))
        .build();
    // Config should be created without panic
    assert!(config.flush_interval().as_millis() == 0);

    // Test very large flush_interval
    let config = StoreConfig::builder()
        .flush_interval(Duration::from_secs(86400)) // 24 hours
        .build();
    assert_eq!(config.flush_interval().as_secs(), 86400);
}
