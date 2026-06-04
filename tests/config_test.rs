//! StoreConfig and DataSetConfig builder API tests.
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
