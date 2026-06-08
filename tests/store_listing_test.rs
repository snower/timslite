//! Store dataset enumeration tests: get_dataset_names, get_dataset_types.

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
fn test_get_dataset_names_empty() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let store = Store::open(&dir, StoreConfig::default()).unwrap();

    let names = store.get_dataset_names().unwrap();
    assert!(names.is_empty(), "empty store should return empty names");

    store.close().unwrap();
}

#[test]
fn test_get_dataset_names_after_create() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "sensor_a",
            "temperature",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    store
        .create_dataset(
            "sensor_b",
            "humidity",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    store
        .create_dataset(
            "sensor_a",
            "pressure",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let mut names = store.get_dataset_names().unwrap();
    names.sort();
    assert_eq!(names, vec!["sensor_a", "sensor_b"]);

    store.close().unwrap();
}

#[test]
fn test_get_dataset_names_dedup() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    // Same name, different types
    store
        .create_dataset("device", "cpu", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    store
        .create_dataset(
            "device",
            "memory",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    store
        .create_dataset("device", "disk", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    let names = store.get_dataset_names().unwrap();
    assert_eq!(names, vec!["device"], "should deduplicate names");

    store.close().unwrap();
}

#[test]
fn test_get_dataset_types() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("server", "cpu", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    store
        .create_dataset(
            "server",
            "memory",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    store
        .create_dataset("server", "disk", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    store
        .create_dataset(
            "router",
            "bandwidth",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let server_types = store.get_dataset_types("server").unwrap();
    assert_eq!(server_types, vec!["cpu", "disk", "memory"]); // sorted

    let router_types = store.get_dataset_types("router").unwrap();
    assert_eq!(router_types, vec!["bandwidth"]);

    store.close().unwrap();
}

#[test]
fn test_get_dataset_types_not_found() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let store = Store::open(&dir, StoreConfig::default()).unwrap();

    let types = store.get_dataset_types("nonexistent").unwrap();
    assert!(
        types.is_empty(),
        "non-existent name should return empty types"
    );

    store.close().unwrap();
}

#[test]
fn test_get_dataset_types_after_drop() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("app", "logs", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    store
        .create_dataset("app", "metrics", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    store
        .create_dataset("app", "traces", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    // Drop one type
    store.drop_dataset_by_name("app", "metrics").unwrap();

    let types = store.get_dataset_types("app").unwrap();
    assert_eq!(types, vec!["logs", "traces"]); // sorted, metrics dropped

    store.close().unwrap();
}
