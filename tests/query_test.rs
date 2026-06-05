//! Query iterator and empty range tests.
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
fn t13_1_iterator_small_range() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "iter_small",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("iter_small", "data").unwrap();
    for i in 0..30i64 {
        let data = format!("item_{}", i).into_bytes();
        let arc = store.get_dataset(&ds).unwrap();
        arc.lock().unwrap().write(i * 10 + 10, &data).unwrap();
    }

    let arc = store.get_dataset(&ds).unwrap();
    // Query small range: ts 50..120 (should return items at 60,70,80,90,100,110)
    let entries = arc.lock().unwrap().query(50, 120).unwrap();
    assert!(!entries.is_empty());
    for (ts, _) in &entries {
        assert!(*ts >= 50 && *ts <= 120);
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
            "back_compat",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("back_compat", "data").unwrap();
    // ts_start > ts_end should return empty
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(100, 1).unwrap();
    assert_eq!(entries.len(), 0);

    store.close().unwrap();
}

#[test]
fn t13_4_query_empty_range() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "empty_range",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("empty_range", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Query before any writes 鈥?should return empty
    let entries = arc.lock().unwrap().query(1, 100).unwrap();
    assert_eq!(entries.len(), 0);

    store.close().unwrap();
}
