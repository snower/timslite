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

#[test]
fn t13_5_cross_segment_query() {
    use timslite::{DataSet, DataSetKey};

    let dir = temp_dir();
    let ds_dir = dir.join("cross_seg");
    fs::create_dir_all(&ds_dir).unwrap();
    let id = DataSetKey {
        name: "cross_seg".into(),
        dataset_type: "data".into(),
    };

    // Use small segment size to force data across multiple segments
    let data_segment_size: u64 = 180;
    let mut ds = DataSet::create(
        id.clone(),
        ds_dir.clone(),
        data_segment_size,
        4096,
        0, // no compression for predictable sizing
        0,
        data_segment_size, // initial = max → rollover on overflow
        4096,
        0,
    )
    .unwrap();

    // Write records that span multiple data segments
    for i in 1..=6i64 {
        let data = format!("record_{}", i);
        ds.write(i * 10, data.as_bytes()).unwrap();
    }

    // Verify multiple segment files exist
    let data_dir = ds_dir.join("data");
    let seg_count = fs::read_dir(&data_dir).unwrap().count();
    assert!(
        seg_count >= 2,
        "should have at least 2 data segment files, got {}",
        seg_count
    );

    // Query across all segments
    let entries = ds.query(1, 60).unwrap();
    assert_eq!(
        entries.len(),
        6,
        "cross-segment query should return all 6 records"
    );
    assert_eq!(entries[0].0, 10);
    assert_eq!(entries[5].0, 60);

    // Partial cross-segment query (middle range)
    let partial = ds.query(20, 40).unwrap();
    assert_eq!(partial.len(), 3);
    assert_eq!(partial[0].0, 20);
    assert_eq!(partial[2].0, 40);
}
