//! Dataset basic lifecycle tests: create, open, close, persistence, flush.
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
        let data: Vec<u8> = format!("a_{}", i).into_bytes();
        let ds_arc = store.get_dataset(&ds1).unwrap();
        ds_arc.lock().unwrap().write(i + 1, &data).unwrap();
    }
    for i in 0..30i64 {
        let data: Vec<u8> = format!("b_{}", i).into_bytes();
        let ds_arc = store.get_dataset(&ds2).unwrap();
        ds_arc.lock().unwrap().write(i + 1, &data).unwrap();
    }

    // Verify dataset_a
    {
        let ds_arc = store.get_dataset(&ds1).unwrap();
        let mut ds = ds_arc.lock().unwrap();
        let entries = ds.query(1, 50, None).unwrap();
        assert_eq!(entries.len(), 50);
        for (i, (ts, data)) in entries.iter().enumerate() {
            assert_eq!(*ts, (i + 1) as i64);
            assert_eq!(*data, format!("a_{}", i).as_bytes());
        }
    }

    // Verify dataset_b isolated
    {
        let ds_arc = store.get_dataset(&ds2).unwrap();
        let mut ds = ds_arc.lock().unwrap();
        let entries = ds.query(1, 30, None).unwrap();
        assert_eq!(entries.len(), 30);
        for (i, (ts, data)) in entries.iter().enumerate() {
            assert_eq!(*ts, (i + 1) as i64);
            assert_eq!(*data, format!("b_{}", i).as_bytes());
        }
    }

    store.close().unwrap();
}

#[test]
fn t8_1_3_block_aggregation() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "block_agg",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("block_agg", "data").unwrap();

    for i in 0..200i64 {
        let data: Vec<u8> = format!("value_{}", i).into_bytes();
        let arc = store.get_dataset(&ds).unwrap();
        arc.lock().unwrap().write(i + 1, &data).unwrap();
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
            let data = format!("persist_{}", i).into_bytes();
            let arc = store.get_dataset(&ds).unwrap();
            arc.lock().unwrap().write(i + 1, &data).unwrap();
        }
        let arc = store.get_dataset(&ds).unwrap();
        arc.lock().unwrap().flush().unwrap();
        store.close().unwrap();
    }

    // Reopen and verify data persists
    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        let ds = store.open_dataset("persist", "data").unwrap();
        let arc = store.get_dataset(&ds).unwrap();
        let entries = arc.lock().unwrap().query(1, 50, None).unwrap();
        assert_eq!(entries.len(), 50);
        for (i, (ts, data)) in entries.iter().enumerate() {
            assert_eq!(*ts, (i + 1) as i64);
            assert_eq!(*data, format!("persist_{}", i).as_bytes());
        }
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
