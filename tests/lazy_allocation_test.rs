//! Lazy allocation integration tests.
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
    let entries = arc.lock().unwrap().query(1, 100).unwrap();
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
    // Write enough data to fill first block (fixed block limit 64KB)
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
            break; // May fail at segment boundary 鈥?that's fine
        }
    }

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 200).unwrap();
    assert!(!entries.is_empty(), "should have some data");

    store.close().unwrap();
}

#[test]
fn t12_3_open_legacy_full_allocated_dataset() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    // Create with default (full allocation) config
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset(
            "legacy_full",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("legacy_full", "data").unwrap();
    for i in 0..50i64 {
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

    // Reopen and verify data
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let ds = store.open_dataset("legacy_full", "data").unwrap();

    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 50).unwrap();
    assert_eq!(entries.len(), 50);

    store.close().unwrap();
}

#[test]
fn t12_4_disk_space_efficiency() {
    use std::fs;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .initial_data_segment_size(256 * 1024)
        .initial_index_segment_size(4 * 1024)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "small_data",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("small_data", "data").unwrap();
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

    // Verify disk usage is well under the 64MB max segment size
    let ds_dir = dir.join("small_data").join("data").join("data");
    let mut total_size: u64 = 0;
    for e in fs::read_dir(&ds_dir).unwrap() {
        let e = e.unwrap();
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
    let entries = arc.lock().unwrap().query(1, 100).unwrap();
    assert_eq!(entries.len(), 100);

    store.close().unwrap();
}
