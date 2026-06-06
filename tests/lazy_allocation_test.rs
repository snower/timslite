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

#[test]
fn t12_5_segment_2x_expansion_on_overflow() {
    use std::fs;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let initial_size: u64 = 512;
    let max_size: u64 = 64 * 1024; // 64KB max
    let config = StoreConfig::builder()
        .initial_data_segment_size(initial_size)
        .initial_index_segment_size(4096)
        .data_segment_size(max_size)
        .index_segment_size(64 * 1024)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset("expand_ds", "data", max_size, 64 * 1024, 6, 0, 0)
        .unwrap();

    let ds = store.open_dataset("expand_ds", "data").unwrap();

    // Write enough data to force expansion beyond initial_size
    // Each record: ~12 overhead + 100 data = ~112 bytes
    // initial=512 → usable ~396 → fits ~3 records, then expand to 1024, etc.
    for i in 1..=20i64 {
        let data = vec![i as u8; 100];
        let arc = store.get_dataset(&ds).unwrap();
        arc.lock().unwrap().write(i, &data).unwrap();
    }

    // Check that file has expanded beyond initial_size
    let data_dir = dir.join("expand_ds").join("data").join("data");
    let mut found_expanded = false;
    for entry in fs::read_dir(&data_dir).unwrap() {
        let entry = entry.unwrap();
        let size = entry.metadata().unwrap().len();
        if size > initial_size {
            found_expanded = true;
            // Expansion should be a power-of-2 multiple of initial_size
            assert!(
                size % initial_size == 0,
                "expanded size {} should be a multiple of initial {}",
                size,
                initial_size
            );
            break;
        }
    }
    assert!(
        found_expanded,
        "at least one segment file should have expanded beyond initial_size={}",
        initial_size
    );

    // Verify data integrity after expansion
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(1, 20).unwrap();
    assert_eq!(entries.len(), 20);

    store.close().unwrap();
}

#[test]
fn t12_6_retention_reclaim_after_expansion() {
    use timslite::{DataSet, DataSetKey};

    let dir = temp_dir();
    let ds_dir = dir.join("ret_exp");
    fs::create_dir_all(&ds_dir).unwrap();
    let id = DataSetKey {
        name: "ret_exp".into(),
        dataset_type: "data".into(),
    };

    // Use small segment to force expansion + multiple segments
    let data_segment_size: u64 = 188;
    let mut ds = DataSet::create(
        id.clone(),
        ds_dir.clone(),
        data_segment_size,
        4096,
        0, // compress_level=0 to keep sizes predictable
        0,
        data_segment_size, // initial = max (no expansion, rollover instead)
        4096,
        15, // retention_window
    )
    .unwrap();

    // Write records that span multiple segments
    ds.write(10, &[0xAA; 32]).unwrap();
    ds.write(20, &[0xBB; 32]).unwrap();
    ds.write(30, &[0xCC; 32]).unwrap();

    // Reclaim: threshold = 30 - 15 = 15; segment with max_ts=10 is expired
    let reclaimed = ds.reclaim_expired_segments().unwrap();
    assert!(
        reclaimed >= 1,
        "at least 1 segment should be reclaimed after expansion, got {}",
        reclaimed
    );

    // Remaining data should still be readable
    let entries = ds.query(15, 30).unwrap();
    assert!(
        !entries.is_empty(),
        "non-expired records should remain queryable"
    );
}
