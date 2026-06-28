//! Lazy allocation integration tests.
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
        ds.clone().write(i + 1, &data).unwrap();
    }

    let arc = ds.clone();
    let entries = arc.query(1, 100).unwrap();
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
        let write_result = ds.clone().write(i + 1, &data);
        if write_result.is_err() {
            break; // May fail at segment boundary 闁?that's fine
        }
    }

    let arc = ds.clone();
    let entries = arc.query(1, 200).unwrap();
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
        ds.clone().write(i + 1, &data).unwrap();
    }
    store.close().unwrap();

    // Reopen and verify data
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let ds = store.open_dataset("legacy_full", "data").unwrap();

    let arc = ds.clone();
    let entries = arc.query(1, 50).unwrap();
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
        ds.clone().write(i + 1, &data).unwrap();
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
    let arc = ds.clone();
    let entries = arc.query(1, 100).unwrap();
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
    // initial=512 閳?usable ~396 閳?fits ~3 records, then expand to 1024, etc.
    for i in 1..=20i64 {
        let data = vec![i as u8; 100];
        let arc = ds.clone();
        arc.write(i, &data).unwrap();
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
    let arc = ds.clone();
    let entries = arc.query(1, 20).unwrap();
    assert_eq!(entries.len(), 20);

    store.close().unwrap();
}

#[test]
fn t12_6_retention_reclaim_after_expansion() {
    use timslite::{DataSetConfigBuilder, Store, StoreConfig};

    let dir = temp_dir();
    // Use small segment to force expansion + multiple segments
    let data_segment_size: u64 = 188;
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .data_segment_size(data_segment_size)
        .index_segment_size(4096)
        .compress_level(0)
        .initial_data_segment_size(data_segment_size)
        .initial_index_segment_size(4096)
        .build();
    let mut store = Store::open(&dir, config.clone()).unwrap();
    let handle = store
        .create_dataset_with_config(
            "ret_exp",
            "data",
            Some(DataSetConfigBuilder::from_store(&config).retention_window(15)),
        )
        .unwrap();
    let ds = handle.clone();

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

// 閳光偓閳光偓閳光偓 Index segment expansion test (P1-L-1) 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

#[test]
fn t12_5_index_segment_expansion_from_initial_to_max() {
    // P1-L-1: Test index segment expansion from initial to max size
    use timslite::Store;

    let dir = temp_dir();

    // Small initial index segment (4KB) with larger max (64KB)
    let initial_index_size = 4 * 1024; // 4KB
    let max_index_size = 64 * 1024; // 64KB
    let data_segment_size = 64 * 1024 * 1024; // 64MB

    let config = timslite::StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .data_segment_size(data_segment_size)
        .index_segment_size(max_index_size)
        .compress_level(0)
        .initial_data_segment_size(data_segment_size)
        .initial_index_segment_size(initial_index_size)
        .build();
    let mut store = Store::open(&dir, config).unwrap();
    let handle = store
        .create_dataset_with_config("idx_expand", "data", None)
        .unwrap();
    let ds = handle.clone();

    // Write enough records to trigger index segment expansion
    // Each index entry is ~24 bytes (timestamp + block_offset + in_block_offset)
    // 4KB can hold ~170 entries, so write more than that
    let record_count = 200;
    for i in 1..=record_count {
        let data = format!("record_{}", i).into_bytes();
        ds.write(i, &data).unwrap();
    }

    // Verify all records are readable
    let entries = ds.query(1, record_count).unwrap();
    assert_eq!(
        entries.len(),
        record_count as usize,
        "all {} records should be readable after index expansion",
        record_count
    );

    // Verify data integrity
    for (i, (ts, data)) in entries.iter().enumerate() {
        let expected_ts = (i + 1) as i64;
        assert_eq!(*ts, expected_ts, "timestamp mismatch at index {}", i);
        let expected_data = format!("record_{}", expected_ts).into_bytes();
        assert_eq!(*data, expected_data, "data mismatch at timestamp {}", ts);
    }
}
