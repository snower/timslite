//! Query iterator and empty range tests.
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
        let arc = ds.clone();
        arc.write(i * 10 + 10, &data).unwrap();
    }

    let arc = ds.clone();
    // Query small range: ts 50..120 (should return items at 60,70,80,90,100,110)
    let entries = arc.query(50, 120).unwrap();
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
    let arc = ds.clone();
    let entries = arc.query(100, 1).unwrap();
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
    let arc = ds.clone();

    // Query before any writes 闁?should return empty
    let entries = arc.query(1, 100).unwrap();
    assert_eq!(entries.len(), 0);

    store.close().unwrap();
}

#[test]
fn t13_5_cross_segment_query() {
    use timslite::Store;

    let dir = temp_dir();

    // Use small segment size to force data across multiple segments
    let data_segment_size: u64 = 180;
    let config = timslite::StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .data_segment_size(data_segment_size)
        .index_segment_size(4096)
        .compress_level(0)
        .initial_data_segment_size(data_segment_size)
        .initial_index_segment_size(4096)
        .build();
    let mut store = Store::open(&dir, config).unwrap();
    let handle = store
        .create_dataset_with_config("cross_seg", "data", None)
        .unwrap();
    let ds = handle.clone();

    // Write records that span multiple data segments
    for i in 1..=6i64 {
        let data = format!("record_{}", i);
        ds.write(i * 10, data.as_bytes()).unwrap();
    }

    // Verify multiple segment files exist
    let data_dir = dir.join("cross_seg").join("data").join("data");
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

#[test]
fn t13_6_query_iterator_full_lifecycle() {
    // Comprehensive test covering all 5 query pipeline paths:
    // 1. Memory index buffer (unflushed writes)
    // 2. Disk index segments (flushed data)
    // 3. Lazy-open data segments (segments reopened after close)
    // 4. HotBlockCache (same-block sequential reads via QueryIterator)
    // 5. Global BlockCache (via Store + cache_max_memory)
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();

    // Small segment sizes to force multiple data + index segments
    let data_segment_size: u64 = 64 * 1024;
    let index_segment_size: u64 = 4096;

    // Use Store API to get BlockCache (path 5)
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .cache_max_memory(1024 * 1024)
        .initial_data_segment_size(data_segment_size)
        .initial_index_segment_size(index_segment_size)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "lifecycle",
            "data",
            data_segment_size,
            index_segment_size,
            6,
            0,
            0,
        )
        .unwrap();
    let handle = store.open_dataset("lifecycle", "data").unwrap();

    // Phase 1: Write data through Store (flushed to disk, paths 2, 3)
    {
        let arc = handle.clone();
        let ds = arc.clone();
        for i in 1..=50i64 {
            let data = format!("record_{:03}", i);
            ds.write(i * 100, data.as_bytes()).unwrap();
        }
        ds.flush().unwrap();
        ds.close().unwrap();
    }

    // Phase 2: Reopen dataset (path 3: lazy-open segments)
    let handle2 = store.open_dataset("lifecycle", "data").unwrap();
    {
        let arc = handle2.clone();
        let ds = arc.clone();

        // Write additional records that stay in memory buffer (path 1)
        for i in 51..=60i64 {
            let data = format!("record_{:03}", i);
            ds.write(i * 100, data.as_bytes()).unwrap();
        }

        // Full lifecycle query across disk + memory (paths 1+2+3+4+5)
        let entries = ds.query(100, 6000).unwrap();
        assert_eq!(
            entries.len(),
            60,
            "full lifecycle query should return all 60 records"
        );

        // Verify data integrity
        for (i, (ts, data)) in entries.iter().enumerate() {
            let expected_ts = (i as i64 + 1) * 100;
            let expected_data = format!("record_{:03}", i + 1);
            assert_eq!(*ts, expected_ts, "timestamp mismatch at index {}", i);
            assert_eq!(
                *data,
                expected_data.as_bytes(),
                "data mismatch at ts={}",
                ts
            );
        }

        // Query again 閳?BlockCache should serve cached reads (path 5)
        let entries2 = ds.query(100, 6000).unwrap();
        assert_eq!(entries2.len(), 60);

        // Partial query spanning disk + memory boundary
        let mid = ds.query(4500, 5500).unwrap();
        assert_eq!(mid.len(), 11, "ts 4500..5500 should return records 45..55");
        assert_eq!(mid[0].0, 4500);
        assert_eq!(mid[10].0, 5500);
    }

    store.close().unwrap();
}
