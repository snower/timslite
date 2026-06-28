//! Recovery tests for Store-managed datasets after flush and drop without close.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use timslite::{Store, StoreConfig};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("timslite_crash_recovery");
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = d.join(format!(
        "{}_{:?}_{id}",
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn store_config() -> StoreConfig {
    StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .data_segment_size(64 * 1024 * 1024)
        .index_segment_size(4 * 1024 * 1024)
        .initial_data_segment_size(4096)
        .initial_index_segment_size(4096)
        .compress_level(6)
        .build()
}

#[test]
fn t_crash_recover_pending_block_after_drop_without_close() {
    let dir = temp_dir("crash_pending");
    let config = store_config();

    // Phase 1: write records, flush, then drop without close.
    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let handle = store
            .create_dataset_with_config("crash_ds", "data", None)
            .unwrap();
        let ds = handle.clone();

        ds.write(10, b"rec_10").unwrap();
        ds.write(20, b"rec_20").unwrap();
        ds.write(30, b"rec_30").unwrap();
        ds.flush().unwrap();
        // Drop without calling ds.close(); pending block stays raw on disk.
    }

    // Phase 2: reopen and verify data is recoverable.
    {
        let mut store = Store::open(&dir, config).unwrap();
        let handle = store.open_dataset("crash_ds", "data").unwrap();
        let ds = handle.clone();

        let (ts, data) = ds.read(10).unwrap().unwrap();
        assert_eq!(ts, 10);
        assert_eq!(data, b"rec_10");

        let (ts, data) = ds.read(20).unwrap().unwrap();
        assert_eq!(ts, 20);
        assert_eq!(data, b"rec_20");

        let (ts, data) = ds.read(30).unwrap().unwrap();
        assert_eq!(ts, 30);
        assert_eq!(data, b"rec_30");

        ds.write(40, b"rec_40").unwrap();
        ds.write(50, b"rec_50").unwrap();

        let entries = ds.query(1, 100).unwrap();
        assert_eq!(
            entries.len(),
            5,
            "all 5 records should be queryable after recovery"
        );
        assert_eq!(entries[0].0, 10);
        assert_eq!(entries[4].0, 50);

        ds.close().unwrap();
    }
}

#[test]
fn t_crash_recover_index_segment_integrity() {
    let dir = temp_dir("crash_index");
    let config = store_config();

    // Phase 1: write enough records to span multiple index entries, flush, then drop without close.
    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let handle = store
            .create_dataset_with_config("idx_ds", "data", None)
            .unwrap();
        let ds = handle.clone();

        for i in 1..=30i64 {
            ds.write(i * 10, format!("rec_{}", i * 10).as_bytes())
                .unwrap();
        }
        ds.flush().unwrap();
        // Drop without close; index segment stays as-is on disk.
    }

    // Phase 2: reopen and verify index integrity.
    {
        let mut store = Store::open(&dir, config).unwrap();
        let handle = store.open_dataset("idx_ds", "data").unwrap();
        let ds = handle.clone();

        let info = ds.inspect().unwrap();
        assert!(
            info.state.index_segments > 0 || info.state.pending_index_entries > 0,
            "index segments or pending entries should exist after recovery: {:?}",
            info.state
        );

        if let Some(base_ts) = info.state.base_timestamp {
            assert_eq!(base_ts, 10, "base_timestamp should be 10");
        }

        let entries = ds.query(1, 300).unwrap();
        assert_eq!(
            entries.len(),
            30,
            "all 30 records should be queryable via index after recovery"
        );
        assert_eq!(entries[0].0, 10);
        assert_eq!(entries[29].0, 300);

        for i in 1..=30i64 {
            let (ts, data) = ds.read(i * 10).unwrap().unwrap();
            assert_eq!(ts, i * 10);
            assert_eq!(data, format!("rec_{}", i * 10).as_bytes());
        }

        assert_eq!(
            info.state.latest_written_timestamp,
            Some(300),
            "latest_written_timestamp should be 300"
        );

        ds.close().unwrap();
    }
}

#[test]
fn t_crash_recover_multiple_pending_records_after_drop() {
    let dir = temp_dir("crash_multi_pending");
    let config = store_config();

    // Phase 1: write many records in the same pending block, flush, then drop without close.
    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let handle = store
            .create_dataset_with_config("multi_p", "data", None)
            .unwrap();
        let ds = handle.clone();

        for i in 1..=20i64 {
            let data = format!("payload_{}", i);
            ds.write(i * 10, data.as_bytes()).unwrap();
        }
        ds.flush().unwrap();
        // Drop without close.
    }

    // Phase 2: reopen and verify.
    {
        let mut store = Store::open(&dir, config).unwrap();
        let handle = store.open_dataset("multi_p", "data").unwrap();
        let ds = handle.clone();

        for i in 1..=20i64 {
            let result = ds.read(i * 10).unwrap();
            assert!(
                result.is_some(),
                "record at ts={} should be readable after recovery",
                i * 10
            );
            let (ts, data) = result.unwrap();
            assert_eq!(ts, i * 10);
            assert_eq!(data, format!("payload_{}", i).as_bytes());
        }

        let entries = ds.query(1, 200).unwrap();
        assert_eq!(entries.len(), 20);

        ds.close().unwrap();
    }
}
