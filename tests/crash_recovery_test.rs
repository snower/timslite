//! Crash recovery tests: pending block restore after abrupt drop + reopen.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

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

#[test]
fn t_crash_recover_pending_block_after_drop_without_close() {
    use timslite::{DataSet, DataSetKey};

    let dir = temp_dir("crash_pending");
    let id = DataSetKey {
        name: "crash_ds".into(),
        dataset_type: "data".into(),
    };

    // Phase 1: write records, drop without close (simulate crash)
    {
        let ds = DataSet::create(
            id.clone(),
            dir.clone(),
            64 * 1024 * 1024, // data_segment_size
            4 * 1024 * 1024,  // index_segment_size
            6,                // compress_level
            0,                // index_continuous
            4096,             // initial_data_segment_size
            4096,             // initial_index_segment_size
            0,                // retention_window
        )
        .unwrap();

        ds.write(10, b"rec_10").unwrap();
        ds.write(20, b"rec_20").unwrap();
        ds.write(30, b"rec_30").unwrap();
        // Drop without calling ds.close() — pending block stays raw on disk
    }

    // Phase 2: reopen and verify data is recoverable
    {
        let ds = DataSet::open(id.clone(), dir.clone()).unwrap();

        // Pending records should be readable from mmap
        let (ts, data) = ds.read(10).unwrap().unwrap();
        assert_eq!(ts, 10);
        assert_eq!(data, b"rec_10");

        let (ts, data) = ds.read(20).unwrap().unwrap();
        assert_eq!(ts, 20);
        assert_eq!(data, b"rec_20");

        let (ts, data) = ds.read(30).unwrap().unwrap();
        assert_eq!(ts, 30);
        assert_eq!(data, b"rec_30");

        // Write more records to trigger sealing of old pending block
        ds.write(40, b"rec_40").unwrap();
        ds.write(50, b"rec_50").unwrap();

        // Query all records
        let entries = ds.query(1, 100).unwrap();
        assert_eq!(
            entries.len(),
            5,
            "all 5 records should be queryable after crash recovery"
        );
        assert_eq!(entries[0].0, 10);
        assert_eq!(entries[4].0, 50);

        ds.close().unwrap();
    }
}

#[test]
fn t_crash_recover_index_segment_integrity() {
    use timslite::{DataSet, DataSetKey};

    let dir = temp_dir("crash_index");
    let id = DataSetKey {
        name: "idx_ds".into(),
        dataset_type: "data".into(),
    };

    // Phase 1: write enough records to span multiple index entries, drop without close
    {
        let ds = DataSet::create(
            id.clone(),
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024, // index_segment_size
            6,               // compress_level
            0,               // index_continuous
            4096,            // initial_data_segment_size
            4096,            // initial_index_segment_size
            0,               // retention_window
        )
        .unwrap();

        // Write 30 records with timestamps at intervals of 10
        for i in 1..=30i64 {
            ds.write(i * 10, format!("rec_{}", i * 10).as_bytes())
                .unwrap();
        }
        // Drop without close — index segment stays as-is on disk
    }

    // Phase 2: reopen and verify index integrity
    {
        let ds = DataSet::open(id.clone(), dir.clone()).unwrap();

        // Verify inspect shows index segments exist
        let info = ds.inspect().unwrap();
        assert!(
            info.state.index_segments > 0 || info.state.pending_index_entries > 0,
            "index segments or pending entries should exist after crash recovery: {:?}",
            info.state
        );

        // Verify base_timestamp is correct (first record at ts=10)
        // base_timestamp may be None if index is pending flush, so check query instead
        if let Some(base_ts) = info.state.base_timestamp {
            assert_eq!(base_ts, 10, "base_timestamp should be 10");
        }

        // Verify all records are queryable (proves index is functional)
        let entries = ds.query(1, 300).unwrap();
        assert_eq!(
            entries.len(),
            30,
            "all 30 records should be queryable via index after crash"
        );
        assert_eq!(entries[0].0, 10);
        assert_eq!(entries[29].0, 300);

        // Verify specific timestamp lookups work (proves index mapping is correct)
        for i in 1..=30i64 {
            let (ts, data) = ds.read(i * 10).unwrap().unwrap();
            assert_eq!(ts, i * 10);
            assert_eq!(data, format!("rec_{}", i * 10).as_bytes());
        }

        // Verify latest_written_timestamp is preserved
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
    use timslite::{DataSet, DataSetKey};

    let dir = temp_dir("crash_multi_pending");
    let id = DataSetKey {
        name: "multi_p".into(),
        dataset_type: "data".into(),
    };

    // Phase 1: write many records in same pending block, drop without close
    {
        let ds = DataSet::create(
            id.clone(),
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            4096,
            4096,
            0,
        )
        .unwrap();

        for i in 1..=20i64 {
            let data = format!("payload_{}", i);
            ds.write(i * 10, data.as_bytes()).unwrap();
        }
        // Drop without close
    }

    // Phase 2: reopen and verify
    {
        let ds = DataSet::open(id.clone(), dir.clone()).unwrap();

        // All 20 records should be readable
        for i in 1..=20i64 {
            let result = ds.read(i * 10).unwrap();
            assert!(
                result.is_some(),
                "record at ts={} should be readable after crash recovery",
                i * 10
            );
            let (ts, data) = result.unwrap();
            assert_eq!(ts, i * 10);
            assert_eq!(data, format!("payload_{}", i).as_bytes());
        }

        // Query all
        let entries = ds.query(1, 200).unwrap();
        assert_eq!(entries.len(), 20);

        ds.close().unwrap();
    }
}
