//! Continuous index mode deep scenario tests.
//!
//! Covers large-gap writes, logical hole backfill, correction on filler positions,
//! segment_capacity calculation, base_timestamp persistence, negative timestamps,
//! and multi-segment gap scenarios.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use timslite::{DataSetConfigBuilder, Store, StoreConfig};

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

fn store_config() -> StoreConfig {
    StoreConfig::builder()
        .enable_background_thread(false)
        .data_segment_size(1024 * 1024)
        .index_segment_size(64 * 1024)
        .initial_data_segment_size(4096)
        .initial_index_segment_size(4096)
        .build()
}

fn continuous_ds_config(store_cfg: &StoreConfig) -> DataSetConfigBuilder {
    DataSetConfigBuilder::from_store(store_cfg)
        .index_continuous(1)
        .data_segment_size(4096)
        .index_segment_size(4096)
}

/// Helper: create store + continuous dataset, return (store, dataset_arc).
fn setup_continuous(name: &str) -> (Store, std::sync::Arc<timslite::DataSet>) {
    let dir = temp_dir();
    let cfg = store_config();
    let mut store = Store::open(&dir, cfg.clone()).unwrap();
    let ds_cfg = continuous_ds_config(&cfg);
    let handle = store
        .create_dataset_with_config(name, "data", Some(ds_cfg))
        .unwrap();
    let ds = store.get_dataset(&handle).unwrap();
    (store, ds)
}

#[test]
fn t32_1_large_gap_no_intermediate_segments() {
    let (mut store, ds) = setup_continuous("large_gap");

    // First write sets base_timestamp = 100
    ds.write(100, b"first").unwrap();
    ds.flush().unwrap();

    // Large gap write: ts=1000000
    ds.write(1000000, b"second").unwrap();
    ds.flush().unwrap();

    let info = store.inspect_dataset("large_gap", "data").unwrap();

    // With index_segment_size=4096, segment_capacity = floor((4096-128)/18) = 220.
    // ts=100 is in ordinal 0, ts=1000000 is in ordinal floor((1000000-100)/220) = 4545.
    // Only the tail of ordinal 0 and the prefix of ordinal 4545 are materialized;
    // all intermediate ordinals (1..4544) remain logical holes.
    // So we expect exactly 2 index segments on disk.
    assert_eq!(
        info.state.index_segments, 2,
        "expected 2 index segments for large gap"
    );

    // Both records readable
    let r1 = ds.read(100).unwrap();
    assert!(r1.is_some());
    assert_eq!(r1.unwrap().1, b"first");

    let r2 = ds.read(1000000).unwrap();
    assert!(r2.is_some());
    assert_eq!(r2.unwrap().1, b"second");

    // Intermediate timestamps should be empty (logical hole)
    let r_mid = ds.read(500000).unwrap();
    assert!(
        r_mid.is_none(),
        "intermediate ts in logical hole should be None"
    );

    store.close().unwrap();
}

#[test]
fn t32_2_backfill_logical_hole() {
    let (mut store, ds) = setup_continuous("backfill_hole");

    ds.write(100, b"first").unwrap();
    ds.write(1000000, b"second").unwrap();
    ds.flush().unwrap();

    // Now backfill into the logical hole
    ds.write(500000, b"backfill").unwrap();
    ds.flush().unwrap();

    let info = store.inspect_dataset("backfill_hole", "data").unwrap();

    // We now have 3 segments: ordinal 0 (ts=100), ordinal 2272 (ts=500000),
    // ordinal 4545 (ts=1000000). The new segment should have prefix fillers
    // from segment_start to ts=499999.
    assert_eq!(
        info.state.index_segments, 3,
        "expected 3 index segments after backfill"
    );

    // All three records readable
    assert_eq!(ds.read(100).unwrap().unwrap().1, b"first");
    assert_eq!(ds.read(500000).unwrap().unwrap().1, b"backfill");
    assert_eq!(ds.read(1000000).unwrap().unwrap().1, b"second");

    // Query across all three should return exactly 3 entries (fillers skipped)
    let entries = ds.query(100, 1000000).unwrap();
    assert_eq!(
        entries.len(),
        3,
        "query should return exactly 3 real entries"
    );

    store.close().unwrap();
}

#[test]
fn t32_3_correction_on_filler_position() {
    let (mut store, ds) = setup_continuous("corr_filler");

    // Write ts=100, then ts=120 – the gap 101..119 are fillers within the same segment
    ds.write(100, b"orig_100").unwrap();
    ds.write(120, b"orig_120").unwrap();
    ds.flush().unwrap();

    // ts=110 is a filler position; correction write should succeed
    // (write with ts < latest triggers continuous-mode backfill logic)
    ds.write(110, b"filled_110").unwrap();
    ds.flush().unwrap();

    // Verify the correction took effect
    let r = ds.read(110).unwrap();
    assert!(r.is_some(), "filler position should now have data");
    assert_eq!(r.unwrap().1, b"filled_110");

    // Original entries still intact
    assert_eq!(ds.read(100).unwrap().unwrap().1, b"orig_100");
    assert_eq!(ds.read(120).unwrap().unwrap().1, b"orig_120");

    // Query should now return 3 entries
    let entries = ds.query(100, 120).unwrap();
    assert_eq!(entries.len(), 3);

    store.close().unwrap();
}

#[test]
fn t32_4_segment_capacity_calculation() {
    // segment_capacity = floor((index_segment_size - 128) / 18)
    // For index_segment_size = 4096: floor((4096 - 128) / 18) = floor(3968 / 18) = 220
    let segment_capacity: i64 = (4096 - 128) / 18;
    assert_eq!(segment_capacity, 220);

    // Verify by filling exactly segment_capacity entries in one segment.
    // base_timestamp = 0, so segment covers ts 0..219 (220 entries).
    // Writing ts=0..219 should fit in 1 segment; writing ts=220 should create a 2nd.
    let (mut store, ds) = setup_continuous("capacity_test");

    for ts in 0..segment_capacity {
        ds.write(ts, b"x").unwrap();
    }
    ds.flush().unwrap();

    let info = store.inspect_dataset("capacity_test", "data").unwrap();
    assert_eq!(
        info.state.index_segments, 1,
        "segment_capacity entries should fit in 1 segment"
    );

    // One more write beyond capacity → new segment
    ds.write(segment_capacity, b"overflow").unwrap();
    ds.flush().unwrap();

    let info = store.inspect_dataset("capacity_test", "data").unwrap();
    assert_eq!(
        info.state.index_segments, 2,
        "writing beyond capacity should create a 2nd segment"
    );

    store.close().unwrap();
}

#[test]
fn t32_5_reopen_base_timestamp_preserved() {
    let dir = temp_dir();
    let cfg = store_config();

    // Phase 1: write and close
    {
        let mut store = Store::open(&dir, cfg.clone()).unwrap();
        let ds_cfg = continuous_ds_config(&cfg);
        let handle = store
            .create_dataset_with_config("reopen_bt", "data", Some(ds_cfg))
            .unwrap();
        let ds = store.get_dataset(&handle).unwrap();

        ds.write(500, b"hello").unwrap();
        ds.write(700, b"world").unwrap();
        ds.flush().unwrap();

        let info = store.inspect_dataset("reopen_bt", "data").unwrap();
        assert_eq!(info.state.base_timestamp, Some(500));

        store.close().unwrap();
    }

    // Phase 2: reopen and verify base_timestamp
    {
        let mut store = Store::open(&dir, cfg.clone()).unwrap();
        let handle = store.open_dataset("reopen_bt", "data").unwrap();
        let ds = store.get_dataset(&handle).unwrap();

        let info = store.inspect_dataset("reopen_bt", "data").unwrap();
        assert_eq!(
            info.state.base_timestamp,
            Some(500),
            "base_timestamp must be preserved after reopen"
        );

        // Data must still be readable
        assert_eq!(ds.read(500).unwrap().unwrap().1, b"hello");
        assert_eq!(ds.read(700).unwrap().unwrap().1, b"world");

        // Write after reopen with a gap
        ds.write(1000, b"after_reopen").unwrap();
        ds.flush().unwrap();
        assert_eq!(ds.read(1000).unwrap().unwrap().1, b"after_reopen");

        store.close().unwrap();
    }
}

#[test]
fn t32_6_negative_base_timestamp() {
    let (mut store, ds) = setup_continuous("neg_ts");

    ds.write(-100, b"neg").unwrap();
    ds.write(-50, b"less_neg").unwrap();
    ds.write(0, b"zero").unwrap();
    ds.write(50, b"pos").unwrap();
    ds.flush().unwrap();

    let info = store.inspect_dataset("neg_ts", "data").unwrap();
    assert_eq!(info.state.base_timestamp, Some(-100));
    assert_eq!(info.state.latest_written_timestamp, Some(50));
    assert_eq!(
        info.state.index_segments, 1,
        "all 4 entries should fit in 1 segment"
    );

    // Verify all records
    assert_eq!(ds.read(-100).unwrap().unwrap().1, b"neg");
    assert_eq!(ds.read(-50).unwrap().unwrap().1, b"less_neg");
    assert_eq!(ds.read(0).unwrap().unwrap().1, b"zero");
    assert_eq!(ds.read(50).unwrap().unwrap().1, b"pos");

    // Filler positions should be None
    assert!(ds.read(-75).unwrap().is_none());
    assert!(ds.read(-25).unwrap().is_none());
    assert!(ds.read(25).unwrap().is_none());

    // Query across all
    let entries = ds.query(-100, 50).unwrap();
    assert_eq!(entries.len(), 4);

    store.close().unwrap();
}

#[test]
fn t32_7_multiple_segments_with_gaps() {
    // segment_capacity = 220 for index_segment_size=4096.
    // Write at ts=0, ts=500, ts=1500 to span 3 segments:
    //   segment 0: ts 0..219   (ts=0 is here)
    //   segment 1: ts 220..439 (ts=500 would be in segment 2)
    //   segment 2: ts 440..659 (ts=500 is here)
    //   segment 7: ts 1540..1759 (ts=1500 is in segment 6: ts 1320..1539)
    // Actually let me recalculate:
    //   seg_ord(ts) = floor((ts - 0) / 220)
    //   seg_ord(0) = 0
    //   seg_ord(500) = floor(500/220) = 2  → segment start = 440
    //   seg_ord(1500) = floor(1500/220) = 6 → segment start = 1320
    // So writes at ts=0, 500, 1500 span segments 0, 2, 6 → 3 segments on disk.
    let (mut store, ds) = setup_continuous("multi_seg_gap");

    ds.write(0, b"seg0").unwrap();
    ds.flush().unwrap();

    // ts=500 is in segment 2 (ordinal 2). Gap: segment 1 is a logical hole.
    ds.write(500, b"seg2").unwrap();
    ds.flush().unwrap();

    // ts=1500 is in segment 6 (ordinal 6). Gap: segments 3-5 are logical holes.
    ds.write(1500, b"seg6").unwrap();
    ds.flush().unwrap();

    let info = store.inspect_dataset("multi_seg_gap", "data").unwrap();
    assert_eq!(
        info.state.index_segments, 3,
        "expected 3 index segments: ordinal 0, 2, and 6"
    );
    assert_eq!(info.state.latest_written_timestamp, Some(1500));
    assert_eq!(info.state.base_timestamp, Some(0));

    // All real entries readable
    assert_eq!(ds.read(0).unwrap().unwrap().1, b"seg0");
    assert_eq!(ds.read(500).unwrap().unwrap().1, b"seg2");
    assert_eq!(ds.read(1500).unwrap().unwrap().1, b"seg6");

    // Logical hole positions should be None
    assert!(
        ds.read(300).unwrap().is_none(),
        "ts=300 in logical hole (segment 1)"
    );
    assert!(
        ds.read(800).unwrap().is_none(),
        "ts=800 in logical hole (segment 3)"
    );
    assert!(
        ds.read(1200).unwrap().is_none(),
        "ts=1200 in logical hole (segment 5)"
    );

    // Query should return exactly 3 entries
    let entries = ds.query(0, 1500).unwrap();
    assert_eq!(entries.len(), 3);

    store.close().unwrap();
}
