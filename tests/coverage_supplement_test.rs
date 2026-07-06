//! Coverage supplement tests for store and dataset operations.
//!
//! Targets uncovered code paths not exercised by existing integration tests:
//! - Store::open with various config combinations
//! - Store::drop_dataset lifecycle
//! - Store::inspect_dataset for various dataset states
//! - DataSet::write + read roundtrip with various data sizes
//! - DataSet::delete operations
//! - DataSet::correction writes
//! - DataSet::append operations
//! - DataSet::query with different ranges
//! - DataSet::query_iterator usage
//! - DataSet::read_latest operations
//! - Store with journal enabled vs disabled
//! - Store with cache enabled vs disabled
//! - Store with background tasks enabled vs disabled

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let d = std::env::temp_dir().join("timslite_integration");
    fs::create_dir_all(&d).expect("create base temp dir");
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    d.join(format!(
        "test_{:?}_{id}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ))
}

// ============================================================================
// Store::open with various config combinations
// ============================================================================

#[test]
fn store_open_default_config() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let store = Store::open(&dir, StoreConfig::default()).expect("open store with defaults");
    store.close().expect("close store");
}

#[test]
fn store_open_custom_flush_and_idle() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .flush_interval(Duration::from_secs(60))
        .idle_timeout(Duration::from_secs(120))
        .enable_journal(false)
        .build();
    let store = Store::open(&dir, config).expect("open store with custom flush/idle");
    store.close().expect("close store");
}

#[test]
fn store_open_small_segments() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .data_segment_size(256 * 1024)
        .index_segment_size(64 * 1024)
        .initial_data_segment_size(4096)
        .initial_index_segment_size(4096)
        .enable_journal(false)
        .build();
    let store = Store::open(&dir, config).expect("open store with small segments");
    store.close().expect("close store");
}

#[test]
fn store_open_high_compression() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .compress_level(9)
        .enable_journal(false)
        .build();
    let store = Store::open(&dir, config).expect("open store with high compression");
    store.close().expect("close store");
}

#[test]
fn store_open_low_compression() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .compress_level(1)
        .enable_journal(false)
        .build();
    let store = Store::open(&dir, config).expect("open store with low compression");
    store.close().expect("close store");
}

#[test]
fn store_open_creates_directory() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir().join("nested").join("path");
    assert!(!dir.exists(), "directory should not exist yet");
    let store = Store::open(&dir, StoreConfig::default()).expect("open store creates dirs");
    assert!(dir.exists(), "directory should have been created");
    store.close().expect("close store");
}

#[test]
fn store_open_reopen_preserves_data() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();

    // First open: create dataset and write data
    {
        let mut store = Store::open(&dir, config.clone()).expect("first open");
        store
            .create_dataset(
                "reopen_ds",
                "data",
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                0,
            )
            .expect("create dataset");
        let ds = store
            .open_dataset("reopen_ds", "data")
            .expect("open dataset");
        ds.write(100, b"persistent_data").expect("write data");
        ds.flush().expect("flush");
        store.close().expect("close first store");
    }

    // Second open: read back data
    {
        let mut store = Store::open(&dir, config).expect("reopen store");
        let ds = store
            .open_dataset("reopen_ds", "data")
            .expect("reopen dataset");
        let result = ds.read(100).expect("read back").expect("data should exist");
        assert_eq!(result.0, 100);
        assert_eq!(result.1, b"persistent_data");
        store.close().expect("close second store");
    }
}

// ============================================================================
// Store::drop_dataset
// ============================================================================

#[test]
fn store_drop_dataset_basic() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "drop_me",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store.open_dataset("drop_me", "data").expect("open dataset");
    ds.write(1, b"will_be_deleted").expect("write data");
    drop(ds);

    store.drop_dataset("drop_me", "data").expect("drop dataset");

    // Verify dataset is gone
    let result = store.open_dataset("drop_me", "data");
    assert!(result.is_err(), "opening dropped dataset should fail");

    // Verify listing doesn't include it
    let names = store.get_dataset_names().expect("list names");
    assert!(
        !names.contains(&"drop_me".to_string()),
        "dropped dataset should not appear in listing"
    );

    store.close().expect("close store");
}

#[test]
fn store_drop_dataset_not_found() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    let result = store.drop_dataset("nonexistent", "data");
    assert!(result.is_err(), "dropping nonexistent dataset should fail");

    store.close().expect("close store");
}

#[test]
fn store_drop_one_of_many_datasets() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset("keep_a", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("create keep_a");
    store
        .create_dataset("drop_b", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("create drop_b");
    store
        .create_dataset("keep_c", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("create keep_c");

    store.drop_dataset("drop_b", "data").expect("drop drop_b");

    let mut names = store.get_dataset_names().expect("list names");
    names.sort();
    assert_eq!(names, vec!["keep_a", "keep_c"]);

    // Verify remaining datasets still work
    let ds_a = store.open_dataset("keep_a", "data").expect("open keep_a");
    ds_a.write(1, b"still_here").expect("write to keep_a");
    let result = ds_a.read(1).expect("read keep_a").expect("data exists");
    assert_eq!(result.1, b"still_here");

    store.close().expect("close store");
}

// ============================================================================
// Store::create_dataset parameter variations
// ============================================================================

#[test]
fn store_create_dataset_with_retention() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "retention_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            1000, // retention window = 1000 timestamp units
        )
        .expect("create dataset with retention");

    let ds = store
        .open_dataset("retention_ds", "data")
        .expect("open dataset");
    ds.write(100, b"early").expect("write early data");
    ds.write(200, b"later").expect("write later data");

    let early = ds
        .read(100)
        .expect("read early")
        .expect("early data exists");
    assert_eq!(early.1, b"early");
    let later = ds
        .read(200)
        .expect("read later")
        .expect("later data exists");
    assert_eq!(later.1, b"later");

    store.close().expect("close store");
}

#[test]
fn store_create_dataset_with_continuous_index() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "continuous_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1, // continuous index mode
            0,
        )
        .expect("create continuous index dataset");

    let ds = store
        .open_dataset("continuous_ds", "data")
        .expect("open continuous dataset");

    // Write sequential timestamps
    for i in 1..=50i64 {
        let data = format!("seq_{}", i).into_bytes();
        ds.write(i, &data).expect("write sequential");
    }

    let entries = ds.query(1, 50).expect("query all").to_vec();
    assert_eq!(entries.len(), 50);
    assert_eq!(entries[0].0, 1);
    assert_eq!(entries[49].0, 50);

    store.close().expect("close store");
}

#[test]
fn store_create_duplicate_dataset_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset("dup_ds", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("first create");

    let result = store.create_dataset("dup_ds", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0);
    assert!(result.is_err(), "duplicate create should fail");

    store.close().expect("close store");
}

// ============================================================================
// Store::list_datasets with multiple datasets
// ============================================================================

#[test]
fn store_list_datasets_multiple_types() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "sensor_a",
            "temperature",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create sensor_a/temp");
    store
        .create_dataset(
            "sensor_a",
            "humidity",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create sensor_a/humidity");
    store
        .create_dataset(
            "sensor_b",
            "temperature",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create sensor_b/temp");

    let types_a = store
        .get_dataset_types("sensor_a")
        .expect("get types for sensor_a");
    assert_eq!(types_a.len(), 2, "sensor_a should have 2 types");

    let types_b = store
        .get_dataset_types("sensor_b")
        .expect("get types for sensor_b");
    assert_eq!(types_b.len(), 1, "sensor_b should have 1 type");

    store.close().expect("close store");
}

#[test]
fn store_list_datasets_after_drop_and_create() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset("alpha", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("create alpha");
    store
        .create_dataset("beta", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("create beta");

    store.drop_dataset("alpha", "data").expect("drop alpha");

    store
        .create_dataset("gamma", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("create gamma");

    let mut names = store.get_dataset_names().expect("list names");
    names.sort();
    assert_eq!(names, vec!["beta", "gamma"]);

    store.close().expect("close store");
}

// ============================================================================
// Store::inspect_dataset
// ============================================================================

#[test]
fn store_inspect_empty_dataset() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "inspect_empty",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let result = store
        .inspect_dataset("inspect_empty", "data")
        .expect("inspect empty dataset");

    assert_eq!(result.info.name, "inspect_empty");
    assert_eq!(result.info.dataset_type, "data");
    assert_eq!(result.info.compress_level, 6);
    assert_eq!(result.info.index_continuous, 0);
    assert_eq!(result.info.retention_window, 0);
    assert!(
        result.state.latest_written_timestamp.is_none(),
        "empty dataset should have no latest timestamp"
    );

    store.close().expect("close store");
}

#[test]
fn store_inspect_dataset_after_writes() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "inspect_written",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("inspect_written", "data")
        .expect("open dataset");
    ds.write(100, b"first").expect("write first");
    ds.write(200, b"second").expect("write second");
    ds.flush().expect("flush");

    let result = store
        .inspect_dataset("inspect_written", "data")
        .expect("inspect written dataset");

    assert_eq!(
        result.state.latest_written_timestamp,
        Some(200),
        "latest timestamp should be 200"
    );
    assert!(
        result.state.total_record_count >= 2,
        "should have at least 2 records"
    );

    store.close().expect("close store");
}

// ============================================================================
// DataSet::write + read roundtrip with various data sizes
// ============================================================================

#[test]
fn write_read_roundtrip_empty_data() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "empty_data",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("empty_data", "data")
        .expect("open dataset");
    ds.write(1, b"").expect("write empty data");

    let result = ds.read(1).expect("read empty").expect("should exist");
    assert_eq!(result.0, 1);
    assert!(result.1.is_empty(), "data should be empty");

    store.close().expect("close store");
}

#[test]
fn write_read_roundtrip_small_data() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

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
        .expect("create dataset");

    let ds = store
        .open_dataset("small_data", "data")
        .expect("open dataset");
    let payload = b"hello";
    ds.write(42, payload).expect("write small data");

    let result = ds.read(42).expect("read small").expect("should exist");
    assert_eq!(result.0, 42);
    assert_eq!(result.1, payload);

    store.close().expect("close store");
}

#[test]
fn write_read_roundtrip_medium_data() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "medium_data",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("medium_data", "data")
        .expect("open dataset");
    let payload = vec![0xABu8; 16 * 1024]; // 16 KiB
    ds.write(1, &payload).expect("write medium data");

    let result = ds.read(1).expect("read medium").expect("should exist");
    assert_eq!(result.0, 1);
    assert_eq!(result.1, payload);

    store.close().expect("close store");
}

#[test]
fn write_read_roundtrip_large_single_record() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "large_data",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("large_data", "data")
        .expect("open dataset");
    let payload = vec![0xCDu8; 128 * 1024]; // 128 KiB (exceeds BLOCK_MAX_SIZE, goes to single-record block)
    ds.write(999, &payload).expect("write large data");

    let result = ds.read(999).expect("read large").expect("should exist");
    assert_eq!(result.0, 999);
    assert_eq!(result.1.len(), 128 * 1024);
    assert!(result.1.iter().all(|&b| b == 0xCD));

    store.close().expect("close store");
}

#[test]
fn write_read_roundtrip_multiple_timestamps() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "multi_ts",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("multi_ts", "data")
        .expect("open dataset");

    // Write 50 records with varying sizes
    for i in 0..50i64 {
        let size = ((i as usize) + 1) * 100;
        let payload = vec![i as u8; size];
        ds.write(i * 10 + 1, &payload).expect("write record");
    }

    // Read back and verify each
    for i in 0..50i64 {
        let expected_size = ((i as usize) + 1) * 100;
        let result = ds
            .read(i * 10 + 1)
            .expect("read record")
            .expect("should exist");
        assert_eq!(result.0, i * 10 + 1);
        assert_eq!(result.1.len(), expected_size);
        assert!(result.1.iter().all(|&b| b == i as u8));
    }

    store.close().expect("close store");
}

#[test]
fn write_read_nonexistent_timestamp_returns_none() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "miss_read",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("miss_read", "data")
        .expect("open dataset");
    ds.write(100, b"exists").expect("write data");

    assert!(
        ds.read(99).expect("read 99").is_none(),
        "ts 99 should not exist"
    );
    assert!(
        ds.read(101).expect("read 101").is_none(),
        "ts 101 should not exist"
    );
    assert!(
        ds.read(1).expect("read 1").is_none(),
        "ts 1 should not exist"
    );

    store.close().expect("close store");
}

// ============================================================================
// DataSet::delete operations
// ============================================================================

#[test]
fn delete_single_record() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "del_single",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("del_single", "data")
        .expect("open dataset");
    ds.write(100, b"to_delete").expect("write data");

    let result = ds.read(100).expect("read").expect("should exist");
    assert_eq!(result.1, b"to_delete");

    ds.delete(100).expect("delete record");

    assert!(
        ds.read(100).expect("read after delete").is_none(),
        "deleted record should return None"
    );

    store.close().expect("close store");
}

#[test]
fn delete_leaves_other_records_intact() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "del_partial",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("del_partial", "data")
        .expect("open dataset");
    ds.write(100, b"keep_a").expect("write 100");
    ds.write(200, b"delete_me").expect("write 200");
    ds.write(300, b"keep_b").expect("write 300");

    ds.delete(200).expect("delete 200");

    let a = ds.read(100).expect("read 100").expect("100 should exist");
    assert_eq!(a.1, b"keep_a");
    assert!(
        ds.read(200).expect("read 200").is_none(),
        "200 should be deleted"
    );
    let b = ds.read(300).expect("read 300").expect("300 should exist");
    assert_eq!(b.1, b"keep_b");

    store.close().expect("close store");
}

#[test]
fn delete_nonexistent_returns_error() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "del_miss",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("del_miss", "data")
        .expect("open dataset");
    ds.write(100, b"exists").expect("write data");

    let result = ds.delete(999);
    assert!(result.is_err(), "deleting nonexistent should fail");

    // Original data still intact
    let r = ds.read(100).expect("read").expect("should exist");
    assert_eq!(r.1, b"exists");

    store.close().expect("close store");
}

// ============================================================================
// DataSet::correction writes
// ============================================================================

#[test]
fn correction_write_same_timestamp_overwrites() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "correction",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("correction", "data")
        .expect("open dataset");
    ds.write(100, b"original").expect("write original");

    let r = ds.read(100).expect("read").expect("should exist");
    assert_eq!(r.1, b"original");

    // Correction: overwrite with same-size data
    ds.write(100, b"FIXED__!").expect("correction same size");

    let r = ds
        .read(100)
        .expect("read after correction")
        .expect("should exist");
    assert_eq!(r.1, b"FIXED__!");

    store.close().expect("close store");
}

#[test]
fn correction_write_resize_larger() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "corr_grow",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("corr_grow", "data")
        .expect("open dataset");
    ds.write(100, b"tiny").expect("write tiny");

    // Correction: replace with much larger data
    let big = vec![0xEFu8; 2000];
    ds.write(100, &big).expect("correction to larger");

    let r = ds.read(100).expect("read larger").expect("should exist");
    assert_eq!(r.1.len(), 2000);
    assert!(r.1.iter().all(|&b| b == 0xEF));

    store.close().expect("close store");
}

#[test]
fn correction_write_resize_smaller() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "corr_shrink",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("corr_shrink", "data")
        .expect("open dataset");
    let big = vec![0xABu8; 5000];
    ds.write(100, &big).expect("write big");

    // Correction: replace with smaller data
    ds.write(100, b"small").expect("correction to smaller");

    let r = ds.read(100).expect("read smaller").expect("should exist");
    assert_eq!(r.1, b"small");

    store.close().expect("close store");
}

#[test]
fn correction_write_persists_after_reopen() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();

    // Write and correct
    {
        let mut store = Store::open(&dir, config.clone()).expect("open store");
        store
            .create_dataset(
                "corr_persist",
                "data",
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                0,
            )
            .expect("create dataset");
        let ds = store
            .open_dataset("corr_persist", "data")
            .expect("open dataset");
        ds.write(100, b"version1").expect("write v1");
        ds.write(100, b"version2").expect("correction to v2");
        ds.flush().expect("flush");
        store.close().expect("close");
    }

    // Reopen and verify correction persisted
    {
        let mut store = Store::open(&dir, config).expect("reopen store");
        let ds = store
            .open_dataset("corr_persist", "data")
            .expect("reopen dataset");
        let r = ds.read(100).expect("read").expect("should exist");
        assert_eq!(r.1, b"version2", "correction should persist");
        store.close().expect("close");
    }
}

// ============================================================================
// DataSet::append operations
// ============================================================================

#[test]
fn append_to_existing_record() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "append_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("append_ds", "data")
        .expect("open dataset");
    ds.write(100, b"hello").expect("write initial");
    ds.append(100, b" world").expect("append");

    let r = ds.read(100).expect("read").expect("should exist");
    assert_eq!(r.1, b"hello world");

    store.close().expect("close store");
}

#[test]
fn append_multiple_times() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "append_multi",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("append_multi", "data")
        .expect("open dataset");
    ds.write(100, b"a").expect("write");
    ds.append(100, b"b").expect("append 1");
    ds.append(100, b"c").expect("append 2");
    ds.append(100, b"d").expect("append 3");

    let r = ds.read(100).expect("read").expect("should exist");
    assert_eq!(r.1, b"abcd");

    store.close().expect("close store");
}

#[test]
fn append_empty_data_is_noop() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "append_empty",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("append_empty", "data")
        .expect("open dataset");
    ds.write(100, b"data").expect("write");
    ds.append(100, b"").expect("append empty");

    let r = ds.read(100).expect("read").expect("should exist");
    assert_eq!(r.1, b"data", "empty append should not change data");

    store.close().expect("close store");
}

#[test]
fn append_non_latest_timestamp_fails() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "append_old",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("append_old", "data")
        .expect("open dataset");
    ds.write(100, b"old").expect("write old");
    ds.write(200, b"new").expect("write new");

    // Appending to 100 (not latest) should fail
    let result = ds.append(100, b"more");
    assert!(
        result.is_err(),
        "append to non-latest timestamp should fail"
    );

    // Original data intact
    let r = ds.read(100).expect("read").expect("should exist");
    assert_eq!(r.1, b"old");

    store.close().expect("close store");
}

// ============================================================================
// DataSet::query with different ranges
// ============================================================================

#[test]
fn query_exact_single_timestamp() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "query_single",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("query_single", "data")
        .expect("open dataset");
    ds.write(50, b"only").expect("write");

    let entries = ds.query(50, 50).expect("query single");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, 50);
    assert_eq!(entries[0].1, b"only");

    store.close().expect("close store");
}

#[test]
fn query_range_with_gaps() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "query_gaps",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("query_gaps", "data")
        .expect("open dataset");
    // Write with gaps: 10, 30, 50, 70, 90
    for i in [10i64, 30, 50, 70, 90] {
        ds.write(i, format!("val_{}", i).as_bytes()).expect("write");
    }

    // Query range that includes gaps: 25..65 → should get 30, 50
    let entries = ds.query(25, 65).expect("query range with gaps");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, 30);
    assert_eq!(entries[1].0, 50);

    store.close().expect("close store");
}

#[test]
fn query_empty_range_returns_empty() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "query_empty",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("query_empty", "data")
        .expect("open dataset");
    ds.write(100, b"data").expect("write");

    // Query range that contains no records
    let entries = ds.query(200, 300).expect("query empty range");
    assert!(entries.is_empty(), "empty range should return no results");

    store.close().expect("close store");
}

#[test]
fn query_backward_range_returns_empty() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "query_back",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("query_back", "data")
        .expect("open dataset");
    ds.write(100, b"data").expect("write");

    // start > end should return empty
    let entries = ds.query(300, 100).expect("query backward");
    assert!(entries.is_empty(), "backward range should return empty");

    store.close().expect("close store");
}

#[test]
fn query_full_range_returns_all() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "query_full",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("query_full", "data")
        .expect("open dataset");
    for i in 1..=20i64 {
        ds.write(i * 100, format!("item_{}", i).as_bytes())
            .expect("write");
    }

    let entries = ds.query(1, i64::MAX).expect("query full range");
    assert_eq!(entries.len(), 20);

    // Verify ordering
    for (i, (ts, _)) in entries.iter().enumerate() {
        assert_eq!(*ts, (i as i64 + 1) * 100);
    }

    store.close().expect("close store");
}

// ============================================================================
// DataSet::query_iterator usage
// ============================================================================

#[test]
fn query_iterator_basic() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "iter_basic",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("iter_basic", "data")
        .expect("open dataset");
    for i in 1..=10i64 {
        ds.write(i, format!("iter_{}", i).as_bytes())
            .expect("write");
    }

    let mut count = 0;
    for entry in ds.query_iter(1, 10).expect("create iterator") {
        let (ts, data) = entry.expect("iterate");
        count += 1;
        assert!((1..=10).contains(&ts));
        assert_eq!(data, format!("iter_{}", ts).as_bytes());
    }
    assert_eq!(count, 10);

    store.close().expect("close store");
}

#[test]
fn query_iterator_empty_range() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "iter_empty",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("iter_empty", "data")
        .expect("open dataset");
    ds.write(100, b"data").expect("write");

    let mut count = 0;
    for _entry in ds.query_iter(200, 300).expect("create iterator") {
        count += 1;
    }
    assert_eq!(count, 0, "empty range iterator should yield nothing");

    store.close().expect("close store");
}

// ============================================================================
// DataSet::read_latest operations
// ============================================================================

#[test]
fn read_latest_returns_most_recent() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "latest_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("latest_ds", "data")
        .expect("open dataset");
    ds.write(100, b"first").expect("write 100");
    ds.write(200, b"second").expect("write 200");
    ds.write(300, b"third").expect("write 300");

    let result = ds
        .read_latest()
        .expect("read latest")
        .expect("should exist");
    assert_eq!(result.0, 300);
    assert_eq!(result.1, b"third");

    store.close().expect("close store");
}

#[test]
fn read_latest_empty_dataset_returns_none() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "latest_empty",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("latest_empty", "data")
        .expect("open dataset");
    let result = ds.read_latest().expect("read latest");
    assert!(
        result.is_none(),
        "empty dataset should return None for read_latest"
    );

    store.close().expect("close store");
}

#[test]
fn read_latest_after_delete_latest_record() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "latest_del",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("latest_del", "data")
        .expect("open dataset");
    ds.write(100, b"first").expect("write 100");
    ds.write(200, b"latest").expect("write 200");

    ds.delete(200).expect("delete latest");

    // read_latest should still resolve to ts=200 (latest_written_timestamp not rolled back),
    // but since the record is deleted, it returns None.
    let result = ds.read_latest().expect("read latest after delete");
    // Per contract: latest_written_timestamp stays at 200, but the record is deleted.
    // read(-1) = read(200) = None because the record is deleted.
    assert!(
        result.is_none(),
        "deleted latest record should return None from read_latest"
    );

    store.close().expect("close store");
}

// ============================================================================
// Store with journal enabled vs disabled
// ============================================================================

#[test]
fn store_with_journal_disabled_no_journal_dataset() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "no_journal",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");
    let ds = store
        .open_dataset("no_journal", "data")
        .expect("open dataset");
    ds.write(1, b"test").expect("write");
    store.close().expect("close store");

    // Verify journal directory doesn't exist
    let journal_dir = dir.join(".journal");
    assert!(
        !journal_dir.exists(),
        "journal directory should not exist when journal is disabled"
    );
}

#[test]
fn store_with_journal_enabled_creates_journal_dir() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(true).build();
    let mut store = Store::open(&dir, config).expect("open store with journal");

    store
        .create_dataset(
            "with_journal",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");
    let ds = store
        .open_dataset("with_journal", "data")
        .expect("open dataset");
    ds.write(1, b"journaled").expect("write");
    store.close().expect("close store");

    // Verify journal directory exists
    let journal_dir = dir.join(".journal");
    assert!(
        journal_dir.exists(),
        "journal directory should exist when journal is enabled"
    );
}

// ============================================================================
// Store with cache enabled vs disabled (cache_max_memory)
// ============================================================================

#[test]
fn store_with_small_cache() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .cache_max_memory(4096) // Very small cache
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).expect("open store with small cache");

    store
        .create_dataset(
            "small_cache",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("small_cache", "data")
        .expect("open dataset");
    // Write large records that exceed cache capacity
    for i in 0..10i64 {
        let payload = vec![i as u8; 4096];
        ds.write(i + 1, &payload).expect("write large record");
    }

    // All records should still be readable even with small cache
    for i in 0..10i64 {
        let result = ds.read(i + 1).expect("read").expect("should exist");
        assert_eq!(result.1.len(), 4096);
        assert!(result.1.iter().all(|&b| b == i as u8));
    }

    store.close().expect("close store");
}

#[test]
fn store_with_zero_cache() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .cache_max_memory(0) // No cache
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).expect("open store with no cache");

    store
        .create_dataset(
            "no_cache",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("no_cache", "data")
        .expect("open dataset");
    ds.write(1, b"no_cache_data").expect("write");

    let result = ds.read(1).expect("read").expect("should exist");
    assert_eq!(result.1, b"no_cache_data");

    store.close().expect("close store");
}

// ============================================================================
// Store with background tasks enabled vs disabled
// ============================================================================

#[test]
fn store_with_background_thread_disabled_manual_tick() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .flush_interval(Duration::from_millis(100))
        .build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "manual_tick",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("manual_tick", "data")
        .expect("open dataset");
    ds.write(1, b"tick_data").expect("write");

    std::thread::sleep(Duration::from_millis(200));
    let result = store.tick_background_tasks().expect("tick");
    assert!(result.executed_tasks > 0, "tick should have executed tasks");

    // Data should still be readable
    let r = ds.read(1).expect("read").expect("should exist");
    assert_eq!(r.1, b"tick_data");

    store.close().expect("close store");
}

#[test]
fn store_next_background_delay_returns_value() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .flush_interval(Duration::from_secs(3600))
        .build();
    let store = Store::open(&dir, config).expect("open store");

    let delay = store.next_background_delay().expect("get delay");
    // Delay should be positive and less than the flush interval
    assert!(delay.as_secs() <= 3600);

    store.close().expect("close store");
}

// ============================================================================
// Combined scenarios
// ============================================================================

#[test]
fn write_delete_write_same_timestamp() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "del_reuse",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("del_reuse", "data")
        .expect("open dataset");

    // Write → delete → write again at same timestamp
    ds.write(100, b"first").expect("write first");
    ds.delete(100).expect("delete");
    assert!(
        ds.read(100).expect("read deleted").is_none(),
        "should be None after delete"
    );

    // Write again at same timestamp
    ds.write(100, b"reborn").expect("write again");
    let r = ds.read(100).expect("read reborn").expect("should exist");
    assert_eq!(r.1, b"reborn");

    store.close().expect("close store");
}

#[test]
fn multiple_datasets_isolated_queries() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset("iso_a", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("create iso_a");
    store
        .create_dataset("iso_b", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("create iso_b");

    let ds_a = store.open_dataset("iso_a", "data").expect("open iso_a");
    let ds_b = store.open_dataset("iso_b", "data").expect("open iso_b");

    // Write same timestamp to both with different data
    ds_a.write(100, b"from_a").expect("write to a");
    ds_b.write(100, b"from_b").expect("write to b");

    // Queries should be isolated
    let entries_a = ds_a.query(100, 100).expect("query a");
    assert_eq!(entries_a.len(), 1);
    assert_eq!(entries_a[0].1, b"from_a");

    let entries_b = ds_b.query(100, 100).expect("query b");
    assert_eq!(entries_b.len(), 1);
    assert_eq!(entries_b[0].1, b"from_b");

    ds_a.delete(100).expect("delete from a");
    assert!(
        ds_a.read(100).expect("read a").is_none(),
        "a should be deleted"
    );
    let r_b = ds_b.read(100).expect("read b").expect("b should exist");
    assert_eq!(r_b.1, b"from_b");

    store.close().expect("close store");
}

#[test]
fn dataset_flush_and_reopen_preserves_state() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();

    // Write data and flush
    {
        let mut store = Store::open(&dir, config.clone()).expect("open store");
        store
            .create_dataset(
                "flush_reopen",
                "data",
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                0,
            )
            .expect("create dataset");
        let ds = store
            .open_dataset("flush_reopen", "data")
            .expect("open dataset");
        for i in 1..=20i64 {
            ds.write(i, format!("flush_{}", i).as_bytes())
                .expect("write");
        }
        ds.flush().expect("flush");
        store.close().expect("close");
    }

    // Reopen and verify
    {
        let mut store = Store::open(&dir, config).expect("reopen store");
        let ds = store
            .open_dataset("flush_reopen", "data")
            .expect("reopen dataset");
        for i in 1..=20i64 {
            let r = ds.read(i).expect("read").expect("should exist");
            assert_eq!(r.1, format!("flush_{}", i).as_bytes());
        }
        let entries = ds.query(1, 20).expect("query all");
        assert_eq!(entries.len(), 20);
        store.close().expect("close");
    }
}

#[test]
fn store_open_by_identifier() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    let ds = store
        .create_dataset("by_id", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .expect("create dataset");
    let identifier = ds.identifier();
    ds.write(100, b"by_id_data").expect("write");
    drop(ds);

    // Open by identifier
    let ds2 = store
        .open_dataset_by_identifier(identifier)
        .expect("open by identifier");
    let r = ds2.read(100).expect("read").expect("should exist");
    assert_eq!(r.1, b"by_id_data");

    store.close().expect("close store");
}

#[test]
fn read_negative_one_is_exact_timestamp() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "neg_one",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store.open_dataset("neg_one", "data").expect("open dataset");
    ds.write(-1, b"minus-one").expect("write -1");
    ds.write(100, b"first").expect("write 100");
    ds.write(200, b"latest").expect("write 200");

    // read(-1) reads the exact signed business timestamp.
    let result = ds.read(-1).expect("read -1").expect("should exist");
    assert_eq!(result.0, -1);
    assert_eq!(result.1, b"minus-one");

    store.close().expect("close store");
}

#[test]
fn query_after_write_and_delete_partial() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).expect("open store");

    store
        .create_dataset(
            "partial_del",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .expect("create dataset");

    let ds = store
        .open_dataset("partial_del", "data")
        .expect("open dataset");
    for i in 1..=10i64 {
        ds.write(i, format!("item_{}", i).as_bytes())
            .expect("write");
    }

    for i in [2i64, 4, 6, 8, 10] {
        ds.delete(i).expect("delete even");
    }

    // Query should return only odd-numbered records
    let entries = ds.query(1, 10).expect("query after partial delete");
    assert_eq!(entries.len(), 5, "should have 5 odd records");
    for (ts, _) in &entries {
        assert!(ts % 2 == 1, "only odd timestamps should remain");
    }

    store.close().expect("close store");
}
