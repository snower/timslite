//! Dataset lifecycle tests: create/open/drop error handling and validation.
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
fn t8_2_1_create_returns_error_if_exists() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "dup_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    // Second create of same dataset should fail
    let result = store.create_dataset(
        "dup_test",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
    );
    assert!(result.is_err());
    if let Err(err) = result {
        assert!(err.to_string().contains("already exists"));
    }

    store.close().unwrap();
}

#[test]
fn t8_2_2_open_returns_error_if_not_exists() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    // Open non-existent dataset
    let result = store.open_dataset("no_such", "data");
    assert!(result.is_err());
    if let Err(err) = result {
        assert!(err.to_string().contains("not found"));
    }

    store.close().unwrap();
}

#[test]
fn direct_dataset_close_removes_store_entry_and_invalidates_handle() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let handle = store
        .create_dataset("direct_close", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    handle.write(1, b"before close").unwrap();

    let dataset = handle.clone();
    dataset.close().unwrap();

    assert!(
        handle.read(1).is_err(),
        "old Store handle should be invalid after direct DataSet::close"
    );
    assert!(
        dataset.write(2, b"after close").is_err(),
        "closed DataSet object should reject further writes"
    );

    let reopened = store.open_dataset("direct_close", "data").unwrap();
    let row = reopened.read(1).unwrap().unwrap();
    assert_eq!(row.1, b"before close");

    store.close().unwrap();
}

#[test]
fn t8_2_3_drop_deletes_dataset() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "drop_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds_handle = store.open_dataset("drop_test", "data").unwrap();

    // Write some data
    let arc = ds_handle.clone();
    arc.write(100, b"test").unwrap();
    ds_handle.close().unwrap();

    // Drop the dataset
    store.drop_dataset("drop_test", "data").unwrap();

    // Verify directory is gone
    let dataset_dir = dir.join("drop_test").join("data");
    assert!(!dataset_dir.exists());

    store.close().unwrap();
}

#[test]
fn t8_2_4_create_after_drop() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "recreate",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    let ds = store.open_dataset("recreate", "data").unwrap();
    let arc = ds.clone();
    arc.write(1, b"first").unwrap();
    store.close().unwrap();

    // Re-open store, drop, recreate
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store.drop_dataset("recreate", "data").unwrap();

    // Now create should succeed (different params are fine since old data is gone)
    store
        .create_dataset(
            "recreate",
            "data",
            32 * 1024 * 1024,
            2 * 1024 * 1024,
            9,
            0,
            0,
        )
        .unwrap();
    let ds = store.open_dataset("recreate", "data").unwrap();

    // Data from first creation should be gone
    let arc = ds.clone();
    let entries = arc.query(0, 10).unwrap();
    assert_eq!(entries.len(), 0);

    store.close().unwrap();
}

#[test]
fn t8_2_5_dataset_name_type_validation() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    let valid = store
        .create_dataset(
            "AZaz09-_",
            "type_09-OK",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();
    valid.close().unwrap();
    assert!(dir
        .join("AZaz09-_")
        .join("type_09-OK")
        .join("meta")
        .exists());

    let invalid_create = store.create_dataset(
        "bad/name",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
    );
    assert!(matches!(invalid_create, Err(TmslError::InvalidData(_))));

    let invalid_create = store.create_dataset(
        "bad.name",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
    );
    assert!(matches!(invalid_create, Err(TmslError::InvalidData(_))));

    let invalid_create =
        store.create_dataset("", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0);
    assert!(matches!(invalid_create, Err(TmslError::InvalidData(_))));

    let invalid_open = store.open_dataset("AZaz09-_", "bad type");
    assert!(matches!(invalid_open, Err(TmslError::InvalidData(_))));

    let invalid_drop = store.drop_dataset("..", "data");
    assert!(matches!(invalid_drop, Err(TmslError::InvalidData(_))));
}

#[test]
fn t8_2_6_dataset_name_type_length_must_fit_journal_tlv_policy() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let long_name = "a".repeat(256);

    let err = match store.create_dataset(&long_name, "metrics", 1024 * 1024, 64 * 1024, 6, 0, 0) {
        Ok(_) => panic!("dataset name longer than journal text limit must fail"),
        Err(err) => err,
    };

    assert!(err.to_string().contains("at most 255 bytes"));
    assert!(!dir.join(&long_name).exists());
}

#[test]
fn t8_2_7_dataset_name_edge_cases_unicode_space_backslash_boundary() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    // Space in name 閳?rejected
    let r = store.create_dataset(
        "my dataset",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
    );
    assert!(
        matches!(r, Err(TmslError::InvalidData(_))),
        "space in name should be rejected"
    );

    // Backslash 閳?rejected
    let r = store.create_dataset(
        "my\\dataset",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
    );
    assert!(
        matches!(r, Err(TmslError::InvalidData(_))),
        "backslash in name should be rejected"
    );

    // Unicode characters 閳?rejected (non-ASCII)
    let r = store.create_dataset(
        "\u{6570}\u{636e}\u{96c6}",
        "data",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        0,
    );
    assert!(
        matches!(r, Err(TmslError::InvalidData(_))),
        "unicode in name should be rejected"
    );

    // Exactly 255-byte name 閳?accepted (boundary)
    let name_255 = "a".repeat(255);
    let r = store.create_dataset(&name_255, "data", 1024 * 1024, 64 * 1024, 6, 0, 0);
    assert!(
        r.is_ok(),
        "255-byte name should be accepted, got: {:?}",
        r.err()
    );

    // 256-byte name 閳?rejected (over limit)
    let name_256 = "b".repeat(256);
    let r = store.create_dataset(&name_256, "data", 1024 * 1024, 64 * 1024, 6, 0, 0);
    assert!(r.is_err(), "256-byte name should be rejected");
}

#[test]
fn t8_2_8_store_open_on_corrupted_directory() {
    // Store::open() on a directory with corrupted meta file should fail gracefully
    use std::io::Write;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    // Create a dataset, then corrupt its meta file
    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        store
            .create_dataset("corrupt_ds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
            .unwrap();
        store.close().unwrap();
    }

    // Corrupt the meta file by overwriting with garbage
    let meta_path = dir.join("corrupt_ds").join("data").join("meta");
    assert!(meta_path.exists(), "meta file should exist");
    {
        let mut f = fs::OpenOptions::new().write(true).open(&meta_path).unwrap();
        f.write_all(&[0xFF; 100]).unwrap();
    }

    // Reopening the store should still work (store-level open is lazy)
    // but opening the corrupted dataset should fail
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let result = store.open_dataset("corrupt_ds", "data");
    assert!(
        result.is_err(),
        "opening dataset with corrupted meta should fail"
    );
    store.close().unwrap();
}

#[test]
fn t8_2_9_store_open_on_truncated_meta_file() {
    // DataSet::open() on a truncated (too-short) meta file should fail
    use std::io::Write;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    // Create dataset directory structure manually with truncated meta
    let ds_dir = dir.join("trunc_ds").join("data");
    fs::create_dir_all(&ds_dir).unwrap();
    fs::create_dir_all(ds_dir.join("..").join("..").join("trunc_ds")).unwrap();

    let meta_path = dir.join("trunc_ds").join("data").join("meta");
    {
        let mut f = fs::File::create(&meta_path).unwrap();
        // Write only 3 bytes (far too short for any valid meta)
        f.write_all(&[0x54, 0x4D, 0x53]).unwrap(); // partial "TMS"
    }

    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let result = store.open_dataset("trunc_ds", "data");
    assert!(
        result.is_err(),
        "opening dataset with truncated meta should fail"
    );
    store.close().unwrap();
}

#[test]
fn t8_2_10_store_close_with_open_dataset_handles() {
    // Store::close() should gracefully close all datasets even if handles exist
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds1", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let _h1 = store.open_dataset("ds1", "data").unwrap();

    // Write some data
    {
        let arc = _h1.clone();
        arc.write(1, b"data1").unwrap();
    }

    // Close store without explicitly closing dataset handles
    // Store::close() should flush and close all datasets
    let result = store.close();
    assert!(
        result.is_ok(),
        "Store::close() with open handles should succeed, got: {:?}",
        result.err()
    );

    // Verify data persisted
    let mut store2 = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store2.open_dataset("ds1", "data").unwrap();
    let arc = h.clone();
    let entries = arc.query(1, 1).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, b"data1");
    store2.close().unwrap();
}

// ─── Segment lifecycle integration tests ──────────────────────────────────────

/// Write data, trigger idle-close via background tick, then read back — verifying
/// that ensure_open transparently re-mmaps and reopens the segment.
#[test]
fn test_segment_idle_close_and_reopen() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .idle_timeout(Duration::from_millis(10))
        .flush_interval(Duration::from_millis(5))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    let handle = store
        .create_dataset("idle_close_test", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    // Write several records
    for ts in 1..=5 {
        let payload = format!("record_{ts}").into_bytes();
        store.write_dataset(handle, ts, &payload).unwrap();
    }

    // Let idle timeout elapse, then tick to trigger idle-close
    std::thread::sleep(Duration::from_millis(15));
    let result = store.tick_background_tasks().unwrap();
    // At minimum retention reclaim runs; idle_check may also be due
    assert!(result.executed_tasks > 0);

    // After idle-close, reading should transparently reopen
    for ts in 1..=5 {
        let row = store.read_dataset(handle, ts).unwrap().unwrap();
        assert_eq!(row.1, format!("record_{ts}").as_bytes());
    }

    store.close().unwrap();
}

/// Write enough data to overflow the initial small segment, triggering segment
/// expansion via ensure_data_capacity → expand().
#[test]
fn test_segment_expansion_on_overflow() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    // Small initial segment: 8 KiB data segment with max expansion to 256 KiB
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    let handle = store
        .create_dataset(
            "expand_test",
            "data",
            256 * 1024, // max data segment
            64 * 1024,  // index segment
            6,
            0,
            0,
        )
        .unwrap();

    // Write records that together exceed 8 KiB (≈80 records @ 100 bytes each)
    let payload = vec![b'X'; 100];
    for ts in 0..100i64 {
        store.write_dataset(handle, ts, &payload).unwrap();
    }

    // Read back every record — segment expansion should have happened transparently
    for ts in 0..100i64 {
        let row = store.read_dataset(handle, ts).unwrap().unwrap();
        assert_eq!(row.0, ts);
        assert_eq!(row.1.len(), 100);
    }

    store.close().unwrap();
}

/// Verify flush runs on tick, writing mmap contents to disk so data survives
/// a store close/reopen cycle.
#[test]
fn test_segment_sync_on_flush() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .flush_interval(Duration::from_millis(5))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    let handle = store
        .create_dataset("flush_sync_test", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    // Write data that will be flushed by tick
    store.write_dataset(handle, 10, b"pre-flush").unwrap();
    store.write_dataset(handle, 20, b"also-pre-flush").unwrap();

    // Wait for flush interval then trigger flush via tick
    std::thread::sleep(Duration::from_millis(10));
    let result = store.tick_background_tasks().unwrap();
    assert!(result.executed_tasks > 0, "expect at least flush to run");

    // Close and reopen to verify data persisted
    store.close().unwrap();

    let mut store2 = Store::open(&dir, StoreConfig::default()).unwrap();
    let h2 = store2.open_dataset("flush_sync_test", "data").unwrap();

    let row10 = store2.read_dataset(h2, 10).unwrap().unwrap();
    assert_eq!(row10.1, b"pre-flush");

    let row20 = store2.read_dataset(h2, 20).unwrap().unwrap();
    assert_eq!(row20.1, b"also-pre-flush");

    store2.close().unwrap();
}

/// Multiple datasets concurrently opened, written, closed, and reopened — verifying
/// segment lifecycle across datasets.
#[test]
fn test_multiple_segments_lifecycle() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    let ds_a = store
        .create_dataset("mseg_a", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let ds_b = store
        .create_dataset("mseg_b", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    // Write interleaved timestamps
    store.write_dataset(ds_a, 1, b"a1").unwrap();
    store.write_dataset(ds_b, 1, b"b1").unwrap();
    store.write_dataset(ds_a, 2, b"a2").unwrap();
    store.write_dataset(ds_b, 2, b"b2").unwrap();

    // Close everything and reopen
    store.close().unwrap();

    let mut store2 = Store::open(&dir, StoreConfig::default()).unwrap();
    let da = store2.open_dataset("mseg_a", "data").unwrap();
    let db = store2.open_dataset("mseg_b", "data").unwrap();

    assert_eq!(store2.read_dataset(da, 1).unwrap().unwrap().1, b"a1");
    assert_eq!(store2.read_dataset(da, 2).unwrap().unwrap().1, b"a2");
    assert_eq!(store2.read_dataset(db, 1).unwrap().unwrap().1, b"b1");
    assert_eq!(store2.read_dataset(db, 2).unwrap().unwrap().1, b"b2");

    store2.close().unwrap();
}

/// Tick background tasks manually (with background thread disabled).
/// Exercises flush, idle-close, cache eviction, and retention reclaim paths.
#[test]
fn test_store_background_tick() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .flush_interval(Duration::from_millis(5))
        .idle_timeout(Duration::from_millis(10))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    // Tick on empty store should be harmless
    let result = store.tick_background_tasks().unwrap();
    assert!(result.next_delay >= Duration::ZERO);

    // Create dataset and write data
    let handle = store
        .create_dataset("bg_tick_test", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    store.write_dataset(handle, 1, b"bg-tick-data").unwrap();

    // Let flush interval elapse, then tick
    std::thread::sleep(Duration::from_millis(10));
    let result = store.tick_background_tasks().unwrap();
    assert!(result.executed_tasks > 0);
    assert!(result.next_delay >= Duration::ZERO);

    // Data should still be readable after tick
    let row = store.read_dataset(handle, 1).unwrap().unwrap();
    assert_eq!(row.1, b"bg-tick-data");

    store.close().unwrap();
}

/// Full dataset close and reopen cycle: write → close_dataset → open_dataset → read.
#[test]
fn test_dataset_close_and_reopen() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    let handle = store
        .create_dataset("close_reopen", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    // Write diverse records
    for ts in [10, 20, 50, 100] {
        let payload = format!("ts_{ts}").into_bytes();
        store.write_dataset(handle, ts, &payload).unwrap();
    }

    // Close the dataset
    store.close_dataset(handle).unwrap();

    // Reopen and verify all records survived
    let h2 = store.open_dataset("close_reopen", "data").unwrap();
    for ts in [10, 20, 50, 100] {
        let row = store.read_dataset(h2, ts).unwrap().unwrap();
        assert_eq!(row.1, format!("ts_{ts}").as_bytes());
    }

    store.close().unwrap();
}
