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
    store.write_dataset(handle, 1, b"before close").unwrap();

    let dataset = store.get_dataset(&handle).unwrap();
    dataset.close().unwrap();

    assert!(
        store.read_dataset(handle, 1).is_err(),
        "old Store handle should be invalid after direct DataSet::close"
    );
    assert!(
        dataset.write(2, b"after close").is_err(),
        "closed DataSet object should reject further writes"
    );

    let reopened = store.open_dataset("direct_close", "data").unwrap();
    let row = store.read_dataset(reopened, 1).unwrap().unwrap();
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
    let arc = store.get_dataset(&ds_handle).unwrap();
    arc.lock().unwrap().write(100, b"test").unwrap();
    store.close_dataset(ds_handle).unwrap();

    // Drop the dataset
    store.drop_dataset_by_name("drop_test", "data").unwrap();

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
    let arc = store.get_dataset(&ds).unwrap();
    arc.lock().unwrap().write(1, b"first").unwrap();
    store.close().unwrap();

    // Re-open store, drop, recreate
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store.drop_dataset_by_name("recreate", "data").unwrap();

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
    let arc = store.get_dataset(&ds).unwrap();
    let entries = arc.lock().unwrap().query(0, 10).unwrap();
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
    store.close_dataset(valid).unwrap();
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

    let invalid_drop = store.drop_dataset_by_name("..", "data");
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

    // Space in name → rejected
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

    // Backslash → rejected
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

    // Unicode characters → rejected (non-ASCII)
    let r = store.create_dataset("数据集", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0);
    assert!(
        matches!(r, Err(TmslError::InvalidData(_))),
        "unicode in name should be rejected"
    );

    // Exactly 255-byte name → accepted (boundary)
    let name_255 = "a".repeat(255);
    let r = store.create_dataset(&name_255, "data", 1024 * 1024, 64 * 1024, 6, 0, 0);
    assert!(
        r.is_ok(),
        "255-byte name should be accepted, got: {:?}",
        r.err()
    );

    // 256-byte name → rejected (over limit)
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
        let arc = store.get_dataset(&_h1).unwrap();
        arc.lock().unwrap().write(1, b"data1").unwrap();
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
    let arc = store2.get_dataset(&h).unwrap();
    let entries = arc.lock().unwrap().query(1, 1).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, b"data1");
    store2.close().unwrap();
}
