use std::time::{SystemTime, UNIX_EPOCH};

use timslite::{Store, StoreConfig};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "timslite_store_readonly_lock_{name}_{:?}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn auto_read_only_opens_when_writer_holds_lock_and_rejects_mutations() {
    let dir = temp_dir("auto_conflict");
    let mut writer = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(!writer.is_read_only());
    let handle = writer
        .create_dataset("metrics", "raw", 262144, 4096, 0, 0, 0)
        .unwrap();
    handle.write(1, b"one").unwrap();
    handle.clone().flush().unwrap();

    let mut reader = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(reader.is_read_only());
    let read_handle = reader.open_dataset("metrics", "raw").unwrap();
    assert_eq!(read_handle.read(1).unwrap(), Some((1, b"one".to_vec())));
    assert!(read_handle.write(2, b"two").is_err());
    assert!(reader
        .create_dataset("other", "raw", 262144, 4096, 0, 0, 0)
        .is_err());
    assert!(read_handle.open_queue().is_err());
    let writer_queue = handle.open_queue().unwrap();
    writer_queue.open_consumer("writer").unwrap();
    assert!(reader.tick_background_tasks().is_err());
    assert!(reader.next_background_delay().is_err());
}

#[test]
fn forced_writable_fails_when_writer_holds_lock() {
    let dir = temp_dir("forced_writable_conflict");
    let _writer = Store::open(&dir, StoreConfig::default()).unwrap();
    let config = StoreConfig::builder().read_only(Some(false)).build();

    assert!(Store::open(&dir, config).is_err());
}

#[test]
fn forced_read_only_ignores_writer_lock() {
    let dir = temp_dir("forced_readonly_conflict");
    let _writer = Store::open(&dir, StoreConfig::default()).unwrap();
    let config = StoreConfig::builder().read_only(Some(true)).build();

    let reader = Store::open(&dir, config).unwrap();
    assert!(reader.is_read_only());
}

#[test]
fn stale_lock_file_without_os_lock_does_not_force_read_only() {
    let dir = temp_dir("stale_lock_file");
    std::fs::write(dir.join(".lock"), b"stale").unwrap();

    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(!store.is_read_only());
    store
        .create_dataset("metrics", "raw", 262144, 4096, 0, 0, 0)
        .unwrap();
}

#[test]
fn read_only_journal_reads_existing_records_and_missing_journal_is_empty() {
    let dir = temp_dir("journal_existing");
    {
        let mut writer = Store::open(&dir, StoreConfig::default()).unwrap();
        let handle = writer
            .create_dataset("metrics", "raw", 262144, 4096, 0, 0, 0)
            .unwrap();
        handle.write(1, b"one").unwrap();
        writer.close().unwrap();
    }

    let reader_config = StoreConfig::builder().read_only(Some(true)).build();
    let reader = Store::open(&dir, reader_config).unwrap();
    assert!(reader.is_read_only());
    let latest = reader.journal_latest_sequence().unwrap();
    assert!(latest.is_some());
    let rows = reader.journal_query(1, latest.unwrap()).unwrap();
    assert!(!rows.is_empty());

    let missing_dir = temp_dir("journal_missing");
    let missing_config = StoreConfig::builder().read_only(Some(true)).build();
    let missing = Store::open(&missing_dir, missing_config).unwrap();
    assert_eq!(missing.journal_latest_sequence().unwrap(), None);
    assert!(missing.journal_query(1, 10).unwrap().is_empty());
    assert!(!missing_dir.join(".journal").exists());
}
