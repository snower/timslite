use std::time::{SystemTime, UNIX_EPOCH};

use timslite::{DataSetConfigBuilder, Store, StoreConfig};

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
        let config = StoreConfig::default();
        let mut writer = Store::open(&dir, config.clone()).unwrap();
        let handle = writer
            .create_dataset_with_config(
                "metrics",
                "raw",
                Some(DataSetConfigBuilder::from_store(&config).enable_journal(true)),
            )
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

#[test]
fn test_store_open_and_close() {
    let dir = temp_dir("open_and_close");
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(!store.is_read_only());
    let handle = store
        .create_dataset("metrics", "raw", 262144, 4096, 0, 0, 0)
        .unwrap();
    handle.write(1, b"value_one").unwrap();
    assert_eq!(handle.read(1).unwrap(), Some((1, b"value_one".to_vec())));
    store.close().unwrap();
}

#[test]
fn test_store_multiple_open_close() {
    let dir = temp_dir("multiple_open_close");
    for i in 0..3 {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        assert!(!store.is_read_only());

        if i == 0 {
            store
                .create_dataset("metrics", "raw", 262144, 4096, 0, 0, 0)
                .unwrap();
        }

        let handle = store.open_dataset("metrics", "raw").unwrap();
        handle.write(i as i64 * 100 + 1, b"data").unwrap();
        handle.flush().unwrap();

        let read_handle = store.open_dataset("metrics", "raw").unwrap();
        assert_eq!(
            read_handle.read(i as i64 * 100 + 1).unwrap(),
            Some((i as i64 * 100 + 1, b"data".to_vec()))
        );
        store.close().unwrap();
    }
}

#[test]
fn test_store_background_tick_with_pending() {
    let dir = temp_dir("tick_with_pending");
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();
    assert!(!store.is_read_only());

    let handle = store
        .create_dataset("metrics", "raw", 262144, 4096, 0, 0, 0)
        .unwrap();
    handle.write(1, b"tick_data").unwrap();

    store.tick_background_tasks().unwrap();
    store.next_background_delay().unwrap();

    store.close().unwrap();
}

#[test]
fn test_store_read_only_rejects_background_tick() {
    let dir = temp_dir("readonly_no_tick");
    let mut writer = Store::open(&dir, StoreConfig::default()).unwrap();
    writer
        .create_dataset("metrics", "raw", 262144, 4096, 0, 0, 0)
        .unwrap();
    writer.close().unwrap();

    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(!store.is_read_only());
    let handle = store.open_dataset("metrics", "raw").unwrap();
    handle.write(1, b"lock_holder").unwrap();
    handle.flush().unwrap();

    let reader = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(reader.is_read_only());

    assert!(reader.tick_background_tasks().is_err());
    assert!(reader.next_background_delay().is_err());
}

#[test]
fn test_store_read_only_rejects_queue_operations() {
    let dir = temp_dir("readonly_no_queue");
    let mut writer = Store::open(&dir, StoreConfig::default()).unwrap();
    let handle = writer
        .create_dataset("metrics", "raw", 262144, 4096, 0, 0, 0)
        .unwrap();
    handle.write(1, b"queue_data").unwrap();
    handle.flush().unwrap();

    let mut reader = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(reader.is_read_only());
    let read_handle = reader.open_dataset("metrics", "raw").unwrap();

    assert!(read_handle.open_queue().is_err());
    assert!(reader.open_journal_queue().is_err());
}

#[test]
fn test_store_concurrent_readers() {
    let dir = temp_dir("concurrent_readers");
    let mut writer = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(!writer.is_read_only());
    let handle = writer
        .create_dataset("metrics", "raw", 262144, 4096, 0, 0, 0)
        .unwrap();
    handle.write(10, b"record_ten").unwrap();
    handle.write(20, b"record_twenty").unwrap();
    handle.flush().unwrap();

    let mut reader1 = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(reader1.is_read_only());
    let r1_handle = reader1.open_dataset("metrics", "raw").unwrap();
    assert_eq!(
        r1_handle.read(10).unwrap(),
        Some((10, b"record_ten".to_vec()))
    );
    assert_eq!(
        r1_handle.read(20).unwrap(),
        Some((20, b"record_twenty".to_vec()))
    );

    let mut reader2 = Store::open(&dir, StoreConfig::default()).unwrap();
    assert!(reader2.is_read_only());
    let r2_handle = reader2.open_dataset("metrics", "raw").unwrap();
    assert_eq!(
        r2_handle.read(10).unwrap(),
        Some((10, b"record_ten".to_vec()))
    );
    assert_eq!(
        r2_handle.read(20).unwrap(),
        Some((20, b"record_twenty".to_vec()))
    );
}
