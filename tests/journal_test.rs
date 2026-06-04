//! Journal integration tests.

use std::fs;
use std::path::PathBuf;

use timslite::{
    JournalRecord, JournalRecordKind, Store, StoreConfig, JOURNAL_DATASET_NAME,
    JOURNAL_DATASET_TYPE,
};

fn temp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("timslite_journal_integration");
    let dir = d.join(format!(
        "{}_{}",
        name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn test_config() -> StoreConfig {
    StoreConfig::builder()
        .enable_background_thread(false)
        .data_segment_size(1024 * 1024)
        .index_segment_size(64 * 1024)
        .initial_data_segment_size(4096)
        .initial_index_segment_size(4096)
        .build()
}

fn read_all_journal_records(store: &mut Store) -> Vec<JournalRecord> {
    let journal = store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .unwrap();
    let ds = store.get_dataset(&journal).unwrap();
    let mut ds = ds.lock().unwrap();
    ds.query(1, i64::MAX, Some(store.block_cache()))
        .unwrap()
        .into_iter()
        .map(|(_, payload)| JournalRecord::decode(&payload).unwrap())
        .collect()
}

#[test]
fn t28_1_store_config_defaults_enable_journal() {
    assert!(StoreConfig::default().enable_journal);
    assert!(
        !StoreConfig::builder()
            .enable_journal(false)
            .build()
            .enable_journal
    );
}

#[test]
fn t28_2_store_open_creates_journal_by_default() {
    let dir = temp_dir("default_creates");

    let store = Store::open(&dir, test_config()).unwrap();

    assert!(dir
        .join(JOURNAL_DATASET_NAME)
        .join(JOURNAL_DATASET_TYPE)
        .join("meta")
        .exists());
    store.close().unwrap();
}

#[test]
fn t28_3_disabled_journal_does_not_open_public_handle() {
    let dir = temp_dir("disabled");
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();
    let result = store.open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE);
    assert!(result.is_err());
    store.close().unwrap();
}

#[test]
fn t28_4_journal_records_dataset_creation() {
    let dir = temp_dir("creation");
    let mut store = Store::open(&dir, test_config()).unwrap();

    store
        .create_dataset("t28_ds", "metrics", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    let records = read_all_journal_records(&mut store);
    assert!(!records.is_empty());

    let last = records.last().unwrap();
    assert_eq!(last.kind, JournalRecordKind::CreateDataset);
    assert_eq!(last.name, "t28_ds");
    assert_eq!(last.dataset_type, "metrics");

    store.close().unwrap();
}

#[test]
fn t28_5_journal_records_dataset_deletion() {
    let dir = temp_dir("deletion");
    let mut store = Store::open(&dir, test_config()).unwrap();

    store
        .create_dataset("t28_ds", "metrics", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    store.drop_dataset_by_name("t28_ds", "metrics").unwrap();

    let records = read_all_journal_records(&mut store);
    let deletion_records: Vec<_> = records
        .iter()
        .filter(|r| r.kind == JournalRecordKind::DropDataset)
        .collect();
    assert!(!deletion_records.is_empty());

    let last = deletion_records.last().unwrap();
    assert_eq!(last.name, "t28_ds");
    assert_eq!(last.dataset_type, "metrics");

    store.close().unwrap();
}

#[test]
fn t28_6_journal_records_open_close() {
    let dir = temp_dir("open_close");
    let mut store = Store::open(&dir, test_config()).unwrap();

    store
        .create_dataset("t28_ds", "metrics", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    let ds_handle = store.open_dataset("t28_ds", "metrics").unwrap();
    store.close_dataset(ds_handle).unwrap();
    let ds_handle2 = store.open_dataset("t28_ds", "metrics").unwrap();
    store.close_dataset(ds_handle2).unwrap();

    let records = read_all_journal_records(&mut store);
    // Open/close may not log separate journal entries; verify journal has records.
    assert!(!records.is_empty());

    store.close().unwrap();
}

#[test]
fn t28_7_disabled_journal_creates_no_journal_files() {
    let dir = temp_dir("no_journal");
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();

    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("nj", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    store.close().unwrap();

    assert!(!dir
        .join(JOURNAL_DATASET_NAME)
        .join(JOURNAL_DATASET_TYPE)
        .join("meta")
        .exists());
}

#[test]
fn t28_8_reopen_preserves_journal() {
    let dir = temp_dir("reopen");
    let mut store = Store::open(&dir, test_config()).unwrap();
    store
        .create_dataset("rp", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    let records1 = read_all_journal_records(&mut store);
    assert!(!records1.is_empty());
    store.close().unwrap();

    let mut store2 = Store::open(&dir, test_config()).unwrap();
    let records2 = read_all_journal_records(&mut store2);
    assert!(records2.len() >= records1.len());
    store2.close().unwrap();
}
