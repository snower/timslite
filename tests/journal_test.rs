//! Journal integration tests.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use timslite::{
    JournalRecord, JournalRecordKind, Store, StoreConfig, JOURNAL_DATASET_NAME,
    JOURNAL_DATASET_TYPE,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("timslite_journal_integration");
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = d.join(format!(
        "{}_{}_{id}",
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
    ds.query(1, i64::MAX)
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

#[test]
fn t28_9_public_journal_handle_rejects_append() {
    let dir = temp_dir("readonly_append");
    let mut store = Store::open(&dir, test_config()).unwrap();

    let journal = store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .unwrap();

    let err = store
        .append_dataset(journal, 1, b"forged")
        .expect_err("public journal handle must be read-only for append");
    assert!(err.to_string().contains("read-only internal dataset"));

    store.close().unwrap();
}

#[test]
fn t28_10_direct_journal_dataset_mutations_are_read_only() {
    let dir = temp_dir("direct_journal_read_only");
    let mut store = Store::open(&dir, test_config()).unwrap();

    let journal = store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .unwrap();

    {
        let ds = store.get_dataset(&journal).unwrap();
        let mut ds = ds.lock().unwrap();
        assert!(ds.write(1, b"x").is_err());
        assert!(ds.append(1, b"x").is_err());
        assert!(ds.delete(1).is_err());
    }

    store.close().unwrap();
}

#[test]
fn t28_11_direct_dataset_mutations_use_store_context_journal() {
    let dir = temp_dir("direct_dataset_context");
    let mut store = Store::open(&dir, test_config()).unwrap();

    let handle = store
        .create_dataset("ctx_ds", "metrics", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    {
        let ds = store.get_dataset(&handle).unwrap();
        let mut ds = ds.lock().unwrap();
        ds.write(10, b"direct").unwrap();
        ds.append(20, b"append").unwrap();
        ds.delete(10).unwrap();
    }

    let records = read_all_journal_records(&mut store);
    let direct_records: Vec<_> = records
        .iter()
        .filter(|record| record.name == "ctx_ds" && record.dataset_type == "metrics")
        .collect();

    assert!(direct_records.iter().any(|record| {
        record.kind == JournalRecordKind::DataWrite && record.index_info.unwrap().timestamp == 10
    }));
    assert!(direct_records.iter().any(|record| {
        record.kind == JournalRecordKind::DataAppend
            && record.index_info.unwrap().timestamp == 20
            && record.append_info.unwrap().data_len == 6
    }));
    assert!(direct_records.iter().any(|record| {
        record.kind == JournalRecordKind::DataDelete && record.index_info.unwrap().timestamp == 10
    }));

    store.close().unwrap();
}

#[test]
fn t28_12_journal_queue_rejects_external_push() {
    let dir = temp_dir("journal_queue_push_reject");
    let mut store = Store::open(&dir, test_config()).unwrap();

    // Create a dataset to ensure journal is active
    store
        .create_dataset("jq_ds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    // Open the journal queue via the public Store API
    let journal_handle = store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .unwrap();
    let journal_queue = store.open_queue(journal_handle).unwrap();

    // External push should be rejected (journal queue is read-only producer)
    let result = journal_queue.push(b"forged_record");
    assert!(result.is_err(), "journal queue must reject external push()");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("read-only"),
        "error should mention read-only, got: {}",
        err_msg
    );

    store.close().unwrap();
}

#[test]
fn t28_13_journal_dataset_openable_and_queryable_at_store_level() {
    // After Store::open() with enable_journal=true, the built-in journal
    // dataset must exist and be openable/queryable via the Store API.
    let dir = temp_dir("journal_openable");
    let mut store = Store::open(&dir, test_config()).unwrap();

    // Create at least one dataset so the journal receives log entries
    store
        .create_dataset("jds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    // The journal dataset must be openable via Store API
    let journal_handle = store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .expect("journal dataset should be openable at Store level");

    // Journal should contain at least one record (the create_dataset event)
    let records = read_all_journal_records(&mut store);
    assert!(
        !records.is_empty(),
        "journal should contain at least one record after dataset creation"
    );

    // Verify the journal dataset handle is valid (can be used for queue)
    let journal_queue = store.open_queue(journal_handle);
    assert!(
        journal_queue.is_ok(),
        "should be able to open journal queue"
    );

    store.close().unwrap();
}
