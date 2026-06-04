use std::fs;
use std::path::PathBuf;
use std::time::Duration;

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

    assert!(!dir.join(JOURNAL_DATASET_NAME).exists());
    assert!(store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .is_err());
}

#[test]
fn t28_4_create_write_delete_drop_are_recorded() {
    let dir = temp_dir("records");
    let mut store = Store::open(&dir, test_config()).unwrap();

    let handle = store
        .create_dataset("sensor", "events", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    store.write_dataset(handle, 10, b"first").unwrap();
    store.write_dataset(handle, 10, b"corrected").unwrap();
    store.delete_dataset_record(handle, 10).unwrap();
    store.drop_dataset(handle).unwrap();

    let records = read_all_journal_records(&mut store);
    let kinds: Vec<_> = records.iter().map(|r| r.kind).collect();

    assert_eq!(
        kinds,
        vec![
            JournalRecordKind::CreateDataset,
            JournalRecordKind::DataWrite,
            JournalRecordKind::DataWrite,
            JournalRecordKind::DataDelete,
            JournalRecordKind::DropDataset,
        ]
    );
    assert!(records.iter().all(|r| r.name == "sensor"));
    assert!(records.iter().all(|r| r.dataset_type == "events"));
    assert!(records[0].metadata.as_ref().unwrap().len() > 8);
    assert_eq!(records[1].index_info.as_ref().unwrap().timestamp, 10);
    assert_eq!(records[3].index_info.as_ref().unwrap().timestamp, 10);
}

#[test]
fn t28_5_journal_dataset_is_readonly_and_queue_push_is_rejected() {
    let dir = temp_dir("readonly");
    let mut store = Store::open(&dir, test_config()).unwrap();
    let journal = store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .unwrap();

    assert!(store.write_dataset(journal, 1, b"fake").is_err());
    assert!(store.delete_dataset_record(journal, 1).is_err());
    assert!(store.drop_dataset(journal).is_err());

    let queue = store.open_queue(journal).unwrap();
    assert!(store.queue_push(&queue, b"fake").is_err());
}

#[test]
fn t28_6_journal_queue_polls_realtime_records() {
    let dir = temp_dir("queue");
    let mut store = Store::open(&dir, test_config()).unwrap();
    let queue = store.open_journal_queue().unwrap();
    let consumer = store.open_consumer(&queue, "replica_a").unwrap();

    let _handle = store
        .create_dataset("stream", "events", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    let (_, payload) = store
        .queue_poll(&consumer, Duration::from_millis(250))
        .unwrap()
        .expect("journal record should be available");
    let record = JournalRecord::decode(&payload).unwrap();

    assert_eq!(record.kind, JournalRecordKind::CreateDataset);
    assert_eq!(record.name, "stream");
    assert_eq!(record.dataset_type, "events");
}

#[test]
fn t28_7_journal_read_query_iter_latest_and_ack_work() {
    let dir = temp_dir("read_iter_ack");
    let mut store = Store::open(&dir, test_config()).unwrap();

    let queue = store.open_journal_queue().unwrap();
    let consumer = store.open_consumer(&queue, "replica_b").unwrap();
    let _handle = store
        .create_dataset("iter", "events", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    let (polled_ts, payload) = store
        .queue_poll(&consumer, Duration::from_millis(250))
        .unwrap()
        .expect("journal record should be available");
    store.queue_ack(&consumer, polled_ts).unwrap();
    let from_queue = JournalRecord::decode(&payload).unwrap();

    let journal = store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .unwrap();
    let ds = store.get_dataset(&journal).unwrap();
    let mut ds = ds.lock().unwrap();

    assert_eq!(ds.latest_written_timestamp(), polled_ts);
    let from_read = ds
        .read(polled_ts, Some(store.block_cache()))
        .unwrap()
        .unwrap();
    let from_read = JournalRecord::decode(&from_read.1).unwrap();
    assert_eq!(from_read.kind, from_queue.kind);
    assert_eq!(from_read.name, from_queue.name);

    let mut iter = ds
        .query_iter(1, i64::MAX, Some(store.block_cache()))
        .unwrap();
    let (_, iter_payload) = iter.next_entry().unwrap().unwrap();
    let from_iter = JournalRecord::decode(&iter_payload).unwrap();
    assert_eq!(from_iter.kind, JournalRecordKind::CreateDataset);
    assert_eq!(from_iter.name, "iter");
    assert!(iter.next_entry().unwrap().is_none());
}
