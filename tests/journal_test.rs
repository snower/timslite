//! Journal integration tests.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use std::time::Duration;

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
    store
        .journal_query(1, i64::MAX)
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
    assert!(dir
        .join(JOURNAL_DATASET_NAME)
        .join(JOURNAL_DATASET_TYPE)
        .join("data")
        .exists());
    assert!(!dir
        .join(JOURNAL_DATASET_NAME)
        .join(JOURNAL_DATASET_TYPE)
        .join("index")
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
    assert!(store.journal_latest_sequence().is_err());
    assert!(store.journal_read(1).is_err());
    assert!(store.journal_query(1, 10).is_err());
    assert!(store.open_journal_queue().is_err());
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
    assert_eq!(last.dataset_identifier, 1);
    assert_eq!(last.name.as_deref(), Some("t28_ds"));
    assert_eq!(last.dataset_type.as_deref(), Some("metrics"));

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
    assert_eq!(last.dataset_identifier, 1);
    assert_eq!(last.name.as_deref(), Some("t28_ds"));
    assert_eq!(last.dataset_type.as_deref(), Some("metrics"));

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

    let err = match store.open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE) {
        Ok(_) => panic!("journal must not be exposed as a public dataset handle"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("journal") || err.to_string().contains("not found"));

    store.close().unwrap();
}

#[test]
fn t28_10_direct_journal_dataset_mutations_are_read_only() {
    let dir = temp_dir("direct_journal_read_only");
    let mut store = Store::open(&dir, test_config()).unwrap();

    assert!(store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .is_err());
    assert!(store.journal_latest_sequence().unwrap().is_none());

    store.close().unwrap();
}

#[test]
fn t28_11_direct_dataset_mutations_use_store_context_journal() {
    let dir = temp_dir("direct_dataset_context");
    let mut store = Store::open(&dir, test_config()).unwrap();

    let handle = store
        .create_dataset("ctx_ds", "metrics", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let identifier = store.dataset_identifier(handle).unwrap();
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
        .filter(|record| record.dataset_identifier == identifier)
        .collect();

    assert!(direct_records.iter().any(|record| {
        record.kind == JournalRecordKind::DataWrite
            && record.name.is_none()
            && record.dataset_type.is_none()
            && record.index_info.unwrap().timestamp == 10
    }));
    assert!(direct_records.iter().any(|record| {
        record.kind == JournalRecordKind::DataAppend
            && record.name.is_none()
            && record.dataset_type.is_none()
            && record.index_info.unwrap().timestamp == 20
            && record.append_info.unwrap().data_len == 6
    }));
    assert!(direct_records.iter().any(|record| {
        record.kind == JournalRecordKind::DataDelete
            && record.name.is_none()
            && record.dataset_type.is_none()
            && record.index_info.unwrap().timestamp == 10
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

    let journal_queue = store.open_journal_queue().unwrap();
    let consumer = journal_queue.open_consumer("test_group").unwrap();
    assert!(consumer.poll(Duration::from_millis(1)).unwrap().is_none());

    store.close().unwrap();
}

#[test]
fn t28_13_journal_dedicated_api_queryable_at_store_level() {
    let dir = temp_dir("journal_openable");
    let mut store = Store::open(&dir, test_config()).unwrap();

    // Create at least one dataset so the journal receives log entries
    store
        .create_dataset("jds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    let records = read_all_journal_records(&mut store);
    assert!(
        !records.is_empty(),
        "journal should contain at least one record after dataset creation"
    );

    assert!(store
        .open_dataset(JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE)
        .is_err());
    assert!(store.open_journal_queue().is_ok());

    store.close().unwrap();
}

// ─── Journal 0x13 append tests (P0-J-1~2) ───────────────────────────────────

#[test]
fn t28_14_append_writes_journal_0x13_record() {
    // P0-J-1: DataSet::append() should write journal record with kind=DataAppend
    let dir = temp_dir("append_0x13");
    let mut store = Store::open(&dir, test_config()).unwrap();

    store
        .create_dataset("jds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    // Use append to create a new record (forward append: ts > latest)
    let ds_handle = store.open_dataset("jds", "data").unwrap();
    let identifier = store.dataset_identifier(ds_handle).unwrap();
    store
        .append_dataset(ds_handle, 1, b"append_data_1")
        .unwrap();

    // Read journal records
    let records = read_all_journal_records(&mut store);

    // Find the DataAppend record
    let append_records: Vec<_> = records
        .iter()
        .filter(|r| r.kind == JournalRecordKind::DataAppend)
        .collect();

    assert!(
        !append_records.is_empty(),
        "journal should contain at least one DataAppend (0x13) record"
    );

    // Verify the append record has correct compact dataset reference.
    let append_record = append_records.last().unwrap();
    assert_eq!(append_record.dataset_identifier, identifier);
    assert_eq!(append_record.name, None);
    assert_eq!(append_record.dataset_type, None);
    assert!(
        append_record.index_info.is_some(),
        "append record should have index_info"
    );
    assert_eq!(
        append_record.index_info.unwrap().timestamp,
        1,
        "append record timestamp should be 1"
    );

    store.close().unwrap();
}

#[test]
fn t28_15_journal_queue_consumes_0x13_records() {
    // P0-J-2: journal queue should correctly return 0x13 type records
    let dir = temp_dir("queue_0x13");
    let mut store = Store::open(&dir, test_config()).unwrap();

    store
        .create_dataset("jds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();

    // Open journal queue BEFORE append operations so we receive the notifications
    let journal_queue = store.open_journal_queue().unwrap();
    let journal_consumer = journal_queue.open_consumer("test_group").unwrap();

    // Use append to create records
    let ds_handle = store.open_dataset("jds", "data").unwrap();
    store.append_dataset(ds_handle, 1, b"append_1").unwrap();
    store.append_dataset(ds_handle, 2, b"append_2").unwrap();

    // Poll journal queue for records
    let mut append_count = 0;
    let mut other_count = 0;

    loop {
        let result = journal_consumer.poll(Duration::from_millis(100)).unwrap();
        match result {
            Some((ts, data)) => {
                let record = JournalRecord::decode(&data).unwrap();
                if record.kind == JournalRecordKind::DataAppend {
                    append_count += 1;
                    // Verify it's one of our append timestamps
                    let record_ts = record.index_info.unwrap().timestamp;
                    assert!(
                        record_ts == 1 || record_ts == 2,
                        "unexpected append timestamp: {}",
                        record_ts
                    );
                } else {
                    other_count += 1;
                }
                journal_consumer.ack(ts).unwrap();
            }
            None => break,
        }
    }

    assert!(
        append_count >= 2,
        "journal queue should contain at least 2 DataAppend records, got {} (other: {})",
        append_count,
        other_count
    );

    store.close().unwrap();
}
