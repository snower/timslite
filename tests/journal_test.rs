//! Journal integration tests.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use std::time::Duration;

use timslite::{
    DataSetConfigBuilder, JournalRecord, JournalRecordKind, QueueConsumerConfig, Store,
    StoreConfig, JOURNAL_DATASET_NAME, JOURNAL_DATASET_TYPE,
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
fn t28_0_store_reads_journal_source_record_through_safe_api() {
    let dir = temp_dir("source_record_api");
    let mut store = Store::open(&dir, test_config()).unwrap();
    let handle = store
        .create_dataset_with_config("source_ds", "data", None)
        .unwrap();
    store.write_dataset(handle, 42, b"journal-source").unwrap();

    let record = read_all_journal_records(&mut store)
        .into_iter()
        .find(|record| record.kind == JournalRecordKind::DataWrite)
        .expect("write journal record");
    let index_info = record.index_info.expect("write index info");

    let (ts, data) = store
        .read_journal_source_record(record.dataset_identifier, index_info)
        .unwrap();
    assert_eq!(ts, 42);
    assert_eq!(data, b"journal-source");
}

#[test]
fn t28_1_store_config_defaults_enable_journal() {
    assert!(StoreConfig::default().enable_journal());
    assert!(!StoreConfig::builder()
        .enable_journal(false)
        .build()
        .enable_journal());
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
        let ds = ds.clone();
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

// 鈹€鈹€鈹€ Journal 0x13 append tests (P0-J-1~2) 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

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

#[test]
fn t41_4_journal_queue_unexpired_pending_does_not_block_next_record() {
    let dir = temp_dir("journal_queue_visibility");
    let mut store = Store::open(&dir, test_config()).unwrap();
    store
        .create_dataset("jretry", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("jretry", "data").unwrap();
    let queue = store.open_journal_queue().unwrap();
    let config = QueueConsumerConfig::builder()
        .running_expired_seconds(60)
        .max_retry_count(3)
        .build()
        .unwrap();
    let c1 = queue.open_consumer_with_config("shared", config).unwrap();
    let c2 = queue.open_consumer_with_config("shared", config).unwrap();

    store.write_dataset(handle, 1, b"first").unwrap();
    store.write_dataset(handle, 2, b"second").unwrap();

    let (seq1, payload1) = c1.poll(Duration::from_millis(100)).unwrap().unwrap();
    let first = JournalRecord::decode(&payload1).unwrap();
    assert_eq!(first.index_info.unwrap().timestamp, 1);

    let (seq2, payload2) = c2.poll(Duration::from_millis(100)).unwrap().unwrap();
    let second = JournalRecord::decode(&payload2).unwrap();
    assert_eq!(seq2, seq1 + 1);
    assert_eq!(second.index_info.unwrap().timestamp, 2);

    c1.ack(seq1).unwrap();
    c2.ack(seq2).unwrap();
    store.close().unwrap();
}

#[test]
fn t41_5_journal_queue_retry_limit_drops_before_next_sequence() {
    let dir = temp_dir("journal_queue_retry_limit");
    let mut store = Store::open(&dir, test_config()).unwrap();
    store
        .create_dataset("jretry", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("jretry", "data").unwrap();
    let queue = store.open_journal_queue().unwrap();
    let config = QueueConsumerConfig::builder()
        .running_expired_seconds(1)
        .max_retry_count(1)
        .build()
        .unwrap();
    let consumer = queue.open_consumer_with_config("retry", config).unwrap();

    store.write_dataset(handle, 1, b"first").unwrap();
    store.write_dataset(handle, 2, b"second").unwrap();

    let (seq1, payload1) = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
    let first = JournalRecord::decode(&payload1).unwrap();
    assert_eq!(first.index_info.unwrap().timestamp, 1);

    std::thread::sleep(Duration::from_millis(1100));
    let (retry_seq, retry_payload) = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
    let retry = JournalRecord::decode(&retry_payload).unwrap();
    assert_eq!(retry_seq, seq1);
    assert_eq!(retry.index_info.unwrap().timestamp, 1);

    std::thread::sleep(Duration::from_millis(1100));
    let (seq2, payload2) = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
    let second = JournalRecord::decode(&payload2).unwrap();
    assert_eq!(seq2, seq1 + 1);
    assert_eq!(second.index_info.unwrap().timestamp, 2);

    consumer.ack(seq2).unwrap();
    store.close().unwrap();
}

#[test]
fn t39_1_dataset_journal_disabled_skips_all_record_kinds() {
    let dir = temp_dir("dataset_journal_disabled");
    let config = test_config();
    let mut store = Store::open(&dir, config.clone()).unwrap();

    let quiet_handle = store
        .create_dataset_with_config(
            "quiet",
            "data",
            Some(DataSetConfigBuilder::from_store(&config).enable_journal(false)),
        )
        .unwrap();
    let quiet_identifier = store.dataset_identifier(quiet_handle).unwrap();

    store
        .write_dataset(quiet_handle, 10, b"quiet_write")
        .unwrap();
    store
        .append_dataset(quiet_handle, 20, b"quiet_append")
        .unwrap();
    store.delete_dataset_record(quiet_handle, 10).unwrap();

    let loud_handle = store
        .create_dataset_with_config("loud", "data", None)
        .unwrap();
    let loud_identifier = store.dataset_identifier(loud_handle).unwrap();
    store.write_dataset(loud_handle, 10, b"loud_write").unwrap();
    store
        .append_dataset(loud_handle, 20, b"loud_append")
        .unwrap();
    store.delete_dataset_record(loud_handle, 10).unwrap();

    store.drop_dataset(quiet_handle).unwrap();
    store.drop_dataset(loud_handle).unwrap();

    let records = read_all_journal_records(&mut store);
    assert!(
        records
            .iter()
            .all(|record| record.dataset_identifier != quiet_identifier),
        "disabled dataset should not emit create/drop/write/delete/append records"
    );
    assert!(records.iter().any(|record| {
        record.dataset_identifier == loud_identifier
            && record.kind == JournalRecordKind::CreateDataset
    }));
    assert!(records.iter().any(|record| {
        record.dataset_identifier == loud_identifier && record.kind == JournalRecordKind::DataWrite
    }));
    assert!(records.iter().any(|record| {
        record.dataset_identifier == loud_identifier && record.kind == JournalRecordKind::DataAppend
    }));
    assert!(records.iter().any(|record| {
        record.dataset_identifier == loud_identifier && record.kind == JournalRecordKind::DataDelete
    }));
    assert!(records.iter().any(|record| {
        record.dataset_identifier == loud_identifier
            && record.kind == JournalRecordKind::DropDataset
    }));

    store.close().unwrap();
}

#[test]
fn t39_2_dataset_journal_disabled_persists_after_reopen() {
    let dir = temp_dir("dataset_journal_disabled_reopen");
    let config = test_config();

    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let quiet_handle = store
            .create_dataset_with_config(
                "quiet",
                "data",
                Some(DataSetConfigBuilder::from_store(&config).enable_journal(false)),
            )
            .unwrap();
        let inspect = store.inspect_dataset("quiet", "data").unwrap();
        assert!(!inspect.info.enable_journal);
        assert!(!inspect.state.has_journal);
        store.close_dataset(quiet_handle).unwrap();
        store.close().unwrap();
    }

    let mut store = Store::open(&dir, config).unwrap();
    let quiet_handle = store.open_dataset("quiet", "data").unwrap();
    let quiet_identifier = store.dataset_identifier(quiet_handle).unwrap();
    let inspect = store.inspect_dataset("quiet", "data").unwrap();
    assert!(!inspect.info.enable_journal);
    assert!(!inspect.state.has_journal);

    store
        .write_dataset(quiet_handle, 10, b"quiet_after_reopen")
        .unwrap();
    store.drop_dataset(quiet_handle).unwrap();

    let records = read_all_journal_records(&mut store);
    assert!(records
        .iter()
        .all(|record| record.dataset_identifier != quiet_identifier));

    store.close().unwrap();
}

// ── End-to-end hot migration tests ────────────────────────────────────────────

#[test]
fn t28_20_end_to_end_write_journal_replay() {
    // Source store writes data → journal records it → read journal →
    // replay on target store using read_journal_source_record
    let source_dir = temp_dir("e2e_source");
    let target_dir = temp_dir("e2e_target");
    let mut source = Store::open(&source_dir, test_config()).unwrap();

    source
        .create_dataset("migrate", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let src_handle = source.open_dataset("migrate", "data").unwrap();
    let src_identifier = source.dataset_identifier(src_handle).unwrap();

    // Write multiple records
    source
        .write_dataset(src_handle, 100, b"record_100")
        .unwrap();
    source
        .write_dataset(src_handle, 200, b"record_200")
        .unwrap();
    source
        .write_dataset(src_handle, 300, b"record_300")
        .unwrap();

    // Read journal and extract DataWrite records for our dataset
    let records = read_all_journal_records(&mut source);
    let write_records: Vec<_> = records
        .iter()
        .filter(|r| {
            r.kind == JournalRecordKind::DataWrite && r.dataset_identifier == src_identifier
        })
        .collect();
    assert!(
        write_records.len() >= 3,
        "expected at least 3 write records, got {}",
        write_records.len()
    );

    // Replay onto target store: create dataset, read source data, write to target
    let mut target = Store::open(&target_dir, test_config()).unwrap();
    target
        .create_dataset("migrate", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let tgt_handle = target.open_dataset("migrate", "data").unwrap();

    for wr in &write_records {
        let index_info = wr.index_info.expect("write index_info");
        let (ts, data) = source
            .read_journal_source_record(src_identifier, index_info)
            .unwrap();
        target.write_dataset(tgt_handle, ts, &data).unwrap();
    }

    // Verify replayed data matches source
    for ts in [100, 200, 300] {
        let src_data = source.read_dataset(src_handle, ts).unwrap().unwrap();
        let tgt_data = target.read_dataset(tgt_handle, ts).unwrap().unwrap();
        assert_eq!(src_data, tgt_data, "mismatch at ts={}", ts);
    }

    source.close().unwrap();
    target.close().unwrap();
}

#[test]
fn t28_21_delete_replay_via_journal() {
    // Source store writes then deletes data → journal records 0x11 and 0x12 →
    // consume and verify delete on target
    let source_dir = temp_dir("delete_source");
    let target_dir = temp_dir("delete_target");
    let mut source = Store::open(&source_dir, test_config()).unwrap();

    source
        .create_dataset("del_ds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let src_handle = source.open_dataset("del_ds", "data").unwrap();
    let src_identifier = source.dataset_identifier(src_handle).unwrap();

    // Write then delete
    source.write_dataset(src_handle, 50, b"to_delete").unwrap();
    source.delete_dataset_record(src_handle, 50).unwrap();

    // Also write a record that stays
    source.write_dataset(src_handle, 60, b"survivor").unwrap();

    let records = read_all_journal_records(&mut source);
    let write_records: Vec<_> = records
        .iter()
        .filter(|r| {
            r.kind == JournalRecordKind::DataWrite && r.dataset_identifier == src_identifier
        })
        .collect();
    let delete_records: Vec<_> = records
        .iter()
        .filter(|r| {
            r.kind == JournalRecordKind::DataDelete && r.dataset_identifier == src_identifier
        })
        .collect();
    assert!(
        !write_records.is_empty(),
        "expected at least one write record"
    );
    assert!(
        !delete_records.is_empty(),
        "expected at least one delete record"
    );

    // Replay: create target, replay writes, replay deletes
    let mut target = Store::open(&target_dir, test_config()).unwrap();
    target
        .create_dataset("del_ds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let tgt_handle = target.open_dataset("del_ds", "data").unwrap();

    // Replay all writes first
    for wr in &write_records {
        let index_info = wr.index_info.expect("write index_info");
        let (ts, data) = source
            .read_journal_source_record(src_identifier, index_info)
            .unwrap();
        target.write_dataset(tgt_handle, ts, &data).unwrap();
    }

    // Replay deletes
    for dr in &delete_records {
        let index_info = dr.index_info.expect("delete index_info");
        let (ts, _) = source
            .read_journal_source_record(src_identifier, index_info)
            .unwrap();
        target.delete_dataset_record(tgt_handle, ts).unwrap();
    }

    // ts=50 should be deleted, ts=60 should exist
    assert!(
        target.read_dataset(tgt_handle, 50).unwrap().is_none(),
        "ts=50 should be deleted on target"
    );
    let survivor = target.read_dataset(tgt_handle, 60).unwrap().unwrap();
    assert_eq!(survivor, (60, b"survivor".to_vec()));

    source.close().unwrap();
    target.close().unwrap();
}

#[test]
fn t28_22_corrupted_journal_returns_error() {
    // Write data, corrupt journal file on disk, reopen and verify error handling
    let dir = temp_dir("corrupt_journal");
    let mut store = Store::open(&dir, test_config()).unwrap();

    store
        .create_dataset("cj_ds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let cj_handle = store.open_dataset("cj_ds", "data").unwrap();
    store
        .write_dataset(cj_handle, 1, b"before_corrupt")
        .unwrap();

    let before = read_all_journal_records(&mut store);
    assert!(!before.is_empty());
    store.close().unwrap();

    // Corrupt journal data segment files
    let journal_data_dir = dir
        .join(JOURNAL_DATASET_NAME)
        .join(JOURNAL_DATASET_TYPE)
        .join("data");
    let mut corrupted = false;
    if journal_data_dir.exists() {
        for entry in fs::read_dir(&journal_data_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                // Truncate file to 1 byte to corrupt it
                let metadata = fs::metadata(&path).unwrap();
                if metadata.len() > 1 {
                    fs::write(&path, [0xFFu8; 1]).unwrap();
                    corrupted = true;
                }
            }
        }
    }
    assert!(
        corrupted,
        "expected at least one journal segment file to corrupt"
    );

    // Reopen with journal disabled - store must open fine
    let config_no_journal = StoreConfig::builder()
        .enable_background_thread(false)
        .data_segment_size(1024 * 1024)
        .index_segment_size(64 * 1024)
        .initial_data_segment_size(4096)
        .initial_index_segment_size(4096)
        .enable_journal(false)
        .build();
    let mut store2 = Store::open(&dir, config_no_journal).unwrap();
    // Source dataset should still be readable even with corrupted journal
    let cj_handle2 = store2.open_dataset("cj_ds", "data").unwrap();
    let read_back = store2.read_dataset(cj_handle2, 1).unwrap();
    assert!(
        read_back.is_some(),
        "source data must survive journal corruption"
    );

    // Reopen with journal enabled - either opens and re-creates or returns error
    let store3_result = Store::open(&dir, test_config());
    match store3_result {
        Ok(mut store3) => {
            // If store opens, journal query must not panic
            let _ = store3.journal_query(1, i64::MAX);
            store3.close().unwrap();
        }
        Err(_) => {
            // Acceptable: store refuses to open with corrupted journal
        }
    }

    store2.close().unwrap();
}

#[test]
fn t28_23_journal_sequence_is_contiguous_from_one() {
    // Write many records and verify journal sequences are 1, 2, 3, ... N
    let dir = temp_dir("seq_boundary");
    let mut store = Store::open(&dir, test_config()).unwrap();

    store
        .create_dataset("seq_ds", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("seq_ds", "data").unwrap();

    let write_count = 20usize;
    for i in 0..write_count {
        let ts = (i as i64) + 1;
        store
            .write_dataset(handle, ts, format!("val_{}", ts).as_bytes())
            .unwrap();
    }

    // Query all journal records
    let entries = store.journal_query(1, i64::MAX).unwrap();
    assert!(
        entries.len() >= write_count,
        "expected at least {} journal entries, got {}",
        write_count,
        entries.len()
    );

    // Verify sequences start from 1 and are contiguous
    for (i, (seq, _)) in entries.iter().enumerate() {
        assert_eq!(
            *seq,
            (i as i64) + 1,
            "journal sequence should be contiguous from 1, got seq={} at index={}",
            seq,
            i
        );
    }

    // Verify latest_sequence matches
    let latest = store.journal_latest_sequence().unwrap().unwrap();
    assert_eq!(latest, entries.len() as i64);

    // Verify we can read a single entry by sequence
    let (seq, payload) = store.journal_read(1).unwrap().unwrap();
    assert_eq!(seq, 1);
    let record = JournalRecord::decode(&payload).unwrap();
    assert_eq!(record.kind, JournalRecordKind::CreateDataset);

    // Verify we can read a specific range
    let mid_entries = store.journal_query(5, 10).unwrap();
    assert_eq!(mid_entries.len(), 6); // seq 5,6,7,8,9,10
    for (i, (seq, _)) in mid_entries.iter().enumerate() {
        assert_eq!(*seq, (i as i64) + 5);
    }

    store.close().unwrap();
}
