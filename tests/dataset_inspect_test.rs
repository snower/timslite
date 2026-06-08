//! Integration tests for dataset inspect API.

use timslite::{DataSet, DataSetKey, Store, StoreConfig};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("timslite_inspect_test");
    let dir = d.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_inspect_basic() {
    let dir = temp_dir("inspect_basic");
    let id = DataSetKey {
        name: "sensor".into(),
        dataset_type: "temperature".into(),
    };
    let ds = DataSet::create(
        id,
        dir.clone(),
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        256 * 1024,
        4 * 1024,
        0,
    )
    .unwrap();

    let result = ds.inspect().unwrap();

    // Verify info fields
    assert_eq!(result.info.name, "sensor");
    assert_eq!(result.info.dataset_type, "temperature");
    assert_eq!(result.info.data_segment_size, 64 * 1024 * 1024);
    assert_eq!(result.info.index_segment_size, 4 * 1024 * 1024);
    assert_eq!(result.info.initial_data_segment_size, 256 * 1024);
    assert_eq!(result.info.initial_index_segment_size, 4 * 1024);
    assert_eq!(result.info.compress_level, 6);
    assert_eq!(result.info.index_continuous, 0);
    assert_eq!(result.info.retention_window, 0);
    assert!(result.info.create_time > 0);

    // Verify state fields (empty dataset)
    assert_eq!(result.state.latest_written_timestamp, 0);
    assert_eq!(result.state.open_data_segments, 0);
    assert_eq!(result.state.closed_data_segments, 0);
    assert_eq!(result.state.total_record_count, 0);
    assert_eq!(result.state.total_data_size, 0);
    assert_eq!(result.state.total_uncompressed_size, 0);
    assert_eq!(result.state.total_invalid_record_count, 0);
    assert_eq!(result.state.min_timestamp, 0);
    assert_eq!(result.state.max_timestamp, 0);
    assert_eq!(result.state.open_index_segments, 0);
    assert_eq!(result.state.closed_index_segments, 0);
    assert_eq!(result.state.pending_index_entries, 0);
    assert_eq!(result.state.base_timestamp, None);
    assert!(!result.state.read_only);
    assert!(!result.state.has_queue);
    assert_eq!(result.state.queue_consumer_groups, 0);
}

#[test]
fn test_inspect_info_fields() {
    let dir = temp_dir("inspect_info_fields");
    let id = DataSetKey {
        name: "test_data".into(),
        dataset_type: "metrics".into(),
    };
    let ds = DataSet::create(
        id,
        dir.clone(),
        128 * 1024 * 1024, // data_segment_size
        8 * 1024 * 1024,   // index_segment_size
        9,                 // compress_level
        1,                 // index_continuous
        512 * 1024,        // initial_data_segment_size
        8 * 1024,          // initial_index_segment_size
        1000,              // retention_window
    )
    .unwrap();

    let result = ds.inspect().unwrap();

    assert_eq!(result.info.name, "test_data");
    assert_eq!(result.info.dataset_type, "metrics");
    assert_eq!(result.info.data_segment_size, 128 * 1024 * 1024);
    assert_eq!(result.info.index_segment_size, 8 * 1024 * 1024);
    assert_eq!(result.info.initial_data_segment_size, 512 * 1024);
    assert_eq!(result.info.initial_index_segment_size, 8 * 1024);
    assert_eq!(result.info.compress_level, 9);
    assert_eq!(result.info.index_continuous, 1);
    assert_eq!(result.info.retention_window, 1000);
}

#[test]
fn test_inspect_state_after_write() {
    let dir = temp_dir("inspect_state_after_write");
    let id = DataSetKey {
        name: "sensor".into(),
        dataset_type: "temperature".into(),
    };
    let mut ds = DataSet::create(
        id,
        dir.clone(),
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        256 * 1024,
        4 * 1024,
        0,
    )
    .unwrap();

    // Write some data
    ds.write(100, b"hello").unwrap();
    ds.write(200, b"world").unwrap();
    ds.write(300, b"test").unwrap();

    let result = ds.inspect().unwrap();

    assert_eq!(result.state.latest_written_timestamp, 300);
    assert_eq!(result.state.total_record_count, 3);
    assert!(result.state.total_data_size > 0);
    assert!(result.state.total_uncompressed_size > 0);
    assert_eq!(result.state.total_invalid_record_count, 0);
    assert_eq!(result.state.min_timestamp, 100);
    assert_eq!(result.state.max_timestamp, 300);
    assert!(result.state.open_data_segments > 0);
}

#[test]
fn test_inspect_state_multi_segment() {
    let dir = temp_dir("inspect_state_multi_segment");
    let id = DataSetKey {
        name: "sensor".into(),
        dataset_type: "temperature".into(),
    };
    // Use small segment size to force multiple segments
    let data_segment_size = 256;
    let mut ds = DataSet::create(
        id,
        dir.clone(),
        data_segment_size,
        4096,
        0,
        0,
        data_segment_size,
        4096,
        0,
    )
    .unwrap();

    // Write data that will span multiple segments
    for i in 0..10 {
        let ts = (i + 1) * 100;
        let data = vec![0xAA; 64];
        ds.write(ts, &data).unwrap();
    }

    let result = ds.inspect().unwrap();

    assert_eq!(result.state.latest_written_timestamp, 1000);
    assert_eq!(result.state.total_record_count, 10);
    assert!(result.state.open_data_segments > 1 || result.state.closed_data_segments > 0);
    assert_eq!(result.state.min_timestamp, 100);
    assert_eq!(result.state.max_timestamp, 1000);
}

#[test]
fn test_inspect_state_empty_dataset() {
    let dir = temp_dir("inspect_state_empty");
    let id = DataSetKey {
        name: "sensor".into(),
        dataset_type: "temperature".into(),
    };
    let ds = DataSet::create(
        id,
        dir.clone(),
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        256 * 1024,
        4 * 1024,
        0,
    )
    .unwrap();

    let result = ds.inspect().unwrap();

    assert_eq!(result.state.latest_written_timestamp, 0);
    assert_eq!(result.state.total_record_count, 0);
    assert_eq!(result.state.total_data_size, 0);
    assert_eq!(result.state.min_timestamp, 0);
    assert_eq!(result.state.max_timestamp, 0);
    assert_eq!(result.state.base_timestamp, None);
}

#[test]
fn test_inspect_with_queue() {
    let dir = temp_dir("inspect_with_queue");
    let id = DataSetKey {
        name: "sensor".into(),
        dataset_type: "temperature".into(),
    };
    let mut ds = DataSet::create(
        id,
        dir.clone(),
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        256 * 1024,
        4 * 1024,
        0,
    )
    .unwrap();

    // Open queue
    ds.open_queue().unwrap();

    let result = ds.inspect().unwrap();

    assert!(result.state.has_queue);
}

#[test]
fn test_inspect_not_found() {
    let dir = temp_dir("inspect_not_found");
    let config = StoreConfig {
        enable_background_thread: false,
        enable_journal: false,
        ..Default::default()
    };
    let store = Store::open(&dir, config).unwrap();

    let result = store.inspect_dataset("nonexistent", "data");
    assert!(result.is_err());
}

#[test]
fn test_inspect_after_drop() {
    let dir = temp_dir("inspect_after_drop");
    let config = StoreConfig {
        enable_background_thread: false,
        enable_journal: false,
        ..Default::default()
    };
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "sensor",
            "temperature",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    // Inspect should work before drop
    let result = store.inspect_dataset("sensor", "temperature").unwrap();
    assert_eq!(result.info.name, "sensor");

    // Drop the dataset
    store.drop_dataset_by_name("sensor", "temperature").unwrap();

    // Inspect should fail after drop
    let result = store.inspect_dataset("sensor", "temperature");
    assert!(result.is_err());
}
