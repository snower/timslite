//! Integration tests for dataset inspect API.

use std::sync::Arc;

use timslite::{DataSet, DataSetConfigBuilder, DataSetHandle, Store, StoreConfig};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("timslite_inspect_test");
    let dir = d.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn store_config() -> StoreConfig {
    StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build()
}

#[allow(clippy::too_many_arguments)]
fn create_dataset(
    dir: &std::path::Path,
    name: &str,
    dataset_type: &str,
    data_segment_size: u64,
    index_segment_size: u64,
    compress_level: u8,
    index_continuous: u8,
    initial_data_segment_size: u64,
    initial_index_segment_size: u64,
    retention_window: u64,
) -> (Store, DataSetHandle, Arc<DataSet>) {
    let mut store = Store::open(dir, store_config()).unwrap();
    let builder = DataSetConfigBuilder::from_store(store.config())
        .data_segment_size(data_segment_size)
        .index_segment_size(index_segment_size)
        .compress_level(compress_level)
        .index_continuous(index_continuous)
        .initial_data_segment_size(initial_data_segment_size)
        .initial_index_segment_size(initial_index_segment_size)
        .retention_window(retention_window);
    let handle = store
        .create_dataset_with_config(name, dataset_type, Some(builder))
        .unwrap();
    let dataset = store.get_dataset(&handle).unwrap();
    (store, handle, dataset)
}

#[test]
fn test_inspect_basic() {
    let dir = temp_dir("inspect_basic");
    let (_store, _handle, ds) = create_dataset(
        &dir,
        "sensor",
        "temperature",
        64 * 1024 * 1024,
        16 * 1024 * 1024,
        6,
        0,
        256 * 1024,
        16 * 1024,
        0,
    );

    let result = ds.inspect().unwrap();

    // Verify info fields
    assert_eq!(result.info.name, "sensor");
    assert_eq!(result.info.dataset_type, "temperature");
    assert_eq!(result.info.data_segment_size, 64 * 1024 * 1024);
    assert_eq!(result.info.index_segment_size, 16 * 1024 * 1024);
    assert_eq!(result.info.initial_data_segment_size, 256 * 1024);
    assert_eq!(result.info.initial_index_segment_size, 16 * 1024);
    assert_eq!(result.info.compress_level, 6);
    assert_eq!(result.info.index_continuous, 0);
    assert_eq!(result.info.retention_window, 0);
    assert!(result.info.create_time > 0);

    // Verify state fields (empty dataset)
    assert_eq!(result.state.latest_written_timestamp, None);
    assert_eq!(result.state.open_data_segments, 0);
    assert_eq!(result.state.data_segments, 0);
    assert_eq!(result.state.total_record_count, 0);
    assert_eq!(result.state.total_data_size, 0);
    assert_eq!(result.state.total_uncompressed_size, 0);
    assert_eq!(result.state.total_invalid_record_count, 0);
    assert_eq!(result.state.min_timestamp, None);
    assert_eq!(result.state.max_timestamp, None);
    assert_eq!(result.state.open_index_segments, 0);
    assert_eq!(result.state.index_segments, 0);
    assert_eq!(result.state.pending_index_entries, 0);
    assert_eq!(result.state.base_timestamp, None);
    assert!(!result.state.read_only);
    assert!(!result.state.has_queue);
    assert_eq!(result.state.queue_consumer_groups, 0);
}

#[test]
fn test_inspect_timestamp_zero_is_present_value() {
    let dir = temp_dir("inspect_timestamp_zero");
    let (_store, _handle, ds) = create_dataset(
        &dir,
        "zero",
        "metrics",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        1,
        256 * 1024,
        4 * 1024,
        0,
    );

    ds.write(0, b"zero").unwrap();
    let result = ds.inspect().unwrap();

    assert_eq!(result.state.latest_written_timestamp, Some(0));
    assert_eq!(result.state.min_timestamp, Some(0));
    assert_eq!(result.state.max_timestamp, Some(0));
    assert_eq!(result.state.base_timestamp, Some(0));
}

#[test]
fn test_inspect_info_fields() {
    let dir = temp_dir("inspect_info_fields");
    let (_store, _handle, ds) = create_dataset(
        &dir,
        "test_data",
        "metrics",
        128 * 1024 * 1024, // data_segment_size
        8 * 1024 * 1024,   // index_segment_size
        9,                 // compress_level
        1,                 // index_continuous
        512 * 1024,        // initial_data_segment_size
        8 * 1024,          // initial_index_segment_size
        1000,              // retention_window
    );

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
    let (_store, _handle, ds) = create_dataset(
        &dir,
        "sensor",
        "temperature",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        256 * 1024,
        4 * 1024,
        0,
    );

    // Write some data
    ds.write(100, b"hello").unwrap();
    ds.write(200, b"world").unwrap();
    ds.write(300, b"test").unwrap();

    let result = ds.inspect().unwrap();

    assert_eq!(result.state.latest_written_timestamp, Some(300));
    assert_eq!(result.state.total_record_count, 3);
    assert!(result.state.total_data_size > 0);
    assert!(result.state.total_uncompressed_size > 0);
    assert_eq!(result.state.total_invalid_record_count, 0);
    assert_eq!(result.state.min_timestamp, Some(100));
    assert_eq!(result.state.max_timestamp, Some(300));
    assert!(result.state.open_data_segments > 0);
}

#[test]
fn test_inspect_state_multi_segment() {
    let dir = temp_dir("inspect_state_multi_segment");
    // Use small segment size to force multiple segments
    let data_segment_size = 256;
    let (_store, _handle, ds) = create_dataset(
        &dir,
        "sensor",
        "temperature",
        data_segment_size,
        4096,
        0,
        0,
        data_segment_size,
        4096,
        0,
    );

    // Write data that will span multiple segments
    for i in 0..10 {
        let ts = (i + 1) * 100;
        let data = vec![0xAA; 64];
        ds.write(ts, &data).unwrap();
    }

    let result = ds.inspect().unwrap();

    assert_eq!(result.state.latest_written_timestamp, Some(1000));
    assert_eq!(result.state.total_record_count, 10);
    assert!(result.state.data_segments > 1);
    assert_eq!(result.state.min_timestamp, Some(100));
    assert_eq!(result.state.max_timestamp, Some(1000));
}

#[test]
fn test_inspect_state_file_created_on_dataset_create() {
    let dir = temp_dir("inspect_state_file_created");
    let (_store, _handle, _ds) = create_dataset(
        &dir,
        "sensor",
        "temperature",
        1024,
        4096,
        6,
        0,
        1024,
        4096,
        0,
    );

    let state_path = dir.join("sensor").join("temperature").join("state");
    assert!(state_path.exists(), "dataset state file should be created");
    assert_eq!(std::fs::metadata(state_path).unwrap().len(), 64);
}

#[test]
fn test_inspect_counts_archived_segments_after_reopen_without_opening_all_segments() {
    let dir = temp_dir("inspect_archived_after_reopen");
    let data_segment_size = 256;
    let (mut store, _handle, ds) = create_dataset(
        &dir,
        "sensor",
        "temperature",
        data_segment_size,
        4096,
        0,
        0,
        data_segment_size,
        4096,
        0,
    );

    for i in 0..10 {
        ds.write((i + 1) * 100, &[0xAA; 64]).unwrap();
    }
    ds.close().unwrap();

    let reopened_handle = store.open_dataset("sensor", "temperature").unwrap();
    let reopened = store.get_dataset(&reopened_handle).unwrap();
    let result = reopened.inspect().unwrap();

    assert_eq!(result.state.open_data_segments, 0);
    assert!(result.state.data_segments > 1);
    assert_eq!(result.state.total_record_count, 10);
    assert!(result.state.total_data_size > 0);
    assert!(result.state.total_uncompressed_size > 0);
    assert_eq!(result.state.total_invalid_record_count, 0);
    assert_eq!(result.state.min_timestamp, Some(100));
    assert_eq!(result.state.max_timestamp, Some(1000));
}

#[test]
fn test_inspect_archived_delete_updates_invalid_count() {
    let dir = temp_dir("inspect_archived_delete_invalid");
    let data_segment_size = 256;
    let (_store, _handle, ds) = create_dataset(
        &dir,
        "sensor",
        "temperature",
        data_segment_size,
        4096,
        0,
        0,
        data_segment_size,
        4096,
        0,
    );

    for i in 0..10 {
        ds.write((i + 1) * 100, &[0xAA; 64]).unwrap();
    }
    let before = ds.inspect().unwrap();
    assert!(before.state.data_segments > 1);
    assert_eq!(before.state.total_invalid_record_count, 0);

    ds.delete(100).unwrap();
    let after = ds.inspect().unwrap();

    assert_eq!(
        after.state.total_record_count,
        before.state.total_record_count
    );
    assert_eq!(after.state.total_invalid_record_count, 1);
}

#[test]
fn test_inspect_retention_reclaim_subtracts_archived_stats() {
    let dir = temp_dir("inspect_retention_subtracts_state");
    let data_segment_size = 256;
    let (_store, _handle, ds) = create_dataset(
        &dir,
        "sensor",
        "temperature",
        data_segment_size,
        4096,
        0,
        0,
        data_segment_size,
        4096,
        500,
    );

    for i in 0..10 {
        ds.write((i + 1) * 100, &[0xAA; 64]).unwrap();
    }
    let before = ds.inspect().unwrap();

    let reclaimed = ds.reclaim_expired_segments().unwrap();
    let after = ds.inspect().unwrap();

    assert!(reclaimed > 0);
    assert!(after.state.data_segments < before.state.data_segments);
    assert!(after.state.total_record_count < before.state.total_record_count);
    assert!(after.state.total_data_size < before.state.total_data_size);
    assert!(after.state.total_uncompressed_size < before.state.total_uncompressed_size);
}

#[test]
fn test_inspect_state_empty_dataset() {
    let dir = temp_dir("inspect_state_empty");
    let (_store, _handle, ds) = create_dataset(
        &dir,
        "sensor",
        "temperature",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        256 * 1024,
        4 * 1024,
        0,
    );

    let result = ds.inspect().unwrap();

    assert_eq!(result.state.latest_written_timestamp, None);
    assert_eq!(result.state.total_record_count, 0);
    assert_eq!(result.state.total_data_size, 0);
    assert_eq!(result.state.min_timestamp, None);
    assert_eq!(result.state.max_timestamp, None);
    assert_eq!(result.state.base_timestamp, None);
}

#[test]
fn test_inspect_with_queue() {
    let dir = temp_dir("inspect_with_queue");
    let (mut store, handle, ds) = create_dataset(
        &dir,
        "sensor",
        "temperature",
        64 * 1024 * 1024,
        4 * 1024 * 1024,
        6,
        0,
        256 * 1024,
        4 * 1024,
        0,
    );

    // Open queue
    let _queue = store.open_queue(handle).unwrap();

    let result = ds.inspect().unwrap();

    assert!(result.state.has_queue);
}

#[test]
fn test_inspect_not_found() {
    let dir = temp_dir("inspect_not_found");
    let store = Store::open(&dir, store_config()).unwrap();

    let result = store.inspect_dataset("nonexistent", "data");
    assert!(result.is_err());
}

#[test]
fn test_store_inspect_unopened_dataset_opens_and_keeps_it_loaded() {
    let dir = temp_dir("inspect_lazy_open");
    let config = store_config();

    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let handle = store
            .create_dataset_with_config("inspect_lazy", "data", None)
            .unwrap();
        store.write_dataset(handle, 1, b"row").unwrap();
        store.close().unwrap();
    }

    let mut store = Store::open(&dir, config).unwrap();
    let result = store.inspect_dataset("inspect_lazy", "data").unwrap();
    assert_eq!(result.info.name, "inspect_lazy");

    let handle = store.open_dataset("inspect_lazy", "data").unwrap();
    assert_eq!(store.read_dataset(handle, 1).unwrap().unwrap().1, b"row");
}

#[test]
fn test_inspect_after_drop() {
    let dir = temp_dir("inspect_after_drop");
    let mut store = Store::open(&dir, store_config()).unwrap();

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
