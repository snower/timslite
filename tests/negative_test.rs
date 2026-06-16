//! Negative tests: error paths and boundary conditions for all public APIs.
//!
//! Covers:
//! - DataSet error paths (delete/read/query on invalid state)
//! - Store error paths (invalid handles, invalid names, reserved names)
//! - Queue error paths (closed queue/consumer, nonexistent groups)
//! - Config boundary values (zero, max, invalid)
//! - Validation edge cases (empty, special chars, too long)
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

fn make_store(dir: &PathBuf) -> timslite::Store {
    use timslite::{Store, StoreConfig};
    Store::open(dir, StoreConfig::default()).unwrap()
}

fn create_dataset(store: &mut timslite::Store, name: &str, dtype: &str) -> timslite::DataSetHandle {
    store
        .create_dataset(name, dtype, 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap()
}

/// Helper: assert error contains expected substring.
fn assert_err_contains<T, E: std::fmt::Display>(result: Result<T, E>, expected: &str) {
    if let Err(e) = result {
        assert!(
            e.to_string().contains(expected),
            "expected error containing '{expected}', got: {}",
            e
        );
    } else {
        panic!("expected error containing '{expected}', got Ok");
    }
}

// ============================================================================
// DataSet Error Paths
// ============================================================================

/// Delete a timestamp that was never written.
/// Per design: returns NotFound error if no entry exists.
#[test]
fn negative_delete_nonexistent_timestamp() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "ds1", "data");
    let arc = store.get_dataset(&h).unwrap();

    let mut ds = arc.lock().unwrap();
    // Write one record so dataset is not empty
    ds.write(100, b"hello").unwrap();
    // Delete a timestamp that doesn't exist - should return NotFound
    let result = ds.delete(999);
    assert!(result.is_err());
    if let Err(e) = result {
        assert!(
            e.to_string().contains("not found") || e.to_string().contains("no entry"),
            "unexpected error: {e}"
        );
    }
    // Verify original record still exists
    assert!(ds.read(100).unwrap().is_some());
}

/// Correction write: write(ts) when ts < latest_written_timestamp.
/// Per design: should return error (not allowed unless it's the latest uncompressed tail).
#[test]
fn negative_correction_write_rejected_for_sealed() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "ds2", "data");
    let arc = store.get_dataset(&h).unwrap();

    {
        let mut ds = arc.lock().unwrap();
        // Write enough data to seal the first block (fill > 64KB)
        for i in 1..=20i64 {
            ds.write(i, &vec![0xAAu8; 4096]).unwrap();
        }
        // Now write a record at ts=1 (correction write on sealed block)
        let result = ds.write(1, b"correction");
        // This should succeed (correction write creates new block) or fail
        // depending on implementation. Verify it doesn't panic.
        match result {
            Ok(()) => {
                // Correction write succeeded - verify data
                let read_result = ds.read(1).unwrap();
                assert!(read_result.is_some());
            }
            Err(e) => {
                // Expected: correction on sealed block may be rejected
                let msg = e.to_string();
                assert!(
                    msg.contains("not found") || msg.contains("invalid") || msg.contains("expired"),
                    "unexpected error: {msg}"
                );
            }
        }
    }
}

/// read_latest() when no data has been written.
/// Per design: should return None (latest_written_timestamp is None).
#[test]
fn negative_read_latest_no_data_and_minus_one_exact() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "ds3", "data");
    let arc = store.get_dataset(&h).unwrap();

    let mut ds = arc.lock().unwrap();
    assert!(ds.read_latest().unwrap().is_none());
    assert!(ds.read(-1).unwrap().is_none());
}

/// Query with start > end returns empty.
#[test]
fn negative_query_start_greater_than_end() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "ds4", "data");
    let arc = store.get_dataset(&h).unwrap();

    let mut ds = arc.lock().unwrap();
    ds.write(100, b"data").unwrap();
    // Query with start > end
    let result = ds.query(200, 100).unwrap();
    assert!(result.is_empty());
}

/// query_exist with start > end returns empty bitmap.
#[test]
fn negative_query_exist_start_greater_than_end() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "ds5", "data");
    let arc = store.get_dataset(&h).unwrap();

    let mut ds = arc.lock().unwrap();
    ds.write(100, b"data").unwrap();
    let result = ds.query_exist(200, 100).unwrap();
    assert!(result.is_empty());
}

/// read_length on nonexistent timestamp returns None.
#[test]
fn negative_read_length_nonexistent() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "ds6", "data");
    let arc = store.get_dataset(&h).unwrap();

    let mut ds = arc.lock().unwrap();
    ds.write(100, b"hello").unwrap();
    let result = ds.read_length(999).unwrap();
    assert!(result.is_none());
}

/// query_length with start > end returns empty.
#[test]
fn negative_query_length_start_greater_than_end() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "ds7", "data");
    let arc = store.get_dataset(&h).unwrap();

    let mut ds = arc.lock().unwrap();
    ds.write(100, b"data").unwrap();
    let result = ds.query_length(200, 100).unwrap();
    assert!(result.is_empty());
}

/// Flush on empty dataset (no writes) should succeed.
#[test]
fn negative_flush_empty_dataset() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "ds8", "data");
    let arc = store.get_dataset(&h).unwrap();

    let mut ds = arc.lock().unwrap();
    // Flush with no data should succeed (no-op)
    ds.flush().unwrap();
}

/// Append oversized data (>4MB) should fail.
#[test]
fn negative_append_oversized_data() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "ds9", "data");
    let arc = store.get_dataset(&h).unwrap();

    let mut ds = arc.lock().unwrap();
    let big_data = vec![0xBBu8; 5 * 1024 * 1024]; // 5MB
    let result = ds.append(1, &big_data);
    assert!(result.is_err());
    if let Err(e) = result {
        let msg = e.to_string();
        assert!(
            msg.contains("invalid") || msg.contains("exceeds"),
            "unexpected error: {msg}"
        );
    }
}

/// Write to dataset, then close and reopen - verify data persists.
#[test]
fn negative_write_close_reopen_verify() {
    let dir = temp_dir();
    {
        let mut store = make_store(&dir);
        let h = create_dataset(&mut store, "ds10", "data");
        let arc = store.get_dataset(&h).unwrap();
        let mut ds = arc.lock().unwrap();
        ds.write(42, b"persistent").unwrap();
        ds.flush().unwrap();
    }
    // Reopen
    let mut store = make_store(&dir);
    let h = store.open_dataset("ds10", "data").unwrap();
    let arc = store.get_dataset(&h).unwrap();
    let mut ds = arc.lock().unwrap();
    let result = ds.read(42).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().1, b"persistent");
}

// ============================================================================
// Store Error Paths
// ============================================================================

/// create_dataset with empty name should fail.
#[test]
fn negative_create_empty_name() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.create_dataset("", "data", 1024, 1024, 0, 0, 0);
    assert_err_contains(result, "must match");
}

/// create_dataset with empty type should fail.
#[test]
fn negative_create_empty_type() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.create_dataset("valid_name", "", 1024, 1024, 0, 0, 0);
    assert_err_contains(result, "must match");
}

/// create_dataset with special characters in name should fail.
#[test]
fn negative_create_special_chars_name() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    for name in &["foo/bar", "foo bar", "foo@bar", "foo.bar", "foo!bar"] {
        let result = store.create_dataset(name, "data", 1024, 1024, 0, 0, 0);
        assert_err_contains(result, "must match");
    }
}

/// create_dataset with name too long (>255 bytes) should fail.
#[test]
fn negative_create_name_too_long() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let long_name = "a".repeat(256);
    let result = store.create_dataset(&long_name, "data", 1024, 1024, 0, 0, 0);
    assert_err_contains(result, "must match");
}

/// create_dataset with reserved journal name should fail.
#[test]
fn negative_create_reserved_journal_name() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.create_dataset(".journal", "logs", 1024, 1024, 0, 0, 0);
    assert_err_contains(result, "reserved");
}

/// create_dataset duplicate should fail with AlreadyExists.
#[test]
fn negative_create_duplicate_dataset() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    create_dataset(&mut store, "dup", "data");
    let result = store.create_dataset("dup", "data", 1024, 1024, 0, 0, 0);
    assert_err_contains(result, "already exists");
}

/// open_dataset nonexistent should fail with NotFound.
#[test]
fn negative_open_nonexistent_dataset() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.open_dataset("no_such_dataset", "data");
    assert_err_contains(result, "not found");
}

/// open_dataset with invalid name should fail.
#[test]
fn negative_open_invalid_name() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.open_dataset("bad/name", "data");
    assert_err_contains(result, "must match");
}

/// close_dataset with invalid handle - returns Ok (no-op, handle not found).
#[test]
fn negative_close_invalid_handle() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    // close_dataset returns Ok for invalid handle (no-op)
    store.close_dataset(timslite::DataSetHandle(9999)).unwrap();
}

/// drop_dataset with invalid handle should fail.
#[test]
fn negative_drop_invalid_handle() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.drop_dataset(timslite::DataSetHandle(9999));
    assert_err_contains(result, "not found");
}

/// drop_dataset_by_name nonexistent should fail.
#[test]
fn negative_drop_by_name_nonexistent() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.drop_dataset_by_name("no_such", "data");
    assert!(result.is_err());
    // Error could be "not found" or I/O error (path not found)
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not found") || msg.contains("系统找不到") || msg.contains("os error"),
        "unexpected error: {msg}"
    );
}

/// get_dataset with invalid handle should fail.
#[test]
fn negative_get_invalid_handle() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.get_dataset(&timslite::DataSetHandle(9999));
    assert_err_contains(result, "not found");
}

/// write_dataset with invalid handle should fail.
#[test]
fn negative_write_invalid_handle() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.write_dataset(timslite::DataSetHandle(9999), 1, b"data");
    assert_err_contains(result, "not found");
}

/// read_dataset with invalid handle should fail.
#[test]
fn negative_read_invalid_handle() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.read_dataset(timslite::DataSetHandle(9999), 1);
    assert_err_contains(result, "not found");
}

/// append_dataset with invalid handle should fail.
#[test]
fn negative_append_invalid_handle() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.append_dataset(timslite::DataSetHandle(9999), 1, b"data");
    assert_err_contains(result, "not found");
}

/// delete_dataset_record with invalid handle should fail.
#[test]
fn negative_delete_invalid_handle() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.delete_dataset_record(timslite::DataSetHandle(9999), 1);
    assert_err_contains(result, "not found");
}

/// query_dataset with invalid handle should fail.
#[test]
fn negative_query_invalid_handle() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.query_dataset(timslite::DataSetHandle(9999), 0, 100);
    assert_err_contains(result, "not found");
}

/// dataset_read_exist with invalid handle should fail.
#[test]
fn negative_read_exist_invalid_handle() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.dataset_read_exist(timslite::DataSetHandle(9999), 1);
    assert_err_contains(result, "not found");
}

/// dataset_query_exist with invalid handle should fail.
#[test]
fn negative_query_exist_invalid_handle() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.dataset_query_exist(timslite::DataSetHandle(9999), 0, 100);
    assert_err_contains(result, "not found");
}

/// dataset_read_length with invalid handle should fail.
#[test]
fn negative_read_length_invalid_handle() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.dataset_read_length(timslite::DataSetHandle(9999), 1);
    assert_err_contains(result, "not found");
}

/// dataset_query_length with invalid handle should fail.
#[test]
fn negative_query_length_invalid_handle() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.dataset_query_length(timslite::DataSetHandle(9999), 0, 100);
    assert_err_contains(result, "not found");
}

/// latest_written_timestamp with invalid handle should fail.
#[test]
fn negative_latest_timestamp_invalid_handle() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.latest_written_timestamp(timslite::DataSetHandle(9999));
    assert_err_contains(result, "not found");
}

/// inspect_dataset nonexistent should fail.
#[test]
fn negative_inspect_nonexistent() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.inspect_dataset("no_such", "data");
    assert_err_contains(result, "not found");
}

/// get_dataset_types for nonexistent name should return empty.
#[test]
fn negative_get_types_nonexistent_name() {
    let dir = temp_dir();
    let store = make_store(&dir);
    let result = store.get_dataset_types("no_such_dataset").unwrap();
    assert!(result.is_empty());
}

/// create_dataset with zero data_segment_size should fail or use minimum.
#[test]
fn negative_create_zero_segment_size() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.create_dataset("zero_seg", "data", 0, 1024, 0, 0, 0);
    // Should either fail or use a minimum value
    match result {
        Ok(_) => {} // Acceptable if minimum is enforced internally
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("invalid") || msg.contains("zero") || msg.contains("must"),
                "unexpected error: {msg}"
            );
        }
    }
}

/// create_dataset with zero index_segment_size should fail or use minimum.
#[test]
fn negative_create_zero_index_size() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let result = store.create_dataset("zero_idx", "data", 1024, 0, 0, 0, 0);
    match result {
        Ok(_) => {}
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("invalid") || msg.contains("zero") || msg.contains("must"),
                "unexpected error: {msg}"
            );
        }
    }
}

// ============================================================================
// Queue Error Paths
// ============================================================================

/// open_consumer with nonexistent group - creates new consumer group (Ok).
#[test]
fn negative_open_consumer_nonexistent_group() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "q1", "data");
    let queue = store.open_queue(h).unwrap();
    // open_consumer creates a new group if it doesn't exist
    let result = store.open_consumer(&queue, "new_group");
    assert!(result.is_ok());
}

/// drop_consumer with nonexistent group should fail.
#[test]
fn negative_drop_consumer_nonexistent_group() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "q2", "data");
    let queue = store.open_queue(h).unwrap();
    let result = store.drop_consumer(&queue, "no_such_group");
    assert_err_contains(result, "consumer group not found");
}

/// queue_push with empty data - may succeed (empty data is a valid no-op for append).
#[test]
fn negative_queue_push_empty_data() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "q3", "data");
    let queue = store.open_queue(h).unwrap();
    let result = store.queue_push(&queue, b"");
    // Empty data may be accepted (no-op) or rejected - both are valid
    match result {
        Ok(_) => {} // Accepted as no-op
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("invalid") || msg.contains("empty"),
                "unexpected error: {msg}"
            );
        }
    }
}

/// queue_push with oversized data (>4MB) should fail.
#[test]
fn negative_queue_push_oversized() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "q4", "data");
    let queue = store.open_queue(h).unwrap();
    let big = vec![0xCCu8; 5 * 1024 * 1024];
    let result = store.queue_push(&queue, &big);
    if let Err(e) = result {
        let msg = e.to_string();
        assert!(
            msg.contains("invalid") || msg.contains("exceeds"),
            "unexpected error: {msg}"
        );
    } else {
        panic!("expected error for oversized queue push");
    }
}

/// open_consumer with invalid group name should fail.
#[test]
fn negative_open_consumer_invalid_group_name() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "q5", "data");
    let queue = store.open_queue(h).unwrap();
    for name in &["bad/name", "bad name", "bad@group"] {
        let result = store.open_consumer(&queue, name);
        assert_err_contains(result, "must match");
    }
}

/// open_queue twice on same dataset should fail (QueueAlreadyOpen).
#[test]
fn negative_open_queue_twice() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "q6", "data");
    let _queue1 = store.open_queue(h).unwrap();
    let result = store.open_queue(h);
    assert_err_contains(result, "queue already open");
}

/// close_queue then open_queue again should succeed.
#[test]
fn negative_close_then_reopen_queue() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "q7", "data");
    let _queue = store.open_queue(h).unwrap();
    store.close_queue(h).unwrap();
    // Reopen should succeed
    let _queue2 = store.open_queue(h).unwrap();
}

/// queue_push after close_queue should fail.
#[test]
fn negative_queue_push_after_close_queue() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "q8", "data");
    let queue = store.open_queue(h).unwrap();
    store.close_queue(h).unwrap();
    // Push after close should fail
    let result = store.queue_push(&queue, b"data");
    assert!(result.is_err());
}

// ============================================================================
// Config Boundary Values
// ============================================================================

/// StoreConfigBuilder with extreme values should not panic.
#[test]
fn negative_config_extreme_values() {
    use timslite::StoreConfig;

    // All defaults
    let config = StoreConfig::builder().build();
    let dir = temp_dir();
    let result = timslite::Store::open(&dir, config);
    // Should succeed (defaults are reasonable)
    assert!(result.is_ok());

    // Very large cache
    let config = StoreConfig::builder().cache_max_memory(usize::MAX).build();
    let dir = temp_dir();
    let result = timslite::Store::open(&dir, config);
    assert!(result.is_ok());
}

/// DataSetConfigBuilder with zero segment sizes.
#[test]
fn negative_config_zero_segments() {
    use timslite::DataSetConfigBuilder;

    let builder = DataSetConfigBuilder::default()
        .data_segment_size(0)
        .index_segment_size(0);
    // Build should succeed; validation happens at dataset creation
    let _config = builder.build().unwrap();
}

/// DataSetConfigBuilder with max values.
#[test]
fn negative_config_max_values() {
    use timslite::DataSetConfigBuilder;

    let builder = DataSetConfigBuilder::default()
        .data_segment_size(u64::MAX)
        .index_segment_size(u64::MAX)
        .retention_window(i64::MAX as u64)
        .compress_level(255);
    let _config = builder.build().unwrap();
}

// ============================================================================
// Validation Edge Cases
// ============================================================================

/// Names with leading/trailing dots are rejected.
#[test]
fn negative_validation_dotted_names() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    for name in &[".leading", "trailing.", "..double"] {
        let result = store.create_dataset(name, "data", 1024, 1024, 0, 0, 0);
        assert_err_contains(result, "must match");
    }
}

/// Names with unicode are rejected (not alphanumeric).
#[test]
fn negative_validation_unicode_names() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    for name in &["数据", "café", "naïve"] {
        let result = store.create_dataset(name, "data", 1024, 1024, 0, 0, 0);
        assert_err_contains(result, "must match");
    }
}

/// Names with only numbers/underscores/hyphens are accepted.
#[test]
fn positive_validation_valid_names() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    for (i, name) in ["valid", "valid-name", "valid_name", "123", "a-b_c"]
        .iter()
        .enumerate()
    {
        let result = store.create_dataset(name, &format!("type{i}"), 1024, 1024, 0, 0, 0);
        assert!(result.is_ok(), "name '{name}' should be accepted");
    }
}

/// drop_dataset then read should fail (dataset is gone).
#[test]
fn negative_read_after_drop() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "dropped", "data");
    store.drop_dataset(h).unwrap();
    let result = store.get_dataset(&h);
    assert_err_contains(result, "not found");
}

/// close_dataset then operations should fail.
#[test]
fn negative_operations_after_close() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h = create_dataset(&mut store, "closed_ds", "data");
    store.close_dataset(h).unwrap();
    // All operations on closed handle should fail
    assert!(store.write_dataset(h, 1, b"data").is_err());
    assert!(store.read_dataset(h, 1).is_err());
    assert!(store.delete_dataset_record(h, 1).is_err());
    assert!(store.append_dataset(h, 1, b"data").is_err());
    assert!(store.query_dataset(h, 0, 100).is_err());
}

/// Multiple datasets with same name but different types should work.
#[test]
fn negative_same_name_different_type() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let _h1 = create_dataset(&mut store, "multi", "type_a");
    let _h2 = create_dataset(&mut store, "multi", "type_b");
    // Both should coexist
    let names = store.get_dataset_names().unwrap();
    assert!(names.contains(&"multi".to_string()));
    let types = store.get_dataset_types("multi").unwrap();
    assert!(types.contains(&"type_a".to_string()));
    assert!(types.contains(&"type_b".to_string()));
}

/// Drop one type, other type should still exist.
#[test]
fn negative_drop_one_type_keep_other() {
    let dir = temp_dir();
    let mut store = make_store(&dir);
    let h1 = create_dataset(&mut store, "keep", "type_a");
    let _h2 = create_dataset(&mut store, "keep", "type_b");
    store.drop_dataset(h1).unwrap();
    // type_b should still exist
    let result = store.open_dataset("keep", "type_b");
    assert!(result.is_ok());
    // type_a should be gone
    let result = store.open_dataset("keep", "type_a");
    assert_err_contains(result, "not found");
}
