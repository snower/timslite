//! Integration tests for lightweight read operations: read_exist, query_exist, read_length, query_length, query_length_iter.

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

// ─── read_exist tests ────────────────────────────────────────────────────────

#[test]
fn test_read_exist_existing_timestamp() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    // Write some data
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(100, b"hello").unwrap();
    ds.write(200, b"world").unwrap();
    drop(ds);

    // Check existence
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    assert!(ds.read_exist(100).unwrap());
    assert!(ds.read_exist(200).unwrap());
    assert!(!ds.read_exist(300).unwrap());
}

#[test]
fn test_read_exist_latest_timestamp() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    // No data yet
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    assert!(!ds.read_exist(-1).unwrap());
    drop(ds);

    // Write data
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(100, b"hello").unwrap();
    drop(ds);

    // Now -1 should exist
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    assert!(ds.read_exist(-1).unwrap());
}

#[test]
fn test_read_exist_empty_dataset() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    assert!(!ds.read_exist(1).unwrap());
    assert!(!ds.read_exist(-1).unwrap());
}

// ─── query_exist tests ────────────────────────────────────────────────────────

#[test]
fn test_query_exist_basic() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    // Write data at timestamps 1, 3, 5
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(1, b"a").unwrap();
    ds.write(3, b"b").unwrap();
    ds.write(5, b"c").unwrap();
    drop(ds);

    // Query existence for range [1, 7]
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let bitmap = ds.query_exist(1, 7).unwrap();

    // Bitmap should be: bits 0,2,4 set (timestamps 1,3,5)
    // Byte 0: 0b00010101 = 0x15
    assert_eq!(bitmap.len(), 1);
    assert_eq!(bitmap[0], 0x15);
}

#[test]
fn test_query_exist_empty_range() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();

    // Empty range
    let bitmap = ds.query_exist(10, 5).unwrap();
    assert!(bitmap.is_empty());
}

#[test]
fn test_query_exist_cross_byte_boundary() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    // Write data at timestamps 1 and 9
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(1, b"a").unwrap();
    ds.write(9, b"b").unwrap();
    drop(ds);

    // Query range [1, 16] - should span 2 bytes
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let bitmap = ds.query_exist(1, 16).unwrap();

    assert_eq!(bitmap.len(), 2);
    // Byte 0: bit 0 set (timestamp 1)
    assert_eq!(bitmap[0], 0x01);
    // Byte 1: bit 0 set (timestamp 9 = byte 1, bit 0)
    assert_eq!(bitmap[1], 0x01);
}

// ─── read_length tests ────────────────────────────────────────────────────────

#[test]
fn test_read_length_existing() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let data = b"hello world";
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(100, data).unwrap();
    drop(ds);

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let len = ds.read_length(100).unwrap();
    assert_eq!(len, Some(data.len() as u32));
}

#[test]
fn test_read_length_nonexistent() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let len = ds.read_length(100).unwrap();
    assert_eq!(len, None);
}

#[test]
fn test_read_length_latest() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let data = b"test data 123";
    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(100, data).unwrap();
    drop(ds);

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let len = ds.read_length(-1).unwrap();
    assert_eq!(len, Some(data.len() as u32));
}

// ─── query_length tests ────────────────────────────────────────────────────────

#[test]
fn test_query_length_basic() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(1, b"aaa").unwrap();
    ds.write(2, b"bb").unwrap();
    ds.write(3, b"c").unwrap();
    drop(ds);

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let result = ds.query_length(1, 3).unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], (1, 3));
    assert_eq!(result[1], (2, 2));
    assert_eq!(result[2], (3, 1));
}

#[test]
fn test_query_length_empty_range() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let result = ds.query_length(10, 5).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_query_length_sparse() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(1, b"a").unwrap();
    ds.write(5, b"bbbbb").unwrap();
    ds.write(10, b"cc").unwrap();
    drop(ds);

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let result = ds.query_length(1, 10).unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], (1, 1));
    assert_eq!(result[1], (5, 5));
    assert_eq!(result[2], (10, 2));
}

// ─── query_length_iter tests ──────────────────────────────────────────────────

#[test]
fn test_query_length_iter_basic() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(1, b"aaa").unwrap();
    ds.write(2, b"bb").unwrap();
    ds.write(3, b"c").unwrap();
    drop(ds);

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let iter = ds.query_length_iter(1, 3).unwrap();
    let result: Vec<(i64, u32)> = iter.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], (1, 3));
    assert_eq!(result[1], (2, 2));
    assert_eq!(result[2], (3, 1));
}

#[test]
fn test_query_length_iter_empty() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    let iter = ds.query_length_iter(10, 5).unwrap();
    let result: Vec<(i64, u32)> = iter.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_query_length_iter_matches_query_length() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    for i in 1..=20 {
        let data = vec![0u8; i as usize];
        ds.write(i, &data).unwrap();
    }
    drop(ds);

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();

    // Get results from both methods
    let vec_result = ds.query_length(1, 20).unwrap();
    let iter_result: Vec<(i64, u32)> = ds
        .query_length_iter(1, 20)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(vec_result, iter_result);
}

// ─── Store facade tests ──────────────────────────────────────────────────────

#[test]
fn test_store_dataset_read_exist() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(100, b"data").unwrap();
    drop(ds);

    assert!(store.dataset_read_exist(handle, 100).unwrap());
    assert!(!store.dataset_read_exist(handle, 200).unwrap());
}

#[test]
fn test_store_dataset_query_exist() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(1, b"a").unwrap();
    ds.write(3, b"b").unwrap();
    drop(ds);

    let bitmap = store.dataset_query_exist(handle, 1, 5).unwrap();
    assert_eq!(bitmap.len(), 1);
    // Bits 0 and 2 set
    assert_eq!(bitmap[0], 0x05);
}

#[test]
fn test_store_dataset_read_length() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(100, b"hello").unwrap();
    drop(ds);

    assert_eq!(store.dataset_read_length(handle, 100).unwrap(), Some(5));
    assert_eq!(store.dataset_read_length(handle, 200).unwrap(), None);
}

#[test]
fn test_store_dataset_query_length() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = store.get_dataset(&handle).unwrap();
    let mut ds = ds_arc.lock().unwrap();
    ds.write(1, b"aaa").unwrap();
    ds.write(2, b"bb").unwrap();
    drop(ds);

    let result = store.dataset_query_length(handle, 1, 5).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], (1, 3));
    assert_eq!(result[1], (2, 2));
}
