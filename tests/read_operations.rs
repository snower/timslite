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

// 閳光偓閳光偓閳光偓 read_exist tests 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

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
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(100, b"hello").unwrap();
    ds.write(200, b"world").unwrap();
    drop(ds);

    // Check existence
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
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
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    assert!(!ds.read_exist(-1).unwrap());
    drop(ds);

    // Negative timestamps are relative offsets from latest_written_timestamp.
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    assert!(ds.write(-1, b"minus-one").is_err());
    ds.write(100, b"hello").unwrap();
    drop(ds);

    // -1 resolves to latest_written_timestamp.
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    assert!(ds.read_exist(-1).unwrap());
    assert!(ds.read_exist(100).unwrap());
}

#[test]
fn test_negative_read_arguments_resolve_from_latest_timestamp() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let ds = store.open_dataset("ds", "type").unwrap();

    ds.write(10, b"ten").unwrap();
    ds.write(11, b"eleven").unwrap();
    ds.write(12, b"twelve").unwrap();

    assert_eq!(ds.read(-1).unwrap(), Some((12, b"twelve".to_vec())));
    assert_eq!(ds.read(-2).unwrap(), Some((11, b"eleven".to_vec())));
    assert_eq!(ds.read(-3).unwrap(), Some((10, b"ten".to_vec())));
    assert_eq!(ds.read(-4).unwrap(), None);

    assert!(ds.read_exist(-1).unwrap());
    assert!(ds.read_exist(-2).unwrap());
    assert_eq!(ds.read_length(-3).unwrap(), Some(3));
    assert_eq!(ds.read_length(-4).unwrap(), None);
}

#[test]
fn test_negative_query_arguments_resolve_from_latest_timestamp() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let ds = store.open_dataset("ds", "type").unwrap();

    ds.write(20, b"twenty").unwrap();
    ds.write(21, b"twenty-one").unwrap();
    ds.write(22, b"twenty-two").unwrap();

    let rows = ds.query(-3, -1).unwrap();
    assert_eq!(
        rows,
        vec![
            (20, b"twenty".to_vec()),
            (21, b"twenty-one".to_vec()),
            (22, b"twenty-two".to_vec())
        ]
    );

    let bitmap = ds.query_exist(-3, -1).unwrap();
    assert_eq!(bitmap.len(), 1);
    assert_eq!(bitmap[0] & 0b0000_0111, 0b0000_0111);

    let lengths = ds.query_length(-2, -1).unwrap();
    assert_eq!(lengths, vec![(21, 10), (22, 10)]);

    let iter_rows = ds.query_iter(-2, -1).unwrap().collect_all().unwrap();
    assert_eq!(
        iter_rows,
        vec![(21, b"twenty-one".to_vec()), (22, b"twenty-two".to_vec())]
    );

    let length_iter_rows = ds.query_length_iter(-2, -1).unwrap().collect_all().unwrap();
    assert_eq!(length_iter_rows, vec![(21, 10), (22, 10)]);
}

#[test]
fn test_negative_write_timestamps_are_rejected() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let ds = store.open_dataset("ds", "type").unwrap();

    assert!(ds.write(-1, b"minus-one").is_err());
    assert!(ds.append(-1, b"minus-one").is_err());
    assert!(ds.delete(-1).is_err());

    ds.write(0, b"zero").unwrap();
    assert_eq!(ds.read(0).unwrap(), Some((0, b"zero".to_vec())));
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    assert!(!ds.read_exist(1).unwrap());
    assert!(!ds.read_exist(-1).unwrap());
}

// 閳光偓閳光偓閳光偓 query_exist tests 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

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
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(1, b"a").unwrap();
    ds.write(3, b"b").unwrap();
    ds.write(5, b"c").unwrap();
    drop(ds);

    // Query existence for range [1, 7]
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();

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
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(1, b"a").unwrap();
    ds.write(9, b"b").unwrap();
    drop(ds);

    // Query range [1, 16] - should span 2 bytes
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    let bitmap = ds.query_exist(1, 16).unwrap();

    assert_eq!(bitmap.len(), 2);
    // Byte 0: bit 0 set (timestamp 1)
    assert_eq!(bitmap[0], 0x01);
    // Byte 1: bit 0 set (timestamp 9 = byte 1, bit 0)
    assert_eq!(bitmap[1], 0x01);
}

#[test]
fn test_query_exist_excludes_expired_timestamps() {
    use timslite::{DataSetConfig, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let config = DataSetConfig::builder().retention_window(50);
    store
        .create_dataset_with_config("ds", "type", Some(config))
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(100, b"expired").unwrap();
    ds.write(160, b"visible").unwrap();

    let bitmap = ds.query_exist(100, 160).unwrap();
    assert_eq!(bitmap.len(), 8);
    assert_eq!(bitmap[0] & 0x01, 0, "timestamp 100 is expired");
    assert_ne!(bitmap[7] & 0x10, 0, "timestamp 160 is visible");
}

#[test]
fn test_query_exist_rejects_bitmap_larger_than_4mib() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    let max_bitmap_bytes = 4usize * 1024 * 1024;
    let too_many_timestamps = (max_bitmap_bytes as i64) * 8 + 1;
    let err = ds.query_exist(0, too_many_timestamps - 1).unwrap_err();
    assert!(
        err.to_string().contains("query_exist bitmap"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_public_query_length_iter_reads_from_source_cursor() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds = handle.clone();
    ds.write(10, b"ten").unwrap();
    ds.write(20, b"twenty").unwrap();

    let mut iter = ds.query_length_iter(10, 20).unwrap();
    ds.close().unwrap();

    let err = iter
        .next()
        .expect("source iterator should attempt a read")
        .unwrap_err();
    assert!(
        err.to_string().contains("is closed"),
        "unexpected error: {err}"
    );
}

// 閳光偓閳光偓閳光偓 read_length tests 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

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
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(100, data).unwrap();
    drop(ds);

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
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
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(99, b"previous").unwrap();
    ds.write(100, data).unwrap();
    drop(ds);

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    let len = ds.read_length(-1).unwrap();
    assert_eq!(len, Some(data.len() as u32));
    let len = ds.read_length(-2).unwrap();
    assert_eq!(len, Some("previous".len() as u32));
    let len = ds.read_length(100).unwrap();
    assert_eq!(len, Some(data.len() as u32));
}

// 閳光偓閳光偓閳光偓 query_length tests 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

#[test]
fn test_query_length_basic() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(1, b"aaa").unwrap();
    ds.write(2, b"bb").unwrap();
    ds.write(3, b"c").unwrap();
    drop(ds);

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(1, b"a").unwrap();
    ds.write(5, b"bbbbb").unwrap();
    ds.write(10, b"cc").unwrap();
    drop(ds);

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    let result = ds.query_length(1, 10).unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], (1, 1));
    assert_eq!(result[1], (5, 5));
    assert_eq!(result[2], (10, 2));
}

// 閳光偓閳光偓閳光偓 query_length_iter tests 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

#[test]
fn test_query_length_iter_basic() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(1, b"aaa").unwrap();
    ds.write(2, b"bb").unwrap();
    ds.write(3, b"c").unwrap();
    drop(ds);

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    for i in 1..=20 {
        let data = vec![0u8; i as usize];
        ds.write(i, &data).unwrap();
    }
    drop(ds);

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();

    // Get results from both methods
    let vec_result = ds.query_length(1, 20).unwrap();
    let iter_result: Vec<(i64, u32)> = ds
        .query_length_iter(1, 20)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(vec_result, iter_result);
}

// 閳光偓閳光偓閳光偓 Store facade tests 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

#[test]
fn test_store_dataset_read_exist() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(100, b"data").unwrap();
    drop(ds);

    assert!(handle.read_exist(100).unwrap());
    assert!(!handle.read_exist(200).unwrap());
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(1, b"a").unwrap();
    ds.write(3, b"b").unwrap();
    drop(ds);

    let bitmap = handle.query_exist(1, 5).unwrap();
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(100, b"hello").unwrap();
    drop(ds);

    assert_eq!(handle.read_length(100).unwrap(), Some(5));
    assert_eq!(handle.read_length(200).unwrap(), None);
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

    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(1, b"aaa").unwrap();
    ds.write(2, b"bb").unwrap();
    drop(ds);

    let result = handle.query_length(1, 5).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], (1, 3));
    assert_eq!(result[1], (2, 2));
}

// 閳光偓閳光偓閳光偓 Read Operations deleted record tests (P1-R-1~4) 閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓

#[test]
fn test_read_exist_deleted_timestamp_returns_false_for_filler() {
    // P1-6: read_exist reports current visible data existence, not raw index entry presence.
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    // Write data then delete it
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(100, b"to_be_deleted").unwrap();

    // Verify it exists before delete
    assert!(ds.read_exist(100).unwrap());

    // Delete the record (creates filler entry)
    ds.delete(100).unwrap();

    // After delete, read_exist should return false because filler is not visible data.
    assert!(
        !ds.read_exist(100).unwrap(),
        "deleted timestamp should return false for read_exist"
    );
    drop(ds);

    store.close().unwrap();
}

#[test]
fn test_read_length_deleted_timestamp_returns_none() {
    // P1-R-2: read_length(deleted_ts) should return None
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    // Write data then delete it
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(100, b"to_be_deleted").unwrap();

    // Verify length before delete
    let len = ds.read_length(100).unwrap();
    assert_eq!(len, Some(13));

    // Delete the record
    ds.delete(100).unwrap();

    // After delete, read_length should return None
    let len = ds.read_length(100).unwrap();
    assert!(
        len.is_none(),
        "deleted timestamp should return None for read_length"
    );
    drop(ds);

    store.close().unwrap();
}

#[test]
fn test_query_exist_excludes_deleted_timestamps_as_fillers() {
    // P1-6: query_exist reports current visible data existence, not raw index entry presence.
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    // Write multiple records and delete one
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(1, b"aaa").unwrap();
    ds.write(2, b"bbb").unwrap();
    ds.write(3, b"ccc").unwrap();

    // Delete timestamp 2 (creates filler entry)
    ds.delete(2).unwrap();

    // query_exist returns bit=1 only for visible data.
    let bitmap = ds.query_exist(1, 3).unwrap();
    assert_eq!(bitmap.len(), 1, "should have 1 byte for 3 timestamps");

    let ts1_exists = (bitmap[0] & 0x01) != 0;
    let ts2_exists = (bitmap[0] & 0x02) != 0;
    let ts3_exists = (bitmap[0] & 0x04) != 0;

    assert!(ts1_exists, "timestamp 1 should exist");
    assert!(!ts2_exists, "timestamp 2 should not exist after delete");
    assert!(ts3_exists, "timestamp 3 should exist");
    drop(ds);

    store.close().unwrap();
}

#[test]
fn test_query_length_skips_deleted_timestamps() {
    // P1-R-4: query_length should skip deleted timestamps
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("ds", "type").unwrap();

    // Write multiple records and delete one
    let ds_arc = handle.clone();
    let ds = ds_arc.clone();
    ds.write(1, b"aaa").unwrap(); // length 3
    ds.write(2, b"bb").unwrap(); // length 2
    ds.write(3, b"cccc").unwrap(); // length 4

    // Delete timestamp 2
    ds.delete(2).unwrap();

    // query_length should skip deleted timestamps
    let length_results = ds.query_length(1, 3).unwrap();
    assert_eq!(length_results.len(), 2, "should have 2 records");
    assert_eq!(length_results[0], (1, 3), "first should be (1, 3)");
    assert_eq!(length_results[1], (3, 4), "second should be (3, 4)");
    drop(ds);

    store.close().unwrap();
}

#[test]
fn query_length_iter_does_not_reuse_mutable_raw_hot_block_after_write() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let ds = store.open_dataset("ds", "type").unwrap();

    ds.write(1, b"aaa").unwrap();
    ds.write(2, b"bbb").unwrap();

    let mut iter = ds.query_length_iter(1, 2).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (1, 3));

    ds.write(2, b"corrected").unwrap();

    assert_eq!(
        iter.next().unwrap().unwrap(),
        (2, "corrected".len() as u32),
        "iterator hot cache must not keep mutable pending raw block data across writes"
    );
    assert!(iter.next().is_none());

    store.close().unwrap();
}

#[test]
fn query_length_iter_reads_current_index_entry_after_correction() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let ds = store.open_dataset("ds", "type").unwrap();

    ds.write(1, b"one").unwrap();
    ds.write(2, b"old").unwrap();
    ds.write(3, b"three").unwrap();

    let mut iter = ds.query_length_iter(1, 3).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (1, 3));

    ds.write(2, b"corrected").unwrap();

    assert_eq!(iter.next().unwrap().unwrap(), (2, 9));
    assert_eq!(iter.next().unwrap().unwrap(), (3, 5));
    assert!(iter.next().is_none());

    store.close().unwrap();
}

#[test]
fn query_length_iter_skips_entry_deleted_after_iterator_creation() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let ds = store.open_dataset("ds", "type").unwrap();

    ds.write(1, b"one").unwrap();
    ds.write(2, b"two").unwrap();
    ds.write(3, b"three").unwrap();

    let mut iter = ds.query_length_iter(1, 3).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (1, 3));

    ds.delete(2).unwrap();

    assert_eq!(iter.next().unwrap().unwrap(), (3, 5));
    assert!(iter.next().is_none());

    store.close().unwrap();
}

#[test]
fn query_length_iter_reverse_skip_and_collect_take_chain() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 1, 0)
        .unwrap();
    let ds = store.open_dataset("ds", "type").unwrap();

    ds.write(10, b"aa").unwrap();
    ds.write(20, b"bbbb").unwrap();
    ds.write(30, b"cccccc").unwrap();
    ds.write(40, b"dddddddd").unwrap();
    ds.write(50, b"eeeeeeeeee").unwrap();
    ds.delete(40).unwrap();

    let rows = ds
        .query_length_iter(10, 50)
        .unwrap()
        .reverse()
        .skip(1)
        .collect_take(2)
        .unwrap();

    assert_eq!(rows, vec![(30, 6), (20, 4)]);

    store.close().unwrap();
}
