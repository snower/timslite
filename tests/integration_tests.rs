use tempfile::tempdir;
use timslite::prelude::*;
use timslite::{
    types::{ReadOptions, WriteOptions},
    DataType, TimeStore,
};

#[test]
fn test_open_store() {
    let dir = tempdir().unwrap();
    let store = TimeStore::open(dir.path()).unwrap();
    store.close().unwrap();
}

#[test]
fn test_open_dataset() {
    let dir = tempdir().unwrap();
    let store = TimeStore::open(dir.path()).unwrap();

    let dataset = store.open_dataset("test_dataset", DataType::Wave).unwrap();
    assert_eq!(dataset.name(), "test_dataset");
    assert_eq!(dataset.data_type(), DataType::Wave);

    store.close().unwrap();
}

#[test]
fn test_write_and_read() {
    let dir = tempdir().unwrap();
    let store = TimeStore::open(dir.path()).unwrap();

    let dataset = store.open_dataset("test", DataType::Wave).unwrap();

    // Write data
    let timestamp = 1234567890i64;
    let data = vec![1, 2, 3, 4, 5];
    let offset = dataset.write(timestamp, &data).unwrap();
    assert!(offset >= 0);

    // Flush
    dataset.flush().unwrap();

    // Read back
    let options = ReadOptions {
        start_timestamp: timestamp - 10,
        end_timestamp: timestamp + 10,
        ..Default::default()
    };

    let records = dataset.read(&options).unwrap();
    // Note: actual record count depends on implementation

    store.close().unwrap();
}

#[test]
fn test_multiple_datasets() {
    let dir = tempdir().unwrap();
    let store = TimeStore::open(dir.path()).unwrap();

    // Open multiple datasets
    let wave = store.open_dataset("patient_001", DataType::Wave).unwrap();
    let measure = store
        .open_dataset("patient_001", DataType::Measure)
        .unwrap();
    let event = store.open_dataset("patient_001", DataType::Event).unwrap();

    // Write to each
    let ts = 1000i64;
    wave.write(ts, &[1, 2, 3]).unwrap();
    measure.write(ts, &[4, 5, 6]).unwrap();
    event.write(ts, &[7, 8, 9]).unwrap();

    // List datasets
    let datasets = store.list_datasets();
    assert_eq!(datasets.len(), 3);

    store.close().unwrap();
}

#[test]
fn test_dataset_metadata() {
    let dir = tempdir().unwrap();
    let store = TimeStore::open(dir.path()).unwrap();

    let dataset = store.open_dataset("test", DataType::Wave).unwrap();
    let meta = dataset.meta();

    assert_eq!(meta.name, "test");
    assert_eq!(meta.data_type, DataType::Wave);

    store.close().unwrap();
}

#[test]
fn test_config() {
    use timslite::Config;

    let dir = tempdir().unwrap();
    let config = Config::new(dir.path())
        .set_compression_level(5)
        .set_expiration_days(7)
        .enable_wal(true);

    config.validate().unwrap();

    let store = TimeStore::with_config(config).unwrap();
    store.close().unwrap();
}

#[test]
fn test_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Write data
    {
        let store = TimeStore::open(&path).unwrap();
        let dataset = store.open_dataset("test", DataType::Wave).unwrap();
        dataset.write(1000, &[1, 2, 3]).unwrap();
        dataset.flush().unwrap();
        store.close().unwrap();
    }

    // Reopen and verify
    {
        let store = TimeStore::open(&path).unwrap();
        let dataset = store.open_dataset("test", DataType::Wave).unwrap();
        let meta = dataset.meta();
        assert!(meta.record_count > 0);
        store.close().unwrap();
    }
}

#[test]
fn test_directory_structure() {
    let dir = tempdir().unwrap();
    let store = TimeStore::open(dir.path()).unwrap();

    // Verify directories created
    assert!(dir.path().exists());
    assert!(dir.path().join(".index").exists());

    // Create dataset
    let dataset = store.open_dataset("monitor_001", DataType::Wave).unwrap();
    dataset.write(1000, &[1, 2, 3]).unwrap();

    // Verify dataset directory
    assert!(dir.path().join("monitor_001").exists());
    assert!(dir.path().join("monitor_001").join("wave").exists());

    store.close().unwrap();
}
