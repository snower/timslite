use timslite::types::DataType;

#[test]
fn test_data_type_properties() {
    assert_eq!(DataType::Wave.default_file_size(), 64 * 1024 * 1024);
    assert_eq!(DataType::Measure.default_file_size(), 32 * 1024 * 1024);
    assert!(DataType::Wave.should_compress());
    assert!(!DataType::Index.should_compress());
}

#[test]
fn test_data_type_conversion() {
    assert_eq!(DataType::from_i32(1), Some(DataType::Wave));
    assert_eq!(DataType::from_i32(2), Some(DataType::Measure));
    assert_eq!(DataType::from_i32(99), None);
}

#[test]
fn test_data_type_display() {
    assert_eq!(format!("{}", DataType::Wave), "wave");
    assert_eq!(format!("{}", DataType::Measure), "measure");
}

#[test]
fn test_index_info() {
    use timslite::types::IndexInfo;

    let info = IndexInfo::new(100, 200, 1000);
    assert!(info.has_wave());
    assert!(info.has_measure());
    assert_eq!(info.timestamp, 1000);

    // Test serialization
    let bytes = info.to_bytes();
    let restored = IndexInfo::from_bytes(&bytes);
    assert_eq!(info.wave_offset, restored.wave_offset);
    assert_eq!(info.measure_offset, restored.measure_offset);
    assert_eq!(info.timestamp, restored.timestamp);
}

#[test]
fn test_data_record() {
    use timslite::types::DataRecord;

    let record = DataRecord::new(12345, vec![1, 2, 3, 4, 5]);
    assert_eq!(record.timestamp, 12345);
    assert_eq!(record.data.len(), 5);
}

#[test]
fn test_read_options() {
    use timslite::types::ReadOptions;

    let options = ReadOptions::default();
    assert_eq!(options.start_timestamp, 0);
    assert_eq!(options.end_timestamp, i64::MAX);
    assert_eq!(options.sampling_period, 0);
    assert!(options.decompress);
}

#[test]
fn test_write_options() {
    use timslite::types::WriteOptions;

    let options = WriteOptions::default();
    assert!(options.compress);
    assert_eq!(options.compression_level, 7);
}

#[test]
fn test_dataset_meta() {
    use timslite::types::{DataType, DatasetMeta};

    let meta = DatasetMeta::new("test".to_string(), DataType::Wave);
    assert_eq!(meta.name, "test");
    assert_eq!(meta.data_type, DataType::Wave);
    assert!(meta.created_at > 0);
}
