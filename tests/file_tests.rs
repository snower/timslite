use std::path::PathBuf;
use tempfile::tempdir;
use timslite::file::{filename_to_offset, offset_to_filename, FileHeader, MappedFile};
use timslite::types::DataType;

#[test]
fn test_file_header_creation() {
    let header = FileHeader::new(DataType::Wave, 1024);

    assert_eq!(header.version, 1);
    assert_eq!(header.data_type, DataType::Wave);
    assert_eq!(header.file_size, 1024);
    assert_eq!(header.compress_type, 1); // Wave should compress
}

#[test]
fn test_file_header_serialization() {
    let header = FileHeader::new(DataType::Wave, 1024);
    let bytes = header.to_bytes();

    let restored = FileHeader::from_bytes(&bytes).unwrap();
    assert_eq!(header.version, restored.version);
    assert_eq!(header.data_type, restored.data_type);
    assert_eq!(header.file_size, restored.file_size);
}

#[test]
fn test_offset_to_filename() {
    assert_eq!(offset_to_filename(0), "00000000000000000000");
    assert_eq!(offset_to_filename(12345), "00000000000000012345");
    assert_eq!(offset_to_filename(999999999), "00000000000999999999");
}

#[test]
fn test_filename_to_offset() {
    assert_eq!(filename_to_offset("00000000000000000000"), Some(0));
    assert_eq!(filename_to_offset("00000000000000012345"), Some(12345));
    assert_eq!(filename_to_offset("invalid"), None);
}

#[test]
fn test_mapped_file_create() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_file");

    let header = FileHeader::new(DataType::Wave, 1024 * 1024);
    let file = MappedFile::create(&path, header).unwrap();

    assert!(path.exists());
    assert!(!file.is_full());
}

#[test]
fn test_mapped_file_append() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_file");

    let header = FileHeader::new(DataType::Measure, 1024 * 1024);
    let file = MappedFile::create(&path, header).unwrap();

    let data = vec![1, 2, 3, 4, 5];
    let offset = file.append(&data).unwrap();

    assert!(offset >= 0);
    assert!(file.wrote_position() > 0);
}

#[test]
fn test_mapped_file_write_and_read() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_file");

    let header = FileHeader::new(DataType::Event, 1024 * 1024);
    let file = MappedFile::create(&path, header).unwrap();

    // Write data
    let data = vec![10, 20, 30, 40, 50];
    let offset = file.append(&data).unwrap();

    // Read back
    let read_data = file.read(offset as u64).unwrap();
    assert_eq!(data, read_data);
}

#[test]
fn test_mapped_file_close() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_file");

    let header = FileHeader::new(DataType::Wave, 1024 * 1024);
    let file = MappedFile::create(&path, header).unwrap();

    file.close().unwrap();
    // File should be closed successfully
}

#[test]
fn test_index_type_no_compression() {
    let header = FileHeader::new(DataType::Index, 1024);
    assert_eq!(header.compress_type, 0); // Index should not compress
}
