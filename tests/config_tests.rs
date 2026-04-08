use tempfile::tempdir;
use timslite::Config;

#[test]
fn test_config_creation() {
    let dir = tempdir().unwrap();
    let config = Config::new(dir.path());

    assert_eq!(config.data_dir, dir.path());
    assert!(config.index_dir.ends_with(".index"));
}

#[test]
fn test_config_validation() {
    let dir = tempdir().unwrap();
    let config = Config::new(dir.path());
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_file_sizes() {
    use timslite::types::DataType;

    let dir = tempdir().unwrap();
    let config = Config::new(dir.path());

    assert_eq!(config.file_size(DataType::Index), 16 * 1024 * 1024);
    assert_eq!(config.file_size(DataType::Wave), 64 * 1024 * 1024);
}

#[test]
fn test_config_builder() {
    let dir = tempdir().unwrap();
    let config = Config::new(dir.path())
        .set_compression_level(5)
        .set_expiration_days(30)
        .enable_wal(true);

    assert_eq!(config.compression_level, 5);
    assert_eq!(config.expiration_days, 30);
    assert!(config.enable_wal);
}

#[test]
fn test_config_custom_file_size() {
    use timslite::types::DataType;

    let dir = tempdir().unwrap();
    let config = Config::new(dir.path()).set_file_size(DataType::Wave, 128 * 1024 * 1024);

    assert_eq!(config.file_size(DataType::Wave), 128 * 1024 * 1024);
}
