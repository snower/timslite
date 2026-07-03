use timslite_dotnet::{
    CreateDatasetOptions, DatasetConfig, StoreBridge,
    StoreConfig, TmslError,
};

fn temp_dir(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("timslite_dotnet_rust_{name}_{}", uuid()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_string_lossy().to_string()
}

fn uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{nanos:x}")
}

#[test]
fn store_config_default_roundtrip() {
    let config = StoreConfig::default();
    let inner = config.to_inner();
    assert_eq!(inner.flush_interval(), std::time::Duration::from_secs(15));
}

#[test]
fn store_config_custom_values() {
    let config = StoreConfig {
        flush_interval_secs: Some(10),
        idle_timeout_secs: Some(20),
        data_segment_size: Some(1024 * 1024),
        index_segment_size: Some(512 * 1024),
        compress_level: Some(6),
        cache_max_memory: Some(8 * 1024 * 1024),
        enable_background_thread: Some(false),
        enable_journal: Some(true),
        read_only: Some(false),
        ..Default::default()
    };
    let inner = config.to_inner();
    assert_eq!(inner.flush_interval(), std::time::Duration::from_secs(10));
    assert_eq!(inner.idle_timeout(), std::time::Duration::from_secs(20));
}

#[test]
fn dataset_config_apply_to_builder() {
    let ds_config = DatasetConfig {
        data_segment_size: Some(2 * 1024 * 1024),
        index_segment_size: Some(1024 * 1024),
        compress_level: Some(3),
        retention_window: Some(0),
        enable_journal: Some(false),
        ..Default::default()
    };
    let store_config = timslite::StoreConfig::default();
    let builder =
        ds_config.apply_to_builder(timslite::DataSetConfigBuilder::from_store(&store_config));
    let built = builder.build().unwrap();
    assert_eq!(built.data_segment_size(), 2 * 1024 * 1024);
    assert_eq!(built.index_segment_size(), 1024 * 1024);
}

#[test]
fn dataset_config_defaults_no_override() {
    let ds_config = DatasetConfig::default();
    let store_config = timslite::StoreConfig::default();
    let builder =
        ds_config.apply_to_builder(timslite::DataSetConfigBuilder::from_store(&store_config));
    let built = builder.build().unwrap();
    let default_built = timslite::DataSetConfigBuilder::from_store(&store_config).build().unwrap();
    assert_eq!(built.data_segment_size(), default_built.data_segment_size());
    assert_eq!(built.compress_level(), default_built.compress_level());
}

#[test]
fn error_conversion_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
    let rust_err = timslite::TmslError::Io(io_err);
    let bridge_err: TmslError = rust_err.into();
    match bridge_err {
        TmslError::Io { message } => assert!(message.contains("file missing")),
        _ => panic!("expected Io variant"),
    }
}

#[test]
fn error_conversion_already_exists() {
    let rust_err = timslite::TmslError::AlreadyExists("dataset exists".into());
    let bridge_err: TmslError = rust_err.into();
    match bridge_err {
        TmslError::AlreadyExists { message } => assert_eq!(message, "dataset exists"),
        _ => panic!("expected AlreadyExists variant"),
    }
}

#[test]
fn error_conversion_not_found() {
    let rust_err = timslite::TmslError::NotFound("not there".into());
    let bridge_err: TmslError = rust_err.into();
    match bridge_err {
        TmslError::NotFound { message } => assert_eq!(message, "not there"),
        _ => panic!("expected NotFound variant"),
    }
}

#[test]
fn error_conversion_segment_full() {
    let rust_err = timslite::TmslError::SegmentFull;
    let bridge_err: TmslError = rust_err.into();
    match bridge_err {
        TmslError::SegmentFull => {}
        _ => panic!("expected SegmentFull variant"),
    }
}

#[test]
fn error_conversion_invalid_magic() {
    let rust_err = timslite::TmslError::InvalidMagic;
    let bridge_err: TmslError = rust_err.into();
    match bridge_err {
        TmslError::InvalidMagic => {}
        _ => panic!("expected InvalidMagic variant"),
    }
}

#[test]
fn error_conversion_queue_closed() {
    let rust_err = timslite::TmslError::QueueClosed("queue gone".into());
    let bridge_err: TmslError = rust_err.into();
    match bridge_err {
        TmslError::QueueClosed { message } => assert_eq!(message, "queue gone"),
        _ => panic!("expected QueueClosed variant"),
    }
}

#[test]
fn store_bridge_closed_guard_is_closed() {
    let dir = temp_dir("closed_guard");
    let store = StoreBridge::open(dir, StoreConfig::default()).unwrap();
    assert!(!store.is_closed());
    store.close().unwrap();
    assert!(store.is_closed());
}

#[test]
fn store_bridge_closed_guard_is_read_only() {
    let dir = temp_dir("closed_ro");
    let store = StoreBridge::open(dir, StoreConfig::default()).unwrap();
    store.close().unwrap();
    match store.is_read_only() {
        Err(TmslError::StoreClosed { .. }) => {}
        other => panic!("expected StoreClosed, got: {other:?}"),
    }
}

#[test]
fn store_bridge_closed_guard_create_dataset() {
    let dir = temp_dir("closed_create");
    let store = StoreBridge::open(dir, StoreConfig::default()).unwrap();
    store.close().unwrap();
    match store.create_dataset("a".into(), "b".into(), CreateDatasetOptions::default()) {
        Err(TmslError::StoreClosed { .. }) => {}
        other => panic!("expected StoreClosed, got: {other:?}"),
    }
}

#[test]
fn store_bridge_closed_guard_open_dataset() {
    let dir = temp_dir("closed_open");
    let store = StoreBridge::open(dir, StoreConfig::default()).unwrap();
    store.close().unwrap();
    match store.open_dataset("a".into(), "b".into()) {
        Err(TmslError::StoreClosed { .. }) => {}
        Err(other) => panic!("expected StoreClosed error, got Err({other:?})"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[test]
fn store_bridge_closed_guard_drop_dataset() {
    let dir = temp_dir("closed_drop");
    let store = StoreBridge::open(dir, StoreConfig::default()).unwrap();
    store.close().unwrap();
    match store.drop_dataset("a".into(), "b".into()) {
        Err(TmslError::StoreClosed { .. }) => {}
        other => panic!("expected StoreClosed, got: {other:?}"),
    }
}

#[test]
fn store_bridge_closed_guard_get_dataset_names() {
    let dir = temp_dir("closed_names");
    let store = StoreBridge::open(dir, StoreConfig::default()).unwrap();
    store.close().unwrap();
    match store.get_dataset_names() {
        Err(TmslError::StoreClosed { .. }) => {}
        other => panic!("expected StoreClosed, got: {other:?}"),
    }
}

#[test]
fn store_bridge_closed_guard_inspect() {
    let dir = temp_dir("closed_inspect");
    let store = StoreBridge::open(dir, StoreConfig::default()).unwrap();
    store.close().unwrap();
    match store.inspect_dataset("a".into(), "b".into()) {
        Err(TmslError::StoreClosed { .. }) => {}
        other => panic!("expected StoreClosed, got: {other:?}"),
    }
}

#[test]
fn store_bridge_closed_guard_tick() {
    let dir = temp_dir("closed_tick");
    let store = StoreBridge::open(dir, StoreConfig::default()).unwrap();
    store.close().unwrap();
    match store.tick_background_tasks() {
        Err(TmslError::StoreClosed { .. }) => {}
        other => panic!("expected StoreClosed, got: {other:?}"),
    }
}

#[test]
fn store_bridge_close_is_idempotent() {
    let dir = temp_dir("close_idempotent");
    let store = StoreBridge::open(dir, StoreConfig::default()).unwrap();
    store.close().unwrap();
    store.close().unwrap();
    assert!(store.is_closed());
}

#[test]
fn iterator_exhaustion_returns_none() {
    let dir = temp_dir("iter_exhaust");
    let store = StoreBridge::open(dir.clone(), StoreConfig::default()).unwrap();
    store
        .create_dataset("iter".into(), "test".into(), CreateDatasetOptions::default())
        .unwrap();
    let ds = store
        .open_dataset("iter".into(), "test".into())
        .unwrap();

    for i in 1..=3 {
        ds.write(i * 100, format!("data_{i}").into_bytes()).unwrap();
    }

    let iter = ds.query_iter(100, 301).unwrap();
    let mut count = 0;
    loop {
        match iter.next() {
            Ok(Some(_)) => count += 1,
            Ok(None) => break,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert_eq!(count, 3);

    let result = iter.next();
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn iterator_collect_all_after_partial() {
    let dir = temp_dir("iter_collect");
    let store = StoreBridge::open(dir.clone(), StoreConfig::default()).unwrap();
    store
        .create_dataset("col".into(), "test".into(), CreateDatasetOptions::default())
        .unwrap();
    let ds = store
        .open_dataset("col".into(), "test".into())
        .unwrap();

    for i in 1..=5 {
        ds.write(i * 100, format!("r{i}").into_bytes()).unwrap();
    }

    let iter = ds.query_iter(100, 501).unwrap();
    let first = iter.next().unwrap().unwrap();
    assert_eq!(first.timestamp, 100);

    let remaining = iter.collect_all().unwrap();
    assert_eq!(remaining.len(), 4);
    assert_eq!(remaining[0].timestamp, 200);
    assert_eq!(remaining[3].timestamp, 500);
}

#[test]
fn iterator_collect_take() {
    let dir = temp_dir("iter_take");
    let store = StoreBridge::open(dir.clone(), StoreConfig::default()).unwrap();
    store
        .create_dataset("take".into(), "test".into(), CreateDatasetOptions::default())
        .unwrap();
    let ds = store
        .open_dataset("take".into(), "test".into())
        .unwrap();

    for i in 1..=5 {
        ds.write(i * 100, format!("t{i}").into_bytes()).unwrap();
    }

    let iter = ds.query_iter(100, 501).unwrap();
    let taken = iter.collect_take(2).unwrap();
    assert_eq!(taken.len(), 2);
    assert_eq!(taken[0].timestamp, 100);
    assert_eq!(taken[1].timestamp, 200);
}

#[test]
fn iterator_reverse() {
    let dir = temp_dir("iter_rev");
    let store = StoreBridge::open(dir.clone(), StoreConfig::default()).unwrap();
    store
        .create_dataset("rev".into(), "test".into(), CreateDatasetOptions::default())
        .unwrap();
    let ds = store
        .open_dataset("rev".into(), "test".into())
        .unwrap();

    for i in 1..=3 {
        ds.write(i * 100, format!("v{i}").into_bytes()).unwrap();
    }

    let iter = ds.query_iter(100, 301).unwrap();
    iter.reverse().unwrap();
    let first = iter.next().unwrap().unwrap();
    assert_eq!(first.timestamp, 300);
}

#[test]
fn iterator_skip() {
    let dir = temp_dir("iter_skip");
    let store = StoreBridge::open(dir.clone(), StoreConfig::default()).unwrap();
    store
        .create_dataset("skip".into(), "test".into(), CreateDatasetOptions::default())
        .unwrap();
    let ds = store
        .open_dataset("skip".into(), "test".into())
        .unwrap();

    for i in 1..=5 {
        ds.write(i * 100, format!("s{i}").into_bytes()).unwrap();
    }

    let iter = ds.query_iter(100, 501).unwrap();
    iter.skip(3).unwrap();
    let first = iter.next().unwrap().unwrap();
    assert_eq!(first.timestamp, 400);
}

#[test]
fn iterator_empty_range_returns_none() {
    let dir = temp_dir("iter_empty");
    let store = StoreBridge::open(dir.clone(), StoreConfig::default()).unwrap();
    store
        .create_dataset("empty".into(), "test".into(), CreateDatasetOptions::default())
        .unwrap();
    let ds = store
        .open_dataset("empty".into(), "test".into())
        .unwrap();

    let iter = ds.query_iter(1000, 2000).unwrap();
    let result = iter.next().unwrap();
    assert!(result.is_none());
}
