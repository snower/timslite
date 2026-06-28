//! Public Rust API shape tests.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let d = std::env::temp_dir().join("timslite_public_api");
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

#[test]
fn store_returns_dataset_for_direct_record_operations() {
    use timslite::{DataSetConfigBuilder, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let dataset = store
        .create_dataset_with_config(
            "public_api",
            "events",
            Some(
                DataSetConfigBuilder::from_store(store.config())
                    .data_segment_size(1024 * 1024)
                    .index_segment_size(64 * 1024),
            ),
        )
        .unwrap();

    dataset.write(1, b"one").unwrap();
    dataset.append(2, b"two").unwrap();
    assert_eq!(dataset.read(1).unwrap().unwrap().1, b"one");
    assert_eq!(dataset.read_latest().unwrap().unwrap().1, b"two");

    let reopened = store
        .open_dataset_by_identifier(dataset.identifier())
        .unwrap();
    assert_eq!(reopened.read(2).unwrap().unwrap().1, b"two");

    store.close().unwrap();
}

#[test]
fn queue_opens_from_dataset_object() {
    use timslite::{DataSetConfigBuilder, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let dataset = store
        .create_dataset_with_config(
            "queue_api",
            "events",
            Some(
                DataSetConfigBuilder::from_store(store.config())
                    .data_segment_size(1024 * 1024)
                    .index_segment_size(64 * 1024),
            ),
        )
        .unwrap();
    let queue = dataset.open_queue().unwrap();
    let consumer = queue.open_consumer("group").unwrap();

    let ts = queue.push(b"queued").unwrap();
    assert_eq!(ts, 1);
    assert_eq!(
        consumer.poll(Duration::from_millis(1)).unwrap().unwrap().1,
        b"queued"
    );

    store.close().unwrap();
}
