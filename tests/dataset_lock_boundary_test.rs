use std::thread;

use timslite::{Store, StoreConfig};

fn temp_dir() -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "timslite_dataset_lock_boundary_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn store_dataset_handle_serializes_direct_dataset_api_calls() {
    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("shared", "data", 1024 * 1024, 64 * 1024, 6, 0, 0)
        .unwrap();
    let handle = store.open_dataset("shared", "data").unwrap();
    let dataset = handle.clone();

    let writer = {
        let dataset = dataset.clone();
        thread::spawn(move || {
            for ts in 1_i64..=128 {
                dataset.write(ts, &ts.to_le_bytes()).unwrap();
            }
        })
    };

    let reader = {
        let dataset = dataset.clone();
        thread::spawn(move || {
            for _ in 0..128 {
                let _ = dataset.read_latest().unwrap();
            }
        })
    };

    writer.join().unwrap();
    reader.join().unwrap();

    let latest = dataset.read_latest().unwrap().unwrap();
    assert_eq!(latest.0, 128);
    assert_eq!(latest.1, 128_i64.to_le_bytes());
}
