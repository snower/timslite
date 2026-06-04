//! Correction write tests: overwrite same-size and resize.
use std::fs;
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    let d = std::env::temp_dir().join("timslite_integration");
    fs::create_dir_all(&d).unwrap();
    d.join(format!(
        "test_{:?}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn t17_1_correction_write_same_size() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("cw", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    let ds = store.open_dataset("cw", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write original data
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"alpha").unwrap();
        lock.write(200, b"beta.").unwrap();

        // Correction write: same size
        lock.write(200, b"BETA.").unwrap();

        let entries = lock.query(100, 200, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].1, b"alpha");
        assert_eq!(entries[1].1, b"BETA.");
    }

    store.close().unwrap();
}

#[test]
fn t17_2_correction_write_resize_reopen() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "cw_resize",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cw_resize", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write original (small)
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"tiny").unwrap();

        // Correction: resize to larger
        let bigger = vec![0xABu8; 300];
        lock.write(100, &bigger).unwrap();

        // Correction: resize back to smaller
        lock.write(100, b"x").unwrap();

        let entries = lock.query(100, 100, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"x");
    }
    drop(arc);
    store.close().unwrap();
}
