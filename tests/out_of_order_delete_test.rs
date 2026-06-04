//! Out-of-order writes, continuous index, delete lifecycle, and mixed operations tests.
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
fn t18_1_out_of_order_write() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("ooo_ds", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    let ds = store.open_dataset("ooo_ds", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"v1").unwrap();
        lock.write(200, b"v2").unwrap();
        lock.write(300, b"v3").unwrap();

        // Out-of-order writes — each replaces a real entry
        lock.write(100, b"v1_updated").unwrap();
        lock.write(200, b"v2_updated").unwrap();

        // Query should reflect latest data
        let entries = lock.query(100, 300, None).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].1, b"v1_updated");
        assert_eq!(entries[1].1, b"v2_updated");
        assert_eq!(entries[2].1, b"v3");
    }

    drop(arc);
    store.close().unwrap();
}

#[test]
fn t18_1b_out_of_order_write_continuous() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("ooo_ds", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 1, 0)
        .unwrap();

    let ds = store.open_dataset("ooo_ds", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"v1").unwrap();
        lock.write(150, b"v2").unwrap();

        // Out-of-order: replace real entry at 100
        lock.write(100, b"v1_new").unwrap();

        let entries = lock.query(100, 150, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].1, b"v1_new");
        assert_eq!(entries[1].1, b"v2");
    }

    drop(arc);
    store.close().unwrap();
}

#[test]
fn t18_2_delete_lifecycle() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset("del_ds", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    let ds = store.open_dataset("del_ds", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    {
        let mut lock = arc.lock().unwrap();
        lock.write(1, b"keep").unwrap();
        lock.write(2, b"delete_me").unwrap();
        lock.write(3, b"also_keep").unwrap();

        // Delete middle entry
        lock.delete(2).unwrap();

        // Deleted entry should not appear in query
        let entries = lock.query(1, 3, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].1, b"keep");
        assert_eq!(entries[1].1, b"also_keep");

        // Deleting already-deleted should error
        assert!(lock.delete(2).is_err());
    }

    drop(arc);
    store.close().unwrap();
}

#[test]
fn t18_3_mixed_operations() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "mixed_ds",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("mixed_ds", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    {
        let mut lock = arc.lock().unwrap();
        lock.write(1, b"first").unwrap();
        lock.write(3, b"third").unwrap();
        lock.write(5, b"fifth").unwrap();

        // Out-of-order overwrite
        lock.write(3, b"THIRD_UPDATED").unwrap();

        // Delete ts=1 and verify query returns only ts=3 and ts=5
        lock.delete(1).unwrap();
        let entries_after_del = lock.query(1, 5, None).unwrap();
        assert_eq!(entries_after_del.len(), 2);
    }
    drop(arc);
    store.close().unwrap();
}
