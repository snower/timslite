//! Correction write tests: overwrite same-size and resize.
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

        let entries = lock.query(100, 200).unwrap();
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

        let entries = lock.query(100, 100).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"x");
    }
    drop(arc);
    store.close().unwrap();
}

#[test]
fn t17_3_correction_write_on_sealed_compressed_block() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "cw_sealed",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cw_sealed", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write enough data to trigger block sealing and compression
    // BLOCK_MAX_SIZE is 64KB, so write multiple records to overflow
    let big_data = vec![0xAAu8; 10_000]; // 10KB per record
    {
        let mut lock = arc.lock().unwrap();
        // Write 10 records of 10KB each = 100KB total, will seal multiple blocks
        for i in 1..=10i64 {
            lock.write(i, &big_data).unwrap();
        }
        // At this point, earlier blocks should be sealed and compressed

        // Correction write on a timestamp that's in a sealed+compressed block
        let new_data = vec![0xBBu8; 10_000];
        lock.write(1, &new_data).unwrap();

        // Verify the correction succeeded
        let (ts, data) = lock.read(1).unwrap().unwrap();
        assert_eq!(ts, 1);
        assert_eq!(data, vec![0xBBu8; 10_000]);

        // Verify other records are still intact
        for i in 2..=10i64 {
            let (ts, data) = lock.read(i).unwrap().unwrap();
            assert_eq!(ts, i);
            assert_eq!(data, big_data);
        }
    }

    store.close().unwrap();
}
