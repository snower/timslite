//! Iterator tests: cross-segment, cross-block queries.
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

/// P0-I-1: Iterator cross-segment query
///
/// Write enough data to span multiple data segments.
/// Query across segment boundaries and verify all records are returned.
#[test]
fn t33_1_iterator_cross_segment() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    // Use small data segment size to force multiple segments
    store
        .create_dataset(
            "cross_seg",
            "data",
            256 * 1024, // 256KB data segment size
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cross_seg", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write enough data to span multiple segments
    // 256KB segment / 4KB per record = ~64 records per segment
    // Write 200 records to ensure at least 3 segments
    {
        let lock = arc.clone();
        for i in 0..200i64 {
            let data = vec![0xABu8; 4096]; // 4KB per record
            lock.write(i * 10 + 1, &data).unwrap();
        }
    }

    // Query across all segments
    {
        let lock = arc.clone();
        let entries = lock.query(1, 2001).unwrap();
        assert_eq!(entries.len(), 200, "should return all 200 records");

        // Verify ordering and data
        for (i, (ts, data)) in entries.iter().enumerate() {
            assert_eq!(*ts, i as i64 * 10 + 1);
            assert_eq!(data.len(), 4096);
            assert!(data.iter().all(|&b| b == 0xAB));
        }
    }

    // Query a range that spans segment boundary
    {
        let lock = arc.clone();
        // Query records 501..=1501 (inclusive, should span multiple segments)
        let entries = lock.query(501, 1501).unwrap();
        assert_eq!(
            entries.len(),
            101,
            "should return 101 records (inclusive range)"
        );

        for (ts, _) in &entries {
            assert!(*ts >= 501 && *ts <= 1501);
        }
    }

    store.close().unwrap();
}

/// P0-I-2: Iterator cross-block query
///
/// Write enough data to span multiple blocks within a segment.
/// Query across block boundaries.
#[test]
fn t33_2_iterator_cross_block() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "cross_block",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cross_block", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write enough data to span multiple blocks
    // Block max size is 64KB, each record is 4KB
    // So ~16 records per block
    // Write 50 records to ensure at least 3 blocks
    {
        let lock = arc.clone();
        for i in 0..50i64 {
            let data = vec![0xCDu8; 4096]; // 4KB per record
            lock.write(i * 10 + 1, &data).unwrap();
        }
    }

    // Query all records
    {
        let lock = arc.clone();
        let entries = lock.query(1, 501).unwrap();
        assert_eq!(entries.len(), 50, "should return all 50 records");

        for (i, (ts, data)) in entries.iter().enumerate() {
            assert_eq!(*ts, i as i64 * 10 + 1);
            assert_eq!(data.len(), 4096);
            assert!(data.iter().all(|&b| b == 0xCD));
        }
    }

    // Query a range that spans block boundary
    {
        let lock = arc.clone();
        // Query records 101..=301 (inclusive, should span multiple blocks)
        let entries = lock.query(101, 301).unwrap();
        assert_eq!(
            entries.len(),
            21,
            "should return 21 records (inclusive range)"
        );

        for (ts, _) in &entries {
            assert!(*ts >= 101 && *ts <= 301);
        }
    }

    store.close().unwrap();
}

/// P0-I-3: Iterator with empty range
///
/// Query with start > end should return empty.
#[test]
fn t33_3_iterator_empty_range() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "empty_range",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("empty_range", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write some data
    {
        let lock = arc.clone();
        lock.write(100, b"data_100").unwrap();
        lock.write(200, b"data_200").unwrap();
        lock.write(300, b"data_300").unwrap();
    }

    // Query with start > end should return empty
    {
        let lock = arc.clone();
        let entries = lock.query(300, 100).unwrap();
        assert_eq!(entries.len(), 0, "start > end should return empty");
    }

    // Query with range that has no data
    {
        let lock = arc.clone();
        let entries = lock.query(500, 600).unwrap();
        assert_eq!(entries.len(), 0, "range with no data should return empty");
    }

    store.close().unwrap();
}

/// P0-I-4: Iterator with single record
///
/// Query that matches exactly one record.
#[test]
fn t33_4_iterator_single_record() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "single_rec",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("single_rec", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write one record
    {
        let lock = arc.clone();
        lock.write(100, b"only_record").unwrap();
    }

    // Query exact timestamp
    {
        let lock = arc.clone();
        let entries = lock.query(100, 100).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"only_record");
    }

    // Query range containing the record
    {
        let lock = arc.clone();
        let entries = lock.query(50, 150).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 100);
    }

    store.close().unwrap();
}

/// P1-I-5: Iterator with deleted records
///
/// Query should skip deleted records.
#[test]
fn t33_5_iterator_skip_deleted() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "skip_deleted",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("skip_deleted", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write records
    {
        let lock = arc.clone();
        lock.write(100, b"keep_100").unwrap();
        lock.write(200, b"delete_200").unwrap();
        lock.write(300, b"keep_300").unwrap();
        lock.write(400, b"delete_400").unwrap();
        lock.write(500, b"keep_500").unwrap();
    }

    // Delete some records
    {
        let lock = arc.clone();
        lock.delete(200).unwrap();
        lock.delete(400).unwrap();
    }

    // Query should skip deleted records
    {
        let lock = arc.clone();
        let entries = lock.query(100, 500).unwrap();
        assert_eq!(entries.len(), 3, "should return 3 non-deleted records");

        let timestamps: Vec<i64> = entries.iter().map(|(ts, _)| *ts).collect();
        assert_eq!(timestamps, vec![100, 300, 500]);
    }

    store.close().unwrap();
}

/// P1-I-6: Iterator with correction writes
///
/// Query should return corrected data, not original.
#[test]
fn t33_6_iterator_correction_writes() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "correction",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("correction", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write original data
    {
        let lock = arc.clone();
        lock.write(100, b"original_100").unwrap();
        lock.write(200, b"original_200").unwrap();
        lock.write(300, b"original_300").unwrap();
    }

    // Correction write
    {
        let lock = arc.clone();
        lock.write(200, b"corrected_200").unwrap();
    }

    // Query should return corrected data
    {
        let lock = arc.clone();
        let entries = lock.query(100, 300).unwrap();
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"original_100");

        assert_eq!(entries[1].0, 200);
        assert_eq!(entries[1].1, b"corrected_200");

        assert_eq!(entries[2].0, 300);
        assert_eq!(entries[2].1, b"original_300");
    }

    store.close().unwrap();
}
