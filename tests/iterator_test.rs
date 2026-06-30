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
    let arc = ds.clone();

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
    let arc = ds.clone();

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
    let arc = ds.clone();

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
    let arc = ds.clone();

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
    let arc = ds.clone();

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
    let arc = ds.clone();

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

/// P1-I-7: Iterator across idle-closed segment
///
/// Write data, trigger idle-close, then query across the closed segment.
/// The query should transparently re-open the segment and return all data.
#[test]
fn t33_7_closed_segment_transparent_reopen() {
    use std::time::Duration;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .idle_timeout(Duration::from_millis(50))
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "closed_seg",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("closed_seg", "data").unwrap();
    let arc = ds.clone();

    // Write records in the first batch
    {
        let lock = arc.clone();
        for i in 0..20i64 {
            lock.write(i * 10 + 1, &[0xAA; 256]).unwrap();
        }
    }

    // Drop arc to release the dataset reference before idle-close
    drop(arc);

    // Wait for idle timeout to expire, then trigger idle-close
    std::thread::sleep(Duration::from_millis(100));
    let _ = store.tick_background_tasks().unwrap();

    // Write more records (this will re-open the dataset and create new data)
    let arc = ds.clone();
    {
        let lock = arc.clone();
        for i in 20..40i64 {
            lock.write(i * 10 + 1, &[0xBB; 256]).unwrap();
        }
    }

    // Query across the entire range including the closed segment data
    {
        let lock = arc.clone();
        let entries = lock.query(1, 391).unwrap();
        assert_eq!(
            entries.len(),
            40,
            "should return all 40 records after idle-close reopen"
        );

        // Verify first batch (from closed segment)
        for (i, (ts, data)) in entries.iter().enumerate().take(20) {
            assert_eq!(*ts, i as i64 * 10 + 1);
            assert!(data.iter().all(|&b| b == 0xAA));
        }

        // Verify second batch (from new data)
        for (i, (ts, data)) in entries.iter().enumerate().take(40).skip(20) {
            assert_eq!(*ts, i as i64 * 10 + 1);
            assert!(data.iter().all(|&b| b == 0xBB));
        }
    }

    store.close().unwrap();
}

/// P1-I-8: Iterator with in-memory pending block data
///
/// Write data without flushing, then query immediately.
/// The query should include data from the in-memory pending block.
#[test]
fn t33_8_in_memory_pending_block_query() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "pending_q",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("pending_q", "data").unwrap();
    let arc = ds.clone();

    // Write some data and flush it to disk
    {
        let lock = arc.clone();
        for i in 0..10i64 {
            lock.write(i * 10 + 1, &[0xCC; 128]).unwrap();
        }
        lock.flush().unwrap();
    }

    // Write more data WITHOUT flushing (stays in pending block)
    {
        let lock = arc.clone();
        for i in 10..20i64 {
            lock.write(i * 10 + 1, &[0xDD; 128]).unwrap();
        }
        // Intentionally NOT flushing
    }

    // Query should include both flushed and unflushed data
    {
        let lock = arc.clone();
        let entries = lock.query(1, 191).unwrap();
        assert_eq!(
            entries.len(),
            20,
            "query should include both flushed and pending data"
        );

        // Verify flushed records
        for (i, (ts, data)) in entries.iter().enumerate().take(10) {
            assert_eq!(*ts, i as i64 * 10 + 1);
            assert!(data.iter().all(|&b| b == 0xCC));
        }

        // Verify pending (unflushed) records
        for (i, (ts, data)) in entries.iter().enumerate().take(20).skip(10) {
            assert_eq!(*ts, i as i64 * 10 + 1);
            assert!(data.iter().all(|&b| b == 0xDD));
        }
    }

    // Query a range that only covers pending data
    {
        let lock = arc.clone();
        let entries = lock.query(101, 191).unwrap();
        assert_eq!(
            entries.len(),
            10,
            "query on pending-only range should return 10 records"
        );
        for (ts, data) in &entries {
            assert!(*ts >= 101 && *ts <= 191);
            assert!(data.iter().all(|&b| b == 0xDD));
        }
    }

    store.close().unwrap();
}

/// P1-I-9: Iterator after data modification
///
/// Query initial data, write more records, then query again.
/// The second query should include the newly written data.
#[test]
fn t33_9_query_after_data_modification() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "mod_iter",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("mod_iter", "data").unwrap();
    let arc = ds.clone();

    // Write initial batch
    {
        let lock = arc.clone();
        for i in 0..10i64 {
            lock.write(i * 10 + 1, &[0x11; 64]).unwrap();
        }
    }

    // First query: should return 10 records
    {
        let lock = arc.clone();
        let entries = lock.query(1, 91).unwrap();
        assert_eq!(entries.len(), 10, "initial query should return 10 records");

        for (i, (ts, data)) in entries.iter().enumerate() {
            assert_eq!(*ts, i as i64 * 10 + 1);
            assert!(data.iter().all(|&b| b == 0x11));
        }
    }

    // Write a second batch with higher timestamps
    {
        let lock = arc.clone();
        for i in 10..20i64 {
            lock.write(i * 10 + 1, &[0x22; 64]).unwrap();
        }
    }

    // Second query over the full range: should return all 20 records
    {
        let lock = arc.clone();
        let entries = lock.query(1, 191).unwrap();
        assert_eq!(
            entries.len(),
            20,
            "second query should return all 20 records"
        );

        // Verify first batch
        for (i, (ts, data)) in entries.iter().enumerate().take(10) {
            assert_eq!(*ts, i as i64 * 10 + 1);
            assert!(data.iter().all(|&b| b == 0x11));
        }

        // Verify second batch
        for (i, (ts, data)) in entries.iter().enumerate().take(20).skip(10) {
            assert_eq!(*ts, i as i64 * 10 + 1);
            assert!(data.iter().all(|&b| b == 0x22));
        }
    }

    // Third query: only the new range should return 10 records
    {
        let lock = arc.clone();
        let entries = lock.query(101, 191).unwrap();
        assert_eq!(
            entries.len(),
            10,
            "query on new range should return 10 records"
        );
        for (ts, data) in &entries {
            assert!(*ts >= 101 && *ts <= 191);
            assert!(data.iter().all(|&b| b == 0x22));
        }
    }

    store.close().unwrap();
}

#[test]
fn public_query_iter_reads_current_index_entry_after_correction() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let ds = store.open_dataset("ds", "type").unwrap();

    ds.write(1, b"one").unwrap();
    ds.write(2, b"old").unwrap();
    ds.write(3, b"three").unwrap();

    let mut iter = ds.query_iter(1, 3).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (1, b"one".to_vec()));

    ds.write(2, b"corrected").unwrap();

    assert_eq!(iter.next().unwrap().unwrap(), (2, b"corrected".to_vec()));
    assert_eq!(iter.next().unwrap().unwrap(), (3, b"three".to_vec()));
    assert!(iter.next().is_none());

    store.close().unwrap();
}

#[test]
fn public_query_iter_skips_entry_deleted_after_iterator_creation() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder().enable_journal(false).build();
    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("ds", "type", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let ds = store.open_dataset("ds", "type").unwrap();

    ds.write(1, b"one").unwrap();
    ds.write(2, b"two").unwrap();
    ds.write(3, b"three").unwrap();

    let mut iter = ds.query_iter(1, 3).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (1, b"one".to_vec()));

    ds.delete(2).unwrap();

    assert_eq!(iter.next().unwrap().unwrap(), (3, b"three".to_vec()));
    assert!(iter.next().is_none());

    store.close().unwrap();
}
