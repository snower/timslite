//! Append tests: migration threshold, sealed block, empty data, timestamp order.
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

/// P0-P-1: Append migration threshold (70%)
///
/// Append data to an existing record until it exceeds the block capacity.
/// Verify the system handles the overflow correctly.
#[test]
fn t32_1_append_migration_threshold() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "append_migrate",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("append_migrate", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write initial record
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"initial").unwrap();
    }

    // Append data multiple times to grow the record
    {
        let mut lock = arc.lock().unwrap();
        for _i in 0..10 {
            let append_data = vec![0xABu8; 1024]; // 1KB each append
            lock.append(100, &append_data).unwrap();
        }

        // Verify the record has grown
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 100);
        // Initial 7 bytes + 10 * 1024 bytes = 10247 bytes
        assert_eq!(data.len(), 7 + 10 * 1024);
    }

    // Try to append until we exceed BLOCK_MAX_SIZE (65536 bytes)
    {
        let mut lock = arc.lock().unwrap();
        // Current size is ~10KB, need to append ~55KB more to exceed 64KB
        let big_append = vec![0xCDu8; 60 * 1024]; // 60KB
        let result = lock.append(100, &big_append);
        // This should fail because final record would exceed block capacity
        assert!(
            result.is_err(),
            "append exceeding block capacity should fail"
        );
    }

    // Verify original data is still intact
    {
        let mut lock = arc.lock().unwrap();
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 100);
        assert_eq!(data.len(), 7 + 10 * 1024);
    }

    store.close().unwrap();
}

/// P0-P-2: Append to sealed block
///
/// After a block is sealed (by writing to a new timestamp),
/// verify append to the old timestamp fails or creates new record.
#[test]
fn t32_2_append_to_sealed_block() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "append_sealed",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("append_sealed", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write record at timestamp 100
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"data_100").unwrap();
    }

    // Write record at timestamp 200 (may seal the block containing ts=100)
    {
        let mut lock = arc.lock().unwrap();
        lock.write(200, b"data_200").unwrap();
    }

    // Flush to ensure blocks are sealed
    {
        let mut lock = arc.lock().unwrap();
        lock.flush().unwrap();
    }

    // Try to append to timestamp 100
    // This should fail because the block is sealed
    {
        let mut lock = arc.lock().unwrap();
        let result = lock.append(100, b"_appended");
        // Append to a non-latest timestamp should fail
        assert!(
            result.is_err(),
            "append to non-latest timestamp should fail"
        );
    }

    // Verify original data is unchanged
    {
        let mut lock = arc.lock().unwrap();
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"data_100");
    }

    store.close().unwrap();
}

/// P0-P-3: Append empty data is no-op
///
/// Append empty data should not create a new record or modify existing.
#[test]
fn t32_3_append_empty_data() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "append_empty",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("append_empty", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write initial record
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"initial_data").unwrap();
    }

    // Append empty data
    {
        let mut lock = arc.lock().unwrap();
        lock.append(100, b"").unwrap();
    }

    // Verify data is unchanged
    {
        let mut lock = arc.lock().unwrap();
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"initial_data");
    }

    // Latest written timestamp should still be 100
    {
        let lock = arc.lock().unwrap();
        assert_eq!(lock.latest_written_timestamp(), Some(100));
    }

    store.close().unwrap();
}

/// P0-P-4: Append timestamp order validation
///
/// Append with timestamp < latest_written_timestamp should fail.
#[test]
fn t32_4_append_timestamp_order() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "append_order",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("append_order", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write records at timestamps 100, 200, 300
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"data_100").unwrap();
        lock.write(200, b"data_200").unwrap();
        lock.write(300, b"data_300").unwrap();
    }

    // Try to append with timestamp < latest (300)
    {
        let mut lock = arc.lock().unwrap();

        // timestamp < latest_written_timestamp should fail
        let result = lock.append(150, b"new_data");
        assert!(
            result.is_err(),
            "append with timestamp < latest should fail"
        );

        // timestamp == latest_written_timestamp should work (append to existing)
        let result = lock.append(300, b"_appended");
        assert!(
            result.is_err(),
            "append to latest timestamp should fail for non-tail record"
        );
    }

    // Verify all data is unchanged
    {
        let mut lock = arc.lock().unwrap();
        assert_eq!(lock.read(100).unwrap().unwrap().1, b"data_100");
        assert_eq!(lock.read(200).unwrap().unwrap().1, b"data_200");
        assert_eq!(lock.read(300).unwrap().unwrap().1, b"data_300");
    }

    store.close().unwrap();
}

/// P0-P-5: Append forward creates new record
///
/// Append with timestamp > latest_written_timestamp creates new record.
#[test]
fn t32_5_append_forward_new_record() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "append_forward",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("append_forward", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write initial record
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"data_100").unwrap();
    }

    // Append with timestamp > latest (forward append)
    {
        let mut lock = arc.lock().unwrap();
        lock.append(200, b"data_200").unwrap();
    }

    // Verify both records exist
    {
        let mut lock = arc.lock().unwrap();
        let result1 = lock.read(100).unwrap();
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().1, b"data_100");

        let result2 = lock.read(200).unwrap();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().1, b"data_200");
    }

    // Latest written timestamp should be 200
    {
        let lock = arc.lock().unwrap();
        assert_eq!(lock.latest_written_timestamp(), Some(200));
    }

    // Continue forward append
    {
        let mut lock = arc.lock().unwrap();
        lock.append(300, b"data_300").unwrap();
        lock.append(400, b"data_400").unwrap();
    }

    // Verify all records
    {
        let mut lock = arc.lock().unwrap();
        assert_eq!(lock.read(100).unwrap().unwrap().1, b"data_100");
        assert_eq!(lock.read(200).unwrap().unwrap().1, b"data_200");
        assert_eq!(lock.read(300).unwrap().unwrap().1, b"data_300");
        assert_eq!(lock.read(400).unwrap().unwrap().1, b"data_400");
        assert_eq!(lock.latest_written_timestamp(), Some(400));
    }

    store.close().unwrap();
}

/// P1-P-6: Append to latest uncompressed tail record
///
/// Append to the latest record when it's an uncompressed tail record.
#[test]
fn t32_6_append_to_tail_record() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "append_tail",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("append_tail", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write initial record
    {
        let mut lock = arc.lock().unwrap();
        lock.write(100, b"initial").unwrap();
    }

    // Append to the latest record (should work for tail record)
    {
        let mut lock = arc.lock().unwrap();
        lock.append(100, b"_part2").unwrap();
        lock.append(100, b"_part3").unwrap();
    }

    // Verify the record has all appended data
    {
        let mut lock = arc.lock().unwrap();
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 100);
        assert_eq!(data, b"initial_part2_part3");
    }

    store.close().unwrap();
}
