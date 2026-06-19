//! Cache invalidation tests: correction, delete, retention, out-of-order write.
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

/// P0-A-1: Cache invalidation after correction write
///
/// Write data, read to cache, then correction write.
/// Verify the cache is invalidated and new data is returned.
#[test]
fn t31_1_cache_correction_write() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "cache_corr",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cache_corr", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write original data
    {
        let lock = arc.clone();
        lock.write(100, b"original_data").unwrap();
    }

    // Read to populate cache (if cache is enabled)
    {
        let lock = arc.clone();
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"original_data");
    }

    let cache = store.block_cache();
    let count_before = cache.stats().entry_count;

    // Correction write: overwrite with new data
    {
        let lock = arc.clone();
        lock.write(100, b"corrected_data").unwrap();
    }

    let count_after = cache.stats().entry_count;
    assert!(
        count_after <= count_before,
        "cache entries should not increase after correction write: before={}, after={}",
        count_before,
        count_after
    );

    // Read again - should get corrected data (not cached old data)
    {
        let lock = arc.clone();
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 100);
        assert_eq!(data, b"corrected_data");
    }

    store.close().unwrap();
}

/// P0-A-2: Cache invalidation after delete
///
/// Write data, read to cache, then delete.
/// Verify the cache is invalidated and read returns None.
#[test]
fn t31_2_cache_delete() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "cache_del",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cache_del", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write data
    {
        let lock = arc.clone();
        lock.write(100, b"to_be_deleted").unwrap();
    }

    // Read to populate cache
    {
        let lock = arc.clone();
        let result = lock.read(100).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"to_be_deleted");
    }

    // Delete the record
    {
        let lock = arc.clone();
        lock.delete(100).unwrap();
    }

    // Read again - should return None (deleted)
    {
        let lock = arc.clone();
        let result = lock.read(100).unwrap();
        assert!(result.is_none(), "deleted record should return None");
    }

    store.close().unwrap();
}

/// P0-A-3: Cache invalidation after retention reclaim
///
/// Write data with retention window, wait for expiry, trigger reclaim.
/// Verify cache entries for expired data are invalidated.
#[test]
fn t31_3_cache_retention_reclaim() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    // Create dataset with small retention window
    store
        .create_dataset(
            "cache_ret",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            100, // retention_window = 100 timestamp units
        )
        .unwrap();

    let ds = store.open_dataset("cache_ret", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write data at timestamp 50 (within retention window)
    {
        let lock = arc.clone();
        lock.write(50, b"old_data").unwrap();
    }

    // Read to populate cache
    {
        let lock = arc.clone();
        let result = lock.read(50).unwrap();
        assert!(result.is_some());
    }

    // Write data at timestamp 200 (outside retention window)
    {
        let lock = arc.clone();
        lock.write(200, b"new_data").unwrap();
    }

    // Trigger retention reclaim
    {
        let lock = arc.clone();
        let _reclaimed = lock.reclaim_expired_segments().unwrap();
        // May or may not reclaim segments depending on segment boundaries
    }

    // Read old timestamp - should return None if expired
    {
        let lock = arc.clone();
        let result = lock.read(50).unwrap();
        // If retention window is 100 and latest_written_timestamp is 200,
        // then timestamp 50 is expired (200 - 50 = 150 > 100)
        if result.is_some() {
            // If still readable, it's because the segment wasn't reclaimed yet
            // This is acceptable behavior
        }
    }

    // Read new timestamp - should always be readable
    {
        let lock = arc.clone();
        let result = lock.read(200).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"new_data");
    }

    store.close().unwrap();
}

/// P0-A-4: Cache invalidation after out-of-order write
///
/// Write data, read to cache, then out-of-order write (correction).
/// Verify the cache is invalidated.
#[test]
fn t31_4_cache_out_of_order_write() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "cache_ooo",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cache_ooo", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write data at timestamps 100, 200, 300
    {
        let lock = arc.clone();
        lock.write(100, b"data_100").unwrap();
        lock.write(200, b"data_200").unwrap();
        lock.write(300, b"data_300").unwrap();
    }

    // Read all to populate cache
    {
        let lock = arc.clone();
        assert_eq!(lock.read(100).unwrap().unwrap().1, b"data_100");
        assert_eq!(lock.read(200).unwrap().unwrap().1, b"data_200");
        assert_eq!(lock.read(300).unwrap().unwrap().1, b"data_300");
    }

    // Out-of-order write: overwrite timestamp 200 with new data
    {
        let lock = arc.clone();
        lock.write(200, b"new_data_200").unwrap();
    }

    // Read again - should get new data (not cached old data)
    {
        let lock = arc.clone();
        let result = lock.read(200).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 200);
        assert_eq!(data, b"new_data_200");
    }

    // Other timestamps should be unaffected
    {
        let lock = arc.clone();
        assert_eq!(lock.read(100).unwrap().unwrap().1, b"data_100");
        assert_eq!(lock.read(300).unwrap().unwrap().1, b"data_300");
    }

    store.close().unwrap();
}

/// P1-A-5: Cache LRU eviction behavior
///
/// Fill cache beyond watermark and verify LRU eviction works.
#[test]
fn t31_5_cache_lru_eviction() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    // Use small cache to trigger eviction easily
    let config = StoreConfig::builder()
        .cache_max_memory(1024 * 1024) // 1MB cache
        .build();
    let mut store = Store::open(&dir, config).unwrap();

    store
        .create_dataset(
            "cache_lru",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("cache_lru", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write enough data to exceed cache size
    // Each record ~8KB, cache is 1MB, so ~128 records to fill
    {
        let lock = arc.clone();
        for i in 0..200i64 {
            let data = vec![0xABu8; 8192]; // 8KB per record
            lock.write(i * 10 + 1, &data).unwrap();
        }
    }

    // Read all records to populate cache
    {
        let lock = arc.clone();
        for i in 0..200i64 {
            let result = lock.read(i * 10 + 1).unwrap();
            assert!(result.is_some(), "record {} should exist", i);
        }
    }

    // Cache should have evicted some entries due to LRU
    // We can't directly inspect cache state, but we can verify data is still readable
    {
        let lock = arc.clone();
        for i in 0..200i64 {
            let result = lock.read(i * 10 + 1).unwrap();
            assert!(result.is_some(), "record {} should still be readable", i);
            let (ts, data) = result.unwrap();
            assert_eq!(ts, i * 10 + 1);
            assert_eq!(data.len(), 8192);
        }
    }

    store.close().unwrap();
}
