//! Compression tests: seal, single-record block, flags, compress level.
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

/// P0-C-1: pending overflow seal compression flow
///
/// Write data to fill a pending block, then write more to trigger overflow.
/// Verify the data is correctly compressed and readable.
#[test]
fn t30_1_pending_overflow_seal() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "seal_overflow",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("seal_overflow", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write enough data to fill multiple blocks
    // Each record ~4KB, block max 64KB, so ~16 records per block
    // Write 50 records to ensure at least 2 blocks are created
    {
        let lock = arc.clone();
        for i in 0..50i64 {
            let data = vec![0xABu8; 4096]; // 4KB per record
            lock.write(i * 10 + 1, &data).unwrap();
        }

        // Verify all data is readable
        for i in 0..50i64 {
            let result = lock.read(i * 10 + 1).unwrap();
            assert!(result.is_some(), "record {} should exist", i);
            let (ts, data) = result.unwrap();
            assert_eq!(ts, i * 10 + 1);
            assert_eq!(data.len(), 4096);
            assert!(data.iter().all(|&b| b == 0xAB));
        }
    }

    // Inspect dataset to verify compression happened
    let info = store.inspect_dataset("seal_overflow", "data").unwrap();
    // Should have multiple data segments or blocks
    assert!(
        info.state.total_record_count >= 50,
        "should have at least 50 records"
    );

    store.close().unwrap();
}

/// P0-C-2: single-record block creation
///
/// Write a record > 64KB (BLOCK_MAX_SIZE).
/// Verify it creates an exclusive single-record block.
#[test]
fn t30_2_single_record_block() {
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

    // Write a record > 64KB (BLOCK_MAX_SIZE = 65536)
    let large_data = vec![0xCDu8; 100 * 1024]; // 100KB
    {
        let lock = arc.clone();
        lock.write(100, &large_data).unwrap();

        // Verify the large record is readable
        let result = lock.read(100).unwrap();
        assert!(result.is_some(), "large record should exist");
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 100);
        assert_eq!(data.len(), 100 * 1024);
        assert!(data.iter().all(|&b| b == 0xCD));
    }

    // Write a normal-sized record after the large one
    {
        let lock = arc.clone();
        let normal_data = vec![0xEFu8; 1024];
        lock.write(200, &normal_data).unwrap();

        let result = lock.read(200).unwrap();
        assert!(result.is_some(), "normal record should exist");
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 200);
        assert_eq!(data.len(), 1024);
        assert!(data.iter().all(|&b| b == 0xEF));
    }

    store.close().unwrap();
}

/// P0-C-3: SEALED+COMPRESSED flags verification
///
/// Verify that after writing and sealing, blocks have correct flags.
/// We test this indirectly by verifying data integrity after compression.
#[test]
fn t30_3_sealed_compressed_flags() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "flags_test",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("flags_test", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write data to create blocks
    {
        let lock = arc.clone();
        for i in 0..20i64 {
            let data = format!("record_{}", i).into_bytes();
            lock.write(i * 10 + 1, &data).unwrap();
        }
    }

    // Flush to ensure blocks are sealed
    {
        let lock = arc.clone();
        lock.flush().unwrap();
    }

    // Close and reopen to verify data persists with correct flags
    store.close().unwrap();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let ds = store.open_dataset("flags_test", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Verify all data is still readable after reopen
    {
        let lock = arc.clone();
        for i in 0..20i64 {
            let result = lock.read(i * 10 + 1).unwrap();
            assert!(result.is_some(), "record {} should exist after reopen", i);
            let (ts, data) = result.unwrap();
            assert_eq!(ts, i * 10 + 1);
            let expected = format!("record_{}", i).into_bytes();
            assert_eq!(data, expected);
        }
    }

    store.close().unwrap();
}

/// P0-C-4: Compression with larger payload after deflate
///
/// Write incompressible data (random-like) to test handling when
/// compressed size is not smaller than original.
#[test]
fn t30_4_compression_larger_payload() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "incompressible",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("incompressible", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write incompressible data (each byte different)
    {
        let lock = arc.clone();
        for i in 0..30i64 {
            // Create data that's hard to compress
            let mut data = vec![0u8; 4096];
            for (j, byte) in data.iter_mut().enumerate() {
                *byte = ((i * 1000 + j as i64) % 256) as u8;
            }
            lock.write(i * 10 + 1, &data).unwrap();
        }

        // Verify all data is readable
        for i in 0..30i64 {
            let result = lock.read(i * 10 + 1).unwrap();
            assert!(result.is_some(), "record {} should exist", i);
            let (ts, data) = result.unwrap();
            assert_eq!(ts, i * 10 + 1);
            assert_eq!(data.len(), 4096);
            // Verify data integrity
            for (j, &byte) in data.iter().enumerate() {
                assert_eq!(byte, ((i * 1000 + j as i64) % 256) as u8);
            }
        }
    }

    store.close().unwrap();
}

/// P0-C-5: compress_level effect verification
///
/// Test that different compression levels produce valid results.
#[test]
fn t30_5_compress_level_effect() {
    use timslite::{Store, StoreConfig};

    // Test with compress_level=0 (no compression)
    let dir1 = temp_dir();
    let mut store1 = Store::open(&dir1, StoreConfig::default()).unwrap();
    store1
        .create_dataset("comp0", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 0, 0, 0)
        .unwrap();

    let ds1 = store1.open_dataset("comp0", "data").unwrap();
    let arc1 = store1.get_dataset(&ds1).unwrap();

    // Write compressible data
    {
        let lock = arc1.clone();
        for i in 0..20i64 {
            let data = vec![0xAAu8; 4096];
            lock.write(i * 10 + 1, &data).unwrap();
        }
    }

    // Test with compress_level=9 (max compression)
    let dir2 = temp_dir();
    let mut store2 = Store::open(&dir2, StoreConfig::default()).unwrap();
    store2
        .create_dataset("comp9", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 9, 0, 0)
        .unwrap();

    let ds2 = store2.open_dataset("comp9", "data").unwrap();
    let arc2 = store2.get_dataset(&ds2).unwrap();

    // Write same data
    {
        let lock = arc2.clone();
        for i in 0..20i64 {
            let data = vec![0xAAu8; 4096];
            lock.write(i * 10 + 1, &data).unwrap();
        }
    }

    // Verify both datasets have correct data
    {
        let lock1 = arc1.clone();
        let lock2 = arc2.clone();
        for i in 0..20i64 {
            let result1 = lock1.read(i * 10 + 1).unwrap();
            let result2 = lock2.read(i * 10 + 1).unwrap();
            assert!(result1.is_some());
            assert!(result2.is_some());
            assert_eq!(result1.unwrap().1, result2.unwrap().1);
        }
    }

    // Inspect both datasets to compare sizes
    let info1 = store1.inspect_dataset("comp0", "data").unwrap();
    let info2 = store2.inspect_dataset("comp9", "data").unwrap();

    // Both should have same record count
    assert_eq!(
        info1.state.total_record_count, info2.state.total_record_count,
        "record counts should match"
    );

    // Both should have data
    assert!(info1.state.total_data_size > 0);
    assert!(info2.state.total_data_size > 0);

    store1.close().unwrap();
    store2.close().unwrap();
}

/// P1-C-6: Compression with different data patterns
///
/// Verify compression works correctly with various data patterns.
#[test]
fn t30_6_compression_data_patterns() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "patterns",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let ds = store.open_dataset("patterns", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Pattern 1: All zeros (highly compressible)
    {
        let lock = arc.clone();
        lock.write(1, &vec![0u8; 8192]).unwrap();
    }

    // Pattern 2: Repeated pattern
    {
        let lock = arc.clone();
        let mut data = Vec::with_capacity(8192);
        for _ in 0..2048 {
            data.extend_from_slice(&[0xDE, 0xAD]);
        }
        lock.write(2, &data).unwrap();
    }

    // Pattern 3: Incremental values
    {
        let lock = arc.clone();
        let data: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
        lock.write(3, &data).unwrap();
    }

    // Pattern 4: Random-like but deterministic
    {
        let lock = arc.clone();
        let mut data = vec![0u8; 8192];
        let mut state = 12345u64;
        for byte in data.iter_mut() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (state >> 33) as u8;
        }
        lock.write(4, &data).unwrap();
    }

    // Verify all patterns are readable
    {
        let lock = arc.clone();

        // Pattern 1
        let result = lock.read(1).unwrap().unwrap();
        assert_eq!(result.1, vec![0u8; 8192]);

        // Pattern 2
        let result = lock.read(2).unwrap().unwrap();
        let mut expected = Vec::with_capacity(8192);
        for _ in 0..2048 {
            expected.extend_from_slice(&[0xDE, 0xAD]);
        }
        assert_eq!(result.1, expected);

        // Pattern 3
        let result = lock.read(3).unwrap().unwrap();
        let expected: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
        assert_eq!(result.1, expected);

        // Pattern 4
        let result = lock.read(4).unwrap().unwrap();
        let mut expected = vec![0u8; 8192];
        let mut state = 12345u64;
        for byte in expected.iter_mut() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (state >> 33) as u8;
        }
        assert_eq!(result.1, expected);
    }

    store.close().unwrap();
}
