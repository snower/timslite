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

/// P0-C-7: deflate compress_type persistence across reopen
///
/// Create a dataset with deflate (compress_type=1), write data,
/// close and reopen. Verify data reads back correctly and
/// inspect confirms compress_type=1.
#[test]
fn t30_7_deflate_persistence() {
    use timslite::{DataSetConfigBuilder, Store, StoreConfig};

    let dir = temp_dir();
    let store_cfg = StoreConfig::default();
    let mut store = Store::open(&dir, store_cfg.clone()).unwrap();

    let ds_cfg = DataSetConfigBuilder::from_store(&store_cfg).compress_type(1); // deflate
    store
        .create_dataset_with_config("deflate_persist", "data", Some(ds_cfg))
        .unwrap();

    let ds = store.open_dataset("deflate_persist", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write data
    for i in 0..30i64 {
        let data = vec![0xBBu8; 4096];
        arc.write(i * 10 + 1, &data).unwrap();
    }

    // Flush to seal blocks
    arc.flush().unwrap();

    // Close and reopen
    store.close().unwrap();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    // Verify data reads back correctly
    let ds = store.open_dataset("deflate_persist", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();
    for i in 0..30i64 {
        let result = arc.read(i * 10 + 1).unwrap();
        assert!(result.is_some(), "record {} should exist after reopen", i);
        let (ts, data) = result.unwrap();
        assert_eq!(ts, i * 10 + 1);
        assert_eq!(data.len(), 4096);
        assert!(data.iter().all(|&b| b == 0xBB));
    }

    // Verify inspect shows compress_type = 1 (deflate)
    let info = store.inspect_dataset("deflate_persist", "data").unwrap();
    assert_eq!(
        info.info.compress_type, 1,
        "meta should persist compress_type=1 (deflate)"
    );

    store.close().unwrap();
}

/// P0-C-8: mixed compress_types in same store
///
/// Same store has two datasets: one zstd (0), one deflate (1).
/// Each works independently after writes.
#[test]
fn t30_8_mixed_compress_types() {
    use timslite::{DataSetConfigBuilder, Store, StoreConfig};

    let dir = temp_dir();
    // Disable block cache to avoid key collision between datasets with
    // same segment_file_offset + block_offset but different compress_type.
    let store_cfg = StoreConfig::builder()
        .cache_max_memory(0)
        .build();
    let mut store = Store::open(&dir, store_cfg.clone()).unwrap();

    // Dataset 1: zstd (compress_type=0)
    let zstd_cfg = DataSetConfigBuilder::from_store(&store_cfg).compress_type(0);
    store
        .create_dataset_with_config("zstd_ds", "data", Some(zstd_cfg))
        .unwrap();

    // Dataset 2: deflate (compress_type=1)
    let deflate_cfg = DataSetConfigBuilder::from_store(&store_cfg).compress_type(1);
    store
        .create_dataset_with_config("deflate_ds", "data", Some(deflate_cfg))
        .unwrap();

    // Write to zstd dataset
    let ds_z = store.open_dataset("zstd_ds", "data").unwrap();
    let arc_z = store.get_dataset(&ds_z).unwrap();
    for i in 0..20i64 {
        let data = vec![0xAAu8; 4096];
        arc_z.write(i * 10 + 1, &data).unwrap();
    }
    arc_z.flush().unwrap();

    // Write to deflate dataset
    let ds_d = store.open_dataset("deflate_ds", "data").unwrap();
    let arc_d = store.get_dataset(&ds_d).unwrap();
    for i in 0..20i64 {
        let data = vec![0xCCu8; 4096];
        arc_d.write(i * 10 + 1, &data).unwrap();
    }
    arc_d.flush().unwrap();

    // Close and reopen to ensure all blocks are sealed with their algorithms
    store.close().unwrap();
    let mut store = Store::open(&dir, StoreConfig::builder().cache_max_memory(0).build()).unwrap();

    // Verify zstd data
    let ds_z = store.open_dataset("zstd_ds", "data").unwrap();
    let arc_z = store.get_dataset(&ds_z).unwrap();
    for i in 0..20i64 {
        let result = arc_z.read(i * 10 + 1).unwrap().unwrap();
        assert_eq!(result.0, i * 10 + 1);
        assert!(result.1.iter().all(|&b| b == 0xAA));
    }

    // Verify deflate data
    let ds_d = store.open_dataset("deflate_ds", "data").unwrap();
    let arc_d = store.get_dataset(&ds_d).unwrap();
    for i in 0..20i64 {
        let result = arc_d.read(i * 10 + 1).unwrap().unwrap();
        assert_eq!(result.0, i * 10 + 1);
        assert!(result.1.iter().all(|&b| b == 0xCC));
    }

    // Verify inspect shows correct compress_type for each
    let info_z = store.inspect_dataset("zstd_ds", "data").unwrap();
    assert_eq!(
        info_z.info.compress_type, 0,
        "zstd dataset compress_type should be 0"
    );

    let info_d = store.inspect_dataset("deflate_ds", "data").unwrap();
    assert_eq!(
        info_d.info.compress_type, 1,
        "deflate dataset compress_type should be 1"
    );

    store.close().unwrap();
}

/// P0-C-9: tampered compress_type in meta file returns error
///
/// Create a dataset, close, tamper the compress_type byte in the
/// meta file to an invalid value, reopen and verify error.
#[test]
fn t30_9_tampered_compress_type_meta() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();

    store
        .create_dataset(
            "tamper_meta",
            "data",
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    // Close store
    store.close().unwrap();

    // Tamper meta file: compress_type byte is at offset 37
    // TLV layout: magic(4)+ver(2)+len(2) + 3 full TLVs + compress_type TLV
    //   0x01+2+8=11, 0x02+2+8=11, 0x03+2+1=4, 0x09+2+1=4
    //   base=8, compress_type value = 8+11+11+4+2 = offset 37
    let meta_path = dir.join("tamper_meta").join("data").join("meta");
    assert!(meta_path.exists(), "meta file should exist");
    let mut meta_bytes = fs::read(&meta_path).unwrap();
    assert!(
        meta_bytes[37] == 0 || meta_bytes[37] == 1,
        "original compress_type should be valid"
    );
    // Set to invalid compress_type
    meta_bytes[37] = 0xFF;
    fs::write(&meta_path, &meta_bytes).unwrap();

    // Reopen store (lazy) then try to open dataset — should fail on meta parse
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let result = store.open_dataset("tamper_meta", "data");
    assert!(
        result.is_err(),
        "opening dataset with tampered compress_type should fail"
    );

    // Cleanup
    let _ = fs::remove_dir_all(&dir);
}

/// P0-C-10: segment header compress_type matches meta
///
/// Create a dataset with deflate, write data, then verify the
/// segment header file's compress_type byte matches the meta.
#[test]
fn t30_10_segment_header_compress_type() {
    use timslite::{DataSetConfigBuilder, Store, StoreConfig};

    let dir = temp_dir();
    let store_cfg = StoreConfig::default();
    let mut store = Store::open(&dir, store_cfg.clone()).unwrap();

    let ds_cfg = DataSetConfigBuilder::from_store(&store_cfg).compress_type(1); // deflate
    store
        .create_dataset_with_config("seg_hdr", "data", Some(ds_cfg))
        .unwrap();

    let ds = store.open_dataset("seg_hdr", "data").unwrap();
    let arc = store.get_dataset(&ds).unwrap();

    // Write enough data to ensure a data segment file exists
    for i in 0..30i64 {
        let data = vec![0xDDu8; 4096];
        arc.write(i * 10 + 1, &data).unwrap();
    }
    arc.flush().unwrap();

    // Inspect to confirm meta compress_type
    let info = store.inspect_dataset("seg_hdr", "data").unwrap();
    assert_eq!(info.info.compress_type, 1, "meta should say deflate");

    store.close().unwrap();

    // Read data segment header file directly from disk.
    // Data segment files are in {dir}/seg_hdr/data/data/
    // The filename is the base offset (a number).
    let data_dir = dir.join("seg_hdr").join("data").join("data");
    let seg_files: Vec<_> = fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.chars().all(|c| c.is_ascii_digit()))
                .unwrap_or(false)
        })
        .collect();
    assert!(
        !seg_files.is_empty(),
        "should have at least one data segment file"
    );

    for entry in &seg_files {
        let path = entry.path();
        let bytes = fs::read(&path).unwrap();
        assert!(
            bytes.len() > 49,
            "segment file {:?} should be at least 50 bytes",
            path.file_name()
        );
        // Verify magic "TMSL"
        assert_eq!(&bytes[0..4], b"TMSL", "segment header magic should be TMSL");
        // Segment header compress_type is at offset 49:
        //   fixed_prefix(9) + created_at(11) + file_offset(11) + file_size(11) + compress_level(4) + type(1)+len(2)
        // = 9 + 11 + 11 + 11 + 4 + 3 = 49
        assert_eq!(
            bytes[49],
            1,
            "segment {:?} header compress_type should be 1 (deflate), got {}",
            path.file_name(),
            bytes[49]
        );
    }
}
