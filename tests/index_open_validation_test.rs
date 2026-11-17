//! Strict index-file validation on dataset open.
//!
//! Covers immutable max `file_size` normalization, continuous slot grids and
//! off-grid segment starts, and non-continuous entry ordering.

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use timslite::{DataSetConfigBuilder, Store, StoreConfig};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join("timslite_index_open_validation");
    let dir = root.join(format!(
        "{name}_{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn store_config() -> StoreConfig {
    StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build()
}

fn dataset_config(
    store_cfg: &StoreConfig,
    index_continuous: u8,
    index_segment_size: u64,
    index_data_size: u32,
) -> DataSetConfigBuilder {
    DataSetConfigBuilder::from_store(store_cfg)
        .index_continuous(index_continuous)
        .index_segment_size(index_segment_size)
        .initial_index_segment_size(index_segment_size)
        .index_data_size(index_data_size)
}

fn index_dir(data_dir: &Path, name: &str) -> PathBuf {
    data_dir.join(name).join("data").join("index")
}

fn numeric_index_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<(i64, PathBuf)> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            let start = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<i64>().ok())?;
            Some((start, path))
        })
        .collect();
    files.sort_by_key(|(start, _)| *start);
    files.into_iter().map(|(_, path)| path).collect()
}

fn first_index_file(dir: &Path) -> PathBuf {
    numeric_index_files(dir)
        .into_iter()
        .next()
        .expect("dataset must have an index segment")
}

/// Locate the value bytes of a meta TLV with the given tag/width.
fn tlv_value_offset(header: &[u8], tag: u8, value_len: usize) -> Option<usize> {
    let meta_len = u16::from_le_bytes([header[7], header[8]]) as usize;
    let end = (9 + meta_len).min(header.len());
    let mut off = 9;
    while off + 3 <= end {
        let t = header[off];
        let len = u16::from_le_bytes([header[off + 1], header[off + 2]]) as usize;
        off += 3;
        if off + len > end {
            return None;
        }
        if t == tag && len == value_len {
            return Some(off);
        }
        off += len;
    }
    None
}

fn rewrite_tlv_u64(path: &Path, tag: u8, value: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut header = vec![0u8; 128];
    file.read_exact(&mut header).unwrap();
    let off = tlv_value_offset(&header, tag, 8).expect("meta tlv value");
    file.seek(SeekFrom::Start(off as u64)).unwrap();
    file.write_all(&value.to_le_bytes()).unwrap();
    file.sync_all().unwrap();
}

fn overwrite_entry_timestamp(path: &Path, slot: usize, timestamp: i64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start((128 + slot * 16) as u64))
        .unwrap();
    file.write_all(&timestamp.to_le_bytes()).unwrap();
    file.sync_all().unwrap();
}

fn open_dataset_error(dir: &Path, config: StoreConfig) -> String {
    let mut store = Store::open(dir, config).unwrap();
    match store.open_dataset("ds", "data") {
        Ok(_) => panic!("expected open_dataset to reject the corrupted index file"),
        Err(err) => format!("{err}"),
    }
}

#[test]
fn open_dataset_rejects_index_max_file_size_mismatch_in_both_index_modes() {
    for index_continuous in [0u8, 1] {
        let dir = temp_dir(&format!("max_mismatch_{index_continuous}"));
        let config = store_config();
        {
            let mut store = Store::open(&dir, config.clone()).unwrap();
            let ds_cfg = dataset_config(&config, index_continuous, 4096, 0);
            let ds = store
                .create_dataset_with_config("ds", "data", Some(ds_cfg))
                .unwrap();
            ds.write(100, b"a").unwrap();
            ds.write(200, b"b").unwrap();
            ds.flush().unwrap();
            store.close().unwrap();
        }

        // 128 + 504 * 16 = 8192: another aligned capacity, but not the
        // dataset's configured maximum (4096).
        let path = first_index_file(&index_dir(&dir, "ds"));
        rewrite_tlv_u64(&path, 0x03, 8192);

        let err = open_dataset_error(&dir, config);
        assert!(err.contains("file_size"), "err: {err}");
    }
}

#[test]
fn irregular_index_segment_size_normalizes_for_stride_24() {
    let dir = temp_dir("irregular_stride_24");
    let config = store_config();
    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let ds_cfg = dataset_config(&config, 0, 4097, 3);
        let ds = store
            .create_dataset_with_config("ds", "data", Some(ds_cfg))
            .unwrap();
        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();
        ds.flush().unwrap();
        store.close().unwrap();
    }

    // 4097 normalizes to 128 + 165 * 24 = 4088; reopening must not reject it.
    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let ds = store.open_dataset("ds", "data").unwrap();
        assert_eq!(ds.read(100).unwrap().unwrap().1, b"a");
        assert_eq!(ds.read(200).unwrap().unwrap().1, b"b");
        store.close().unwrap();
    }

    // 128 + 166 * 24 = 4112 is aligned but still the wrong immutable maximum.
    let path = first_index_file(&index_dir(&dir, "ds"));
    rewrite_tlv_u64(&path, 0x03, 4112);
    let err = open_dataset_error(&dir, config);
    assert!(err.contains("file_size"), "err: {err}");
}

#[test]
fn open_dataset_rejects_continuous_off_grid_slot_timestamps() {
    let dir = temp_dir("continuous_slot_grid");
    let config = store_config();
    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let ds_cfg = dataset_config(&config, 1, 4096, 0);
        let ds = store
            .create_dataset_with_config("ds", "data", Some(ds_cfg))
            .unwrap();
        for ts in 100..104 {
            ds.write(ts, b"x").unwrap();
        }
        ds.flush().unwrap();
        store.close().unwrap();
    }

    // Last slot decodes and stays ordered, but is off the slot grid.
    let path = first_index_file(&index_dir(&dir, "ds"));
    overwrite_entry_timestamp(&path, 3, 200);

    let err = open_dataset_error(&dir, config);
    assert!(err.contains("grid"), "err: {err}");
}

#[test]
fn open_dataset_rejects_renamed_off_grid_continuous_segment() {
    let dir = temp_dir("renamed_off_grid");
    let config = store_config();
    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let ds_cfg = dataset_config(&config, 1, 4096, 0);
        let ds = store
            .create_dataset_with_config("ds", "data", Some(ds_cfg))
            .unwrap();
        // Capacity is (4096 - 128) / 16 = 248, so this creates starts 0 and 248.
        for ts in 0..=248 {
            ds.write(ts, b"x").unwrap();
        }
        ds.flush().unwrap();
        store.close().unwrap();
    }

    let idx_dir = index_dir(&dir, "ds");
    let original = idx_dir.join(format!("{:020}", 248));
    let renamed = idx_dir.join(format!("{:020}", 249));
    fs::rename(&original, &renamed).unwrap();
    // Keep the renamed file internally consistent so only the base grid rejects it.
    rewrite_tlv_u64(&renamed, 0x02, 249);
    overwrite_entry_timestamp(&renamed, 0, 249);

    let err = open_dataset_error(&dir, config);
    assert!(err.contains("grid"), "err: {err}");
}

#[test]
fn open_dataset_rejects_noncontinuous_unordered_entries() {
    let dir = temp_dir("noncontinuous_order");
    let config = store_config();
    {
        let mut store = Store::open(&dir, config.clone()).unwrap();
        let ds_cfg = dataset_config(&config, 0, 4096, 0);
        let ds = store
            .create_dataset_with_config("ds", "data", Some(ds_cfg))
            .unwrap();
        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();
        ds.write(300, b"c").unwrap();
        ds.flush().unwrap();
        store.close().unwrap();
    }

    let path = first_index_file(&index_dir(&dir, "ds"));
    overwrite_entry_timestamp(&path, 1, 50);

    let err = open_dataset_error(&dir, config);
    assert!(err.contains("strictly greater"), "err: {err}");
}
