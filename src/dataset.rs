//! DataSet: aggregates DataSegmentSet + TimeIndex for a (name, type) pair.
//!
//! Lifecycle: `create` (explicit, with parameters) / `open` (reads from meta) / `close` / `drop_dataset`.
//! Parameters (`data_segment_size`, `index_segment_size`, `compress_level`) are set **only at creation time**
//! and written to the meta file. They are **immutable** — subsequent opens read from meta.

use std::path::PathBuf;
use std::time::Instant;

use crate::cache::BlockCache;
use crate::config::DataSetConfig;
use crate::error::{Result, TmslError};
use crate::index::segment::{
    IndexEntry, IndexSegment, BLOCK_OFFSET_FILLER, IN_BLOCK_OFFSET_FILLER,
};
use crate::index::TimeIndex;
use crate::meta::DataSetMeta;
use crate::segment::DataSegmentSet;
use crate::segment::ReadIndexEntry;

/// Dataset key for identifying a (name, type) pair.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DataSetKey {
    pub name: String,
    pub dataset_type: String,
}

// ─── DataSet ─────────────────────────────────────────────────────────────

pub struct DataSet {
    pub id: DataSetKey,
    pub base_dir: PathBuf,
    config: DataSetConfig,
    segments: DataSegmentSet,
    time_index: TimeIndex,
    last_used_at: Instant,
    latest_written_timestamp: i64, // For continuous mode: track the highest timestamp
}

impl DataSet {
    /// Create a new dataset (explicit creation, errors if already exists).
    ///
    /// Parameters are written to the meta file and are **immutable** — cannot be changed
    /// after creation.
    pub fn create(
        id: DataSetKey,
        base_dir: PathBuf,
        data_segment_size: u64,
        index_segment_size: u64,
        compress_level: u8,
        block_max_size: u32,
        index_continuous: u8,
    ) -> Result<Self> {
        let meta_path = base_dir.join("meta");
        if meta_path.exists() {
            return Err(TmslError::AlreadyExists(format!(
                "dataset already exists at {:?}",
                base_dir
            )));
        }

        // Ensure data/ subdirectory exists
        let data_dir = base_dir.join("data");
        std::fs::create_dir_all(&data_dir)?;
        // Ensure index/ subdirectory exists
        let index_dir = base_dir.join("index");
        std::fs::create_dir_all(&index_dir)?;

        // Write meta file (immutable config, written only once)
        let meta = DataSetMeta::new(
            data_segment_size,
            index_segment_size,
            compress_level,
            index_continuous,
        );
        meta.write_to_file(&meta_path)?;

        let segments =
            DataSegmentSet::new(&base_dir, data_segment_size, block_max_size, compress_level)?;
        let time_index = TimeIndex::new(&index_dir, index_segment_size)?;

        Ok(Self {
            id,
            base_dir,
            config: DataSetConfig {
                data_segment_size,
                index_segment_size,
                block_max_size,
                compress_level,
                index_continuous,
            },
            segments,
            time_index,
            last_used_at: Instant::now(),
            latest_written_timestamp: 0,
        })
    }

    /// Open an existing dataset (reads config from meta file).
    ///
    /// Fails if the dataset does not exist (no meta file).
    /// Segment sizes and compress_level are read from meta and cannot be overridden.
    pub fn open(id: DataSetKey, base_dir: PathBuf, block_max_size: u32) -> Result<Self> {
        let meta_path = base_dir.join("meta");
        if !meta_path.exists() {
            return Err(TmslError::NotFound(format!(
                "dataset meta not found at {:?}",
                meta_path
            )));
        }

        // Read meta file (immutable config)
        let meta = DataSetMeta::read_from_file(&meta_path)?;

        let config = DataSetConfig {
            data_segment_size: meta.data_segment_size,
            index_segment_size: meta.index_segment_size,
            block_max_size,
            compress_level: meta.compress_level,
            index_continuous: meta.index_continuous,
        };

        let segments = DataSegmentSet::load_existing(
            &base_dir,
            config.data_segment_size,
            config.block_max_size,
            config.compress_level,
        )?;
        let index_dir = base_dir.join("index");
        let time_index = TimeIndex::load_existing(&index_dir, config.index_segment_size)?;

        // Recover latest_written_timestamp from index segments
        let latest_written_timestamp = Self::recover_latest_timestamp(&time_index);

        Ok(Self {
            id,
            base_dir,
            config,
            segments,
            time_index,
            last_used_at: Instant::now(),
            latest_written_timestamp,
        })
    }

    /// Delete an entire dataset directory (destructive, not recoverable).
    pub fn drop_dataset(base_dir: &std::path::Path) -> Result<()> {
        std::fs::remove_dir_all(base_dir)?;
        Ok(())
    }

    /// Write a record to this dataset.
    ///
    /// # Continuous Mode (`index_continuous == 1`)
    /// - `timestamp > latest_written_timestamp`: fills missing timestamps with filler entries,
    ///   then appends the real entry.
    /// - `timestamp < latest_written_timestamp`: data is appended to the latest data segment,
    ///   but the corresponding filler entry in the index is replaced with the real entry (mmap overwrite).
    /// - `timestamp == latest_written_timestamp`: error (duplicate timestamp).
    ///
    /// # Non-Continuous Mode (`index_continuous == 0`)
    /// - `timestamp <= latest_written_timestamp`: error (out-of-order).
    pub fn write(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        if timestamp <= 0 {
            return Err(TmslError::InvalidData("timestamp must be > 0".into()));
        }

        if self.config.index_continuous == 0 {
            // Non-continuous mode: reject out-of-order
            if timestamp <= self.latest_written_timestamp {
                return Err(TmslError::InvalidData(format!(
                    "out-of-order: timestamp {} <= latest {}",
                    timestamp, self.latest_written_timestamp
                )));
            }
            // Normal write
            let (seg_offset, block_rel_offset, in_block_offset) =
                self.segments.append(timestamp, data)?;
            self.time_index
                .add_entry(timestamp, seg_offset + block_rel_offset, in_block_offset)?;
            self.latest_written_timestamp = timestamp;
        } else {
            // Continuous mode
            if timestamp < self.latest_written_timestamp {
                // Back-fill: find and replace filler
                let (seg_offset, block_rel_offset, in_block_offset) =
                    self.segments.append(timestamp, data)?;
                self.replace_filler_with_real(
                    timestamp,
                    seg_offset + block_rel_offset,
                    in_block_offset,
                )?;
                // Do NOT update latest_written_timestamp
            } else if timestamp == self.latest_written_timestamp {
                return Err(TmslError::InvalidData(format!(
                    "duplicate timestamp: {}",
                    timestamp
                )));
            } else {
                // Normal in-order write: fill gaps then append
                for ts in (self.latest_written_timestamp + 1)..timestamp {
                    self.time_index.add_filler_entry(ts);
                }
                let (seg_offset, block_rel_offset, in_block_offset) =
                    self.segments.append(timestamp, data)?;
                self.time_index.add_entry(
                    timestamp,
                    seg_offset + block_rel_offset,
                    in_block_offset,
                )?;
                self.latest_written_timestamp = timestamp;
            }
        }

        self.last_used_at = Instant::now();
        Ok(())
    }

    /// Replace a filler entry at the given timestamp with real data.
    /// Only valid in continuous mode when timestamp < latest_written_timestamp.
    fn replace_filler_with_real(
        &mut self,
        timestamp: i64,
        block_offset: u64,
        in_block_offset: u16,
    ) -> Result<()> {
        // Try in-memory buffer first (unflushed filler entries)
        if let Some(pos) = self
            .time_index
            .in_memory_buffer
            .iter()
            .position(|e| e.timestamp == timestamp)
        {
            let entry = &self.time_index.in_memory_buffer[pos];
            if entry.block_offset == BLOCK_OFFSET_FILLER {
                let new_entry = IndexEntry {
                    timestamp,
                    block_offset,
                    in_block_offset,
                };
                self.time_index.in_memory_buffer[pos] = new_entry;
                return Ok(());
            } else {
                return Err(TmslError::InvalidData(format!(
                    "cannot overwrite: entry at timestamp {} already has real data",
                    timestamp
                )));
            }
        }

        // Try open segments first
        for seg in &mut self.time_index.index_segments {
            if let Some(idx) = seg.find_entry_index(timestamp) {
                let entry = seg.find_exact(timestamp).unwrap();
                if entry.block_offset == BLOCK_OFFSET_FILLER {
                    seg.ensure_open()?;
                    let new_entry = IndexEntry {
                        timestamp,
                        block_offset,
                        in_block_offset,
                    };
                    seg.overwrite_entry(idx, &new_entry)?;
                    return Ok(());
                } else {
                    return Err(TmslError::InvalidData(format!(
                        "cannot overwrite: entry at timestamp {} already has real data",
                        timestamp
                    )));
                }
            }
        }

        // Try closed segments
        for meta in &self.time_index.closed_index_segments {
            let mut seg = IndexSegment::open(&meta.path, meta.start_timestamp)?;
            if let Some(idx) = seg.find_entry_index(timestamp) {
                let entry = seg.find_exact(timestamp).unwrap();
                if entry.block_offset == BLOCK_OFFSET_FILLER {
                    let new_entry = IndexEntry {
                        timestamp,
                        block_offset,
                        in_block_offset,
                    };
                    seg.overwrite_entry(idx, &new_entry)?;
                    seg.idle_close()?;
                    return Ok(());
                } else {
                    return Err(TmslError::InvalidData(format!(
                        "cannot overwrite: entry at timestamp {} already has real data",
                        timestamp
                    )));
                }
            }
        }

        Err(TmslError::NotFound(format!(
            "no entry found at timestamp {}",
            timestamp
        )))
    }

    /// Query records in the time range [start_ts, end_ts].
    /// Filler entries (sentinel block_offset) are skipped.
    pub fn query(
        &mut self,
        start_ts: i64,
        end_ts: i64,
        cache: Option<&BlockCache>,
    ) -> Result<Vec<(i64, Vec<u8>)>> {
        let entries = self.time_index.query(start_ts, end_ts)?;
        let mut records = Vec::with_capacity(entries.len());
        for entry in &entries {
            // Skip filler entries
            if entry.block_offset == BLOCK_OFFSET_FILLER {
                continue;
            }
            let re = ReadIndexEntry {
                timestamp: entry.timestamp,
                block_offset: entry.block_offset,
                in_block_offset: entry.in_block_offset,
            };
            let (ts, data) = self.segments.read_at_index(&re, cache)?;
            records.push((ts, data));
        }
        records.sort_by_key(|(ts, _)| *ts);
        Ok(records)
    }

    /// Flush all data.
    pub fn flush(&mut self) -> Result<()> {
        // Flush in-memory index buffer to disk
        self.time_index.flush_to_disk()?;
        self.segments.sync_all()?;
        self.time_index.sync_all()?;
        self.last_used_at = Instant::now();
        Ok(())
    }

    /// Close all segments.
    pub fn close(&mut self) -> Result<()> {
        self.flush()?;
        self.segments.idle_close_all()?;
        self.time_index.idle_close_all()?;
        Ok(())
    }

    /// Mark usage.
    pub fn touch(&mut self) {
        self.last_used_at = Instant::now();
    }

    /// Recover the latest written timestamp from index segments (on open).
    fn recover_latest_timestamp(time_index: &TimeIndex) -> i64 {
        let mut latest = 0i64;
        for meta in &time_index.closed_index_segments {
            if let Ok(seg) = IndexSegment::open(&meta.path, meta.start_timestamp) {
                if seg.wrote_count > 0 {
                    // Read the last entry's timestamp
                    let mmap = seg.mmap.as_ref().unwrap();
                    let pos = crate::header::HEADER_SIZE as usize
                        + (seg.wrote_count - 1) * crate::index::INDEX_ENTRY_SIZE;
                    let mmap_bytes = mmap.as_ref();
                    let ts = i64::from_le_bytes(mmap_bytes[pos..pos + 8].try_into().unwrap());
                    if ts > latest {
                        latest = ts;
                    }
                }
            }
        }
        for seg in &time_index.index_segments {
            if seg.wrote_count > 0 {
                if let Some(mmap) = seg.mmap.as_ref() {
                    let pos = crate::header::HEADER_SIZE as usize
                        + (seg.wrote_count - 1) * crate::index::INDEX_ENTRY_SIZE;
                    let mmap_bytes = mmap.as_ref();
                    let ts = i64::from_le_bytes(mmap_bytes[pos..pos + 8].try_into().unwrap());
                    if ts > latest {
                        latest = ts;
                    }
                }
            }
        }
        for entry in &time_index.in_memory_buffer {
            if entry.timestamp > latest {
                latest = entry.timestamp;
            }
        }
        latest
    }

    /// Get the base directory.
    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    /// Get the last used time.
    pub fn last_used_at(&self) -> Instant {
        self.last_used_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::segment::IndexEntry;

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join("timslite_dataset_test");
        let dir = d.join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_continuous_mode_filler_filling() {
        let dir = temp_dir("continuous_filler");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            1, // continuous
        )
        .unwrap();

        // Write ts=100
        ds.write(100, b"hello").unwrap();
        assert_eq!(ds.latest_written_timestamp, 100);

        // Write ts=110 -> should fill ts=101..109 with filler
        ds.write(110, b"world").unwrap();
        assert_eq!(ds.latest_written_timestamp, 110);

        // Flush to disk
        ds.flush().unwrap();

        // Query should return only 2 real entries (filler filtered)
        let entries = ds.query(100, 110, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[1].0, 110);
    }

    #[test]
    fn test_continuous_mode_backfill_replaces_filler() {
        let dir = temp_dir("continuous_backfill");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            1, // continuous
        )
        .unwrap();

        // Write ts=100
        ds.write(100, b"first").unwrap();
        // Write ts=150 -> fills ts=101..149
        ds.write(150, b"last").unwrap();

        // Back-fill ts=125 (replaces filler)
        ds.write(125, b"middle").unwrap();
        assert_eq!(ds.latest_written_timestamp, 150); // unchanged

        // Query should return 3 real entries
        let entries = ds.query(100, 150, None).unwrap();
        assert_eq!(entries.len(), 3);
        let ts_values: Vec<i64> = entries.iter().map(|e| e.0).collect();
        assert_eq!(ts_values, vec![100, 125, 150]);
    }

    #[test]
    fn test_continuous_mode_duplicate_timestamp_rejected() {
        let dir = temp_dir("continuous_dup");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            1,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"second").unwrap();

        // Duplicate ts=150 should fail
        let result = ds.write(150, b"dup");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("duplicate timestamp"));
    }

    #[test]
    fn test_noncontinuous_mode_out_of_order_rejected() {
        let dir = temp_dir("noncontinuous_ooo");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            0, // non-continuous (default)
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"second").unwrap();

        // Out-of-order should fail
        let result = ds.write(120, b"middle");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out-of-order"));
    }

    #[test]
    fn test_timestamp_zero_rejected() {
        let dir = temp_dir("ts_zero");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            1,
        )
        .unwrap();

        let result = ds.write(0, b"invalid");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("timestamp must be > 0"));

        // Also negative
        let result = ds.write(-1, b"invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_continuous_backfill_non_filler_rejected() {
        let dir = temp_dir("backfill_real");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            1,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"last").unwrap();

        // try backfill at ts=100 (which is a real entry, not filler)
        let result = ds.write(100, b"overwrite_real");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("already has real data"));
    }

    #[test]
    fn test_continuous_backfill_not_found() {
        let dir = temp_dir("backfill_nofound");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            1,
        )
        .unwrap();

        // Write ts=1 (filler range is empty since latest=0)
        ds.write(1, b"first").unwrap();
        // Write ts=10 -> fillers for ts=2..9
        ds.write(10, b"last").unwrap();

        // Backfill at ts=2 (which IS a filler) should succeed
        ds.write(2, b"filled").unwrap();
        assert_eq!(ds.latest_written_timestamp, 10); // unchanged

        // Verify 3 real entries
        let entries = ds.query(1, 10, None).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_continuous_open_recovery_latest_timestamp() {
        let dir = temp_dir("continuous_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };

        // Create and write
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                65536,
                1,
            )
            .unwrap();
            ds.write(100, b"first").unwrap();
            ds.write(150, b"last").unwrap();
            ds.close().unwrap();
        }

        // Open and check latest_written_timestamp recovered
        let ds2 = DataSet::open(id, dir, 65536).unwrap();
        assert_eq!(ds2.latest_written_timestamp, 150);
    }
}
