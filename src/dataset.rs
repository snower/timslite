//! DataSet: aggregates DataSegmentSet + TimeIndex for a (name, type) pair.
//!
//! Lifecycle: `create` (explicit, with parameters) / `open` (reads from meta) / `close` / `drop_dataset`.
//! Parameters (`data_segment_size`, `index_segment_size`, `compress_level`) are set **only at creation time**
//! and written to the meta file. They are **immutable** — subsequent opens read from meta.

use std::path::PathBuf;
use std::time::Instant;

use crate::config::DataSetConfig;
use crate::error::{Result, TmslError};
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
        let meta = DataSetMeta::new(data_segment_size, index_segment_size, compress_level);
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
            },
            segments,
            time_index,
            last_used_at: Instant::now(),
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
        };

        let segments = DataSegmentSet::load_existing(
            &base_dir,
            config.data_segment_size,
            config.block_max_size,
            config.compress_level,
        )?;
        let index_dir = base_dir.join("index");
        let time_index = TimeIndex::load_existing(&index_dir, config.index_segment_size)?;

        Ok(Self {
            id,
            base_dir,
            config,
            segments,
            time_index,
            last_used_at: Instant::now(),
        })
    }

    /// Delete an entire dataset directory (destructive, not recoverable).
    pub fn drop_dataset(base_dir: &std::path::Path) -> Result<()> {
        std::fs::remove_dir_all(base_dir)?;
        Ok(())
    }

    /// Write a record to this dataset.
    pub fn write(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        let (seg_offset, block_rel_offset, in_block_offset) =
            self.segments.append(timestamp, data)?;

        self.time_index.add_entry(
            timestamp,
            seg_offset + block_rel_offset, // absolute block offset
            in_block_offset,
        )?;

        self.last_used_at = Instant::now();
        Ok(())
    }

    /// Query records in the time range [start_ts, end_ts].
    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, Vec<u8>)>> {
        let entries = self.time_index.query(start_ts, end_ts)?;
        let mut records = Vec::with_capacity(entries.len());
        for entry in &entries {
            let re = ReadIndexEntry {
                timestamp: entry.timestamp,
                block_offset: entry.block_offset,
                in_block_offset: entry.in_block_offset,
            };
            let (ts, data) = self.segments.read_at_index(&re)?;
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

    /// Get the base directory.
    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    /// Get the last used time.
    pub fn last_used_at(&self) -> Instant {
        self.last_used_at
    }
}
