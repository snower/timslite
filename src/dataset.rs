//! DataSet: aggregates DataSegmentSet + TimeIndex for a (name, type) pair.

use std::path::PathBuf;
use std::time::Instant;

use crate::config::{DataSetConfig, StoreConfig};
use crate::error::Result;
use crate::index::TimeIndex;
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
    /// Create a new dataset.
    pub fn new(id: DataSetKey, base_dir: PathBuf, config: &StoreConfig) -> Result<Self> {
        std::fs::create_dir_all(&base_dir)?;
        let segments = DataSegmentSet::load_existing(
            &base_dir,
            config.data_segment_size,
            config.block_max_size,
            config.compress_level,
        )?;
        let index_dir = base_dir.join(".index");
        let time_index = TimeIndex::load_existing(&index_dir, config.index_segment_size)?;

        Ok(Self {
            id,
            base_dir,
            config: DataSetConfig::from_store(config),
            segments,
            time_index,
            last_used_at: Instant::now(),
        })
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
