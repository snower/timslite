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
use crate::index::segment::{IndexEntry, IndexSegment, BLOCK_OFFSET_FILLER};
use crate::index::TimeIndex;
use crate::meta::DataSetMeta;
use crate::query::iter::QueryIterator;
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
    pub(crate) config: DataSetConfig,
    segments: DataSegmentSet,
    time_index: TimeIndex,
    last_used_at: Instant,
    latest_written_timestamp: i64, // For continuous mode: track the highest timestamp
    retention_ms: u64,             // 0 = no limit (same unit as timestamp)
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
        initial_data_segment_size: u64,
        initial_index_segment_size: u64,
        retention_ms: u64,
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
            initial_data_segment_size,
            initial_index_segment_size,
            retention_ms,
        );
        meta.write_to_file(&meta_path)?;

        let segments = DataSegmentSet::new(
            &base_dir,
            data_segment_size,
            initial_data_segment_size,
            block_max_size,
            compress_level,
        )?;
        let time_index = TimeIndex::new(
            &index_dir,
            index_segment_size,
            initial_index_segment_size,
            index_continuous != 0,
        )?;

        Ok(Self {
            id,
            base_dir,
            config: DataSetConfig {
                data_segment_size,
                index_segment_size,
                block_max_size,
                compress_level,
                index_continuous,
                initial_data_segment_size,
                initial_index_segment_size,
                retention_ms,
            },
            segments,
            time_index,
            last_used_at: Instant::now(),
            latest_written_timestamp: 0,
            retention_ms,
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
            initial_data_segment_size: meta.initial_data_segment_size,
            initial_index_segment_size: meta.initial_index_segment_size,
            retention_ms: meta.retention_ms,
        };
        let retention_ms = meta.retention_ms;

        let segments = DataSegmentSet::load_existing(
            &base_dir,
            config.data_segment_size,
            meta.initial_data_segment_size,
            config.block_max_size,
            config.compress_level,
        )?;
        let index_dir = base_dir.join("index");
        let time_index = TimeIndex::load_existing(
            &index_dir,
            config.index_segment_size,
            meta.initial_index_segment_size,
            config.index_continuous != 0,
        )?;

        // Recover latest_written_timestamp from index segments
        let latest_written_timestamp =
            Self::recover_latest_timestamp(&time_index, config.index_segment_size);

        Ok(Self {
            id,
            base_dir,
            config,
            segments,
            time_index,
            last_used_at: Instant::now(),
            latest_written_timestamp,
            retention_ms,
        })
    }

    /// Delete an entire dataset directory (destructive, not recoverable).
    pub fn drop_dataset(base_dir: &std::path::Path) -> Result<()> {
        std::fs::remove_dir_all(base_dir)?;
        Ok(())
    }

    /// Write a record to this dataset.
    ///
    /// # Timestamp dispatch (both indexing modes)
    ///
    /// - `timestamp <= 0`: error.
    /// - `timestamp == latest_written_timestamp` (and latest > 0): **correction write** —
    ///   in-place overwrite of the data bytes in the last uncompressed block of the latest
    ///   data segment. The index entry is unchanged. May change data length.
    /// - `timestamp < latest_written_timestamp`: **out-of-order write** — appends data to
    ///   the latest data segment and updates the existing index entry in place. Requires
    ///   that an index entry at `timestamp` already exists (always true in continuous mode;
    ///   true in non-continuous mode only if `timestamp` was previously written). If the
    ///   old entry referenced real data, the old data segment's `invalid_record_count`
    ///   is incremented (the previous data becomes an orphan record).
    /// - `timestamp > latest_written_timestamp`: **normal write** — in continuous mode fills
    ///   missing timestamps with filler entries first, then appends the real entry.
    pub fn write(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        if timestamp <= 0 {
            return Err(TmslError::InvalidData("timestamp must be > 0".into()));
        }

        // Correction write: same timestamp as latest → in-place overwrite in
        // the last uncompressed block of the latest data segment. Index unchanged.
        if self.latest_written_timestamp > 0 && timestamp == self.latest_written_timestamp {
            return self.correct_write(timestamp, data);
        }

        // Out-of-order write: timestamp < latest → append to latest segment,
        // update existing index entry in place. May increment invalid_record_count
        // on the old data segment.
        if timestamp < self.latest_written_timestamp {
            return self.out_of_order_write(timestamp, data);
        }

        // Normal write: timestamp > latest
        if self.config.index_continuous == 0 {
            let (seg_offset, block_rel_offset, in_block_offset) =
                self.segments.append(timestamp, data)?;
            self.time_index
                .add_entry(timestamp, seg_offset + block_rel_offset, in_block_offset)?;
        } else {
            // Continuous mode: fill gaps with filler, then append
            for ts in (self.latest_written_timestamp + 1)..timestamp {
                self.time_index.add_filler_entry(ts);
            }
            let (seg_offset, block_rel_offset, in_block_offset) =
                self.segments.append(timestamp, data)?;
            self.time_index
                .add_entry(timestamp, seg_offset + block_rel_offset, in_block_offset)?;
        }
        self.latest_written_timestamp = timestamp;
        self.last_used_at = Instant::now();
        Ok(())
    }

    /// Out-of-order write: timestamp < latest_written_timestamp (both modes).
    ///
    /// Appends data to the latest segment and updates the existing index entry
    /// in place with the new data location. If the old entry referenced real data,
    /// the old data segment's `invalid_record_count` is incremented.
    ///
    /// Requires an existing index entry at `timestamp`:
    /// - Continuous mode: always has an entry (filler or real data)
    /// - Non-continuous mode: only if `timestamp` was previously written
    fn out_of_order_write(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        let (seg_offset, block_rel_offset, in_block_offset) =
            self.segments.append(timestamp, data)?;
        let new_block_offset = seg_offset + block_rel_offset;

        let old_entry =
            self.time_index
                .update_entry(timestamp, new_block_offset, in_block_offset)?;

        if old_entry.block_offset != BLOCK_OFFSET_FILLER {
            self.segments
                .increment_invalid_record_count(old_entry.block_offset)?;
        }

        // latest_written_timestamp unchanged
        self.last_used_at = Instant::now();
        Ok(())
    }

    /// Correction write: overwrite the data of an existing record in place.
    ///
    /// The record is located via the existing index entry, then its data bytes
    /// in the last uncompressed block of the latest data segment are replaced.
    /// Supports variable data length — updates block + segment counters accordingly.
    fn correct_write(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        let entry = self.time_index.find_entry(timestamp)?.ok_or_else(|| {
            TmslError::NotFound(format!(
                "no index entry for correction timestamp {}",
                timestamp
            ))
        })?;

        self.segments.overwrite_in_last_block(
            entry.block_offset,
            entry.in_block_offset,
            timestamp,
            data,
        )?;

        // latest_written_timestamp unchanged; index unchanged.
        self.last_used_at = Instant::now();
        Ok(())
    }

    /// Delete the record at the given timestamp.
    ///
    /// Marks the index entry as sentinel (block_offset = FILLER, in_block_offset = FILLER)
    /// and increments the data segment's `invalid_record_count` by 1.
    ///
    /// Returns `TmslError::NotFound` if:
    /// - `timestamp` is invalid (≤ 0)
    /// - the dataset is empty
    /// - no entry exists at `timestamp`
    /// - the entry is already a filler (no real data)
    pub fn delete(&mut self, timestamp: i64) -> Result<()> {
        if timestamp <= 0 {
            return Err(TmslError::InvalidData("timestamp must be > 0".into()));
        }
        if self.latest_written_timestamp == 0 {
            return Err(TmslError::NotFound(format!(
                "no entry to delete at timestamp {} (dataset is empty)",
                timestamp
            )));
        }

        let old_entry = self.time_index.find_and_delete_entry(timestamp)?;
        // Old entry references real data → increment invalid_record_count on its segment
        self.segments
            .increment_invalid_record_count(old_entry.block_offset)?;

        self.last_used_at = Instant::now();
        Ok(())
    }

    /// Return a lazy query iterator for records in [start_ts, end_ts].
    #[allow(clippy::needless_lifetimes)]
    pub fn query_iter<'a, 'b>(
        &'a mut self,
        start_ts: i64,
        end_ts: i64,
        cache: Option<&'b BlockCache>,
    ) -> Result<QueryIterator<'a, 'b>> {
        let (start_ts, end_ts) = self.clamp_query_range(start_ts, end_ts);
        if start_ts > end_ts {
            return Ok(QueryIterator::new(vec![], &mut self.segments, cache));
        }
        let entries = self.time_index.query(start_ts, end_ts)?;
        Ok(QueryIterator::new(entries, &mut self.segments, cache))
    }

    /// Query records in the time range [start_ts, end_ts].
    /// Filler entries (sentinel block_offset) are skipped.
    #[allow(clippy::needless_lifetimes)]
    pub fn query(
        &mut self,
        start_ts: i64,
        end_ts: i64,
        cache: Option<&BlockCache>,
    ) -> Result<Vec<(i64, Vec<u8>)>> {
        let iter = self.query_iter(start_ts, end_ts, cache)?;
        iter.collect_all()
    }

    pub fn query_index_entries(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<IndexEntry>> {
        let (start_ts, end_ts) = self.clamp_query_range(start_ts, end_ts);
        if start_ts > end_ts {
            return Ok(vec![]);
        }
        self.time_index.query(start_ts, end_ts)
    }

    pub fn read_entry_at_index(
        &mut self,
        entry: &IndexEntry,
        cache: Option<&BlockCache>,
    ) -> Result<(i64, Vec<u8>)> {
        let re = ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset,
            in_block_offset: entry.in_block_offset,
        };
        self.segments.read_at_index(&re, cache)
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
    fn recover_latest_timestamp(time_index: &TimeIndex, max_file_size: u64) -> i64 {
        let mut latest = 0i64;
        for meta in &time_index.closed_index_segments {
            if let Ok(seg) = IndexSegment::open(&meta.path, meta.start_timestamp, max_file_size) {
                if seg.wrote_count > 0 {
                    // Read the last entry's timestamp
                    let mmap = seg.mmap.as_ref().unwrap();
                    let pos = crate::header::INDEX_HEADER_SIZE as usize
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
                    let pos = crate::header::INDEX_HEADER_SIZE as usize
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

    /// Data retention period (same unit as timestamp; 0 = no limit).
    pub fn retention_ms(&self) -> u64 {
        self.retention_ms
    }

    /// Clamp an inclusive query range to the data retention window.
    /// Returns (effective_start, effective_end). If retention is disabled
    /// or latest_written_timestamp is unknown, returns the original range.
    fn clamp_query_range(&self, start_ts: i64, end_ts: i64) -> (i64, i64) {
        if self.retention_ms == 0 || self.latest_written_timestamp <= 0 {
            return (start_ts, end_ts);
        }
        let threshold = self
            .latest_written_timestamp
            .saturating_sub(self.retention_ms as i64);
        (start_ts.max(threshold), end_ts)
    }

    /// Compute retention expiration threshold, or -1 if retention disabled / no data yet.
    fn retention_threshold(&self) -> i64 {
        if self.retention_ms == 0 || self.latest_written_timestamp <= 0 {
            return -1;
        }
        self.latest_written_timestamp
            .saturating_sub(self.retention_ms as i64)
    }

    /// Reclaim expired data & index segments whose entries fall entirely before the
    /// retention threshold. Closes the dataset first (all segments go to closed set).
    /// Returns the total number of segment files deleted.
    pub fn reclaim_expired_segments(&mut self) -> Result<usize> {
        let threshold = self.retention_threshold();
        if threshold < 0 {
            return Ok(0);
        }

        // Close all open segments so they appear in closed_segments / closed_index_segments
        self.close()?;

        // Reclaim index segments (read-only mmap per segment, released immediately)
        let idx_reclaimed = self
            .time_index
            .reclaim_expired_segments(threshold, self.config.index_segment_size)?;

        // Reclaim data segments (uses cached max_timestamp in closed_segments vec)
        let data_reclaimed = self.segments.reclaim_expired_segments(threshold)?;

        self.last_used_at = Instant::now();
        Ok(idx_reclaimed + data_reclaimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            1,          // continuous
            256 * 1024, // initial_data_segment_size
            4 * 1024,   // initial_index_segment_size
            0,          // retention_ms
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
            1,          // continuous
            256 * 1024, // initial_data_segment_size
            4 * 1024,   // initial_index_segment_size
            0,          // retention_ms
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
    fn test_correction_write_continuous_mode() {
        let dir = temp_dir("correction_continuous");
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
            1, // continuous mode
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"second").unwrap();

        // Same ts=150 → correction write (in-place overwrite)
        ds.write(150, b"corrected").unwrap();

        let entries = ds.query(100, 150, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].0, 150);
        assert_eq!(entries[1].1, b"corrected");
        // latest_written_timestamp should be unchanged
        assert_eq!(ds.latest_written_timestamp, 150);
    }

    #[test]
    fn test_correction_write_non_continuous_mode() {
        let dir = temp_dir("correction_noncontinuous");
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
            0, // non-continuous mode
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"second").unwrap();

        // Same ts=150 → correction write
        ds.write(150, b"corrected").unwrap();

        let entries = ds.query(100, 150, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].0, 150);
        assert_eq!(entries[1].1, b"corrected");
        assert_eq!(ds.latest_written_timestamp, 150);
    }

    #[test]
    fn test_correction_write_resize_larger() {
        let dir = temp_dir("correction_resize_larger");
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
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"short").unwrap();

        // Resize to larger
        let big_data = vec![0xABu8; 200];
        ds.write(100, &big_data).unwrap();

        let entries = ds.query(100, 100, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.len(), 200);
        assert_eq!(entries[0].1, big_data);
    }

    #[test]
    fn test_correction_write_resize_smaller() {
        let dir = temp_dir("correction_resize_smaller");
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
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        let big_data = vec![0xCDu8; 200];
        ds.write(100, &big_data).unwrap();

        // Resize to smaller
        ds.write(100, b"tiny").unwrap();

        let entries = ds.query(100, 100, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"tiny");
    }

    #[test]
    fn test_correction_write_multiple_times() {
        let dir = temp_dir("correction_multi");
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
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"v1").unwrap();
        ds.write(100, b"v2_").unwrap();
        ds.write(100, b"v3__").unwrap();

        let entries = ds.query(100, 100, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"v3__");
    }

    #[test]
    fn test_correction_write_then_new_write() {
        let dir = temp_dir("correction_then_new");
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
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(100, b"corrected_first").unwrap();
        ds.write(200, b"second").unwrap();

        let entries = ds.query(100, 200, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"corrected_first");
        assert_eq!(entries[1].0, 200);
        assert_eq!(entries[1].1, b"second");
    }

    #[test]
    fn test_correction_write_reopen_persistence() {
        let dir = temp_dir("correction_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                65536,
                0,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();

            ds.write(100, b"original").unwrap();
            ds.write(100, b"corrected").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Reopen and verify
        let mut ds = DataSet::open(id, dir.clone(), 65536).unwrap();
        let entries = ds.query(100, 100, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"corrected");
    }

    #[test]
    fn test_noncontinuous_mode_out_of_order_rejected_when_no_entry() {
        // In non-continuous mode, out-of-order write fails if there is no
        // existing index entry at the target timestamp.
        let dir = temp_dir("noncontinuous_ooo_no_entry");
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
            0, // non-continuous
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"second").unwrap();

        // ts=120 was never written → no index entry → out-of-order write rejected
        let result = ds.write(120, b"middle");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("no index entry") || msg.contains("out-of-order"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_noncontinuous_mode_out_of_order_succeeds_with_existing_entry() {
        // In non-continuous mode, out-of-order write SUCCEEDS if an entry at
        // the target timestamp already exists. New data is appended; old data
        // becomes an orphan (invalid_record_count++).
        let dir = temp_dir("noncontinuous_ooo_with_entry");
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
            0, // non-continuous
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(200, b"second").unwrap();
        // Out-of-order write at ts=100 (entry exists from earlier write)
        ds.write(100, b"updated_first").unwrap();

        assert_eq!(ds.latest_written_timestamp, 200); // unchanged
        let entries = ds.query(100, 200, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"updated_first");
        assert_eq!(entries[1].0, 200);
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
            256 * 1024, // initial_data_segment_size
            4 * 1024,   // initial_index_segment_size
            0,          // retention_ms
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
    fn test_out_of_order_write_overwrites_real_entry() {
        // Out-of-order write at an existing real entry succeeds: data is
        // appended to latest segment, index entry is updated in place, and
        // the old data segment's invalid_record_count is incremented.
        let dir = temp_dir("ooo_overwrite_real");
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
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"last").unwrap();

        // Out-of-order at ts=100 (real entry) → succeeds via out_of_order_write
        ds.write(100, b"updated_first").unwrap();
        assert_eq!(ds.latest_written_timestamp, 150); // unchanged

        // Query should still return ts=100 and ts=150 with updated data
        let entries = ds.query(100, 150, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"updated_first");
        assert_eq!(entries[1].0, 150);
    }

    #[test]
    fn test_out_of_order_increments_invalid_record_count() {
        // Out-of-order writes that replace real data increment invalid_record_count
        // on the old data segment, and the count is persisted across reopen.
        let dir = temp_dir("ooo_invalid_count");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                65536,
                1,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();

            ds.write(100, b"v1").unwrap();
            ds.write(200, b"latest").unwrap();

            // Two out-of-order writes at ts=100 — each increments invalid_record_count
            ds.write(100, b"v2").unwrap();
            ds.write(100, b"v3").unwrap();

            // The old data segment (only one segment here, everything fits) should have
            // invalid_record_count = 2 after two out-of-order writes.
            let seg = ds.segments.segments.last().unwrap();
            assert_eq!(
                seg.invalid_record_count, 2,
                "expected invalid_record_count=2, got {}",
                seg.invalid_record_count
            );

            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Reopen and verify the count persists. Trigger segment open via query.
        let mut ds2 = DataSet::open(id, dir, 65536).unwrap();
        // Query forces segment open; after open, invalid_record_count is read from file header.
        let entries = ds2.query(100, 200, None).unwrap();
        assert_eq!(entries.len(), 2); // ts=100 ("v3") and ts=200 ("latest")
        let seg2 = ds2.segments.segments.last().unwrap();
        assert_eq!(seg2.invalid_record_count, 2);
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
            256 * 1024, // initial_data_segment_size
            4 * 1024,   // initial_index_segment_size
            0,          // retention_ms
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
                256 * 1024, // initial_data_segment_size
                4 * 1024,   // initial_index_segment_size
                0,          // retention_ms
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

    // ─── Retention tests ──────────────────────────────────────────────────

    #[test]
    fn test_retention_ms_no_reclaim_when_zero() {
        let dir = temp_dir("retention_no_reclaim");
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
            0,
            256 * 1024,
            4 * 1024,
            0, // retention_ms = 0 (no limit)
        )
        .unwrap();

        // Write old data, then idle-close all segments
        ds.write(100, b"old").unwrap();
        ds.flush().unwrap();
        ds.segments.idle_close_all().unwrap();
        ds.time_index.idle_close_all().unwrap();

        // Write new data to force different segment
        ds.write(200, b"new").unwrap();

        // reclaim should do nothing because retention_ms = 0
        let reclaimed = ds.reclaim_expired_segments().unwrap();
        assert_eq!(reclaimed, 0);
        assert!(ds.retention_ms() == 0);
    }

    #[test]
    fn test_retention_ms_stored_and_roundtrip() {
        let dir = temp_dir("retention_stored");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let ret = 30 * 86400 * 1000u64;
        let ds = DataSet::create(
            id.clone(),
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            0,
            256 * 1024,
            4 * 1024,
            ret,
        )
        .unwrap();
        assert_eq!(ds.retention_ms(), ret);

        // Reopen and verify
        let ds2 = DataSet::open(id, dir, 65536).unwrap();
        assert_eq!(ds2.retention_ms(), ret);
    }

    #[test]
    fn test_retention_reclaim_basic() {
        let dir = temp_dir("retention_basic");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        // retention = 50 (same unit as timestamps)
        let mut ds = DataSet::create(
            id.clone(),
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            0,
            256 * 1024,
            4 * 1024,
            50,
        )
        .unwrap();

        // Write multiple records ALL within retention window of latest (200)
        // threshold = 200 - 50 = 150. All of [150, 200] must be queryable.
        ds.write(150, b"a").unwrap();
        ds.write(180, b"b").unwrap();
        ds.write(200, b"c").unwrap();

        assert_eq!(ds.latest_written_timestamp, 200);

        // Query [150, 200] → clamp to [max(150,150)=150, 200] → 3 records
        let entries = ds.query(150, 200, None).unwrap();
        assert_eq!(entries.len(), 3);

        // Query [100, 200] → clamp to [max(100,150)=150, 200] → 3 records
        let entries_before = ds.query(100, 200, None).unwrap();
        assert_eq!(entries_before.len(), 3);

        // Single data segment with max_ts=200 >= threshold → no reclaim
        let reclaimed = ds.reclaim_expired_segments().unwrap();
        assert_eq!(reclaimed, 0);

        // After reclaim, still queryable
        let entries_after = ds.query(150, 200, None).unwrap();
        assert_eq!(entries_after.len(), 3);
    }

    #[test]
    fn test_retention_reclaim_removes_all_when_expired() {
        // This test confirms that when retention is 0 (no limit), nothing is reclaimed
        // regardless of how old the data is.
        let dir = temp_dir("retention_zero_no_reclaim");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id.clone(),
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            65536,
            0,
            256 * 1024,
            4 * 1024,
            0, // retention_ms = 0
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.write(130, b"b").unwrap();
        ds.write(500, b"c").unwrap();

        let reclaimed = ds.reclaim_expired_segments().unwrap();
        assert_eq!(reclaimed, 0);

        let entries = ds.query(100, 500, None).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_retention_query_clamped() {
        let dir = temp_dir("retention_clamped");
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
            0,
            256 * 1024,
            4 * 1024,
            50, // retention_ms = 50
        )
        .unwrap();

        // Write old ts=100, then new ts=200
        ds.write(100, b"old").unwrap();
        ds.write(200, b"new").unwrap();

        // threshold = 200 - 50 = 150
        // Query [100, 200] should be clamped to [150, 200], returning only 1 record
        let entries = ds.query(100, 200, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 200);

        // Query entirely within expired range → empty
        let empty = ds.query(50, 130, None).unwrap();
        assert!(empty.is_empty());

        // Query fully within valid range
        let valid = ds.query(180, 200, None).unwrap();
        assert_eq!(valid.len(), 1);
    }

    // ─── Delete tests ──────────────────────────────────────────────────────

    #[test]
    fn test_delete_existing_entry() {
        let dir = temp_dir("delete_existing");
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
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();

        ds.delete(100).unwrap();

        // Query should return only ts=200
        let entries = ds.query(100, 200, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 200);
        assert_eq!(entries[0].1, b"b");
    }

    #[test]
    fn test_delete_filler_entry_error() {
        // In continuous mode, a filler position has no real data.
        // Delete should reject it with NotFound.
        let dir = temp_dir("delete_filler");
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
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(1, b"first").unwrap();
        ds.write(5, b"last").unwrap();
        // ts=3 is a filler
        let result = ds.delete(3);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("filler"),
            "expected filler error"
        );
    }

    #[test]
    fn test_delete_nonexistent_error() {
        let dir = temp_dir("delete_nonexistent");
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
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();

        let result = ds.delete(999);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_idempotent_error() {
        // Deleting the same timestamp twice → second delete errors.
        let dir = temp_dir("delete_idempotent");
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
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.delete(100).unwrap();

        // Second delete on same timestamp should fail
        let result = ds.delete(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_increments_invalid_record_count() {
        let dir = temp_dir("delete_increments_count");
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
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();
        ds.write(300, b"c").unwrap();

        ds.delete(100).unwrap();
        ds.delete(200).unwrap();

        // Both deletes target the same segment → count = 2
        let seg = ds.segments.segments.last().unwrap();
        assert_eq!(seg.invalid_record_count, 2);

        // Only ts=300 should remain queryable
        let entries = ds.query(100, 300, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 300);
    }

    #[test]
    fn test_delete_then_out_of_order_rewrite() {
        // After delete(ts), rewrite at ts becomes out-of-order → replaces filler.
        // invalid_record_count should NOT increase on the rewrite (filler → real).
        let dir = temp_dir("delete_then_ooo");
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
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        // Continuous: writes ts=100 and ts=150 with filler in between
        ds.write(100, b"first").unwrap();
        ds.write(150, b"last").unwrap();

        ds.delete(100).unwrap();
        // After delete: entry at 100 is filler, invalid_record_count=1

        // Rewrite at ts=100 → out-of-order, replaces filler (FILLER → real):
        // invalid_record_count unchanged
        ds.write(100, b"replaced").unwrap();

        let seg = ds.segments.segments.last().unwrap();
        assert_eq!(
            seg.invalid_record_count, 1,
            "expected 1, got {}",
            seg.invalid_record_count
        );

        let entries = ds.query(100, 150, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"replaced");
    }

    #[test]
    fn test_delete_persists_across_reopen() {
        let dir = temp_dir("delete_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                65536,
                0,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();
            ds.write(100, b"a").unwrap();
            ds.write(200, b"b").unwrap();
            ds.delete(100).unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Reopen
        let mut ds2 = DataSet::open(id, dir, 65536).unwrap();
        let entries = ds2.query(100, 200, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 200);

        // Reopened segment should preserve invalid_record_count
        let seg2 = ds2.segments.segments.last().unwrap();
        assert_eq!(seg2.invalid_record_count, 1);
    }
}
