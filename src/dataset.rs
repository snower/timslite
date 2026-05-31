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
use crate::query::iter::{QueryIterator, QuerySource};
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
    ///   in-place overwrite of the data bytes in the last pending raw block of the latest
    ///   data segment. The index entry is unchanged. If the target block has already been
    ///   sealed and compressed, falls back to out-of-order write:
    ///   appends data to latest segment, updates index entry, and increments the old
    ///   segment's `invalid_record_count`.
    /// - `timestamp < latest_written_timestamp`: **out-of-order write** — appends data to
    ///   the latest data segment and updates the existing index position in place. In
    ///   continuous mode, sparse logical holes are materialized on demand.
    /// - `timestamp > latest_written_timestamp`: **normal write** — in continuous mode only
    ///   materializes filler entries in the previous and current edge index segments.
    pub fn write(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        self.write_with_cache(timestamp, data, None)
    }

    /// Write a record and invalidate global cache entries affected by
    /// correction/out-of-order index rewrites.
    pub fn write_with_cache(
        &mut self,
        timestamp: i64,
        data: &[u8],
        cache: Option<&BlockCache>,
    ) -> Result<()> {
        if timestamp <= 0 {
            return Err(TmslError::InvalidData("timestamp must be > 0".into()));
        }
        if self.is_timestamp_expired(timestamp) {
            return Err(self.expired_error(timestamp));
        }

        // Correction write: same timestamp as latest → in-place overwrite in
        // the last pending raw block of the latest data segment. Index unchanged.
        if self.latest_written_timestamp > 0 && timestamp == self.latest_written_timestamp {
            return self.correct_write(timestamp, data, cache);
        }

        // Out-of-order write: timestamp < latest → append to latest segment,
        // update existing index entry in place. May increment invalid_record_count
        // on the old data segment.
        if timestamp < self.latest_written_timestamp {
            return self.out_of_order_write(timestamp, data, cache);
        }

        // Normal write: timestamp > latest
        if self.config.index_continuous == 0 {
            let (seg_offset, block_rel_offset, in_block_offset) =
                self.segments.append(timestamp, data)?;
            self.time_index
                .add_entry(timestamp, seg_offset + block_rel_offset, in_block_offset)?;
        } else {
            let (seg_offset, block_rel_offset, in_block_offset) =
                self.segments.append(timestamp, data)?;
            self.time_index.add_sparse_continuous_entry(
                self.latest_written_timestamp,
                timestamp,
                seg_offset + block_rel_offset,
                in_block_offset,
            )?;
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
    /// Non-continuous mode requires an existing index entry at `timestamp`.
    /// Continuous mode may update a real entry, replace a filler, or materialize
    /// a sparse logical hole on demand.
    fn out_of_order_write(
        &mut self,
        timestamp: i64,
        data: &[u8],
        cache: Option<&BlockCache>,
    ) -> Result<()> {
        let (seg_offset, block_rel_offset, in_block_offset) =
            self.segments.append(timestamp, data)?;
        let new_block_offset = seg_offset + block_rel_offset;

        let old_entry =
            self.time_index
                .update_entry(timestamp, new_block_offset, in_block_offset)?;

        if old_entry.block_offset != BLOCK_OFFSET_FILLER {
            self.invalidate_cache_for_entry(&old_entry, cache);
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
    /// in the last pending raw block of the latest data segment are replaced.
    /// Supports variable data length — updates block + segment counters accordingly.
    ///
    /// If the target block has been sealed and compressed,
    /// falls back to out-of-order write: appends data to latest segment, updates
    /// the index entry, and increments `invalid_record_count` on the old segment.
    fn correct_write(
        &mut self,
        timestamp: i64,
        data: &[u8],
        cache: Option<&BlockCache>,
    ) -> Result<()> {
        match self.time_index.find_entry(timestamp)? {
            Some(entry) => {
                match self.segments.overwrite_in_last_block(
                    entry.block_offset,
                    entry.in_block_offset,
                    timestamp,
                    data,
                ) {
                    Ok(()) => {
                        // latest_written_timestamp unchanged; index unchanged.
                        self.invalidate_cache_for_entry(&entry, cache);
                        self.last_used_at = Instant::now();
                        Ok(())
                    }
                    Err(_) => {
                        // Target block cannot be modified in place (sealed/compressed or
                        // not the last block/record) → fall back to out-of-order write:
                        // append to latest segment, update index, increment invalid_record_count
                        self.out_of_order_write(timestamp, data, cache)
                    }
                }
            }
            None => Err(TmslError::NotFound(format!(
                "no index entry for correction timestamp {}",
                timestamp
            ))),
        }
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
        self.delete_with_cache(timestamp, None)
    }

    /// Delete a record and invalidate any global cache entry for the old block.
    pub fn delete_with_cache(&mut self, timestamp: i64, cache: Option<&BlockCache>) -> Result<()> {
        if timestamp <= 0 {
            return Err(TmslError::InvalidData("timestamp must be > 0".into()));
        }
        if self.latest_written_timestamp == 0 {
            return Err(TmslError::NotFound(format!(
                "no entry to delete at timestamp {} (dataset is empty)",
                timestamp
            )));
        }
        if self.is_timestamp_expired(timestamp) {
            return Err(self.expired_error(timestamp));
        }

        let old_entry = self.time_index.find_and_delete_entry(timestamp)?;
        self.invalidate_cache_for_entry(&old_entry, cache);
        // Old entry references real data → increment invalid_record_count on its segment
        self.segments
            .increment_invalid_record_count(old_entry.block_offset)?;

        self.last_used_at = Instant::now();
        Ok(())
    }

    fn invalidate_cache_for_entry(&self, entry: &IndexEntry, cache: Option<&BlockCache>) {
        if entry.block_offset == BLOCK_OFFSET_FILLER {
            return;
        }
        if let Some(cache) = cache {
            let key = self
                .segments
                .cache_key_for_absolute_offset(entry.block_offset);
            cache.invalidate(&key);
        }
    }

    /// Read a single record by exact timestamp.
    ///
    /// Special case: `timestamp == -1` resolves to `latest_written_timestamp` and
    /// reads the newest record. Returns `None` if the dataset is empty or the
    /// latest entry has been deleted.
    ///
    /// Returns `Ok(Some((timestamp, data)))` if found, `Ok(None)` if not found
    /// or entry is a filler (deleted or never-written in continuous mode).
    pub fn read(
        &mut self,
        timestamp: i64,
        cache: Option<&BlockCache>,
    ) -> Result<Option<(i64, Vec<u8>)>> {
        let effective_ts = if timestamp == -1 {
            if self.latest_written_timestamp <= 0 {
                return Ok(None);
            }
            self.latest_written_timestamp
        } else {
            timestamp
        };

        if self.is_timestamp_expired(effective_ts) {
            return Ok(None);
        }

        let entry = match self.time_index.find_entry(effective_ts)? {
            Some(e) => e,
            None => return Ok(None),
        };
        if entry.block_offset == BLOCK_OFFSET_FILLER {
            return Ok(None);
        }
        let re = ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset,
            in_block_offset: entry.in_block_offset,
        };
        let (ts, data) = self.segments.read_at_index(&re, cache)?;
        self.last_used_at = Instant::now();
        Ok(Some((ts, data)))
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
        let sources = self.time_index.prepare_query_sources(start_ts, end_ts)?;
        Ok(QueryIterator::new_with_sources(
            sources,
            &mut self.segments,
            cache,
        ))
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

    pub fn query_sources(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<QuerySource>> {
        let (start_ts, end_ts) = self.clamp_query_range(start_ts, end_ts);
        if start_ts > end_ts {
            return Ok(Vec::new());
        }
        self.time_index.prepare_query_sources(start_ts, end_ts)
    }

    pub fn read_entry_at_index(
        &mut self,
        entry: &IndexEntry,
        cache: Option<&BlockCache>,
    ) -> Result<(i64, Vec<u8>)> {
        if self.is_timestamp_expired(entry.timestamp) {
            return Err(self.expired_error(entry.timestamp));
        }
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
                    let pos = seg.header_size as usize
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
                    let pos = seg.header_size as usize
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

    /// Latest successfully written timestamp (0 = dataset is empty).
    ///
    /// Recovered from index segments on `open`, then maintained in memory.
    /// Used by `read(-1)` shortcut and retention threshold calculation.
    pub fn latest_written_timestamp(&self) -> i64 {
        self.latest_written_timestamp
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

    fn is_timestamp_expired(&self, timestamp: i64) -> bool {
        let threshold = self.retention_threshold();
        threshold >= 0 && timestamp < threshold
    }

    fn expired_error(&self, timestamp: i64) -> TmslError {
        TmslError::Expired(format!(
            "timestamp {} is older than retention threshold {}",
            timestamp,
            self.retention_threshold()
        ))
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

    fn numeric_index_files(dir: &std::path::Path) -> Vec<i64> {
        let mut starts: Vec<i64> = std::fs::read_dir(dir.join("index"))
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.parse::<i64>().ok())
            })
            .collect();
        starts.sort_unstable();
        starts
    }

    fn make_cache_dataset(name: &str) -> DataSet {
        let dir = temp_dir(name);
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        DataSet::create(
            id,
            dir,
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap()
    }

    #[test]
    fn test_block_offset_routes_to_next_data_segment_after_rollover() {
        let dir = temp_dir("block_offset_segment_rollover");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let data_segment_size = 180;
        let mut ds = DataSet::create(
            id,
            dir,
            data_segment_size,
            4 * 1024,
            6,
            64,
            0,
            data_segment_size,
            4 * 1024,
            0,
        )
        .unwrap();

        let first = vec![0x11; 32];
        let second = vec![0x22; 32];

        ds.write(10, &first).unwrap();
        ds.write(20, &second).unwrap();

        let index_entries = ds.query_index_entries(10, 20).unwrap();
        assert_eq!(index_entries.len(), 2);
        assert_eq!(index_entries[0].block_offset, 0);
        assert_eq!(index_entries[1].block_offset, data_segment_size);

        let rows = ds.query(10, 20, None).unwrap();
        assert_eq!(rows, vec![(10, first), (20, second)]);
    }

    #[test]
    fn test_continuous_first_write_does_not_fill_from_zero() {
        let dir = temp_dir("continuous_first_write_sparse");
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

        ds.write(100, b"first").unwrap();

        assert_eq!(ds.time_index.in_memory_buffer.len(), 1);
        assert_eq!(ds.time_index.in_memory_buffer[0].timestamp, 100);
        assert!(!ds.time_index.in_memory_buffer[0].is_filler());
        assert!(!dir.join("index").join("base").exists());
    }

    #[test]
    fn test_continuous_large_gap_filler_is_bounded_by_edge_segments() {
        let dir = temp_dir("continuous_large_gap_sparse");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let index_segment_size = 512;
        let segment_capacity = ((index_segment_size - crate::INDEX_HEADER_SIZE)
            / crate::INDEX_ENTRY_SIZE as u64) as usize;
        let first_ts = 10;
        let second_ts = first_ts + segment_capacity as i64 * 4 + 5;
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            index_segment_size,
            6,
            65536,
            1,
            256 * 1024,
            128,
            0,
        )
        .unwrap();

        ds.write(first_ts, b"first").unwrap();
        ds.write(second_ts, b"second").unwrap();

        let filler_count = ds
            .time_index
            .in_memory_buffer
            .iter()
            .filter(|entry| entry.is_filler())
            .count();
        assert!(
            filler_count < 2 * segment_capacity - 2,
            "filler_count={} segment_capacity={}",
            filler_count,
            segment_capacity
        );
    }

    #[test]
    fn test_continuous_large_gap_flush_skips_middle_segments() {
        let dir = temp_dir("continuous_large_gap_flush");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let index_segment_size = 512;
        let segment_capacity = ((index_segment_size - crate::INDEX_HEADER_SIZE)
            / crate::INDEX_ENTRY_SIZE as u64) as i64;
        let first_ts = 10;
        let second_ts = first_ts + segment_capacity * 4 + 5;
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            index_segment_size,
            6,
            65536,
            1,
            256 * 1024,
            128,
            0,
        )
        .unwrap();

        ds.write(first_ts, b"first").unwrap();
        ds.write(second_ts, b"second").unwrap();
        ds.flush().unwrap();

        assert!(!dir.join("index").join("base").exists());
        assert_eq!(
            numeric_index_files(&dir),
            vec![first_ts, first_ts + segment_capacity * 4]
        );
        assert!(ds
            .read(first_ts + segment_capacity * 2, None)
            .unwrap()
            .is_none());

        let entries = ds.query(first_ts, second_ts, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, first_ts);
        assert_eq!(entries[1].0, second_ts);
    }

    #[test]
    fn test_continuous_backfill_logical_hole_creates_target_segment() {
        let dir = temp_dir("continuous_backfill_logical_hole");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let index_segment_size = 512;
        let segment_capacity = ((index_segment_size - crate::INDEX_HEADER_SIZE)
            / crate::INDEX_ENTRY_SIZE as u64) as i64;
        let first_ts = 10;
        let hole_ts = first_ts + segment_capacity * 2;
        let second_ts = first_ts + segment_capacity * 4 + 5;

        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                index_segment_size,
                6,
                65536,
                1,
                256 * 1024,
                128,
                0,
            )
            .unwrap();

            ds.write(first_ts, b"first").unwrap();
            ds.write(second_ts, b"second").unwrap();
            ds.flush().unwrap();
            ds.write(hole_ts, b"hole").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }

        assert_eq!(
            numeric_index_files(&dir),
            vec![
                first_ts,
                first_ts + segment_capacity * 2,
                first_ts + segment_capacity * 4
            ]
        );

        let mut ds = DataSet::open(id, dir.clone(), 65536).unwrap();
        assert_eq!(ds.latest_written_timestamp(), second_ts);
        let entries = ds.query(first_ts, second_ts, None).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, first_ts);
        assert_eq!(entries[1].0, hole_ts);
        assert_eq!(entries[1].1, b"hole");
        assert_eq!(entries[2].0, second_ts);
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

    #[test]
    fn test_retention_read_expired_returns_none() {
        let dir = temp_dir("retention_read_expired");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir,
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

        ds.write(100, b"old").unwrap();
        ds.write(200, b"new").unwrap();

        assert!(ds.read(100, None).unwrap().is_none());
        assert_eq!(ds.read(200, None).unwrap().unwrap().1, b"new");
    }

    #[test]
    fn test_retention_read_entry_at_index_rejects_expired_entry() {
        let dir = temp_dir("retention_read_entry_expired");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir,
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

        ds.write(100, b"old").unwrap();
        let old_entry = ds.time_index.find_entry(100).unwrap().unwrap();
        ds.write(200, b"new").unwrap();

        let err = ds.read_entry_at_index(&old_entry, None).unwrap_err();
        assert!(matches!(err, TmslError::Expired(_)));
    }

    #[test]
    fn test_retention_delete_expired_rejected() {
        let dir = temp_dir("retention_delete_expired");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir,
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

        ds.write(100, b"old").unwrap();
        ds.write(200, b"new").unwrap();

        let err = ds.delete(100).unwrap_err();
        assert!(matches!(err, TmslError::Expired(_)));
        assert!(ds.read(100, None).unwrap().is_none());
    }

    #[test]
    fn test_retention_out_of_order_rewrite_expired_rejected() {
        let dir = temp_dir("retention_ooo_expired");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir,
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

        ds.write(100, b"old").unwrap();
        ds.write(200, b"new").unwrap();

        let err = ds.write(100, b"rewrite").unwrap_err();
        assert!(matches!(err, TmslError::Expired(_)));
        assert!(ds.read(100, None).unwrap().is_none());
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

    // ─── Correction write across pending/compressed states ───────────────

    #[test]
    fn test_correction_write_preserves_pending_after_reopen() {
        // close/open preserves pending raw state, so same-timestamp correction can
        // still overwrite in place after reopen.
        let dir = temp_dir("correction_pending_reopen");
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
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        {
            let mut ds = DataSet::open(id, dir.clone(), 65536).unwrap();
            ds.write(100, b"corrected").unwrap();

            // Query should return the corrected data
            let entries = ds.query(100, 100, None).unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, 100);
            assert_eq!(entries[0].1, b"corrected");

            // latest_written_timestamp unchanged
            assert_eq!(ds.latest_written_timestamp, 100);

            let seg = ds.segments.segments.last().unwrap();
            assert_eq!(seg.invalid_record_count, 0);
        }
    }

    #[test]
    fn test_correction_write_falls_back_on_compressed_block() {
        // When a correction write targets a SEALED+COMPRESSED block (single-record
        // compression), in-place overwrite fails → falls back to out-of-order write.
        // Triggers compressed block via block_max_size=0 (forces any record > 0 bytes
        // to be written as an exclusive sealed block, which is compressed if effective).
        let dir = temp_dir("correction_fallback_compressed");
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
            0, // block_max_size=0 → every record is an exclusive block
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        // Use highly compressible data to ensure compression is effective → COMPRESSED flag
        let compressible = vec![0xAB_u8; 500];
        ds.write(100, &compressible).unwrap();

        // Correction write at ts=100 → target block is SEALED+COMPRESSED → falls back
        let corrected = vec![0xCD_u8; 300];
        ds.write(100, &corrected).unwrap();

        // Query should return the corrected data
        let entries = ds.query(100, 100, None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, corrected);

        // latest_written_timestamp unchanged
        assert_eq!(ds.latest_written_timestamp, 100);

        // invalid_record_count should be 1 (old data orphaned)
        let seg = ds.segments.segments.last().unwrap();
        assert_eq!(seg.invalid_record_count, 1);
    }

    #[test]
    fn test_correction_fallback_invalidates_cached_compressed_block() {
        let mut ds = make_cache_dataset("cache_correction_fallback");
        let cache = BlockCache::new(1024 * 1024);
        let original = vec![0xAB_u8; 500];
        let corrected = vec![0xCD_u8; 300];

        ds.write(100, &original).unwrap();
        assert_eq!(ds.read(100, Some(&cache)).unwrap().unwrap().1, original);
        assert_eq!(cache.stats().entry_count, 1);

        ds.write_with_cache(100, &corrected, Some(&cache)).unwrap();

        assert_eq!(cache.stats().entry_count, 0);
        assert_eq!(ds.read(100, None).unwrap().unwrap().1, corrected);
    }

    #[test]
    fn test_out_of_order_invalidates_cached_compressed_block() {
        let mut ds = make_cache_dataset("cache_out_of_order");
        let cache = BlockCache::new(1024 * 1024);
        let original = vec![0xAB_u8; 500];
        let updated = vec![0xEF_u8; 250];

        ds.write(100, &original).unwrap();
        ds.write(200, b"latest").unwrap();
        assert_eq!(ds.read(100, Some(&cache)).unwrap().unwrap().1, original);
        assert_eq!(cache.stats().entry_count, 1);

        ds.write_with_cache(100, &updated, Some(&cache)).unwrap();

        assert_eq!(cache.stats().entry_count, 0);
        assert_eq!(ds.read(100, None).unwrap().unwrap().1, updated);
    }

    #[test]
    fn test_delete_invalidates_cached_compressed_block() {
        let mut ds = make_cache_dataset("cache_delete");
        let cache = BlockCache::new(1024 * 1024);
        let original = vec![0xAB_u8; 500];

        ds.write(100, &original).unwrap();
        assert_eq!(ds.read(100, Some(&cache)).unwrap().unwrap().1, original);
        assert_eq!(cache.stats().entry_count, 1);

        ds.delete_with_cache(100, Some(&cache)).unwrap();

        assert_eq!(cache.stats().entry_count, 0);
        assert!(ds.read(100, None).unwrap().is_none());
    }

    #[test]
    fn test_correction_write_fallback_reopen_persistence() {
        // Correction-write fallback result must persist across close+reopen.
        let dir = temp_dir("correction_fallback_persist");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        // Phase 1: write multiple records, then close
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
            ds.write(100, b"first").unwrap();
            ds.write(200, b"last").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Phase 2: reopen and rewrite ts=100. This is out-of-order because latest is 200.
        {
            let mut ds = DataSet::open(id.clone(), dir.clone(), 65536).unwrap();
            ds.write(100, b"corrected_100").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Phase 3: reopen and verify
        {
            let mut ds = DataSet::open(id, dir, 65536).unwrap();
            let entries = ds.query(100, 200, None).unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].0, 100);
            assert_eq!(entries[0].1, b"corrected_100");
            assert_eq!(entries[1].0, 200);
            assert_eq!(entries[1].1, b"last");

            let seg = ds.segments.segments.last().unwrap();
            assert_eq!(seg.invalid_record_count, 1);
        }
    }

    // ─── read() tests ────────────────────────────────────────────────────

    #[test]
    fn test_read_found() {
        let dir = temp_dir("read_found");
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

        ds.write(100, b"hello").unwrap();
        ds.write(200, b"world").unwrap();

        // Read existing timestamp
        let result = ds.read(100, None).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 100);
        assert_eq!(data, b"hello");

        // Read second timestamp
        let result = ds.read(200, None).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 200);
        assert_eq!(data, b"world");
    }

    #[test]
    fn test_read_not_found() {
        let dir = temp_dir("read_not_found");
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

        ds.write(100, b"hello").unwrap();

        // Read non-existent timestamp → None
        let result = ds.read(999, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_deleted_returns_none() {
        let dir = temp_dir("read_deleted");
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

        ds.write(100, b"hello").unwrap();
        ds.write(200, b"world").unwrap();

        // Delete ts=100
        ds.delete(100).unwrap();

        // Read deleted timestamp → None (filler)
        let result = ds.read(100, None).unwrap();
        assert!(result.is_none());

        // Other timestamp still readable
        let result = ds.read(200, None).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"world");
    }

    #[test]
    fn test_read_continuous_filler_returns_none() {
        let dir = temp_dir("read_continuous_filler");
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

        ds.write(100, b"hello").unwrap();
        ds.write(110, b"world").unwrap();
        ds.flush().unwrap();

        // Filler positions (101..109) → None
        let result = ds.read(105, None).unwrap();
        assert!(result.is_none());

        // Real positions (100, 110) → Some
        let result = ds.read(100, None).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"hello");

        let result = ds.read(110, None).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"world");
    }

    #[test]
    fn test_read_after_reopen() {
        let dir = temp_dir("read_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };

        // Phase 1: write + close
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
            ds.write(100, b"persistent").unwrap();
            ds.write(200, b"data").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }

        // Phase 2: reopen and read
        {
            let mut ds = DataSet::open(id, dir, 65536).unwrap();

            let result = ds.read(100, None).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().1, b"persistent");

            let result = ds.read(200, None).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().1, b"data");

            let result = ds.read(999, None).unwrap();
            assert!(result.is_none());
        }
    }

    // ─── latest_written_timestamp() + read(-1) tests ────────────────────

    #[test]
    fn test_latest_written_timestamp_after_writes() {
        let dir = temp_dir("latest_ts_writes");
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

        // Empty dataset
        assert_eq!(ds.latest_written_timestamp(), 0);

        // First write sets latest
        ds.write(100, b"a").unwrap();
        assert_eq!(ds.latest_written_timestamp(), 100);

        ds.write(150, b"b").unwrap();
        assert_eq!(ds.latest_written_timestamp(), 150);

        // Correction write at 150 (== latest) keeps latest unchanged
        ds.write(150, b"corrected").unwrap();
        assert_eq!(ds.latest_written_timestamp(), 150);

        // Out-of-order write at an existing timestamp keeps latest unchanged
        ds.write(100, b"ooo_at_100").unwrap();
        assert_eq!(ds.latest_written_timestamp(), 150);
    }

    #[test]
    fn test_latest_written_timestamp_after_reopen() {
        let dir = temp_dir("latest_ts_reopen");
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
            ds.write(250, b"b").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }

        // Reopen → latest_written_timestamp recovered from index
        {
            let ds = DataSet::open(id, dir, 65536).unwrap();
            assert_eq!(ds.latest_written_timestamp(), 250);
        }
    }

    #[test]
    fn test_read_minus_one_empty_dataset() {
        let dir = temp_dir("read_minus_one_empty");
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

        assert!(ds.read(-1, None).unwrap().is_none());
    }

    #[test]
    fn test_read_minus_one_returns_latest() {
        let dir = temp_dir("read_minus_one_latest");
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
        ds.write(200, b"second").unwrap();
        ds.write(300, b"latest").unwrap();

        let result = ds.read(-1, None).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 300);
        assert_eq!(data, b"latest");
    }

    #[test]
    fn test_read_minus_one_after_delete_latest() {
        // After deleting the latest, latest_written_timestamp still points to it
        // but the index entry is a filler, so read(-1) returns None.
        let dir = temp_dir("read_minus_one_deleted_latest");
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
        ds.write(200, b"later").unwrap();
        ds.delete(200).unwrap();

        assert!(ds.read(-1, None).unwrap().is_none());

        // Earlier record still reachable via explicit timestamp
        let r = ds.read(100, None).unwrap().unwrap();
        assert_eq!(r.1, b"first");
    }

    #[test]
    fn test_read_minus_one_after_reopen() {
        let dir = temp_dir("read_minus_one_reopen");
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
            ds.write(500, b"latest").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }

        {
            let mut ds = DataSet::open(id, dir, 65536).unwrap();
            assert_eq!(ds.latest_written_timestamp(), 500);

            let r = ds.read(-1, None).unwrap().unwrap();
            assert_eq!(r.0, 500);
            assert_eq!(r.1, b"latest");
        }
    }
}
