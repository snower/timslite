//! TimeIndex: manages index segments with lazy lifecycle and time-range queries.
//!
//! Index entries are appended directly to mmap-backed index segments. Segments
//! are filled sequentially (not by hash).

pub mod segment;

use std::collections::{
    btree_map::{Entry, OccupiedEntry},
    BTreeMap,
};
use std::path::Path;

pub use self::segment::INDEX_ENTRY_SIZE;
use self::segment::{
    IndexEntry, IndexSegment, IndexSegmentMeta, BLOCK_OFFSET_FILLER, IN_BLOCK_OFFSET_FILLER,
};
use crate::bg::SegmentDirtySink;
use crate::error::{Result, TmslError};
use crate::header::{IndexFileMetadata, INDEX_HEADER_SIZE};

pub(crate) enum IndexSegmentEntryState {
    Open(IndexSegment),
    Closed(IndexSegmentMeta),
}

pub(crate) trait ArchivedIndexTimestampRangeSink {
    fn set_archived_index_timestamp_range(&mut self, range: Option<(i64, i64)>) -> Result<()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IndexEntryPosition {
    pub segment_start_timestamp: i64,
    pub entry_index: usize,
}

// 鈹€鈹€鈹€ TimeIndex 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

pub struct TimeIndex {
    pub base_dir: std::path::PathBuf,
    pub segment_size: u64,
    pub initial_segment_size: u64,
    pub compress_level: u8,
    pub compress_type: u8,
    pub(crate) index_segments: BTreeMap<i64, IndexSegmentEntryState>,
    pub index_continuous: bool, // true = continuous mode (O(1) lookup enabled)
    pub base_timestamp: Option<i64>,
    pub index_header_size: u64,
    dirty_sink: Option<SegmentDirtySink>,
}

impl TimeIndex {
    pub(crate) fn open_index_segments(&self) -> impl Iterator<Item = &IndexSegment> {
        self.index_segments
            .values()
            .filter_map(|entry| match entry {
                IndexSegmentEntryState::Open(seg) => Some(seg),
                IndexSegmentEntryState::Closed(_) => None,
            })
    }

    pub(crate) fn open_index_segments_mut(&mut self) -> impl Iterator<Item = &mut IndexSegment> {
        self.index_segments
            .values_mut()
            .filter_map(|entry| match entry {
                IndexSegmentEntryState::Open(seg) => Some(seg),
                IndexSegmentEntryState::Closed(_) => None,
            })
    }

    pub(crate) fn closed_index_segment_metas(&self) -> impl Iterator<Item = &IndexSegmentMeta> {
        self.index_segments
            .values()
            .filter_map(|entry| match entry {
                IndexSegmentEntryState::Open(_) => None,
                IndexSegmentEntryState::Closed(meta) => Some(meta),
            })
    }

    pub(crate) fn open_len(&self) -> usize {
        self.open_index_segments().count()
    }

    pub(crate) fn closed_len(&self) -> usize {
        self.closed_index_segment_metas().count()
    }

    pub(crate) fn total_len(&self) -> usize {
        self.index_segments.len()
    }

    pub(crate) fn set_dirty_sink(&mut self, dirty_sink: Option<SegmentDirtySink>) {
        self.dirty_sink = dirty_sink;
        let sink = self.dirty_sink.clone();
        for seg in self.open_index_segments_mut() {
            seg.set_dirty_sink(sink.clone());
        }
    }

    fn segment_timestamp_range(entry: &IndexSegmentEntryState) -> Option<(i64, i64)> {
        match entry {
            IndexSegmentEntryState::Open(seg) => {
                if seg.wrote_count == 0 {
                    None
                } else {
                    seg.last_timestamp_cached()
                        .map(|last| (seg.start_timestamp, last))
                }
            }
            IndexSegmentEntryState::Closed(meta) => {
                if meta.wrote_count == 0 {
                    None
                } else {
                    meta.last_timestamp.map(|last| (meta.start_timestamp, last))
                }
            }
        }
    }

    pub(crate) fn timestamp_range_snapshot(&self) -> Option<(i64, i64)> {
        let mut min_ts: Option<i64> = None;
        let mut max_ts: Option<i64> = None;

        for entry in self.index_segments.values() {
            if let Some((seg_min, seg_max)) = Self::segment_timestamp_range(entry) {
                min_ts = Some(min_ts.map_or(seg_min, |min| min.min(seg_min)));
                max_ts = Some(max_ts.map_or(seg_max, |max| max.max(seg_max)));
            }
        }

        match (min_ts, max_ts) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    pub(crate) fn archived_timestamp_range_snapshot(&self) -> Option<(i64, i64)> {
        let active_key = self.index_segments.last_key_value().map(|(key, _)| *key);
        let mut min_ts: Option<i64> = None;
        let mut max_ts: Option<i64> = None;

        for (key, entry) in &self.index_segments {
            if Some(*key) == active_key {
                continue;
            }
            if let Some((seg_min, seg_max)) = Self::segment_timestamp_range(entry) {
                min_ts = Some(min_ts.map_or(seg_min, |min| min.min(seg_min)));
                max_ts = Some(max_ts.map_or(seg_max, |max| max.max(seg_max)));
            }
        }

        match (min_ts, max_ts) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    pub(crate) fn active_timestamp_range_snapshot(&self) -> Option<(i64, i64)> {
        let mut min_ts: Option<i64> = None;
        let mut max_ts: Option<i64> = None;

        if let Some((_, entry)) = self.index_segments.last_key_value() {
            if let Some((seg_min, seg_max)) = Self::segment_timestamp_range(entry) {
                min_ts = Some(seg_min);
                max_ts = Some(seg_max);
            }
        }

        match (min_ts, max_ts) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    fn latest_materialized_timestamp(&self) -> Option<i64> {
        self.index_segments
            .values()
            .filter_map(Self::segment_timestamp_range)
            .map(|(_, max)| max)
            .max()
    }

    fn open_index_segment(&mut self, start_timestamp: i64) -> Result<&mut IndexSegment> {
        match self.index_segments.entry(start_timestamp) {
            Entry::Occupied(entry) => {
                Self::open_occupied_index_segment(entry, self.segment_size, self.dirty_sink.clone())
            }
            Entry::Vacant(_) => Err(TmslError::NotFound(format!(
                "no index segment at {}",
                start_timestamp
            ))),
        }
    }

    fn open_occupied_index_segment(
        entry: OccupiedEntry<'_, i64, IndexSegmentEntryState>,
        segment_size: u64,
        dirty_sink: Option<SegmentDirtySink>,
    ) -> Result<&mut IndexSegment> {
        let start_timestamp = *entry.key();
        let entry = entry.into_mut();
        if let IndexSegmentEntryState::Closed(meta) = entry {
            let path = meta.path.clone();
            let meta_start = meta.start_timestamp;
            let mut seg = IndexSegment::open(&path, meta_start, segment_size)?;
            seg.set_dirty_sink(dirty_sink);
            *entry = IndexSegmentEntryState::Open(seg);
        }
        match entry {
            IndexSegmentEntryState::Open(seg) => Ok(seg),
            IndexSegmentEntryState::Closed(_) => Err(TmslError::NotFound(format!(
                "no index segment at {}",
                start_timestamp
            ))),
        }
    }

    fn segment_start_candidate_for_ts(&self, timestamp: i64) -> Option<i64> {
        self.index_segments
            .range(..=timestamp)
            .next_back()
            .map(|(start, _)| *start)
    }

    fn segment_keys_for_range(&self, start_ts: i64, end_ts: i64) -> Vec<i64> {
        let mut keys = Vec::new();
        if let Some((key, _)) = self.index_segments.range(..start_ts).next_back() {
            keys.push(*key);
        }
        keys.extend(
            self.index_segments
                .range(start_ts..=end_ts)
                .map(|(key, _)| *key),
        );
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    fn first_segment_key_for_range(&self, start_ts: i64, end_ts: i64) -> Option<i64> {
        self.index_segments
            .range(..start_ts)
            .next_back()
            .or_else(|| self.index_segments.range(start_ts..=end_ts).next())
            .map(|(key, _)| *key)
    }

    fn last_segment_key_for_range(&self, _start_ts: i64, end_ts: i64) -> Option<i64> {
        self.index_segments
            .range(..=end_ts)
            .next_back()
            .map(|(key, _)| *key)
    }

    fn next_segment_key_after(&self, segment_start_timestamp: i64) -> Option<i64> {
        self.index_segments
            .range((
                std::ops::Bound::Excluded(segment_start_timestamp),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .map(|(key, _)| *key)
    }

    fn previous_segment_key_before(&self, segment_start_timestamp: i64) -> Option<i64> {
        self.index_segments
            .range(..segment_start_timestamp)
            .next_back()
            .map(|(key, _)| *key)
    }

    /// Create a new TimeIndex.
    pub fn new(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        index_continuous: bool,
    ) -> Result<Self> {
        Self::new_with_compression(
            base_dir,
            segment_size,
            initial_segment_size,
            index_continuous,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
        )
    }

    pub fn new_with_compression(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        index_continuous: bool,
        compress_level: u8,
        compress_type: u8,
    ) -> Result<Self> {
        std::fs::create_dir_all(base_dir)?;
        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            segment_size,
            initial_segment_size,
            compress_level,
            compress_type,
            index_segments: BTreeMap::new(),
            index_continuous,
            base_timestamp: None,
            index_header_size: INDEX_HEADER_SIZE,
            dirty_sink: None,
        })
    }

    /// Add a filler entry (sentinel for continuous mode).
    pub fn add_filler_entry(&mut self, timestamp: i64) -> Result<()> {
        let entry = IndexEntry::new(
            timestamp,
            crate::index::segment::BLOCK_OFFSET_FILLER,
            crate::index::segment::IN_BLOCK_OFFSET_FILLER,
        );
        if self.index_continuous {
            self.ensure_base_timestamp(timestamp)?;
            self.append_continuous_entry_to_disk(&entry, None)
        } else {
            self.append_noncontinuous_entry_to_disk(&entry, None)
        }
    }

    /// Add an entry directly to the mmap-backed index segment.
    pub fn add_entry(
        &mut self,
        timestamp: i64,
        block_offset: u64,
        in_block_offset: u16,
    ) -> Result<()> {
        self.add_entry_with_archived_range_sink(timestamp, block_offset, in_block_offset, None)
    }

    pub(crate) fn add_entry_with_archived_range_sink(
        &mut self,
        timestamp: i64,
        block_offset: u64,
        in_block_offset: u16,
        archived_range_sink: Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<()> {
        if self.index_continuous && self.base_timestamp.is_none() {
            self.ensure_base_timestamp(timestamp)?;
        }
        let entry = IndexEntry::new(timestamp, block_offset, in_block_offset);
        if self.index_continuous {
            self.append_continuous_entry_to_disk(&entry, archived_range_sink)?;
        } else {
            self.append_noncontinuous_entry_to_disk(&entry, archived_range_sink)?;
        }
        Ok(())
    }

    /// Add a real entry in continuous mode without materializing full middle gaps.
    pub fn add_sparse_continuous_entry(
        &mut self,
        previous_latest: Option<i64>,
        timestamp: i64,
        block_offset: u64,
        in_block_offset: u16,
    ) -> Result<()> {
        self.add_sparse_continuous_entry_with_archived_range_sink(
            previous_latest,
            timestamp,
            block_offset,
            in_block_offset,
            None,
        )
    }

    pub(crate) fn add_sparse_continuous_entry_with_archived_range_sink(
        &mut self,
        previous_latest: Option<i64>,
        timestamp: i64,
        block_offset: u64,
        in_block_offset: u16,
        archived_range_sink: Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<()> {
        if !self.index_continuous {
            return self.add_entry_with_archived_range_sink(
                timestamp,
                block_offset,
                in_block_offset,
                archived_range_sink,
            );
        }

        self.ensure_base_timestamp(timestamp)?;
        let real_entry = IndexEntry::new(timestamp, block_offset, in_block_offset);

        if let Some(previous_latest) = previous_latest {
            let prev_segment_start = self.segment_start_for(previous_latest)?;
            let curr_segment_start = self.segment_start_for(timestamp)?;
            if prev_segment_start == curr_segment_start {
                self.push_filler_range(previous_latest + 1, timestamp - 1)?;
            } else {
                let segment_capacity = self.segment_capacity()? as i64;
                let prev_segment_end = prev_segment_start + segment_capacity - 1;
                self.push_filler_range(previous_latest + 1, prev_segment_end)?;
                self.push_filler_range(curr_segment_start, timestamp - 1)?;
            }
        }

        self.append_continuous_entry_to_disk(&real_entry, archived_range_sink)?;
        Ok(())
    }

    fn ensure_base_timestamp(&mut self, timestamp: i64) -> Result<i64> {
        if let Some(base) = self.base_timestamp {
            if timestamp < base {
                return Err(TmslError::NotFound(format!(
                    "timestamp {} is before continuous index base {}",
                    timestamp, base
                )));
            }
            return Ok(base);
        }

        self.base_timestamp = Some(timestamp);
        Ok(timestamp)
    }

    fn segment_capacity(&self) -> Result<usize> {
        if self.segment_size <= self.index_header_size {
            return Err(TmslError::InvalidData(format!(
                "index segment size {} is too small",
                self.segment_size
            )));
        }
        let capacity =
            ((self.segment_size - self.index_header_size) / INDEX_ENTRY_SIZE as u64) as usize;
        if capacity == 0 {
            return Err(TmslError::InvalidData(format!(
                "index segment size {} cannot hold an index entry",
                self.segment_size
            )));
        }
        Ok(capacity)
    }

    pub fn segment_start_for(&self, timestamp: i64) -> Result<i64> {
        let base = self
            .base_timestamp
            .ok_or_else(|| TmslError::NotFound("continuous index base timestamp missing".into()))?;
        if timestamp < base {
            return Err(TmslError::NotFound(format!(
                "timestamp {} is before continuous index base {}",
                timestamp, base
            )));
        }
        let capacity = self.segment_capacity()? as i64;
        let ordinal = (timestamp - base) / capacity;
        Ok(base + ordinal * capacity)
    }

    pub(crate) fn flush_target_start_for_timestamp(&self, timestamp: i64) -> Result<i64> {
        if self.index_continuous {
            return self.segment_start_for(timestamp);
        }
        self.segment_start_candidate_for_ts(timestamp)
            .ok_or_else(|| {
                TmslError::NotFound(format!("no index segment for timestamp {}", timestamp))
            })
    }

    fn entry_index_for(&self, timestamp: i64) -> Result<usize> {
        let segment_start = self.segment_start_for(timestamp)?;
        Ok((timestamp - segment_start) as usize)
    }

    fn push_filler_range(&mut self, start: i64, end: i64) -> Result<()> {
        if start > end {
            return Ok(());
        }
        for ts in start..=end {
            self.add_filler_entry(ts)?;
        }
        Ok(())
    }

    fn materialized_count_for_segment(&self, segment_start: i64) -> Result<usize> {
        let mut count = 0usize;

        if let Some(entry) = self.index_segments.get(&segment_start) {
            match entry {
                IndexSegmentEntryState::Open(seg) => count = count.max(seg.wrote_count),
                IndexSegmentEntryState::Closed(meta) => count = count.max(meta.wrote_count),
            }
        }

        Ok(count)
    }

    /// Find the IndexEntry at the given timestamp (for correction write).
    ///
    /// Searches the unified segment registry.
    /// Returns `Ok(None)` if not found.
    pub fn find_entry(&mut self, timestamp: i64) -> Result<Option<IndexEntry>> {
        let ic = self.index_continuous;

        let segment_start = if ic {
            match self.segment_start_for(timestamp) {
                Ok(start) => Some(start),
                Err(TmslError::NotFound(_)) => None,
                Err(e) => return Err(e),
            }
        } else {
            self.segment_start_candidate_for_ts(timestamp)
        };
        let Some(segment_start) = segment_start else {
            return Ok(None);
        };
        let seg = match self.open_index_segment(segment_start) {
            Ok(seg) => seg,
            Err(TmslError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(seg.find_exact_cs(timestamp, ic))
    }

    /// Update the index entry at `timestamp` with a new (block_offset, in_block_offset),
    /// returning the old entry. Used for out-of-order writes.
    ///
    /// The caller is responsible for examining the returned old entry to determine
    /// whether `invalid_record_count` should be incremented on the old data segment
    /// (i.e. when old_entry.block_offset != BLOCK_OFFSET_FILLER).
    ///
    /// Returns Err(NotFound) if no entry exists at `timestamp`.
    pub fn update_entry(
        &mut self,
        timestamp: i64,
        new_block_offset: u64,
        new_in_block_offset: u16,
    ) -> Result<IndexEntry> {
        self.update_entry_with_archived_range_sink(
            timestamp,
            new_block_offset,
            new_in_block_offset,
            None,
        )
    }

    pub(crate) fn update_entry_with_archived_range_sink(
        &mut self,
        timestamp: i64,
        new_block_offset: u64,
        new_in_block_offset: u16,
        archived_range_sink: Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<IndexEntry> {
        let ic = self.index_continuous;
        let new_entry = IndexEntry::new(timestamp, new_block_offset, new_in_block_offset);

        let segment_start = if ic {
            Some(self.segment_start_for(timestamp)?)
        } else {
            self.segment_start_candidate_for_ts(timestamp)
        };
        if let Some(segment_start) = segment_start {
            let seg = match self.open_index_segment(segment_start) {
                Ok(seg) => seg,
                Err(TmslError::NotFound(_)) => {
                    return self.upsert_or_not_found(
                        ic,
                        timestamp,
                        new_block_offset,
                        new_in_block_offset,
                        archived_range_sink,
                    )
                }
                Err(e) => return Err(e),
            };
            if let Some((idx, old)) = seg.find_entry_index_and_entry_cs(timestamp, ic, None) {
                seg.overwrite_entry(idx, &new_entry)?;
                return Ok(old);
            }
        }

        self.upsert_or_not_found(
            ic,
            timestamp,
            new_block_offset,
            new_in_block_offset,
            archived_range_sink,
        )
    }

    fn upsert_or_not_found(
        &mut self,
        index_continuous: bool,
        timestamp: i64,
        new_block_offset: u64,
        new_in_block_offset: u16,
        archived_range_sink: Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<IndexEntry> {
        if index_continuous {
            return self.upsert_sparse_continuous_entry(
                timestamp,
                new_block_offset,
                new_in_block_offset,
                archived_range_sink,
            );
        }

        Err(TmslError::NotFound(format!(
            "no index entry at timestamp {} (required for out-of-order write)",
            timestamp
        )))
    }

    fn upsert_sparse_continuous_entry(
        &mut self,
        timestamp: i64,
        new_block_offset: u64,
        new_in_block_offset: u16,
        archived_range_sink: Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<IndexEntry> {
        let segment_start = self.segment_start_for(timestamp)?;
        let entry_index = self.entry_index_for(timestamp)?;
        let materialized_count = self.materialized_count_for_segment(segment_start)?;

        if materialized_count > entry_index {
            return Err(TmslError::InvalidData(format!(
                "continuous index hole invariant violated at timestamp {}",
                timestamp
            )));
        }

        self.push_filler_range(segment_start + materialized_count as i64, timestamp - 1)?;
        let entry = IndexEntry::new(timestamp, new_block_offset, new_in_block_offset);
        self.append_continuous_entry_to_disk(&entry, archived_range_sink)?;

        Ok(IndexEntry::new(
            timestamp,
            BLOCK_OFFSET_FILLER,
            IN_BLOCK_OFFSET_FILLER,
        ))
    }

    /// Find the index entry at `timestamp` and mark it as sentinel (deleted).
    /// Returns the old entry so the caller can locate its data segment and
    /// increment `invalid_record_count` there.
    ///
    /// Returns Err(NotFound) if no entry exists or the entry is already a filler.
    pub fn find_and_delete_entry(&mut self, timestamp: i64) -> Result<IndexEntry> {
        let ic = self.index_continuous;
        let sentinel = IndexEntry::new(timestamp, BLOCK_OFFSET_FILLER, IN_BLOCK_OFFSET_FILLER);

        let segment_start = if ic {
            Some(self.segment_start_for(timestamp)?)
        } else {
            self.segment_start_candidate_for_ts(timestamp)
        };
        if let Some(segment_start) = segment_start {
            let seg = match self.open_index_segment(segment_start) {
                Ok(seg) => seg,
                Err(TmslError::NotFound(_)) => {
                    return Err(TmslError::NotFound(format!(
                        "no entry at timestamp {} to delete",
                        timestamp
                    )));
                }
                Err(e) => return Err(e),
            };
            if let Some((idx, old)) = seg.find_entry_index_and_entry_cs(timestamp, ic, None) {
                if old.is_filler() {
                    return Err(TmslError::NotFound(format!(
                        "no real data at timestamp {} (filler)",
                        timestamp
                    )));
                }
                seg.overwrite_entry(idx, &sentinel)?;
                return Ok(old);
            }
        }

        Err(TmslError::NotFound(format!(
            "no entry at timestamp {} to delete",
            timestamp
        )))
    }

    fn append_with_expansion(seg: &mut IndexSegment, entry: &IndexEntry) -> Result<()> {
        loop {
            match seg.append_entry(entry) {
                Ok(()) => return Ok(()),
                Err(TmslError::SegmentFull) => {
                    if seg.current_file_size >= seg.max_file_size {
                        seg.seal()?;
                        seg.sync()?;
                        return Err(TmslError::SegmentFull);
                    }
                    seg.expand()?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn append_noncontinuous_entry_to_disk(
        &mut self,
        entry: &IndexEntry,
        mut archived_range_sink: Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<()> {
        if let Some(latest) = self.latest_materialized_timestamp() {
            if entry.timestamp <= latest {
                return Err(TmslError::InvalidData(format!(
                    "non-continuous index append timestamp {} must be greater than latest {}",
                    entry.timestamp, latest
                )));
            }
        }
        loop {
            let seg = self.get_or_create_segment_for_ts_with_sink(
                entry.timestamp,
                &mut archived_range_sink,
            )?;
            match Self::append_with_expansion(seg, entry) {
                Ok(()) => return Ok(()),
                Err(TmslError::SegmentFull) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn append_continuous_entry_to_disk(
        &mut self,
        entry: &IndexEntry,
        archived_range_sink: Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<()> {
        let segment_start = self.segment_start_for(entry.timestamp)?;
        let entry_index = self.entry_index_for(entry.timestamp)?;
        let mut archived_range_sink = archived_range_sink;
        let seg =
            self.get_or_create_segment_by_start_with_sink(segment_start, &mut archived_range_sink)?;
        if seg.wrote_count != entry_index {
            return Err(TmslError::InvalidData(format!(
                "continuous index append expected entry_index {}, got wrote_count {}",
                entry_index, seg.wrote_count
            )));
        }
        Self::append_with_expansion(seg, entry).map_err(|e| match e {
            TmslError::SegmentFull => TmslError::InvalidData(format!(
                "continuous index segment {} is full before timestamp {}",
                segment_start, entry.timestamp
            )),
            other => other,
        })
    }

    fn get_or_create_segment_for_ts_with_sink(
        &mut self,
        start_ts: i64,
        archived_range_sink: &mut Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<&mut IndexSegment> {
        if let Some(latest_key) = self.index_segments.last_key_value().map(|(key, _)| *key) {
            let latest_available = {
                let latest = self.open_index_segment(latest_key)?;
                !latest.is_full()
                    && IndexEntry::timestamp_delta_for_segment(start_ts, latest.start_timestamp)
                        .is_ok()
            };
            if latest_available {
                return self.open_index_segment(latest_key);
            }
        }

        self.get_or_create_segment_by_start_with_sink(start_ts, archived_range_sink)
    }

    fn get_or_create_segment_by_start(&mut self, segment_start: i64) -> Result<&mut IndexSegment> {
        let mut archived_range_sink = None;
        self.get_or_create_segment_by_start_with_sink(segment_start, &mut archived_range_sink)
    }

    fn get_or_create_segment_by_start_with_sink(
        &mut self,
        segment_start: i64,
        archived_range_sink: &mut Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<&mut IndexSegment> {
        if self.index_segments.contains_key(&segment_start) {
            return self.open_index_segment(segment_start);
        }

        if let Some(latest_key) = self.index_segments.last_key_value().map(|(key, _)| *key) {
            if let Some(IndexSegmentEntryState::Open(seg)) =
                self.index_segments.get_mut(&latest_key)
            {
                seg.sync()?;
            }
        }

        self.refresh_archived_range_before_segment_create(archived_range_sink)?;
        let mut seg = IndexSegment::create_with_compression(
            &self.base_dir,
            segment_start,
            self.initial_segment_size,
            self.segment_size,
            self.compress_level,
            self.compress_type,
        )?;
        seg.set_dirty_sink(self.dirty_sink.clone());
        self.index_segments
            .insert(segment_start, IndexSegmentEntryState::Open(seg));
        match self.index_segments.get_mut(&segment_start) {
            Some(IndexSegmentEntryState::Open(seg)) => Ok(seg),
            _ => unreachable!(),
        }
    }

    fn refresh_archived_range_before_segment_create(
        &self,
        archived_range_sink: &mut Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<()> {
        let Some(sink) = archived_range_sink.as_mut() else {
            return Ok(());
        };
        sink.set_archived_index_timestamp_range(self.timestamp_range_snapshot())
    }

    /// Query entries in the time range [start_ts, end_ts].
    /// In continuous mode, uses O(1) direct calculation for segment lookups.
    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<IndexEntry>> {
        let mut results = Vec::new();
        let ic = self.index_continuous;

        for key in self.segment_keys_for_range(start_ts, end_ts) {
            let seg = self.open_index_segment(key)?;
            seg.ensure_open()?;
            results.extend(seg.query_range_cs(start_ts, end_ts, ic));
        }

        Ok(results)
    }

    pub(crate) fn next_query_entry_at_or_after(
        &mut self,
        start_ts: i64,
        end_ts: i64,
        cursor: Option<IndexEntryPosition>,
    ) -> Result<Option<(IndexEntryPosition, IndexEntry)>> {
        if start_ts > end_ts {
            return Ok(None);
        }

        let ic = self.index_continuous;
        let mut key = match cursor {
            Some(cursor)
                if self
                    .index_segments
                    .contains_key(&cursor.segment_start_timestamp) =>
            {
                Some(cursor.segment_start_timestamp)
            }
            Some(cursor) => self.next_segment_key_after(cursor.segment_start_timestamp),
            None => self.first_segment_key_for_range(start_ts, end_ts),
        };

        while let Some(segment_start_timestamp) = key {
            if segment_start_timestamp > end_ts {
                return Ok(None);
            }

            let next_key = self.next_segment_key_after(segment_start_timestamp);
            let after_cursor_index = cursor
                .filter(|cursor| cursor.segment_start_timestamp == segment_start_timestamp)
                .and_then(|cursor| cursor.entry_index.checked_add(1));

            let seg = self.open_index_segment(segment_start_timestamp)?;
            seg.ensure_open()?;
            let lower_bound = seg.lower_bound_cs(start_ts, ic);
            let start_index =
                after_cursor_index.map_or(lower_bound, |index| index.max(lower_bound));

            for entry_index in start_index..seg.wrote_count {
                let entry = seg.read_entry_at_index(entry_index)?;
                if entry.timestamp < start_ts {
                    continue;
                }
                if entry.timestamp > end_ts {
                    return Ok(None);
                }
                return Ok(Some((
                    IndexEntryPosition {
                        segment_start_timestamp,
                        entry_index,
                    },
                    entry,
                )));
            }

            key = next_key;
        }

        Ok(None)
    }

    pub(crate) fn next_query_entry_at_or_before(
        &mut self,
        start_ts: i64,
        end_ts: i64,
        cursor: Option<IndexEntryPosition>,
    ) -> Result<Option<(IndexEntryPosition, IndexEntry)>> {
        if start_ts > end_ts {
            return Ok(None);
        }

        let ic = self.index_continuous;
        let mut key = match cursor {
            Some(cursor)
                if self
                    .index_segments
                    .contains_key(&cursor.segment_start_timestamp) =>
            {
                Some(cursor.segment_start_timestamp)
            }
            Some(cursor) => self.previous_segment_key_before(cursor.segment_start_timestamp),
            None => self.last_segment_key_for_range(start_ts, end_ts),
        };

        while let Some(segment_start_timestamp) = key {
            let previous_key = self.previous_segment_key_before(segment_start_timestamp);
            let before_cursor_index = cursor
                .filter(|cursor| cursor.segment_start_timestamp == segment_start_timestamp)
                .and_then(|cursor| cursor.entry_index.checked_sub(1));

            let seg = self.open_index_segment(segment_start_timestamp)?;
            seg.ensure_open()?;
            let lower_bound = seg.lower_bound_cs(start_ts, ic);
            let upper_bound = seg.upper_bound_cs(end_ts, ic);
            if lower_bound >= upper_bound {
                key = previous_key;
                continue;
            }
            let last_index =
                before_cursor_index.map_or(upper_bound - 1, |index| index.min(upper_bound - 1));
            if last_index < lower_bound {
                key = previous_key;
                continue;
            }

            for entry_index in (lower_bound..=last_index).rev() {
                let entry = seg.read_entry_at_index(entry_index)?;
                if entry.timestamp > end_ts {
                    continue;
                }
                if entry.timestamp < start_ts {
                    return Ok(None);
                }
                return Ok(Some((
                    IndexEntryPosition {
                        segment_start_timestamp,
                        entry_index,
                    },
                    entry,
                )));
            }

            key = previous_key;
        }

        Ok(None)
    }

    // 鈹€鈹€鈹€ Lifecycle management 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

    pub fn sync_all(&mut self) -> Result<()> {
        for seg in self.open_index_segments_mut() {
            seg.sync()?;
        }
        Ok(())
    }

    pub(crate) fn sync_segment(&mut self, start_timestamp: i64) -> Result<()> {
        if let Some(IndexSegmentEntryState::Open(seg)) =
            self.index_segments.get_mut(&start_timestamp)
        {
            if !seg.is_flushed {
                seg.sync()?;
            }
        }
        Ok(())
    }

    pub fn idle_close_all(&mut self) -> Result<()> {
        let keys: Vec<i64> = self
            .index_segments
            .iter()
            .filter_map(|(key, entry)| match entry {
                IndexSegmentEntryState::Open(_) => Some(*key),
                IndexSegmentEntryState::Closed(_) => None,
            })
            .collect();
        for key in keys {
            let Some(IndexSegmentEntryState::Open(mut seg)) = self.index_segments.remove(&key)
            else {
                continue;
            };
            let last_timestamp = seg.last_timestamp();
            let meta = IndexSegmentMeta::new_with_last_timestamp(
                seg.path.clone(),
                seg.start_timestamp,
                seg.entries_capacity,
                seg.wrote_count,
                seg.header_size,
                last_timestamp,
            );
            seg.idle_close()?;
            self.index_segments
                .insert(key, IndexSegmentEntryState::Closed(meta));
        }
        Ok(())
    }

    /// Delete index segments whose last entry timestamp is strictly less than `threshold`.
    /// Must be called only when all segments are closed (via idle_close_all).
    /// Returns the number of files removed.
    pub fn reclaim_expired_segments(
        &mut self,
        threshold: i64,
        _max_file_size: u64,
    ) -> Result<usize> {
        let before = self.index_segments.len();
        self.index_segments.retain(|_, entry| {
            let IndexSegmentEntryState::Closed(meta) = entry else {
                return true;
            };
            match segment::last_entry_timestamp(&meta.path) {
                Ok(Some(last_ts)) if last_ts < threshold => {
                    let _ = std::fs::remove_file(&meta.path);
                    log::info!("[retention] deleted index segment: {:?}", meta.path);
                    false
                }
                Ok(None) => {
                    let _ = std::fs::remove_file(&meta.path);
                    log::info!("[retention] deleted empty index segment: {:?}", meta.path);
                    false
                }
                Ok(_) => true,
                Err(e) => {
                    log::warn!(
                        "[retention] failed to read {:?}: {} (keeping)",
                        meta.path,
                        e
                    );
                    true
                }
            }
        });
        Ok(before - self.index_segments.len())
    }

    /// Load existing index segments from disk.
    /// Index files are in the `index/` subdirectory.
    pub fn load_existing(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        index_continuous: bool,
    ) -> Result<Self> {
        Self::load_existing_with_compression(
            base_dir,
            segment_size,
            initial_segment_size,
            index_continuous,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
        )
    }

    pub fn load_existing_with_compression(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        index_continuous: bool,
        compress_level: u8,
        compress_type: u8,
    ) -> Result<Self> {
        let mut metas: Vec<IndexSegmentMeta> = Vec::new();
        if base_dir.exists() {
            for entry in std::fs::read_dir(base_dir)? {
                let p = entry?.path();
                if !p.is_file() {
                    continue;
                }
                if let Some(stem) = p.file_stem().and_then(|n| n.to_str()) {
                    if let Ok(start_ts) = stem.parse::<i64>() {
                        let file_size = std::fs::metadata(&p)?.len();
                        let (wrote_count, header_size, last_timestamp) =
                            Self::read_record_count_from_file(&p);
                        let entries_capacity = ((file_size.saturating_sub(header_size))
                            / INDEX_ENTRY_SIZE as u64)
                            as usize;
                        metas.push(IndexSegmentMeta::new_with_last_timestamp(
                            p,
                            start_ts,
                            entries_capacity,
                            wrote_count,
                            header_size,
                            last_timestamp,
                        ));
                    }
                }
            }
        }
        metas.sort_by_key(|m| m.start_timestamp);
        let base_timestamp = if index_continuous {
            metas.first().map(|meta| meta.start_timestamp)
        } else {
            None
        };
        let index_header_size = metas
            .first()
            .map(|meta| meta.header_size)
            .unwrap_or(INDEX_HEADER_SIZE);

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            segment_size,
            initial_segment_size,
            compress_level,
            compress_type,
            index_segments: metas
                .into_iter()
                .map(|meta| (meta.start_timestamp, IndexSegmentEntryState::Closed(meta)))
                .collect(),
            index_continuous,
            base_timestamp,
            index_header_size,
            dirty_sink: None,
        })
    }

    /// Read wrote_count from the file header without fully opening the segment.
    fn read_record_count_from_file(path: &Path) -> (usize, u64, Option<i64>) {
        if let Ok(file) = std::fs::OpenOptions::new().read(true).open(path) {
            if let Ok(mmap) = unsafe { memmap2::MmapOptions::new().map(&file) } {
                if let Ok(metadata) = IndexFileMetadata::read_from(&mmap) {
                    let header_size = metadata.header_size;
                    let count = ((metadata.wrote_position.saturating_sub(header_size))
                        / INDEX_ENTRY_SIZE as u64) as usize;
                    let last_timestamp = if count == 0 {
                        None
                    } else {
                        let pos = header_size as usize + (count - 1) * INDEX_ENTRY_SIZE;
                        if pos + INDEX_ENTRY_SIZE <= mmap.len() {
                            let buf: [u8; INDEX_ENTRY_SIZE] =
                                mmap[pos..pos + INDEX_ENTRY_SIZE].try_into().unwrap();
                            IndexEntry::from_bytes_for_segment(metadata.file_offset, &buf)
                                .ok()
                                .map(|entry| entry.timestamp)
                        } else {
                            None
                        }
                    };
                    return (count, header_size, last_timestamp);
                }
            }
        }
        (0, INDEX_HEADER_SIZE, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bg::{DataSetFlushTarget, SegmentDirtySink, SegmentFlushTarget};
    use crate::dataset::DataSetKey;
    use std::fs;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct CaptureArchivedRangeSink {
        calls: Vec<Option<(i64, i64)>>,
    }

    impl ArchivedIndexTimestampRangeSink for CaptureArchivedRangeSink {
        fn set_archived_index_timestamp_range(&mut self, range: Option<(i64, i64)>) -> Result<()> {
            self.calls.push(range);
            Ok(())
        }
    }

    fn temp_dir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join("timslite_test_index");
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn fresh_subdir(name: &str) -> std::path::PathBuf {
        let sub = temp_dir().join(name);
        let _ = fs::remove_dir_all(&sub);
        fs::create_dir_all(&sub).unwrap();
        sub
    }

    fn missing_meta(dir: &Path, start_timestamp: i64) -> IndexSegmentMeta {
        IndexSegmentMeta::new(
            dir.join(format!("missing_{}", start_timestamp)),
            start_timestamp,
            4,
            4,
            INDEX_HEADER_SIZE,
        )
    }

    #[test]
    fn test_time_index_dirty_sink_enqueues_from_index_write() {
        let sub = fresh_subdir("dirty_sink_index_append");
        let mut idx = TimeIndex::new(&sub, 512, 512, false).unwrap();
        let dataset = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let queue: Arc<Mutex<std::collections::VecDeque<DataSetFlushTarget>>> =
            Arc::new(Mutex::new(std::collections::VecDeque::new()));
        idx.set_dirty_sink(Some(SegmentDirtySink::new(dataset.clone(), queue.clone())));

        idx.add_entry(100, 0, 0).unwrap();
        idx.add_entry(200, 32, 0).unwrap();

        let queue = queue.lock().unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].dataset, dataset);
        assert!(matches!(
            queue[0].segment,
            SegmentFlushTarget::Index {
                start_timestamp: 100
            }
        ));
    }

    #[test]
    fn test_time_index_archived_range_sink_runs_before_new_segment_create() {
        let sub = fresh_subdir("archived_range_sink_before_index_create");
        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();
        let mut sink = CaptureArchivedRangeSink::default();

        for ts in 100..105 {
            idx.add_entry_with_archived_range_sink(ts, ts as u64, 0, Some(&mut sink))
                .unwrap();
        }
        assert_eq!(sink.calls, vec![None]);
        assert_eq!(idx.total_len(), 1);

        idx.add_entry_with_archived_range_sink(105, 105, 0, Some(&mut sink))
            .unwrap();

        assert_eq!(sink.calls, vec![None, Some((100, 104))]);
        assert_eq!(idx.total_len(), 2);
    }

    #[test]
    fn test_index_entry_roundtrip() {
        let entry = IndexEntry::new(1234567890, 1024, 42);
        let bytes = entry.to_bytes_for_segment(1234567000).unwrap();
        let parsed = IndexEntry::from_bytes_for_segment(1234567000, &bytes).unwrap();
        assert_eq!(entry, parsed);
    }

    #[test]
    fn test_index_entry_binary_size() {
        assert_eq!(INDEX_ENTRY_SIZE, 14);
        let entry = IndexEntry::new(42, u64::MAX, u16::MAX);
        assert_eq!(entry.to_bytes_for_segment(0).unwrap().len(), 14);
    }

    #[test]
    fn test_continuous_segment_capacity_uses_fixed_index_area_start() {
        let dir = temp_dir();
        let sub = dir.join("continuous_fixed_index_area_start");
        let _ = fs::remove_dir_all(&sub);
        fs::create_dir_all(&sub).unwrap();

        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();
        idx.ensure_base_timestamp(1000).unwrap();

        assert_eq!(idx.segment_capacity().unwrap(), 5);
        assert_eq!(idx.segment_start_for(1003).unwrap(), 1000);
        assert_eq!(idx.segment_start_for(1004).unwrap(), 1000);
        assert_eq!(idx.segment_start_for(1005).unwrap(), 1005);
    }

    #[test]
    fn test_index_segment_append_and_query() {
        let dir = temp_dir();
        let sub = dir.join("append_query");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 1000, 4096, 4096).unwrap();

        for i in 0..50 {
            let entry = IndexEntry::new(1000 + i, 128 + i as u64 * 100, (i * 5) as u16);
            seg.append_entry(&entry).unwrap();
        }

        let entries = seg.query_range_cs(1010, 1020, false);
        assert_eq!(entries.len(), 11);
        assert_eq!(entries[0].timestamp, 1010);
        assert_eq!(entries.last().unwrap().timestamp, 1020);
    }

    #[test]
    fn test_time_index_registries_stay_sorted_across_lazy_open() {
        let sub = fresh_subdir("ordered_registry_lazy_open");
        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();
        for ts in 100..108 {
            idx.add_entry(ts, ts as u64, 0).unwrap();
        }
        idx.sync_all().unwrap();
        assert_eq!(
            idx.open_index_segments()
                .map(|seg| seg.start_timestamp)
                .collect::<Vec<_>>(),
            vec![100, 105]
        );

        idx.idle_close_all().unwrap();
        assert_eq!(idx.open_len(), 0);
        assert_eq!(
            idx.closed_index_segment_metas()
                .map(|meta| meta.start_timestamp)
                .collect::<Vec<_>>(),
            vec![100, 105]
        );

        idx.get_or_create_segment_by_start(105).unwrap();
        idx.get_or_create_segment_by_start(100).unwrap();
        assert_eq!(
            idx.open_index_segments()
                .map(|seg| seg.start_timestamp)
                .collect::<Vec<_>>(),
            vec![100, 105]
        );
    }

    #[test]
    fn test_add_entry_writes_index_segment_immediately() {
        let sub = fresh_subdir("add_entry_immediate_segment_write");
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false).unwrap();

        idx.add_entry(100, 2048, 7).unwrap();

        assert_eq!(idx.total_len(), 1);
        let segment_path = match idx.index_segments.get(&100).unwrap() {
            IndexSegmentEntryState::Open(seg) => seg.path.clone(),
            IndexSegmentEntryState::Closed(meta) => meta.path.clone(),
        };
        let reopened = IndexSegment::open(&segment_path, 100, 4096).unwrap();
        let entry = reopened.find_exact(100).unwrap();
        assert_eq!(entry.block_offset, 2048);
        assert_eq!(entry.in_block_offset, 7);
    }

    #[test]
    fn test_sparse_continuous_add_does_not_cleanup_pure_filler_segments() {
        let sub = fresh_subdir("sparse_continuous_add_no_inline_cleanup");
        let mut idx = TimeIndex::new(&sub, 512, 512, true).unwrap();

        idx.ensure_base_timestamp(100).unwrap();
        let capacity = idx.segment_capacity().unwrap() as i64;
        idx.push_filler_range(100 + capacity, 100 + capacity + 1)
            .unwrap();
        assert!(idx.index_segments.contains_key(&(100 + capacity)));

        idx.add_sparse_continuous_entry(None, 100 + capacity * 2, 2048, 7)
            .unwrap();

        assert!(
            idx.index_segments.contains_key(&(100 + capacity)),
            "index writes must not run pure-filler cleanup inline"
        );
    }

    #[test]
    fn test_sparse_continuous_add_flushes_previous_edge_segment() {
        let sub = fresh_subdir("sparse_continuous_add_flushes_previous_edge");
        let mut idx = TimeIndex::new(&sub, 512, 512, true).unwrap();

        idx.add_sparse_continuous_entry(None, 100, 1000, 0).unwrap();
        idx.sync_segment(100).unwrap();
        let capacity = idx.segment_capacity().unwrap() as i64;
        let target_ts = 100 + capacity * 2 + 1;

        idx.add_sparse_continuous_entry(Some(100), target_ts, 2000, 0)
            .unwrap();

        let previous = match idx.index_segments.get(&100).unwrap() {
            IndexSegmentEntryState::Open(seg) => seg,
            IndexSegmentEntryState::Closed(_) => unreachable!(),
        };
        assert!(
            previous.is_flushed,
            "previous edge segment should be flushed after filler completion"
        );

        let target_start = idx.segment_start_for(target_ts).unwrap();
        let target = match idx.index_segments.get(&target_start).unwrap() {
            IndexSegmentEntryState::Open(seg) => seg,
            IndexSegmentEntryState::Closed(_) => unreachable!(),
        };
        assert!(
            !target.is_flushed,
            "target segment remains dirty for normal enqueue"
        );
    }

    #[test]
    fn test_add_entry_rejects_out_of_order_direct_append() {
        let sub = fresh_subdir("add_entry_rejects_out_of_order");
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false).unwrap();

        idx.add_entry(100, 1000, 0).unwrap();
        idx.add_entry(200, 2000, 0).unwrap();

        assert!(idx.add_entry(150, 1500, 0).is_err());
    }

    #[test]
    fn test_continuous_find_entry_uses_computed_closed_segment() {
        let sub = fresh_subdir("continuous_direct_closed_lookup");
        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();
        for ts in 100..108 {
            idx.add_entry(ts, ts as u64, 0).unwrap();
        }
        idx.sync_all().unwrap();
        idx.idle_close_all().unwrap();
        let meta = missing_meta(&sub, 50);
        idx.index_segments
            .insert(meta.start_timestamp, IndexSegmentEntryState::Closed(meta));

        let entry = idx.find_entry(102).unwrap().unwrap();
        assert_eq!(entry.timestamp, 102);
        assert_eq!(entry.block_offset, 102);
    }

    #[test]
    fn test_noncontinuous_update_delete_skip_unrelated_closed_segment() {
        let sub = fresh_subdir("noncontinuous_candidate_closed_lookup");
        let mut idx = TimeIndex::new(&sub, 200, 200, false).unwrap();
        idx.add_entry(100, 1000, 1).unwrap();
        idx.add_entry(101, 1001, 1).unwrap();
        idx.sync_all().unwrap();
        idx.idle_close_all().unwrap();
        let meta = missing_meta(&sub, 0);
        idx.index_segments
            .insert(meta.start_timestamp, IndexSegmentEntryState::Closed(meta));

        let old = idx.update_entry(101, 9001, 9).unwrap();
        assert_eq!(old.block_offset, 1001);

        let updated = idx.find_entry(101).unwrap().unwrap();
        assert_eq!(updated.block_offset, 9001);
        assert_eq!(updated.in_block_offset, 9);

        let deleted = idx.find_and_delete_entry(101).unwrap();
        assert_eq!(deleted.block_offset, 9001);
        assert!(idx.find_and_delete_entry(101).is_err());
    }

    #[test]
    fn test_noncontinuous_flush_splits_segment_when_delta_exceeds_u32() {
        let sub = fresh_subdir("noncontinuous_delta_split");
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false).unwrap();

        idx.add_entry(0, 100, 0).unwrap();
        idx.add_entry(u32::MAX as i64 + 1, 200, 0).unwrap();
        idx.sync_all().unwrap();

        assert_eq!(idx.total_len(), 2);
        assert!(idx.index_segments.contains_key(&0));
        assert!(idx.index_segments.contains_key(&(u32::MAX as i64 + 1)));
    }

    #[test]
    fn test_index_segment_lower_bound() {
        let dir = temp_dir();
        let sub = dir.join("lower_bound");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096).unwrap();
        for i in 0..10 {
            seg.append_entry(&IndexEntry::new(i * 10, i as u64, 0))
                .unwrap();
        }

        assert_eq!(seg.lower_bound_cs(25, false), 3);
        assert_eq!(seg.lower_bound_cs(20, false), 2);
        assert_eq!(seg.lower_bound_cs(-5, false), 0);
        assert_eq!(seg.lower_bound_cs(100, false), 10);

        // Continuous mode (entries are contiguous from 0): O(1)
        // Continuous: range [0..9], target 25 鈫?out of range 鈫?wrote_count
        assert_eq!(seg.lower_bound_cs(9, true), 9);
        assert_eq!(seg.lower_bound_cs(10, true), 10); // out of range 鈫?wrote_count
        assert_eq!(seg.lower_bound_cs(-5, true), 0);
        assert_eq!(seg.lower_bound_cs(25, true), 10); // out of range 鈫?wrote_count
    }

    #[test]
    fn test_index_segment_direct_lookup() {
        let dir = temp_dir();
        let sub = dir.join("direct_lookup");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        // Create segment starting at ts=100, continuous entries
        let mut seg = IndexSegment::create(&sub, 100, 4096, 4096).unwrap();
        for i in 0..20 {
            seg.append_entry(&IndexEntry::new(100 + i, i as u64, (i * 3) as u16))
                .unwrap();
        }

        // Direct lookup within range
        let entry = seg.direct_lookup(105).unwrap();
        assert_eq!(entry.timestamp, 105);
        assert_eq!(entry.block_offset, 5);

        let entry = seg.direct_lookup(100).unwrap();
        assert_eq!(entry.timestamp, 100);
        assert_eq!(entry.block_offset, 0);

        let entry = seg.direct_lookup(119).unwrap();
        assert_eq!(entry.timestamp, 119);
        assert_eq!(entry.block_offset, 19);

        // Out of range
        assert!(seg.direct_lookup(99).is_none());
        assert!(seg.direct_lookup(120).is_none());
        assert!(seg.direct_lookup(0).is_none());
    }

    #[test]
    fn test_index_segment_find_entry_index_cs() {
        let dir = temp_dir();
        let sub = dir.join("find_entry_index_cs");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096).unwrap();
        for i in 0..10 {
            seg.append_entry(&IndexEntry::new(10 + i, i as u64, 0))
                .unwrap();
        }

        // Non-continuous mode (binary search)
        assert_eq!(seg.find_entry_index_cs(15, false, None), Some(5));
        assert_eq!(seg.find_entry_index_cs(99, false, None), None);

        // Continuous mode (O(1) direct calculation)
        assert_eq!(seg.find_entry_index_cs(10, true, None), Some(0));
        assert_eq!(seg.find_entry_index_cs(15, true, None), Some(5));
        assert_eq!(seg.find_entry_index_cs(19, true, None), Some(9));
        assert_eq!(seg.find_entry_index_cs(9, true, None), None);
        assert_eq!(seg.find_entry_index_cs(20, true, None), None);
    }

    #[test]
    fn test_index_segment_find_exact_cs() {
        let dir = temp_dir();
        let sub = dir.join("find_exact_cs");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096).unwrap();
        for i in 0..10 {
            seg.append_entry(&IndexEntry::new(10 + i, i as u64 * 100, 0))
                .unwrap();
        }

        // Non-continuous
        let e = seg.find_exact_cs(13, false).unwrap();
        assert_eq!(e.block_offset, 300);

        // Continuous
        let e = seg.find_exact_cs(17, true).unwrap();
        assert_eq!(e.block_offset, 700);
        assert!(seg.find_exact_cs(99, true).is_none());
    }

    #[test]
    fn test_index_segment_query_range_cs() {
        let dir = temp_dir();
        let sub = dir.join("query_range_cs");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096).unwrap();
        for i in 0..20 {
            seg.append_entry(&IndexEntry::new(10 + i, i as u64, 0))
                .unwrap();
        }

        // Non-continuous
        let r = seg.query_range_cs(15, 19, false);
        assert_eq!(r.len(), 5);

        // Continuous
        let r = seg.query_range_cs(15, 19, true);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0].timestamp, 15);
        assert_eq!(r[4].timestamp, 19);

        // Edge: start before segment
        let r = seg.query_range_cs(0, 13, true);
        assert_eq!(r.len(), 4); // 10, 11, 12, 13

        // Edge: end after segment (range 25..50, segment has 10..29)
        let r = seg.query_range_cs(25, 50, true);
        assert_eq!(r.len(), 5); // timestamps 25, 26, 27, 28, 29
    }

    #[test]
    fn test_index_segment_upper_bound_cs() {
        let dir = temp_dir();
        let sub = dir.join("upper_bound_cs");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096).unwrap();
        for i in 0..10 {
            seg.append_entry(&IndexEntry::new(10 + i, i as u64, 0))
                .unwrap();
        }

        // Non-continuous
        assert_eq!(seg.upper_bound_cs(15, false), 6);
        assert_eq!(seg.upper_bound_cs(9, false), 0);
        assert_eq!(seg.upper_bound_cs(20, false), 10);

        // Continuous
        assert_eq!(seg.upper_bound_cs(15, true), 6); // 10+1-10+6
        assert_eq!(seg.upper_bound_cs(9, true), 0); // before range
        assert_eq!(seg.upper_bound_cs(19, true), 10); // at last entry -> return wrote_count
        assert_eq!(seg.upper_bound_cs(20, true), 10); // after range
    }

    #[test]
    fn test_time_index_add_and_query() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut idx = TimeIndex::new(&dir, 4096, 4096, false).unwrap();

        for i in 0..20 {
            idx.add_entry(1000 + i, i as u64 * 200, (i * 3) as u16)
                .unwrap();
        }

        // Query should find all 20 directly from index segments.
        let entries = idx.query(1000, 1019).unwrap();
        assert_eq!(entries.len(), 20);
    }

    #[test]
    fn test_time_index_flush_and_reopen() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut idx = TimeIndex::new(&dir, 4096, 4096, false).unwrap();

        for i in 0..100 {
            idx.add_entry(2000 + i, i as u64 * 300, (i * 7) as u16)
                .unwrap();
        }
        idx.sync_all().unwrap();

        // Load fresh
        let mut idx2 = TimeIndex::load_existing(&dir, 4096, 4096, false).unwrap();
        // Query all
        let entries = idx2.query(2000, 2099).unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_time_index_sync_keeps_pure_filler_segments() {
        let sub = fresh_subdir("pure_filler_retained");
        let mut idx = TimeIndex::new(&sub, 512, 512, true).unwrap();

        idx.ensure_base_timestamp(1000).unwrap();
        let capacity = idx.segment_capacity().unwrap() as i64;
        let filler_start = 1000 + capacity;
        idx.push_filler_range(filler_start, filler_start + 2)
            .unwrap();
        assert!(idx.index_segments.contains_key(&filler_start));

        idx.sync_all().unwrap();

        assert!(
            idx.index_segments.contains_key(&filler_start),
            "index sync must not remove pure-filler segments"
        );
        let entries = idx.query(filler_start, filler_start + 1).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(IndexEntry::is_filler));
    }

    #[test]
    fn test_timestamp_range_snapshot() {
        let sub = fresh_subdir("ts_range_snapshot");
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false).unwrap();

        // Empty index has no range
        assert_eq!(idx.timestamp_range_snapshot(), None);

        // Entries are immediately visible through index segments.
        idx.add_entry(100, 1000, 0).unwrap();
        idx.add_entry(150, 1500, 0).unwrap();
        idx.add_entry(200, 2000, 0).unwrap();
        assert_eq!(idx.timestamp_range_snapshot(), Some((100, 200)));

        // Flush to disk and check range across segments
        idx.sync_all().unwrap();
        assert_eq!(idx.timestamp_range_snapshot(), Some((100, 200)));

        // Add more entries after flush
        idx.add_entry(300, 3000, 0).unwrap();
        assert_eq!(idx.timestamp_range_snapshot(), Some((100, 300)));
    }

    #[test]
    fn test_archived_timestamp_range_snapshot() {
        let sub = fresh_subdir("archived_ts_range");
        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();

        // Single segment 鈥?nothing is archived (the only segment is active)
        idx.add_entry(100, 100, 0).unwrap();
        idx.add_entry(101, 101, 0).unwrap();
        idx.sync_all().unwrap();
        assert_eq!(idx.archived_timestamp_range_snapshot(), None);

        // Force second segment by writing beyond first segment capacity
        for ts in 102..107 {
            idx.add_entry(ts, ts as u64, 0).unwrap();
        }
        idx.sync_all().unwrap();

        // Now first segment should be archived
        let archived = idx.archived_timestamp_range_snapshot();
        assert!(archived.is_some());
        let (min, max) = archived.unwrap();
        assert!(min <= 101);
        assert!(max >= 100);
    }

    #[test]
    fn test_active_timestamp_range_snapshot() {
        let sub = fresh_subdir("active_ts_range");
        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();

        // Nothing active when empty
        assert_eq!(idx.active_timestamp_range_snapshot(), None);

        // Use sequential timestamps for continuous mode.
        idx.add_entry(100, 100, 0).unwrap();
        idx.add_entry(101, 101, 0).unwrap();
        idx.add_entry(102, 102, 0).unwrap();
        assert_eq!(idx.active_timestamp_range_snapshot(), Some((100, 102)));

        // Flush to disk 鈥?active timestamp range should still be correct
        idx.sync_all().unwrap();
        let range = idx.active_timestamp_range_snapshot();
        assert!(range.is_some());
        let (min, max) = range.unwrap();
        assert!(min >= 100);
        assert!(max <= 102);
    }

    #[test]
    fn test_open_len_closed_len() {
        let sub = fresh_subdir("open_closed_len");
        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();

        // Start: all segments are open, none closed
        idx.add_entry(100, 100, 0).unwrap();
        idx.add_entry(101, 101, 0).unwrap();
        idx.add_entry(102, 102, 0).unwrap();
        idx.sync_all().unwrap();
        assert_eq!(idx.open_len(), 1);
        assert_eq!(idx.closed_len(), 0);

        // After idle_close_all: all become closed
        idx.idle_close_all().unwrap();
        assert_eq!(idx.open_len(), 0);
        assert_eq!(idx.closed_len(), 1);

        // Re-open a segment
        idx.get_or_create_segment_by_start(100).unwrap();
        assert_eq!(idx.open_len(), 1);
        assert_eq!(idx.closed_len(), 0);
    }

    #[test]
    fn test_timestamp_range_snapshot_with_closed_segments() {
        let sub = fresh_subdir("ts_range_closed");
        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();

        for ts in 100..108 {
            idx.add_entry(ts, ts as u64, 0).unwrap();
        }
        idx.sync_all().unwrap();

        // Verify range before closing
        let range = idx.timestamp_range_snapshot();
        assert_eq!(range, Some((100, 107)));

        // Close all and verify range still works
        idx.idle_close_all().unwrap();
        assert_eq!(idx.timestamp_range_snapshot(), Some((100, 107)));
    }
}
