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
    inspect_index_file, IndexEntry, IndexSegment, IndexSegmentMeta, BLOCK_OFFSET_FILLER,
    IN_BLOCK_OFFSET_FILLER,
};
use crate::bg::SegmentDirtySink;
use crate::config::index_entry_stride;
use crate::error::{Result, TmslError};
use crate::header::INDEX_HEADER_SIZE;

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
    pub index_data_size: u32,
    pub entry_stride: usize,
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
            Entry::Occupied(entry) => Self::open_occupied_index_segment(
                entry,
                self.segment_size,
                self.index_data_size,
                self.dirty_sink.clone(),
            ),
            Entry::Vacant(_) => Err(TmslError::NotFound(format!(
                "no index segment at {}",
                start_timestamp
            ))),
        }
    }

    fn open_occupied_index_segment(
        entry: OccupiedEntry<'_, i64, IndexSegmentEntryState>,
        segment_size: u64,
        index_data_size: u32,
        dirty_sink: Option<SegmentDirtySink>,
    ) -> Result<&mut IndexSegment> {
        let start_timestamp = *entry.key();
        let entry = entry.into_mut();
        if let IndexSegmentEntryState::Closed(meta) = entry {
            let path = meta.path.clone();
            let meta_start = meta.start_timestamp;
            let mut seg = IndexSegment::open(&path, meta_start, segment_size, index_data_size)?;
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
        index_data_size: u32,
    ) -> Result<Self> {
        Self::new_with_compression(
            base_dir,
            segment_size,
            initial_segment_size,
            index_continuous,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            index_data_size,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_compression(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        index_continuous: bool,
        compress_level: u8,
        compress_type: u8,
        index_data_size: u32,
    ) -> Result<Self> {
        std::fs::create_dir_all(base_dir)?;
        let entry_stride = index_entry_stride(index_data_size)?;
        let min_size = INDEX_HEADER_SIZE + entry_stride as u64;
        if segment_size < min_size || initial_segment_size < min_size {
            return Err(TmslError::InvalidData(format!(
                "index segment sizes must be >= {min_size} for {entry_stride}-byte entries"
            )));
        }
        if initial_segment_size > segment_size {
            return Err(TmslError::InvalidData(
                "initial_index_segment_size must be <= index_segment_size".into(),
            ));
        }
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
            index_data_size,
            entry_stride,
            dirty_sink: None,
        })
    }

    /// Add a filler entry (sentinel for continuous mode).
    ///
    /// `base_block_offset` is the triggering real entry's absolute block start;
    /// filler-only segment creation must never capture a post-write cursor.
    pub fn add_filler_entry(&mut self, timestamp: i64, base_block_offset: u64) -> Result<()> {
        let entry = IndexEntry::new(
            timestamp,
            crate::index::segment::BLOCK_OFFSET_FILLER,
            crate::index::segment::IN_BLOCK_OFFSET_FILLER,
        );
        if self.index_continuous {
            self.ensure_base_timestamp(timestamp)?;
            self.append_continuous_entry_to_disk(&entry, base_block_offset, None)
        } else {
            self.append_noncontinuous_entry_to_disk(&entry, base_block_offset, None)
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
            self.append_continuous_entry_to_disk(&entry, block_offset, archived_range_sink)?;
        } else {
            self.append_noncontinuous_entry_to_disk(&entry, block_offset, archived_range_sink)?;
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

        // Preflight the real entry before filler materialization or archived
        // range publication.
        let curr_segment_start = self.segment_start_for(timestamp)?;
        self.preflight_real_entry(&real_entry, curr_segment_start)?;

        if let Some(previous_latest) = previous_latest {
            let prev_segment_start = self.segment_start_for(previous_latest)?;
            // checked: i64::MIN timestamps have no predecessor, so the
            // filler gap before them is empty rather than an overflow.
            if let (Some(gap_start), Some(gap_end)) =
                (previous_latest.checked_add(1), timestamp.checked_sub(1))
            {
                if prev_segment_start == curr_segment_start {
                    self.push_filler_range(gap_start, gap_end, block_offset)?;
                } else {
                    let segment_capacity = self.segment_capacity()? as i64;
                    // A boundary segment truncated by i64::MAX ends at MAX.
                    let prev_segment_end = prev_segment_start.saturating_add(segment_capacity - 1);
                    self.push_filler_range(gap_start, prev_segment_end, block_offset)?;
                    self.push_filler_range(curr_segment_start, gap_end, block_offset)?;
                }
            }
        }

        self.append_continuous_entry_to_disk(&real_entry, block_offset, archived_range_sink)?;
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
            ((self.segment_size - self.index_header_size) / self.entry_stride as u64) as usize;
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
        // base and timestamp are valid i64 values whose span can exceed the
        // i64 range; segment math runs in i128 and the boundary must stay
        // representable as an i64 timestamp.
        let capacity = self.segment_capacity()? as i128;
        let ordinal = (timestamp as i128 - base as i128) / capacity;
        let start = base as i128 + ordinal * capacity;
        i64::try_from(start).map_err(|_| {
            TmslError::InvalidData(format!(
                "continuous segment boundary {} for timestamp {} is not representable",
                start, timestamp
            ))
        })
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

    fn push_filler_range(&mut self, start: i64, end: i64, base_block_offset: u64) -> Result<()> {
        if start > end {
            return Ok(());
        }
        for ts in start..=end {
            self.add_filler_entry(ts, base_block_offset)?;
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

        let entry = IndexEntry::new(timestamp, new_block_offset, new_in_block_offset);
        // Preflight before any filler materialization.
        self.preflight_real_entry(&entry, segment_start)?;

        // checked_sub: timestamp i64::MIN leaves no room for a gap before it.
        if let Some(gap_end) = timestamp.checked_sub(1) {
            self.push_filler_range(
                segment_start + materialized_count as i64,
                gap_end,
                new_block_offset,
            )?;
        }
        self.append_continuous_entry_to_disk(&entry, new_block_offset, archived_range_sink)?;

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

    fn preflight_against_base(&self, entry: &IndexEntry, base_block_offset: u64) -> Result<()> {
        if entry.is_filler() {
            return Ok(());
        }
        let custom_storage;
        let custom: &[u8] = if self.index_data_size == 0 {
            &[]
        } else {
            custom_storage = vec![0u8; self.index_data_size as usize];
            &custom_storage
        };
        entry
            .to_bytes_for_disk(
                base_block_offset,
                self.entry_stride,
                self.index_data_size,
                custom,
            )
            .map(|_| ())
    }

    fn segment_base_for_start(&self, segment_start: i64) -> Option<u64> {
        match self.index_segments.get(&segment_start) {
            Some(IndexSegmentEntryState::Open(seg)) => Some(seg.base_block_offset),
            Some(IndexSegmentEntryState::Closed(meta)) => Some(meta.base_block_offset),
            None => None,
        }
    }

    /// Validate a real entry against the base of its target segment (or the
    /// entry's own block start when the segment does not exist yet) before any
    /// index/filler/archive side effect.
    fn preflight_real_entry(&self, entry: &IndexEntry, segment_start: i64) -> Result<()> {
        if entry.is_filler() {
            return Ok(());
        }
        let base = self
            .segment_base_for_start(segment_start)
            .unwrap_or(entry.block_offset);
        self.preflight_against_base(entry, base)
    }

    fn append_noncontinuous_entry_to_disk(
        &mut self,
        entry: &IndexEntry,
        creation_base_block_offset: u64,
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

        // Determine the exact base that will be used by the first append and
        // preflight before creating/syncing/publishing any segment state.
        let preflight_base =
            if let Some(latest_key) = self.index_segments.last_key_value().map(|(key, _)| *key) {
                let base = {
                    let latest = self.open_index_segment(latest_key)?;
                    if latest.is_full() {
                        creation_base_block_offset
                    } else {
                        latest.base_block_offset
                    }
                };
                base
            } else {
                creation_base_block_offset
            };
        self.preflight_against_base(entry, preflight_base)?;

        loop {
            let seg = self.get_or_create_segment_for_ts_with_sink(
                entry.timestamp,
                creation_base_block_offset,
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
        creation_base_block_offset: u64,
        archived_range_sink: Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<()> {
        let segment_start = self.segment_start_for(entry.timestamp)?;
        let entry_index = self.entry_index_for(entry.timestamp)?;
        let base = self
            .segment_base_for_start(segment_start)
            .unwrap_or(creation_base_block_offset);
        self.preflight_against_base(entry, base)?;

        let mut archived_range_sink = archived_range_sink;
        let seg = self.get_or_create_segment_by_start_with_sink(
            segment_start,
            creation_base_block_offset,
            &mut archived_range_sink,
        )?;
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
        creation_base_block_offset: u64,
        archived_range_sink: &mut Option<&mut dyn ArchivedIndexTimestampRangeSink>,
    ) -> Result<&mut IndexSegment> {
        if let Some(latest_key) = self.index_segments.last_key_value().map(|(key, _)| *key) {
            let latest_available = {
                let latest = self.open_index_segment(latest_key)?;
                // Global i64 timestamps: only capacity gates segment rotation now.
                !latest.is_full()
            };
            if latest_available {
                return self.open_index_segment(latest_key);
            }
        }

        self.get_or_create_segment_by_start_with_sink(
            start_ts,
            creation_base_block_offset,
            archived_range_sink,
        )
    }

    fn get_or_create_segment_by_start(
        &mut self,
        segment_start: i64,
        creation_base_block_offset: u64,
    ) -> Result<&mut IndexSegment> {
        let mut archived_range_sink = None;
        self.get_or_create_segment_by_start_with_sink(
            segment_start,
            creation_base_block_offset,
            &mut archived_range_sink,
        )
    }

    fn get_or_create_segment_by_start_with_sink(
        &mut self,
        segment_start: i64,
        creation_base_block_offset: u64,
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
            creation_base_block_offset,
            self.index_data_size,
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
                seg.base_block_offset,
                seg.index_data_size,
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
        index_data_size: u32,
    ) -> Result<Self> {
        Self::load_existing_with_compression(
            base_dir,
            segment_size,
            initial_segment_size,
            index_continuous,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            index_data_size,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn load_existing_with_compression(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        index_continuous: bool,
        compress_level: u8,
        compress_type: u8,
        index_data_size: u32,
    ) -> Result<Self> {
        let entry_stride = index_entry_stride(index_data_size)?;
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
                        let (wrote_count, header_size, last_timestamp, base_block_offset) =
                            inspect_index_file(
                                &p,
                                start_ts,
                                index_data_size,
                                segment_size,
                                index_continuous,
                            )
                            .map_err(|e| {
                                TmslError::InvalidData(format!(
                                    "malformed index segment {:?}: {e}",
                                    p
                                ))
                            })?;
                        let entries_capacity = ((file_size.saturating_sub(header_size))
                            / entry_stride as u64)
                            as usize;
                        metas.push(IndexSegmentMeta::new_with_last_timestamp(
                            p,
                            start_ts,
                            entries_capacity,
                            wrote_count,
                            header_size,
                            base_block_offset,
                            index_data_size,
                            last_timestamp,
                        ));
                    }
                }
            }
        }
        metas.sort_by_key(|m| m.start_timestamp);
        validate_loaded_segment_layout(&metas, segment_size, entry_stride, index_continuous)?;
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
            index_data_size,
            entry_stride,
            dirty_sink: None,
        })
    }
}

/// Validate cross-segment timestamp layout after strict inspection.
///
/// Continuous segments must start on the dataset base grid
/// (`base_timestamp + ordinal * max_capacity`); non-continuous segments must
/// stay strictly ordered with each segment ending before the next start.
fn validate_loaded_segment_layout(
    metas: &[IndexSegmentMeta],
    segment_size: u64,
    entry_stride: usize,
    index_continuous: bool,
) -> Result<()> {
    if metas.is_empty() {
        return Ok(());
    }
    if index_continuous {
        let capacity =
            segment::capacity_for_file_size(segment_size, INDEX_HEADER_SIZE, entry_stride)? as i128;
        let base = metas[0].start_timestamp as i128;
        for meta in metas {
            let delta = meta.start_timestamp as i128 - base;
            if delta < 0 || delta % capacity != 0 {
                return Err(TmslError::InvalidData(format!(
                    "continuous index segment start {} is not on base {} capacity {} grid",
                    meta.start_timestamp, base, capacity
                )));
            }
        }
    } else {
        for pair in metas.windows(2) {
            if let Some(previous_last) = pair[0].last_timestamp {
                if previous_last >= pair[1].start_timestamp {
                    return Err(TmslError::InvalidData(format!(
                        "non-continuous index segment range ending {} overlaps next segment start {}",
                        previous_last, pair[1].start_timestamp
                    )));
                }
            }
        }
    }
    Ok(())
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
            0,
            0,
        )
    }

    #[test]
    fn test_time_index_dirty_sink_enqueues_from_index_write() {
        let sub = fresh_subdir("dirty_sink_index_append");
        let mut idx = TimeIndex::new(&sub, 512, 512, false, 0).unwrap();
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
        // 208-byte segment = capacity 5 with 16-byte entries.
        let mut idx = TimeIndex::new(&sub, 208, 208, true, 0).unwrap();
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
        let bytes = entry.to_bytes_for_disk(1000, 16, 0, &[]).unwrap();
        let parsed = IndexEntry::from_bytes_for_disk(&bytes, 1000, 16, 0).unwrap();
        assert_eq!(entry, parsed);
    }

    #[test]
    fn test_index_entry_binary_size() {
        assert_eq!(INDEX_ENTRY_SIZE, 16);
        let entry = IndexEntry::new(42, 1000, u16::MAX - 1);
        assert_eq!(entry.to_bytes_for_disk(1000, 16, 0, &[]).unwrap().len(), 16);
        assert_eq!(
            entry
                .to_bytes_for_disk(1000, 32, 16, &[0; 16])
                .unwrap()
                .len(),
            32
        );
    }

    #[test]
    fn test_continuous_segment_capacity_uses_fixed_index_area_start() {
        let dir = temp_dir();
        let sub = dir.join("continuous_fixed_index_area_start");
        let _ = fs::remove_dir_all(&sub);
        fs::create_dir_all(&sub).unwrap();

        // 288-byte segment = capacity 10 with 16-byte entries.
        let mut idx = TimeIndex::new(&sub, 288, 288, true, 0).unwrap();
        idx.ensure_base_timestamp(1000).unwrap();

        assert_eq!(idx.segment_capacity().unwrap(), 10);
        assert_eq!(idx.segment_start_for(1003).unwrap(), 1000);
        assert_eq!(idx.segment_start_for(1009).unwrap(), 1000);
        assert_eq!(idx.segment_start_for(1010).unwrap(), 1010);
    }

    #[test]
    fn test_index_segment_append_and_query() {
        let dir = temp_dir();
        let sub = dir.join("append_query");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 1000, 4096, 4096, 0, 0).unwrap();

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
        // 208-byte segment = capacity 5 with 16-byte entries.
        let mut idx = TimeIndex::new(&sub, 208, 208, true, 0).unwrap();
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

        idx.get_or_create_segment_by_start(105, 0).unwrap();
        idx.get_or_create_segment_by_start(100, 0).unwrap();
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
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false, 0).unwrap();

        idx.add_entry(100, 2048, 7).unwrap();

        assert_eq!(idx.total_len(), 1);
        let segment_path = match idx.index_segments.get(&100).unwrap() {
            IndexSegmentEntryState::Open(seg) => seg.path.clone(),
            IndexSegmentEntryState::Closed(meta) => meta.path.clone(),
        };
        let reopened = IndexSegment::open(&segment_path, 100, 4096, 0).unwrap();
        let entry = reopened.find_exact(100).unwrap();
        assert_eq!(entry.block_offset, 2048);
        assert_eq!(entry.in_block_offset, 7);
    }

    #[test]
    fn test_sparse_continuous_add_does_not_cleanup_pure_filler_segments() {
        let sub = fresh_subdir("sparse_continuous_add_no_inline_cleanup");
        let mut idx = TimeIndex::new(&sub, 512, 512, true, 0).unwrap();

        idx.ensure_base_timestamp(100).unwrap();
        let capacity = idx.segment_capacity().unwrap() as i64;
        idx.push_filler_range(100 + capacity, 100 + capacity + 1, 0)
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
        let mut idx = TimeIndex::new(&sub, 512, 512, true, 0).unwrap();

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
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false, 0).unwrap();

        idx.add_entry(100, 1000, 0).unwrap();
        idx.add_entry(200, 2000, 0).unwrap();

        assert!(idx.add_entry(150, 1500, 0).is_err());
    }

    #[test]
    fn test_continuous_find_entry_uses_computed_closed_segment() {
        let sub = fresh_subdir("continuous_direct_closed_lookup");
        let mut idx = TimeIndex::new(&sub, 200, 200, true, 0).unwrap();
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
        let mut idx = TimeIndex::new(&sub, 200, 200, false, 0).unwrap();
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

    // Global i64 entry timestamps: a delta beyond u32 range no longer forces a
    // segment split; only capacity does.
    #[test]
    fn test_noncontinuous_global_i64_timestamp_does_not_split_segment() {
        let sub = fresh_subdir("noncontinuous_global_ts_no_split");
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false, 0).unwrap();

        idx.add_entry(0, 100, 0).unwrap();
        idx.add_entry(u32::MAX as i64 + 1, 200, 0).unwrap();
        idx.sync_all().unwrap();

        assert_eq!(idx.total_len(), 1);
        assert!(idx.index_segments.contains_key(&0));
        assert_eq!(
            idx.find_entry(u32::MAX as i64 + 1)
                .unwrap()
                .unwrap()
                .block_offset,
            200
        );
    }

    #[test]
    fn test_index_segment_lower_bound() {
        let dir = temp_dir();
        let sub = dir.join("lower_bound");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096, 0, 0).unwrap();
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
        let mut seg = IndexSegment::create(&sub, 100, 4096, 4096, 0, 0).unwrap();
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

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096, 0, 0).unwrap();
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

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096, 0, 0).unwrap();
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

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096, 0, 0).unwrap();
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

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096, 0, 0).unwrap();
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

        let mut idx = TimeIndex::new(&dir, 4096, 4096, false, 0).unwrap();

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

        let mut idx = TimeIndex::new(&dir, 4096, 4096, false, 0).unwrap();

        for i in 0..100 {
            idx.add_entry(2000 + i, i as u64 * 300, (i * 7) as u16)
                .unwrap();
        }
        idx.sync_all().unwrap();

        // Load fresh
        let mut idx2 = TimeIndex::load_existing(&dir, 4096, 4096, false, 0).unwrap();
        // Query all
        let entries = idx2.query(2000, 2099).unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_time_index_sync_keeps_pure_filler_segments() {
        let sub = fresh_subdir("pure_filler_retained");
        let mut idx = TimeIndex::new(&sub, 512, 512, true, 0).unwrap();

        idx.ensure_base_timestamp(1000).unwrap();
        let capacity = idx.segment_capacity().unwrap() as i64;
        let filler_start = 1000 + capacity;
        idx.push_filler_range(filler_start, filler_start + 2, 0)
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
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false, 0).unwrap();

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
        let mut idx = TimeIndex::new(&sub, 200, 200, true, 0).unwrap();

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
        // 288-byte segment = capacity 5 with 32-byte entries.
        let mut idx = TimeIndex::new(&sub, 288, 288, true, 0).unwrap();

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
        // 288-byte segment = capacity 5 with 32-byte entries.
        let mut idx = TimeIndex::new(&sub, 288, 288, true, 0).unwrap();

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
        idx.get_or_create_segment_by_start(100, 0).unwrap();
        assert_eq!(idx.open_len(), 1);
        assert_eq!(idx.closed_len(), 0);
    }

    #[test]
    fn test_timestamp_range_snapshot_with_closed_segments() {
        let sub = fresh_subdir("ts_range_closed");
        // 288-byte segment = capacity 5 with 32-byte entries.
        let mut idx = TimeIndex::new(&sub, 288, 288, true, 0).unwrap();

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

    #[test]
    fn test_compact_index_roundtrip_for_n_0_3_16() {
        for index_data_size in [0u32, 3, 16] {
            let sub = fresh_subdir(&format!("compact_roundtrip_n{index_data_size}"));
            let mut idx = TimeIndex::new(&sub, 4096, 4096, true, index_data_size).unwrap();
            // Same-block rollover: identical block offset, increasing units.
            idx.add_sparse_continuous_entry(None, 100, 5_000, 0)
                .unwrap();
            idx.add_entry(101, 5_000, 1).unwrap();
            idx.add_entry(102, 5_000, 2).unwrap();
            idx.add_sparse_continuous_entry(Some(102), 105, 7_000, 3)
                .unwrap();
            idx.sync_all().unwrap();
            idx.idle_close_all().unwrap();

            let mut reopened =
                TimeIndex::load_existing(&sub, 4096, 4096, true, index_data_size).unwrap();
            let entries = reopened.query(100, 105).unwrap();
            assert_eq!(entries.len(), 6);
            assert_eq!(entries[0].block_offset, 5_000);
            assert_eq!(entries[0].in_block_offset, 0);
            assert_eq!(entries[1].block_offset, 5_000);
            assert_eq!(entries[1].in_block_offset, 1);
            assert_eq!(entries[2].in_block_offset, 2);
            assert!(entries[3].is_filler());
            assert!(entries[4].is_filler());
            assert_eq!(entries[5].block_offset, 7_000);
            assert_eq!(entries[5].in_block_offset, 3);
        }
    }

    #[test]
    fn test_sparse_filler_segment_uses_triggering_real_block_base() {
        let sub = fresh_subdir("sparse_filler_base");
        // 208 bytes => 5 entries of stride 16.
        let mut idx = TimeIndex::new(&sub, 208, 208, true, 0).unwrap();
        idx.add_sparse_continuous_entry(None, 100, 1_000, 0)
            .unwrap();
        idx.add_sparse_continuous_entry(Some(100), 112, 9_000, 4)
            .unwrap();

        let filler_segment = match idx.index_segments.get(&110).unwrap() {
            IndexSegmentEntryState::Open(seg) => seg,
            IndexSegmentEntryState::Closed(meta) => {
                panic!("expected open filler segment, got closed {:?}", meta.path)
            }
        };
        assert_eq!(filler_segment.base_block_offset, 9_000);
        assert_eq!(filler_segment.wrote_count, 3); // fillers 110,111 + real 112
        assert!(filler_segment.find_exact(110).unwrap().is_filler());

        idx.sync_all().unwrap();
        idx.idle_close_all().unwrap();
        match idx.index_segments.get(&110).unwrap() {
            IndexSegmentEntryState::Closed(meta) => assert_eq!(meta.base_block_offset, 9_000),
            IndexSegmentEntryState::Open(_) => panic!("expected closed"),
        }

        let mut reopened = TimeIndex::load_existing(&sub, 208, 208, true, 0).unwrap();
        let entries = reopened.query(110, 112).unwrap();
        assert_eq!(entries.len(), 3);
        assert!(entries[0].is_filler());
        assert!(entries[1].is_filler());
        assert_eq!(entries[2].block_offset, 9_000);
        assert_eq!(entries[2].in_block_offset, 4);
    }

    #[test]
    fn test_historical_overwrite_out_of_range_preserves_old_entry() {
        let sub = fresh_subdir("historical_overwrite_range");
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false, 0).unwrap();
        idx.add_entry(100, 1_000, 0).unwrap();
        idx.add_entry(200, 2_000, 1).unwrap();
        let before = idx.find_entry(200).unwrap().unwrap();

        // 500 is below the segment's immutable base block offset.
        assert!(idx.update_entry(200, 500, 9).is_err());
        assert_eq!(idx.find_entry(200).unwrap().unwrap(), before);
        assert_eq!(idx.total_len(), 1);

        // Excessive delta is also rejected without touching the slot.
        assert!(idx
            .update_entry(
                200,
                1_000 + crate::index::segment::MAX_REAL_BLOCK_OFFSET_DELTA + 1,
                9
            )
            .is_err());
        assert_eq!(idx.find_entry(200).unwrap().unwrap(), before);
    }

    #[test]
    fn test_load_existing_rejects_discovered_corrupt_segment() {
        let sub = fresh_subdir("strict_load_corrupt");
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false, 3).unwrap();
        idx.add_entry(100, 1_000, 0).unwrap();
        idx.sync_all().unwrap();
        idx.idle_close_all().unwrap();

        let path = match idx.index_segments.get(&100).unwrap() {
            IndexSegmentEntryState::Closed(meta) => meta.path.clone(),
            IndexSegmentEntryState::Open(seg) => seg.path.clone(),
        };
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            // stride 24 (N=3): padding begins at 128 + 16 + 3.
            file.seek(SeekFrom::Start(128 + 16 + 3)).unwrap();
            file.write_all(&[1]).unwrap();
            file.sync_all().unwrap();
        }
        let err = TimeIndex::load_existing(&sub, 4096, 4096, false, 3)
            .err()
            .expect("corrupt discovered segment must fail load");
        assert!(format!("{err}").contains("padding"), "err: {err}");
        // Sizing metadata mismatch is also rejected.
        assert!(TimeIndex::load_existing(&sub, 4096, 4096, false, 0).is_err());
    }

    #[test]
    fn test_continuous_segment_start_for_i64_extremes() {
        let sub = fresh_subdir("continuous_i64_extremes");
        // segment_size 512 -> capacity = (512 - 128) / 32 = 12 entries.
        let mut idx = TimeIndex::new(&sub, 512, 512, true, 0).unwrap();
        idx.add_filler_entry(i64::MIN, 0).unwrap();

        // MAX - MIN exceeds i64 range: boundary math must not overflow/panic.
        let start = idx.segment_start_for(i64::MAX).unwrap();
        let cap = ((512 - INDEX_HEADER_SIZE) / INDEX_ENTRY_SIZE as u64) as i128;
        let diff = i64::MAX as i128 - i64::MIN as i128;
        assert_eq!(start as i128, i64::MIN as i128 + (diff / cap) * cap);
        let entry_index = idx.entry_index_for(i64::MAX).unwrap();
        assert!((entry_index as i128) < cap);
        assert_eq!(start as i128 + entry_index as i128, i64::MAX as i128);
    }

    #[test]
    fn test_load_existing_validates_continuous_grid_at_i64_extremes() {
        for (name, start) in [("grid_min", i64::MIN), ("grid_max", i64::MAX - 2)] {
            let sub = fresh_subdir(name);
            // 208 bytes => 5 entries of stride 16.
            let mut idx = TimeIndex::new(&sub, 208, 208, true, 0).unwrap();
            idx.add_entry(start, 1_000, 0).unwrap();
            idx.add_entry(start + 1, 1_016, 0).unwrap();
            idx.sync_all().unwrap();
            idx.idle_close_all().unwrap();

            let loaded = TimeIndex::load_existing(&sub, 208, 208, true, 0).unwrap();
            assert!(loaded.index_segments.contains_key(&start));
        }
    }

    #[test]
    fn test_load_existing_rejects_off_grid_continuous_segment_start() {
        let sub = fresh_subdir("off_grid_start");
        // 208 bytes => 5 entry capacity; valid segment starts are 0 and 5.
        let mut idx = TimeIndex::new(&sub, 208, 208, true, 0).unwrap();
        for ts in 0..10 {
            idx.add_entry(ts, 1_000 + ts as u64 * 16, 0).unwrap();
        }
        idx.sync_all().unwrap();
        idx.idle_close_all().unwrap();

        // A self-consistent segment whose start (7) is not on the 0/5 grid.
        let mut off_grid = IndexSegment::create(&sub, 7, 208, 208, 9_000, 0).unwrap();
        off_grid
            .append_entry(&IndexEntry::new(7, 9_000, 0))
            .unwrap();
        off_grid.sync().unwrap();
        drop(off_grid);

        let err = TimeIndex::load_existing(&sub, 208, 208, true, 0)
            .err()
            .expect("off-grid continuous segment start must fail load");
        assert!(format!("{err}").contains("grid"), "err: {err}");
    }

    #[test]
    fn test_load_existing_rejects_overlapping_noncontinuous_segments() {
        let sub = fresh_subdir("overlap_noncontinuous");
        let mut idx = TimeIndex::new(&sub, 4096, 4096, false, 0).unwrap();
        idx.add_entry(100, 1_000, 0).unwrap();
        idx.add_entry(200, 1_016, 0).unwrap();
        idx.sync_all().unwrap();
        idx.idle_close_all().unwrap();

        // Segment starting at 150 overlaps the [100, 200] range already loaded.
        let mut overlapping = IndexSegment::create(&sub, 150, 4096, 4096, 5_000, 0).unwrap();
        overlapping
            .append_entry(&IndexEntry::new(150, 5_000, 0))
            .unwrap();
        overlapping.sync().unwrap();
        drop(overlapping);

        let err = TimeIndex::load_existing(&sub, 4096, 4096, false, 0)
            .err()
            .expect("overlapping non-continuous segments must fail load");
        assert!(format!("{err}").contains("overlaps"), "err: {err}");
    }
}
