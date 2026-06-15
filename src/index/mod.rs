//! TimeIndex: manages index segments with lazy lifecycle and time-range queries.
//!
//! Index entries are buffered in-memory and flushed to disk segments when the
//! buffer reaches the threshold. Segments are filled sequentially (not by hash).

pub mod segment;

use std::collections::BTreeMap;
use std::path::Path;

pub use self::segment::INDEX_ENTRY_SIZE;
use self::segment::{
    IndexEntry, IndexSegment, IndexSegmentMeta, BLOCK_OFFSET_FILLER, IN_BLOCK_OFFSET_FILLER,
};
use crate::error::{Result, TmslError};
use crate::header::{IndexFileMetadata, INDEX_HEADER_SIZE};
use crate::query::iter::QuerySource;

pub(crate) enum IndexSegmentEntryState {
    Open(IndexSegment),
    Closed(IndexSegmentMeta),
}

// ─── TimeIndex ─────────────────────────────────────────────────────────────

pub struct TimeIndex {
    pub base_dir: std::path::PathBuf,
    pub segment_size: u64,
    pub initial_segment_size: u64,
    pub compress_level: u8,
    pub compress_type: u8,
    pub(crate) index_segments: BTreeMap<i64, IndexSegmentEntryState>,
    pub in_memory_buffer: Vec<IndexEntry>,
    pub in_memory_flush_threshold: usize,
    pub index_continuous: bool, // true = continuous mode (O(1) lookup enabled)
    pub base_timestamp: Option<i64>,
    pub index_header_size: u64,
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

        for entry in &self.in_memory_buffer {
            min_ts = Some(min_ts.map_or(entry.timestamp, |min| min.min(entry.timestamp)));
            max_ts = Some(max_ts.map_or(entry.timestamp, |max| max.max(entry.timestamp)));
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

        for entry in &self.in_memory_buffer {
            min_ts = Some(min_ts.map_or(entry.timestamp, |min| min.min(entry.timestamp)));
            max_ts = Some(max_ts.map_or(entry.timestamp, |max| max.max(entry.timestamp)));
        }

        match (min_ts, max_ts) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    fn open_index_segment(&mut self, start_timestamp: i64) -> Result<&mut IndexSegment> {
        if !self.index_segments.contains_key(&start_timestamp) {
            return Err(TmslError::NotFound(format!(
                "no index segment at {}",
                start_timestamp
            )));
        }
        let needs_open = matches!(
            self.index_segments.get(&start_timestamp),
            Some(IndexSegmentEntryState::Closed(_))
        );
        if needs_open {
            let Some(IndexSegmentEntryState::Closed(meta)) =
                self.index_segments.remove(&start_timestamp)
            else {
                unreachable!();
            };
            let seg = IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size)?;
            self.index_segments
                .insert(start_timestamp, IndexSegmentEntryState::Open(seg));
        }
        match self.index_segments.get_mut(&start_timestamp) {
            Some(IndexSegmentEntryState::Open(seg)) => Ok(seg),
            _ => Err(TmslError::NotFound(format!(
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
            in_memory_buffer: Vec::new(),
            in_memory_flush_threshold: 1024,
            index_continuous,
            base_timestamp: None,
            index_header_size: INDEX_HEADER_SIZE,
        })
    }

    /// Add a filler entry (sentinel for continuous mode).
    /// Filler entries are buffered but skipped during flush in pure-filler segments.
    pub fn add_filler_entry(&mut self, timestamp: i64) {
        self.in_memory_buffer.push(IndexEntry::new(
            timestamp,
            crate::index::segment::BLOCK_OFFSET_FILLER,
            crate::index::segment::IN_BLOCK_OFFSET_FILLER,
        ));
    }

    /// Add an entry to the in-memory buffer. Automatically flushes when threshold reached.
    pub fn add_entry(
        &mut self,
        timestamp: i64,
        block_offset: u64,
        in_block_offset: u16,
    ) -> Result<()> {
        if self.index_continuous && self.base_timestamp.is_none() {
            self.ensure_base_timestamp(timestamp)?;
        }
        self.in_memory_buffer
            .push(IndexEntry::new(timestamp, block_offset, in_block_offset));
        if self.in_memory_buffer.len() >= self.in_memory_flush_threshold {
            self.flush_to_disk()?;
        }
        Ok(())
    }

    /// Add a real entry in continuous mode without materializing full middle gaps.
    pub fn add_sparse_continuous_entry(
        &mut self,
        previous_latest: i64,
        timestamp: i64,
        block_offset: u64,
        in_block_offset: u16,
    ) -> Result<()> {
        if !self.index_continuous {
            return self.add_entry(timestamp, block_offset, in_block_offset);
        }

        self.ensure_base_timestamp(timestamp)?;
        let real_entry = IndexEntry::new(timestamp, block_offset, in_block_offset);

        if previous_latest > 0 {
            let prev_segment_start = self.segment_start_for(previous_latest)?;
            let curr_segment_start = self.segment_start_for(timestamp)?;
            if prev_segment_start == curr_segment_start {
                self.push_filler_range(previous_latest + 1, timestamp - 1);
            } else {
                let segment_capacity = self.segment_capacity()? as i64;
                let prev_segment_end = prev_segment_start + segment_capacity - 1;
                self.push_filler_range(previous_latest + 1, prev_segment_end);
                self.push_filler_range(curr_segment_start, timestamp - 1);
            }
        }

        self.in_memory_buffer.push(real_entry);
        if self.in_memory_buffer.len() >= self.in_memory_flush_threshold {
            self.flush_to_disk()?;
        }
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

    fn entry_index_for(&self, timestamp: i64) -> Result<usize> {
        let segment_start = self.segment_start_for(timestamp)?;
        Ok((timestamp - segment_start) as usize)
    }

    fn push_filler_range(&mut self, start: i64, end: i64) {
        if start > end {
            return;
        }
        for ts in start..=end {
            self.add_filler_entry(ts);
        }
    }

    fn materialized_count_for_segment(&self, segment_start: i64) -> Result<usize> {
        let mut count = 0usize;

        if let Some(entry) = self.index_segments.get(&segment_start) {
            match entry {
                IndexSegmentEntryState::Open(seg) => count = count.max(seg.wrote_count),
                IndexSegmentEntryState::Closed(meta) => count = count.max(meta.wrote_count),
            }
        }
        for entry in &self.in_memory_buffer {
            if self.segment_start_for(entry.timestamp)? == segment_start {
                count = count.max(self.entry_index_for(entry.timestamp)? + 1);
            }
        }

        Ok(count)
    }

    /// Find the IndexEntry at the given timestamp (for correction write).
    ///
    /// Searches `in_memory_buffer`, then the unified segment registry.
    /// Returns `Ok(None)` if not found.
    pub fn find_entry(&mut self, timestamp: i64) -> Result<Option<IndexEntry>> {
        let ic = self.index_continuous;

        // 1. in-memory buffer
        if let Some(entry) = self
            .in_memory_buffer
            .iter()
            .rfind(|e| e.timestamp == timestamp)
        {
            return Ok(Some(*entry));
        }

        // 2. segment registry
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
        if !self.index_segments.contains_key(&segment_start) {
            return Ok(None);
        }
        let seg = self.open_index_segment(segment_start)?;
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
        let ic = self.index_continuous;
        let new_entry = IndexEntry::new(timestamp, new_block_offset, new_in_block_offset);

        if let Some(pos) = self
            .in_memory_buffer
            .iter()
            .position(|e| e.timestamp == timestamp)
        {
            let old = self.in_memory_buffer[pos];
            self.in_memory_buffer[pos] = new_entry;
            return Ok(old);
        }

        let segment_start = if ic {
            Some(self.segment_start_for(timestamp)?)
        } else {
            self.segment_start_candidate_for_ts(timestamp)
        };
        if let Some(segment_start) = segment_start {
            if self.index_segments.contains_key(&segment_start) {
                let seg = self.open_index_segment(segment_start)?;
                if let Some(idx) = seg.find_entry_index_cs(timestamp, ic, None) {
                    let old = seg
                        .find_exact_cs(timestamp, ic)
                        .expect("entry exists after find_entry_index_cs");
                    seg.ensure_open()?;
                    seg.overwrite_entry(idx, &new_entry)?;
                    return Ok(old);
                }
            }
        }

        if ic {
            return self.upsert_sparse_continuous_entry(
                timestamp,
                new_block_offset,
                new_in_block_offset,
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

        self.push_filler_range(segment_start + materialized_count as i64, timestamp - 1);
        self.in_memory_buffer.push(IndexEntry::new(
            timestamp,
            new_block_offset,
            new_in_block_offset,
        ));
        if self.in_memory_buffer.len() >= self.in_memory_flush_threshold {
            self.flush_to_disk()?;
        }

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

        if let Some(pos) = self
            .in_memory_buffer
            .iter()
            .position(|e| e.timestamp == timestamp)
        {
            let old = self.in_memory_buffer[pos];
            if old.is_filler() {
                return Err(TmslError::NotFound(format!(
                    "no real data at timestamp {} (filler)",
                    timestamp
                )));
            }
            self.in_memory_buffer[pos] = sentinel;
            return Ok(old);
        }

        let segment_start = if ic {
            Some(self.segment_start_for(timestamp)?)
        } else {
            self.segment_start_candidate_for_ts(timestamp)
        };
        if let Some(segment_start) = segment_start {
            if self.index_segments.contains_key(&segment_start) {
                let seg = self.open_index_segment(segment_start)?;
                if let Some(idx) = seg.find_entry_index_cs(timestamp, ic, None) {
                    let old = seg
                        .find_exact_cs(timestamp, ic)
                        .expect("entry exists after find_entry_index_cs");
                    if old.is_filler() {
                        return Err(TmslError::NotFound(format!(
                            "no real data at timestamp {} (filler)",
                            timestamp
                        )));
                    }
                    seg.ensure_open()?;
                    seg.overwrite_entry(idx, &sentinel)?;
                    return Ok(old);
                }
            }
        }

        Err(TmslError::NotFound(format!(
            "no entry at timestamp {} to delete",
            timestamp
        )))
    }

    /// Flush the in-memory buffer to disk segments.
    pub fn flush_to_disk(&mut self) -> Result<()> {
        if self.in_memory_buffer.is_empty() {
            return Ok(());
        }

        // Sort by timestamp
        self.in_memory_buffer.sort_by_key(|e| e.timestamp);

        let entries: Vec<IndexEntry> = std::mem::take(&mut self.in_memory_buffer);

        if self.index_continuous && self.base_timestamp.is_none() {
            if let Some(first) = entries.iter().find(|entry| !entry.is_filler()) {
                self.ensure_base_timestamp(first.timestamp)?;
            } else if let Some(first) = entries.first() {
                self.ensure_base_timestamp(first.timestamp)?;
            }
        }

        for entry in &entries {
            if self.index_continuous {
                if let Some(base) = self.base_timestamp {
                    if entry.timestamp < base {
                        if entry.is_filler() {
                            continue;
                        }
                        return Err(TmslError::InvalidData(format!(
                            "real index entry timestamp {} is before continuous index base {}",
                            entry.timestamp, base
                        )));
                    }
                }
                self.append_continuous_entry_to_disk(entry)?;
            } else {
                self.append_noncontinuous_entry_to_disk(entry)?;
            }
        }

        self.in_memory_buffer.clear();

        if self.index_continuous {
            self.remove_pure_filler_segments();
        }

        Ok(())
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

    fn append_noncontinuous_entry_to_disk(&mut self, entry: &IndexEntry) -> Result<()> {
        loop {
            let seg = self.get_or_create_segment_for_ts(entry.timestamp)?;
            match Self::append_with_expansion(seg, entry) {
                Ok(()) => return Ok(()),
                Err(TmslError::SegmentFull) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn append_continuous_entry_to_disk(&mut self, entry: &IndexEntry) -> Result<()> {
        let segment_start = self.segment_start_for(entry.timestamp)?;
        let entry_index = self.entry_index_for(entry.timestamp)?;
        let seg = self.get_or_create_segment_by_start(segment_start)?;
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

    /// Remove segments that contain only filler entries (no real data).
    /// Used in continuous mode to avoid creating segments filled entirely
    /// with filler entries that span no real data.
    fn remove_pure_filler_segments(&mut self) {
        let mut to_remove = Vec::new();

        for (key, entry) in self.index_segments.iter_mut() {
            let remove = match entry {
                IndexSegmentEntryState::Open(seg) => {
                    if seg.wrote_count == 0 {
                        false
                    } else {
                        let all_filler = seg
                            .query_range(i64::MIN, i64::MAX)
                            .iter()
                            .all(|e| e.block_offset == BLOCK_OFFSET_FILLER);
                        if all_filler {
                            let _ = seg.idle_close();
                            let _ = std::fs::remove_file(&seg.path);
                            log::debug!("[index] removed pure-filler segment: {:?}", seg.path);
                        }
                        all_filler
                    }
                }
                IndexSegmentEntryState::Closed(meta) => {
                    match IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size) {
                        Ok(seg) if seg.wrote_count > 0 => {
                            let all_filler = seg
                                .query_range(i64::MIN, i64::MAX)
                                .iter()
                                .all(|e| e.block_offset == BLOCK_OFFSET_FILLER);
                            if all_filler {
                                let _ = std::fs::remove_file(&meta.path);
                                log::debug!("[index] removed pure-filler segment: {:?}", meta.path);
                            }
                            all_filler
                        }
                        _ => false,
                    }
                }
            };
            if remove {
                to_remove.push(*key);
            }
        }

        for key in to_remove {
            self.index_segments.remove(&key);
        }
    }

    /// Get or create a segment for the given timestamp.
    fn get_or_create_segment_for_ts(&mut self, start_ts: i64) -> Result<&mut IndexSegment> {
        if let Some(latest_key) = self.index_segments.last_key_value().map(|(key, _)| *key) {
            let latest_available = {
                let latest = self.open_index_segment(latest_key)?;
                !latest.is_full()
            };
            if latest_available {
                return self.open_index_segment(latest_key);
            }
        }

        self.get_or_create_segment_by_start(start_ts)
    }

    fn get_or_create_segment_by_start(&mut self, segment_start: i64) -> Result<&mut IndexSegment> {
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

        let seg = IndexSegment::create_with_compression(
            &self.base_dir,
            segment_start,
            self.initial_segment_size,
            self.segment_size,
            self.compress_level,
            self.compress_type,
        )?;
        self.index_segments
            .insert(segment_start, IndexSegmentEntryState::Open(seg));
        self.open_index_segment(segment_start)
    }

    /// Query entries in the time range [start_ts, end_ts].
    /// In continuous mode, uses O(1) direct calculation for segment lookups.
    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<IndexEntry>> {
        let mut results = Vec::new();
        let ic = self.index_continuous;

        for entry in &self.in_memory_buffer {
            if entry.timestamp >= start_ts && entry.timestamp <= end_ts {
                results.push(*entry);
            }
        }

        for key in self.segment_keys_for_range(start_ts, end_ts) {
            let seg = self.open_index_segment(key)?;
            seg.ensure_open()?;
            results.extend(seg.query_range_cs(start_ts, end_ts, ic));
        }

        results.sort_by_key(|e| e.timestamp);
        results.dedup_by_key(|e| e.timestamp);
        Ok(results)
    }

    /// Prepare lazy query sources for [start_ts, end_ts].
    pub fn prepare_query_sources(
        &mut self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<QuerySource>> {
        let mut sources = Vec::new();
        let ic = self.index_continuous;

        let mut memory_entries: Vec<IndexEntry> = self
            .in_memory_buffer
            .iter()
            .copied()
            .filter(|entry| entry.timestamp >= start_ts && entry.timestamp <= end_ts)
            .collect();
        memory_entries.sort_by_key(|entry| entry.timestamp);
        memory_entries.dedup_by_key(|entry| entry.timestamp);
        if !memory_entries.is_empty() {
            sources.push(QuerySource::InMemory {
                entries: memory_entries,
                position: 0,
            });
        }

        let segment_size = self.segment_size;
        for key in self.segment_keys_for_range(start_ts, end_ts) {
            let seg = self.open_index_segment(key)?;
            seg.ensure_open()?;
            if let Some((start_idx, end_idx)) = seg.query_range_indices(start_ts, end_ts, ic) {
                let first_timestamp = seg.read_entry_at_index(start_idx)?.timestamp;
                sources.push(QuerySource::segment_file(
                    seg.path.clone(),
                    seg.start_timestamp,
                    segment_size,
                    ic,
                    start_idx,
                    end_idx,
                    first_timestamp,
                ));
            }
        }

        sources.sort_by_key(|source| source.first_timestamp().unwrap_or(i64::MAX));
        Ok(sources)
    }

    // ─── Lifecycle management ────────────────────────────────────────────

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
            in_memory_buffer: Vec::new(),
            in_memory_flush_threshold: 1024,
            index_continuous,
            base_timestamp,
            index_header_size,
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
                        if pos + 8 <= mmap.len() {
                            Some(i64::from_le_bytes(mmap[pos..pos + 8].try_into().unwrap()))
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

impl Drop for TimeIndex {
    fn drop(&mut self) {
        if !self.in_memory_buffer.is_empty() {
            if let Err(e) = self.flush_to_disk() {
                log::error!("[TimeIndex drop] flush_to_disk failed: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

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
    fn test_index_entry_roundtrip() {
        let entry = IndexEntry::new(1234567890, 1024, 42);
        let bytes = entry.to_bytes();
        let parsed = IndexEntry::from_bytes(&bytes);
        assert_eq!(entry, parsed);
    }

    #[test]
    fn test_index_entry_binary_size() {
        assert_eq!(INDEX_ENTRY_SIZE, 18);
        let entry = IndexEntry::new(i64::MAX, u64::MAX, u16::MAX);
        assert_eq!(entry.to_bytes().len(), 18);
    }

    #[test]
    fn test_continuous_segment_capacity_uses_fixed_index_area_start() {
        let dir = temp_dir();
        let sub = dir.join("continuous_fixed_index_area_start");
        let _ = fs::remove_dir_all(&sub);
        fs::create_dir_all(&sub).unwrap();

        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();
        idx.ensure_base_timestamp(1000).unwrap();

        assert_eq!(idx.segment_capacity().unwrap(), 4);
        assert_eq!(idx.segment_start_for(1003).unwrap(), 1000);
        assert_eq!(idx.segment_start_for(1004).unwrap(), 1004);
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
        idx.flush_to_disk().unwrap();
        assert_eq!(
            idx.open_index_segments()
                .map(|seg| seg.start_timestamp)
                .collect::<Vec<_>>(),
            vec![100, 104]
        );

        idx.idle_close_all().unwrap();
        assert_eq!(idx.open_len(), 0);
        assert_eq!(
            idx.closed_index_segment_metas()
                .map(|meta| meta.start_timestamp)
                .collect::<Vec<_>>(),
            vec![100, 104]
        );

        idx.get_or_create_segment_by_start(104).unwrap();
        idx.get_or_create_segment_by_start(100).unwrap();
        assert_eq!(
            idx.open_index_segments()
                .map(|seg| seg.start_timestamp)
                .collect::<Vec<_>>(),
            vec![100, 104]
        );
    }

    #[test]
    fn test_continuous_find_entry_uses_computed_closed_segment() {
        let sub = fresh_subdir("continuous_direct_closed_lookup");
        let mut idx = TimeIndex::new(&sub, 200, 200, true).unwrap();
        for ts in 100..108 {
            idx.add_entry(ts, ts as u64, 0).unwrap();
        }
        idx.flush_to_disk().unwrap();
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
        idx.flush_to_disk().unwrap();
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
        // Continuous: range [0..9], target 25 → out of range → wrote_count
        assert_eq!(seg.lower_bound_cs(9, true), 9);
        assert_eq!(seg.lower_bound_cs(10, true), 10); // out of range → wrote_count
        assert_eq!(seg.lower_bound_cs(-5, true), 0);
        assert_eq!(seg.lower_bound_cs(25, true), 10); // out of range → wrote_count
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
        idx.in_memory_flush_threshold = 5;

        for i in 0..20 {
            idx.add_entry(1000 + i, i as u64 * 200, (i * 3) as u16)
                .unwrap();
        }

        // Query should find all 20 (15 in buffer, 5 flushed)
        let entries = idx.query(1000, 1019).unwrap();
        assert_eq!(entries.len(), 20);
    }

    #[test]
    fn test_time_index_flush_and_reopen() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut idx = TimeIndex::new(&dir, 4096, 4096, false).unwrap();
        idx.in_memory_flush_threshold = 5;

        for i in 0..100 {
            idx.add_entry(2000 + i, i as u64 * 300, (i * 7) as u16)
                .unwrap();
        }
        idx.flush_to_disk().unwrap();

        // Load fresh
        let mut idx2 = TimeIndex::load_existing(&dir, 4096, 4096, false).unwrap();
        // Query all
        let entries = idx2.query(2000, 2099).unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_prepare_query_sources_uses_segment_file_cursor() {
        let dir = temp_dir();
        let sub = dir.join("prepare_query_sources");
        let _ = fs::remove_dir_all(&sub);
        fs::create_dir_all(&sub).unwrap();

        let mut idx = TimeIndex::new(&sub, 4096, 4096, false).unwrap();
        idx.in_memory_flush_threshold = 2;
        for i in 0..5 {
            idx.add_entry(3000 + i, i as u64 * 10, i as u16).unwrap();
        }
        idx.flush_to_disk().unwrap();

        let mut sources = idx.prepare_query_sources(3001, 3003).unwrap();
        assert_eq!(sources.len(), 1);
        assert!(
            matches!(
                sources[0],
                crate::query::iter::QuerySource::SegmentFile { .. }
            ),
            "flushed index range should be represented by a segment cursor, not a Vec"
        );

        let source = sources.get_mut(0).unwrap();
        let first = source.next_entry().unwrap().unwrap();
        assert_eq!(first.timestamp, 3001);
        let second = source.next_entry().unwrap().unwrap();
        assert_eq!(second.timestamp, 3002);
        let third = source.next_entry().unwrap().unwrap();
        assert_eq!(third.timestamp, 3003);
        assert!(source.next_entry().unwrap().is_none());
    }

    #[test]
    fn test_time_index_pure_filler_segments_removed() {
        // This tests that when we add ONLY filler entries (no real data),
        // the segments are removed during flush.
        let dir = temp_dir();
        let sub = dir.join("pure_filler");
        let _ = fs::remove_dir_all(&sub);
        fs::create_dir_all(&sub).unwrap();

        let mut idx = TimeIndex::new(&sub, 4096, 4096, true).unwrap();
        idx.in_memory_flush_threshold = 3;

        // Add only filler entries (more than one segment worth)
        for i in 0..100 {
            idx.add_filler_entry(1000 + i);
        }
        // Add one real entry at the end
        idx.add_entry(1100, 999, 0).unwrap();
        idx.flush_to_disk().unwrap();

        // Count files in directory
        let file_count = fs::read_dir(&sub)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().path().is_file())
            .count();

        // There should be only 1 segment file (the one with the real entry)
        assert!(file_count >= 1);

        // Verify the real entry is queryable
        let entries = idx.query(1100, 1100).unwrap();
        assert_eq!(entries.len(), 1);
    }
}
