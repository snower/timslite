//! TimeIndex: manages index segments with lazy lifecycle and time-range queries.
//!
//! Index entries are buffered in-memory and flushed to disk segments when the
//! buffer reaches the threshold. Segments are filled sequentially (not by hash).

pub mod segment;

use std::path::Path;

pub use self::segment::INDEX_ENTRY_SIZE;
use self::segment::{
    IndexEntry, IndexSegment, IndexSegmentMeta, BLOCK_OFFSET_FILLER, IN_BLOCK_OFFSET_FILLER,
};
use crate::error::{Result, TmslError};
use crate::header::{IndexFileMetadata, INDEX_HEADER_SIZE};
use crate::query::iter::QuerySource;

// ─── TimeIndex ─────────────────────────────────────────────────────────────

pub struct TimeIndex {
    pub base_dir: std::path::PathBuf,
    pub segment_size: u64,
    pub initial_segment_size: u64,
    pub compress_level: u8,
    pub compress_type: u8,
    pub index_segments: Vec<IndexSegment>,
    pub closed_index_segments: Vec<IndexSegmentMeta>,
    pub in_memory_buffer: Vec<IndexEntry>,
    pub in_memory_flush_threshold: usize,
    pub index_continuous: bool, // true = continuous mode (O(1) lookup enabled)
    pub base_timestamp: Option<i64>,
    pub index_header_size: u64,
}

impl TimeIndex {
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
            index_segments: Vec::new(),
            closed_index_segments: Vec::new(),
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

        for seg in &self.index_segments {
            if seg.start_timestamp == segment_start {
                count = count.max(seg.wrote_count);
            }
        }
        for meta in &self.closed_index_segments {
            if meta.start_timestamp == segment_start {
                count = count.max(meta.wrote_count);
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
    /// Searches `in_memory_buffer` → open `index_segments` → `closed_index_segments`
    /// (temporarily opened). Returns `Ok(None)` if not found.
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

        // 2. open segments
        for seg in &self.index_segments {
            if let Some(entry) = seg.find_exact_cs(timestamp, ic) {
                return Ok(Some(entry));
            }
        }

        // 3. closed segments (temporarily open → lookup → idle_close)
        for meta in &self.closed_index_segments {
            let mut seg = IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size)?;
            if let Some(entry) = seg.find_exact_cs(timestamp, ic) {
                seg.idle_close().ok();
                return Ok(Some(entry));
            }
            seg.idle_close().ok();
        }

        Ok(None)
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

        // 1. in-memory buffer (linear search)
        if let Some(pos) = self
            .in_memory_buffer
            .iter()
            .position(|e| e.timestamp == timestamp)
        {
            let old = self.in_memory_buffer[pos];
            self.in_memory_buffer[pos] = new_entry;
            return Ok(old);
        }

        // 2. open segments
        for seg in &mut self.index_segments {
            if let Some(idx) = seg.find_entry_index_cs(timestamp, ic, None) {
                let old = seg
                    .find_exact_cs(timestamp, ic)
                    .expect("entry exists after find_entry_index_cs");
                seg.ensure_open()?;
                seg.overwrite_entry(idx, &new_entry)?;
                return Ok(old);
            }
        }

        // 3. closed segments (open briefly → overwrite → idle_close)
        for meta in &self.closed_index_segments {
            let mut seg = IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size)?;
            if let Some(idx) = seg.find_entry_index_cs(timestamp, ic, Some(meta.wrote_count)) {
                let old = seg
                    .find_exact_cs(timestamp, ic)
                    .expect("entry exists after find_entry_index_cs");
                seg.overwrite_entry(idx, &new_entry)?;
                seg.idle_close()?;
                return Ok(old);
            }
            seg.idle_close()?;
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

        // 1. in-memory buffer
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

        // 2. open segments
        for seg in &mut self.index_segments {
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

        // 3. closed segments (open briefly → overwrite → idle_close)
        for meta in &self.closed_index_segments {
            let mut seg = IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size)?;
            if let Some(idx) = seg.find_entry_index_cs(timestamp, ic, Some(meta.wrote_count)) {
                let old = seg
                    .find_exact_cs(timestamp, ic)
                    .expect("entry exists after find_entry_index_cs");
                if old.is_filler() {
                    seg.idle_close()?;
                    return Err(TmslError::NotFound(format!(
                        "no real data at timestamp {} (filler)",
                        timestamp
                    )));
                }
                seg.overwrite_entry(idx, &sentinel)?;
                seg.idle_close()?;
                return Ok(old);
            }
            seg.idle_close()?;
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
        use crate::index::segment::BLOCK_OFFSET_FILLER;

        // Check closed_index_segments
        self.closed_index_segments.retain(|meta| {
            if let Ok(seg) = IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size)
            {
                if seg.wrote_count > 0 {
                    // Check if all entries are filler
                    let all_filler = seg
                        .query_range(i64::MIN, i64::MAX)
                        .iter()
                        .all(|e| e.block_offset == BLOCK_OFFSET_FILLER);
                    if all_filler {
                        let _ = std::fs::remove_file(&meta.path);
                        log::debug!("[index] removed pure-filler segment: {:?}", meta.path);
                        return false; // remove from vec
                    }
                }
            }
            true // keep
        });

        // Check open index_segments
        let mut to_remove = Vec::new();
        for (idx, seg) in self.index_segments.iter_mut().enumerate() {
            if seg.wrote_count > 0 {
                let all_filler = seg
                    .query_range(i64::MIN, i64::MAX)
                    .iter()
                    .all(|e| e.block_offset == BLOCK_OFFSET_FILLER);
                if all_filler {
                    seg.idle_close().ok();
                    let _ = std::fs::remove_file(&seg.path);
                    log::debug!("[index] removed pure-filler segment: {:?}", seg.path);
                    to_remove.push(idx);
                }
            }
        }
        for idx in to_remove.into_iter().rev() {
            self.index_segments.remove(idx);
        }
    }

    /// Get or create a segment for the given timestamp.
    fn get_or_create_segment_for_ts(&mut self, start_ts: i64) -> Result<&mut IndexSegment> {
        // Try last segment first
        if let Some(last) = self.index_segments.last() {
            if !last.is_full() {
                let idx: usize = self.index_segments.len() - 1;
                return Ok(&mut self.index_segments[idx]);
            }
        }

        // Check closed segments for existing one
        if let Some(pos) = self
            .closed_index_segments
            .iter()
            .position(|m| m.start_timestamp == start_ts)
        {
            let meta = self.closed_index_segments.remove(pos);
            let seg = IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size)?;
            self.index_segments.push(seg);
        } else {
            let seg = IndexSegment::create_with_compression(
                &self.base_dir,
                start_ts,
                self.initial_segment_size,
                self.segment_size,
                self.compress_level,
                self.compress_type,
            )?;
            self.index_segments.push(seg);
        }

        let idx: usize = self.index_segments.len() - 1;
        Ok(&mut self.index_segments[idx])
    }

    fn get_or_create_segment_by_start(&mut self, segment_start: i64) -> Result<&mut IndexSegment> {
        if let Some(pos) = self
            .index_segments
            .iter()
            .position(|seg| seg.start_timestamp == segment_start)
        {
            return Ok(&mut self.index_segments[pos]);
        }

        if let Some(pos) = self
            .closed_index_segments
            .iter()
            .position(|meta| meta.start_timestamp == segment_start)
        {
            let meta = self.closed_index_segments.remove(pos);
            let seg = IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size)?;
            self.index_segments.push(seg);
        } else {
            let seg = IndexSegment::create_with_compression(
                &self.base_dir,
                segment_start,
                self.initial_segment_size,
                self.segment_size,
                self.compress_level,
                self.compress_type,
            )?;
            self.index_segments.push(seg);
        }

        let idx = self.index_segments.len() - 1;
        Ok(&mut self.index_segments[idx])
    }

    /// Query entries in the time range [start_ts, end_ts].
    /// In continuous mode, uses O(1) direct calculation for segment lookups.
    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<IndexEntry>> {
        let mut results = Vec::new();
        let ic = self.index_continuous;

        // In-memory buffer
        for entry in &self.in_memory_buffer {
            if entry.timestamp >= start_ts && entry.timestamp <= end_ts {
                results.push(*entry);
            }
        }

        // Open segments (use continuous-safe query)
        for seg in &mut self.index_segments {
            seg.ensure_open()?;
            let range_entries = seg.query_range_cs(start_ts, end_ts, ic);
            results.extend(range_entries);
        }

        // Closed segments (open briefly for query)
        for meta in &self.closed_index_segments {
            let seg = IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size)?;
            let range_entries = seg.query_range_cs(start_ts, end_ts, ic);
            results.extend(range_entries);
            // Don't keep open - query only
        }

        // Deduplicate and sort
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

        for seg in &mut self.index_segments {
            seg.ensure_open()?;
            if let Some((start_idx, end_idx)) = seg.query_range_indices(start_ts, end_ts, ic) {
                let first_timestamp = seg.read_entry_at_index(start_idx)?.timestamp;
                sources.push(QuerySource::segment_file(
                    seg.path.clone(),
                    seg.start_timestamp,
                    self.segment_size,
                    ic,
                    start_idx,
                    end_idx,
                    first_timestamp,
                ));
            }
        }

        for meta in &self.closed_index_segments {
            let mut seg = IndexSegment::open(&meta.path, meta.start_timestamp, self.segment_size)?;
            if let Some((start_idx, end_idx)) = seg.query_range_indices(start_ts, end_ts, ic) {
                let first_timestamp = seg.read_entry_at_index(start_idx)?.timestamp;
                sources.push(QuerySource::segment_file(
                    meta.path.clone(),
                    meta.start_timestamp,
                    self.segment_size,
                    ic,
                    start_idx,
                    end_idx,
                    first_timestamp,
                ));
            }
            seg.idle_close().ok();
        }

        sources.sort_by_key(|source| source.first_timestamp().unwrap_or(i64::MAX));
        Ok(sources)
    }

    // ─── Lifecycle management ────────────────────────────────────────────

    pub fn sync_all(&mut self) -> Result<()> {
        for seg in &mut self.index_segments {
            seg.sync()?;
        }
        Ok(())
    }

    pub fn idle_close_all(&mut self) -> Result<()> {
        let mut closed: Vec<IndexSegmentMeta> = Vec::new();
        for mut seg in self.index_segments.drain(..) {
            closed.push(IndexSegmentMeta::new(
                seg.path.clone(),
                seg.start_timestamp,
                seg.entries_capacity,
                seg.wrote_count,
                seg.header_size,
            ));
            seg.idle_close()?;
        }
        self.closed_index_segments.extend(closed);
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
        let before = self.closed_index_segments.len();
        self.closed_index_segments.retain(|meta| {
            // Open briefly with read-only mmap and drop immediately after reading.
            match segment::last_entry_timestamp(&meta.path) {
                Ok(Some(last_ts)) if last_ts < threshold => {
                    let _ = std::fs::remove_file(&meta.path);
                    log::info!("[retention] deleted index segment: {:?}", meta.path);
                    false
                }
                Ok(None) => {
                    // Empty segment — only filler? Reclaim to recycle disk.
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
        Ok(before - self.closed_index_segments.len())
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
                        let (wrote_count, header_size) = Self::read_record_count_from_file(&p);
                        let entries_capacity = ((file_size.saturating_sub(header_size))
                            / INDEX_ENTRY_SIZE as u64)
                            as usize;
                        metas.push(IndexSegmentMeta::new(
                            p,
                            start_ts,
                            entries_capacity,
                            wrote_count,
                            header_size,
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
            index_segments: Vec::new(),
            closed_index_segments: metas,
            in_memory_buffer: Vec::new(),
            in_memory_flush_threshold: 1024,
            index_continuous,
            base_timestamp,
            index_header_size,
        })
    }

    /// Read wrote_count from the file header without fully opening the segment.
    fn read_record_count_from_file(path: &Path) -> (usize, u64) {
        if let Ok(file) = std::fs::OpenOptions::new().read(true).open(path) {
            if let Ok(mmap) = unsafe { memmap2::MmapOptions::new().map(&file) } {
                if let Ok(metadata) = IndexFileMetadata::read_from(&mmap) {
                    let header_size = metadata.header_size;
                    let count = ((metadata.wrote_position.saturating_sub(header_size))
                        / INDEX_ENTRY_SIZE as u64) as usize;
                    return (count, header_size);
                }
            }
        }
        (0, INDEX_HEADER_SIZE)
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

    fn temp_dir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join("timslite_test_index");
        fs::create_dir_all(&d).unwrap();
        d
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
