//! TimeIndex: manages index segments with lazy lifecycle and time-range queries.
//!
//! Index entries are buffered in-memory and flushed to disk segments when the
//! buffer reaches the threshold. Segments are filled sequentially (not by hash).

pub mod segment;

use std::path::Path;

pub use self::segment::INDEX_ENTRY_SIZE;
use self::segment::{IndexEntry, IndexSegment, IndexSegmentMeta};
use crate::error::Result;
use crate::header::HEADER_SIZE;
use crate::util::read_u64_from_mmap;
use memmap2::MmapMut;

// ─── TimeIndex ─────────────────────────────────────────────────────────────

pub struct TimeIndex {
    pub base_dir: std::path::PathBuf,
    pub segment_size: u64,
    pub index_segments: Vec<IndexSegment>,
    pub closed_index_segments: Vec<IndexSegmentMeta>,
    pub in_memory_buffer: Vec<IndexEntry>,
    pub in_memory_flush_threshold: usize,
    pub index_continuous: bool, // true = continuous mode (O(1) lookup enabled)
}

impl TimeIndex {
    /// Create a new TimeIndex.
    pub fn new(base_dir: &Path, segment_size: u64, index_continuous: bool) -> Result<Self> {
        std::fs::create_dir_all(base_dir)?;
        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            segment_size,
            index_segments: Vec::new(),
            closed_index_segments: Vec::new(),
            in_memory_buffer: Vec::new(),
            in_memory_flush_threshold: 1024,
            index_continuous,
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
        self.in_memory_buffer
            .push(IndexEntry::new(timestamp, block_offset, in_block_offset));
        if self.in_memory_buffer.len() >= self.in_memory_flush_threshold {
            self.flush_to_disk()?;
        }
        Ok(())
    }

    /// Flush the in-memory buffer to disk segments.
    pub fn flush_to_disk(&mut self) -> Result<()> {
        if self.in_memory_buffer.is_empty() {
            return Ok(());
        }

        // Sort by timestamp
        self.in_memory_buffer.sort_by_key(|e| e.timestamp);

        // Take ownership of entries
        let entries: Vec<IndexEntry> = std::mem::take(&mut self.in_memory_buffer);
        let mut remaining: &[IndexEntry] = &entries;

        while !remaining.is_empty() {
            // Get the first entry's timestamp to determine which segment to use
            let start_ts = remaining[0].timestamp;

            // Get or create the current segment for this timestamp
            let seg = self.get_or_create_segment_for_ts(start_ts)?;

            // How many entries can we fit?
            let capacity = seg.entries_capacity;
            let wrote = seg.wrote_count;
            let space = capacity.saturating_sub(wrote);
            if space == 0 {
                // Current segment is full, mark it and move on
                seg.seal()?;
                continue;
            }
            let to_write = space.min(remaining.len());

            // Write entries
            let mut written = 0;
            for entry in &remaining[..to_write] {
                if let Err(e) = seg.append_entry(entry) {
                    if matches!(e, crate::error::TmslError::InvalidData(_)) {
                        seg.seal()?;
                        break;
                    }
                    return Err(e);
                }
                written += 1;
            }

            remaining = &remaining[written..];
        }

        self.in_memory_buffer.clear();

        // After all entries are flushed, check for pure-filler segments
        self.remove_pure_filler_segments();

        Ok(())
    }

    /// Remove segments that contain only filler entries (no real data).
    /// Used in continuous mode to avoid creating segments filled entirely
    /// with filler entries that span no real data.
    fn remove_pure_filler_segments(&mut self) {
        use crate::index::segment::BLOCK_OFFSET_FILLER;

        // Check closed_index_segments
        self.closed_index_segments.retain(|meta| {
            if let Ok(seg) = IndexSegment::open(&meta.path, meta.start_timestamp) {
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
            let seg = IndexSegment::open(&meta.path, meta.start_timestamp)?;
            self.index_segments.push(seg);
        } else {
            let seg = IndexSegment::create(&self.base_dir, start_ts, self.segment_size)?;
            self.index_segments.push(seg);
        }

        let idx: usize = self.index_segments.len() - 1;
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
            let mut seg = IndexSegment::open(&meta.path, meta.start_timestamp)?;
            let range_entries = seg.query_range_cs(start_ts, end_ts, ic);
            results.extend(range_entries);
            // Don't keep open - query only
        }

        // Deduplicate and sort
        results.sort_by_key(|e| e.timestamp);
        results.dedup_by_key(|e| e.timestamp);
        Ok(results)
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
            ));
            seg.idle_close()?;
        }
        self.closed_index_segments.extend(closed);
        Ok(())
    }

    /// Load existing index segments from disk.
    /// Index files are in the `index/` subdirectory.
    pub fn load_existing(
        base_dir: &Path,
        segment_size: u64,
        index_continuous: bool,
    ) -> Result<Self> {
        let mut metas: Vec<IndexSegmentMeta> = Vec::new();
        if base_dir.exists() {
            for entry in std::fs::read_dir(base_dir)? {
                let p = entry?.path();
                if !p.is_file() {
                    continue;
                }
                if let Some(stem) = p.file_stem().and_then(|n| n.to_str()) {
                    if let Ok(start_ts) = i64::from_str_radix(stem, 10) {
                        let file_size = std::fs::metadata(&p)?.len();
                        let entries_capacity =
                            ((file_size - HEADER_SIZE as u64) / INDEX_ENTRY_SIZE as u64) as usize;
                        // Read record_count from header (offset 44 + 8 = 52)
                        let wrote_count = Self::read_record_count_from_file(&p);
                        metas.push(IndexSegmentMeta::new(
                            p,
                            start_ts,
                            entries_capacity,
                            wrote_count,
                        ));
                    }
                }
            }
        }
        metas.sort_by_key(|m| m.start_timestamp);

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            segment_size,
            index_segments: Vec::new(),
            closed_index_segments: metas,
            in_memory_buffer: Vec::new(),
            in_memory_flush_threshold: 1024,
            index_continuous,
        })
    }

    /// Read record_count from the file header without fully opening the segment.
    fn read_record_count_from_file(path: &Path) -> usize {
        if let Ok(file) = std::fs::OpenOptions::new().read(true).open(path) {
            if let Ok(mmap) = unsafe { MmapMut::map_mut(&file) } {
                // record_count is at state offset 8 from state start (offset 44)
                return read_u64_from_mmap(&mmap, 52) as usize;
            }
        }
        0
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
    fn test_index_segment_append_and_query() {
        let dir = temp_dir();
        let sub = dir.join("append_query");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 1000, 4096).unwrap();

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

        let mut seg = IndexSegment::create(&sub, 0, 4096).unwrap();
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
        let mut seg = IndexSegment::create(&sub, 100, 4096).unwrap();
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

        let mut seg = IndexSegment::create(&sub, 10, 4096).unwrap();
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

        let mut seg = IndexSegment::create(&sub, 10, 4096).unwrap();
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

        let mut seg = IndexSegment::create(&sub, 10, 4096).unwrap();
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

        let mut seg = IndexSegment::create(&sub, 10, 4096).unwrap();
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

        let mut idx = TimeIndex::new(&dir, 4096, false).unwrap();
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

        let mut idx = TimeIndex::new(&dir, 4096, false).unwrap();
        idx.in_memory_flush_threshold = 5;

        for i in 0..100 {
            idx.add_entry(2000 + i, i as u64 * 300, (i * 7) as u16)
                .unwrap();
        }
        idx.flush_to_disk().unwrap();

        // Load fresh
        let mut idx2 = TimeIndex::load_existing(&dir, 4096, false).unwrap();
        // Query all
        let entries = idx2.query(2000, 2099).unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_time_index_pure_filler_segments_removed() {
        // This tests that when we add ONLY filler entries (no real data),
        // the segments are removed during flush.
        let dir = temp_dir();
        let sub = dir.join("pure_filler");
        let _ = fs::remove_dir_all(&sub);
        fs::create_dir_all(&sub).unwrap();

        let mut idx = TimeIndex::new(&sub, 4096, true).unwrap();
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
