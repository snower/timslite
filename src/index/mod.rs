//! TimeIndex: manages index segments with lazy lifecycle and time-range queries.
//!
//! Index entries are buffered in-memory and flushed to disk segments when the
//! buffer reaches the threshold. Segments are filled sequentially (not by hash).

mod segment;

use std::path::Path;

use self::segment::{IndexEntry, IndexSegment, IndexSegmentMeta, INDEX_ENTRY_SIZE};
use crate::error::Result;

// ─── TimeIndex ─────────────────────────────────────────────────────────────

pub struct TimeIndex {
    pub base_dir: std::path::PathBuf,
    pub segment_size: u64,
    pub index_segments: Vec<IndexSegment>,
    pub closed_index_segments: Vec<IndexSegmentMeta>,
    pub in_memory_buffer: Vec<IndexEntry>,
    pub in_memory_flush_threshold: usize,
}

impl TimeIndex {
    /// Create a new TimeIndex.
    pub fn new(base_dir: &Path, segment_size: u64) -> Result<Self> {
        std::fs::create_dir_all(base_dir)?;
        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            segment_size,
            index_segments: Vec::new(),
            closed_index_segments: Vec::new(),
            in_memory_buffer: Vec::new(),
            in_memory_flush_threshold: 1024,
        })
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
        Ok(())
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
    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<IndexEntry>> {
        let mut results = Vec::new();

        // In-memory buffer
        for entry in &self.in_memory_buffer {
            if entry.timestamp >= start_ts && entry.timestamp <= end_ts {
                results.push(*entry);
            }
        }

        // Open segments
        for seg in &mut self.index_segments {
            seg.ensure_open()?;
            let range_entries = seg.query_range(start_ts, end_ts);
            results.extend(range_entries);
        }

        // Closed segments (open briefly for query)
        for meta in &self.closed_index_segments {
            let seg = IndexSegment::open(&meta.path, meta.start_timestamp)?;
            results.extend(seg.query_range(start_ts, end_ts));
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
            ));
            seg.idle_close()?;
        }
        self.closed_index_segments.extend(closed);
        Ok(())
    }

    /// Load existing index segments from disk.
    pub fn load_existing(base_dir: &Path, segment_size: u64) -> Result<Self> {
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
                        let entries_capacity = ((file_size - crate::header::HEADER_SIZE as u64)
                            / INDEX_ENTRY_SIZE as u64)
                            as usize;
                        metas.push(IndexSegmentMeta::new(p, start_ts, entries_capacity));
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
        })
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

        let entries = seg.query_range(1010, 1020);
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

        assert_eq!(seg.lower_bound(25), 3);
        assert_eq!(seg.lower_bound(20), 2);
        assert_eq!(seg.lower_bound(-5), 0);
        assert_eq!(seg.lower_bound(100), 10);
    }

    #[test]
    fn test_index_segment_find_exact() {
        let dir = temp_dir();
        let sub = dir.join("find_exact");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096).unwrap();
        seg.append_entry(&IndexEntry::new(100, 500, 10)).unwrap();
        seg.append_entry(&IndexEntry::new(200, 600, 20)).unwrap();

        let found = seg.find_exact(200).unwrap();
        assert_eq!(found.timestamp, 200);
        assert_eq!(found.block_offset, 600);
        assert_eq!(found.in_block_offset, 20);

        assert!(seg.find_exact(150).is_none());
    }

    #[test]
    fn test_index_segment_lifecycle() {
        let dir = temp_dir();
        let sub = dir.join("lifecycle");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 500, 4096).unwrap();
        seg.append_entry(&IndexEntry::new(500, 100, 0)).unwrap();

        seg.idle_close().unwrap();
        assert!(seg.mmap.is_none());

        seg.ensure_open().unwrap();
        assert!(seg.mmap.is_some());

        // Entry should still be there
        let found = seg.find_exact(500).unwrap();
        assert_eq!(found.timestamp, 500);
    }

    #[test]
    fn test_time_index_add_and_query() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut idx = TimeIndex::new(&dir, 4096).unwrap();
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

        let mut idx = TimeIndex::new(&dir, 4096).unwrap();
        idx.in_memory_flush_threshold = 5;

        for i in 0..100 {
            idx.add_entry(2000 + i, i as u64 * 300, (i * 7) as u16)
                .unwrap();
        }
        idx.flush_to_disk().unwrap();

        // Load fresh
        let mut idx2 = TimeIndex::load_existing(&dir, 4096).unwrap();
        // Query all
        let entries = idx2.query(2000, 2099).unwrap();
        assert_eq!(entries.len(), 100);
    }
}
