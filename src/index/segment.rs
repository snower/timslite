//! IndexSegment: single index file with memory-mapped entries and lifecycle management.
//!
//! Each index segment stores 18-byte IndexEntry records (timestamp, block_offset, in_block_offset)
//! in a sorted, append-only fashion.

use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;
use std::time::Instant;

use crate::error::{Result, TmslError};
use crate::header::{write_index_wrote_position_to_mmap, IndexFileMetadata, FIXED_PREFIX_SIZE};
use crate::util::read_i64_from_mmap;

// ─── IndexEntry ──────────────────────────────────────────────────────────────

pub const INDEX_ENTRY_SIZE: usize = 18;

/// Sentinel value for filler entry block_offset (no real data).
pub const BLOCK_OFFSET_FILLER: u64 = 0xFFFFFFFFFFFFFFFF;
/// Sentinel value for filler entry in_block_offset (no real data).
pub const IN_BLOCK_OFFSET_FILLER: u16 = 0xFFFF;

/// A single index entry: 18 bytes on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub timestamp: i64,
    pub block_offset: u64,    // logical offset relative to the data area start
    pub in_block_offset: u16, // relative to block payload start
}

impl IndexEntry {
    /// Serialize an entry to exactly 18 bytes.
    pub fn to_bytes(&self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[8..16].copy_from_slice(&self.block_offset.to_le_bytes());
        buf[16..18].copy_from_slice(&self.in_block_offset.to_le_bytes());
        buf
    }

    /// Parse an entry from exactly 18 bytes.
    pub fn from_bytes(buf: &[u8; INDEX_ENTRY_SIZE]) -> Self {
        Self {
            timestamp: i64::from_le_bytes(buf[0..8].try_into().unwrap()),
            block_offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            in_block_offset: u16::from_le_bytes(buf[16..18].try_into().unwrap()),
        }
    }

    /// Create a new entry.
    pub fn new(timestamp: i64, block_offset: u64, in_block_offset: u16) -> Self {
        Self {
            timestamp,
            block_offset,
            in_block_offset,
        }
    }

    /// Check if this entry is a filler (sentinel) entry.
    pub fn is_filler(&self) -> bool {
        self.block_offset == BLOCK_OFFSET_FILLER && self.in_block_offset == IN_BLOCK_OFFSET_FILLER
    }
}

// ─── IndexSegment ────────────────────────────────────────────────────────────

/// A single index file, memory-mapped for fast read/write.
///
/// Index entries are written sequentially and never modified after sealing.
/// Entries are sorted by timestamp within a segment.
pub struct IndexSegment {
    /// Path to this segment file.
    pub path: std::path::PathBuf,
    /// The timestamp of the first entry in this segment.
    pub start_timestamp: i64,
    /// Maximum number of entries this segment can hold.
    pub entries_capacity: usize,
    /// Number of entries written so far.
    pub wrote_count: usize,
    /// Memory-mapped data. None = closed/unmapped.
    pub mmap: Option<MmapMut>,
    /// Whether this segment is sealed.
    pub sealed: bool,
    /// Most recent access time.
    pub last_accessed_at: Instant,
    pub is_flushed: bool,
    pub queued_for_flush: bool,
    /// Current actual file size (grows with expansion).
    pub current_file_size: u64,
    /// Expansion upper limit (= segment_size, immutable).
    pub max_file_size: u64,
    /// Physical byte offset where index entries start in this file.
    pub header_size: u64,
}

impl IndexSegment {
    /// Create a new index segment file at `base_dir/{start_timestamp}`.
    pub fn create(
        base_dir: &Path,
        start_timestamp: i64,
        initial_size: u64,
        max_file_size: u64,
    ) -> Result<Self> {
        Self::create_with_compression(
            base_dir,
            start_timestamp,
            initial_size,
            max_file_size,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
        )
    }

    pub fn create_with_compression(
        base_dir: &Path,
        start_timestamp: i64,
        initial_size: u64,
        max_file_size: u64,
        compress_level: u8,
        compress_type: u8,
    ) -> Result<Self> {
        crate::compress::validate_compress_type(compress_type)?;
        let metadata = IndexFileMetadata::create_default(
            start_timestamp,
            max_file_size,
            compress_level,
            compress_type,
        );
        let header_size = metadata.header_size;
        if initial_size < header_size {
            return Err(TmslError::InvalidData(format!(
                "initial index segment size {} is smaller than header {}",
                initial_size, header_size
            )));
        }
        std::fs::create_dir_all(base_dir)?;
        let entries_capacity = ((initial_size - header_size) / INDEX_ENTRY_SIZE as u64) as usize;
        let file_size = header_size + entries_capacity as u64 * INDEX_ENTRY_SIZE as u64;

        let file_name = format!("{:020}", start_timestamp);
        let path = base_dir.join(&file_name);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(file_size)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        metadata.write_to(&mut mmap);
        metadata.sync(&mut mmap)?;

        Ok(Self {
            path,
            start_timestamp,
            entries_capacity,
            wrote_count: 0,
            mmap: Some(mmap),
            sealed: false,
            last_accessed_at: Instant::now(),
            is_flushed: true,
            queued_for_flush: false,
            current_file_size: file_size,
            max_file_size,
            header_size,
        })
    }

    /// Open an existing index segment by path.
    pub fn open(path: &Path, start_timestamp: i64, max_file_size: u64) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let actual_file_size = file.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let metadata = IndexFileMetadata::read_from(&mmap)?;
        let header_size = metadata.header_size;

        if metadata.magic != *b"TMSL" {
            return Err(TmslError::InvalidMagic);
        }

        let entries_capacity =
            ((actual_file_size.saturating_sub(header_size)) / INDEX_ENTRY_SIZE as u64) as usize;
        let wrote_count = ((metadata.wrote_position.saturating_sub(header_size))
            / INDEX_ENTRY_SIZE as u64) as usize;

        Ok(Self {
            path: path.to_path_buf(),
            start_timestamp,
            entries_capacity,
            wrote_count,
            mmap: Some(mmap),
            sealed: false,
            last_accessed_at: Instant::now(),
            is_flushed: true,
            queued_for_flush: false,
            current_file_size: actual_file_size,
            max_file_size,
            header_size,
        })
    }

    /// Append an index entry to the segment.
    pub fn append_entry(&mut self, entry: &IndexEntry) -> Result<()> {
        if self.wrote_count >= self.entries_capacity {
            self.sealed = true;
            return Err(TmslError::SegmentFull);
        }
        let mmap = self
            .mmap
            .as_mut()
            .ok_or_else(|| TmslError::MmapError("index segment closed".into()))?;

        let pos = self.header_size as usize + self.wrote_count * INDEX_ENTRY_SIZE;
        mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&entry.to_bytes());
        self.wrote_count += 1;

        let abs_pos = self.header_size + self.wrote_count as u64 * INDEX_ENTRY_SIZE as u64;
        write_index_wrote_position_to_mmap(mmap, abs_pos)?;

        self.last_accessed_at = Instant::now();
        self.mark_dirty();
        Ok(())
    }

    /// Whether the segment is full.
    pub fn is_full(&self) -> bool {
        self.wrote_count >= self.entries_capacity && self.current_file_size >= self.max_file_size
    }

    /// Expand the segment file by doubling (up to max_file_size).
    /// Unmaps → set_len → remaps → recalculates entries_capacity.
    pub fn expand(&mut self) -> Result<()> {
        let target = (self.current_file_size.saturating_mul(2)).min(self.max_file_size);
        if target == self.current_file_size {
            return Err(TmslError::InvalidData(format!(
                "index segment already at max_file_size ({})",
                self.max_file_size
            )));
        }

        // Unmap
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.mmap = None;

        // Resize
        file.set_len(target)?;

        // Remap
        let new_mmap = unsafe { MmapMut::map_mut(&file)? };
        self.mmap = Some(new_mmap);

        // Recalculate entries_capacity
        self.current_file_size = target;
        self.entries_capacity = ((target - self.header_size) / INDEX_ENTRY_SIZE as u64) as usize;
        self.mark_dirty();

        Ok(())
    }

    /// Seal the segment.
    pub fn seal(&mut self) -> Result<()> {
        // Mark as full in header (wrote_position and record_count already updated via append_entry)
        self.sealed = true;
        Ok(())
    }

    // ─── Query operations ────────────────────────────────────────────────

    /// Direct lookup: O(1) for continuous mode.
    /// Checks if target_ts is within [start_timestamp, start_timestamp + wrote_count - 1],
    /// directly calculates the entry position, reads and validates the timestamp.
    pub fn direct_lookup(&self, target_ts: i64) -> Option<IndexEntry> {
        let mmap = self.mmap.as_ref()?;
        let end_ts = self.start_timestamp + self.wrote_count as i64;
        if target_ts < self.start_timestamp || target_ts >= end_ts {
            return None;
        }
        let entry_index = (target_ts - self.start_timestamp) as usize;
        let pos = self.header_size as usize + entry_index * INDEX_ENTRY_SIZE;
        // Read timestamp (first 8 bytes) to validate
        let ts = read_i64_from_mmap(mmap, pos);
        if ts != target_ts {
            return None; // Defensive: should never happen in continuous mode
        }
        let buf: [u8; INDEX_ENTRY_SIZE] = mmap[pos..pos + INDEX_ENTRY_SIZE].try_into().unwrap();
        Some(IndexEntry::from_bytes(&buf))
    }

    /// Binary search: find the first entry with timestamp >= target_ts.
    pub fn lower_bound(&self, target_ts: i64) -> usize {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let (mut lo, mut hi) = (0usize, self.wrote_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let pos = self.header_size as usize + mid * INDEX_ENTRY_SIZE;
            let ts = read_i64_from_mmap(mmap, pos);
            if ts < target_ts {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Continuous-safe lower_bound: O(1) direct calculation in continuous mode,
    /// falls back to binary search for non-continuous mode.
    pub fn lower_bound_cs(&self, target_ts: i64, index_continuous: bool) -> usize {
        if !index_continuous {
            return self.lower_bound(target_ts);
        }
        if self.wrote_count == 0 {
            return 0;
        }
        if target_ts < self.start_timestamp {
            return 0;
        }
        let end_ts = self.start_timestamp + self.wrote_count as i64;
        if target_ts >= end_ts {
            return self.wrote_count;
        }
        (target_ts - self.start_timestamp) as usize
    }

    /// Binary search: find the first entry with timestamp > target_ts.
    pub fn upper_bound(&self, target_ts: i64) -> usize {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let (mut lo, mut hi) = (0usize, self.wrote_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let pos = self.header_size as usize + mid * INDEX_ENTRY_SIZE;
            let ts = read_i64_from_mmap(mmap, pos);
            if ts <= target_ts {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Continuous-safe upper_bound: O(1) direct calculation in continuous mode,
    /// falls back to binary search for non-continuous mode.
    pub fn upper_bound_cs(&self, target_ts: i64, index_continuous: bool) -> usize {
        if !index_continuous {
            return self.upper_bound(target_ts);
        }
        if self.wrote_count == 0 {
            return 0;
        }
        if target_ts < self.start_timestamp {
            return 0;
        }
        let end_ts = self.start_timestamp + self.wrote_count as i64;
        if target_ts >= end_ts {
            return self.wrote_count;
        }
        ((target_ts + 1 - self.start_timestamp) as usize).min(self.wrote_count)
    }

    /// Exact match: find entry with timestamp == target_ts.
    pub fn find_exact(&self, target_ts: i64) -> Option<IndexEntry> {
        let mmap = self.mmap.as_ref()?;
        if self.wrote_count == 0 {
            return None;
        }
        let (mut lo, mut hi) = (0usize, self.wrote_count - 1);
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let pos = self.header_size as usize + mid * INDEX_ENTRY_SIZE;
            let ts = read_i64_from_mmap(mmap, pos);
            match ts.cmp(&target_ts) {
                std::cmp::Ordering::Equal => {
                    let buf: [u8; 18] = mmap[pos..pos + 18].try_into().unwrap();
                    return Some(IndexEntry::from_bytes(&buf));
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    hi = mid - 1;
                }
            }
        }
        None
    }

    /// Binary search: find entry index with timestamp == target_ts.
    /// Returns Some(entry_index) if found, None otherwise.
    pub fn find_entry_index(&self, target_ts: i64) -> Option<usize> {
        let mmap = self.mmap.as_ref()?;
        if self.wrote_count == 0 {
            return None;
        }
        let (mut lo, mut hi) = (0usize, self.wrote_count - 1);
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let pos = self.header_size as usize + mid * INDEX_ENTRY_SIZE;
            let ts = read_i64_from_mmap(mmap, pos);
            match ts.cmp(&target_ts) {
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    hi = mid - 1;
                }
            }
        }
        None
    }

    /// Continuous-safe exact match: O(1) direct_lookup in continuous mode,
    /// falls back to binary search for non-continuous mode.
    pub fn find_exact_cs(&self, target_ts: i64, index_continuous: bool) -> Option<IndexEntry> {
        if index_continuous {
            self.direct_lookup(target_ts)
        } else {
            self.find_exact(target_ts)
        }
    }

    /// Continuous-safe find entry index: O(1) direct calculation in continuous mode,
    /// falls back to binary search for non-continuous mode.
    pub fn find_entry_index_cs(
        &self,
        target_ts: i64,
        index_continuous: bool,
        wrote_count: Option<usize>,
    ) -> Option<usize> {
        if index_continuous {
            let wc = wrote_count.unwrap_or(self.wrote_count);
            if wc == 0 {
                return None;
            }
            let end_ts = self.start_timestamp + wc as i64;
            if target_ts >= self.start_timestamp && target_ts < end_ts {
                let entry_index = (target_ts - self.start_timestamp) as usize;
                // Validate that the entry exists (in case mmap has different data)
                if let Some(mmap) = self.mmap.as_ref() {
                    let pos = self.header_size as usize + entry_index * INDEX_ENTRY_SIZE;
                    let ts = read_i64_from_mmap(mmap, pos);
                    if ts == target_ts {
                        return Some(entry_index);
                    }
                }
            }
            None
        } else {
            self.find_entry_index(target_ts)
        }
    }

    /// Overwrite an entry at the given index. Only valid for open segments.
    /// Used in continuous mode when back-filling filler entries with real data.
    pub fn overwrite_entry(&mut self, entry_index: usize, new_entry: &IndexEntry) -> Result<()> {
        if entry_index >= self.wrote_count {
            return Err(TmslError::InvalidData(format!(
                "entry index {} out of range [0, {})",
                entry_index, self.wrote_count
            )));
        }
        let mmap = self
            .mmap
            .as_mut()
            .ok_or_else(|| TmslError::MmapError("index segment closed".into()))?;
        let pos = self.header_size as usize + entry_index * INDEX_ENTRY_SIZE;
        mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&new_entry.to_bytes());
        // No header update needed — record_count stays the same
        self.last_accessed_at = Instant::now();
        self.mark_dirty();
        Ok(())
    }

    /// Range query indices: returns (start_idx, end_idx) for [start_ts, end_ts].
    /// Returns None if the range is empty.
    pub fn query_range_indices(
        &self,
        start_ts: i64,
        end_ts: i64,
        index_continuous: bool,
    ) -> Option<(usize, usize)> {
        if self.wrote_count == 0 {
            return None;
        }
        let start_idx = self.lower_bound_cs(start_ts, index_continuous);
        let end_idx = self.upper_bound_cs(end_ts, index_continuous);
        if start_idx >= end_idx {
            None
        } else {
            Some((start_idx, end_idx.min(self.wrote_count)))
        }
    }

    /// Read one entry by zero-based entry index.
    pub fn read_entry_at_index(&mut self, entry_index: usize) -> Result<IndexEntry> {
        self.ensure_open()?;
        if entry_index >= self.wrote_count {
            return Err(TmslError::InvalidData(format!(
                "entry index {} out of range [0, {})",
                entry_index, self.wrote_count
            )));
        }
        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| TmslError::MmapError("index segment closed".into()))?;
        let pos = self.header_size as usize + entry_index * INDEX_ENTRY_SIZE;
        let buf: [u8; INDEX_ENTRY_SIZE] = mmap[pos..pos + INDEX_ENTRY_SIZE].try_into().unwrap();
        self.last_accessed_at = Instant::now();
        Ok(IndexEntry::from_bytes(&buf))
    }

    pub(crate) fn last_timestamp(&mut self) -> Option<i64> {
        if self.wrote_count == 0 {
            return None;
        }
        self.read_entry_at_index(self.wrote_count - 1)
            .ok()
            .map(|entry| entry.timestamp)
    }

    pub(crate) fn last_timestamp_cached(&self) -> Option<i64> {
        if self.wrote_count == 0 {
            return None;
        }
        let mmap = self.mmap.as_ref()?;
        let pos = self.header_size as usize + (self.wrote_count - 1) * INDEX_ENTRY_SIZE;
        if pos + 8 > mmap.len() {
            return None;
        }
        Some(i64::from_le_bytes(mmap[pos..pos + 8].try_into().ok()?))
    }

    /// Range query: all entries with timestamp in [start_ts, end_ts].
    pub fn query_range(&self, start_ts: i64, end_ts: i64) -> Vec<IndexEntry> {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let mut results = Vec::new();
        let start_idx = self.lower_bound(start_ts);
        for i in start_idx..self.wrote_count {
            let pos = self.header_size as usize + i * INDEX_ENTRY_SIZE;
            let ts = read_i64_from_mmap(mmap, pos);
            if ts > end_ts {
                break;
            }
            let buf: [u8; 18] = mmap[pos..pos + 18].try_into().unwrap();
            results.push(IndexEntry::from_bytes(&buf));
        }
        results
    }

    /// Continuous-safe range query: O(1) starting index in continuous mode.
    pub fn query_range_cs(
        &self,
        start_ts: i64,
        end_ts: i64,
        index_continuous: bool,
    ) -> Vec<IndexEntry> {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let mut results = Vec::new();
        let start_idx = self.lower_bound_cs(start_ts, index_continuous);
        for i in start_idx..self.wrote_count {
            let pos = self.header_size as usize + i * INDEX_ENTRY_SIZE;
            let ts = read_i64_from_mmap(mmap, pos);
            if ts > end_ts {
                break;
            }
            let buf: [u8; 18] = mmap[pos..pos + 18].try_into().unwrap();
            results.push(IndexEntry::from_bytes(&buf));
        }
        results
    }

    // ─── Lifecycle ───────────────────────────────────────────────────────

    pub fn ensure_open(&mut self) -> Result<()> {
        if self.mmap.is_some() {
            return Ok(());
        }
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        self.mmap = Some(mmap);
        self.last_accessed_at = Instant::now();
        self.is_flushed = true;
        self.queued_for_flush = false;
        Ok(())
    }

    pub fn idle_close(&mut self) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush()?;
        }
        self.mmap = None;
        self.last_accessed_at = Instant::now();
        self.is_flushed = true;
        self.queued_for_flush = false;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush()?;
        }
        self.last_accessed_at = Instant::now();
        self.is_flushed = true;
        self.queued_for_flush = false;
        Ok(())
    }

    pub(crate) fn take_flush_enqueue_marker(&mut self) -> bool {
        if !self.is_flushed && !self.queued_for_flush {
            self.queued_for_flush = true;
            true
        } else {
            false
        }
    }

    fn mark_dirty(&mut self) {
        self.is_flushed = false;
    }
}

impl Drop for IndexSegment {
    fn drop(&mut self) {
        if let Some(ref mut m) = self.mmap {
            if let Err(e) = m.flush() {
                log::error!("[IndexSegment drop] mmap flush failed: {}", e);
            }
        }
    }
}

pub(crate) struct IndexSegmentMeta {
    pub path: std::path::PathBuf,
    pub start_timestamp: i64,
    pub entries_capacity: usize,
    pub wrote_count: usize, // record_count from header, enables O(1) range check without opening file
    pub header_size: u64,
    pub last_timestamp: Option<i64>,
}

impl IndexSegmentMeta {
    pub fn new(
        path: std::path::PathBuf,
        start_timestamp: i64,
        entries_capacity: usize,
        wrote_count: usize,
        header_size: u64,
    ) -> Self {
        Self {
            path,
            start_timestamp,
            entries_capacity,
            wrote_count,
            header_size,
            last_timestamp: None,
        }
    }

    pub fn new_with_last_timestamp(
        path: std::path::PathBuf,
        start_timestamp: i64,
        entries_capacity: usize,
        wrote_count: usize,
        header_size: u64,
        last_timestamp: Option<i64>,
    ) -> Self {
        Self {
            path,
            start_timestamp,
            entries_capacity,
            wrote_count,
            header_size,
            last_timestamp,
        }
    }
}

/// Read the last entry's timestamp from an index segment file, if non-empty.
///
/// Opens the file in read-only mmap mode (safe on Windows), reads the last 18-byte
/// entry's timestamp, and immediately drops the mmap+file handle.
///
/// Returns `Ok(Some(ts))` if the segment has at least one entry, or `Ok(None)` if
/// the segment is empty. Caller should fall back to `meta.start_timestamp` for
/// deciding whether to reclaim empty segments.
pub fn last_entry_timestamp(path: &Path) -> Result<Option<i64>> {
    let file = OpenOptions::new().read(true).open(path)?;
    let file_len = file.metadata()?.len();
    if file_len < FIXED_PREFIX_SIZE as u64 {
        return Ok(None);
    }
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
    let metadata = IndexFileMetadata::read_from(&mmap)?;
    let header_size = metadata.header_size;
    let wrote_pos = metadata.wrote_position;

    let wrote_count = if wrote_pos > header_size {
        ((wrote_pos - header_size) / INDEX_ENTRY_SIZE as u64) as usize
    } else {
        0
    };

    if wrote_count == 0 {
        return Ok(None);
    }

    let last_offset = header_size as usize + (wrote_count - 1) * INDEX_ENTRY_SIZE;
    if last_offset + 8 > mmap.len() {
        return Err(TmslError::InvalidData("truncated last index entry".into()));
    }
    let last_ts = i64::from_le_bytes(mmap[last_offset..last_offset + 8].try_into().unwrap());
    Ok(Some(last_ts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::INDEX_HEADER_SIZE;

    fn temp_dir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join("timslite_test_segment");
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn rewrite_segment_with_extended_header(seg: &mut IndexSegment) {
        let extra_meta = [0xEF, 3, 0, 7, 8, 9];
        let used = seg.wrote_count * INDEX_ENTRY_SIZE;
        let mmap = seg.mmap.as_mut().unwrap();

        let entries = mmap[INDEX_HEADER_SIZE as usize..INDEX_HEADER_SIZE as usize + used].to_vec();
        let base_meta_len = 41u16;
        let old_state_start = 9 + base_meta_len as usize + 2;
        let old_state = mmap[old_state_start..old_state_start + 8].to_vec();

        let meta_length = base_meta_len + extra_meta.len() as u16;
        mmap[7..9].copy_from_slice(&meta_length.to_le_bytes());
        let extra_start = 9 + base_meta_len as usize;
        mmap[extra_start..extra_start + extra_meta.len()].copy_from_slice(&extra_meta);
        let state_length_offset = 9 + meta_length as usize;
        let state_start = state_length_offset + 2;
        mmap[state_length_offset..state_length_offset + 2].copy_from_slice(&8u16.to_le_bytes());
        mmap[state_start..state_start + old_state.len()].copy_from_slice(&old_state);
        mmap[state_start..state_start + 8]
            .copy_from_slice(&(INDEX_HEADER_SIZE + used as u64).to_le_bytes());
        mmap[INDEX_HEADER_SIZE as usize..INDEX_HEADER_SIZE as usize + used]
            .copy_from_slice(&entries);
        mmap.flush().unwrap();

        seg.header_size = INDEX_HEADER_SIZE;
    }

    #[test]
    fn test_index_segment_find_entry_index() {
        let dir = temp_dir();
        let sub = dir.join("find_entry_index");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096).unwrap();
        for i in 0..20 {
            seg.append_entry(&IndexEntry::new(i * 10, i as u64 * 100, (i * 3) as u16))
                .unwrap();
        }
        // Find exact matches
        assert_eq!(seg.find_entry_index(50), Some(5));
        assert_eq!(seg.find_entry_index(0), Some(0));
        assert_eq!(seg.find_entry_index(190), Some(19));
        // Not found
        assert_eq!(seg.find_entry_index(55), None);
        assert_eq!(seg.find_entry_index(-1), None);
        assert_eq!(seg.find_entry_index(200), None);
    }

    #[test]
    fn test_open_reads_entries_after_extended_header() {
        let dir = temp_dir();
        let sub = dir.join("extended_header");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096).unwrap();
        seg.append_entry(&IndexEntry::new(10, 100, 1)).unwrap();
        seg.append_entry(&IndexEntry::new(20, 200, 2)).unwrap();
        let path = seg.path.clone();
        rewrite_segment_with_extended_header(&mut seg);
        drop(seg);

        let reopened = IndexSegment::open(&path, 0, 4096).unwrap();
        assert_eq!(reopened.header_size, INDEX_HEADER_SIZE);
        assert_eq!(reopened.wrote_count, 2);
        assert_eq!(reopened.find_exact(10).unwrap().block_offset, 100);
        assert_eq!(reopened.find_exact(20).unwrap().in_block_offset, 2);
    }

    #[test]
    fn test_index_segment_overwrite_entry() {
        let dir = temp_dir();
        let sub = dir.join("overwrite_entry");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096).unwrap();
        // Add a filler-like entry
        seg.append_entry(&IndexEntry::new(100, 0xFFFFFFFFFFFFFFFF, 0xFFFF))
            .unwrap();
        seg.append_entry(&IndexEntry::new(200, 0xFFFFFFFFFFFFFFFF, 0xFFFF))
            .unwrap();

        // Overwrite index 1 with real data
        let new_entry = IndexEntry::new(200, 12345, 42);
        seg.overwrite_entry(1, &new_entry).unwrap();

        // Verify via find_exact
        let found = seg.find_exact(200).unwrap();
        assert_eq!(found.block_offset, 12345);
        assert_eq!(found.in_block_offset, 42);

        // Verify out of range error
        let result = seg.overwrite_entry(5, &IndexEntry::new(999, 0, 0));
        assert!(result.is_err());
    }

    proptest::proptest! {
        #[test]
        fn proptest_index_entry_roundtrip(
            timestamp in proptest::num::i64::ANY,
            block_offset in proptest::num::u64::ANY,
            in_block_offset in proptest::num::u16::ANY,
        ) {
            let entry = IndexEntry::new(timestamp, block_offset, in_block_offset);
            let bytes = entry.to_bytes();
            let parsed = IndexEntry::from_bytes(&bytes);
            assert_eq!(parsed.timestamp, timestamp);
            assert_eq!(parsed.block_offset, block_offset);
            assert_eq!(parsed.in_block_offset, in_block_offset);
        }
    }
}
