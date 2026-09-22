//! IndexSegment: single index file with memory-mapped entries and lifecycle management.
//!
//! Each index segment stores fixed 32-byte IndexEntry records
//! (timestamp: i64, block_offset: u64, in_block_offset_units: u16, 14 reserved zero
//! bytes) in a sorted, append-only fashion.

use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;
use std::time::Instant;

use crate::bg::{SegmentDirtySink, SegmentFlushTarget};
use crate::error::{Result, TmslError};
use crate::header::{write_index_wrote_position_to_mmap, IndexFileMetadata, FIXED_PREFIX_SIZE};

// ─── IndexEntry ──────────────────────────────────────────────────────────────

pub const INDEX_ENTRY_SIZE: usize = 32;

/// Number of trailing reserved (must-be-zero) bytes in an on-disk index entry.
const INDEX_ENTRY_RESERVED_BYTES: usize = 14;

/// Sentinel value for filler entry block_offset (no real data).
pub const BLOCK_OFFSET_FILLER: u64 = 0xFFFFFFFFFFFFFFFF;
/// Sentinel value for filler entry in_block_offset (no real data).
pub const IN_BLOCK_OFFSET_FILLER: u16 = 0xFFFF;

/// A single index entry in memory. On disk (32 bytes, little-endian):
/// `[timestamp: i64][block_offset: u64][in_block_offset_units: u16][14 reserved zeros]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub timestamp: i64,
    pub block_offset: u64,    // logical offset relative to the data area start
    pub in_block_offset: u16, // 4-byte units relative to block payload start
}

impl IndexEntry {
    /// Serialize an entry to exactly 32 bytes.
    pub fn to_bytes_for_segment(&self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[8..16].copy_from_slice(&self.block_offset.to_le_bytes());
        buf[16..18].copy_from_slice(&self.in_block_offset.to_le_bytes());
        // buf[18..32] stays zero: reserved bytes.
        buf
    }

    /// Parse an entry from exactly 32 bytes. Rejects non-zero reserved bytes.
    pub fn from_bytes_for_segment(buf: &[u8; INDEX_ENTRY_SIZE]) -> Result<Self> {
        if buf[INDEX_ENTRY_SIZE - INDEX_ENTRY_RESERVED_BYTES..] != [0u8; INDEX_ENTRY_RESERVED_BYTES]
        {
            return Err(TmslError::InvalidData(
                "index entry reserved bytes must be zero".into(),
            ));
        }
        Ok(Self {
            timestamp: i64::from_le_bytes(buf[0..8].try_into().unwrap()),
            block_offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            in_block_offset: u16::from_le_bytes(buf[16..18].try_into().unwrap()),
        })
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
    dirty_sink: Option<SegmentDirtySink>,
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
            dirty_sink: None,
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
        if metadata.file_offset != start_timestamp {
            return Err(TmslError::InvalidData(format!(
                "index segment header start {} does not match file name start {}",
                metadata.file_offset, start_timestamp
            )));
        }

        // Reject files whose state does not match the fixed 32-byte entry
        // grid, then structurally validate every materialized entry against
        // the new-format layout (zero reserved bytes). Under the
        // no-compatibility contract this deterministically rejects old 14-byte
        // delta-format segments instead of silently reinterpreting them, even
        // when a 14-byte entry count happens to land on the 32-byte grid.
        let entry_area_used = metadata.wrote_position.saturating_sub(header_size);
        if metadata.wrote_position < header_size
            || !entry_area_used.is_multiple_of(INDEX_ENTRY_SIZE as u64)
        {
            return Err(TmslError::InvalidData(format!(
                "index wrote_position {} is not aligned to {}-byte entries",
                metadata.wrote_position, INDEX_ENTRY_SIZE
            )));
        }

        let entries_capacity =
            ((actual_file_size.saturating_sub(header_size)) / INDEX_ENTRY_SIZE as u64) as usize;
        let wrote_count = (entry_area_used / INDEX_ENTRY_SIZE as u64) as usize;
        for i in 0..wrote_count {
            let pos = header_size as usize + i * INDEX_ENTRY_SIZE;
            if pos + INDEX_ENTRY_SIZE > mmap.len() {
                return Err(TmslError::InvalidData(format!(
                    "index entry {i} at {pos} is beyond file bounds"
                )));
            }
            let buf: [u8; INDEX_ENTRY_SIZE] = mmap[pos..pos + INDEX_ENTRY_SIZE]
                .try_into()
                .expect("entry-sized slice");
            IndexEntry::from_bytes_for_segment(&buf).map_err(|e| {
                TmslError::InvalidData(format!(
                    "index segment {} entry {i} malformed: {e}",
                    path.display()
                ))
            })?;
        }

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
            dirty_sink: None,
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

        let bytes = entry.to_bytes_for_segment();
        let pos = self.header_size as usize + self.wrote_count * INDEX_ENTRY_SIZE;
        mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&bytes);
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
        // i128: a segment materialized to i64::MAX has end_ts beyond i64.
        let end_ts = self.start_timestamp as i128 + self.wrote_count as i128;
        if (target_ts as i128) < self.start_timestamp as i128 || (target_ts as i128) >= end_ts {
            return None;
        }
        let entry_index = (target_ts - self.start_timestamp) as usize;
        let pos = self.header_size as usize + entry_index * INDEX_ENTRY_SIZE;
        let ts = Self::read_timestamp_at_pos(mmap, pos)?;
        if ts != target_ts {
            return None; // Defensive: should never happen in continuous mode
        }
        let buf: [u8; INDEX_ENTRY_SIZE] = mmap[pos..pos + INDEX_ENTRY_SIZE].try_into().unwrap();
        IndexEntry::from_bytes_for_segment(&buf).ok()
    }

    fn read_timestamp_at_pos(mmap: &[u8], pos: usize) -> Option<i64> {
        if pos + 8 > mmap.len() {
            return None;
        }
        Some(i64::from_le_bytes(mmap[pos..pos + 8].try_into().ok()?))
    }

    fn read_entry_at_pos(mmap: &[u8], pos: usize) -> Option<IndexEntry> {
        if pos + INDEX_ENTRY_SIZE > mmap.len() {
            return None;
        }
        let buf: [u8; INDEX_ENTRY_SIZE] = mmap[pos..pos + INDEX_ENTRY_SIZE].try_into().ok()?;
        IndexEntry::from_bytes_for_segment(&buf).ok()
    }

    /// Binary search: find the first entry with timestamp >= target_ts.
    pub fn lower_bound(&self, target_ts: i64) -> usize {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let (mut lo, mut hi) = (0usize, self.wrote_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let pos = self.header_size as usize + mid * INDEX_ENTRY_SIZE;
            let ts = Self::read_timestamp_at_pos(mmap, pos).unwrap_or(i64::MAX);
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
        let end_ts = self.start_timestamp as i128 + self.wrote_count as i128;
        if (target_ts as i128) >= end_ts {
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
            let ts = Self::read_timestamp_at_pos(mmap, pos).unwrap_or(i64::MAX);
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
        let end_ts = self.start_timestamp as i128 + self.wrote_count as i128;
        if (target_ts as i128) >= end_ts {
            return self.wrote_count;
        }
        // i128: target_ts may be i64::MAX.
        let next = (target_ts as i128 + 1 - self.start_timestamp as i128) as usize;
        next.min(self.wrote_count)
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
            let ts = Self::read_timestamp_at_pos(mmap, pos)?;
            match ts.cmp(&target_ts) {
                std::cmp::Ordering::Equal => {
                    let buf: [u8; INDEX_ENTRY_SIZE] =
                        mmap[pos..pos + INDEX_ENTRY_SIZE].try_into().unwrap();
                    return IndexEntry::from_bytes_for_segment(&buf).ok();
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
            let ts = Self::read_timestamp_at_pos(mmap, pos)?;
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
            let end_ts = self.start_timestamp as i128 + wc as i128;
            if (target_ts as i128) >= self.start_timestamp as i128 && (target_ts as i128) < end_ts {
                let entry_index = (target_ts - self.start_timestamp) as usize;
                // Validate that the entry exists (in case mmap has different data)
                if let Some(mmap) = self.mmap.as_ref() {
                    let pos = self.header_size as usize + entry_index * INDEX_ENTRY_SIZE;
                    let ts = Self::read_timestamp_at_pos(mmap, pos)?;
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

    pub(crate) fn find_entry_index_and_entry_cs(
        &self,
        target_ts: i64,
        index_continuous: bool,
        wrote_count: Option<usize>,
    ) -> Option<(usize, IndexEntry)> {
        let mmap = self.mmap.as_ref()?;
        if index_continuous {
            let wc = wrote_count.unwrap_or(self.wrote_count);
            if wc == 0 {
                return None;
            }
            let end_ts = self.start_timestamp as i128 + wc as i128;
            if (target_ts as i128) < self.start_timestamp as i128 || (target_ts as i128) >= end_ts {
                return None;
            }
            let entry_index = (target_ts - self.start_timestamp) as usize;
            let pos = self.header_size as usize + entry_index * INDEX_ENTRY_SIZE;
            let ts = Self::read_timestamp_at_pos(mmap, pos)?;
            if ts != target_ts {
                return None;
            }
            return Self::read_entry_at_pos(mmap, pos).map(|entry| (entry_index, entry));
        }

        if self.wrote_count == 0 {
            return None;
        }
        let (mut lo, mut hi) = (0usize, self.wrote_count - 1);
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let pos = self.header_size as usize + mid * INDEX_ENTRY_SIZE;
            let ts = Self::read_timestamp_at_pos(mmap, pos)?;
            match ts.cmp(&target_ts) {
                std::cmp::Ordering::Equal => {
                    return Self::read_entry_at_pos(mmap, pos).map(|entry| (mid, entry));
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
        let bytes = new_entry.to_bytes_for_segment();
        let pos = self.header_size as usize + entry_index * INDEX_ENTRY_SIZE;
        mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&bytes);
        // No header update needed — record_count stays the same
        self.last_accessed_at = Instant::now();
        self.mark_dirty();
        Ok(())
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
        IndexEntry::from_bytes_for_segment(&buf)
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
        Self::read_timestamp_at_pos(mmap, pos)
    }

    /// Range query: all entries with timestamp in [start_ts, end_ts].
    pub fn query_range(&self, start_ts: i64, end_ts: i64) -> Vec<IndexEntry> {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let mut results = Vec::new();
        let start_idx = self.lower_bound(start_ts);
        for i in start_idx..self.wrote_count {
            let pos = self.header_size as usize + i * INDEX_ENTRY_SIZE;
            let Some(ts) = Self::read_timestamp_at_pos(mmap, pos) else {
                break;
            };
            if ts > end_ts {
                break;
            }
            let buf: [u8; INDEX_ENTRY_SIZE] = mmap[pos..pos + INDEX_ENTRY_SIZE].try_into().unwrap();
            if let Ok(entry) = IndexEntry::from_bytes_for_segment(&buf) {
                results.push(entry);
            }
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
            let Some(ts) = Self::read_timestamp_at_pos(mmap, pos) else {
                break;
            };
            if ts > end_ts {
                break;
            }
            let buf: [u8; INDEX_ENTRY_SIZE] = mmap[pos..pos + INDEX_ENTRY_SIZE].try_into().unwrap();
            if let Ok(entry) = IndexEntry::from_bytes_for_segment(&buf) {
                results.push(entry);
            }
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

    pub(crate) fn set_dirty_sink(&mut self, dirty_sink: Option<SegmentDirtySink>) {
        self.dirty_sink = dirty_sink;
        self.enqueue_dirty_if_needed();
    }

    fn mark_dirty(&mut self) {
        self.is_flushed = false;
        self.enqueue_dirty_if_needed();
    }

    fn enqueue_dirty_if_needed(&mut self) {
        if self.is_flushed || self.queued_for_flush {
            return;
        }
        let Some(sink) = self.dirty_sink.as_ref() else {
            return;
        };
        sink.enqueue(SegmentFlushTarget::Index {
            start_timestamp: self.start_timestamp,
        });
        self.queued_for_flush = true;
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
/// Opens the file in read-only mmap mode (safe on Windows), reads the last 14-byte
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
    fn test_open_rejects_old_14byte_entry_segment() {
        let dir = temp_dir();
        let sub = dir.join("old14_open_reject");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let path = sub.join(format!("{:020}", 1_000i64));

        // Old 14-byte entry layout: [delta:u32][block_offset:u64][in_block:u16].
        // 16 entries make the area 224 bytes, a multiple of the new 32-byte
        // grid, so alignment alone cannot distinguish it from a valid file.
        const OLD_ENTRIES: usize = 16;
        let file_len = INDEX_HEADER_SIZE as usize + OLD_ENTRIES * 14;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len(file_len as u64).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let mut meta =
            IndexFileMetadata::create_default(1_000, 4096, 6, crate::compress::COMPRESS_TYPE_ZSTD);
        meta.wrote_position = INDEX_HEADER_SIZE + (OLD_ENTRIES * 14) as u64;
        meta.write_to(&mut mmap);
        for i in 0..OLD_ENTRIES {
            let base = INDEX_HEADER_SIZE as usize + i * 14;
            mmap[base..base + 4].copy_from_slice(&(i as u32).to_le_bytes());
            mmap[base + 4..base + 12].copy_from_slice(&((i as u64) * 32).to_le_bytes());
            mmap[base + 12..base + 14].copy_from_slice(&0u16.to_le_bytes());
        }
        mmap.flush().unwrap();
        drop(mmap);
        drop(file);

        let err = IndexSegment::open(&path, 1_000, 4096)
            .err()
            .expect("old 14-byte entry segments must fail to open, not be reinterpreted");
        assert!(
            matches!(err, TmslError::InvalidData(_)),
            "expected structural rejection, got {err:?}"
        );
    }

    // Spec-encodes the not-yet-implemented 32-byte global-i64 index entry format.
    #[test]
    fn test_index_entry_on_disk_is_32_bytes_with_global_i64_timestamp() {
        let dir = temp_dir();
        let sub = dir.join("idx32_global_ts");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 1_000, 4096, 4096).unwrap();
        let entry = IndexEntry::new(2_000, 0x0102_0304_0506_0708, 0x0A0B);
        seg.append_entry(&entry).unwrap();

        let base = seg.header_size as usize;
        let mmap = seg.mmap.as_ref().unwrap();
        let bytes = &mmap[base..base + 32];

        assert_eq!(
            &bytes[0..8],
            &2_000i64.to_le_bytes(),
            "timestamp must be stored as a global signed i64, not a u32 delta"
        );
        assert_eq!(
            &bytes[8..16],
            &0x0102_0304_0506_0708u64.to_le_bytes(),
            "block_offset must be a u64 at bytes 8..16"
        );
        assert_eq!(
            &bytes[16..18],
            &0x0A0Bu16.to_le_bytes(),
            "in_block_offset_units must be a u16 at bytes 16..18"
        );
        assert_eq!(
            &bytes[18..32],
            &[0u8; 14],
            "trailing 14 reserved bytes must be written as zero"
        );
        assert_eq!(
            INDEX_ENTRY_SIZE, 32,
            "index entry on-disk size must be 32 bytes"
        );
    }

    // Below-segment-start timestamp: u32 delta cannot encode it, global i64 must.
    #[test]
    fn test_index_entry_stores_global_i64_timestamp_below_segment_start() {
        let dir = temp_dir();
        let sub = dir.join("idx32_below_start");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 1_000, 4096, 4096).unwrap();
        let entry = IndexEntry::new(-50, 0x10, 3);
        seg.append_entry(&entry)
            .expect("a global i64 timestamp below the segment start must serialize");

        let base = seg.header_size as usize;
        let mmap = seg.mmap.as_ref().unwrap();
        assert_eq!(
            &mmap[base..base + 8],
            &(-50i64).to_le_bytes(),
            "negative global timestamp must be stored as signed i64 LE"
        );
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
    fn test_index_entry_rejects_nonzero_reserved_bytes() {
        let entry = IndexEntry::new(7, 8, 9);
        let mut buf = entry.to_bytes_for_segment();
        buf[INDEX_ENTRY_SIZE - 1] = 1;
        let err = IndexEntry::from_bytes_for_segment(&buf).unwrap_err();
        assert!(matches!(err, TmslError::InvalidData(_)));
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

    #[test]
    fn test_index_segment_finds_index_and_entry_together() {
        let dir = temp_dir();
        let sub = dir.join("find_index_and_entry");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096).unwrap();
        seg.append_entry(&IndexEntry::new(100, 10, 1)).unwrap();
        seg.append_entry(&IndexEntry::new(110, 20, 2)).unwrap();
        seg.append_entry(&IndexEntry::new(120, 30, 3)).unwrap();

        let (idx, entry) = seg.find_entry_index_and_entry_cs(110, false, None).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(entry, IndexEntry::new(110, 20, 2));

        assert!(seg
            .find_entry_index_and_entry_cs(115, false, None)
            .is_none());
    }

    proptest::proptest! {
        #[test]
        fn proptest_index_entry_roundtrip(
            segment_start in -1_000_000_000i64..1_000_000_000i64,
            delta in proptest::num::u32::ANY,
            block_offset in proptest::num::u64::ANY,
            in_block_offset in proptest::num::u16::ANY,
        ) {
            let timestamp = segment_start + delta as i64;
            let entry = IndexEntry::new(timestamp, block_offset, in_block_offset);
            let bytes = entry.to_bytes_for_segment();
            let parsed = IndexEntry::from_bytes_for_segment(&bytes).unwrap();
            assert_eq!(parsed.timestamp, timestamp);
            assert_eq!(parsed.block_offset, block_offset);
            assert_eq!(parsed.in_block_offset, in_block_offset);
        }
    }
}
