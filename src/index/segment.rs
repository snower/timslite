//! IndexSegment: single index file with memory-mapped entries and lifecycle management.
//!
//! Each index segment stores 18-byte IndexEntry records (timestamp, block_offset, in_block_offset)
//! in a sorted, append-only fashion.

use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;
use std::time::Instant;

use crate::error::{Result, TmslError};
use crate::header::{FileMetadata, HEADER_SIZE};
use crate::util::{read_i64_from_mmap, read_u16_from_mmap, write_u64_to_mmap};

// ─── IndexEntry ──────────────────────────────────────────────────────────────

pub const INDEX_ENTRY_SIZE: usize = 18;

/// A single index entry: 18 bytes on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub timestamp: i64,
    pub block_offset: u64,    // relative to HEADER_SIZE in the data segment
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
}

impl IndexSegment {
    /// Create a new index segment file at `base_dir/{start_timestamp}`.
    pub fn create(base_dir: &Path, start_timestamp: i64, segment_size: u64) -> Result<Self> {
        std::fs::create_dir_all(base_dir)?;
        // entries_capacity = (segment_size - HEADER_SIZE) / 18
        let entries_capacity = ((segment_size - HEADER_SIZE) / INDEX_ENTRY_SIZE as u64) as usize;
        let file_size = HEADER_SIZE + entries_capacity as u64 * INDEX_ENTRY_SIZE as u64;

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

        let metadata = FileMetadata::create_default(
            crate::header::FILE_TYPE_INDEX,
            start_timestamp,
            file_size as u32,
        );
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
        })
    }

    /// Open an existing index segment by path.
    pub fn open(path: &Path, start_timestamp: i64) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let metadata = FileMetadata::read_from(&mmap)?;

        if metadata.magic != *b"TMSL" {
            return Err(TmslError::InvalidMagic);
        }

        let entries_capacity =
            ((metadata.file_size as u64 - HEADER_SIZE) / INDEX_ENTRY_SIZE as u64) as usize;
        let wrote_count = metadata.record_count as usize;

        Ok(Self {
            path: path.to_path_buf(),
            start_timestamp,
            entries_capacity,
            wrote_count,
            mmap: Some(mmap),
            sealed: false,
            last_accessed_at: Instant::now(),
        })
    }

    /// Append an index entry to the segment.
    pub fn append_entry(&mut self, entry: &IndexEntry) -> Result<()> {
        if self.wrote_count >= self.entries_capacity {
            self.sealed = true;
            return Err(TmslError::InvalidData("index segment full".into()));
        }
        let mmap = self
            .mmap
            .as_mut()
            .ok_or_else(|| TmslError::MmapError("index segment closed".into()))?;

        let pos = HEADER_SIZE as usize + self.wrote_count * INDEX_ENTRY_SIZE;
        mmap[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&entry.to_bytes());
        self.wrote_count += 1;

        // Update file header (state fields at offset 44)
        let abs_pos = (HEADER_SIZE + self.wrote_count as u64 * INDEX_ENTRY_SIZE as u64);
        // wrote_position at offset 44
        mmap[44..52].copy_from_slice(&abs_pos.to_le_bytes());
        // record_count at offset 52
        mmap[52..60].copy_from_slice(&(self.wrote_count as u64).to_le_bytes());

        self.last_accessed_at = Instant::now();
        Ok(())
    }

    /// Whether the segment is full.
    pub fn is_full(&self) -> bool {
        self.wrote_count >= self.entries_capacity
    }

    /// Seal the segment.
    pub fn seal(&mut self) -> Result<()> {
        // Mark as full in header (wrote_position and record_count already updated via append_entry)
        self.sealed = true;
        Ok(())
    }

    // ─── Query operations ────────────────────────────────────────────────

    /// Binary search: find the first entry with timestamp >= target_ts.
    pub fn lower_bound(&self, target_ts: i64) -> usize {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let (mut lo, mut hi) = (0usize, self.wrote_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let pos = HEADER_SIZE as usize + mid * INDEX_ENTRY_SIZE;
            let ts = read_i64_from_mmap(mmap, pos);
            if ts < target_ts {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Binary search: find the first entry with timestamp > target_ts.
    pub fn upper_bound(&self, target_ts: i64) -> usize {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let (mut lo, mut hi) = (0usize, self.wrote_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let pos = HEADER_SIZE as usize + mid * INDEX_ENTRY_SIZE;
            let ts = read_i64_from_mmap(mmap, pos);
            if ts <= target_ts {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
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
            let pos = HEADER_SIZE as usize + mid * INDEX_ENTRY_SIZE;
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

    /// Range query: all entries with timestamp in [start_ts, end_ts].
    pub fn query_range(&self, start_ts: i64, end_ts: i64) -> Vec<IndexEntry> {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let mut results = Vec::new();
        let start_idx = self.lower_bound(start_ts);
        for i in start_idx..self.wrote_count {
            let pos = HEADER_SIZE as usize + i * INDEX_ENTRY_SIZE;
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
        Ok(())
    }

    pub fn idle_close(&mut self) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush()?;
        }
        self.mmap = None;
        self.last_accessed_at = Instant::now();
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush()?;
        }
        self.last_accessed_at = Instant::now();
        Ok(())
    }
}

pub(crate) struct IndexSegmentMeta {
    pub path: std::path::PathBuf,
    pub start_timestamp: i64,
    pub entries_capacity: usize,
}

impl IndexSegmentMeta {
    pub fn new(path: std::path::PathBuf, start_timestamp: i64, entries_capacity: usize) -> Self {
        Self {
            path,
            start_timestamp,
            entries_capacity,
        }
    }
}
