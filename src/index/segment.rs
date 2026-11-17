//! IndexSegment: single index file with memory-mapped entries and lifecycle management.
//!
//! Each index segment uses a compact 16-byte core entry
//! (timestamp: i64, block_offset_delta: u48, in_block_offset_units: u16),
//! followed by `index_data_size` custom bytes and zero padding to the runtime
//! `align_up(16 + index_data_size, 8)` stride.

use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;
use std::time::Instant;

use crate::bg::{SegmentDirtySink, SegmentFlushTarget};
use crate::config::index_entry_stride;
use crate::error::{Result, TmslError};
use crate::header::{write_index_wrote_position_to_mmap, IndexFileMetadata, FIXED_PREFIX_SIZE};

// ─── IndexEntry ──────────────────────────────────────────────────────────────

/// On-disk compact core size before custom bytes and alignment padding.
pub const INDEX_ENTRY_SIZE: usize = 16;

const TIMESTAMP_LEN: usize = 8;
const DELTA_OFFSET: usize = 8;
const DELTA_LEN: usize = 6;
const UNITS_OFFSET: usize = 14;
const UNITS_LEN: usize = 2;
const CUSTOM_OFFSET: usize = INDEX_ENTRY_SIZE;

/// Sentinel value for filler entry block_offset delta (no real data).
pub const BLOCK_OFFSET_FILLER: u64 = 0xFFFF_FFFF_FFFF_FFFF;
/// Sentinel value for filler entry in_block_offset (no real data).
pub const IN_BLOCK_OFFSET_FILLER: u16 = 0xFFFF;

/// All-ones u48 delta paired with `IN_BLOCK_OFFSET_FILLER` denotes a filler.
pub const BLOCK_OFFSET_DELTA_SENTINEL: u64 = 0x0000_FFFF_FFFF_FFFF;
/// Largest delta representable for a real (non-filler) entry.
pub const MAX_REAL_BLOCK_OFFSET_DELTA: u64 = 0x0000_FFFF_FFFF_FFFE;

fn read_u48_le(buf: &[u8]) -> u64 {
    let mut raw = [0u8; 8];
    raw[..DELTA_LEN].copy_from_slice(&buf[..DELTA_LEN]);
    u64::from_le_bytes(raw)
}

fn write_u48_le(buf: &mut [u8], value: u64) {
    let raw = value.to_le_bytes();
    buf[..DELTA_LEN].copy_from_slice(&raw[..DELTA_LEN]);
}

/// A single index entry in memory with an absolute logical block offset.
///
/// On disk the block offset is stored relative to the segment's immutable
/// `base_block_offset`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub timestamp: i64,
    pub block_offset: u64, // absolute logical offset relative to the data area start
    pub in_block_offset: u16, // 4-byte units relative to block payload start
}

impl IndexEntry {
    /// Serialize an entry into its runtime-stride on-disk representation.
    ///
    /// `custom_bytes` must be exactly `index_data_size` long. Filler entries
    /// always write a zero custom region.
    pub fn to_bytes_for_disk(
        &self,
        base_block_offset: u64,
        entry_stride: usize,
        index_data_size: u32,
        custom_bytes: &[u8],
    ) -> Result<Vec<u8>> {
        let expected_stride = index_entry_stride(index_data_size)?;
        if entry_stride != expected_stride {
            return Err(TmslError::InvalidData(format!(
                "index entry stride {} does not match index_data_size {} (expected {})",
                entry_stride, index_data_size, expected_stride
            )));
        }
        if custom_bytes.len() != index_data_size as usize {
            return Err(TmslError::InvalidData(format!(
                "index custom bytes length {} does not match index_data_size {}",
                custom_bytes.len(),
                index_data_size
            )));
        }

        let mut buf = vec![0u8; entry_stride];
        buf[..TIMESTAMP_LEN].copy_from_slice(&self.timestamp.to_le_bytes());

        if self.is_filler() {
            write_u48_le(
                &mut buf[DELTA_OFFSET..DELTA_OFFSET + DELTA_LEN],
                BLOCK_OFFSET_DELTA_SENTINEL,
            );
            buf[UNITS_OFFSET..UNITS_OFFSET + UNITS_LEN]
                .copy_from_slice(&IN_BLOCK_OFFSET_FILLER.to_le_bytes());
            // Custom region and padding stay zero for fillers/deletes.
            return Ok(buf);
        }

        if self.in_block_offset == IN_BLOCK_OFFSET_FILLER {
            return Err(TmslError::InvalidData(format!(
                "real index entry at timestamp {} cannot use the in-block sentinel",
                self.timestamp
            )));
        }
        if self.block_offset == BLOCK_OFFSET_FILLER {
            return Err(TmslError::InvalidData(format!(
                "real index entry at timestamp {} cannot use the block-offset sentinel",
                self.timestamp
            )));
        }
        let delta = self
            .block_offset
            .checked_sub(base_block_offset)
            .ok_or_else(|| {
                TmslError::InvalidData(format!(
                    "index entry block_offset {} is below segment base {}",
                    self.block_offset, base_block_offset
                ))
            })?;
        if delta > MAX_REAL_BLOCK_OFFSET_DELTA {
            return Err(TmslError::InvalidData(format!(
                "index entry block_offset delta {} exceeds real max {:#X}",
                delta, MAX_REAL_BLOCK_OFFSET_DELTA
            )));
        }

        write_u48_le(&mut buf[DELTA_OFFSET..DELTA_OFFSET + DELTA_LEN], delta);
        buf[UNITS_OFFSET..UNITS_OFFSET + UNITS_LEN]
            .copy_from_slice(&self.in_block_offset.to_le_bytes());
        buf[CUSTOM_OFFSET..CUSTOM_OFFSET + index_data_size as usize].copy_from_slice(custom_bytes);
        Ok(buf)
    }

    /// Parse an entry from its runtime-stride on-disk representation.
    pub fn from_bytes_for_disk(
        buf: &[u8],
        base_block_offset: u64,
        entry_stride: usize,
        index_data_size: u32,
    ) -> Result<Self> {
        Self::decode_slot(buf, base_block_offset, entry_stride, index_data_size, false)
            .map(|(entry, _)| entry)
    }

    pub(crate) fn from_bytes_for_disk_with_custom(
        buf: &[u8],
        base_block_offset: u64,
        entry_stride: usize,
        index_data_size: u32,
    ) -> Result<(Self, Vec<u8>)> {
        let (entry, custom) =
            Self::decode_slot(buf, base_block_offset, entry_stride, index_data_size, true)?;
        Ok((entry, custom.unwrap_or_default()))
    }

    fn decode_slot(
        buf: &[u8],
        base_block_offset: u64,
        entry_stride: usize,
        index_data_size: u32,
        capture_custom: bool,
    ) -> Result<(Self, Option<Vec<u8>>)> {
        let expected_stride = index_entry_stride(index_data_size)?;
        if entry_stride != expected_stride {
            return Err(TmslError::InvalidData(format!(
                "index entry stride {} does not match index_data_size {} (expected {})",
                entry_stride, index_data_size, expected_stride
            )));
        }
        if buf.len() != entry_stride {
            return Err(TmslError::InvalidData(format!(
                "index entry buffer length {} does not match stride {}",
                buf.len(),
                entry_stride
            )));
        }

        let custom_len = index_data_size as usize;
        let padding = &buf[CUSTOM_OFFSET + custom_len..];
        if padding.iter().any(|byte| *byte != 0) {
            return Err(TmslError::InvalidData(
                "index entry alignment padding must be zero".into(),
            ));
        }

        let timestamp = i64::from_le_bytes(buf[..TIMESTAMP_LEN].try_into().unwrap());
        let delta = read_u48_le(&buf[DELTA_OFFSET..DELTA_OFFSET + DELTA_LEN]);
        let units = u16::from_le_bytes(
            buf[UNITS_OFFSET..UNITS_OFFSET + UNITS_LEN]
                .try_into()
                .unwrap(),
        );

        if delta == BLOCK_OFFSET_DELTA_SENTINEL {
            if units != IN_BLOCK_OFFSET_FILLER {
                return Err(TmslError::InvalidData(format!(
                    "index entry at timestamp {} has an unpaired filler delta",
                    timestamp
                )));
            }
            let custom =
                capture_custom.then(|| buf[CUSTOM_OFFSET..CUSTOM_OFFSET + custom_len].to_vec());
            return Ok((
                Self::new(timestamp, BLOCK_OFFSET_FILLER, IN_BLOCK_OFFSET_FILLER),
                custom,
            ));
        }
        if units == IN_BLOCK_OFFSET_FILLER {
            return Err(TmslError::InvalidData(format!(
                "index entry at timestamp {} has an unpaired filler in-block offset",
                timestamp
            )));
        }
        let block_offset = base_block_offset.checked_add(delta).ok_or_else(|| {
            TmslError::InvalidData(format!(
                "index block offset base {} + delta {} overflows",
                base_block_offset, delta
            ))
        })?;
        if block_offset == BLOCK_OFFSET_FILLER {
            return Err(TmslError::InvalidData(format!(
                "index entry at timestamp {} decodes to the block-offset sentinel",
                timestamp
            )));
        }
        let custom =
            capture_custom.then(|| buf[CUSTOM_OFFSET..CUSTOM_OFFSET + custom_len].to_vec());
        Ok((Self::new(timestamp, block_offset, units), custom))
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
    /// Immutable base block offset used by the relative on-disk codec.
    pub base_block_offset: u64,
    /// Immutable per-entry custom byte count.
    pub index_data_size: u32,
    /// Runtime entry stride: align_up(16 + index_data_size, 8).
    pub entry_stride: usize,
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

pub(crate) fn capacity_for_file_size(
    file_size: u64,
    header_size: u64,
    stride: usize,
) -> Result<usize> {
    let stride = stride as u64;
    if file_size < header_size || (file_size - header_size) < stride {
        return Err(TmslError::InvalidData(format!(
            "index file size {} cannot hold header {} plus one {}-byte entry",
            file_size, header_size, stride
        )));
    }
    Ok(((file_size - header_size) / stride) as usize)
}

/// Normalize a configured maximum index file size to the aligned immutable
/// `file_size` persisted in segment headers. Creation applies the same
/// rounding so configured sizes need not be exact multiples of the stride.
pub(crate) fn aligned_max_file_size(
    configured_max_file_size: u64,
    header_size: u64,
    stride: usize,
) -> Result<u64> {
    let max_entries_capacity =
        capacity_for_file_size(configured_max_file_size, header_size, stride)?;
    Ok(header_size + max_entries_capacity as u64 * stride as u64)
}

fn validate_index_file(
    mmap: &[u8],
    expected_start_timestamp: Option<i64>,
    expected_index_data_size: Option<u32>,
    expected_max_file_size: Option<u64>,
    index_continuous: bool,
) -> Result<(IndexFileMetadata, usize, Option<i64>)> {
    let metadata = IndexFileMetadata::read_from(mmap)?;
    if let Some(expected) = expected_start_timestamp {
        if metadata.file_offset != expected {
            return Err(TmslError::InvalidData(format!(
                "index segment header start {} does not match file name start {}",
                metadata.file_offset, expected
            )));
        }
    }
    if let Some(expected) = expected_index_data_size {
        if metadata.index_data_size != expected {
            return Err(TmslError::InvalidData(format!(
                "index segment index_data_size {} does not match dataset {}",
                metadata.index_data_size, expected
            )));
        }
    }
    let header_size = metadata.header_size;
    let stride = index_entry_stride(metadata.index_data_size)?;
    if let Some(configured_max_file_size) = expected_max_file_size {
        let expected_file_size =
            aligned_max_file_size(configured_max_file_size, header_size, stride)?;
        if metadata.file_size != expected_file_size {
            return Err(TmslError::InvalidData(format!(
                "index file_size {} does not match aligned dataset maximum {} (configured {})",
                metadata.file_size, expected_file_size, configured_max_file_size
            )));
        }
    }
    let actual_file_size = mmap.len() as u64;
    if actual_file_size > metadata.file_size {
        return Err(TmslError::InvalidData(format!(
            "index file length {} exceeds immutable file_size {}",
            actual_file_size, metadata.file_size
        )));
    }
    let entries_capacity = capacity_for_file_size(actual_file_size, header_size, stride)?;
    if !(actual_file_size - header_size).is_multiple_of(stride as u64) {
        return Err(TmslError::InvalidData(format!(
            "index file length {} is not aligned to {}-byte entries",
            actual_file_size, stride
        )));
    }
    if metadata.wrote_position < header_size {
        return Err(TmslError::InvalidData(format!(
            "index wrote_position {} is below header {}",
            metadata.wrote_position, header_size
        )));
    }
    let entry_area_used = metadata.wrote_position - header_size;
    if !entry_area_used.is_multiple_of(stride as u64) {
        return Err(TmslError::InvalidData(format!(
            "index wrote_position {} is not aligned to {}-byte entries",
            metadata.wrote_position, stride
        )));
    }
    let wrote_count = (entry_area_used / stride as u64) as usize;
    if wrote_count > entries_capacity {
        return Err(TmslError::InvalidData(format!(
            "index wrote_count {} exceeds file capacity {}",
            wrote_count, entries_capacity
        )));
    }

    let mut last_timestamp = None;
    let mut first_timestamp = None;
    for i in 0..wrote_count {
        let pos = header_size as usize + i * stride;
        let end = pos + stride;
        if end > mmap.len() {
            return Err(TmslError::InvalidData(format!(
                "index entry {i} at {pos} is beyond file bounds"
            )));
        }
        let entry = IndexEntry::from_bytes_for_disk(
            &mmap[pos..end],
            metadata.base_block_offset,
            stride,
            metadata.index_data_size,
        )
        .map_err(|e| TmslError::InvalidData(format!("index segment entry {i} malformed: {e}")))?;
        if index_continuous {
            if let Some(start) = expected_start_timestamp {
                // i128 keeps segment boundaries near i64::MAX representable
                // while still rejecting out-of-range slot timestamps.
                let expected = start as i128 + i as i128;
                if entry.timestamp as i128 != expected {
                    return Err(TmslError::InvalidData(format!(
                        "continuous index entry {i} timestamp {} does not match segment start {} slot grid position {}",
                        entry.timestamp, start, expected
                    )));
                }
            }
        } else if let Some(previous) = last_timestamp {
            if entry.timestamp <= previous {
                return Err(TmslError::InvalidData(format!(
                    "non-continuous index entry {i} timestamp {} is not strictly greater than previous timestamp {}",
                    entry.timestamp, previous
                )));
            }
        }
        if i == 0 {
            first_timestamp = Some(entry.timestamp);
        }
        last_timestamp = Some(entry.timestamp);
    }
    if let (Some(expected_start), Some(first)) = (expected_start_timestamp, first_timestamp) {
        if first != expected_start {
            return Err(TmslError::InvalidData(format!(
                "index segment first entry timestamp {} does not match segment start {}",
                first, expected_start
            )));
        }
    }

    Ok((metadata, wrote_count, last_timestamp))
}

impl IndexSegment {
    /// Create a new index segment file at `base_dir/{start_timestamp}`.
    pub fn create(
        base_dir: &Path,
        start_timestamp: i64,
        initial_size: u64,
        max_file_size: u64,
        base_block_offset: u64,
        index_data_size: u32,
    ) -> Result<Self> {
        Self::create_with_compression(
            base_dir,
            start_timestamp,
            initial_size,
            max_file_size,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            base_block_offset,
            index_data_size,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_with_compression(
        base_dir: &Path,
        start_timestamp: i64,
        initial_size: u64,
        max_file_size: u64,
        compress_level: u8,
        compress_type: u8,
        base_block_offset: u64,
        index_data_size: u32,
    ) -> Result<Self> {
        crate::compress::validate_compress_type(compress_type)?;
        if base_block_offset == BLOCK_OFFSET_FILLER {
            return Err(TmslError::InvalidData(
                "index segment base block offset cannot be the filler sentinel".into(),
            ));
        }
        let stride = index_entry_stride(index_data_size)?;
        let metadata = IndexFileMetadata::create_default(
            start_timestamp,
            max_file_size,
            compress_level,
            compress_type,
            base_block_offset,
            index_data_size,
        );
        let header_size = metadata.header_size;
        if initial_size < header_size + stride as u64 {
            return Err(TmslError::InvalidData(format!(
                "initial index segment size {} cannot hold header {} plus one {}-byte entry",
                initial_size, header_size, stride
            )));
        }
        let entries_capacity = capacity_for_file_size(initial_size, header_size, stride)?;
        let max_file_size = aligned_max_file_size(max_file_size, header_size, stride)?;
        let file_size = header_size + entries_capacity as u64 * stride as u64;

        let file_name = format!("{:020}", start_timestamp);
        let path = base_dir.join(&file_name);
        std::fs::create_dir_all(base_dir)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(file_size)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        // Recompute with the aligned immutable maximum.
        let mut metadata = metadata;
        metadata.file_size = max_file_size;
        metadata.write_to(&mut mmap);
        metadata.sync(&mut mmap)?;

        Ok(Self {
            path,
            start_timestamp,
            base_block_offset,
            index_data_size,
            entry_stride: stride,
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
    pub fn open(
        path: &Path,
        start_timestamp: i64,
        max_file_size: u64,
        index_data_size: u32,
    ) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let (metadata, wrote_count, _) = validate_index_file(
            &mmap,
            Some(start_timestamp),
            Some(index_data_size),
            Some(max_file_size),
            false,
        )?;
        let header_size = metadata.header_size;
        let stride = index_entry_stride(metadata.index_data_size)?;
        let actual_file_size = mmap.len() as u64;
        let entries_capacity = capacity_for_file_size(actual_file_size, header_size, stride)?;
        let max_file_size = aligned_max_file_size(max_file_size, header_size, stride)?;

        Ok(Self {
            path: path.to_path_buf(),
            start_timestamp,
            base_block_offset: metadata.base_block_offset,
            index_data_size,
            entry_stride: stride,
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
        let custom_storage;
        let custom: &[u8] = if self.index_data_size == 0 {
            &[]
        } else {
            custom_storage = vec![0u8; self.index_data_size as usize];
            &custom_storage
        };
        let bytes = entry.to_bytes_for_disk(
            self.base_block_offset,
            self.entry_stride,
            self.index_data_size,
            custom,
        )?;
        let mmap = self
            .mmap
            .as_mut()
            .ok_or_else(|| TmslError::MmapError("index segment closed".into()))?;

        let pos = self.header_size as usize + self.wrote_count * self.entry_stride;
        let end = pos + self.entry_stride;
        mmap[pos..end].copy_from_slice(&bytes);
        self.wrote_count += 1;

        let abs_pos = self.header_size + self.wrote_count as u64 * self.entry_stride as u64;
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
        let current_capacity =
            capacity_for_file_size(self.current_file_size, self.header_size, self.entry_stride)?;
        let max_capacity =
            capacity_for_file_size(self.max_file_size, self.header_size, self.entry_stride)?;
        let doubled = self.current_file_size.saturating_mul(2);
        let target_capacity = capacity_for_file_size(
            doubled.max(self.header_size + self.entry_stride as u64),
            self.header_size,
            self.entry_stride,
        )?
        .max(current_capacity + 1)
        .min(max_capacity);
        if target_capacity <= current_capacity {
            return Err(TmslError::InvalidData(format!(
                "index segment already at max_file_size ({})",
                self.max_file_size
            )));
        }
        let target = self.header_size + target_capacity as u64 * self.entry_stride as u64;

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
        self.entries_capacity = target_capacity;
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

    fn entry_pos(&self, entry_index: usize) -> usize {
        self.header_size as usize + entry_index * self.entry_stride
    }

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
        let pos = self.entry_pos(entry_index);
        let ts = Self::read_timestamp_at_pos(mmap, pos)?;
        if ts != target_ts {
            return None; // Defensive: should never happen in continuous mode
        }
        Self::read_entry_at_pos(
            mmap,
            pos,
            self.base_block_offset,
            self.entry_stride,
            self.index_data_size,
        )
    }

    fn read_timestamp_at_pos(mmap: &[u8], pos: usize) -> Option<i64> {
        if pos + TIMESTAMP_LEN > mmap.len() {
            return None;
        }
        Some(i64::from_le_bytes(
            mmap[pos..pos + TIMESTAMP_LEN].try_into().ok()?,
        ))
    }

    fn read_entry_at_pos(
        mmap: &[u8],
        pos: usize,
        base_block_offset: u64,
        entry_stride: usize,
        index_data_size: u32,
    ) -> Option<IndexEntry> {
        if pos + entry_stride > mmap.len() {
            return None;
        }
        IndexEntry::from_bytes_for_disk(
            &mmap[pos..pos + entry_stride],
            base_block_offset,
            entry_stride,
            index_data_size,
        )
        .ok()
    }

    /// Binary search: find the first entry with timestamp >= target_ts.
    pub fn lower_bound(&self, target_ts: i64) -> usize {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let (mut lo, mut hi) = (0usize, self.wrote_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let pos = self.entry_pos(mid);
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
            let pos = self.entry_pos(mid);
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
            let pos = self.entry_pos(mid);
            let ts = Self::read_timestamp_at_pos(mmap, pos)?;
            match ts.cmp(&target_ts) {
                std::cmp::Ordering::Equal => {
                    return Self::read_entry_at_pos(
                        mmap,
                        pos,
                        self.base_block_offset,
                        self.entry_stride,
                        self.index_data_size,
                    );
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
            let pos = self.entry_pos(mid);
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
                    let pos = self.entry_pos(entry_index);
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
            let pos = self.entry_pos(entry_index);
            let ts = Self::read_timestamp_at_pos(mmap, pos)?;
            if ts != target_ts {
                return None;
            }
            return Self::read_entry_at_pos(
                mmap,
                pos,
                self.base_block_offset,
                self.entry_stride,
                self.index_data_size,
            )
            .map(|entry| (entry_index, entry));
        }

        if self.wrote_count == 0 {
            return None;
        }
        let (mut lo, mut hi) = (0usize, self.wrote_count - 1);
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let pos = self.entry_pos(mid);
            let ts = Self::read_timestamp_at_pos(mmap, pos)?;
            match ts.cmp(&target_ts) {
                std::cmp::Ordering::Equal => {
                    return Self::read_entry_at_pos(
                        mmap,
                        pos,
                        self.base_block_offset,
                        self.entry_stride,
                        self.index_data_size,
                    )
                    .map(|entry| (mid, entry));
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
    ///
    /// A real → real overwrite preserves the existing custom region. Delete
    /// (real/filler → filler) and reinsertion (filler → real) clear/initialize
    /// it. The complete slot is encoded and validated before mmap mutation.
    pub fn overwrite_entry(&mut self, entry_index: usize, new_entry: &IndexEntry) -> Result<()> {
        if entry_index >= self.wrote_count {
            return Err(TmslError::InvalidData(format!(
                "entry index {} out of range [0, {})",
                entry_index, self.wrote_count
            )));
        }
        let pos = self.entry_pos(entry_index);
        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| TmslError::MmapError("index segment closed".into()))?;
        if pos + self.entry_stride > mmap.len() {
            return Err(TmslError::InvalidData(format!(
                "index entry {entry_index} at {pos} is beyond file bounds"
            )));
        }
        let (old_entry, custom) = IndexEntry::from_bytes_for_disk_with_custom(
            &mmap[pos..pos + self.entry_stride],
            self.base_block_offset,
            self.entry_stride,
            self.index_data_size,
        )?;
        let custom_out = if !old_entry.is_filler() && !new_entry.is_filler() {
            custom
        } else if self.index_data_size == 0 {
            Vec::new()
        } else {
            vec![0u8; self.index_data_size as usize]
        };
        let bytes = new_entry.to_bytes_for_disk(
            self.base_block_offset,
            self.entry_stride,
            self.index_data_size,
            &custom_out,
        )?;

        let mmap = self
            .mmap
            .as_mut()
            .ok_or_else(|| TmslError::MmapError("index segment closed".into()))?;
        mmap[pos..pos + self.entry_stride].copy_from_slice(&bytes);
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
        let pos = self.entry_pos(entry_index);
        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| TmslError::MmapError("index segment closed".into()))?;
        if pos + self.entry_stride > mmap.len() {
            return Err(TmslError::InvalidData(format!(
                "index entry {entry_index} at {pos} is beyond file bounds"
            )));
        }
        self.last_accessed_at = Instant::now();
        IndexEntry::from_bytes_for_disk(
            &mmap[pos..pos + self.entry_stride],
            self.base_block_offset,
            self.entry_stride,
            self.index_data_size,
        )
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
        let pos = self.entry_pos(self.wrote_count - 1);
        Self::read_timestamp_at_pos(mmap, pos)
    }

    /// Range query: all entries with timestamp in [start_ts, end_ts].
    pub fn query_range(&self, start_ts: i64, end_ts: i64) -> Vec<IndexEntry> {
        let mmap = self.mmap.as_ref().expect("index segment must be open");
        let mut results = Vec::new();
        let start_idx = self.lower_bound(start_ts);
        for i in start_idx..self.wrote_count {
            let pos = self.entry_pos(i);
            let Some(ts) = Self::read_timestamp_at_pos(mmap, pos) else {
                break;
            };
            if ts > end_ts {
                break;
            }
            if let Some(entry) = Self::read_entry_at_pos(
                mmap,
                pos,
                self.base_block_offset,
                self.entry_stride,
                self.index_data_size,
            ) {
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
            let pos = self.entry_pos(i);
            let Some(ts) = Self::read_timestamp_at_pos(mmap, pos) else {
                break;
            };
            if ts > end_ts {
                break;
            }
            if let Some(entry) = Self::read_entry_at_pos(
                mmap,
                pos,
                self.base_block_offset,
                self.entry_stride,
                self.index_data_size,
            ) {
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
    pub base_block_offset: u64,
    pub index_data_size: u32,
    pub last_timestamp: Option<i64>,
}

impl IndexSegmentMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path: std::path::PathBuf,
        start_timestamp: i64,
        entries_capacity: usize,
        wrote_count: usize,
        header_size: u64,
        base_block_offset: u64,
        index_data_size: u32,
    ) -> Self {
        Self {
            path,
            start_timestamp,
            entries_capacity,
            wrote_count,
            header_size,
            base_block_offset,
            index_data_size,
            last_timestamp: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_last_timestamp(
        path: std::path::PathBuf,
        start_timestamp: i64,
        entries_capacity: usize,
        wrote_count: usize,
        header_size: u64,
        base_block_offset: u64,
        index_data_size: u32,
        last_timestamp: Option<i64>,
    ) -> Self {
        Self {
            path,
            start_timestamp,
            entries_capacity,
            wrote_count,
            header_size,
            base_block_offset,
            index_data_size,
            last_timestamp,
        }
    }
}

/// Strictly inspect an index file without opening a mutable segment.
///
/// Returns `(wrote_count, header_size, last_timestamp, base_block_offset)`.
/// Any malformed discovered file is reported as an error.
pub(crate) fn inspect_index_file(
    path: &Path,
    expected_start_timestamp: i64,
    expected_index_data_size: u32,
    expected_max_file_size: u64,
    index_continuous: bool,
) -> Result<(usize, u64, Option<i64>, u64)> {
    let file = OpenOptions::new().read(true).open(path)?;
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
    let (metadata, wrote_count, last_timestamp) = validate_index_file(
        &mmap,
        Some(expected_start_timestamp),
        Some(expected_index_data_size),
        Some(expected_max_file_size),
        index_continuous,
    )?;
    Ok((
        wrote_count,
        metadata.header_size,
        last_timestamp,
        metadata.base_block_offset,
    ))
}

/// Read the last entry's timestamp from an index segment file, if non-empty.
///
/// Opens the file in read-only mmap mode (safe on Windows), reads the compact
/// entry using the file's own stride and base, and immediately drops the
/// mmap+file handle. Malformed files return an error.
pub fn last_entry_timestamp(path: &Path) -> Result<Option<i64>> {
    let file = OpenOptions::new().read(true).open(path)?;
    if file.metadata()?.len() < FIXED_PREFIX_SIZE as u64 {
        return Ok(None);
    }
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
    let (_, _, last_timestamp) = validate_index_file(&mmap, None, None, None, false)?;
    Ok(last_timestamp)
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

    #[test]
    fn test_compact_entry_layout_and_strides() {
        for (n, stride) in [(0u32, 16usize), (3, 24), (16, 32)] {
            let base = 0x1234_5678_9ABCu64;
            let entry = IndexEntry::new(-42, base + 0x0102_0304_0506, 0xBEEF);
            let custom = vec![0x5Au8; n as usize];
            let bytes = entry.to_bytes_for_disk(base, stride, n, &custom).unwrap();
            assert_eq!(bytes.len(), stride);
            assert_eq!(&bytes[0..8], &(-42i64).to_le_bytes());
            assert_eq!(read_u48_le(&bytes[8..14]), 0x0102_0304_0506);
            assert_eq!(&bytes[14..16], &0xBEEFu16.to_le_bytes());
            assert_eq!(&bytes[16..16 + n as usize], &custom[..]);
            assert!(bytes[16 + n as usize..].iter().all(|b| *b == 0));

            let decoded = IndexEntry::from_bytes_for_disk(&bytes, base, stride, n).unwrap();
            assert_eq!(decoded, entry);
        }
    }

    #[test]
    fn test_entry_delta_bounds_and_sentinels() {
        let base = 1_000u64;
        let max = MAX_REAL_BLOCK_OFFSET_DELTA;
        let max_entry = IndexEntry::new(1, base + max, 9);
        let stride = index_entry_stride(0).unwrap();
        assert!(max_entry.to_bytes_for_disk(base, stride, 0, &[]).is_ok());

        // Delta zero is valid.
        let zero = IndexEntry::new(1, base, 0);
        assert!(zero.to_bytes_for_disk(base, stride, 0, &[]).is_ok());

        // Overflow one past max is rejected.
        let overflow = IndexEntry::new(1, base + max + 1, 0);
        assert!(overflow.to_bytes_for_disk(base, stride, 0, &[]).is_err());

        // Underflow is rejected.
        let underflow = IndexEntry::new(1, base - 1, 0);
        assert!(underflow.to_bytes_for_disk(base, stride, 0, &[]).is_err());

        // Real entries cannot use either sentinel field.
        let units_sentinel = IndexEntry::new(1, base + 1, IN_BLOCK_OFFSET_FILLER);
        assert!(units_sentinel
            .to_bytes_for_disk(base, stride, 0, &[])
            .is_err());

        // Paired sentinel decodes as a filler.
        let filler = IndexEntry::new(5, BLOCK_OFFSET_FILLER, IN_BLOCK_OFFSET_FILLER);
        let bytes = filler.to_bytes_for_disk(base, stride, 0, &[]).unwrap();
        assert_eq!(read_u48_le(&bytes[8..14]), BLOCK_OFFSET_DELTA_SENTINEL);
        assert_eq!(
            IndexEntry::from_bytes_for_disk(&bytes, base, stride, 0).unwrap(),
            filler
        );

        // An unpaired sentinel is corrupt.
        let mut unpaired_delta = bytes.clone();
        unpaired_delta[14..16].copy_from_slice(&1u16.to_le_bytes());
        assert!(IndexEntry::from_bytes_for_disk(&unpaired_delta, base, stride, 0).is_err());

        let mut unpaired_units = IndexEntry::new(5, base + 5, 1)
            .to_bytes_for_disk(base, stride, 0, &[])
            .unwrap();
        unpaired_units[14..16].copy_from_slice(&IN_BLOCK_OFFSET_FILLER.to_le_bytes());
        assert!(IndexEntry::from_bytes_for_disk(&unpaired_units, base, stride, 0).is_err());

        // A real entry whose absolute offset equals the u64 sentinel is
        // rejected even when its relative delta is otherwise legal.
        let near_max_base = BLOCK_OFFSET_FILLER - 1;
        assert!(IndexEntry::new(1, BLOCK_OFFSET_FILLER, 0)
            .to_bytes_for_disk(near_max_base, stride, 0, &[])
            .is_err());
        let mut sentinel_absolute = vec![0u8; stride];
        sentinel_absolute[0..8].copy_from_slice(&1i64.to_le_bytes());
        write_u48_le(&mut sentinel_absolute[8..14], 1);
        sentinel_absolute[14..16].copy_from_slice(&0u16.to_le_bytes());
        assert!(
            IndexEntry::from_bytes_for_disk(&sentinel_absolute, near_max_base, stride, 0).is_err()
        );
    }

    #[test]
    fn test_entry_padding_must_be_zero() {
        let base = 0;
        let n = 3u32;
        let stride = index_entry_stride(n).unwrap();
        let entry = IndexEntry::new(10, 10, 1);
        let mut bytes = entry
            .to_bytes_for_disk(base, stride, n, &[1, 2, 3])
            .unwrap();
        let padding_pos = 16 + n as usize;
        bytes[padding_pos] = 1;
        let err = IndexEntry::from_bytes_for_disk(&bytes, base, stride, n).unwrap_err();
        assert!(format!("{err}").contains("padding"));
    }

    #[test]
    fn test_failed_overwrite_leaves_original_slot_unchanged() {
        let dir = temp_dir();
        let sub = dir.join("failed_overwrite");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096, 1_000, 3).unwrap();
        seg.append_entry(&IndexEntry::new(10, 1_010, 1)).unwrap();
        let pos = seg.header_size as usize;
        let before = seg.mmap.as_ref().unwrap()[pos..pos + seg.entry_stride].to_vec();

        // Below base (negative delta) fails before touching mmap.
        assert!(seg
            .overwrite_entry(0, &IndexEntry::new(10, 999, 2))
            .is_err());
        // Excessive delta fails before touching mmap.
        assert!(seg
            .overwrite_entry(
                0,
                &IndexEntry::new(10, 1_000 + MAX_REAL_BLOCK_OFFSET_DELTA + 1, 2,),
            )
            .is_err());

        let after = &seg.mmap.as_ref().unwrap()[pos..pos + seg.entry_stride];
        assert_eq!(before, after);
        assert_eq!(seg.find_exact(10).unwrap(), IndexEntry::new(10, 1_010, 1));
    }

    #[test]
    fn test_strict_load_rejects_padding_and_size_mismatch() {
        let dir = temp_dir();
        let sub = dir.join("strict_load");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096, 100, 3).unwrap();
        seg.append_entry(&IndexEntry::new(10, 110, 1)).unwrap();
        let path = seg.path.clone();
        drop(seg);

        // Correct file inspects.
        assert_eq!(inspect_index_file(&path, 10, 3, 4096, false).unwrap().0, 1);
        // Dataset/header sizing mismatch fails.
        assert!(inspect_index_file(&path, 10, 0, 4096, false).is_err());
        assert!(IndexSegment::open(&path, 10, 4096, 0).is_err());
        assert!(IndexSegment::open(&path, 10, 4096, 3).is_ok());

        // Corrupt alignment padding -> discovered malformed file fails.
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
            let padding = 128 + 16 + 3;
            mmap[padding] = 1;
            mmap.flush().unwrap();
        }
        assert!(inspect_index_file(&path, 10, 3, 4096, false).is_err());
        assert!(IndexSegment::open(&path, 0, 4096, 3).is_err());

        // Restore padding, then make file length unaligned.
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
            mmap[128 + 16 + 3] = 0;
            mmap.flush().unwrap();
        }
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(128 + 21)
            .unwrap();
        assert!(inspect_index_file(&path, 10, 3, 4096, false).is_err());
    }

    #[test]
    fn test_last_entry_timestamp_uses_file_stride() {
        let dir = temp_dir();
        let sub = dir.join("last_ts");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096, 500, 16).unwrap();
        let path = seg.path.clone();
        assert_eq!(last_entry_timestamp(&path).unwrap(), None);
        seg.append_entry(&IndexEntry::new(11, 500, 0)).unwrap();
        seg.append_entry(&IndexEntry::new(12, 600, 1)).unwrap();
        assert_eq!(last_entry_timestamp(&path).unwrap(), Some(12));
    }

    #[test]
    fn test_custom_bytes_survive_real_to_real_overwrite() {
        let dir = temp_dir();
        let sub = dir.join("custom_preserve");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096, 1000, 3).unwrap();
        assert_eq!(seg.entry_stride, 24);
        seg.append_entry(&IndexEntry::new(10, 1000, 1)).unwrap();

        let pos = seg.header_size as usize;
        {
            let mmap = seg.mmap.as_mut().unwrap();
            mmap[pos + 16] = 0xAA;
            mmap[pos + 17] = 0xBB;
            mmap[pos + 18] = 0xCC;
        }
        seg.overwrite_entry(0, &IndexEntry::new(10, 1010, 2))
            .unwrap();
        let mmap = seg.mmap.as_ref().unwrap();
        assert_eq!(&mmap[pos + 16..pos + 19], &[0xAA, 0xBB, 0xCC]);
        assert_eq!(mmap[pos + 19..pos + 24], [0; 5]);
        assert_eq!(seg.find_exact(10).unwrap().block_offset, 1010);

        // Delete clears the custom region.
        seg.overwrite_entry(
            0,
            &IndexEntry::new(10, BLOCK_OFFSET_FILLER, IN_BLOCK_OFFSET_FILLER),
        )
        .unwrap();
        let mmap = seg.mmap.as_ref().unwrap();
        assert_eq!(&mmap[pos + 16..pos + 19], &[0, 0, 0]);

        // Reinsertion initializes it again.
        seg.overwrite_entry(0, &IndexEntry::new(10, 1020, 3))
            .unwrap();
        let mmap = seg.mmap.as_ref().unwrap();
        assert_eq!(&mmap[pos + 16..pos + 19], &[0, 0, 0]);
    }

    #[test]
    fn test_open_reads_entries_after_extended_header() {
        let dir = temp_dir();
        let sub = dir.join("extended_header");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 10, 4096, 4096, 100, 0).unwrap();
        seg.append_entry(&IndexEntry::new(10, 100, 1)).unwrap();
        seg.append_entry(&IndexEntry::new(20, 200, 2)).unwrap();
        let path = seg.path.clone();
        seg.header_size = INDEX_HEADER_SIZE;
        drop(seg);

        let reopened = IndexSegment::open(&path, 10, 4096, 0).unwrap();
        assert_eq!(reopened.header_size, INDEX_HEADER_SIZE);
        assert_eq!(reopened.wrote_count, 2);
        assert_eq!(reopened.find_exact(10).unwrap().block_offset, 100);
        assert_eq!(reopened.find_exact(20).unwrap().in_block_offset, 2);
    }

    #[test]
    fn test_index_segment_find_entry_index() {
        let dir = temp_dir();
        let sub = dir.join("find_entry_index");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096, 0, 0).unwrap();
        for i in 0..20 {
            seg.append_entry(&IndexEntry::new(i * 10, i as u64 * 100, (i * 3) as u16))
                .unwrap();
        }
        assert_eq!(seg.find_entry_index(50), Some(5));
        assert_eq!(seg.find_entry_index(0), Some(0));
        assert_eq!(seg.find_entry_index(190), Some(19));
        assert_eq!(seg.find_entry_index(55), None);
        assert_eq!(seg.find_entry_index(-1), None);
        assert_eq!(seg.find_entry_index(200), None);
    }

    #[test]
    fn test_index_segment_overwrite_entry() {
        let dir = temp_dir();
        let sub = dir.join("overwrite_entry");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096, 50, 0).unwrap();
        seg.append_entry(&IndexEntry::new(
            100,
            BLOCK_OFFSET_FILLER,
            IN_BLOCK_OFFSET_FILLER,
        ))
        .unwrap();
        seg.append_entry(&IndexEntry::new(
            200,
            BLOCK_OFFSET_FILLER,
            IN_BLOCK_OFFSET_FILLER,
        ))
        .unwrap();

        let new_entry = IndexEntry::new(200, 12345, 42);
        seg.overwrite_entry(1, &new_entry).unwrap();
        let found = seg.find_exact(200).unwrap();
        assert_eq!(found.block_offset, 12345);
        assert_eq!(found.in_block_offset, 42);
        assert!(seg.overwrite_entry(5, &IndexEntry::new(999, 0, 0)).is_err());
    }

    #[test]
    fn test_index_segment_finds_index_and_entry_together() {
        let dir = temp_dir();
        let sub = dir.join("find_index_and_entry");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096, 10, 0).unwrap();
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
            timestamp in -1_000_000_000i64..1_000_000_000i64,
            base in 0u64..1_000_000u64,
            delta in 0u64..=MAX_REAL_BLOCK_OFFSET_DELTA.min(1_000_000),
            in_block_offset in 0u16..u16::MAX,
            index_data_size in 0u32..=32,
        ) {
            let stride = index_entry_stride(index_data_size).unwrap();
            let custom = vec![0x5Au8; index_data_size as usize];
            let entry = IndexEntry::new(timestamp, base + delta, in_block_offset);
            let bytes = entry.to_bytes_for_disk(base, stride, index_data_size, &custom).unwrap();
            let parsed = IndexEntry::from_bytes_for_disk(&bytes, base, stride, index_data_size).unwrap();
            assert_eq!(parsed, entry);
        }
    }

    fn rewrite_header_file_size(path: &Path, file_size: u64) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let mut metadata = IndexFileMetadata::read_from(&mmap).unwrap();
        metadata.file_size = file_size;
        metadata.write_to(&mut mmap);
        mmap.flush().unwrap();
    }

    fn overwrite_timestamp(path: &Path, slot: usize, timestamp: i64) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let pos = 128 + slot * 16;
        mmap[pos..pos + 8].copy_from_slice(&timestamp.to_le_bytes());
        mmap.flush().unwrap();
    }

    #[test]
    fn test_strict_inspect_rejects_header_max_file_size_mismatch() {
        let dir = temp_dir();
        let sub = dir.join("max_mismatch");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 0, 4096, 4096, 100, 0).unwrap();
        seg.append_entry(&IndexEntry::new(0, 100, 1)).unwrap();
        let path = seg.path.clone();
        drop(seg);

        assert!(inspect_index_file(&path, 0, 0, 4096, false).is_ok());
        assert!(IndexSegment::open(&path, 0, 4096, 0).is_ok());

        // Another aligned capacity (128 + 504 * 16 = 8192) is still invalid
        // for a dataset configured with 4096.
        rewrite_header_file_size(&path, 8192);
        let err = inspect_index_file(&path, 0, 0, 4096, false).unwrap_err();
        assert!(format!("{err}").contains("file_size"), "err: {err}");
        assert!(IndexSegment::open(&path, 0, 4096, 0).is_err());
    }

    #[test]
    fn test_continuous_slot_grid_is_validated_on_inspect() {
        let dir = temp_dir();
        let sub = dir.join("slot_grid");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let mut seg = IndexSegment::create(&sub, 100, 4096, 4096, 1_000, 0).unwrap();
        for ts in 100..104 {
            seg.append_entry(&IndexEntry::new(ts, 1_000 + ts as u64, 0))
                .unwrap();
        }
        let path = seg.path.clone();
        drop(seg);

        // Shift the last slot upward: still decodable and ordered, so only the
        // continuous slot grid can reject it.
        overwrite_timestamp(&path, 3, 200);
        let err = inspect_index_file(&path, 100, 0, 4096, true).unwrap_err();
        assert!(format!("{err}").contains("grid"), "err: {err}");
        assert!(inspect_index_file(&path, 100, 0, 4096, false).is_ok());
    }

    #[test]
    fn test_irregular_configured_max_file_size_normalizes() {
        let dir = temp_dir();
        let sub = dir.join("irregular_max");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        // N=3 => 24-byte stride; 4097 normalizes to 128 + 165 * 24 = 4088.
        let mut seg = IndexSegment::create(&sub, 0, 4097, 4097, 500, 3).unwrap();
        assert_eq!(seg.entry_stride, 24);
        assert_eq!(seg.current_file_size, 4088);
        assert_eq!(seg.max_file_size, 4088);
        seg.append_entry(&IndexEntry::new(0, 500, 0)).unwrap();
        let path = seg.path.clone();
        drop(seg);

        assert!(inspect_index_file(&path, 0, 3, 4097, false).is_ok());
        assert!(IndexSegment::open(&path, 0, 4097, 3).is_ok());

        // A different aligned capacity (128 + 166 * 24 = 4112) is rejected.
        rewrite_header_file_size(&path, 4112);
        assert!(inspect_index_file(&path, 0, 3, 4097, false).is_err());
    }
}
