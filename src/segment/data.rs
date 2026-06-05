//! DataSegment: single data file with Block management, aggregation, and mmap lifecycle.
//!
//! This is the core storage module: records are aggregated into blocks (max 64KB),
//! blocks are compressed when sealed, and files use mmap with lazy open/close.

use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;
use std::time::Instant;

use crate::block::{
    BlockHeader, BLOCK_FLAG_COMPRESSED, BLOCK_FLAG_SEALED, BLOCK_FLAG_SINGLE_RECORD,
};
use crate::cache::{BlockCache, CacheKey, HotBlockCache};
use crate::compress::{deflate_compress, deflate_decompress};
use crate::error::{Result, TmslError};
use crate::header::{
    clear_pending_from_mmap, write_data_core_state_to_mmap,
    write_data_invalid_record_count_to_mmap, write_data_pending_state_to_mmap, DataFileMetadata,
    PENDING_NONE, TIMESTAMP_MAX_SENTINEL, TIMESTAMP_MIN_SENTINEL,
};
use crate::util::{read_i64_le, read_u16_from_mmap, read_u32_from_mmap, read_u32_le};

// ─── Types ───────────────────────────────────────────────────────────────────

/// Lifecycle state of a data segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentLifecycle {
    /// File is not memory-mapped.
    Closed,
    /// File is open, mmap is valid, read/write operations are allowed.
    OpenReady,
}

/// A single data segment backed by a memory-mapped file.
///
/// Multiple records are aggregated into blocks. When a pending block reaches
/// the size limit, it is sealed and compressed.
pub struct DataSegment {
    pub path: std::path::PathBuf,
    pub file_offset: u64,
    /// Runtime actual size (grows with expansion). Header file_size always = max_file_size.
    pub file_size: u64,
    /// Expansion upper limit (segment_size, immutable).
    pub max_file_size: u64,
    /// Physical byte offset where block data starts in this file.
    pub header_size: u64,
    /// Bytes used in the data area, excluding the variable file header.
    ///
    /// The on-disk header state field `wrote_position` stores the absolute file
    /// offset: `header_size + data_wrote_position`.
    pub data_wrote_position: u64,
    pub record_count: u64,
    pub total_uncompressed_size: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub created_at: i64,
    pub mmap: Option<MmapMut>,
    pub lifecycle: SegmentLifecycle,
    pub last_accessed_at: Instant,

    // ─── Pending block state (from header state) ──────────────────────────
    pub pending_block_offset: Option<u64>,
    pub pending_wrote_position: u64,
    pub pending_record_count: u64,
    pub invalid_record_count: u64,
}

// ─── Construction ────────────────────────────────────────────────────────────

impl DataSegment {
    /// Create a new segment file at `path`, memory-mapped and ready for writing.
    ///
    /// The actual file on disk is truncated to `initial_size` to save disk space.
    /// The header always records `max_file_size` as the standard segment size.
    pub fn create(
        path: &Path,
        file_offset: u64,
        initial_size: u64,
        max_file_size: u64,
    ) -> Result<Self> {
        let metadata = DataFileMetadata::create_default(file_offset as i64, max_file_size as u32);
        let header_size = metadata.header_size;
        if initial_size < header_size {
            return Err(TmslError::InvalidData(format!(
                "initial data segment size {} is smaller than header {}",
                initial_size, header_size
            )));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(initial_size)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        metadata.write_to(&mut mmap);
        metadata.sync(&mut mmap)?;

        let now = Instant::now();
        Ok(Self {
            path: path.to_path_buf(),
            file_offset,
            file_size: initial_size,
            max_file_size,
            header_size,
            data_wrote_position: 0,
            record_count: 0,
            total_uncompressed_size: 0,
            min_timestamp: TIMESTAMP_MIN_SENTINEL,
            max_timestamp: TIMESTAMP_MAX_SENTINEL,
            created_at: metadata.created_at,
            mmap: Some(mmap),
            lifecycle: SegmentLifecycle::OpenReady,
            last_accessed_at: now,
            pending_block_offset: None,
            pending_wrote_position: 0,
            pending_record_count: 0,
            invalid_record_count: 0,
        })
    }

    /// Open an existing segment file, memory-map it, and recover state.
    ///
    /// `max_file_size` is passed in because it is a configuration parameter (segment_size)
    /// not stored in the file. The actual file size is read from disk metadata.
    ///
    /// If the file has a pending block (pending_block_offset != u64::MAX), it is
    /// restored as pending raw state. Reopen does not seal or compress it.
    pub fn open(path: &Path, file_offset: u64, max_file_size: u64) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let actual_file_size = file.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let metadata = DataFileMetadata::read_from(&mmap)?;
        let header_size = metadata.header_size;

        if metadata.magic != *b"TMSL" {
            return Err(TmslError::InvalidMagic);
        }
        if metadata.version > 1 {
            log::warn!(
                "Opening file with version {}, expected 1. Parsing known fields.",
                metadata.version
            );
        }

        let now = Instant::now();
        let mut seg = Self {
            path: path.to_path_buf(),
            file_offset,
            file_size: actual_file_size,
            max_file_size,
            header_size,
            data_wrote_position: metadata.wrote_position.saturating_sub(header_size),
            record_count: metadata.record_count,
            total_uncompressed_size: metadata.total_uncompressed_size,
            min_timestamp: metadata.min_timestamp,
            max_timestamp: metadata.max_timestamp,
            created_at: metadata.created_at,
            mmap: Some(mmap),
            lifecycle: SegmentLifecycle::OpenReady,
            last_accessed_at: now,
            pending_block_offset: None,
            pending_wrote_position: 0,
            pending_record_count: 0,
            invalid_record_count: metadata.invalid_record_count,
        };

        // Pending recovery
        seg.restore_pending_from_metadata(&metadata)?;

        Ok(seg)
    }

    fn restore_pending_from_metadata(&mut self, metadata: &DataFileMetadata) -> Result<()> {
        if metadata.pending_block_offset == PENDING_NONE {
            self.clear_pending();
            return Ok(());
        }

        let pending_end = self
            .header_size
            .checked_add(metadata.pending_block_offset)
            .and_then(|v| v.checked_add(crate::block::BLOCK_HEADER_SIZE))
            .and_then(|v| v.checked_add(metadata.pending_wrote_position))
            .ok_or_else(|| TmslError::InvalidData("pending block position overflow".into()))?;
        if pending_end > self.file_size {
            return Err(TmslError::InvalidData(
                "pending block extends beyond segment file size".into(),
            ));
        }

        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| TmslError::MmapError("segment closed during pending restore".into()))?;
        let hdr_pos = (self.header_size + metadata.pending_block_offset) as usize;
        let header = BlockHeader::read_from(mmap, hdr_pos);
        if header.flags != 0 {
            return Err(TmslError::InvalidData(format!(
                "pending block has non-raw flags: {:#x}",
                header.flags
            )));
        }
        if header.payload_size as u64 != metadata.pending_wrote_position {
            return Err(TmslError::InvalidData(
                "pending block payload_size does not match pending_wrote_position".into(),
            ));
        }
        if header.record_count as u64 != metadata.pending_record_count {
            return Err(TmslError::InvalidData(
                "pending block record_count does not match header state".into(),
            ));
        }

        self.pending_block_offset = Some(metadata.pending_block_offset);
        self.pending_wrote_position = metadata.pending_wrote_position;
        self.pending_record_count = metadata.pending_record_count;
        Ok(())
    }
}

// ─── Lifecycle ───────────────────────────────────────────────────────────────

impl DataSegment {
    /// Ensure the segment is memory-mapped. If closed, re-open it.
    ///
    /// This is used for lazy-open after idle-close. Pending blocks are restored
    /// as pending raw state.
    pub fn ensure_open(&mut self, _compress_level: u8) -> Result<()> {
        if self.mmap.is_some() {
            return Ok(());
        }

        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let actual_file_size = file.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let metadata = DataFileMetadata::read_from(&mmap)?;

        self.file_size = actual_file_size;
        self.header_size = metadata.header_size;
        self.data_wrote_position = metadata.wrote_position.saturating_sub(self.header_size);
        self.record_count = metadata.record_count;
        self.total_uncompressed_size = metadata.total_uncompressed_size;
        self.min_timestamp = metadata.min_timestamp;
        self.max_timestamp = metadata.max_timestamp;
        self.invalid_record_count = metadata.invalid_record_count;

        self.mmap = Some(mmap);
        self.lifecycle = SegmentLifecycle::OpenReady;
        self.last_accessed_at = Instant::now();

        self.restore_pending_from_metadata(&metadata)?;

        Ok(())
    }

    /// Idle-close: sync to disk and unmap. Pending blocks remain pending.
    pub fn idle_close(&mut self, _compress_level: u8) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush()?;
        }
        self.mmap = None;
        self.lifecycle = SegmentLifecycle::Closed;
        Ok(())
    }

    /// Sync only (flush loop). Does NOT seal or compress pending blocks.
    pub fn sync(&mut self) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush()?;
        }
        self.last_accessed_at = Instant::now();
        Ok(())
    }

    /// Expand the segment file by 2x (up to max_file_size).
    ///
    /// Unmaps the file, resizes, remaps, and updates `self.file_size`.
    /// The header file_size is NOT updated (always records max_file_size).
    ///
    /// Returns `Ok(())` on success, or `Err` when already at max_file_size.
    pub fn expand(&mut self) -> Result<()> {
        let target = (self.file_size.saturating_mul(2)).min(self.max_file_size);
        if target == self.file_size {
            return Err(TmslError::InvalidData(format!(
                "segment already at max_file_size ({})",
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
        self.file_size = target;

        Ok(())
    }

    fn ensure_data_capacity(&mut self, required_wrote_position: u64) -> Result<()> {
        while self.file_size.saturating_sub(self.header_size) < required_wrote_position {
            self.expand().map_err(|_| TmslError::SegmentFull)?;
        }
        Ok(())
    }
}

impl Drop for DataSegment {
    fn drop(&mut self) {
        if let Some(ref mut m) = self.mmap {
            if let Err(e) = m.flush() {
                log::error!("[DataSegment drop] mmap flush failed: {}", e);
            }
        }
    }
}

// ─── Write Operations ────────────────────────────────────────────────────────

/// The size overhead of a single record in a block:
/// `data_len` (4 bytes) + `timestamp` (8 bytes) = 12 bytes.
pub(crate) const RECORD_HEADER_SIZE: usize = 12;
pub(crate) const RECORD_OVERHEAD: u64 = RECORD_HEADER_SIZE as u64;
pub(crate) const MAX_RECORD_DATA_SIZE: usize = 4 * 1024 * 1024;
pub(crate) const APPEND_MIGRATION_THRESHOLD: usize =
    (crate::block::BLOCK_MAX_SIZE as usize * 70) / 100;

fn checked_record_size(data_len: usize) -> Result<usize> {
    if data_len > MAX_RECORD_DATA_SIZE {
        return Err(TmslError::InvalidData(
            "record data_len exceeds 4MiB limit".into(),
        ));
    }
    let record_size = RECORD_HEADER_SIZE
        .checked_add(data_len)
        .ok_or_else(|| TmslError::InvalidData("record size overflow".into()))?;
    if record_size > u32::MAX as usize {
        return Err(TmslError::InvalidData(
            "record payload exceeds u32 block payload limit".into(),
        ));
    }
    Ok(record_size)
}

impl DataSegment {
    /// Append a record to this segment.
    ///
    /// Returns `(block_relative_offset, in_block_offset)` where:
    /// - `block_relative_offset` is the block's offset relative to the data area start
    /// - `in_block_offset` is the record's position within the block payload
    ///
    /// If the segment file does not have enough space, returns `Err(TmslError::SegmentFull)`.
    /// The caller (DataSegmentSet) should call `expand()` and retry, or seal+create new segment.
    pub fn append_record(
        &mut self,
        timestamp: i64,
        data: &[u8],
        compress_level: u8,
    ) -> Result<(u64, u16)> {
        let record_size = checked_record_size(data.len())?;
        let total_needed = crate::block::BLOCK_HEADER_SIZE + record_size as u64;

        // Case 1: single record exceeds the fixed block payload limit.
        if record_size > crate::block::BLOCK_MAX_SIZE as usize {
            // Seal any existing pending first
            if self.pending_block_offset.is_some() {
                self.seal_pending_block(compress_level)?;
                self.clear_pending();
            }
            return self.create_single_record_block(timestamp, data, compress_level);
        }

        // Case 2: There is a pending block
        if let Some(_pending_off) = self.pending_block_offset {
            let new_total = self.pending_wrote_position + record_size as u64;

            if new_total > crate::block::BLOCK_MAX_SIZE as u64 {
                // Pending block is full → seal + compress
                self.seal_pending_block(compress_level)?;
                self.clear_pending();
                if self.file_size.saturating_sub(self.header_size)
                    < self.data_wrote_position + total_needed
                {
                    return Err(TmslError::SegmentFull);
                }
                return self.create_pending_and_append(timestamp, data);
            }

            if self.file_size.saturating_sub(self.header_size)
                < self.data_wrote_position + record_size as u64
            {
                return Err(TmslError::SegmentFull);
            }

            // Append to pending (raw, no compression)
            let in_block_offset = u16::try_from(self.pending_wrote_position)
                .map_err(|_| TmslError::InvalidData("pending record offset exceeds u16".into()))?;
            self.write_raw_record_to_pending(timestamp, data)?;
            self.last_accessed_at = Instant::now();
            return Ok((self.pending_block_offset.unwrap(), in_block_offset));
        }

        // Case 3: No pending block → create new one
        if self.file_size.saturating_sub(self.header_size) < self.data_wrote_position + total_needed
        {
            return Err(TmslError::SegmentFull);
        }
        self.create_pending_and_append(timestamp, data)
    }

    /// Write a raw record into the current pending block.
    fn write_raw_record_to_pending(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        let base = (self.header_size
            + self.pending_block_offset.unwrap()
            + crate::block::BLOCK_HEADER_SIZE
            + self.pending_wrote_position) as usize;

        let mmap = self.mmap.as_mut().unwrap();
        let data_len = u32::try_from(data.len())
            .map_err(|_| TmslError::InvalidData("record data_len exceeds u32".into()))?;
        let record_size = RECORD_OVERHEAD + data.len() as u64;
        let new_pending_wrote_position = self.pending_wrote_position + record_size;
        let new_pending_record_count = self.pending_record_count + 1;

        // [data_len: u32][timestamp: i64][data]
        mmap[base..base + 4].copy_from_slice(&data_len.to_le_bytes());
        mmap[base + 4..base + 12].copy_from_slice(&timestamp.to_le_bytes());
        mmap[base + 12..base + 12 + data.len()].copy_from_slice(data);

        // Update block header: payload_size + record_count
        let hdr = (self.header_size + self.pending_block_offset.unwrap()) as usize;
        let new_size = u32::try_from(new_pending_wrote_position)
            .map_err(|_| TmslError::InvalidData("pending payload exceeds u32".into()))?;
        mmap[hdr..hdr + 4].copy_from_slice(&new_size.to_le_bytes());
        mmap[hdr + 6..hdr + 8].copy_from_slice(&(new_pending_record_count as u16).to_le_bytes());

        self.pending_wrote_position = new_pending_wrote_position;
        self.pending_record_count = new_pending_record_count;
        self.data_wrote_position += record_size;
        self.record_count += 1;
        self.total_uncompressed_size += record_size;

        // Update timestamp range
        if timestamp < self.min_timestamp {
            self.min_timestamp = timestamp;
        }
        if timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
        }

        // Update file header wrote_position and pending state
        self.update_file_header_for_pending(self.pending_block_offset.unwrap())?;

        Ok(())
    }

    /// Create a new pending block and write the first record.
    fn create_pending_and_append(&mut self, timestamp: i64, data: &[u8]) -> Result<(u64, u16)> {
        let block_pos = self.header_size + self.data_wrote_position;
        let rec_size = RECORD_OVERHEAD + data.len() as u64;

        // Write BlockHeader (flags=0, not sealed)
        let hdr = BlockHeader::new(rec_size as u32, 0, 1, rec_size as u32);
        let hdr_start = block_pos as usize;
        let mmap = self
            .mmap
            .as_mut()
            .ok_or_else(|| TmslError::MmapError("segment closed during write".into()))?;
        hdr.write_to(mmap, hdr_start);

        // Write record payload
        let data_pos = hdr_start + crate::block::BLOCK_HEADER_SIZE as usize;
        let data_len = u32::try_from(data.len())
            .map_err(|_| TmslError::InvalidData("record data_len exceeds u32".into()))?;
        mmap[data_pos..data_pos + 4].copy_from_slice(&data_len.to_le_bytes());
        mmap[data_pos + 4..data_pos + 12].copy_from_slice(&timestamp.to_le_bytes());
        mmap[data_pos + 12..data_pos + 12 + data.len()].copy_from_slice(data);

        self.pending_block_offset = Some(block_pos - self.header_size);
        self.pending_wrote_position = rec_size;
        self.pending_record_count = 1;
        self.data_wrote_position += crate::block::BLOCK_HEADER_SIZE + rec_size;
        self.record_count += 1;
        self.total_uncompressed_size += rec_size;
        self.last_accessed_at = Instant::now();

        // Update timestamp range
        if timestamp < self.min_timestamp {
            self.min_timestamp = timestamp;
        }
        if timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
        }

        // Write pending info to file header
        self.update_file_header_for_pending(block_pos - self.header_size)?;

        Ok((block_pos - self.header_size, 0))
    }

    /// Seal the pending block: compress and write back.
    pub(crate) fn seal_pending_block(&mut self, compress_level: u8) -> Result<()> {
        let block_rel_offset = self.pending_block_offset.unwrap();
        let hdr_pos = (self.header_size + block_rel_offset) as usize;
        let payload_start = hdr_pos + crate::block::BLOCK_HEADER_SIZE as usize;
        let payload_len = self.pending_wrote_position as usize;

        let raw = {
            let mmap = self.mmap.as_mut().unwrap();
            mmap[payload_start..payload_start + payload_len].to_vec()
        };

        let compressed = deflate_compress(&raw, compress_level)?;
        let compressed_len = u32::try_from(compressed.len())
            .map_err(|_| TmslError::InvalidData("compressed block exceeds u32".into()))?;
        let new_wrote_position =
            block_rel_offset + crate::block::BLOCK_HEADER_SIZE + compressed.len() as u64;
        self.ensure_data_capacity(new_wrote_position)?;

        let header = BlockHeader::new(
            compressed_len,
            BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED,
            self.pending_record_count as u16,
            self.pending_wrote_position as u32,
        );
        {
            let mmap = self.mmap.as_mut().unwrap();
            header.write_to(mmap, hdr_pos);
            mmap[payload_start..payload_start + compressed.len()].copy_from_slice(&compressed);
            clear_pending_from_mmap(mmap)?;
        }
        self.data_wrote_position = new_wrote_position;
        self.update_file_wrote_position()?;

        Ok(())
    }

    /// Create an exclusive block for a single record.
    ///
    /// Used for records larger than an aggregated block and for append migration.
    pub(crate) fn create_single_record_block(
        &mut self,
        timestamp: i64,
        data: &[u8],
        compress_level: u8,
    ) -> Result<(u64, u16)> {
        let rec_size = checked_record_size(data.len())?;
        let block_pos = self.header_size + self.data_wrote_position;

        // Build record payload: [data_len:4][ts:8][data]
        let mut raw = Vec::with_capacity(rec_size);
        raw.extend_from_slice(
            &u32::try_from(data.len())
                .map_err(|_| TmslError::InvalidData("record data_len exceeds u32".into()))?
                .to_le_bytes(),
        );
        raw.extend_from_slice(&timestamp.to_le_bytes());
        raw.extend_from_slice(data);

        let payload = deflate_compress(&raw, compress_level)?;
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| TmslError::InvalidData("compressed block exceeds u32".into()))?;
        let total_needed = crate::block::BLOCK_HEADER_SIZE + payload.len() as u64;
        if self.file_size.saturating_sub(self.header_size) < self.data_wrote_position + total_needed
        {
            return Err(TmslError::SegmentFull);
        }

        let hdr_pos = block_pos as usize;
        let mmap = self.mmap.as_mut().unwrap();
        let header = BlockHeader::new(
            payload_len,
            BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED | BLOCK_FLAG_SINGLE_RECORD,
            1,
            rec_size as u32,
        );
        header.write_to(mmap, hdr_pos);

        let data_pos = hdr_pos + crate::block::BLOCK_HEADER_SIZE as usize;
        mmap[data_pos..data_pos + payload.len()].copy_from_slice(&payload);

        self.data_wrote_position += crate::block::BLOCK_HEADER_SIZE + payload.len() as u64;
        self.record_count += 1;
        self.total_uncompressed_size += rec_size as u64;
        self.last_accessed_at = Instant::now();

        // Update timestamp range
        if timestamp < self.min_timestamp {
            self.min_timestamp = timestamp;
        }
        if timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
        }

        self.update_file_wrote_position()?;

        Ok((block_pos - self.header_size, 0))
    }

    fn validate_mutable_tail_record(
        &self,
        block_rel_offset: u64,
        in_block_offset: u16,
    ) -> Result<(usize, BlockHeader, usize, usize)> {
        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| TmslError::MmapError("segment mmap not open".into()))?;
        let block_abs_start = (self.header_size + block_rel_offset) as usize;
        if block_abs_start + crate::block::BLOCK_HEADER_SIZE as usize > mmap.len() {
            return Err(TmslError::InvalidData("block header out of bounds".into()));
        }
        let hdr = BlockHeader::read_from(mmap, block_abs_start);
        let block_abs_end =
            block_abs_start + crate::block::BLOCK_HEADER_SIZE as usize + hdr.payload_size as usize;
        let seg_wrote_end = (self.header_size + self.data_wrote_position) as usize;
        if block_abs_end != seg_wrote_end {
            return Err(TmslError::InvalidData(
                "append: target block is not the last in segment".into(),
            ));
        }
        if self.pending_block_offset != Some(block_rel_offset) {
            return Err(TmslError::InvalidData(
                "append: target block is not pending".into(),
            ));
        }
        if hdr.is_sealed() || hdr.is_compressed() {
            return Err(TmslError::InvalidData(
                "append: target block is sealed or compressed".into(),
            ));
        }
        if hdr.payload_size != hdr.uncompressed_size {
            return Err(TmslError::InvalidData(
                "append: pending raw payload_size != uncompressed_size".into(),
            ));
        }

        let record_pos =
            block_abs_start + crate::block::BLOCK_HEADER_SIZE as usize + in_block_offset as usize;
        if record_pos + RECORD_HEADER_SIZE > mmap.len() {
            return Err(TmslError::InvalidData("cannot read record header".into()));
        }
        let old_data_len = u32::from_le_bytes(
            mmap[record_pos..record_pos + 4]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read data_len".into()))?,
        ) as usize;
        let record_end_in_payload = in_block_offset as usize + RECORD_HEADER_SIZE + old_data_len;
        if record_end_in_payload != hdr.payload_size as usize {
            return Err(TmslError::InvalidData(
                "append: target record is not the last in block".into(),
            ));
        }
        Ok((block_abs_start, hdr, record_pos, old_data_len))
    }

    pub(crate) fn read_mutable_tail_record(
        &self,
        block_rel_offset: u64,
        in_block_offset: u16,
    ) -> Result<Vec<u8>> {
        let (_block_abs_start, _hdr, record_pos, old_data_len) =
            self.validate_mutable_tail_record(block_rel_offset, in_block_offset)?;
        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| TmslError::MmapError("segment mmap not open".into()))?;
        Ok(
            mmap[record_pos + RECORD_HEADER_SIZE..record_pos + RECORD_HEADER_SIZE + old_data_len]
                .to_vec(),
        )
    }

    pub(crate) fn append_to_last_record(
        &mut self,
        block_rel_offset: u64,
        in_block_offset: u16,
        append_data: &[u8],
    ) -> Result<u32> {
        let (block_abs_start, hdr, record_pos, old_data_len) =
            self.validate_mutable_tail_record(block_rel_offset, in_block_offset)?;
        let final_data_len = old_data_len
            .checked_add(append_data.len())
            .ok_or_else(|| TmslError::InvalidData("append data_len overflow".into()))?;
        let final_record_size = checked_record_size(final_data_len)?;
        if final_record_size > APPEND_MIGRATION_THRESHOLD {
            return Err(TmslError::InvalidData(
                "append: final record exceeds migration threshold".into(),
            ));
        }
        let delta = append_data.len();
        let required = self
            .header_size
            .checked_add(self.data_wrote_position)
            .and_then(|v| v.checked_add(delta as u64))
            .ok_or_else(|| TmslError::InvalidData("append wrote_position overflow".into()))?;
        if required > self.file_size {
            return Err(TmslError::SegmentFull);
        }

        let new_payload_size = hdr
            .payload_size
            .checked_add(delta as u32)
            .ok_or_else(|| TmslError::InvalidData("append block payload overflow".into()))?;
        let new_uncompressed_size = hdr
            .uncompressed_size
            .checked_add(delta as u32)
            .ok_or_else(|| TmslError::InvalidData("append block uncompressed overflow".into()))?;
        let new_data_len = u32::try_from(final_data_len)
            .map_err(|_| TmslError::InvalidData("record data_len exceeds u32".into()))?;

        {
            let mmap = self
                .mmap
                .as_mut()
                .ok_or_else(|| TmslError::MmapError("segment mmap not open".into()))?;
            mmap[record_pos..record_pos + 4].copy_from_slice(&new_data_len.to_le_bytes());
            let append_pos = record_pos + RECORD_HEADER_SIZE + old_data_len;
            mmap[append_pos..append_pos + append_data.len()].copy_from_slice(append_data);
            let new_hdr = BlockHeader::new(
                new_payload_size,
                hdr.flags,
                hdr.record_count,
                new_uncompressed_size,
            );
            new_hdr.write_to(mmap, block_abs_start);
        }

        self.data_wrote_position += delta as u64;
        self.total_uncompressed_size += delta as u64;
        self.pending_wrote_position += delta as u64;
        self.update_file_header_for_pending(block_rel_offset)?;
        self.last_accessed_at = Instant::now();
        u32::try_from(old_data_len)
            .map_err(|_| TmslError::InvalidData("old data_len exceeds u32".into()))
    }

    /// Overwrite the data bytes of the last record in the last pending raw block.
    ///
    /// Used for correction writes: modifies the existing record in place without
    /// creating a new record or index entry. Supports changing data length.
    ///
    /// Returns `Err` if:
    /// - the target block is not the last block in this segment
    /// - the target block is not pending raw (flags must be 0)
    /// - the target record is not the last record in the block
    pub fn overwrite_in_last_block(
        &mut self,
        block_rel_offset: u64,
        in_block_offset: u16,
        new_data: &[u8],
    ) -> Result<()> {
        let mmap = self
            .mmap
            .as_mut()
            .ok_or_else(|| TmslError::MmapError("segment mmap not open".into()))?;

        let block_abs_start = (self.header_size + block_rel_offset) as usize;

        // Read block header (16 bytes starting at block_abs_start)
        let hdr = BlockHeader::read_from(mmap, block_abs_start);

        // 1. Verify the target block is the last in this segment
        let block_abs_end =
            block_abs_start + crate::block::BLOCK_HEADER_SIZE as usize + hdr.payload_size as usize;
        let seg_wrote_end = (self.header_size + self.data_wrote_position) as usize;
        if block_abs_end != seg_wrote_end {
            return Err(TmslError::InvalidData(
                "correction write: target block is not the last in segment".into(),
            ));
        }

        // 2. Correction writes only target the current pending raw block.
        if self.pending_block_offset != Some(block_rel_offset) {
            return Err(TmslError::InvalidData(
                "correction write: target block is not pending".into(),
            ));
        }
        if hdr.is_sealed() || hdr.is_compressed() {
            return Err(TmslError::InvalidData(
                "correction write: target block is sealed or compressed, not supported".into(),
            ));
        }

        // 3. Read old record and verify it is the last record in the block
        let record_pos =
            block_abs_start + crate::block::BLOCK_HEADER_SIZE as usize + in_block_offset as usize;
        if record_pos + RECORD_HEADER_SIZE > mmap.len() {
            return Err(TmslError::InvalidData("cannot read record header".into()));
        }
        let old_data_len = u32::from_le_bytes(
            mmap[record_pos..record_pos + 4]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read data_len".into()))?,
        ) as usize;

        let old_record_bytes = RECORD_OVERHEAD as usize + old_data_len;
        let record_end_in_payload = in_block_offset as usize + old_record_bytes;
        if record_end_in_payload != hdr.payload_size as usize {
            return Err(TmslError::InvalidData(
                "correction write: target record is not the last in block".into(),
            ));
        }

        // 4. Compute delta
        let new_record_bytes = checked_record_size(new_data.len())?;
        let delta = new_record_bytes as i64 - old_record_bytes as i64;
        let new_payload_size_i64 = hdr.payload_size as i64 + delta;
        let new_uncomp_size_i64 = hdr.uncompressed_size as i64 + delta;
        if new_payload_size_i64 < 0 || new_uncomp_size_i64 < 0 {
            return Err(TmslError::InvalidData(
                "correction write: negative block size".into(),
            ));
        }
        if new_payload_size_i64 > u32::MAX as i64 || new_uncomp_size_i64 > u32::MAX as i64 {
            return Err(TmslError::InvalidData(
                "correction write: block size exceeds u32".into(),
            ));
        }
        let new_payload_size = new_payload_size_i64 as u32;
        let new_uncomp_size = new_uncomp_size_i64 as u32;
        if new_payload_size > crate::block::BLOCK_MAX_SIZE && !hdr.is_single_record() {
            return Err(TmslError::InvalidData(
                "correction write: oversized record requires exclusive block".into(),
            ));
        }
        if delta > 0 {
            let required = self.header_size + self.data_wrote_position + delta as u64;
            if required > self.file_size {
                return Err(TmslError::SegmentFull);
            }
        }

        // 5. Write new data_len (u32) and data bytes at the record position
        mmap[record_pos..record_pos + 4].copy_from_slice(
            &u32::try_from(new_data.len())
                .map_err(|_| TmslError::InvalidData("record data_len exceeds u32".into()))?
                .to_le_bytes(),
        );
        // timestamp at record_pos+4..record_pos+12 is preserved
        mmap[record_pos + 12..record_pos + 12 + new_data.len()].copy_from_slice(new_data);

        // 6. Update block header: payload_size + uncompressed_size
        let new_hdr = BlockHeader::new(
            new_payload_size,
            hdr.flags,
            hdr.record_count,
            new_uncomp_size,
        );
        new_hdr.write_to(mmap, block_abs_start);

        // 7. Update segment-level counters
        if delta >= 0 {
            let d = delta as u64;
            self.data_wrote_position += d;
            self.total_uncompressed_size += d;
        } else {
            let d = (-delta) as u64;
            self.data_wrote_position = self.data_wrote_position.saturating_sub(d);
            self.total_uncompressed_size = self.total_uncompressed_size.saturating_sub(d);
        }

        // 8. Update pending_wrote_position if this block is the pending block
        let is_pending = self.pending_block_offset == Some(block_rel_offset);
        if is_pending {
            if delta >= 0 {
                self.pending_wrote_position += delta as u64;
            } else {
                self.pending_wrote_position =
                    self.pending_wrote_position.saturating_sub((-delta) as u64);
            }
            self.update_file_header_for_pending(block_rel_offset)?;
        } else {
            self.update_file_wrote_position()?;
        }

        Ok(())
    }

    /// Clear the pending block state.
    pub(crate) fn clear_pending(&mut self) {
        self.pending_block_offset = None;
        self.pending_wrote_position = 0;
        self.pending_record_count = 0;
    }

    /// Increment the invalid_record_count in memory and persist to file header state.
    /// Used when out-of-order writes or deletes orphan a record in this segment.
    pub fn increment_invalid_record_count(&mut self) -> Result<()> {
        self.invalid_record_count += 1;
        if let Some(ref mut mmap) = self.mmap {
            write_data_invalid_record_count_to_mmap(mmap, self.invalid_record_count)?;
        }
        Ok(())
    }

    // ─── File header helpers ──────────────────────────────────────────────

    fn update_file_wrote_position(&mut self) -> Result<()> {
        let mmap = self.mmap.as_mut().unwrap();
        let abs_pos = self.header_size + self.data_wrote_position;
        write_data_core_state_to_mmap(
            mmap,
            self.min_timestamp,
            self.max_timestamp,
            abs_pos,
            self.record_count,
            self.total_uncompressed_size,
        )
    }

    fn update_file_header_for_pending(&mut self, block_rel_offset: u64) -> Result<()> {
        let mmap = self.mmap.as_mut().unwrap();
        write_data_pending_state_to_mmap(
            mmap,
            block_rel_offset,
            self.pending_wrote_position,
            self.pending_record_count,
        )?;

        // Also update wrote_position
        self.update_file_wrote_position()
    }
}

// ─── Read Operations ─────────────────────────────────────────────────────────

/// Index entry used to locate a record.
/// Mirrors the 18-byte IndexEntry from the index module.
#[derive(Clone, Copy)]
pub struct ReadIndexEntry {
    pub timestamp: i64,
    pub block_offset: u64,    // relative to the data area start
    pub in_block_offset: u16, // relative to block payload start
}

fn is_sealed_compressed_or_pending_raw(flags: u16) -> Result<bool> {
    let is_sealed = flags & BLOCK_FLAG_SEALED != 0;
    let is_compressed = flags & BLOCK_FLAG_COMPRESSED != 0;
    let is_single_record = flags & BLOCK_FLAG_SINGLE_RECORD != 0;
    if is_sealed != is_compressed {
        return Err(TmslError::InvalidData(format!(
            "invalid block flags: SEALED and COMPRESSED must be paired ({:#x})",
            flags
        )));
    }
    if is_single_record && (!is_sealed || !is_compressed) {
        return Err(TmslError::InvalidData(format!(
            "invalid block flags: SINGLE_RECORD requires SEALED|COMPRESSED ({:#x})",
            flags
        )));
    }
    Ok(is_sealed && is_compressed)
}

impl DataSegment {
    /// Read a record at the given index entry.
    ///
    /// Returns `(timestamp, data)`.
    pub fn read_at_index(
        &self,
        entry: &ReadIndexEntry,
        cache: Option<&BlockCache>,
    ) -> Result<(i64, Vec<u8>)> {
        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| TmslError::MmapError("segment is closed, cannot read".into()))?;

        let hdr_pos = (self.header_size + entry.block_offset) as usize;

        // Read block header
        let payload_size = read_u32_from_mmap(mmap, hdr_pos) as usize;
        let flags = read_u16_from_mmap(mmap, hdr_pos + 4);
        let is_compressed = is_sealed_compressed_or_pending_raw(flags)?;

        // ── Cache check ──
        let cache_key = CacheKey::new(self.file_offset, entry.block_offset);

        // Only compressed blocks are globally cached; raw blocks may still be mutable.
        if is_compressed {
            if let Some(cached) = cache.and_then(|c| c.get(&cache_key)) {
                let pos = entry.in_block_offset as usize;
                if pos + RECORD_HEADER_SIZE > cached.len() {
                    return Err(TmslError::InvalidData("record index out of bounds".into()));
                }
                let data_len = read_u32_le(
                    cached[pos..pos + 4]
                        .try_into()
                        .map_err(|_| TmslError::InvalidData("cannot read data_len".into()))?,
                ) as usize;
                let timestamp = read_i64_le(
                    cached[pos + 4..pos + 12]
                        .try_into()
                        .map_err(|_| TmslError::InvalidData("cannot read timestamp".into()))?,
                );
                if pos + RECORD_HEADER_SIZE + data_len > cached.len() {
                    return Err(TmslError::InvalidData("record data out of bounds".into()));
                }
                let data =
                    cached[pos + RECORD_HEADER_SIZE..pos + RECORD_HEADER_SIZE + data_len].to_vec();
                return Ok((timestamp, data));
            }
        }

        // Cache miss or raw block: read from mmap + decode.
        let pay_start = hdr_pos + crate::block::BLOCK_HEADER_SIZE as usize;
        let payload = &mmap[pay_start..pay_start + payload_size];

        let block_data: Vec<u8> = if is_compressed {
            deflate_decompress(payload)?
        } else {
            payload.to_vec()
        };

        if is_compressed {
            if let Some(c) = cache {
                c.put(cache_key, block_data.clone());
            }
        }

        // Locate record: entry.in_block_offset points to [data_len:4]
        let pos = entry.in_block_offset as usize;
        if pos + RECORD_HEADER_SIZE > block_data.len() {
            return Err(TmslError::InvalidData("record index out of bounds".into()));
        }

        let data_len = read_u32_le(
            block_data[pos..pos + 4]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read data_len".into()))?,
        ) as usize;
        let timestamp = read_i64_le(
            block_data[pos + 4..pos + 12]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read timestamp".into()))?,
        );

        if pos + RECORD_HEADER_SIZE + data_len > block_data.len() {
            return Err(TmslError::InvalidData("record data out of bounds".into()));
        }

        let data =
            block_data[pos + RECORD_HEADER_SIZE..pos + RECORD_HEADER_SIZE + data_len].to_vec();
        Ok((timestamp, data))
    }

    /// Read a record at the given index entry, with HotBlockCache support.
    pub fn read_at_index_with_hot_cache(
        &self,
        entry: &ReadIndexEntry,
        cache: Option<&BlockCache>,
        hot_block: &mut HotBlockCache,
    ) -> Result<(i64, Vec<u8>)> {
        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| TmslError::MmapError("segment is closed, cannot read".into()))?;

        let hdr_pos = (self.header_size + entry.block_offset) as usize;
        let cache_key = CacheKey::new(self.file_offset, entry.block_offset);

        if hot_block.is_hit(self.file_offset, entry.block_offset) {
            return hot_block.extract_record(entry.in_block_offset);
        }

        let payload_size = read_u32_from_mmap(mmap, hdr_pos) as usize;
        let flags = read_u16_from_mmap(mmap, hdr_pos + 4);
        let is_compressed = is_sealed_compressed_or_pending_raw(flags)?;

        if is_compressed {
            if let Some(block_data) = cache.and_then(|c| c.get(&cache_key)) {
                hot_block.fill(cache_key, block_data.clone());
                return hot_block.extract_record(entry.in_block_offset);
            }
        }

        let pay_start = hdr_pos + crate::block::BLOCK_HEADER_SIZE as usize;
        let payload = &mmap[pay_start..pay_start + payload_size];

        let block_data: Vec<u8> = if is_compressed {
            deflate_decompress(payload)?
        } else {
            payload.to_vec()
        };

        hot_block.fill(cache_key.clone(), block_data.clone());
        if is_compressed {
            if let Some(c) = cache {
                c.put(cache_key, block_data);
            }
        }

        let pos = entry.in_block_offset as usize;
        if pos + RECORD_HEADER_SIZE > hot_block.current_data.len() {
            return Err(TmslError::InvalidData("record index out of bounds".into()));
        }

        let timestamp = read_i64_le(
            hot_block.current_data[pos + 4..pos + 12]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read timestamp".into()))?,
        );
        let data_len = read_u32_le(
            hot_block.current_data[pos..pos + 4]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read data_len".into()))?,
        ) as usize;

        if pos + RECORD_HEADER_SIZE + data_len > hot_block.current_data.len() {
            return Err(TmslError::InvalidData("record data out of bounds".into()));
        }

        let data = hot_block.current_data
            [pos + RECORD_HEADER_SIZE..pos + RECORD_HEADER_SIZE + data_len]
            .to_vec();
        Ok((timestamp, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{DataFileMetadata, DATA_HEADER_SIZE};
    use std::fs;

    fn temp_dir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join("timslite_test_seg");
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn make_segment(name: &str) -> (DataSegment, std::path::PathBuf) {
        let dir = temp_dir();
        let path = dir.join(name);
        let _ = fs::remove_file(&path);
        let seg = DataSegment::create(&path, 0, 1024 * 1024, 1024 * 1024).unwrap();
        (seg, path)
    }

    fn rewrite_segment_with_extended_header(seg: &mut DataSegment) {
        let extra_meta = [0xEE, 4, 0, 1, 2, 3, 4];
        let old_header = DATA_HEADER_SIZE as usize;
        let new_header = old_header + extra_meta.len();
        let used = seg.data_wrote_position as usize;
        let mmap = seg.mmap.as_mut().unwrap();

        let data = mmap[old_header..old_header + used].to_vec();
        let old_state = mmap[44..old_header].to_vec();
        mmap[new_header..new_header + used].copy_from_slice(&data);

        let meta_length = 33u16 + extra_meta.len() as u16;
        mmap[7..9].copy_from_slice(&meta_length.to_le_bytes());
        mmap[42..42 + extra_meta.len()].copy_from_slice(&extra_meta);
        let state_length_offset = 9 + meta_length as usize;
        let state_start = state_length_offset + 2;
        mmap[state_length_offset..state_length_offset + 2].copy_from_slice(&72u16.to_le_bytes());
        mmap[state_start..state_start + old_state.len()].copy_from_slice(&old_state);
        mmap[state_start + 16..state_start + 24]
            .copy_from_slice(&((new_header + used) as u64).to_le_bytes());
        mmap.flush().unwrap();

        seg.header_size = new_header as u64;
    }

    fn block_flags(seg: &DataSegment, block_rel_offset: u64) -> u16 {
        let mmap = seg.mmap.as_ref().unwrap();
        let hdr_pos = (seg.header_size + block_rel_offset) as usize;
        read_u16_from_mmap(mmap, hdr_pos + 4)
    }

    #[test]
    fn test_create_and_append_single_record() {
        let (mut seg, _path) = make_segment("test_single_rec");
        let (block_off, in_block_off) = seg.append_record(1700000000, b"hello", 6).unwrap();
        assert_eq!(block_off, 0);
        assert_eq!(in_block_off, 0);
        assert_eq!(seg.pending_record_count, 1);
    }

    #[test]
    fn test_header_wrote_position_is_absolute_and_runtime_is_data_relative() {
        let (mut seg, path) = make_segment("test_wrote_position_coordinate");
        seg.append_record(1700000000, b"hello", 6).unwrap();

        let runtime_data_pos = seg.data_wrote_position;
        let header_size = seg.header_size;
        let metadata = DataFileMetadata::read_from(seg.mmap.as_ref().unwrap()).unwrap();

        assert_eq!(metadata.wrote_position, header_size + runtime_data_pos);

        drop(seg);
        let reopened = DataSegment::open(&path, 0, 1024 * 1024).unwrap();
        assert_eq!(reopened.header_size, header_size);
        assert_eq!(reopened.data_wrote_position, runtime_data_pos);
    }

    #[test]
    fn test_append_multiple_records_same_block() {
        let (mut seg, _path) = make_segment("test_multi_rec");
        seg.append_record(1000, b"aaa", 6).unwrap();
        let (off1, ib1) = seg.append_record(2000, b"bbb", 6).unwrap();
        assert_eq!(off1, 0); // same block
        assert!(ib1 > 0); // different position within block
        assert_eq!(seg.pending_record_count, 2);
    }

    #[test]
    fn test_overwrite_in_last_block_rejects_non_tail_record() {
        let (mut seg, _path) = make_segment("test_overwrite_non_tail");
        let (off0, ib0) = seg.append_record(1000, b"aaa", 6).unwrap();
        let (off1, ib1) = seg.append_record(2000, b"bbb", 6).unwrap();
        assert_eq!(off0, off1);
        assert!(ib1 > ib0);

        let err = seg
            .overwrite_in_last_block(off0, ib0, b"longer")
            .expect_err("correction resize must not move following record bytes");

        assert!(err.to_string().contains("not the last in block"));
    }

    #[test]
    fn test_block_overflow_triggers_seal() {
        let (mut seg, _path) = make_segment("test_overflow");
        let data1 = vec![0xAAu8; 65_520]; // record_size = 12 + 65520 = 65532
        let data2 = vec![0xBBu8; 4]; // adding this record crosses BLOCK_MAX_SIZE
        let (off0, _) = seg.append_record(1000, &data1, 6).unwrap();
        let (off1, ib1) = seg.append_record(2000, &data2, 6).unwrap();
        // Second record should be in a NEW block
        assert!(off1 > off0);
        assert_eq!(ib1, 0);
        assert_eq!(
            block_flags(&seg, off0),
            BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED
        );
    }

    #[test]
    fn test_large_record_exclusive_block() {
        let (mut seg, _path) = make_segment("test_large");
        let data = vec![0xABu8; 70_000];
        let (off, ib) = seg.append_record(5000, &data, 6).unwrap();
        assert_eq!(ib, 0);
        // Single record, in its own block (compressed because all 0xAB)
        let entry = ReadIndexEntry {
            timestamp: 5000,
            block_offset: off,
            in_block_offset: ib,
        };
        let (ts, recovered) = seg.read_at_index(&entry, None).unwrap();
        assert_eq!(ts, 5000);
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_large_record_above_u16_roundtrip() {
        let (mut seg, _path) = make_segment("test_large_above_u16");
        let data: Vec<u8> = (0..70_000).map(|i| (i % 251) as u8).collect();

        let (off, ib) = seg.append_record(6000, &data, 6).unwrap();
        assert_eq!(ib, 0);

        let entry = ReadIndexEntry {
            timestamp: 6000,
            block_offset: off,
            in_block_offset: ib,
        };
        let (ts, recovered) = seg.read_at_index(&entry, None).unwrap();
        assert_eq!(ts, 6000);
        assert_eq!(recovered.len(), data.len());
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_single_record_block_is_always_compressed() {
        let (mut seg, _path) = make_segment("test_single_record_always_compressed");
        let data: Vec<u8> = (0..70_000).map(|i| (i % 251) as u8).collect();
        let (off, ib) = seg.append_record(6001, &data, 6).unwrap();
        assert_eq!(ib, 0);
        assert_eq!(
            block_flags(&seg, off),
            BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED | BLOCK_FLAG_SINGLE_RECORD
        );

        let entry = ReadIndexEntry {
            timestamp: 6001,
            block_offset: off,
            in_block_offset: ib,
        };
        let (ts, recovered) = seg.read_at_index(&entry, None).unwrap();
        assert_eq!(ts, 6001);
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_single_record_capacity_uses_compressed_size() {
        let dir = temp_dir();
        let path = dir.join("test_single_record_compressed_capacity");
        let _ = fs::remove_file(&path);
        let file_size = DATA_HEADER_SIZE + 1024;
        let mut seg = DataSegment::create(&path, 0, file_size, file_size).unwrap();
        let data = vec![0u8; 70_000];

        let (off, ib) = seg.append_record(6002, &data, 6).unwrap();

        assert_eq!(ib, 0);
        assert!(seg.data_wrote_position <= file_size - DATA_HEADER_SIZE);
        assert_eq!(
            block_flags(&seg, off),
            BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED | BLOCK_FLAG_SINGLE_RECORD
        );
        let entry = ReadIndexEntry {
            timestamp: 6002,
            block_offset: off,
            in_block_offset: ib,
        };
        let (ts, recovered) = seg.read_at_index(&entry, None).unwrap();
        assert_eq!(ts, 6002);
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_read_write_roundtrip() {
        let (mut seg, _path) = make_segment("test_roundtrip");
        let test_data: Vec<u8> = (0..200).map(|i| (i * 7 + 13) as u8).collect();

        let (block_off, in_block_off) = seg.append_record(9999, &test_data, 6).unwrap();

        // Seal the block to make it readable
        seg.seal_pending_block(6).unwrap();
        seg.clear_pending();

        let entry = ReadIndexEntry {
            timestamp: 9999,
            block_offset: block_off,
            in_block_offset: in_block_off,
        };
        let (ts, data) = seg.read_at_index(&entry, None).unwrap();
        assert_eq!(ts, 9999);
        assert_eq!(data, test_data);
    }

    #[test]
    fn test_raw_block_read_does_not_enter_global_cache() {
        let (mut seg, _path) = make_segment("test_raw_no_global_cache");
        let (block_off, in_block_off) = seg.append_record(7000, b"raw", 6).unwrap();
        let cache = BlockCache::new(1024 * 1024);
        let entry = ReadIndexEntry {
            timestamp: 7000,
            block_offset: block_off,
            in_block_offset: in_block_off,
        };

        let (ts, data) = seg.read_at_index(&entry, Some(&cache)).unwrap();

        assert_eq!(ts, 7000);
        assert_eq!(data, b"raw");
        assert_eq!(cache.stats().entry_count, 0);
    }

    #[test]
    fn test_raw_block_hot_read_does_not_enter_global_cache() {
        let (mut seg, _path) = make_segment("test_raw_hot_no_global_cache");
        let (block_off, in_block_off) = seg.append_record(7001, b"raw-hot", 6).unwrap();
        let cache = BlockCache::new(1024 * 1024);
        let mut hot = HotBlockCache::new();
        let entry = ReadIndexEntry {
            timestamp: 7001,
            block_offset: block_off,
            in_block_offset: in_block_off,
        };

        let (ts, data) = seg
            .read_at_index_with_hot_cache(&entry, Some(&cache), &mut hot)
            .unwrap();

        assert_eq!(ts, 7001);
        assert_eq!(data, b"raw-hot");
        assert!(hot.is_hit(0, block_off));
        assert_eq!(cache.stats().entry_count, 0);
    }

    #[test]
    fn test_compressed_block_read_enters_global_cache() {
        let (mut seg, _path) = make_segment("test_compressed_global_cache");
        let data = vec![0u8; 70_000];
        let (block_off, in_block_off) = seg.append_record(7002, &data, 6).unwrap();
        let cache = BlockCache::new(1024 * 1024);
        let entry = ReadIndexEntry {
            timestamp: 7002,
            block_offset: block_off,
            in_block_offset: in_block_off,
        };

        let (ts, recovered) = seg.read_at_index(&entry, Some(&cache)).unwrap();

        assert_eq!(ts, 7002);
        assert_eq!(recovered, data);
        assert_eq!(cache.stats().entry_count, 1);
    }

    #[test]
    fn test_open_reads_data_after_extended_header() {
        let (mut seg, path) = make_segment("test_extended_header_data");
        let data = b"extended_header_record".to_vec();
        let (block_off, in_block_off) = seg.append_record(12345, &data, 6).unwrap();
        assert_eq!(block_off, 0);
        rewrite_segment_with_extended_header(&mut seg);
        drop(seg);

        let reopened = DataSegment::open(&path, 0, 1024 * 1024).unwrap();
        assert!(reopened.header_size > DATA_HEADER_SIZE);
        let entry = ReadIndexEntry {
            timestamp: 12345,
            block_offset: 0,
            in_block_offset: in_block_off,
        };
        let (ts, recovered) = reopened.read_at_index(&entry, None).unwrap();
        assert_eq!(ts, 12345);
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_idle_close_reopen_recovery() {
        let (mut seg, path) = make_segment("test_idle");
        let (block_off, in_block_off) = seg.append_record(7777, b"idle_test", 6).unwrap();
        assert!(seg.pending_block_offset.is_some());
        assert_eq!(block_flags(&seg, block_off), 0);

        // Idle-close preserves pending raw state.
        seg.idle_close(6).unwrap();
        assert!(seg.mmap.is_none());
        assert_eq!(seg.lifecycle, SegmentLifecycle::Closed);
        assert_eq!(seg.pending_block_offset, Some(block_off));

        // Reopen
        let seg2 = DataSegment::open(&path, 0, 1024 * 1024).unwrap();
        assert!(seg2.mmap.is_some());
        assert_eq!(seg2.lifecycle, SegmentLifecycle::OpenReady);
        assert_eq!(seg2.pending_block_offset, Some(block_off));
        assert_eq!(block_flags(&seg2, block_off), 0);
        let entry = ReadIndexEntry {
            timestamp: 7777,
            block_offset: block_off,
            in_block_offset: in_block_off,
        };
        let (ts, data) = seg2.read_at_index(&entry, None).unwrap();
        assert_eq!(ts, 7777);
        assert_eq!(data, b"idle_test");
    }

    #[test]
    fn test_reopen_preserves_multi_record_pending_state() {
        let (mut seg, path) = make_segment("test_multi_pending_reopen");
        let (block_off, first_in_block) = seg.append_record(8000, b"first", 6).unwrap();
        let (same_block, second_in_block) = seg.append_record(8001, b"second", 6).unwrap();
        assert_eq!(same_block, block_off);
        assert!(second_in_block > first_in_block);

        seg.idle_close(6).unwrap();
        let reopened = DataSegment::open(&path, 0, 1024 * 1024).unwrap();
        assert_eq!(reopened.pending_block_offset, Some(block_off));
        assert_eq!(reopened.pending_record_count, 2);

        let second = ReadIndexEntry {
            timestamp: 8001,
            block_offset: block_off,
            in_block_offset: second_in_block,
        };
        let (ts, data) = reopened.read_at_index(&second, None).unwrap();
        assert_eq!(ts, 8001);
        assert_eq!(data, b"second");
    }

    #[test]
    fn test_sync_does_not_seal() {
        let (mut seg, _path) = make_segment("test_sync");
        seg.append_record(3333, b"sync_test", 6).unwrap();
        assert!(seg.pending_block_offset.is_some());

        seg.sync().unwrap();
        // Pending should still be there
        assert!(seg.pending_block_offset.is_some());
    }

    #[test]
    fn test_ensure_open_after_close() {
        let (mut seg, _path) = make_segment("test_ensure");
        let (block_off, _) = seg.append_record(4444, b"ensure", 6).unwrap();
        seg.idle_close(6).unwrap();
        assert!(seg.mmap.is_none());

        seg.ensure_open(6).unwrap();
        assert!(seg.mmap.is_some());
        assert_eq!(seg.lifecycle, SegmentLifecycle::OpenReady);
        assert_eq!(seg.pending_block_offset, Some(block_off));
        assert_eq!(block_flags(&seg, block_off), 0);
    }

    #[test]
    fn test_multiple_records_read_all() {
        let dir = temp_dir();
        let path = dir.join("test_multi_read");
        let _ = fs::remove_file(&path);
        let mut seg = DataSegment::create(&path, 0, 1024 * 1024, 1024 * 1024).unwrap();

        let mut entries: Vec<ReadIndexEntry> = Vec::new();
        for i in 0..100 {
            let ts = 1_700_000_000 + i;
            let data = format!("record_{i}", i = i).into_bytes();
            let (block_off, in_block_off) = seg.append_record(ts, &data, 6).unwrap();
            entries.push(ReadIndexEntry {
                timestamp: ts,
                block_offset: block_off,
                in_block_offset: in_block_off,
            });
        }

        // Seal the last pending block
        seg.seal_pending_block(6).unwrap();
        seg.clear_pending();

        // Verify all records
        for (i, entry) in entries.iter().enumerate() {
            let (ts, data) = seg.read_at_index(entry, None).unwrap();
            assert_eq!(ts, 1_700_000_000 + i as i64);
            assert_eq!(data, format!("record_{i}", i = i).as_bytes());
        }
    }
}
