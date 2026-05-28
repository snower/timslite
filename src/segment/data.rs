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
    clear_pending_from_mmap, DataFileMetadata, DATA_HEADER_SIZE, PENDING_NONE,
    TIMESTAMP_MAX_SENTINEL, TIMESTAMP_MIN_SENTINEL,
};
use crate::util::{read_i64_le, read_u16_from_mmap, read_u16_le, read_u32_from_mmap};

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
/// Multiple records are aggregated into blocks. When a block reach the size
/// limit (or the segment is idle-closed), it is sealed and optionally compressed.
pub struct DataSegment {
    pub path: std::path::PathBuf,
    pub file_offset: u64,
    /// Runtime actual size (grows with expansion). Header file_size always = max_file_size.
    pub file_size: u64,
    /// Expansion upper limit (segment_size, immutable).
    pub max_file_size: u64,
    pub wrote_position: u64,
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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(initial_size)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let metadata = DataFileMetadata::create_default(file_offset as i64, max_file_size as u32);
        metadata.write_to(&mut mmap);
        metadata.sync(&mut mmap)?;

        let now = Instant::now();
        Ok(Self {
            path: path.to_path_buf(),
            file_offset,
            file_size: initial_size,
            max_file_size,
            wrote_position: 0,
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
        })
    }

    /// Open an existing segment file, memory-map it, and recover state.
    ///
    /// `max_file_size` is passed in because it is a configuration parameter (segment_size)
    /// not stored in the file. The actual file size is read from disk metadata.
    ///
    /// If the file has a pending block (pending_block_offset != u64::MAX), it is sealed
    /// (without compression) during recovery to ensure consistency.
    pub fn open(path: &Path, file_offset: u64, max_file_size: u64) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let actual_file_size = file.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let metadata = DataFileMetadata::read_from(&mmap)?;

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
            wrote_position: metadata.wrote_position.saturating_sub(DATA_HEADER_SIZE),
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
        };

        // Pending recovery
        if metadata.pending_block_offset != PENDING_NONE {
            seg.pending_block_offset = Some(metadata.pending_block_offset);
            seg.pending_wrote_position = metadata.pending_wrote_position;
            seg.pending_record_count = metadata.pending_record_count;
            seg.recover_pending_seal()?;
        }

        Ok(seg)
    }

    /// Seal a recovered pending block (no compression).
    fn recover_pending_seal(&mut self) -> Result<()> {
        let block_rel_offset = self.pending_block_offset.unwrap();
        let hdr_pos = (DATA_HEADER_SIZE + block_rel_offset) as usize;
        let mmap = self.mmap.as_mut().unwrap();

        let payload_size = read_u32_from_mmap(mmap, hdr_pos);
        let record_count = self.pending_record_count as u16;
        let uncomp_size = self.pending_wrote_position as u32;

        let header = BlockHeader::new(payload_size, BLOCK_FLAG_SEALED, record_count, uncomp_size);
        header.write_to(mmap, hdr_pos);

        // Update file header
        clear_pending_from_mmap(self.mmap.as_mut().unwrap());

        self.pending_block_offset = None;
        self.pending_wrote_position = 0;
        self.pending_record_count = 0;
        Ok(())
    }
}

// ─── Lifecycle ───────────────────────────────────────────────────────────────

impl DataSegment {
    /// Ensure the segment is memory-mapped. If closed, re-open it.
    ///
    /// This is used for lazy-open after idle-close. Any pending block from a
    /// previous idle-close is sealed during recovery.
    pub fn ensure_open(&mut self, _compress_level: u8) -> Result<()> {
        if self.mmap.is_some() {
            return Ok(());
        }

        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let metadata = DataFileMetadata::read_from(&mmap)?;

        self.wrote_position = metadata.wrote_position.saturating_sub(DATA_HEADER_SIZE);
        self.record_count = metadata.record_count;
        self.total_uncompressed_size = metadata.total_uncompressed_size;
        self.min_timestamp = metadata.min_timestamp;
        self.max_timestamp = metadata.max_timestamp;

        // Pending recovery (same as open)
        if metadata.pending_block_offset != PENDING_NONE {
            self.pending_block_offset = Some(metadata.pending_block_offset);
            self.pending_wrote_position = metadata.pending_wrote_position;
            self.pending_record_count = metadata.pending_record_count;
        }

        self.mmap = Some(mmap);
        self.lifecycle = SegmentLifecycle::OpenReady;
        self.last_accessed_at = Instant::now();

        // If we recovered a pending block, seal it immediately
        if self.pending_block_offset.is_some() {
            self.recover_pending_seal()?;
        }

        Ok(())
    }

    /// Idle-close: sync to disk, seal any pending block (NO compression), unmap.
    pub fn idle_close(&mut self, _compress_level: u8) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush()?;
        }

        // Seal pending block without compression
        if self.pending_block_offset.is_some() {
            let block_rel_offset = self.pending_block_offset.unwrap();
            let hdr_pos = (DATA_HEADER_SIZE + block_rel_offset) as usize;
            let mmap = self.mmap.as_mut().unwrap();

            let payload_size = read_u32_from_mmap(mmap, hdr_pos);
            let record_count = self.pending_record_count as u16;
            let uncomp_size = self.pending_wrote_position as u32;

            let header =
                BlockHeader::new(payload_size, BLOCK_FLAG_SEALED, record_count, uncomp_size);
            header.write_to(mmap, hdr_pos);

            // Clear pending in file header
            clear_pending_from_mmap(mmap);
            self.pending_block_offset = None;
            self.pending_wrote_position = 0;
            self.pending_record_count = 0;
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
}

// ─── Write Operations ────────────────────────────────────────────────────────

/// The size overhead of a single record in a block:
/// `data_len` (2 bytes) + `timestamp` (8 bytes) = 10 bytes.
const RECORD_OVERHEAD: u64 = 10;

impl DataSegment {
    /// Append a record to this segment.
    ///
    /// Returns `(block_relative_offset, in_block_offset)` where:
    /// - `block_relative_offset` is the block's offset relative to DATA_HEADER_SIZE
    /// - `in_block_offset` is the record's position within the block payload
    ///
    /// If the segment file does not have enough space, returns `Err(TmslError::SegmentFull)`.
    /// The caller (DataSegmentSet) should call `expand()` and retry, or seal+create new segment.
    pub fn append_record(
        &mut self,
        timestamp: i64,
        data: &[u8],
        block_max_size: u32,
        compress_level: u8,
    ) -> Result<(u64, u16)> {
        let record_size = RECORD_OVERHEAD as usize + data.len();
        let total_needed = crate::block::BLOCK_HEADER_SIZE + record_size as u64;

        // Space check: can the current file accommodate at least one more block + record?
        // The current wrote_position already accounts for existing data.
        // If we need to create a new block OR the record needs an exclusive block:
        if self.file_size.saturating_sub(DATA_HEADER_SIZE) < self.wrote_position + total_needed {
            return Err(TmslError::SegmentFull);
        }

        // Case 1: Single record exceeds block_max_size → exclusive block
        if record_size > block_max_size as usize {
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

            if new_total > block_max_size as u64 {
                // Pending block is full → seal + compress
                self.seal_pending_block(compress_level)?;
                self.clear_pending();
                return self.create_pending_and_append(timestamp, data);
            }

            // Append to pending (raw, no compression)
            let in_block_offset = self.pending_wrote_position as u16;
            self.write_raw_record_to_pending(timestamp, data)?;
            self.pending_wrote_position = new_total;
            self.pending_record_count += 1;
            self.last_accessed_at = Instant::now();
            return Ok((self.pending_block_offset.unwrap(), in_block_offset));
        }

        // Case 3: No pending block → create new one
        self.create_pending_and_append(timestamp, data)
    }

    /// Write a raw record into the current pending block.
    fn write_raw_record_to_pending(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        let base = (DATA_HEADER_SIZE
            + self.pending_block_offset.unwrap()
            + crate::block::BLOCK_HEADER_SIZE
            + self.pending_wrote_position) as usize;

        let mmap = self.mmap.as_mut().unwrap();
        let data_len = data.len() as u16;

        // [data_len: u16][timestamp: i64][data]
        mmap[base..base + 2].copy_from_slice(&data_len.to_le_bytes());
        mmap[base + 2..base + 10].copy_from_slice(&timestamp.to_le_bytes());
        mmap[base + 10..base + 10 + data.len()].copy_from_slice(data);

        // Update block header: payload_size + record_count
        let hdr = (DATA_HEADER_SIZE + self.pending_block_offset.unwrap()) as usize;
        let new_size =
            self.pending_wrote_position as u32 + RECORD_OVERHEAD as u32 + data.len() as u32;
        mmap[hdr..hdr + 4].copy_from_slice(&new_size.to_le_bytes());
        mmap[hdr + 6..hdr + 8].copy_from_slice(&(self.pending_record_count as u16).to_le_bytes());

        self.wrote_position += RECORD_OVERHEAD + data.len() as u64;

        // Update timestamp range
        if timestamp < self.min_timestamp {
            self.min_timestamp = timestamp;
        }
        if timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
        }

        // Update file header wrote_position
        self.update_file_wrote_position()?;

        Ok(())
    }

    /// Create a new pending block and write the first record.
    fn create_pending_and_append(&mut self, timestamp: i64, data: &[u8]) -> Result<(u64, u16)> {
        let block_pos = DATA_HEADER_SIZE + self.wrote_position;
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
        let data_len = data.len() as u16;
        mmap[data_pos..data_pos + 2].copy_from_slice(&data_len.to_le_bytes());
        mmap[data_pos + 2..data_pos + 10].copy_from_slice(&timestamp.to_le_bytes());
        mmap[data_pos + 10..data_pos + 10 + data.len()].copy_from_slice(data);

        self.pending_block_offset = Some(block_pos - DATA_HEADER_SIZE);
        self.pending_wrote_position = rec_size;
        self.pending_record_count = 1;
        self.wrote_position += crate::block::BLOCK_HEADER_SIZE + rec_size;
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
        self.update_file_header_for_pending(block_pos - DATA_HEADER_SIZE)?;

        Ok((block_pos - DATA_HEADER_SIZE, 0))
    }

    /// Seal the pending block: compress and write back.
    fn seal_pending_block(&mut self, compress_level: u8) -> Result<()> {
        let block_rel_offset = self.pending_block_offset.unwrap();
        let hdr_pos = (DATA_HEADER_SIZE + block_rel_offset) as usize;
        let payload_start = hdr_pos + crate::block::BLOCK_HEADER_SIZE as usize;
        let payload_len = self.pending_wrote_position as usize;

        let mmap = self.mmap.as_mut().unwrap();

        // Read raw payload
        let raw = mmap[payload_start..payload_start + payload_len].to_vec();

        // Compress
        let compressed = deflate_compress(&raw, compress_level)?;

        if compressed.len() < payload_len {
            // Compression effective: write header + compressed data
            let header = BlockHeader::new(
                compressed.len() as u32,
                BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED,
                self.pending_record_count as u16,
                self.pending_wrote_position as u32,
            );
            header.write_to(mmap, hdr_pos);
            mmap[payload_start..payload_start + compressed.len()].copy_from_slice(&compressed);
        } else {
            // Compression not effective: keep raw, set SEALED only
            let header = BlockHeader::new(
                payload_len as u32,
                BLOCK_FLAG_SEALED,
                self.pending_record_count as u16,
                self.pending_wrote_position as u32,
            );
            header.write_to(mmap, hdr_pos);
        }

        // Clear pending in file header
        clear_pending_from_mmap(mmap);

        Ok(())
    }

    /// Create an exclusive block for a single record > 64KB.
    fn create_single_record_block(
        &mut self,
        timestamp: i64,
        data: &[u8],
        compress_level: u8,
    ) -> Result<(u64, u16)> {
        let rec_size = RECORD_OVERHEAD as usize + data.len();
        let block_pos = DATA_HEADER_SIZE + self.wrote_position;

        // Build record payload: [data_len:2][ts:8][data]
        let mut raw = Vec::with_capacity(rec_size);
        raw.extend_from_slice(&(data.len() as u16).to_le_bytes());
        raw.extend_from_slice(&timestamp.to_le_bytes());
        raw.extend_from_slice(data);

        let compressed = deflate_compress(&raw, compress_level)?;
        let (payload, flags) = if compressed.len() < rec_size {
            (
                compressed,
                BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED | BLOCK_FLAG_SINGLE_RECORD,
            )
        } else {
            (raw, BLOCK_FLAG_SEALED | BLOCK_FLAG_SINGLE_RECORD)
        };

        let hdr_pos = block_pos as usize;
        let mmap = self.mmap.as_mut().unwrap();
        let header = BlockHeader::new(payload.len() as u32, flags, 1, rec_size as u32);
        header.write_to(mmap, hdr_pos);

        let data_pos = hdr_pos + crate::block::BLOCK_HEADER_SIZE as usize;
        mmap[data_pos..data_pos + payload.len()].copy_from_slice(&payload);

        self.wrote_position += crate::block::BLOCK_HEADER_SIZE + payload.len() as u64;
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

        Ok((block_pos - DATA_HEADER_SIZE, 0))
    }

    /// Overwrite the data bytes of the last record in the last (uncompressed) block.
    ///
    /// Used for correction writes: modifies the existing record in place without
    /// creating a new record or index entry. Supports changing data length.
    ///
    /// Returns `Err` if:
    /// - the target block is not the last block in this segment
    /// - the target block is compressed (COMPRESSED flag set)
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

        let block_abs_start = (DATA_HEADER_SIZE + block_rel_offset) as usize;

        // Read block header (16 bytes starting at block_abs_start)
        let hdr = BlockHeader::read_from(mmap, block_abs_start);

        // 1. Verify the target block is the last in this segment
        let block_abs_end =
            block_abs_start + crate::block::BLOCK_HEADER_SIZE as usize + hdr.payload_size as usize;
        let seg_wrote_end = (DATA_HEADER_SIZE + self.wrote_position) as usize;
        if block_abs_end != seg_wrote_end {
            return Err(TmslError::InvalidData(
                "correction write: target block is not the last in segment".into(),
            ));
        }

        // 2. Reject compressed blocks
        if hdr.is_compressed() {
            return Err(TmslError::InvalidData(
                "correction write: target block is compressed, not supported".into(),
            ));
        }

        // 3. Read old record and verify it is the last record in the block
        let record_pos =
            block_abs_start + crate::block::BLOCK_HEADER_SIZE as usize + in_block_offset as usize;
        let old_data_len = u16::from_le_bytes(
            mmap[record_pos..record_pos + 2]
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
        let new_record_bytes = RECORD_OVERHEAD as usize + new_data.len();
        let delta = new_record_bytes as i64 - old_record_bytes as i64;

        // 5. Write new data_len (u16) and data bytes at the record position
        mmap[record_pos..record_pos + 2].copy_from_slice(&(new_data.len() as u16).to_le_bytes());
        // timestamp at record_pos+2..record_pos+10 is preserved
        mmap[record_pos + 10..record_pos + 10 + new_data.len()].copy_from_slice(new_data);

        // 6. Update block header: payload_size + uncompressed_size
        let new_payload_size = (hdr.payload_size as i64 + delta) as u32;
        let new_uncomp_size = (hdr.uncompressed_size as i64 + delta) as u32;
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
            self.wrote_position += d;
            self.total_uncompressed_size += d;
        } else {
            let d = (-delta) as u64;
            self.wrote_position = self.wrote_position.saturating_sub(d);
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
    fn clear_pending(&mut self) {
        self.pending_block_offset = None;
        self.pending_wrote_position = 0;
        self.pending_record_count = 0;
    }

    // ─── File header helpers ──────────────────────────────────────────────

    fn update_file_wrote_position(&mut self) -> Result<()> {
        let mmap = self.mmap.as_mut().unwrap();
        let abs_pos = DATA_HEADER_SIZE + self.wrote_position;
        mmap[44..52].copy_from_slice(&self.min_timestamp.to_le_bytes());
        mmap[52..60].copy_from_slice(&self.max_timestamp.to_le_bytes());
        mmap[60..68].copy_from_slice(&abs_pos.to_le_bytes());
        mmap[68..76].copy_from_slice(&self.record_count.to_le_bytes());
        mmap[76..84].copy_from_slice(&self.total_uncompressed_size.to_le_bytes());
        Ok(())
    }

    fn update_file_header_for_pending(&mut self, block_rel_offset: u64) -> Result<()> {
        let mmap = self.mmap.as_mut().unwrap();
        mmap[84..92].copy_from_slice(&block_rel_offset.to_le_bytes());
        mmap[92..100].copy_from_slice(&self.pending_wrote_position.to_le_bytes());
        mmap[100..108].copy_from_slice(&self.pending_record_count.to_le_bytes());

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
    pub block_offset: u64,    // relative to DATA_HEADER_SIZE
    pub in_block_offset: u16, // relative to block payload start
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

        let hdr_pos = (DATA_HEADER_SIZE + entry.block_offset) as usize;

        // Read block header
        let payload_size = read_u32_from_mmap(mmap, hdr_pos) as usize;
        let flags = read_u16_from_mmap(mmap, hdr_pos + 4);
        let is_compressed = flags & BLOCK_FLAG_COMPRESSED != 0;

        // ── Cache check ──
        let cache_key = CacheKey::new(self.file_offset, entry.block_offset);

        // Try cache hit first: extract record directly from cached data
        if let Some(cached) = cache.and_then(|c| c.get(&cache_key)) {
            let pos = entry.in_block_offset as usize;
            if pos + 10 > cached.len() {
                return Err(TmslError::InvalidData("record index out of bounds".into()));
            }
            let data_len = read_u16_le(
                cached[pos..pos + 2]
                    .try_into()
                    .map_err(|_| TmslError::InvalidData("cannot read data_len".into()))?,
            ) as usize;
            let timestamp = read_i64_le(
                cached[pos + 2..pos + 10]
                    .try_into()
                    .map_err(|_| TmslError::InvalidData("cannot read timestamp".into()))?,
            );
            if pos + 10 + data_len > cached.len() {
                return Err(TmslError::InvalidData("record data out of bounds".into()));
            }
            let data = cached[pos + 10..pos + 10 + data_len].to_vec();
            return Ok((timestamp, data));
        }

        // Cache miss: read from mmap + decompress
        let pay_start = hdr_pos + crate::block::BLOCK_HEADER_SIZE as usize;
        let payload = &mmap[pay_start..pay_start + payload_size];

        let block_data: Vec<u8> = if is_compressed {
            deflate_decompress(payload)?
        } else {
            payload.to_vec()
        };

        // Cache the decompressed block for future reads
        if let Some(c) = cache {
            c.put(cache_key, block_data.clone());
        }

        // Locate record: entry.in_block_offset points to [data_len:2]
        let pos = entry.in_block_offset as usize;
        if pos + 10 > block_data.len() {
            return Err(TmslError::InvalidData("record index out of bounds".into()));
        }

        let data_len = read_u16_le(
            block_data[pos..pos + 2]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read data_len".into()))?,
        ) as usize;
        let timestamp = read_i64_le(
            block_data[pos + 2..pos + 10]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read timestamp".into()))?,
        );

        if pos + 10 + data_len > block_data.len() {
            return Err(TmslError::InvalidData("record data out of bounds".into()));
        }

        let data = block_data[pos + 10..pos + 10 + data_len].to_vec();
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

        let hdr_pos = (DATA_HEADER_SIZE + entry.block_offset) as usize;
        let cache_key = CacheKey::new(self.file_offset, entry.block_offset);

        if hot_block.is_hit(self.file_offset, entry.block_offset) {
            return hot_block.extract_record(entry.in_block_offset);
        }

        if let Some(block_data) = cache.and_then(|c| c.get(&cache_key)) {
            hot_block.fill(cache_key, block_data.clone());
            return hot_block.extract_record(entry.in_block_offset);
        }

        let payload_size = read_u32_from_mmap(mmap, hdr_pos) as usize;
        let flags = read_u16_from_mmap(mmap, hdr_pos + 4);
        let is_compressed = flags & crate::block::BLOCK_FLAG_COMPRESSED != 0;

        let pay_start = hdr_pos + crate::block::BLOCK_HEADER_SIZE as usize;
        let payload = &mmap[pay_start..pay_start + payload_size];

        let block_data: Vec<u8> = if is_compressed {
            deflate_decompress(payload)?
        } else {
            payload.to_vec()
        };

        hot_block.fill(cache_key.clone(), block_data.clone());
        if let Some(c) = cache {
            c.put(cache_key, block_data);
        }

        let pos = entry.in_block_offset as usize;
        if pos + 10 > hot_block.current_data.len() {
            return Err(TmslError::InvalidData("record index out of bounds".into()));
        }

        let timestamp = read_i64_le(
            hot_block.current_data[pos + 2..pos + 10]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read timestamp".into()))?,
        );
        let data_len = read_u16_le(
            hot_block.current_data[pos..pos + 2]
                .try_into()
                .map_err(|_| TmslError::InvalidData("cannot read data_len".into()))?,
        ) as usize;

        if pos + 10 + data_len > hot_block.current_data.len() {
            return Err(TmslError::InvalidData("record data out of bounds".into()));
        }

        let data = hot_block.current_data[pos + 10..pos + 10 + data_len].to_vec();
        Ok((timestamp, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn test_create_and_append_single_record() {
        let (mut seg, _path) = make_segment("test_single_rec");
        let (block_off, in_block_off) = seg.append_record(1700000000, b"hello", 65536, 6).unwrap();
        assert_eq!(block_off, 0);
        assert_eq!(in_block_off, 0);
        assert_eq!(seg.pending_record_count, 1);
    }

    #[test]
    fn test_append_multiple_records_same_block() {
        let (mut seg, _path) = make_segment("test_multi_rec");
        seg.append_record(1000, b"aaa", 65536, 6).unwrap();
        let (off1, ib1) = seg.append_record(2000, b"bbb", 65536, 6).unwrap();
        assert_eq!(off1, 0); // same block
        assert!(ib1 > 0); // different position within block
        assert_eq!(seg.pending_record_count, 2);
    }

    #[test]
    fn test_block_overflow_triggers_seal() {
        let (mut seg, _path) = make_segment("test_overflow");
        // With block_max_size=40, first record (10+20=30) fits,
        // second record (10+12=22) would overflow 30+22=52>40
        let max = 40u32;
        let data1 = vec![0xAAu8; 20]; // record_size = 10+20 = 30
        let data2 = vec![0xBBu8; 12]; // record_size = 10+12 = 22, total would be 52>40
        let (off0, _) = seg.append_record(1000, &data1, max, 6).unwrap();
        let (off1, ib1) = seg.append_record(2000, &data2, max, 6).unwrap();
        // Second record should be in a NEW block
        assert!(off1 > off0);
        assert_eq!(ib1, 0);
    }

    #[test]
    fn test_large_record_exclusive_block() {
        let (mut seg, _path) = make_segment("test_large");
        // Use 60KB data - within u16::MAX but the record overhead (10 bytes)
        // still makes it exceed the default 64KB block_max_size
        let data = vec![0xABu8; 60_000];
        // block_max_size = 65536, record_size = 10 + 60000 = 60010 < 65536
        // This won't trigger single-record path! Let's use smaller max:
        let (off, ib) = seg.append_record(5000, &data, 50_000, 6).unwrap();
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
    fn test_read_write_roundtrip() {
        let (mut seg, _path) = make_segment("test_roundtrip");
        let test_data: Vec<u8> = (0..200).map(|i| (i * 7 + 13) as u8).collect();

        let (block_off, in_block_off) = seg.append_record(9999, &test_data, 65536, 6).unwrap();

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
    fn test_idle_close_reopen_recovery() {
        let (mut seg, path) = make_segment("test_idle");
        seg.append_record(7777, b"idle_test", 65536, 6).unwrap();
        assert!(seg.pending_block_offset.is_some());

        // Idle-close: should seal pending (without compression)
        seg.idle_close(6).unwrap();
        assert!(seg.mmap.is_none());
        assert_eq!(seg.lifecycle, SegmentLifecycle::Closed);

        // Reopen
        let seg2 = DataSegment::open(&path, 0, 1024 * 1024).unwrap();
        assert!(seg2.mmap.is_some());
        assert_eq!(seg2.lifecycle, SegmentLifecycle::OpenReady);
        // Pending should be cleared by recovery
        assert!(seg2.pending_block_offset.is_none());
    }

    #[test]
    fn test_sync_does_not_seal() {
        let (mut seg, _path) = make_segment("test_sync");
        seg.append_record(3333, b"sync_test", 65536, 6).unwrap();
        assert!(seg.pending_block_offset.is_some());

        seg.sync().unwrap();
        // Pending should still be there
        assert!(seg.pending_block_offset.is_some());
    }

    #[test]
    fn test_ensure_open_after_close() {
        let (mut seg, _path) = make_segment("test_ensure");
        seg.append_record(4444, b"ensure", 65536, 6).unwrap();
        seg.idle_close(6).unwrap();
        assert!(seg.mmap.is_none());

        seg.ensure_open(6).unwrap();
        assert!(seg.mmap.is_some());
        assert_eq!(seg.lifecycle, SegmentLifecycle::OpenReady);
        // Pending was sealed during ensure_open recovery
        assert!(seg.pending_block_offset.is_none());
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
            let (block_off, in_block_off) = seg.append_record(ts, &data, 65536, 6).unwrap();
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
