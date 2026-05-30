//! File metadata headers for data and index segments.
//!
//! Data segment: DataFileMetadata (v1 default 116 bytes)
//!   Layout: fixed_prefix(9B) + meta_tlv + state_length(2B) + state
//!
//! Index segment: IndexFileMetadata (v1 default 52 bytes)
//!   Layout: fixed_prefix(9B) + meta_tlv + state_length(2B) + state
//!
//! Meta is immutable (written once at creation). State is mutable (updated on every write).

use crate::error::{Result, TmslError};
use crate::util::*;
use memmap2::MmapMut;

// ─── Constants ───────────────────────────────────────────────────────────────

/// Data segment v1 default header size: 9 + 33 + 2 + 72 = 116 bytes.
pub const DATA_HEADER_SIZE: u64 = 116;

/// Index segment v1 default header size: 9 + 33 + 2 + 8 = 52 bytes.
pub const INDEX_HEADER_SIZE: u64 = 52;

pub const MAGIC: [u8; 4] = *b"TMSL";
pub const VERSION: u16 = 1;

/// File type constants
pub const FILE_TYPE_INDEX: u8 = 1;
pub const FILE_TYPE_DATA: u8 = 2;

/// Meta TLV type codes (immutable, written once at creation)
const META_TYPE_CREATED_AT: u8 = 0x01; // i64 LE, unix ms
const META_TYPE_FILE_OFFSET: u8 = 0x02; // i64 LE
const META_TYPE_FILE_SIZE: u8 = 0x03; // u32 LE
const META_TYPE_COMPRESS_LEVEL: u8 = 0x04; // u8

/// Meta TLV total length for v1: (1+2+8)+(1+2+8)+(1+2+4)+(1+2+1) = 33
const META_LENGTH_V1: u16 = 33;

/// Data state: 9 fields × 8 bytes = 72 bytes
const DATA_STATE_LENGTH_V1: u16 = 72;
/// Index state: 1 field × 8 bytes = 8 bytes
const INDEX_STATE_LENGTH_V1: u16 = 8;

// ─── On-disk offsets (shared prefix) ────────────────────────────────────────

// Fixed prefix (9 bytes)
const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_FILE_TYPE: usize = 6;
const OFF_META_LENGTH: usize = 7;
// Meta TLV starts at offset 9
const META_START: usize = 9;
pub const FIXED_PREFIX_SIZE: usize = META_START;
const STATE_LENGTH_SIZE: usize = 2;
// v1 state fields start at offset 44
#[cfg(test)]
const STATE_START: usize = 44;

// ─── Data segment state field offsets (relative to dynamic state start) ──
const DS_MIN_TIMESTAMP: usize = 0;
const DS_MAX_TIMESTAMP: usize = 8;
const DS_WROTE_POSITION: usize = 16;
const DS_RECORD_COUNT: usize = 24;
const DS_TOTAL_UNCOMP_SIZE: usize = 32;
const DS_PENDING_BLOCK_OFFSET: usize = 40;
const DS_PENDING_WROTE_POSITION: usize = 48;
const DS_PENDING_RECORD_COUNT: usize = 56;
const DS_INVALID_RECORD_COUNT: usize = 64;

// ─── Index segment state field offsets (relative to dynamic state start) ──
const IS_WROTE_POSITION: usize = 0;

/// Sentinel value meaning "no pending block"
pub const PENDING_NONE: u64 = u64::MAX;

/// Sentinel value for empty segment min_timestamp
pub const TIMESTAMP_MIN_SENTINEL: i64 = i64::MAX;
/// Sentinel value for empty segment max_timestamp
pub const TIMESTAMP_MAX_SENTINEL: i64 = i64::MIN;

fn state_length_offset(meta_length: u16) -> usize {
    META_START + meta_length as usize
}

fn state_start(meta_length: u16) -> usize {
    state_length_offset(meta_length) + STATE_LENGTH_SIZE
}

fn header_size(meta_length: u16, state_length: u16) -> u64 {
    (state_start(meta_length) + state_length as usize) as u64
}

fn ensure_len(mmap: &[u8], end: usize, what: &str) -> Result<()> {
    if mmap.len() < end {
        return Err(TmslError::InvalidData(format!(
            "truncated {}: need {} bytes, got {}",
            what,
            end,
            mmap.len()
        )));
    }
    Ok(())
}

fn validate_state_field(mmap: &[u8], expected_file_type: u8, field_offset: usize) -> Result<usize> {
    let (_, _, file_type, meta_length) = read_shared_prefix(mmap)?;
    if file_type != expected_file_type {
        return Err(TmslError::InvalidData(format!(
            "unexpected file_type {}",
            file_type
        )));
    }
    let state_len_off = state_length_offset(meta_length);
    ensure_len(mmap, state_len_off + STATE_LENGTH_SIZE, "state_length")?;
    let state_length = read_u16_from_mmap(mmap, state_len_off);
    let state_start = state_start(meta_length);
    let end = state_start + state_length as usize;
    ensure_len(mmap, end, "state")?;
    if field_offset + 8 > state_length as usize {
        return Err(TmslError::InvalidData(format!(
            "state field offset {} exceeds state_length {}",
            field_offset, state_length
        )));
    }
    Ok(state_start)
}

pub fn data_header_size_from_mmap(mmap: &[u8]) -> Result<u64> {
    let (_, _, file_type, meta_length) = read_shared_prefix(mmap)?;
    if file_type != FILE_TYPE_DATA {
        return Err(TmslError::InvalidData(format!(
            "unexpected file_type {}",
            file_type
        )));
    }
    let state_len_off = state_length_offset(meta_length);
    ensure_len(mmap, state_len_off + STATE_LENGTH_SIZE, "state_length")?;
    let state_length = read_u16_from_mmap(mmap, state_len_off);
    let size = header_size(meta_length, state_length);
    ensure_len(mmap, size as usize, "data header")?;
    Ok(size)
}

pub fn index_header_size_from_mmap(mmap: &[u8]) -> Result<u64> {
    let (_, _, file_type, meta_length) = read_shared_prefix(mmap)?;
    if file_type != FILE_TYPE_INDEX {
        return Err(TmslError::InvalidData(format!(
            "unexpected file_type {}",
            file_type
        )));
    }
    let state_len_off = state_length_offset(meta_length);
    ensure_len(mmap, state_len_off + STATE_LENGTH_SIZE, "state_length")?;
    let state_length = read_u16_from_mmap(mmap, state_len_off);
    let size = header_size(meta_length, state_length);
    ensure_len(mmap, size as usize, "index header")?;
    Ok(size)
}

pub fn write_data_core_state_to_mmap(
    mmap: &mut [u8],
    min_timestamp: i64,
    max_timestamp: i64,
    wrote_position: u64,
    record_count: u64,
    total_uncompressed_size: u64,
) -> Result<()> {
    let state_start = validate_state_field(mmap, FILE_TYPE_DATA, DS_TOTAL_UNCOMP_SIZE)?;
    mmap[state_start + DS_MIN_TIMESTAMP..state_start + DS_MIN_TIMESTAMP + 8]
        .copy_from_slice(&min_timestamp.to_le_bytes());
    mmap[state_start + DS_MAX_TIMESTAMP..state_start + DS_MAX_TIMESTAMP + 8]
        .copy_from_slice(&max_timestamp.to_le_bytes());
    mmap[state_start + DS_WROTE_POSITION..state_start + DS_WROTE_POSITION + 8]
        .copy_from_slice(&wrote_position.to_le_bytes());
    mmap[state_start + DS_RECORD_COUNT..state_start + DS_RECORD_COUNT + 8]
        .copy_from_slice(&record_count.to_le_bytes());
    mmap[state_start + DS_TOTAL_UNCOMP_SIZE..state_start + DS_TOTAL_UNCOMP_SIZE + 8]
        .copy_from_slice(&total_uncompressed_size.to_le_bytes());
    Ok(())
}

pub fn write_data_pending_state_to_mmap(
    mmap: &mut [u8],
    pending_block_offset: u64,
    pending_wrote_position: u64,
    pending_record_count: u64,
) -> Result<()> {
    let state_start = validate_state_field(mmap, FILE_TYPE_DATA, DS_PENDING_RECORD_COUNT)?;
    mmap[state_start + DS_PENDING_BLOCK_OFFSET..state_start + DS_PENDING_BLOCK_OFFSET + 8]
        .copy_from_slice(&pending_block_offset.to_le_bytes());
    mmap[state_start + DS_PENDING_WROTE_POSITION..state_start + DS_PENDING_WROTE_POSITION + 8]
        .copy_from_slice(&pending_wrote_position.to_le_bytes());
    mmap[state_start + DS_PENDING_RECORD_COUNT..state_start + DS_PENDING_RECORD_COUNT + 8]
        .copy_from_slice(&pending_record_count.to_le_bytes());
    Ok(())
}

pub fn write_data_invalid_record_count_to_mmap(
    mmap: &mut [u8],
    invalid_record_count: u64,
) -> Result<()> {
    let state_start = validate_state_field(mmap, FILE_TYPE_DATA, DS_INVALID_RECORD_COUNT)?;
    let off = state_start + DS_INVALID_RECORD_COUNT;
    mmap[off..off + 8].copy_from_slice(&invalid_record_count.to_le_bytes());
    Ok(())
}

pub fn write_index_wrote_position_to_mmap(mmap: &mut [u8], wrote_position: u64) -> Result<()> {
    let state_start = validate_state_field(mmap, FILE_TYPE_INDEX, IS_WROTE_POSITION)?;
    let off = state_start + IS_WROTE_POSITION;
    mmap[off..off + 8].copy_from_slice(&wrote_position.to_le_bytes());
    Ok(())
}

// ─── DataFileMetadata ────────────────────────────────────────────────────────

/// Data segment file metadata in memory.
///
/// Meta fields are immutable (written once at creation).
/// State fields are mutable (updated on every write).
#[derive(Debug, Clone)]
pub struct DataFileMetadata {
    // === Fixed prefix ===
    pub magic: [u8; 4],
    pub version: u16,
    pub file_type: u8,

    // === Meta (immutable TLV, written at creation) ===
    pub created_at: i64,
    pub file_offset: i64,
    pub file_size: u32,
    pub compress_level: u8,
    pub meta_length: u16,
    pub header_size: u64,

    // === State (mutable, 9×8B) ===
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub wrote_position: u64,
    pub record_count: u64,
    pub total_uncompressed_size: u64,
    pub pending_block_offset: u64, // PENDING_NONE = no pending
    pub pending_wrote_position: u64,
    pub pending_record_count: u64,
    pub invalid_record_count: u64,
    pub state_length: u16,
}

impl DataFileMetadata {
    /// Create a new default metadata for a fresh data segment.
    pub fn create_default(file_offset: i64, file_size: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            file_type: FILE_TYPE_DATA,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            file_offset,
            file_size,
            compress_level: 6,
            meta_length: META_LENGTH_V1,
            header_size: DATA_HEADER_SIZE,
            min_timestamp: TIMESTAMP_MIN_SENTINEL,
            max_timestamp: TIMESTAMP_MAX_SENTINEL,
            wrote_position: DATA_HEADER_SIZE,
            record_count: 0,
            total_uncompressed_size: 0,
            pending_block_offset: PENDING_NONE,
            pending_wrote_position: 0,
            pending_record_count: 0,
            invalid_record_count: 0,
            state_length: DATA_STATE_LENGTH_V1,
        }
    }

    fn state_start(&self) -> usize {
        state_start(self.meta_length)
    }

    /// Serialize the metadata to the header area of mmap.
    pub fn write_to(&self, mmap: &mut MmapMut) {
        write_shared_prefix(
            mmap,
            self.magic,
            self.version,
            self.file_type,
            self.meta_length,
        );
        write_meta_tlv(
            mmap,
            self.created_at,
            self.file_offset,
            self.file_size,
            self.compress_level,
        );

        let state_length_offset = state_length_offset(self.meta_length);
        let state_start = self.state_start();

        write_u16_to_mmap(mmap, state_length_offset, self.state_length);

        write_i64_to_mmap(mmap, state_start + DS_MIN_TIMESTAMP, self.min_timestamp);
        write_i64_to_mmap(mmap, state_start + DS_MAX_TIMESTAMP, self.max_timestamp);
        write_u64_to_mmap(mmap, state_start + DS_WROTE_POSITION, self.wrote_position);
        write_u64_to_mmap(mmap, state_start + DS_RECORD_COUNT, self.record_count);
        write_u64_to_mmap(
            mmap,
            state_start + DS_TOTAL_UNCOMP_SIZE,
            self.total_uncompressed_size,
        );
        write_u64_to_mmap(
            mmap,
            state_start + DS_PENDING_BLOCK_OFFSET,
            self.pending_block_offset,
        );
        write_u64_to_mmap(
            mmap,
            state_start + DS_PENDING_WROTE_POSITION,
            self.pending_wrote_position,
        );
        write_u64_to_mmap(
            mmap,
            state_start + DS_PENDING_RECORD_COUNT,
            self.pending_record_count,
        );
        write_u64_to_mmap(
            mmap,
            state_start + DS_INVALID_RECORD_COUNT,
            self.invalid_record_count,
        );
    }

    /// Read DataFileMetadata from a byte slice.
    pub fn read_from(mmap: &[u8]) -> Result<Self> {
        let (magic, version, file_type, meta_length) = read_shared_prefix(mmap)?;
        let (created_at, file_offset, file_size, compress_level) =
            parse_meta_tlv(mmap, META_START, meta_length as usize)?;
        let state_length_offset = state_length_offset(meta_length);
        ensure_len(
            mmap,
            state_length_offset + STATE_LENGTH_SIZE,
            "data state_length",
        )?;
        let state_length = read_u16_from_mmap(mmap, state_length_offset);
        if state_length < DATA_STATE_LENGTH_V1 {
            return Err(TmslError::InvalidData(format!(
                "data state_length {} is smaller than v1 {}",
                state_length, DATA_STATE_LENGTH_V1
            )));
        }
        let state_start = state_start(meta_length);
        let header_size = header_size(meta_length, state_length);
        ensure_len(mmap, header_size as usize, "data header")?;

        let min_timestamp = read_i64_from_mmap(mmap, state_start + DS_MIN_TIMESTAMP);
        let max_timestamp = read_i64_from_mmap(mmap, state_start + DS_MAX_TIMESTAMP);
        let wrote_position = read_u64_from_mmap(mmap, state_start + DS_WROTE_POSITION);
        let record_count = read_u64_from_mmap(mmap, state_start + DS_RECORD_COUNT);
        let total_uncompressed_size = read_u64_from_mmap(mmap, state_start + DS_TOTAL_UNCOMP_SIZE);
        let pending_block_offset = read_u64_from_mmap(mmap, state_start + DS_PENDING_BLOCK_OFFSET);
        let pending_wrote_position =
            read_u64_from_mmap(mmap, state_start + DS_PENDING_WROTE_POSITION);
        let pending_record_count = read_u64_from_mmap(mmap, state_start + DS_PENDING_RECORD_COUNT);
        let invalid_record_count = read_u64_from_mmap(mmap, state_start + DS_INVALID_RECORD_COUNT);

        Ok(Self {
            magic,
            version,
            file_type,
            created_at,
            file_offset,
            file_size,
            compress_level,
            meta_length,
            header_size,
            min_timestamp,
            max_timestamp,
            wrote_position,
            record_count,
            total_uncompressed_size,
            pending_block_offset,
            pending_wrote_position,
            pending_record_count,
            invalid_record_count,
            state_length,
        })
    }

    /// Sync the mmap to disk.
    pub fn sync(&self, mmap: &mut MmapMut) -> Result<()> {
        mmap.flush()?;
        Ok(())
    }

    // ─── State update helpers ────────────────────────────────────────────

    /// Update min_timestamp, max_timestamp, wrote_position, record_count, total_uncompressed_size in mmap.
    #[allow(clippy::too_many_arguments)]
    pub fn write_state_full(
        &mut self,
        mmap: &mut MmapMut,
        min_ts: i64,
        max_ts: i64,
        wrote_pos: u64,
        rec_count: u64,
        uncomp_size: u64,
    ) {
        self.min_timestamp = min_ts;
        self.max_timestamp = max_ts;
        self.wrote_position = wrote_pos;
        self.record_count = rec_count;
        self.total_uncompressed_size = uncomp_size;
        let state_start = self.state_start();
        write_i64_to_mmap(mmap, state_start + DS_MIN_TIMESTAMP, self.min_timestamp);
        write_i64_to_mmap(mmap, state_start + DS_MAX_TIMESTAMP, self.max_timestamp);
        write_u64_to_mmap(mmap, state_start + DS_WROTE_POSITION, self.wrote_position);
        write_u64_to_mmap(mmap, state_start + DS_RECORD_COUNT, self.record_count);
        write_u64_to_mmap(
            mmap,
            state_start + DS_TOTAL_UNCOMP_SIZE,
            self.total_uncompressed_size,
        );
    }

    /// Update all three pending fields in mmap.
    pub fn write_pending(&mut self, mmap: &mut MmapMut) {
        let state_start = self.state_start();
        write_u64_to_mmap(
            mmap,
            state_start + DS_PENDING_BLOCK_OFFSET,
            self.pending_block_offset,
        );
        write_u64_to_mmap(
            mmap,
            state_start + DS_PENDING_WROTE_POSITION,
            self.pending_wrote_position,
        );
        write_u64_to_mmap(
            mmap,
            state_start + DS_PENDING_RECORD_COUNT,
            self.pending_record_count,
        );
    }

    /// Clear pending state in mmap (set to sentinel values).
    pub fn clear_pending(&mut self, mmap: &mut MmapMut) {
        self.pending_block_offset = PENDING_NONE;
        self.pending_wrote_position = 0;
        self.pending_record_count = 0;
        let state_start = self.state_start();
        write_u64_to_mmap(mmap, state_start + DS_PENDING_BLOCK_OFFSET, PENDING_NONE);
        write_u64_to_mmap(mmap, state_start + DS_PENDING_WROTE_POSITION, 0);
        write_u64_to_mmap(mmap, state_start + DS_PENDING_RECORD_COUNT, 0);
    }
}

// ─── IndexFileMetadata ───────────────────────────────────────────────────────

/// Index segment file metadata in memory.
///
/// Meta fields are immutable (written once at creation).
/// State has only wrote_position (8 bytes).
#[derive(Debug, Clone)]
pub struct IndexFileMetadata {
    // === Fixed prefix ===
    pub magic: [u8; 4],
    pub version: u16,
    pub file_type: u8,

    // === Meta (immutable TLV, written at creation) ===
    pub created_at: i64,
    pub file_offset: i64,
    pub file_size: u32,
    pub compress_level: u8,
    pub meta_length: u16,
    pub header_size: u64,

    // === State (mutable, 1×8B) ===
    pub wrote_position: u64,
    pub state_length: u16,
}

impl IndexFileMetadata {
    /// Create a new default metadata for a fresh index segment.
    pub fn create_default(file_offset: i64, file_size: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            file_type: FILE_TYPE_INDEX,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            file_offset,
            file_size,
            compress_level: 6,
            meta_length: META_LENGTH_V1,
            header_size: INDEX_HEADER_SIZE,
            wrote_position: INDEX_HEADER_SIZE,
            state_length: INDEX_STATE_LENGTH_V1,
        }
    }

    fn state_start(&self) -> usize {
        state_start(self.meta_length)
    }

    /// Serialize the metadata to the header area of mmap.
    pub fn write_to(&self, mmap: &mut MmapMut) {
        write_shared_prefix(
            mmap,
            self.magic,
            self.version,
            self.file_type,
            self.meta_length,
        );
        write_meta_tlv(
            mmap,
            self.created_at,
            self.file_offset,
            self.file_size,
            self.compress_level,
        );

        let state_length_offset = state_length_offset(self.meta_length);
        let state_start = self.state_start();

        write_u16_to_mmap(mmap, state_length_offset, self.state_length);
        write_u64_to_mmap(mmap, state_start + IS_WROTE_POSITION, self.wrote_position);
    }

    /// Read IndexFileMetadata from a byte slice.
    pub fn read_from(mmap: &[u8]) -> Result<Self> {
        let (magic, version, file_type, meta_length) = read_shared_prefix(mmap)?;
        let (created_at, file_offset, file_size, compress_level) =
            parse_meta_tlv(mmap, META_START, meta_length as usize)?;
        let state_length_offset = state_length_offset(meta_length);
        ensure_len(
            mmap,
            state_length_offset + STATE_LENGTH_SIZE,
            "index state_length",
        )?;
        let state_length = read_u16_from_mmap(mmap, state_length_offset);
        if state_length < INDEX_STATE_LENGTH_V1 {
            return Err(TmslError::InvalidData(format!(
                "index state_length {} is smaller than v1 {}",
                state_length, INDEX_STATE_LENGTH_V1
            )));
        }
        let state_start = state_start(meta_length);
        let header_size = header_size(meta_length, state_length);
        ensure_len(mmap, header_size as usize, "index header")?;
        let wrote_position = read_u64_from_mmap(mmap, state_start + IS_WROTE_POSITION);

        Ok(Self {
            magic,
            version,
            file_type,
            created_at,
            file_offset,
            file_size,
            compress_level,
            meta_length,
            header_size,
            wrote_position,
            state_length,
        })
    }

    /// Sync the mmap to disk.
    pub fn sync(&self, mmap: &mut MmapMut) -> Result<()> {
        mmap.flush()?;
        Ok(())
    }

    /// Update wrote_position in mmap.
    pub fn write_wrote_position(&mut self, mmap: &mut MmapMut, wrote_pos: u64) {
        self.wrote_position = wrote_pos;
        let state_start = self.state_start();
        write_u64_to_mmap(mmap, state_start + IS_WROTE_POSITION, self.wrote_position);
    }
}

// ─── Shared helpers ──────────────────────────────────────────────────────────

fn write_shared_prefix(
    mmap: &mut MmapMut,
    magic: [u8; 4],
    version: u16,
    file_type: u8,
    meta_length: u16,
) {
    mmap[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&magic);
    write_u16_to_mmap(mmap, OFF_VERSION, version);
    mmap[OFF_FILE_TYPE] = file_type;
    write_u16_to_mmap(mmap, OFF_META_LENGTH, meta_length);
}

fn write_meta_tlv(
    mmap: &mut MmapMut,
    created_at: i64,
    file_offset: i64,
    file_size: u32,
    compress_level: u8,
) {
    let mut off = META_START;
    // created_at
    mmap[off] = META_TYPE_CREATED_AT;
    off += 1;
    write_u16_to_mmap(mmap, off, 8);
    off += 2;
    write_i64_to_mmap(mmap, off, created_at);
    off += 8;
    // file_offset
    mmap[off] = META_TYPE_FILE_OFFSET;
    off += 1;
    write_u16_to_mmap(mmap, off, 8);
    off += 2;
    write_i64_to_mmap(mmap, off, file_offset);
    off += 8;
    // file_size
    mmap[off] = META_TYPE_FILE_SIZE;
    off += 1;
    write_u16_to_mmap(mmap, off, 4);
    off += 2;
    write_u32_to_mmap(mmap, off, file_size);
    off += 4;
    // compress_level
    mmap[off] = META_TYPE_COMPRESS_LEVEL;
    off += 1;
    write_u16_to_mmap(mmap, off, 1);
    off += 2;
    mmap[off] = compress_level;
}

fn read_shared_prefix(mmap: &[u8]) -> Result<([u8; 4], u16, u8, u16)> {
    ensure_len(mmap, FIXED_PREFIX_SIZE, "fixed header prefix")?;
    let magic: [u8; 4] = mmap[OFF_MAGIC..OFF_MAGIC + 4]
        .try_into()
        .map_err(|_| TmslError::InvalidData("cannot read magic".into()))?;
    if magic != MAGIC {
        return Err(TmslError::InvalidMagic);
    }
    let version = read_u16_from_mmap(mmap, OFF_VERSION);
    if version > VERSION {
        log::warn!(
            "File has version {} but we know {}, parsing known fields only",
            version,
            VERSION,
        );
    }
    let file_type = mmap[OFF_FILE_TYPE];
    let meta_length = read_u16_from_mmap(mmap, OFF_META_LENGTH);
    Ok((magic, version, file_type, meta_length))
}

/// Parse Meta TLV entries from mmap. Unknown type codes are skipped.
fn parse_meta_tlv(mmap: &[u8], start: usize, total_len: usize) -> Result<(i64, i64, u32, u8)> {
    let mut created_at = 0i64;
    let mut file_offset = 0i64;
    let mut file_size = 0u32;
    let mut compress_level = 0u8;

    let mut off = start;
    let end = start
        .checked_add(total_len)
        .ok_or_else(|| TmslError::InvalidData("meta length overflow".into()))?;
    ensure_len(mmap, end, "meta tlv")?;
    while off + 3 <= end {
        let t = mmap[off];
        off += 1;
        let len = read_u16_from_mmap(mmap, off) as usize;
        off += 2;
        if off + len > end {
            break;
        }
        match t {
            META_TYPE_CREATED_AT if len == 8 => {
                created_at = read_i64_from_mmap(mmap, off);
            }
            META_TYPE_FILE_OFFSET if len == 8 => {
                file_offset = read_i64_from_mmap(mmap, off);
            }
            META_TYPE_FILE_SIZE if len == 4 => {
                file_size = read_u32_from_mmap(mmap, off);
            }
            META_TYPE_COMPRESS_LEVEL if len == 1 => {
                compress_level = mmap[off];
            }
            _ => {} // Skip unknown type
        }
        off += len;
    }

    Ok((created_at, file_offset, file_size, compress_level))
}

/// Clear pending state directly in an mmap slice (without a struct).
pub fn clear_pending_from_mmap(mmap: &mut [u8]) -> Result<()> {
    write_data_pending_state_to_mmap(mmap, PENDING_NONE, 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use memmap2::MmapMut;
    use std::fs::OpenOptions;
    use std::path::PathBuf;

    fn create_test_mmap(size: u64) -> (MmapMut, PathBuf) {
        let dir = std::env::temp_dir().join("timslite_test_header");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!(
            "test_header_{:?}.dat",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len(size).unwrap();
        file.sync_all().unwrap();
        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        (mmap, path)
    }

    #[test]
    fn test_data_write_read_roundtrip() {
        let (mut mmap, _path) = create_test_mmap(DATA_HEADER_SIZE);
        let meta = DataFileMetadata::create_default(0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);

        let read = DataFileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.magic, MAGIC);
        assert_eq!(read.version, VERSION);
        assert_eq!(read.file_type, FILE_TYPE_DATA);
        assert_eq!(read.file_offset, 0);
        assert_eq!(read.file_size, 64 * 1024 * 1024);
        assert_eq!(read.pending_block_offset, PENDING_NONE);
        assert_eq!(read.min_timestamp, TIMESTAMP_MIN_SENTINEL);
        assert_eq!(read.max_timestamp, TIMESTAMP_MAX_SENTINEL);
    }

    #[test]
    fn test_index_segment_roundtrip() {
        let (mut mmap, _path) = create_test_mmap(INDEX_HEADER_SIZE);
        let meta = IndexFileMetadata::create_default(1700000000, 4 * 1024 * 1024);
        meta.write_to(&mut mmap);

        let read = IndexFileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.file_type, FILE_TYPE_INDEX);
        assert_eq!(read.file_offset, 1700000000);
        assert_eq!(read.wrote_position, INDEX_HEADER_SIZE);
    }

    #[test]
    fn test_data_header_reads_state_after_extended_meta() {
        let extra_meta = [0xEE, 4, 0, 1, 2, 3, 4];
        let meta_length = META_LENGTH_V1 + extra_meta.len() as u16;
        let state_length_offset = META_START + meta_length as usize;
        let state_start = state_length_offset + 2;
        let header_size = state_start as u64 + DATA_STATE_LENGTH_V1 as u64;
        let (mut mmap, _path) = create_test_mmap(header_size);

        write_shared_prefix(&mut mmap, MAGIC, VERSION, FILE_TYPE_DATA, meta_length);
        write_meta_tlv(&mut mmap, 1000, 0, 1024, 6);
        let extra_start = META_START + META_LENGTH_V1 as usize;
        mmap[extra_start..extra_start + extra_meta.len()].copy_from_slice(&extra_meta);
        write_u16_to_mmap(&mut mmap, state_length_offset, DATA_STATE_LENGTH_V1);
        write_i64_to_mmap(&mut mmap, state_start + DS_MIN_TIMESTAMP, 11);
        write_i64_to_mmap(&mut mmap, state_start + DS_MAX_TIMESTAMP, 22);
        write_u64_to_mmap(&mut mmap, state_start + DS_WROTE_POSITION, header_size);
        write_u64_to_mmap(&mut mmap, state_start + DS_RECORD_COUNT, 2);
        write_u64_to_mmap(&mut mmap, state_start + DS_TOTAL_UNCOMP_SIZE, 44);
        write_u64_to_mmap(
            &mut mmap,
            state_start + DS_PENDING_BLOCK_OFFSET,
            PENDING_NONE,
        );
        write_u64_to_mmap(&mut mmap, state_start + DS_PENDING_WROTE_POSITION, 0);
        write_u64_to_mmap(&mut mmap, state_start + DS_PENDING_RECORD_COUNT, 0);
        write_u64_to_mmap(&mut mmap, state_start + DS_INVALID_RECORD_COUNT, 3);

        let read = DataFileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.meta_length, meta_length);
        assert_eq!(read.state_length, DATA_STATE_LENGTH_V1);
        assert_eq!(read.min_timestamp, 11);
        assert_eq!(read.max_timestamp, 22);
        assert_eq!(read.wrote_position, header_size);
        assert_eq!(read.record_count, 2);
        assert_eq!(read.invalid_record_count, 3);
    }

    #[test]
    fn test_index_header_reads_state_after_extended_meta() {
        let extra_meta = [0xEF, 3, 0, 7, 8, 9];
        let meta_length = META_LENGTH_V1 + extra_meta.len() as u16;
        let state_length_offset = META_START + meta_length as usize;
        let state_start = state_length_offset + 2;
        let header_size = state_start as u64 + INDEX_STATE_LENGTH_V1 as u64;
        let (mut mmap, _path) = create_test_mmap(header_size);

        write_shared_prefix(&mut mmap, MAGIC, VERSION, FILE_TYPE_INDEX, meta_length);
        write_meta_tlv(&mut mmap, 1000, 1700000000, 4096, 6);
        let extra_start = META_START + META_LENGTH_V1 as usize;
        mmap[extra_start..extra_start + extra_meta.len()].copy_from_slice(&extra_meta);
        write_u16_to_mmap(&mut mmap, state_length_offset, INDEX_STATE_LENGTH_V1);
        write_u64_to_mmap(&mut mmap, state_start + IS_WROTE_POSITION, header_size);

        let read = IndexFileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.meta_length, meta_length);
        assert_eq!(read.state_length, INDEX_STATE_LENGTH_V1);
        assert_eq!(read.file_offset, 1700000000);
        assert_eq!(read.wrote_position, header_size);
    }

    #[test]
    fn test_invalid_magic() {
        let (mut mmap, _path) = create_test_mmap(DATA_HEADER_SIZE);
        mmap[0..4].copy_from_slice(b"XXXX");
        let result = DataFileMetadata::read_from(&mmap);
        assert!(result.is_err());
        match result.unwrap_err() {
            TmslError::InvalidMagic => {}
            other => panic!("expected InvalidMagic, got: {other:?}"),
        }
    }

    #[test]
    fn test_data_update_state() {
        let (mut mmap, _path) = create_test_mmap(DATA_HEADER_SIZE);
        let mut meta = DataFileMetadata::create_default(0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);
        meta.write_state_full(&mut mmap, 100, 200, 256, 10, 2000);

        assert_eq!(
            read_i64_from_mmap(&mmap, STATE_START + DS_MIN_TIMESTAMP),
            100
        );
        assert_eq!(
            read_i64_from_mmap(&mmap, STATE_START + DS_MAX_TIMESTAMP),
            200
        );
        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + DS_WROTE_POSITION),
            256
        );
        assert_eq!(read_u64_from_mmap(&mmap, STATE_START + DS_RECORD_COUNT), 10);
    }

    #[test]
    fn test_data_pending_state() {
        let (mut mmap, _path) = create_test_mmap(DATA_HEADER_SIZE);
        let mut meta = DataFileMetadata::create_default(0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);
        meta.pending_block_offset = 128;
        meta.pending_wrote_position = 1000;
        meta.pending_record_count = 5;
        meta.write_pending(&mut mmap);

        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + DS_PENDING_BLOCK_OFFSET),
            128
        );
        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + DS_PENDING_WROTE_POSITION),
            1000
        );
        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + DS_PENDING_RECORD_COUNT),
            5
        );

        meta.clear_pending(&mut mmap);
        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + DS_PENDING_BLOCK_OFFSET),
            PENDING_NONE
        );
    }

    #[test]
    fn test_index_update_wrote_position() {
        let (mut mmap, _path) = create_test_mmap(INDEX_HEADER_SIZE);
        let mut meta = IndexFileMetadata::create_default(0, 4 * 1024 * 1024);
        meta.write_to(&mut mmap);
        meta.write_wrote_position(&mut mmap, 100);
        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + IS_WROTE_POSITION),
            100
        );
    }

    #[test]
    fn test_future_version_parse() {
        let (mut mmap, _path) = create_test_mmap(DATA_HEADER_SIZE);
        let meta = DataFileMetadata::create_default(0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);
        write_u16_to_mmap(&mut mmap, OFF_VERSION, 5);

        let read = DataFileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.version, 5);
        assert_eq!(read.file_type, FILE_TYPE_DATA);
    }

    #[test]
    fn test_header_sizes() {
        assert_eq!(DATA_HEADER_SIZE, 116);
        assert_eq!(INDEX_HEADER_SIZE, 52);
    }
}
