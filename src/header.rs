//! File metadata header (100 bytes).
//!
//! Layout: fixed_prefix(9B) + meta_tlv(33B) + state_length(2B) + state(56B) = 100B
//!
//! Meta is immutable (written once at creation). State is mutable (updated on every write).

use crate::error::{Result, TmslError};
use crate::util::*;
use memmap2::MmapMut;

// ─── Constants ───────────────────────────────────────────────────────────────

pub const HEADER_SIZE: u64 = 100;

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

/// State has exactly 7 fields, each 8 bytes
const STATE_LENGTH_V1: u16 = 56;

// ─── On-disk offsets ────────────────────────────────────────────────────────

// Fixed prefix (9 bytes)
const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_FILE_TYPE: usize = 6;
const OFF_META_LENGTH: usize = 7;
// Meta TLV starts at offset 9
const META_START: usize = 9;
// state_length at offset 42
const OFF_STATE_LENGTH: usize = 42;
// State fields start at offset 44
const STATE_START: usize = 44;

// State field offsets (relative to STATE_START, each 8 bytes)
const S_WROTE_POSITION: usize = 0;
const S_RECORD_COUNT: usize = 8;
const S_TOTAL_UNCOMP_SIZE: usize = 16;
const S_INVALID_RECORD_COUNT: usize = 24;
const S_PENDING_BLOCK_OFFSET: usize = 32;
const S_PENDING_WROTE_POSITION: usize = 40;
const S_PENDING_RECORD_COUNT: usize = 48;

/// Sentinel value meaning "no pending block"
pub const PENDING_NONE: u64 = u64::MAX;

/// File metadata in memory.
///
/// Meta fields are immutable (written once at creation).
/// State fields are mutable (updated on every write).
#[derive(Debug, Clone)]
pub struct FileMetadata {
    // === Fixed prefix ===
    pub magic: [u8; 4],
    pub version: u16,
    pub file_type: u8, // FILE_TYPE_DATA or FILE_TYPE_INDEX

    // === Meta (immutable TLV, written at creation) ===
    pub created_at: i64,
    pub file_offset: i64,
    pub file_size: u32,
    pub compress_level: u8,
    pub meta_length: u16,

    // === State (mutable, 7×8B) ===
    pub wrote_position: u64,
    pub record_count: u64,
    pub total_uncompressed_size: u64,
    pub invalid_record_count: u64,
    pub pending_block_offset: u64, // PENDING_NONE = no pending
    pub pending_wrote_position: u64,
    pub pending_record_count: u64,
    pub state_length: u16,
}

impl FileMetadata {
    /// Create a new default metadata for a fresh segment.
    pub fn create_default(file_type: u8, file_offset: i64, file_size: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            file_type,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            file_offset,
            file_size,
            compress_level: 6,
            meta_length: META_LENGTH_V1,
            wrote_position: HEADER_SIZE,
            record_count: 0,
            total_uncompressed_size: 0,
            invalid_record_count: 0,
            pending_block_offset: PENDING_NONE,
            pending_wrote_position: 0,
            pending_record_count: 0,
            state_length: STATE_LENGTH_V1,
        }
    }

    /// Serialize the metadata to the first HEADER_SIZE bytes of mmap.
    pub fn write_to(&self, mmap: &mut MmapMut) {
        // Fixed prefix
        mmap[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&self.magic);
        write_u16_to_mmap(mmap, OFF_VERSION, self.version);
        mmap[OFF_FILE_TYPE] = self.file_type;
        write_u16_to_mmap(mmap, OFF_META_LENGTH, self.meta_length);

        // Meta TLV (starting at META_START=9)
        let mut off = META_START;
        // created_at
        mmap[off] = META_TYPE_CREATED_AT;
        off += 1;
        write_u16_to_mmap(mmap, off, 8);
        off += 2;
        write_i64_to_mmap(mmap, off, self.created_at);
        off += 8;
        // file_offset
        mmap[off] = META_TYPE_FILE_OFFSET;
        off += 1;
        write_u16_to_mmap(mmap, off, 8);
        off += 2;
        write_i64_to_mmap(mmap, off, self.file_offset);
        off += 8;
        // file_size
        mmap[off] = META_TYPE_FILE_SIZE;
        off += 1;
        write_u16_to_mmap(mmap, off, 4);
        off += 2;
        write_u32_to_mmap(mmap, off, self.file_size);
        off += 4;
        // compress_level
        mmap[off] = META_TYPE_COMPRESS_LEVEL;
        off += 1;
        write_u16_to_mmap(mmap, off, 1);
        off += 2;
        mmap[off] = self.compress_level;
        off += 1;

        // state_length
        write_u16_to_mmap(mmap, OFF_STATE_LENGTH, self.state_length);

        // State (7×8B, starting at STATE_START=44)
        write_u64_to_mmap(mmap, STATE_START + S_WROTE_POSITION, self.wrote_position);
        write_u64_to_mmap(mmap, STATE_START + S_RECORD_COUNT, self.record_count);
        write_u64_to_mmap(
            mmap,
            STATE_START + S_TOTAL_UNCOMP_SIZE,
            self.total_uncompressed_size,
        );
        write_u64_to_mmap(
            mmap,
            STATE_START + S_INVALID_RECORD_COUNT,
            self.invalid_record_count,
        );
        write_u64_to_mmap(
            mmap,
            STATE_START + S_PENDING_BLOCK_OFFSET,
            self.pending_block_offset,
        );
        write_u64_to_mmap(
            mmap,
            STATE_START + S_PENDING_WROTE_POSITION,
            self.pending_wrote_position,
        );
        write_u64_to_mmap(
            mmap,
            STATE_START + S_PENDING_RECORD_COUNT,
            self.pending_record_count,
        );
    }

    /// Read FileMetadata from the first HEADER_SIZE bytes of mmap.
    pub fn read_from(mmap: &MmapMut) -> Result<Self> {
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

        // Parse Meta TLV (skip unknown types)
        let (created_at, file_offset, file_size, compress_level) =
            Self::parse_meta_tlv(mmap, META_START, meta_length as usize)?;

        let state_length = read_u16_from_mmap(mmap, OFF_STATE_LENGTH);

        // Parse State (7×8B)
        let wrote_position = read_u64_from_mmap(mmap, STATE_START + S_WROTE_POSITION);
        let record_count = read_u64_from_mmap(mmap, STATE_START + S_RECORD_COUNT);
        let total_uncompressed_size = read_u64_from_mmap(mmap, STATE_START + S_TOTAL_UNCOMP_SIZE);
        let invalid_record_count = read_u64_from_mmap(mmap, STATE_START + S_INVALID_RECORD_COUNT);
        let pending_block_offset = read_u64_from_mmap(mmap, STATE_START + S_PENDING_BLOCK_OFFSET);
        let pending_wrote_position =
            read_u64_from_mmap(mmap, STATE_START + S_PENDING_WROTE_POSITION);
        let pending_record_count = read_u64_from_mmap(mmap, STATE_START + S_PENDING_RECORD_COUNT);

        Ok(Self {
            magic,
            version,
            file_type,
            created_at,
            file_offset,
            file_size,
            compress_level,
            meta_length,
            wrote_position,
            record_count,
            total_uncompressed_size,
            invalid_record_count,
            pending_block_offset,
            pending_wrote_position,
            pending_record_count,
            state_length,
        })
    }

    /// Parse Meta TLV entries from mmap. Unknown type codes are skipped.
    fn parse_meta_tlv(
        mmap: &MmapMut,
        start: usize,
        total_len: usize,
    ) -> Result<(i64, i64, u32, u8)> {
        let mut created_at = 0i64;
        let mut file_offset = 0i64;
        let mut file_size = 0u32;
        let mut compress_level = 0u8;

        let mut off = start;
        let end = start + total_len;
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

    /// Sync the mmap to disk.
    pub fn sync(&self, mmap: &mut MmapMut) -> crate::error::Result<()> {
        mmap.flush()?;
        Ok(())
    }

    // ─── State update helpers ────────────────────────────────────────────

    /// Update wrote_position, record_count, total_uncompressed_size in mmap.
    pub fn write_wrote_position(&mut self, mmap: &mut MmapMut) -> Result<()> {
        write_u64_to_mmap(mmap, STATE_START + S_WROTE_POSITION, self.wrote_position);
        write_u64_to_mmap(mmap, STATE_START + S_RECORD_COUNT, self.record_count);
        write_u64_to_mmap(
            mmap,
            STATE_START + S_TOTAL_UNCOMP_SIZE,
            self.total_uncompressed_size,
        );
        Ok(())
    }

    /// Update all three pending fields in mmap.
    pub fn write_pending(&mut self, mmap: &mut MmapMut) -> Result<()> {
        write_u64_to_mmap(
            mmap,
            STATE_START + S_PENDING_BLOCK_OFFSET,
            self.pending_block_offset,
        );
        write_u64_to_mmap(
            mmap,
            STATE_START + S_PENDING_WROTE_POSITION,
            self.pending_wrote_position,
        );
        write_u64_to_mmap(
            mmap,
            STATE_START + S_PENDING_RECORD_COUNT,
            self.pending_record_count,
        );
        Ok(())
    }

    /// Clear pending state in mmap (set to sentinel values).
    pub fn clear_pending(&mut self, mmap: &mut MmapMut) {
        self.pending_block_offset = PENDING_NONE;
        self.pending_wrote_position = 0;
        self.pending_record_count = 0;
        write_u64_to_mmap(mmap, STATE_START + S_PENDING_BLOCK_OFFSET, PENDING_NONE);
        write_u64_to_mmap(mmap, STATE_START + S_PENDING_WROTE_POSITION, 0);
        write_u64_to_mmap(mmap, STATE_START + S_PENDING_RECORD_COUNT, 0);
    }
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
    fn test_write_read_roundtrip() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        let meta = FileMetadata::create_default(FILE_TYPE_DATA, 0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);

        let read = FileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.magic, MAGIC);
        assert_eq!(read.version, VERSION);
        assert_eq!(read.file_type, FILE_TYPE_DATA);
        assert_eq!(read.file_offset, 0);
        assert_eq!(read.file_size, 64 * 1024 * 1024);
        assert_eq!(read.pending_block_offset, PENDING_NONE);
    }

    #[test]
    fn test_index_segment_roundtrip() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        let meta = FileMetadata::create_default(FILE_TYPE_INDEX, 1700000000, 4 * 1024 * 1024);
        meta.write_to(&mut mmap);

        let read = FileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.file_type, FILE_TYPE_INDEX);
        assert_eq!(read.file_offset, 1700000000);
    }

    #[test]
    fn test_invalid_magic() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        mmap[0..4].copy_from_slice(b"XXXX");
        let result = FileMetadata::read_from(&mmap);
        assert!(result.is_err());
        match result.unwrap_err() {
            TmslError::InvalidMagic => {}
            other => panic!("expected InvalidMagic, got: {other:?}"),
        }
    }

    #[test]
    fn test_update_wrote_position() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        let mut meta = FileMetadata::create_default(FILE_TYPE_DATA, 0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);
        meta.wrote_position = 256;
        meta.record_count = 10;
        meta.total_uncompressed_size = 2000;
        meta.write_wrote_position(&mut mmap).unwrap();

        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + S_WROTE_POSITION),
            256
        );
        assert_eq!(read_u64_from_mmap(&mmap, STATE_START + S_RECORD_COUNT), 10);
    }

    #[test]
    fn test_pending_state() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        let mut meta = FileMetadata::create_default(FILE_TYPE_DATA, 0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);
        meta.pending_block_offset = 128;
        meta.pending_wrote_position = 1000;
        meta.pending_record_count = 5;
        meta.write_pending(&mut mmap).unwrap();

        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + S_PENDING_BLOCK_OFFSET),
            128
        );
        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + S_PENDING_WROTE_POSITION),
            1000
        );
        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + S_PENDING_RECORD_COUNT),
            5
        );

        meta.clear_pending(&mut mmap);
        assert_eq!(
            read_u64_from_mmap(&mmap, STATE_START + S_PENDING_BLOCK_OFFSET),
            PENDING_NONE
        );
    }

    #[test]
    fn test_future_version_parse() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        let meta = FileMetadata::create_default(FILE_TYPE_DATA, 0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);
        write_u16_to_mmap(&mut mmap, OFF_VERSION, 5);

        let read = FileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.version, 5);
        assert_eq!(read.file_type, FILE_TYPE_DATA);
    }

    #[test]
    fn test_header_size_is_100() {
        assert_eq!(HEADER_SIZE, 100);
    }
}
