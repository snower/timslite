//! File metadata header (128 bytes).
//!
//! Layout: fixed_core(10B) + meta_data_len(2B) + extension(52B) + reserved(64B) = 128B

use crate::error::{Result, TmslError};
use crate::util::*;
use memmap2::MmapMut;

// ─── Constants ───────────────────────────────────────────────────────────────

pub const HEADER_SIZE: u64 = 128;
pub const META_DATA_LEN: u16 = 52;

pub const MAGIC: [u8; 4] = *b"TMSL";
pub const VERSION: u16 = 1;

/// File header flags
pub const FILE_FLAG_SEALED: u16 = 0x0001;
pub const FILE_FLAG_HAS_PENDING: u16 = 0x0002;

// ─── Header byte layout offsets ──────────────────────────────────────────────

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_FILE_FLAGS: usize = 6;
const OFF_META_DATA_LEN: usize = 8;
// --- Extension metadata starts at 10 ---
const OFF_FILE_TYPE: usize = 10;
const OFF_FILE_OFFSET: usize = 18;
const OFF_FILE_SIZE: usize = 26;
const OFF_CREATED_AT: usize = 34;
const OFF_WROTE_POSITION: usize = 42;
const OFF_RECORD_COUNT: usize = 50;
const OFF_TOTAL_UNCOMP_SIZE: usize = 58;
// --- Pending fields in reserved area ---
const OFF_PENDING_BLOCK_OFFSET: usize = 64;
const OFF_PENDING_UNCOMP_SIZE: usize = 72;
const OFF_PENDING_RECORD_COUNT: usize = 76;
// Reserved 78..127 (50 bytes)

// ─── FileMetadata ────────────────────────────────────────────────────────────

/// File metadata in memory. Mirrors the 128-byte on-disk layout.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub magic: [u8; 4],
    pub version: u16,
    pub file_flags: u16,
    pub meta_data_len: u16,
    pub file_type: i64,
    pub file_offset: i64,
    pub file_size: i64,
    pub created_at: i64,
    pub wrote_position: i64,
    pub record_count: i64,
    pub total_uncompressed_size: u64,
    pub pending_block_offset: i64,
    pub pending_uncomp_size: u32,
    pub pending_record_count: u16,
}

impl FileMetadata {
    /// Create a new default metadata for a fresh segment.
    pub fn create_default(file_type: i64, file_offset: i64, file_size: i64) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            file_flags: 0,
            meta_data_len: META_DATA_LEN,
            file_type,
            file_offset,
            file_size,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            wrote_position: HEADER_SIZE as i64,
            record_count: 0,
            total_uncompressed_size: 0,
            pending_block_offset: -1,
            pending_uncomp_size: 0,
            pending_record_count: 0,
        }
    }

    /// Write the metadata to the first 128 bytes of `mmap`.
    pub fn write_to(&self, mmap: &mut MmapMut) {
        mmap[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&self.magic);
        write_u16_to_mmap(mmap, OFF_VERSION, self.version);
        write_u16_to_mmap(mmap, OFF_FILE_FLAGS, self.file_flags);
        write_u16_to_mmap(mmap, OFF_META_DATA_LEN, self.meta_data_len);
        write_i64_to_mmap(mmap, OFF_FILE_TYPE, self.file_type);
        write_i64_to_mmap(mmap, OFF_FILE_OFFSET, self.file_offset);
        write_i64_to_mmap(mmap, OFF_FILE_SIZE, self.file_size);
        write_i64_to_mmap(mmap, OFF_CREATED_AT, self.created_at);
        write_i64_to_mmap(mmap, OFF_WROTE_POSITION, self.wrote_position);
        write_i64_to_mmap(mmap, OFF_RECORD_COUNT, self.record_count);
        write_u64_to_mmap(mmap, OFF_TOTAL_UNCOMP_SIZE, self.total_uncompressed_size);
        write_i64_to_mmap(mmap, OFF_PENDING_BLOCK_OFFSET, self.pending_block_offset);
        write_u32_to_mmap(mmap, OFF_PENDING_UNCOMP_SIZE, self.pending_uncomp_size);
        write_u16_to_mmap(mmap, OFF_PENDING_RECORD_COUNT, self.pending_record_count);
    }

    /// Read FileMetadata from the first 128 bytes of `mmap`.
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
                VERSION
            );
        }

        let file_flags = read_u16_from_mmap(mmap, OFF_FILE_FLAGS);
        let meta_data_len = read_u16_from_mmap(mmap, OFF_META_DATA_LEN);

        let file_type = read_i64_from_mmap(mmap, OFF_FILE_TYPE);
        let file_offset = read_i64_from_mmap(mmap, OFF_FILE_OFFSET);
        let file_size = read_i64_from_mmap(mmap, OFF_FILE_SIZE);
        let created_at = read_i64_from_mmap(mmap, OFF_CREATED_AT);
        let wrote_position = read_i64_from_mmap(mmap, OFF_WROTE_POSITION);
        let record_count = read_i64_from_mmap(mmap, OFF_RECORD_COUNT);
        let total_uncompressed_size = read_u64_from_mmap(mmap, OFF_TOTAL_UNCOMP_SIZE);
        let pending_block_offset = read_i64_from_mmap(mmap, OFF_PENDING_BLOCK_OFFSET);
        let pending_uncomp_size = read_u32_from_mmap(mmap, OFF_PENDING_UNCOMP_SIZE);
        let pending_record_count = read_u16_from_mmap(mmap, OFF_PENDING_RECORD_COUNT);

        Ok(Self {
            magic,
            version,
            file_flags,
            meta_data_len,
            file_type,
            file_offset,
            file_size,
            created_at,
            wrote_position,
            record_count,
            total_uncompressed_size,
            pending_block_offset,
            pending_uncomp_size,
            pending_record_count,
        })
    }

    /// Update wrote_position in both memory and mmap.
    pub fn update_wrote_position(&mut self, mmap: &mut MmapMut, pos: i64) {
        self.wrote_position = pos;
        write_i64_to_mmap(mmap, OFF_WROTE_POSITION, pos);
    }

    /// Update pending state in both memory and mmap.
    pub fn update_pending_state(
        &mut self,
        mmap: &mut MmapMut,
        offset: i64,
        uncomp_size: u32,
        count: u16,
    ) {
        self.pending_block_offset = offset;
        self.pending_uncomp_size = uncomp_size;
        self.pending_record_count = count;
        write_i64_to_mmap(mmap, OFF_PENDING_BLOCK_OFFSET, offset);
        write_u32_to_mmap(mmap, OFF_PENDING_UNCOMP_SIZE, uncomp_size);
        write_u16_to_mmap(mmap, OFF_PENDING_RECORD_COUNT, count);
    }

    /// Clear pending state in both memory and mmap and remove FILE_FLAG_HAS_PENDING.
    pub fn clear_pending(&mut self, mmap: &mut MmapMut) {
        self.pending_block_offset = -1;
        self.pending_uncomp_size = 0;
        self.pending_record_count = 0;
        write_i64_to_mmap(mmap, OFF_PENDING_BLOCK_OFFSET, -1);
        write_u32_to_mmap(mmap, OFF_PENDING_UNCOMP_SIZE, 0);
        write_u16_to_mmap(mmap, OFF_PENDING_RECORD_COUNT, 0);
        self.file_flags &= !FILE_FLAG_HAS_PENDING;
        write_u16_to_mmap(mmap, OFF_FILE_FLAGS, self.file_flags);
    }

    /// Flush the in-memory file_flags back to mmap's fixed core area.
    pub fn flush_flags(&mut self, mmap: &mut MmapMut) {
        write_u16_to_mmap(mmap, OFF_FILE_FLAGS, self.file_flags);
    }

    /// Sync the mmap to disk.
    pub fn sync(&self, mmap: &mut MmapMut) -> crate::error::Result<()> {
        mmap.flush()?;
        Ok(())
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
        let meta = FileMetadata::create_default(1, 0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);

        let read = FileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.magic, MAGIC);
        assert_eq!(read.version, VERSION);
        assert_eq!(read.file_type, 1);
        assert_eq!(read.file_offset, 0);
        assert_eq!(read.file_size, 64 * 1024 * 1024);
        assert_eq!(read.pending_block_offset, -1);
        assert_eq!(read.meta_data_len, META_DATA_LEN);
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
        let mut meta = FileMetadata::create_default(1, 0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);
        meta.update_wrote_position(&mut mmap, 256);
        assert_eq!(meta.wrote_position, 256);
        assert_eq!(read_i64_from_mmap(&mmap, OFF_WROTE_POSITION), 256);
    }

    #[test]
    fn test_update_pending_state() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        let mut meta = FileMetadata::create_default(1, 0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);
        meta.update_pending_state(&mut mmap, 128, 1000, 5);
        assert_eq!(meta.pending_block_offset, 128);
        assert_eq!(meta.pending_uncomp_size, 1000);
        assert_eq!(meta.pending_record_count, 5);
    }

    #[test]
    fn test_clear_pending() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        let mut meta = FileMetadata::create_default(1, 0, 64 * 1024 * 1024);
        meta.file_flags |= FILE_FLAG_HAS_PENDING;
        meta.write_to(&mut mmap);
        meta.update_pending_state(&mut mmap, 128, 1000, 5);
        meta.flush_flags(&mut mmap);

        meta.clear_pending(&mut mmap);
        assert_eq!(meta.pending_block_offset, -1);
        assert_eq!(meta.pending_uncomp_size, 0);
        assert_eq!(meta.pending_record_count, 0);
        assert_eq!(meta.file_flags & FILE_FLAG_HAS_PENDING, 0);

        let read = FileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.pending_block_offset, -1);
        assert_eq!(read.file_flags & FILE_FLAG_HAS_PENDING, 0);
    }

    #[test]
    fn test_file_flags_sealed() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        let mut meta = FileMetadata::create_default(1, 0, 64 * 1024 * 1024);
        meta.file_flags |= FILE_FLAG_SEALED;
        meta.write_to(&mut mmap);
        meta.flush_flags(&mut mmap);

        let read = FileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.file_flags & FILE_FLAG_SEALED, FILE_FLAG_SEALED);
    }

    #[test]
    fn test_future_version_parse() {
        let (mut mmap, _path) = create_test_mmap(HEADER_SIZE);
        let meta = FileMetadata::create_default(1, 0, 64 * 1024 * 1024);
        meta.write_to(&mut mmap);
        write_u16_to_mmap(&mut mmap, OFF_VERSION, 5);

        let read = FileMetadata::read_from(&mmap).unwrap();
        assert_eq!(read.version, 5);
        assert_eq!(read.file_type, 1);
    }

    #[test]
    fn test_header_size_is_128() {
        assert_eq!(HEADER_SIZE, 128);
    }
}
