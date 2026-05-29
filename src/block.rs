//! Block header (16 bytes).
//!
//! Block-level aggregation: multiple records are packed into a block,
//! with optional deflate compression applied when the block is sealed.

use crate::util::*;

// ─── Constants ───────────────────────────────────────────────────────────────

pub const BLOCK_HEADER_SIZE: u64 = 16;
pub const BLOCK_MAX_SIZE: u32 = 65_536;

/// Block flags
pub const BLOCK_FLAG_COMPRESSED: u16 = 0x0001;
pub const BLOCK_FLAG_SEALED: u16 = 0x0002;
pub const BLOCK_FLAG_SINGLE_RECORD: u16 = 0x0004;

// Block header byte layout
const BH_PAYLOAD_SIZE: usize = 0; // u32: payload bytes (excluding this 16B header)
const BH_FLAGS: usize = 4; // u16: flags
const BH_RECORD_COUNT: usize = 6; // u16: number of records in block
const BH_UNCOMP_SIZE: usize = 8; // u32: total uncompressed size of all records
const BH_RESERVED: usize = 12; // u32: reserved

// ─── BlockHeader ─────────────────────────────────────────────────────────────

/// Block header representing the 16-byte on-disk structure.
#[derive(Debug, Clone, Copy)]
pub struct BlockHeader {
    pub payload_size: u32,
    pub flags: u16,
    pub record_count: u16,
    pub uncompressed_size: u32,
}

impl BlockHeader {
    /// Create a new block header.
    pub fn new(payload_size: u32, flags: u16, record_count: u16, uncompressed_size: u32) -> Self {
        Self {
            payload_size,
            flags,
            record_count,
            uncompressed_size,
        }
    }

    /// Write the header to `mmap` at `pos`.
    pub fn write_to(&self, mmap: &mut [u8], pos: usize) {
        write_u32_to_mmap(mmap, pos + BH_PAYLOAD_SIZE, self.payload_size);
        write_u16_to_mmap(mmap, pos + BH_FLAGS, self.flags);
        write_u16_to_mmap(mmap, pos + BH_RECORD_COUNT, self.record_count);
        write_u32_to_mmap(mmap, pos + BH_UNCOMP_SIZE, self.uncompressed_size);
        write_u32_to_mmap(mmap, pos + BH_RESERVED, 0);
    }

    /// Read a block header from `mmap` at `pos`.
    pub fn read_from(mmap: &[u8], pos: usize) -> Self {
        Self {
            payload_size: read_u32_from_mmap(mmap, pos + BH_PAYLOAD_SIZE),
            flags: read_u16_from_mmap(mmap, pos + BH_FLAGS),
            record_count: read_u16_from_mmap(mmap, pos + BH_RECORD_COUNT),
            uncompressed_size: read_u32_from_mmap(mmap, pos + BH_UNCOMP_SIZE),
        }
    }

    /// Returns true if the block payload is compressed.
    pub fn is_compressed(&self) -> bool {
        self.flags & BLOCK_FLAG_COMPRESSED != 0
    }

    /// Returns true if the block is sealed (no more writes).
    pub fn is_sealed(&self) -> bool {
        self.flags & BLOCK_FLAG_SEALED != 0
    }

    /// Returns true if the block contains a single large record.
    pub fn is_single_record(&self) -> bool {
        self.flags & BLOCK_FLAG_SINGLE_RECORD != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_header_roundtrip() {
        let mut buf = [0u8; 32];
        let header = BlockHeader::new(1024, BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED, 5, 2048);
        header.write_to(&mut buf, 0);

        let read = BlockHeader::read_from(&buf, 0);
        assert_eq!(read.payload_size, 1024);
        assert_eq!(read.flags, BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED);
        assert_eq!(read.record_count, 5);
        assert_eq!(read.uncompressed_size, 2048);
        assert!(read.is_compressed());
        assert!(read.is_sealed());
        assert!(!read.is_single_record());
    }

    #[test]
    fn test_block_header_flags_individual() {
        assert!(BlockHeader::new(0, BLOCK_FLAG_COMPRESSED, 0, 0).is_compressed());
        assert!(!BlockHeader::new(0, 0, 0, 0).is_compressed());

        assert!(BlockHeader::new(0, BLOCK_FLAG_SEALED, 0, 0).is_sealed());
        assert!(!BlockHeader::new(0, 0, 0, 0).is_sealed());

        assert!(BlockHeader::new(0, BLOCK_FLAG_SINGLE_RECORD, 0, 0).is_single_record());
        assert!(!BlockHeader::new(0, 0, 0, 0).is_single_record());
    }

    #[test]
    fn test_block_header_all_flags_combined() {
        let mut buf = [0u8; 16];
        let flags = BLOCK_FLAG_COMPRESSED | BLOCK_FLAG_SEALED | BLOCK_FLAG_SINGLE_RECORD;
        let header = BlockHeader::new(100, flags, 1, 500);
        header.write_to(&mut buf, 0);
        let read = BlockHeader::read_from(&buf, 0);

        assert!(read.is_compressed());
        assert!(read.is_sealed());
        assert!(read.is_single_record());
        assert_eq!(read.payload_size, 100);
        assert_eq!(read.record_count, 1);
    }

    #[test]
    fn test_block_header_size_is_16() {
        assert_eq!(BLOCK_HEADER_SIZE, 16);
    }

    #[test]
    fn test_block_header_read_at_offset() {
        let mut buf = [0u8; 64];
        let header = BlockHeader::new(256, BLOCK_FLAG_SEALED, 10, 512);
        header.write_to(&mut buf, 32);

        let read = BlockHeader::read_from(&buf, 32);
        assert_eq!(read.payload_size, 256);
        assert_eq!(read.record_count, 10);
    }

    #[test]
    fn test_block_header_reserved_is_zero() {
        let mut buf = [0u8; 16];
        let header = BlockHeader::new(0, 0, 0, 0);
        header.write_to(&mut buf, 0);
        let reserved = read_u32_from_mmap(&buf, BH_RESERVED);
        assert_eq!(reserved, 0);
    }
}
