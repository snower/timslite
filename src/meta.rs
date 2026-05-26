//! DataSetMeta: immutable dataset configuration stored as TLV in a `meta` file.
//!
//! Written once at dataset creation, never updated. Used for validation when reopening
//! to detect configuration mismatches.

use std::path::Path;

use crate::error::{Result, TmslError};
use crate::util::{read_i64_le, read_u64_le};

const META_MAGIC: [u8; 4] = *b"TMSM";
const META_VERSION: u16 = 1;

const META_DATA_SEGMENT_SIZE: u8 = 0x01; // u64 LE
const META_INDEX_SEGMENT_SIZE: u8 = 0x02; // u64 LE
const META_COMPRESS_LEVEL: u8 = 0x03; // u8
const META_CREATE_TIME: u8 = 0x04; // i64 LE (unix ms)
const META_INDEX_CONTINUOUS: u8 = 0x05; // u8
const META_INITIAL_DATA_SEGMENT_SIZE: u8 = 0x06; // u64 LE
const META_INITIAL_INDEX_SEGMENT_SIZE: u8 = 0x07; // u64 LE

/// Immutable dataset configuration. Written once at creation.
#[derive(Debug, Clone)]
pub struct DataSetMeta {
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub compress_level: u8,
    pub create_time: i64, // unix ms
    pub index_continuous: u8,
    pub initial_data_segment_size: u64, // 0 = uninitialized (backward compat)
    pub initial_index_segment_size: u64, // 0 = uninitialized (backward compat)
}

impl DataSetMeta {
    /// Create a new meta (for new datasets, immutable after creation).
    pub fn new(
        data_segment_size: u64,
        index_segment_size: u64,
        compress_level: u8,
        index_continuous: u8,
        initial_data_segment_size: u64,
        initial_index_segment_size: u64,
    ) -> Self {
        Self {
            data_segment_size,
            index_segment_size,
            compress_level,
            index_continuous,
            initial_data_segment_size,
            initial_index_segment_size,
            create_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        }
    }

    /// Serialize: magic(4) + version(2) + meta_data_length(2) + TLV values
    pub fn to_bytes(&self) -> Vec<u8> {
        // Calculate meta_data_length:
        // 7 TLV entries: data_seg_size(14) + idx_seg_size(14) + compress(4) + create_time(10)
        //                + index_continuous(4) + initial_data_seg_size(14) + initial_idx_seg_size(14) = 78
        let meta_data_length: u16 = (1 + 2 + 8)
            + (1 + 2 + 8)
            + (1 + 2 + 1)
            + (1 + 2 + 8)
            + (1 + 2 + 1)
            + (1 + 2 + 8)
            + (1 + 2 + 8);

        let mut buf = Vec::with_capacity(8 + meta_data_length as usize);
        buf.extend_from_slice(&META_MAGIC);
        buf.extend_from_slice(&META_VERSION.to_le_bytes());
        buf.extend_from_slice(&meta_data_length.to_le_bytes());

        // TLV entries
        // data_segment_size
        buf.push(META_DATA_SEGMENT_SIZE);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.data_segment_size.to_le_bytes());
        // index_segment_size
        buf.push(META_INDEX_SEGMENT_SIZE);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.index_segment_size.to_le_bytes());
        // compress_level
        buf.push(META_COMPRESS_LEVEL);
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(self.compress_level);
        // create_time
        buf.push(META_CREATE_TIME);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.create_time.to_le_bytes());
        // index_continuous
        buf.push(META_INDEX_CONTINUOUS);
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(self.index_continuous);
        // initial_data_segment_size
        buf.push(META_INITIAL_DATA_SEGMENT_SIZE);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.initial_data_segment_size.to_le_bytes());
        // initial_index_segment_size
        buf.push(META_INITIAL_INDEX_SEGMENT_SIZE);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.initial_index_segment_size.to_le_bytes());

        buf
    }

    /// Deserialize: validate magic → read version → read meta_data_length → parse TLV
    /// Unknown type codes are skipped.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 8 {
            return Err(TmslError::InvalidData("meta file too short".into()));
        }
        if buf[0..4] != META_MAGIC {
            return Err(TmslError::InvalidData("invalid meta magic".into()));
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version > META_VERSION {
            log::warn!(
                "Meta file has version {}, expected {}",
                version,
                META_VERSION
            );
        }
        let meta_data_length = u16::from_le_bytes(buf[6..8].try_into().unwrap()) as usize;
        if buf.len() < 8 + meta_data_length {
            return Err(TmslError::InvalidData("meta file truncated".into()));
        }

        let mut data_segment_size = 0u64;
        let mut index_segment_size = 0u64;
        let mut compress_level = 0u8;
        let mut create_time = 0i64;
        let mut index_continuous = 0u8;
        let mut initial_data_segment_size = 0u64;
        let mut initial_index_segment_size = 0u64;

        let mut off = 8;
        let end = 8 + meta_data_length;
        while off + 3 <= end {
            let t = buf[off];
            off += 1;
            let len = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
            off += 2;
            if off + len > end {
                break;
            }
            match t {
                META_DATA_SEGMENT_SIZE if len == 8 => {
                    data_segment_size = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_INDEX_SEGMENT_SIZE if len == 8 => {
                    index_segment_size = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_COMPRESS_LEVEL if len == 1 => {
                    compress_level = buf[off];
                }
                META_CREATE_TIME if len == 8 => {
                    create_time = read_i64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_INDEX_CONTINUOUS if len == 1 => {
                    index_continuous = buf[off];
                }
                META_INITIAL_DATA_SEGMENT_SIZE if len == 8 => {
                    initial_data_segment_size = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_INITIAL_INDEX_SEGMENT_SIZE if len == 8 => {
                    initial_index_segment_size = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                _ => {} // Skip unknown
            }
            off += len;
        }

        Ok(Self {
            data_segment_size,
            index_segment_size,
            compress_level,
            create_time,
            index_continuous,
            initial_data_segment_size,
            initial_index_segment_size,
        })
    }

    /// Write to file (called once at dataset creation).
    pub fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        std::fs::write(path, self.to_bytes())
    }

    /// Read from file (called when opening existing dataset).
    pub fn read_from_file(path: &Path) -> Result<Self> {
        let buf = std::fs::read(path).map_err(|e| TmslError::Io(std::io::Error::other(e)))?;
        Self::from_bytes(&buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_roundtrip() {
        let meta = DataSetMeta::new(
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
        );
        let bytes = meta.to_bytes();

        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.data_segment_size, 64 * 1024 * 1024);
        assert_eq!(parsed.index_segment_size, 4 * 1024 * 1024);
        assert_eq!(parsed.compress_level, 6);
        assert_eq!(parsed.index_continuous, 0);
        assert_eq!(parsed.initial_data_segment_size, 256 * 1024);
        assert_eq!(parsed.initial_index_segment_size, 4 * 1024);
    }

    #[test]
    fn test_meta_old_format_compat() {
        // Simulate old format meta (without TLV 0x06/0x07) by manually constructing bytes
        let meta_with = DataSetMeta::new(1024, 512, 6, 0, 256, 128);
        let mut bytes = meta_with.to_bytes();
        // Remove last two TLV entries (0x06 and 0x07) from the end
        // Each TLV is 1(type) + 2(len) + 8(value) = 11 bytes
        let old_len = bytes.len() - 22;
        bytes.truncate(old_len);
        // Fix meta_data_length header
        let new_meta_len: u16 = (old_len - 8) as u16;
        bytes[6..8].copy_from_slice(&new_meta_len.to_le_bytes());

        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.data_segment_size, 1024);
        assert_eq!(parsed.index_segment_size, 512);
        assert_eq!(parsed.compress_level, 6);
        // initial_* should be 0 (not present in old format)
        assert_eq!(parsed.initial_data_segment_size, 0);
        assert_eq!(parsed.initial_index_segment_size, 0);
    }

    #[test]
    fn test_meta_index_continuous_roundtrip() {
        let meta = DataSetMeta::new(1024, 512, 6, 1, 256, 128);
        let bytes = meta.to_bytes();

        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.index_continuous, 1);

        // Also verify 0 works
        let meta_zero = DataSetMeta::new(1024, 512, 6, 0, 256, 128);
        let parsed_zero = DataSetMeta::from_bytes(&meta_zero.to_bytes()).unwrap();
        assert_eq!(parsed_zero.index_continuous, 0);
    }

    #[test]
    fn test_meta_magic() {
        assert_eq!(&META_MAGIC, b"TMSM");
    }

    #[test]
    fn test_meta_invalid_magic() {
        let mut bytes = DataSetMeta::new(100, 200, 3, 0, 100, 200).to_bytes();
        bytes[0..4].copy_from_slice(b"XXXX");
        let result = DataSetMeta::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_meta_file_roundtrip() {
        use std::fs;
        let dir = std::env::temp_dir().join("timslite_meta_test");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test_meta");

        let meta = DataSetMeta::new(1024, 512, 9, 0, 256, 128);
        meta.write_to_file(&path).unwrap();
        let loaded = DataSetMeta::read_from_file(&path).unwrap();
        assert_eq!(loaded.data_segment_size, 1024);
        assert_eq!(loaded.index_segment_size, 512);
        assert_eq!(loaded.compress_level, 9);
        assert_eq!(loaded.index_continuous, 0);
        assert_eq!(loaded.initial_data_segment_size, 256);
        assert_eq!(loaded.initial_index_segment_size, 128);
    }
}
