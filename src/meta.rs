//! DataSetMeta: immutable dataset configuration stored as TLV in a `meta` file.
//!
//! Written once at dataset creation, never updated. It is the sole persistent
//! source of dataset layout/configuration when reopening.

use std::path::Path;

use crate::compress::validate_compress_type;
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
const META_RETENTION_WINDOW: u8 = 0x08; // u64 LE (0 = no limit)
const META_COMPRESS_TYPE: u8 = 0x09; // u8

pub(crate) const META_VALUES_LEN_V1: usize = 78;

/// Immutable dataset configuration. Written once at creation.
#[derive(Debug, Clone)]
pub struct DataSetMeta {
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub compress_level: u8,
    pub compress_type: u8,
    pub create_time: i64, // unix ms
    pub index_continuous: u8,
    pub initial_data_segment_size: u64, // 0 = uninitialized (backward compat)
    pub initial_index_segment_size: u64, // 0 = uninitialized (backward compat)
    pub retention_window: u64,          // 0 = no limit (same unit as timestamp)
}

impl DataSetMeta {
    /// Create a new meta (for new datasets, immutable after creation).
    pub fn new(
        data_segment_size: u64,
        index_segment_size: u64,
        compress_level: u8,
        compress_type: u8,
        index_continuous: u8,
        initial_data_segment_size: u64,
        initial_index_segment_size: u64,
        retention_window: u64,
    ) -> Self {
        Self {
            data_segment_size,
            index_segment_size,
            compress_level,
            compress_type,
            index_continuous,
            initial_data_segment_size,
            initial_index_segment_size,
            retention_window,
            create_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        }
    }

    /// Serialize: magic(4) + version(2) + meta_data_length(2) + TLV values
    pub fn to_bytes(&self) -> Vec<u8> {
        // Calculate meta_data_length:
        // 9 TLV entries: data_seg_size(11) + idx_seg_size(11) + compress_level(4)
        //               + compress_type(4) + create_time(11)
        //               + index_continuous(4) + initial_data_seg_size(11) + initial_idx_seg_size(11)
        //               + retention_window(11) = 78
        // Each u64 TLV: type(1) + length(2) + value(8) = 11 bytes
        // Each u8 TLV:  type(1) + length(2) + value(1) = 4 bytes
        let meta_data_length: u16 = META_VALUES_LEN_V1 as u16;
        debug_assert_eq!(
            META_VALUES_LEN_V1,
            (1 + 2 + 8)
                + (1 + 2 + 8)
                + (1 + 2 + 1)
                + (1 + 2 + 1)
                + (1 + 2 + 8)
                + (1 + 2 + 1)
                + (1 + 2 + 8)
                + (1 + 2 + 8)
                + (1 + 2 + 8)
        );

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
        // compress_type
        buf.push(META_COMPRESS_TYPE);
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(self.compress_type);
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
        // retention_window
        buf.push(META_RETENTION_WINDOW);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.retention_window.to_le_bytes());

        buf
    }

    /// Deserialize: validate magic 鈫?read version 鈫?read meta_data_length 鈫?parse TLV
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
        let mut compress_type = None;
        let mut create_time = 0i64;
        let mut index_continuous = 0u8;
        let mut initial_data_segment_size = 0u64;
        let mut initial_index_segment_size = 0u64;
        let mut retention_window = 0u64;

        let mut off = 8;
        let end = 8 + meta_data_length;
        while off + 3 <= end {
            let t = buf[off];
            off += 1;
            let len = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
            off += 2;
            if off + len > end {
                return Err(TmslError::InvalidData("meta TLV entry truncated".into()));
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
                META_COMPRESS_TYPE if len == 1 => {
                    compress_type = Some(buf[off]);
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
                META_RETENTION_WINDOW if len == 8 => {
                    retention_window = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                _ => {} // Skip unknown
            }
            off += len;
        }
        if off != end {
            return Err(TmslError::InvalidData(
                "meta TLV data has trailing partial entry".into(),
            ));
        }

        if data_segment_size == 0 {
            return Err(TmslError::InvalidData(
                "meta missing data_segment_size".into(),
            ));
        }
        if index_segment_size == 0 {
            return Err(TmslError::InvalidData(
                "meta missing index_segment_size".into(),
            ));
        }
        if compress_level > 9 {
            return Err(TmslError::InvalidData(
                "meta compress_level must be <= 9".into(),
            ));
        }
        let compress_type = compress_type
            .ok_or_else(|| TmslError::InvalidData("meta missing compress_type".into()))?;
        validate_compress_type(compress_type)?;
        if index_continuous > 1 {
            return Err(TmslError::InvalidData(
                "meta index_continuous must be 0 or 1".into(),
            ));
        }
        if initial_data_segment_size == 0 {
            initial_data_segment_size = data_segment_size;
        }
        if initial_index_segment_size == 0 {
            initial_index_segment_size = index_segment_size;
        }
        if initial_data_segment_size > data_segment_size {
            return Err(TmslError::InvalidData(
                "meta initial_data_segment_size exceeds data_segment_size".into(),
            ));
        }
        if initial_index_segment_size > index_segment_size {
            return Err(TmslError::InvalidData(
                "meta initial_index_segment_size exceeds index_segment_size".into(),
            ));
        }

        Ok(Self {
            data_segment_size,
            index_segment_size,
            compress_level,
            compress_type,
            create_time,
            index_continuous,
            initial_data_segment_size,
            initial_index_segment_size,
            retention_window,
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
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256 * 1024,
            4 * 1024,
            30 * 86400,
        );
        let bytes = meta.to_bytes();

        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.data_segment_size, 64 * 1024 * 1024);
        assert_eq!(parsed.index_segment_size, 4 * 1024 * 1024);
        assert_eq!(parsed.compress_level, 6);
        assert_eq!(parsed.compress_type, crate::compress::COMPRESS_TYPE_ZSTD);
        assert_eq!(parsed.index_continuous, 0);
        assert_eq!(parsed.initial_data_segment_size, 256 * 1024);
        assert_eq!(parsed.initial_index_segment_size, 4 * 1024);
        assert_eq!(parsed.retention_window, 30 * 86400);
    }

    #[test]
    fn test_meta_rejects_invalid_compress_type() {
        let invalid = DataSetMeta::new(1024, 512, 6, 99, 0, 256, 128, 0);

        assert!(matches!(
            DataSetMeta::from_bytes(&invalid.to_bytes()),
            Err(TmslError::InvalidData(_))
        ));
    }

    #[test]
    fn test_meta_rejects_missing_compress_type() {
        let meta_with = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            128,
            0,
        );
        let mut bytes = meta_with.to_bytes();
        let mut off = 8;
        while off + 3 <= bytes.len() {
            let t = bytes[off];
            let len = u16::from_le_bytes([bytes[off + 1], bytes[off + 2]]) as usize;
            let entry_len = 3 + len;
            if t == META_COMPRESS_TYPE {
                bytes.drain(off..off + entry_len);
                let meta_len = (bytes.len() - 8) as u16;
                bytes[6..8].copy_from_slice(&meta_len.to_le_bytes());
                break;
            }
            off += entry_len;
        }

        let result = DataSetMeta::from_bytes(&bytes);

        assert!(matches!(result, Err(TmslError::InvalidData(_))));
    }

    #[test]
    fn test_meta_rejects_missing_required_segment_size() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            128,
            0,
        );
        let mut bytes = meta.to_bytes();
        let mut off = 8;
        while off + 3 <= bytes.len() {
            let t = bytes[off];
            let len = u16::from_le_bytes([bytes[off + 1], bytes[off + 2]]) as usize;
            let entry_len = 3 + len;
            if t == META_DATA_SEGMENT_SIZE {
                bytes.drain(off..off + entry_len);
                let meta_len = (bytes.len() - 8) as u16;
                bytes[6..8].copy_from_slice(&meta_len.to_le_bytes());
                break;
            }
            off += entry_len;
        }

        let result = DataSetMeta::from_bytes(&bytes);

        assert!(matches!(result, Err(TmslError::InvalidData(_))));
    }

    #[test]
    fn test_meta_rejects_invalid_create_parameters() {
        let invalid_compress = DataSetMeta::new(
            1024,
            512,
            10,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            128,
            0,
        );
        assert!(matches!(
            DataSetMeta::from_bytes(&invalid_compress.to_bytes()),
            Err(TmslError::InvalidData(_))
        ));

        let invalid_continuous = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            2,
            256,
            128,
            0,
        );
        assert!(matches!(
            DataSetMeta::from_bytes(&invalid_continuous.to_bytes()),
            Err(TmslError::InvalidData(_))
        ));

        let invalid_initial = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            2048,
            128,
            0,
        );
        assert!(matches!(
            DataSetMeta::from_bytes(&invalid_initial.to_bytes()),
            Err(TmslError::InvalidData(_))
        ));
    }

    #[test]
    fn test_meta_index_continuous_roundtrip() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            1,
            256,
            128,
            0,
        );
        let bytes = meta.to_bytes();

        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.index_continuous, 1);

        // Also verify 0 works
        let meta_zero = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            128,
            0,
        );
        let parsed_zero = DataSetMeta::from_bytes(&meta_zero.to_bytes()).unwrap();
        assert_eq!(parsed_zero.index_continuous, 0);
    }

    #[test]
    fn test_meta_retention_window_roundtrip() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            128,
            7 * 86400,
        );
        let parsed = DataSetMeta::from_bytes(&meta.to_bytes()).unwrap();
        assert_eq!(parsed.retention_window, 7 * 86400);

        // Zero = no limit
        let meta_zero = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            128,
            0,
        );
        let parsed_zero = DataSetMeta::from_bytes(&meta_zero.to_bytes()).unwrap();
        assert_eq!(parsed_zero.retention_window, 0);
    }

    #[test]
    fn test_meta_magic() {
        assert_eq!(&META_MAGIC, b"TMSM");
    }

    #[test]
    fn test_meta_invalid_magic() {
        let mut bytes = DataSetMeta::new(
            100,
            200,
            3,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            100,
            200,
            0,
        )
        .to_bytes();
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

        let meta = DataSetMeta::new(
            1024,
            512,
            9,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            128,
            86400,
        );
        meta.write_to_file(&path).unwrap();
        let loaded = DataSetMeta::read_from_file(&path).unwrap();
        assert_eq!(loaded.data_segment_size, 1024);
        assert_eq!(loaded.index_segment_size, 512);
        assert_eq!(loaded.compress_level, 9);
        assert_eq!(loaded.index_continuous, 0);
        assert_eq!(loaded.initial_data_segment_size, 256);
        assert_eq!(loaded.initial_index_segment_size, 128);
        assert_eq!(loaded.retention_window, 86400);
    }

    #[test]
    fn test_meta_roundtrip_zero_values() {
        // from_bytes backward compat: initial_data_segment_size=0 defaults to data_segment_size.
        // So passing 0 for init_data and data_seg=1 → parsed.initial_data_segment_size = 1.
        let meta = DataSetMeta::new(1, 1, 0, crate::compress::COMPRESS_TYPE_ZSTD, 0, 1, 1, 0);
        let bytes = meta.to_bytes();
        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.initial_data_segment_size, 1);
        assert_eq!(parsed.initial_index_segment_size, 1);
    }

    proptest::proptest! {
        #[test]
        fn proptest_meta_roundtrip(
            data_seg in proptest::num::u64::ANY,
            idx_seg in proptest::num::u64::ANY,
            compress in proptest::num::u8::ANY,
            continuous in proptest::num::u8::ANY,
            init_data in proptest::num::u64::ANY,
            init_idx in proptest::num::u64::ANY,
            retention in proptest::num::u64::ANY,
        ) {
            let data_seg = data_seg.max(1);
            let idx_seg = idx_seg.max(1);
            let compress = compress % 10; // compress_level must be <= 9
            let continuous = continuous % 2;
            // init must be <= segment_size (from_bytes validation)
            let init_data = init_data % data_seg + 1;
            let init_idx = init_idx % idx_seg + 1;
            let meta = DataSetMeta::new(
                data_seg, idx_seg, compress, crate::compress::COMPRESS_TYPE_ZSTD, continuous,
                init_data, init_idx, retention,
            );
            let bytes = meta.to_bytes();
            let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
            assert_eq!(parsed.data_segment_size, data_seg);
            assert_eq!(parsed.index_segment_size, idx_seg);
            assert_eq!(parsed.compress_level, compress);
            assert_eq!(parsed.compress_type, crate::compress::COMPRESS_TYPE_ZSTD);
            assert_eq!(parsed.index_continuous, continuous);
            assert_eq!(parsed.initial_data_segment_size, init_data);
            assert_eq!(parsed.initial_index_segment_size, init_idx);
            assert_eq!(parsed.retention_window, retention);
        }
    }
}
