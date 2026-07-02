//! Endian-aware byte conversion utilities.
//!
//! All multi-byte values in timslite are stored in little-endian format.

pub const PATH_COMPONENT_MAX_LEN: usize = 255;

pub fn is_path_safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= PATH_COMPONENT_MAX_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Read a little-endian u16 from a 2-byte buffer.
#[inline]
pub fn read_u16_le(buf: &[u8; 2]) -> u16 {
    u16::from_le_bytes(*buf)
}

/// Write a little-endian u16 into a 2-byte buffer.
#[inline]
pub fn write_u16_le(buf: &mut [u8; 2], v: u16) {
    buf.copy_from_slice(&v.to_le_bytes());
}

/// Read a little-endian u32 from a 4-byte buffer.
#[inline]
pub fn read_u32_le(buf: &[u8; 4]) -> u32 {
    u32::from_le_bytes(*buf)
}

/// Write a little-endian u32 into a 4-byte buffer.
#[inline]
pub fn write_u32_le(buf: &mut [u8; 4], v: u32) {
    buf.copy_from_slice(&v.to_le_bytes());
}

/// Read a little-endian i64 from an 8-byte buffer.
#[inline]
pub fn read_i64_le(buf: &[u8; 8]) -> i64 {
    i64::from_le_bytes(*buf)
}

/// Write a little-endian i64 into an 8-byte buffer.
#[inline]
pub fn write_i64_le(buf: &mut [u8; 8], v: i64) {
    buf.copy_from_slice(&v.to_le_bytes());
}

/// Read a little-endian u64 from an 8-byte buffer.
#[inline]
pub fn read_u64_le(buf: &[u8; 8]) -> u64 {
    u64::from_le_bytes(*buf)
}

/// Write a little-endian u64 into an 8-byte buffer.
#[inline]
pub fn write_u64_le(buf: &mut [u8; 8], v: u64) {
    buf.copy_from_slice(&v.to_le_bytes());
}

// ─── Mmap convenience helpers ───────────────────────────────

/// Read a u16 from an mmap slice at `pos`.
pub fn read_u16_from_mmap(mmap: &[u8], pos: usize) -> u16 {
    let buf: [u8; 2] = mmap[pos..pos + 2]
        .try_into()
        .expect("read_u16_from_mmap: slice bounds");
    read_u16_le(&buf)
}

/// Read a u32 from an mmap slice at `pos`.
pub fn read_u32_from_mmap(mmap: &[u8], pos: usize) -> u32 {
    let buf: [u8; 4] = mmap[pos..pos + 4]
        .try_into()
        .expect("read_u32_from_mmap: slice bounds");
    read_u32_le(&buf)
}

/// Read an i64 from an mmap slice at `pos`.
pub fn read_i64_from_mmap(mmap: &[u8], pos: usize) -> i64 {
    let buf: [u8; 8] = mmap[pos..pos + 8]
        .try_into()
        .expect("read_i64_from_mmap: slice bounds");
    read_i64_le(&buf)
}

/// Read a u64 from an mmap slice at `pos`.
pub fn read_u64_from_mmap(mmap: &[u8], pos: usize) -> u64 {
    let buf: [u8; 8] = mmap[pos..pos + 8]
        .try_into()
        .expect("read_u64_from_mmap: slice bounds");
    read_u64_le(&buf)
}

/// Write a u16 into an mmap slice at `pos`.
pub fn write_u16_to_mmap(mmap: &mut [u8], pos: usize, v: u16) {
    let buf = v.to_le_bytes();
    mmap[pos..pos + 2].copy_from_slice(&buf);
}

/// Write a u32 into an mmap slice at `pos`.
pub fn write_u32_to_mmap(mmap: &mut [u8], pos: usize, v: u32) {
    let buf = v.to_le_bytes();
    mmap[pos..pos + 4].copy_from_slice(&buf);
}

/// Write an i64 into an mmap slice at `pos`.
pub fn write_i64_to_mmap(mmap: &mut [u8], pos: usize, v: i64) {
    let buf = v.to_le_bytes();
    mmap[pos..pos + 8].copy_from_slice(&buf);
}

/// Write a u64 into an mmap slice at `pos`.
pub fn write_u64_to_mmap(mmap: &mut [u8], pos: usize, v: u64) {
    let buf = v.to_le_bytes();
    mmap[pos..pos + 8].copy_from_slice(&buf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u16_roundtrip() {
        let mut buf = [0u8; 2];
        write_u16_le(&mut buf, 0x1234);
        assert_eq!(read_u16_le(&buf), 0x1234);
    }

    #[test]
    fn test_u32_roundtrip() {
        let mut buf = [0u8; 4];
        write_u32_le(&mut buf, 0x1234_5678);
        assert_eq!(read_u32_le(&buf), 0x1234_5678);
    }

    #[test]
    fn test_i64_roundtrip() {
        let mut buf = [0u8; 8];
        write_i64_le(&mut buf, -12_345_678_901_i64);
        assert_eq!(read_i64_le(&buf), -12_345_678_901_i64);
    }

    #[test]
    fn test_u64_roundtrip() {
        let mut buf = [0u8; 8];
        write_u64_le(&mut buf, 0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(read_u64_le(&buf), 0xDEAD_BEEF_CAFE_BABE);
    }

    #[test]
    fn test_mmap_helpers() {
        let mut buf = [0u8; 16];
        write_u16_to_mmap(&mut buf, 0, 0x1234);
        write_u32_to_mmap(&mut buf, 2, 0x1234_5678);
        write_i64_to_mmap(&mut buf, 6, -999_i64);

        assert_eq!(read_u16_from_mmap(&buf, 0), 0x1234);
        assert_eq!(read_u32_from_mmap(&buf, 2), 0x1234_5678);
        assert_eq!(read_i64_from_mmap(&buf, 6), -999);
    }

    #[test]
    fn test_little_endian_order() {
        let mut buf = [0u8; 2];
        write_u16_le(&mut buf, 0x0102);
        assert_eq!(buf[0], 0x02); // least significant byte first
        assert_eq!(buf[1], 0x01);
    }

    // ─── is_path_safe_component tests ─────────────────────────

    #[test]
    fn test_path_safe_alphanumeric() {
        assert!(is_path_safe_component("abc123"));
    }

    #[test]
    fn test_path_safe_hyphens() {
        assert!(is_path_safe_component("my-dataset"));
    }

    #[test]
    fn test_path_safe_underscores() {
        assert!(is_path_safe_component("my_dataset"));
    }

    #[test]
    fn test_path_safe_mixed() {
        assert!(is_path_safe_component("data-set_01"));
    }

    #[test]
    fn test_path_safe_single_char() {
        assert!(is_path_safe_component("a"));
    }

    #[test]
    fn test_path_safe_empty_rejected() {
        assert!(!is_path_safe_component(""));
    }

    #[test]
    fn test_path_safe_too_long_rejected() {
        let long_name = "a".repeat(PATH_COMPONENT_MAX_LEN + 1);
        assert!(!is_path_safe_component(&long_name));
    }

    #[test]
    fn test_path_safe_exact_max_len() {
        let max_name = "a".repeat(PATH_COMPONENT_MAX_LEN);
        assert!(is_path_safe_component(&max_name));
    }

    #[test]
    fn test_path_safe_dot_rejected() {
        assert!(!is_path_safe_component("data.set"));
    }

    #[test]
    fn test_path_safe_slash_rejected() {
        assert!(!is_path_safe_component("data/set"));
    }

    #[test]
    fn test_path_safe_space_rejected() {
        assert!(!is_path_safe_component("data set"));
    }

    // ─── endian roundtrip boundary values ─────────────────────

    #[test]
    fn test_u16_roundtrip_zero() {
        let mut buf = [0u8; 2];
        write_u16_le(&mut buf, 0);
        assert_eq!(read_u16_le(&buf), 0);
    }

    #[test]
    fn test_u16_roundtrip_max() {
        let mut buf = [0u8; 2];
        write_u16_le(&mut buf, u16::MAX);
        assert_eq!(read_u16_le(&buf), u16::MAX);
    }

    #[test]
    fn test_u32_roundtrip_zero() {
        let mut buf = [0u8; 4];
        write_u32_le(&mut buf, 0);
        assert_eq!(read_u32_le(&buf), 0);
    }

    #[test]
    fn test_u32_roundtrip_max() {
        let mut buf = [0u8; 4];
        write_u32_le(&mut buf, u32::MAX);
        assert_eq!(read_u32_le(&buf), u32::MAX);
    }

    #[test]
    fn test_i64_roundtrip_zero() {
        let mut buf = [0u8; 8];
        write_i64_le(&mut buf, 0);
        assert_eq!(read_i64_le(&buf), 0);
    }

    #[test]
    fn test_i64_roundtrip_max() {
        let mut buf = [0u8; 8];
        write_i64_le(&mut buf, i64::MAX);
        assert_eq!(read_i64_le(&buf), i64::MAX);
    }

    #[test]
    fn test_i64_roundtrip_min() {
        let mut buf = [0u8; 8];
        write_i64_le(&mut buf, i64::MIN);
        assert_eq!(read_i64_le(&buf), i64::MIN);
    }

    #[test]
    fn test_i64_roundtrip_negative_one() {
        let mut buf = [0u8; 8];
        write_i64_le(&mut buf, -1);
        assert_eq!(read_i64_le(&buf), -1);
    }

    #[test]
    fn test_u64_roundtrip_zero() {
        let mut buf = [0u8; 8];
        write_u64_le(&mut buf, 0);
        assert_eq!(read_u64_le(&buf), 0);
    }

    #[test]
    fn test_u64_roundtrip_max() {
        let mut buf = [0u8; 8];
        write_u64_le(&mut buf, u64::MAX);
        assert_eq!(read_u64_le(&buf), u64::MAX);
    }

    // ─── mmap helpers roundtrip at various positions ──────────

    #[test]
    fn test_mmap_u16_at_various_positions() {
        let mut buf = vec![0u8; 64];
        for pos in [0, 2, 10, 32, 62] {
            write_u16_to_mmap(&mut buf, pos, 0xBEEF);
            assert_eq!(read_u16_from_mmap(&buf, pos), 0xBEEF);
        }
    }

    #[test]
    fn test_mmap_u32_at_various_positions() {
        let mut buf = vec![0u8; 64];
        for pos in [0, 4, 12, 32, 60] {
            write_u32_to_mmap(&mut buf, pos, 0xCAFE_BABE);
            assert_eq!(read_u32_from_mmap(&buf, pos), 0xCAFE_BABE);
        }
    }

    #[test]
    fn test_mmap_i64_at_various_positions() {
        let mut buf = vec![0u8; 64];
        for pos in [0, 8, 16, 32, 56] {
            write_i64_to_mmap(&mut buf, pos, -42_000_000_000_i64);
            assert_eq!(read_i64_from_mmap(&buf, pos), -42_000_000_000_i64);
        }
    }

    #[test]
    fn test_mmap_u64_roundtrip() {
        let mut buf = vec![0u8; 64];
        write_u64_to_mmap(&mut buf, 0, 0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(read_u64_from_mmap(&buf, 0), 0xDEAD_BEEF_CAFE_BABE);
    }

    #[test]
    fn test_mmap_u64_at_various_positions() {
        let mut buf = vec![0u8; 64];
        for pos in [0, 8, 16, 32, 56] {
            write_u64_to_mmap(&mut buf, pos, 0x1234_5678_9ABC_DEF0);
            assert_eq!(read_u64_from_mmap(&buf, pos), 0x1234_5678_9ABC_DEF0);
        }
    }

    #[test]
    fn test_mmap_boundary_u32_max() {
        let mut buf = vec![0u8; 8];
        write_u32_to_mmap(&mut buf, 0, u32::MAX);
        assert_eq!(read_u32_from_mmap(&buf, 0), u32::MAX);
    }

    #[test]
    fn test_mmap_boundary_i64_min_max() {
        let mut buf = vec![0u8; 16];
        write_i64_to_mmap(&mut buf, 0, i64::MIN);
        write_i64_to_mmap(&mut buf, 8, i64::MAX);
        assert_eq!(read_i64_from_mmap(&buf, 0), i64::MIN);
        assert_eq!(read_i64_from_mmap(&buf, 8), i64::MAX);
    }
}
