//! Endian-aware byte conversion utilities.
//!
//! All multi-byte values in timslite are stored in little-endian format.

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
}
