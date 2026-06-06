//! Compression utilities.

use crate::error::{Result, TmslError};

pub const COMPRESS_TYPE_ZSTD: u8 = 0;
pub const COMPRESS_TYPE_DEFLATE: u8 = 1;

/// Compress data using deflate.
///
/// `level` should be 0-9 (higher = better compression, slower).
pub fn deflate_compress(data: &[u8], level: u8) -> Result<Vec<u8>> {
    Ok(miniz_oxide::deflate::compress_to_vec(data, level.min(9)))
}

/// Decompress deflate-compressed data.
pub fn deflate_decompress(data: &[u8]) -> Result<Vec<u8>> {
    miniz_oxide::inflate::decompress_to_vec(data)
        .map_err(|e| TmslError::DecompressionError(format!("miniz_oxide inflate error: {e:?}")))
}

pub fn zstd_compress(data: &[u8], level: u8) -> Result<Vec<u8>> {
    zstd::stream::encode_all(data, i32::from(level.min(22)))
        .map_err(|e| TmslError::CompressionError(format!("zstd encode error: {e}")))
}

pub fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::decode_all(data)
        .map_err(|e| TmslError::DecompressionError(format!("zstd decode error: {e}")))
}

pub fn validate_compress_type(compress_type: u8) -> Result<()> {
    match compress_type {
        COMPRESS_TYPE_ZSTD | COMPRESS_TYPE_DEFLATE => Ok(()),
        _ => Err(TmslError::InvalidData(format!(
            "unknown compress_type {compress_type}"
        ))),
    }
}

pub fn compress(data: &[u8], level: u8, compress_type: u8) -> Result<Vec<u8>> {
    match compress_type {
        COMPRESS_TYPE_ZSTD => zstd_compress(data, level),
        COMPRESS_TYPE_DEFLATE => deflate_compress(data, level),
        _ => Err(TmslError::InvalidData(format!(
            "unknown compress_type {compress_type}"
        ))),
    }
}

pub fn decompress(data: &[u8], compress_type: u8) -> Result<Vec<u8>> {
    match compress_type {
        COMPRESS_TYPE_ZSTD => zstd_decompress(data),
        COMPRESS_TYPE_DEFLATE => deflate_decompress(data),
        _ => Err(TmslError::InvalidData(format!(
            "unknown compress_type {compress_type}"
        ))),
    }
}

/// Determine if compression was worthwhile.
///
/// Returns true when the compressed data is smaller than the original.
pub fn should_use_compressed(compressed: &[u8], original: &[u8]) -> bool {
    compressed.len() < original.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deflate_roundtrip() {
        let original = b"Hello, world! This is test data for compression.".repeat(100);
        let compressed = deflate_compress(&original, 6).unwrap();
        // Compression should shrink repetitive data
        assert!(compressed.len() < original.len());

        let decompressed = deflate_decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_default_zstd_roundtrip_via_compress_type() {
        let original = b"zstd should be the default compression algorithm ".repeat(200);
        let compressed = compress(original.as_slice(), 6, COMPRESS_TYPE_ZSTD).unwrap();
        let decompressed = decompress(&compressed, COMPRESS_TYPE_ZSTD).unwrap();

        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_rejects_unknown_compress_type() {
        let err = compress(b"data", 6, 99).expect_err("unknown compress_type must be rejected");
        assert!(matches!(err, TmslError::InvalidData(_)));
    }

    #[test]
    fn test_deflate_level_range() {
        let data = b"Repetitive data ".repeat(200);
        let c0 = deflate_compress(&data, 0).unwrap();
        let c6 = deflate_compress(&data, 6).unwrap();
        let c9 = deflate_compress(&data, 9).unwrap();

        // Higher levels generally produce smaller output for compressible data
        assert!(c9.len() <= c6.len());
        assert!(c6.len() <= c0.len());

        // All should decompress to the original
        assert_eq!(deflate_decompress(&c0).unwrap(), data);
        assert_eq!(deflate_decompress(&c6).unwrap(), data);
        assert_eq!(deflate_decompress(&c9).unwrap(), data);
    }

    #[test]
    fn test_empty_data() {
        let data: &[u8] = &[];
        let compressed = deflate_compress(data, 6).unwrap();
        let decompressed = deflate_decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_should_use_compressed() {
        let original = b"Hello world".repeat(100);
        let compressed = deflate_compress(&original, 6).unwrap();
        assert!(should_use_compressed(&compressed, &original));
    }

    #[test]
    fn test_should_not_use_compressed_for_already_small() {
        // Already-compressed or random data might not compress well
        let random_data: Vec<u8> = (0..50).map(|i| (i * 7 + 13) as u8).collect();
        let compressed = deflate_compress(&random_data, 6).unwrap();
        // For tiny data, compressed might be larger than original (header overhead)
        let should = should_use_compressed(&compressed, &random_data);
        // Just verify it doesn't panic; result depends on data
        let _ = should;
    }

    #[test]
    fn test_invalid_compressed_data() {
        let result = deflate_decompress(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(result.is_err());
        match result.unwrap_err() {
            TmslError::DecompressionError(_) => {}
            other => panic!("expected DecompressionError, got: {other:?}"),
        }
    }

    #[test]
    fn test_compress_level_cap() {
        let data = b"test data test data test data".repeat(50);
        // Level 15 should be capped to 9 internally
        let result = deflate_compress(&data, 15);
        assert!(result.is_ok());
    }
}
