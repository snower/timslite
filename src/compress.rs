//! Compression utilities.

use std::io::{Read, Write};

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
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), i32::from(level.min(22)))
        .map_err(|e| TmslError::CompressionError(format!("zstd encode error: {e}")))?;
    encoder
        .include_checksum(true)
        .map_err(|e| TmslError::CompressionError(format!("zstd checksum config error: {e}")))?;
    encoder
        .write_all(data)
        .map_err(|e| TmslError::CompressionError(format!("zstd encode error: {e}")))?;
    encoder
        .finish()
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

/// Decompress data, rejecting output larger than `max_output` bytes.
///
/// Bounds are enforced during decoding so corrupt payloads cannot force an
/// unbounded allocation. Zstd window size is capped as well: the maximum
/// legal stored block is `4 MiB + 12` bytes, whose power-of-two window log is
/// 23, so frames from older writers with a window larger than their tiny
/// output are still accepted up to that block bound.
pub(crate) fn decompress_bounded(
    data: &[u8],
    compress_type: u8,
    max_output: usize,
) -> Result<Vec<u8>> {
    match compress_type {
        COMPRESS_TYPE_ZSTD => {
            let mut decoder = zstd::stream::read::Decoder::new(data)
                .map_err(|e| TmslError::DecompressionError(format!("zstd decode error: {e}")))?;
            decoder
                .window_log_max(zstd_window_log_for_output(max_output))
                .map_err(|e| TmslError::DecompressionError(format!("zstd window error: {e}")))?;
            let limit = (max_output as u64).saturating_add(1);
            let mut out = Vec::new();
            decoder
                .take(limit)
                .read_to_end(&mut out)
                .map_err(|e| TmslError::DecompressionError(format!("zstd decode error: {e}")))?;
            if out.len() > max_output {
                return Err(TmslError::DecompressionError(format!(
                    "zstd output exceeds limit {max_output}"
                )));
            }
            Ok(out)
        }
        COMPRESS_TYPE_DEFLATE => {
            miniz_oxide::inflate::decompress_to_vec_with_limit(data, max_output).map_err(|e| {
                TmslError::DecompressionError(format!("miniz_oxide inflate error: {e:?}"))
            })
        }
        _ => Err(TmslError::InvalidData(format!(
            "unknown compress_type {compress_type}"
        ))),
    }
}

/// Zstd window log cap for bounded decompression.
///
/// `ZSTD_WINDOWLOG_MIN` is 10. The floor at 23 preserves older legitimate
/// frames whose chosen window exceeds their actual (small) output while
/// staying at the largest window needed by a `4 MiB + 12` block.
fn zstd_window_log_for_output(max_output: usize) -> u32 {
    const ZSTD_WINDOW_LOG_MIN: u32 = 10;
    const ZSTD_BLOCK_MAX_WINDOW_LOG: u32 = 23;
    const ZSTD_WINDOW_LOG_MAX: u32 = 31;

    let needed = if max_output <= 1 {
        0
    } else {
        usize::BITS - (max_output - 1).leading_zeros()
    };
    needed
        .max(ZSTD_WINDOW_LOG_MIN)
        .clamp(ZSTD_BLOCK_MAX_WINDOW_LOG, ZSTD_WINDOW_LOG_MAX)
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
    fn test_decompress_bounded_rejects_zstd_output_over_limit() {
        let original = vec![0x5Au8; 4096];
        let compressed = zstd_compress(&original, 6).unwrap();
        let err = decompress_bounded(&compressed, COMPRESS_TYPE_ZSTD, original.len() - 1)
            .expect_err("zstd output above max_output must fail");
        assert!(matches!(err, TmslError::DecompressionError(_)));
    }

    #[test]
    fn test_decompress_bounded_rejects_deflate_output_over_limit() {
        let original = vec![0x5Au8; 4096];
        let compressed = deflate_compress(&original, 6).unwrap();
        let err = decompress_bounded(&compressed, COMPRESS_TYPE_DEFLATE, original.len() - 1)
            .expect_err("deflate output above max_output must fail");
        assert!(matches!(err, TmslError::DecompressionError(_)));
    }

    fn zstd_frame_with_window_log(data: &[u8], window_log: u32) -> Vec<u8> {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        encoder.window_log(window_log).unwrap();
        std::io::Write::write_all(&mut encoder, data).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn test_decompress_bounded_rejects_oversized_zstd_window() {
        // Legal frame header, tiny output, but an advertised 128 MiB window.
        let compressed = zstd_frame_with_window_log(b"tiny zstd window", 27);
        let err = decompress_bounded(&compressed, COMPRESS_TYPE_ZSTD, 64)
            .expect_err("oversized zstd window must be rejected");
        assert!(
            matches!(err, TmslError::DecompressionError(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn test_decompress_bounded_accepts_legacy_window_at_block_max() {
        let original = b"legacy window frame";
        let compressed = zstd_frame_with_window_log(original, 23);
        let decoded = decompress_bounded(&compressed, COMPRESS_TYPE_ZSTD, 64).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decompress_bounded_accepts_exact_limit() {
        for (compress_type, compressed) in [
            (
                COMPRESS_TYPE_ZSTD,
                zstd_compress(&vec![0xA5u8; 4096], 6).unwrap(),
            ),
            (
                COMPRESS_TYPE_DEFLATE,
                deflate_compress(&vec![0xA5u8; 4096], 6).unwrap(),
            ),
        ] {
            let decoded = decompress_bounded(&compressed, compress_type, 4096).unwrap();
            assert_eq!(decoded, vec![0xA5u8; 4096]);
        }
    }

    #[test]
    fn test_default_zstd_roundtrip_via_compress_type() {
        let original = b"zstd should be the default compression algorithm ".repeat(200);
        let compressed = compress(original.as_slice(), 6, COMPRESS_TYPE_ZSTD).unwrap();
        let decompressed = decompress(&compressed, COMPRESS_TYPE_ZSTD).unwrap();

        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_zstd_compress_enables_frame_content_checksum() {
        let original = b"zstd checksum frame header contract ".repeat(200);
        let compressed = zstd_compress(&original, 6).unwrap();

        assert!(compressed.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]));
        let frame_header_descriptor = compressed[4];
        assert_ne!(
            frame_header_descriptor & 0x04,
            0,
            "zstd content checksum flag must be enabled for newly written frames"
        );
        assert_eq!(zstd_decompress(&compressed).unwrap(), original);
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

    #[test]
    fn test_validate_compress_type_valid() {
        assert!(validate_compress_type(COMPRESS_TYPE_ZSTD).is_ok());
        assert!(validate_compress_type(COMPRESS_TYPE_DEFLATE).is_ok());
    }

    #[test]
    fn test_validate_compress_type_invalid() {
        for t in [2u8, 3, 99, 255] {
            let result = validate_compress_type(t);
            assert!(result.is_err(), "type {t} should be rejected");
            assert!(matches!(result.unwrap_err(), TmslError::InvalidData(_)));
        }
    }

    #[test]
    fn test_zstd_roundtrip() {
        let original = b"zstd direct roundtrip test ".repeat(200);
        let compressed = zstd_compress(&original, 6).unwrap();
        assert!(compressed.len() < original.len());
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_compress_decompress_dispatch_deflate() {
        let original = b"deflate dispatch test ".repeat(100);
        let compressed = compress(&original, 6, COMPRESS_TYPE_DEFLATE).unwrap();
        let decompressed = decompress(&compressed, COMPRESS_TYPE_DEFLATE).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_compress_decompress_dispatch_zstd() {
        let original = b"zstd dispatch test ".repeat(100);
        let compressed = compress(&original, 6, COMPRESS_TYPE_ZSTD).unwrap();
        let decompressed = decompress(&compressed, COMPRESS_TYPE_ZSTD).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_decompress_rejects_unknown_type() {
        let result = decompress(b"data", 99);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TmslError::InvalidData(_)));
    }

    #[test]
    fn test_deflate_roundtrip_large_data() {
        let original: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        let compressed = deflate_compress(&original, 6).unwrap();
        let decompressed = deflate_decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_zstd_roundtrip_large_data() {
        let original: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        let compressed = zstd_compress(&original, 3).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_should_use_compressed_returns_false_for_larger() {
        let original = b"ab";
        let compressed = deflate_compress(original, 6).unwrap();
        assert!(!should_use_compressed(&compressed, original));
    }

    #[test]
    fn test_should_use_compressed_returns_false_for_equal() {
        let data = b"equal";
        assert!(!should_use_compressed(data, data));
    }

    #[test]
    fn test_zstd_level_range() {
        let data = b"zstd level test ".repeat(200);
        let c1 = zstd_compress(&data, 1).unwrap();
        let c10 = zstd_compress(&data, 10).unwrap();
        let c22 = zstd_compress(&data, 22).unwrap();

        assert!(c22.len() <= c10.len());
        assert!(c10.len() <= c1.len());

        assert_eq!(zstd_decompress(&c1).unwrap(), data);
        assert_eq!(zstd_decompress(&c10).unwrap(), data);
        assert_eq!(zstd_decompress(&c22).unwrap(), data);
    }

    #[test]
    fn test_compress_type_constants() {
        assert_eq!(COMPRESS_TYPE_ZSTD, 0);
        assert_eq!(COMPRESS_TYPE_DEFLATE, 1);
    }
}
