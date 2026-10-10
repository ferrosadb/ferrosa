//! Tunable, startup-resolved compression for the Accord Apply **region**.
//!
//! The Apply frame's region (see [`ferrosa_net::protocol::encode_accord_apply_v2_region`])
//! may be compressed before it goes on the wire. Compression trades bytes for CPU: it
//! shrinks the peer's transport term but adds compress/decompress time, so whether it
//! is a net win depends on whether that term is byte-bound or CPU/deserialize-bound —
//! it can be a LOSS. It is therefore **opt-in and default-`none`**: an operator who sets
//! nothing gets exactly today's behaviour. The effective codec is the INTERSECTION of
//! what we configured and what the peer can decode (see
//! [`AccordTransport::supports_accord_apply_region_compressed`]).
//!
//! The knobs are read once at startup from the environment, in the same shape as the
//! other Accord bounds ([`crate::accord::state_machine::resolve_txn_timeout`]): a value
//! that is set but invalid is logged and the documented default is used — never a
//! silent pick of a codec the operator did not ask for.
//!
//! [`AccordTransport::supports_accord_apply_region_compressed`]:
//! crate::accord::transport::AccordTransport::supports_accord_apply_region_compressed
//!
//! | Env var | Meaning | Default |
//! |---|---|---|
//! | `FERROSA_ACCORD_COMPRESSION` | codec: `none`/`lz4`/`snappy`/`zstd` | `none` |
//! | `FERROSA_ACCORD_COMPRESSION_LEVEL` | codec level where it has one (zstd); ignored by the fixed-level codecs | `3` |
//! | `FERROSA_ACCORD_COMPRESSION_BLOCK_BYTES` | block size the region is compressed in | `262144` (256 KiB) |
//! | `FERROSA_ACCORD_COMPRESSION_MIN_BYTES` | frames smaller than this are sent uncompressed | `65536` (64 KiB) |

use std::sync::OnceLock;

use ferrosa_net::protocol::{RegionCodec, RegionCompression};

/// Codec selection (`none`/`lz4`/`snappy`/`zstd`).
pub const COMPRESSION_ENV: &str = "FERROSA_ACCORD_COMPRESSION";
/// Codec level, where the codec has one (zstd). Ignored by `lz4`/`snappy`/`none`.
pub const COMPRESSION_LEVEL_ENV: &str = "FERROSA_ACCORD_COMPRESSION_LEVEL";
/// Block size (bytes) the region is compressed in.
pub const COMPRESSION_BLOCK_BYTES_ENV: &str = "FERROSA_ACCORD_COMPRESSION_BLOCK_BYTES";
/// Minimum frame size (bytes) below which no compression is attempted.
pub const COMPRESSION_MIN_BYTES_ENV: &str = "FERROSA_ACCORD_COMPRESSION_MIN_BYTES";

/// Default codec: no compression — the pre-compression wire, byte for byte.
pub const DEFAULT_CODEC: RegionCodec = RegionCodec::None;
/// Default zstd level.
pub const DEFAULT_LEVEL: i32 = 3;
/// Default block size (256 KiB).
pub const DEFAULT_BLOCK_BYTES: usize = 256 * 1024;
/// Default minimum frame size to compress (64 KiB).
pub const DEFAULT_MIN_BYTES: usize = 64 * 1024;

/// Parse [`COMPRESSION_ENV`] into a codec.
///
/// Pure, so it is testable without touching the process environment (`set_var` is
/// process-global and racy under parallel tests). An unknown value is reported and the
/// default (`none`) is used — never a codec the operator did not name.
pub fn resolve_codec(raw: Option<&str>) -> RegionCodec {
    match raw {
        None => DEFAULT_CODEC,
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                // Set-but-empty is treated as unset (fly.io clears a var this way).
                return DEFAULT_CODEC;
            }
            match trimmed.to_ascii_lowercase().as_str() {
                "none" => RegionCodec::None,
                "lz4" => RegionCodec::Lz4,
                "snappy" | "snap" => RegionCodec::Snap,
                "zstd" => RegionCodec::Zstd,
                other => {
                    tracing::warn!(
                        value = %other,
                        env = COMPRESSION_ENV,
                        "accord: unknown compression codec; using none"
                    );
                    DEFAULT_CODEC
                }
            }
        }
    }
}

/// Parse [`COMPRESSION_LEVEL_ENV`] into a codec level. Invalid values fall back to the
/// default with a warning.
pub fn resolve_level(raw: Option<&str>) -> i32 {
    match raw {
        None => DEFAULT_LEVEL,
        Some(value) => match value.trim().parse::<i32>() {
            Ok(level) => level,
            Err(_) => {
                tracing::warn!(
                    value = %value,
                    env = COMPRESSION_LEVEL_ENV,
                    "accord: invalid compression level; using the default"
                );
                DEFAULT_LEVEL
            }
        },
    }
}

/// Parse [`COMPRESSION_BLOCK_BYTES_ENV`] into a block size. A non-positive or
/// non-numeric value falls back to the default with a warning (`0` would mean "one
/// block" at the encoder, which is a footgun to configure by accident).
pub fn resolve_block_bytes(raw: Option<&str>) -> usize {
    match raw {
        None => DEFAULT_BLOCK_BYTES,
        Some(value) => match value.trim().parse::<usize>() {
            Ok(bytes) if bytes > 0 => bytes,
            _ => {
                tracing::warn!(
                    value = %value,
                    env = COMPRESSION_BLOCK_BYTES_ENV,
                    "accord: invalid compression block size; using the default"
                );
                DEFAULT_BLOCK_BYTES
            }
        },
    }
}

/// Parse [`COMPRESSION_MIN_BYTES_ENV`] into a minimum-compress size. `0` is valid (always
/// attempt compression); a non-numeric value falls back to the default with a warning.
pub fn resolve_min_bytes(raw: Option<&str>) -> usize {
    match raw {
        None => DEFAULT_MIN_BYTES,
        Some(value) => match value.trim().parse::<usize>() {
            Ok(bytes) => bytes,
            Err(_) => {
                tracing::warn!(
                    value = %value,
                    env = COMPRESSION_MIN_BYTES_ENV,
                    "accord: invalid compression min-bytes; using the default"
                );
                DEFAULT_MIN_BYTES
            }
        },
    }
}

/// Assemble a [`RegionCompression`] from the four raw env values (pure; used by both the
/// live resolver and tests).
pub fn resolve_region_compression(
    codec: Option<&str>,
    level: Option<&str>,
    block_bytes: Option<&str>,
    min_bytes: Option<&str>,
) -> RegionCompression {
    RegionCompression {
        codec: resolve_codec(codec),
        level: resolve_level(level),
        block_bytes: resolve_block_bytes(block_bytes),
        min_bytes: resolve_min_bytes(min_bytes),
    }
}

/// The resolved, process-wide region compression, read from the environment ONCE.
///
/// Startup-resolved like the other Accord knobs: setting the env var and restarting
/// changes behaviour without a recompile. Cached so the hot path is a single pointer
/// load, not four `getenv` calls per fan-out.
pub fn configured_region_compression() -> RegionCompression {
    static CONFIG: OnceLock<RegionCompression> = OnceLock::new();
    *CONFIG.get_or_init(|| {
        resolve_region_compression(
            std::env::var(COMPRESSION_ENV).ok().as_deref(),
            std::env::var(COMPRESSION_LEVEL_ENV).ok().as_deref(),
            std::env::var(COMPRESSION_BLOCK_BYTES_ENV).ok().as_deref(),
            std::env::var(COMPRESSION_MIN_BYTES_ENV).ok().as_deref(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_resolution_is_case_insensitive_and_defaults_to_none() {
        assert_eq!(resolve_codec(None), RegionCodec::None);
        assert_eq!(resolve_codec(Some("")), RegionCodec::None);
        assert_eq!(resolve_codec(Some("none")), RegionCodec::None);
        assert_eq!(resolve_codec(Some("LZ4")), RegionCodec::Lz4);
        assert_eq!(resolve_codec(Some("snappy")), RegionCodec::Snap);
        assert_eq!(resolve_codec(Some("snap")), RegionCodec::Snap);
        assert_eq!(resolve_codec(Some("zstd")), RegionCodec::Zstd);
    }

    /// An unknown codec must NEVER be silently accepted; it falls back to the documented
    /// default (`none`) so an operator who mistypes gets today's wire, not a codec.
    #[test]
    fn an_unknown_codec_falls_back_to_none_not_silently_some_codec() {
        assert_eq!(resolve_codec(Some("brotli")), RegionCodec::None);
        assert_eq!(resolve_codec(Some("gzip")), RegionCodec::None);
        assert_eq!(resolve_codec(Some("0")), RegionCodec::None);
    }

    #[test]
    fn level_block_and_min_resolve_with_documented_defaults() {
        assert_eq!(resolve_level(None), DEFAULT_LEVEL);
        assert_eq!(resolve_level(Some("9")), 9);
        assert_eq!(resolve_level(Some("not-a-number")), DEFAULT_LEVEL);

        assert_eq!(resolve_block_bytes(None), DEFAULT_BLOCK_BYTES);
        assert_eq!(resolve_block_bytes(Some("4096")), 4096);
        assert_eq!(resolve_block_bytes(Some("0")), DEFAULT_BLOCK_BYTES);
        assert_eq!(resolve_block_bytes(Some("-8")), DEFAULT_BLOCK_BYTES);

        assert_eq!(resolve_min_bytes(None), DEFAULT_MIN_BYTES);
        assert_eq!(resolve_min_bytes(Some("0")), 0);
        assert_eq!(resolve_min_bytes(Some("nope")), DEFAULT_MIN_BYTES);
    }

    #[test]
    fn the_default_configuration_is_no_compression() {
        let cfg = resolve_region_compression(None, None, None, None);
        assert!(
            cfg.is_none(),
            "an operator who sets nothing gets no compression"
        );
        assert_eq!(cfg.codec, RegionCodec::None);
    }
}
