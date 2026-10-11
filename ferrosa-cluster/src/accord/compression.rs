//! Tunable, startup-resolved compression for the Accord Apply **region**.
//!
//! The Apply frame's region (see [`ferrosa_net::protocol::encode_accord_apply_v2_region`])
//! may be compressed before it goes on the wire. Compression trades bytes for CPU: it
//! shrinks the peer's transport term but adds compress/decompress time, so whether it
//! is a net win depends on whether that term is byte-bound or CPU/deserialize-bound —
//! it can be a LOSS. On a real (non-loopback) network a bulk Apply is byte-bound, so the
//! shipped default is **`zstd`**: the measured matrix (PROFILE.md) has zstd winning on
//! BOTH axes — the best ratio AND the lowest codec CPU of the three codecs. An operator
//! who wants the pre-compression wire sets `FERROSA_ACCORD_COMPRESSION=none`.
//!
//! Whatever we configure is still the INTERSECTION of that and what the peer can
//! decode (see [`AccordTransport::supports_accord_apply_region_compressed`]): a peer
//! that advertises only the uncompressed region form is never sent a compressed body.
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
//! | `FERROSA_ACCORD_COMPRESSION` | codec: `none`/`lz4`/`snappy`/`zstd` | `zstd` |
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

/// Default codec: `zstd`.
///
/// Chosen from the measured matrix (PROFILE.md): zstd wins on BOTH axes — the best
/// ratio and the lowest codec CPU of the three — so it is the right default for a
/// byte-bound bulk Apply on a real network. `FERROSA_ACCORD_COMPRESSION=none` restores
/// the pre-compression wire exactly.
pub const DEFAULT_CODEC: RegionCodec = RegionCodec::Zstd;
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
    fn codec_resolution_is_case_insensitive_and_defaults_to_zstd() {
        assert_eq!(resolve_codec(None), RegionCodec::Zstd);
        assert_eq!(resolve_codec(Some("")), RegionCodec::Zstd);
        assert_eq!(resolve_codec(Some("none")), RegionCodec::None);
        assert_eq!(resolve_codec(Some("LZ4")), RegionCodec::Lz4);
        assert_eq!(resolve_codec(Some("snappy")), RegionCodec::Snap);
        assert_eq!(resolve_codec(Some("snap")), RegionCodec::Snap);
        assert_eq!(resolve_codec(Some("zstd")), RegionCodec::Zstd);
    }

    /// An unknown codec must NEVER be silently accepted; it falls back to the documented
    /// default (`zstd`) so an operator who mistypes gets the shipped codec, not one they
    /// did not name.
    #[test]
    fn an_unknown_codec_falls_back_to_the_documented_default_not_to_an_arbitrary_codec() {
        assert_eq!(resolve_codec(Some("brotli")), DEFAULT_CODEC);
        assert_eq!(resolve_codec(Some("gzip")), DEFAULT_CODEC);
        assert_eq!(resolve_codec(Some("0")), DEFAULT_CODEC);
        assert_eq!(DEFAULT_CODEC, RegionCodec::Zstd);
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
    fn the_default_configuration_compresses_with_zstd() {
        let cfg = resolve_region_compression(None, None, None, None);
        assert_eq!(
            cfg.codec,
            RegionCodec::Zstd,
            "the shipped default is zstd (measured best on ratio AND cost)"
        );
        assert!(
            !cfg.is_none(),
            "an operator who sets nothing gets compression"
        );
        assert_eq!(cfg.level, DEFAULT_LEVEL);
        assert_eq!(cfg.block_bytes, DEFAULT_BLOCK_BYTES);
        assert_eq!(cfg.min_bytes, DEFAULT_MIN_BYTES);
    }

    /// The escape hatch: an operator can restore the pre-compression wire byte for byte.
    #[test]
    fn an_explicit_none_restores_the_uncompressed_wire() {
        let cfg = resolve_region_compression(Some("none"), None, None, None);
        assert!(cfg.is_none());
        assert_eq!(cfg.codec, RegionCodec::None);
    }

    /// PROOF that the shipped default actually SHRINKS the bytes on the wire.
    ///
    /// This is the test the compression tunable shipped without: nothing in the tree
    /// ever encoded a region through a real codec and checked the frame shrank, which is
    /// how a previous change could "negotiate compression" yet leave it never applied.
    /// This drives the SAME public encoder the coordinator calls, with the SAME config
    /// the resolver returns when an operator sets nothing, and asserts on the DECODED view.
    #[test]
    fn the_shipped_default_actually_shrinks_the_region_on_a_round_trip() {
        use ferrosa_net::protocol::{
            decode_accord_apply_v2_region, encode_accord_apply_v2_region_with_compression,
            AccordTxnId,
        };

        let cfg = resolve_region_compression(None, None, None, None);
        let txn = AccordTxnId {
            epoch: 1,
            time: 42,
            seq: 0,
            node: 7,
        };

        // A payload shaped like a pgbench write-set: many entries with a repetitive body,
        // region comfortably above the 64 KiB minimum.
        let entries: Vec<Vec<u8>> = (0..4096u32)
            .map(|i| {
                let mut row = format!("aid={i:08}|bid=1|abalance=0|filler=").into_bytes();
                row.extend(std::iter::repeat_n(b'x', 84));
                row
            })
            .collect();
        let uncompressed: usize = entries.iter().map(Vec::len).sum();
        assert!(
            uncompressed >= cfg.min_bytes,
            "test payload must clear the compression minimum"
        );

        let frame = encode_accord_apply_v2_region_with_compression(
            txn,
            entries.iter().map(Vec::as_slice),
            cfg,
        )
        .expect("encode with the shipped default");

        let view = decode_accord_apply_v2_region(&frame).expect("decode the region frame");
        assert_eq!(
            view.compression(),
            RegionCodec::Zstd,
            "the shipped default must be APPLIED to the region, not merely negotiated: a \
             decoded frame reporting None is exactly the 'negotiated but never applied' bug"
        );
        assert!(
            frame.len() < uncompressed,
            "the compressed frame ({} B) must be smaller than the raw region ({} B): the \
             bytes on the wire really shrank",
            frame.len(),
            uncompressed
        );
        // INVARIANT — byte-identity of the write-set as delivered: every entry must come
        // back EXACTLY as it went in.
        assert_eq!(view.len(), entries.len());
        for (index, expected) in entries.iter().enumerate() {
            assert_eq!(
                view.entry(index).expect("entry in range"),
                expected.as_slice(),
                "entry {index} must survive the compress/decompress round trip byte for byte"
            );
        }
        assert_eq!(view.indexed_region_len(), uncompressed as u64);
    }

    /// NEGATIVE CONTROL: below `min_bytes` the frame is sent UNCOMPRESSED, so a small
    /// Apply never pays codec CPU for nothing. The decoded view must report `None`.
    #[test]
    fn a_region_below_the_minimum_is_never_compressed() {
        use ferrosa_net::protocol::{
            decode_accord_apply_v2_region, encode_accord_apply_v2_region_with_compression,
            AccordTxnId,
        };
        let cfg = resolve_region_compression(None, None, None, None);
        let txn = AccordTxnId {
            epoch: 1,
            time: 1,
            seq: 0,
            node: 1,
        };
        let entries = [b"short".to_vec()];
        let frame = encode_accord_apply_v2_region_with_compression(
            txn,
            entries.iter().map(Vec::as_slice),
            cfg,
        )
        .expect("encode");
        let view = decode_accord_apply_v2_region(&frame).expect("decode");
        assert_eq!(view.compression(), RegionCodec::None);
        assert_eq!(view.entry(0).expect("entry"), b"short");
    }

    /// NEGATIVE CONTROL: an incompressible region above the minimum is still sent raw —
    /// compression must NEVER inflate a frame.
    #[test]
    fn an_incompressible_region_is_never_inflated() {
        use ferrosa_net::protocol::{
            decode_accord_apply_v2_region, encode_accord_apply_v2_region_with_compression,
            AccordTxnId,
        };
        let cfg = resolve_region_compression(None, None, None, None);
        let txn = AccordTxnId {
            epoch: 1,
            time: 1,
            seq: 0,
            node: 1,
        };
        // DISTINCT pseudo-random rows (an LCG advanced per byte). Repeating one row would
        // be trivially compressible and would not exercise the "never inflate" fallback.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let entries: Vec<Vec<u8>> = (0..8192)
            .map(|_| {
                (0..96)
                    .map(|_| {
                        state = state
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        (state >> 33) as u8
                    })
                    .collect()
            })
            .collect();
        let uncompressed: usize = entries.iter().map(Vec::len).sum();
        assert!(
            uncompressed >= cfg.min_bytes,
            "the incompressible region must clear the minimum to exercise the fallback"
        );
        let frame = encode_accord_apply_v2_region_with_compression(
            txn,
            entries.iter().map(Vec::as_slice),
            cfg,
        )
        .expect("encode");
        let view = decode_accord_apply_v2_region(&frame).expect("decode");
        assert_eq!(
            view.compression(),
            RegionCodec::None,
            "a codec that cannot shrink the region must leave the frame uncompressed"
        );
        assert_eq!(view.len(), entries.len());
        assert_eq!(view.entry(0).expect("entry"), entries[0].as_slice());
    }
}
