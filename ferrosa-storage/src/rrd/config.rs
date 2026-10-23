//! Configuration for RRD segmented tables, parsed from table extensions.
//!
//! Three keys:
//!
//! - `rrd.segment_window` — one immutable segment file per window of this
//!   length (e.g. `3h`). A segment's rows all fall inside its window.
//!   **Required**, with no shared default: the window is a property of the
//!   series' write rate, and one value cannot serve a 5 s raw tier and a 1 d
//!   rollup tier. Recommended starting values are in the table below.
//! - `rrd.retain` — a segment whose window ended before `now - rrd.retain` is
//!   unlinked. **Required**, for the same reason; must be at least one window
//!   long, or a segment would be eligible for removal before it had closed.
//! - `rrd.block_bytes` — the unit a segment is written and uploaded in.
//!   Optional, defaulting to [`DEFAULT_BLOCK_BYTES`], and an upper bound rather
//!   than a fixed unit: a table whose entire retention holds less than one
//!   block is one object per segment (see [`RrdConfig::effective_block_bytes`]).
//!
//! Recommended starting points, measured against a 4 KiB filesystem block, the
//! 128 KiB object-store billing unit and the 5 MiB multipart minimum — each row
//! keeps the segment worth being a file and the segment count in the hundreds:
//!
//! | tier | window | segment | retain | segments | pin |
//! |---|---|---|---|---|---|
//! | raw 5 s | 3 h | ~165 KiB | 48 h | 16 | nvme |
//! | 1 m | 3 h | ~16 KiB | 7 d | 56 | nvme |
//! | 5 m | 6 h | ~5.8 KiB | 30 d | 120 | — |
//! | 1 h | 2 d | ~3.8 KiB | 365 d | 182 | — |
//! | 1 d | 60 d | ~4.7 KiB | 3 y | 18 | — |
//!
//! The window arithmetic is in **microseconds**, the unit of a cell timestamp
//! and of `SSTableMetadata::min_timestamp`/`max_timestamp`.

use std::collections::HashMap;
use std::time::Duration;

use crate::size_units::parse_size;
use crate::timeseries::config::parse_duration;

/// Table extension naming the segment window.
pub const SEGMENT_WINDOW_KEY: &str = "rrd.segment_window";
/// Table extension naming the retention horizon.
pub const RETAIN_KEY: &str = "rrd.retain";
/// Table extension naming the write/upload block size.
pub const BLOCK_BYTES_KEY: &str = "rrd.block_bytes";

/// Default write/upload block size for a segmented table.
///
/// Set by measurement, not by taste: 1 MiB is the unit that keeps a raw-tier
/// segment a handful of blocks while staying under the point where a partial
/// block wastes an object's worth of space. Change it here and nowhere else.
pub const DEFAULT_BLOCK_BYTES: u64 = 1024 * 1024;

const MICROS_PER_SECOND: u128 = 1_000_000;

/// Configuration for an RRD segmented table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RrdConfig {
    /// Length of one segment window; one immutable segment file per window.
    pub segment_window: Duration,
    /// Retention horizon measured back from now.
    pub retain: Duration,
    /// Upper bound on the unit a segment is written and uploaded in.
    pub block_bytes: u64,
}

impl RrdConfig {
    /// Parse from a table's extensions map.
    ///
    /// `None` when the table sets no `rrd.*` key at all. `Some(Err(..))` when a
    /// key is present but unusable: a malformed duration or size, a missing
    /// `rrd.segment_window` or `rrd.retain`, a zero window or block, or a
    /// retention shorter than one window. A table cannot be half-segmented, so
    /// every one of those is a schema error rather than a silent fall back to
    /// compaction (which would rewrite every segment on every flush).
    pub fn from_extensions(ext: &HashMap<String, String>) -> Option<Result<Self, String>> {
        if !ext.keys().any(|key| key.starts_with("rrd.")) {
            return None;
        }
        Some(Self::parse_extensions(ext))
    }

    /// Whether `segment_window` is present at all, whatever its value.
    ///
    /// Compaction selection uses this, not [`Self::from_extensions`]: a table
    /// whose window failed to parse must not silently fall back to UCS and
    /// rewrite every segment on every flush. DDL rejects the malformed value up
    /// front, so reaching this with an unparseable window means a hand-edited
    /// schema; refusing to compact is the recoverable half of that.
    pub fn is_segmented(ext: &HashMap<String, String>) -> bool {
        ext.contains_key(SEGMENT_WINDOW_KEY)
    }

    fn parse_extensions(ext: &HashMap<String, String>) -> Result<Self, String> {
        let Some(window_raw) = ext.get(SEGMENT_WINDOW_KEY) else {
            return Err(format!(
                "{SEGMENT_WINDOW_KEY} is required: {} describe a segmented table, and a \
                 retention horizon or block size without a window does not",
                keys_present(ext)
            ));
        };
        let segment_window = parse_duration(window_raw)
            .map_err(|reason| format!("invalid {SEGMENT_WINDOW_KEY} '{window_raw}': {reason}"))?;
        if segment_window.is_zero() {
            return Err(format!("{SEGMENT_WINDOW_KEY} must be positive (non-zero)"));
        }

        let Some(retain_raw) = ext.get(RETAIN_KEY) else {
            return Err(format!(
                "{SEGMENT_WINDOW_KEY} is set but {RETAIN_KEY} is missing: segmented storage \
                 needs a retention horizon, and there is no shared default because it is a \
                 property of the series"
            ));
        };
        let retain = parse_duration(retain_raw)
            .map_err(|reason| format!("invalid {RETAIN_KEY} '{retain_raw}': {reason}"))?;
        if retain.is_zero() {
            return Err(format!("{RETAIN_KEY} must be positive (non-zero)"));
        }
        if retain < segment_window {
            return Err(format!(
                "{RETAIN_KEY} ({retain_raw}) is shorter than {SEGMENT_WINDOW_KEY} \
                 ('{window_raw}'): a segment would expire before its window closed"
            ));
        }

        let block_bytes = match ext.get(BLOCK_BYTES_KEY) {
            Some(raw) => parse_size(raw)
                .map_err(|reason| format!("invalid {BLOCK_BYTES_KEY} '{raw}': {reason}"))?,
            None => DEFAULT_BLOCK_BYTES,
        };
        if block_bytes == 0 {
            return Err(format!("{BLOCK_BYTES_KEY} must be positive (non-zero)"));
        }

        Ok(Self {
            segment_window,
            retain,
            block_bytes,
        })
    }

    /// Length of one window in microseconds.
    pub fn window_micros(&self) -> i64 {
        self.segment_window.as_micros() as i64
    }

    /// Start of the window that contains `timestamp_micros`.
    ///
    /// A timestamp exactly on a window edge belongs to the window that starts
    /// there (`window_start_micros(k * w) == k * w`), not to the one that ended.
    pub fn window_start_micros(&self, timestamp_micros: i64) -> i64 {
        let window = self.window_micros();
        timestamp_micros - timestamp_micros.rem_euclid(window)
    }

    /// Exclusive end of the window that contains `timestamp_micros`.
    pub fn window_end_micros(&self, timestamp_micros: i64) -> i64 {
        self.window_start_micros(timestamp_micros) + self.window_micros()
    }

    /// Stable identity of the segment holding `window_start_micros`.
    ///
    /// Derived from the window start alone, so two rows in the same window can
    /// never produce two segments and a closed window can never be re-opened
    /// under a fresh name.
    pub fn segment_id(&self, window_start_micros: i64) -> String {
        format!("rrd-{window_start_micros}")
    }

    /// Timestamps strictly below this are outside retention at `now_micros`.
    pub fn retention_floor_micros(&self, now_micros: i64) -> i64 {
        now_micros - self.retain.as_micros() as i64
    }

    /// The unit a segment is written and uploaded in, given how many bytes this
    /// table holds inside its retention.
    ///
    /// `block_bytes` is a ceiling, not a fixed unit. A table whose whole
    /// retention is smaller than one block is written as one object per
    /// segment, because padding it up to a block it can never fill buys nothing
    /// and wastes the difference. Only a tier that holds more than `block_bytes`
    /// within its retention uses the full block.
    ///
    /// `total_retained_bytes == 0` (nothing retained yet) clamps against
    /// nothing, so the configured ceiling applies.
    pub fn effective_block_bytes(&self, total_retained_bytes: u64) -> u64 {
        if total_retained_bytes == 0 {
            self.block_bytes
        } else {
            self.block_bytes.min(total_retained_bytes)
        }
    }

    /// Convert seconds since the epoch to the microsecond clock used by
    /// extension arithmetic and cell timestamps.
    pub fn micros_from_secs(secs: i64) -> i64 {
        (secs as i128 * MICROS_PER_SECOND as i128) as i64
    }
}

/// The `rrd.*` keys a table did set, for an error message that says what it has.
fn keys_present(ext: &HashMap<String, String>) -> String {
    let mut keys: Vec<&str> = ext
        .keys()
        .filter(|key| key.starts_with("rrd."))
        .map(|key| key.as_str())
        .collect();
    keys.sort_unstable();
    keys.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ext(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn parsed(config: Option<Result<RrdConfig, String>>) -> RrdConfig {
        config.expect("rrd keys present").expect("valid rrd config")
    }

    #[test]
    fn rrd_config_parses_segment_window_and_retain() {
        let config = parsed(RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "5m"),
            (RETAIN_KEY, "2d"),
        ])));
        assert_eq!(config.segment_window, Duration::from_secs(300));
        assert_eq!(config.retain, Duration::from_secs(2 * 86_400));
    }

    #[test]
    fn rrd_config_absent_when_no_rrd_key_is_set() {
        assert!(RrdConfig::from_extensions(&ext(&[
            ("compaction.class", "UnifiedCompactionStrategy"),
            ("storage.pin", "nvme"),
        ]))
        .is_none());
        assert!(RrdConfig::from_extensions(&HashMap::new()).is_none());
        assert!(!RrdConfig::is_segmented(&HashMap::new()));
    }

    #[test]
    fn rrd_config_malformed_segment_window_fails_loud() {
        let reason = RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "five minutes"),
            (RETAIN_KEY, "1h"),
        ]))
        .expect("rrd keys present")
        .expect_err("a malformed window must not fall back to a default");
        assert!(
            reason.contains(SEGMENT_WINDOW_KEY),
            "error must name the offending key, got: {reason}"
        );
        assert!(
            reason.contains("five minutes"),
            "error must quote the offending value, got: {reason}"
        );
    }

    #[test]
    fn rrd_config_malformed_retain_fails_loud() {
        let reason =
            RrdConfig::from_extensions(&ext(&[(SEGMENT_WINDOW_KEY, "5m"), (RETAIN_KEY, "2 days")]))
                .expect("rrd keys present")
                .expect_err("a malformed retain must not fall back to a default");
        assert!(
            reason.contains(RETAIN_KEY),
            "error must name the offending key, got: {reason}"
        );
    }

    #[test]
    fn rrd_config_window_without_retain_fails_loud() {
        let reason = RrdConfig::from_extensions(&ext(&[(SEGMENT_WINDOW_KEY, "5m")]))
            .expect("rrd keys present")
            .expect_err("a segment window without retention is not segmented storage");
        assert!(reason.contains(RETAIN_KEY), "got: {reason}");
    }

    #[test]
    fn rrd_config_retain_without_window_fails_loud() {
        let reason = RrdConfig::from_extensions(&ext(&[(RETAIN_KEY, "2d")]))
            .expect("rrd keys present")
            .expect_err("retention without a segment window is not segmented storage");
        assert!(reason.contains(SEGMENT_WINDOW_KEY), "got: {reason}");
    }

    #[test]
    fn rrd_config_rejects_retention_shorter_than_one_window() {
        let reason =
            RrdConfig::from_extensions(&ext(&[(SEGMENT_WINDOW_KEY, "1h"), (RETAIN_KEY, "30m")]))
                .expect("rrd keys present")
                .expect_err("a segment cannot expire before its window closed");
        assert!(reason.contains(RETAIN_KEY), "got: {reason}");
    }

    #[test]
    fn rrd_config_rejects_zero_window() {
        let reason =
            RrdConfig::from_extensions(&ext(&[(SEGMENT_WINDOW_KEY, "0m"), (RETAIN_KEY, "1h")]))
                .expect("rrd keys present")
                .expect_err("a zero window has no segments");
        assert!(reason.contains(SEGMENT_WINDOW_KEY), "got: {reason}");
    }

    /// Invariant 1, the pure half: every timestamp lands in exactly one window,
    /// and a timestamp on an edge belongs to the window that starts there.
    #[test]
    fn window_assignment_is_total_and_edges_belong_to_the_new_window() {
        let config = parsed(RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "5m"),
            (RETAIN_KEY, "1h"),
        ])));
        let w = config.window_micros();
        for k in -3i64..4 {
            let edge = k * w;
            assert_eq!(
                config.window_start_micros(edge),
                edge,
                "a row exactly on window edge {edge} starts that window"
            );
            assert_eq!(config.window_start_micros(edge - 1), edge - w);
            assert_eq!(config.window_start_micros(edge + 1), edge);
            assert_eq!(config.window_end_micros(edge), edge + w);
        }
        assert_eq!(config.window_start_micros(0), 0);
        assert_eq!(config.window_start_micros(-1), -w);
    }

    /// Invariant 1 and 2, the identity half: the segment for a window is named
    /// from the window start alone, so rows in one window cannot split across
    /// segments and a closed segment cannot reappear under a new name.
    #[test]
    fn segment_identity_depends_only_on_the_window_start() {
        let config = parsed(RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "5m"),
            (RETAIN_KEY, "1h"),
        ])));
        let w = config.window_micros();
        let base = 1_700_000_000_000_000i64;
        let start = config.window_start_micros(base);
        assert!(
            (start..start + w).contains(&base),
            "the window that contains {base} must start at {start}"
        );
        for offset in [0i64, 1, w / 2, w - 1] {
            assert_eq!(
                config.segment_id(config.window_start_micros(start + offset)),
                config.segment_id(start),
                "every timestamp {offset}µs into the window lands in that window's segment"
            );
        }
        assert_ne!(config.segment_id(start), config.segment_id(start + w));
        assert_eq!(config.segment_id(start), format!("rrd-{start}"));
    }

    #[test]
    fn retention_floor_is_now_minus_retain() {
        let config = parsed(RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "5m"),
            (RETAIN_KEY, "2d"),
        ])));
        let now = RrdConfig::micros_from_secs(1_700_000_000);
        assert_eq!(
            config.retention_floor_micros(now),
            now - 2 * 86_400 * 1_000_000
        );
    }

    // ── The parameters must be the configured ones, never a constant. ──────
    //
    // Every test below uses a deliberately non-default window, retention or
    // block size: a test that only passes at 5m/2d/1 MiB would be proving the
    // constant, not the configuration.

    #[test]
    fn segment_boundaries_follow_a_non_default_window() {
        let config = parsed(RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "17m"),
            (RETAIN_KEY, "4h"),
        ])));
        let seventeen_minutes = 17 * 60 * 1_000_000i64;
        assert_eq!(config.window_micros(), seventeen_minutes);
        for k in -2i64..3 {
            let edge = k * seventeen_minutes;
            assert_eq!(config.window_start_micros(edge), edge);
            assert_eq!(
                config.window_start_micros(edge - 1),
                edge - seventeen_minutes
            );
            assert_eq!(
                config.window_start_micros(edge + seventeen_minutes - 1),
                edge
            );
        }
        // A value that would land on a 5m edge but not on a 17m one.
        assert_eq!(
            config.window_start_micros(5 * 60 * 1_000_000),
            0,
            "a 5-minute edge is inside the first 17-minute window"
        );
    }

    #[test]
    fn retention_follows_a_non_default_value() {
        let config = parsed(RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "17m"),
            (RETAIN_KEY, "4h"),
        ])));
        let four_hours = 4 * 3_600 * 1_000_000i64;
        let now = 1_700_000_123_456_789i64;
        assert_eq!(config.retention_floor_micros(now), now - four_hours);
        assert_eq!(config.retain, Duration::from_secs(4 * 3_600));
    }

    #[test]
    fn block_bytes_parses_a_non_default_value() {
        let config = parsed(RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "17m"),
            (RETAIN_KEY, "4h"),
            (BLOCK_BYTES_KEY, "64KiB"),
        ])));
        assert_eq!(config.block_bytes, 64 * 1024);
        // 64 KiB is the configured ceiling; a retention that holds less than
        // that is one object per segment, not a padded 64 KiB block.
        assert_eq!(config.effective_block_bytes(1 << 20), 64 * 1024);
        assert_eq!(config.effective_block_bytes(20_000), 20_000);
        // An empty table has no retained bytes to clamp with.
        assert_eq!(config.effective_block_bytes(0), 64 * 1024);
    }

    #[test]
    fn block_bytes_defaults_when_absent() {
        let config = parsed(RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "17m"),
            (RETAIN_KEY, "4h"),
        ])));
        assert_eq!(config.block_bytes, DEFAULT_BLOCK_BYTES);
    }

    #[test]
    fn malformed_block_bytes_fails_loud() {
        let reason = RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "17m"),
            (RETAIN_KEY, "4h"),
            (BLOCK_BYTES_KEY, "64 kilobytes"),
        ]))
        .expect("rrd keys present")
        .expect_err("a malformed size must not fall back to the default");
        assert!(reason.contains(BLOCK_BYTES_KEY), "got: {reason}");
    }

    #[test]
    fn zero_block_bytes_fails_loud() {
        let reason = RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "17m"),
            (RETAIN_KEY, "4h"),
            (BLOCK_BYTES_KEY, "0"),
        ]))
        .expect("rrd keys present")
        .expect_err("a zero block has no write unit");
        assert!(reason.contains(BLOCK_BYTES_KEY), "got: {reason}");
    }
}
