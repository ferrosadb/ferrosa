//! Configuration for RRD segmented tables, parsed from table extensions.
//!
//! Two keys, both required once either is present:
//!
//! - `rrd.segment_window` — one immutable segment file per window of this
//!   length (e.g. `5m`). A segment's rows all fall inside its window.
//! - `rrd.retain` — a segment whose window ended before `now - rrd.retain` is
//!   unlinked. Must be at least one window long, or a segment would be
//!   eligible for removal before it had ever been closed.
//!
//! The window arithmetic is in **microseconds**, the unit of a cell timestamp
//! and of `SSTableMetadata::min_timestamp`/`max_timestamp`.

use std::collections::HashMap;
use std::time::Duration;

use crate::timeseries::config::parse_duration;

/// Table extension naming the segment window.
pub const SEGMENT_WINDOW_KEY: &str = "rrd.segment_window";
/// Table extension naming the retention horizon.
pub const RETAIN_KEY: &str = "rrd.retain";

const MICROS_PER_SECOND: u128 = 1_000_000;

/// Configuration for an RRD segmented table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RrdConfig {
    /// Length of one segment window; one immutable segment file per window.
    pub segment_window: Duration,
    /// Retention horizon measured back from now.
    pub retain: Duration,
}

impl RrdConfig {
    /// Parse from a table's extensions map.
    ///
    /// `None` when the table sets no `rrd.*` key at all. `Some(Err(..))` when a
    /// key is present but unusable: a malformed duration, a missing companion
    /// key, a zero window, or a retention shorter than one window. A table cannot
    /// be half-segmented, so every one of those is a schema error rather than a
    /// silent fall back to compaction.
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
                "{RETAIN_KEY} is set but {SEGMENT_WINDOW_KEY} is missing: a retention horizon \
                 without a segment window does not describe segmented storage"
            ));
        };
        let segment_window = parse_duration(window_raw)
            .map_err(|reason| format!("invalid {SEGMENT_WINDOW_KEY} '{window_raw}': {reason}"))?;
        if segment_window.is_zero() {
            return Err(format!(
                "{SEGMENT_WINDOW_KEY} must be positive (non-zero)"
            ));
        }

        let Some(retain_raw) = ext.get(RETAIN_KEY) else {
            return Err(format!(
                "{SEGMENT_WINDOW_KEY} is set but {RETAIN_KEY} is missing: segmented storage \
                 needs a retention horizon"
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

        Ok(Self {
            segment_window,
            retain,
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

    /// Convert seconds since the epoch to the microsecond clock used by
    /// extension arithmetic and cell timestamps.
    pub fn micros_from_secs(secs: i64) -> i64 {
        (secs as i128 * MICROS_PER_SECOND as i128) as i64
    }
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
        let reason = RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "5m"),
            (RETAIN_KEY, "2 days"),
        ]))
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
        let reason = RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "1h"),
            (RETAIN_KEY, "30m"),
        ]))
        .expect("rrd keys present")
        .expect_err("a segment cannot expire before its window closed");
        assert!(reason.contains(RETAIN_KEY), "got: {reason}");
    }

    #[test]
    fn rrd_config_rejects_zero_window() {
        let reason = RrdConfig::from_extensions(&ext(&[
            (SEGMENT_WINDOW_KEY, "0m"),
            (RETAIN_KEY, "1h"),
        ]))
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
}
