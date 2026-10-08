//! Legacy nanosecond cell timestamps (t_cf637b6e).
//!
//! Every cell timestamp is **microseconds** since the epoch, except the ones
//! Accord wrote before the fix: `accord_cell_timestamp` stored the HLC's
//! **nanoseconds** (~1.8e18), so every LWT-written cell, liveness stamp and
//! deletion marker sat ~1000x in the future. Those values are on disk (SSTables,
//! commit-log segments, batchlog, hints, the Accord journal) and in flight
//! between nodes of a mixed-version cluster.
//!
//! [`normalize_cell_ts`] maps such a value back to microseconds. It is applied
//! where a timestamp is DECODED (SSTable rows, commit-log `Mutation` bytes)
//! and where a row ENTERS the memtable, so every comparison downstream — LWW
//! merge, the Accord read-at-`t` bound, purge, repair, `writetime()`, PITR —
//! only ever sees microseconds. Compaction rewrites what it reads, so the
//! legacy values migrate off disk; [`legacy_ns_normalised_total`] reads zero
//! once none are left.
//!
//! The legacy range is `[1e18, i64::MAX)`. A real microsecond timestamp reaches
//! 1e18 in the year 33658, and the CQL layer refuses `USING TIMESTAMP` at or
//! above it, so the range is unambiguous. The sentinels `i64::MIN`
//! ([`crate::NO_TIMESTAMP`], `LivenessInfo::NONE`, `DeletionTime::LIVE`) and
//! `i64::MAX` pass through unchanged.

use std::sync::atomic::{AtomicU64, Ordering};

/// The smallest raw timestamp treated as legacy nanoseconds. A microsecond
/// timestamp this large is the year 33658.
pub const LEGACY_NS_THRESHOLD: i64 = 1_000_000_000_000_000_000;

/// The smallest value a normalised legacy timestamp can take
/// (`LEGACY_NS_THRESHOLD / 1000`). A lower bound for every normalised cell, used
/// where an SSTable's real lower bound is unknown.
pub const LEGACY_NS_NORMALISED_FLOOR: i64 = LEGACY_NS_THRESHOLD / 1_000;

/// True when `raw` is a legacy nanosecond cell timestamp.
#[inline]
pub fn is_legacy_ns(raw: i64) -> bool {
    (LEGACY_NS_THRESHOLD..i64::MAX).contains(&raw)
}

/// Map a raw cell timestamp to microseconds: `raw / 1000` iff it is a legacy
/// nanosecond value, otherwise unchanged (sentinels included).
#[inline]
pub fn normalize_cell_ts(raw: i64) -> i64 {
    if is_legacy_ns(raw) {
        raw / 1_000
    } else {
        raw
    }
}

/// The logical `(min, max)` cell-timestamp bounds of an SSTable whose stored
/// header says `(raw_min, raw_max)`, after [`normalize_cell_ts`] is applied to
/// every cell. `raw_max == i64::MAX` means the maximum is unknown.
///
/// - **ns-only** (`raw_min` is legacy): every cell is legacy, and normalising is
///   monotone, so both bounds normalise exactly.
/// - **mixed or unknown** (`raw_min` is micros, `raw_max` legacy or unknown): a
///   normalised legacy cell can sit below `raw_min`, so the lower bound drops to
///   [`LEGACY_NS_NORMALISED_FLOOR`] and the upper bound is unknown. Both only
///   ever widen the range, which is the safe direction for every consumer
///   (delta encoding of a compaction output, the purge overlap guard).
/// - **micros-only**: unchanged.
pub fn normalize_timestamp_bounds(raw_min: i64, raw_max: i64) -> (i64, i64) {
    if is_legacy_ns(raw_min) {
        return (normalize_cell_ts(raw_min), normalize_cell_ts(raw_max));
    }
    if raw_max == i64::MAX || is_legacy_ns(raw_max) {
        return (raw_min.min(LEGACY_NS_NORMALISED_FLOOR), i64::MAX);
    }
    (raw_min, raw_max)
}

/// Where a legacy nanosecond timestamp was found and normalised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyNsSource {
    /// A cell, liveness or deletion timestamp decoded from an SSTable.
    Sstable,
    /// A timestamp decoded from serialized `Mutation` bytes: commit-log replay,
    /// batchlog, internode forwards, hints, Accord apply and read votes, PITR.
    Mutation,
    /// A row handed to the memtable by any write producer.
    MemtableWrite,
}

impl LegacyNsSource {
    /// Every source, in metric-rendering order.
    pub const ALL: [LegacyNsSource; 3] = [
        LegacyNsSource::Sstable,
        LegacyNsSource::Mutation,
        LegacyNsSource::MemtableWrite,
    ];

    /// The `source` label value.
    pub fn label(self) -> &'static str {
        match self {
            LegacyNsSource::Sstable => "sstable",
            LegacyNsSource::Mutation => "mutation",
            LegacyNsSource::MemtableWrite => "memtable_write",
        }
    }

    fn index(self) -> usize {
        match self {
            LegacyNsSource::Sstable => 0,
            LegacyNsSource::Mutation => 1,
            LegacyNsSource::MemtableWrite => 2,
        }
    }
}

static LEGACY_NS_NORMALISED: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];

/// Add `count` normalised timestamps from `source` to
/// `legacy_ns_timestamps_normalised_total`. Callers batch per decoded unit (an
/// SSTable reader pass, a mutation, a row) so the hot path pays one atomic per
/// unit that held legacy values, and none otherwise.
pub fn record_legacy_ns_normalised(source: LegacyNsSource, count: u64) {
    if count > 0 {
        LEGACY_NS_NORMALISED[source.index()].fetch_add(count, Ordering::Relaxed);
    }
}

/// Timestamps normalised from `source` since process start.
pub fn legacy_ns_normalised_total(source: LegacyNsSource) -> u64 {
    LEGACY_NS_NORMALISED[source.index()].load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wall-clock nanoseconds, as the pre-fix Accord apply stamped cells.
    const LEGACY_NS: i64 = 1_791_437_153_001_234_567;

    #[test]
    fn a_legacy_nanosecond_stamp_normalises_to_microseconds() {
        assert_eq!(normalize_cell_ts(LEGACY_NS), 1_791_437_153_001_234);
        assert_eq!(
            normalize_cell_ts(LEGACY_NS_THRESHOLD),
            1_000_000_000_000_000
        );
    }

    #[test]
    fn a_microsecond_stamp_is_unchanged() {
        for ts in [0, 1, 1_791_437_153_001_234, LEGACY_NS_THRESHOLD - 1, -5] {
            assert_eq!(normalize_cell_ts(ts), ts, "{ts}");
        }
    }

    /// Test 14: every sentinel round-trips unchanged.
    #[test]
    fn sentinels_are_unchanged() {
        for ts in [i64::MIN, i64::MAX, crate::NO_TIMESTAMP] {
            assert_eq!(normalize_cell_ts(ts), ts, "{ts}");
            assert!(!is_legacy_ns(ts), "{ts}");
        }
    }

    #[test]
    fn normalising_is_idempotent_and_monotone() {
        let samples = [
            i64::MIN,
            -1,
            0,
            1_700_000_000_000_000,
            LEGACY_NS_THRESHOLD - 1,
            LEGACY_NS_THRESHOLD,
            LEGACY_NS,
            i64::MAX - 1,
            i64::MAX,
        ];
        for &a in &samples {
            assert_eq!(
                normalize_cell_ts(normalize_cell_ts(a)),
                normalize_cell_ts(a)
            );
            for &b in &samples {
                if a <= b && is_legacy_ns(a) == is_legacy_ns(b) {
                    assert!(normalize_cell_ts(a) <= normalize_cell_ts(b), "{a} {b}");
                }
            }
        }
    }

    #[test]
    fn bounds_of_a_nanosecond_only_file_normalise_exactly() {
        assert_eq!(
            normalize_timestamp_bounds(LEGACY_NS, LEGACY_NS + 5_000),
            (LEGACY_NS / 1_000, LEGACY_NS / 1_000 + 5)
        );
        assert_eq!(
            normalize_timestamp_bounds(LEGACY_NS, i64::MAX),
            (LEGACY_NS / 1_000, i64::MAX)
        );
    }

    #[test]
    fn bounds_of_a_mixed_file_widen_to_cover_every_normalised_cell() {
        let micros = 1_791_437_200_000_000;
        assert_eq!(
            normalize_timestamp_bounds(micros, LEGACY_NS),
            (LEGACY_NS_NORMALISED_FLOOR, i64::MAX)
        );
        // An unknown maximum may hide legacy cells.
        assert_eq!(
            normalize_timestamp_bounds(micros, i64::MAX),
            (LEGACY_NS_NORMALISED_FLOOR, i64::MAX)
        );
        // A lower raw minimum is kept.
        assert_eq!(normalize_timestamp_bounds(5, i64::MAX), (5, i64::MAX));
        assert_eq!(
            normalize_timestamp_bounds(i64::MIN, i64::MAX),
            (i64::MIN, i64::MAX)
        );
    }

    #[test]
    fn bounds_of_a_microsecond_only_file_are_unchanged() {
        assert_eq!(normalize_timestamp_bounds(10, 20), (10, 20));
        assert_eq!(
            normalize_timestamp_bounds(1_700_000_000_000_000, LEGACY_NS_THRESHOLD - 1),
            (1_700_000_000_000_000, LEGACY_NS_THRESHOLD - 1)
        );
    }

    #[test]
    fn the_counter_ignores_zero_and_accumulates() {
        let before = legacy_ns_normalised_total(LegacyNsSource::MemtableWrite);
        record_legacy_ns_normalised(LegacyNsSource::MemtableWrite, 0);
        record_legacy_ns_normalised(LegacyNsSource::MemtableWrite, 3);
        assert!(legacy_ns_normalised_total(LegacyNsSource::MemtableWrite) >= before + 3);
    }
}
