//! Module: Runtime tunables for the aligned SSTable write pump.
//! Correctness: An invalid or out-of-bounds env value never changes writer
//!   behavior silently — the default applies and the rejection is logged
//!   exactly once per process, the same rule `direct::configured` uses for
//!   the O_DIRECT switch. `effective_segment` always returns a positive
//!   multiple of the caller's block that is at least as large as the
//!   configured request.
//! Last revised: 2026-09-26
//! Last changed: New module (T-030, sstable-write-pump). This packet ships
//!   `PumpConfig` only — env parsing, bounds, and the block-rounding rule
//!   from `decisions.md` § Runtime tunables. Nothing here wires a pump yet
//!   (`architecture.md` § AlignedPump); that lands in T-032 onward, and per
//!   the orchestrator's 2026-09-26 scope change the segment ring there is
//!   two pre-filled `std::sync::mpsc::sync_channel` queues, not a
//!   `VecDeque`-backed bounded ring (dropped from this packet as dead code).

use std::sync::atomic::{AtomicBool, Ordering};

/// Env var for the aligned segment size in bytes, before rounding to a block
/// multiple. See [`PumpConfig::from_env`].
pub const SEGMENT_BYTES_ENV: &str = "FERROSA_SSTABLE_WRITE_SEGMENT_BYTES";

/// Env var for the number of segments that may be in flight to the flusher.
/// `0` selects the synchronous (depth-0) pump. See [`PumpConfig::from_env`].
pub const QUEUE_DEPTH_ENV: &str = "FERROSA_SSTABLE_WRITE_QUEUE_DEPTH";

const DEFAULT_SEGMENT_BYTES: usize = 1024 * 1024; // 1 MiB
const MIN_SEGMENT_BYTES: usize = 1;
const MAX_SEGMENT_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

const DEFAULT_QUEUE_DEPTH: usize = 3;
const MIN_QUEUE_DEPTH: usize = 0; // 0 = synchronous
const MAX_QUEUE_DEPTH: usize = 16;

static SEGMENT_BYTES_WARNED: AtomicBool = AtomicBool::new(false);
static QUEUE_DEPTH_WARNED: AtomicBool = AtomicBool::new(false);
static ROUNDING_WARNED: AtomicBool = AtomicBool::new(false);

/// Runtime tunables for the aligned write pump (`decisions.md` § Runtime
/// tunables). Read once when a writer opens; no restart is needed to pick up
/// a new value for the next writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PumpConfig {
    /// Requested segment size in bytes, before rounding to a block multiple.
    pub segment_bytes: usize,
    /// Segments that may be in flight to the flusher. `0` means synchronous.
    pub queue_depth: usize,
}

impl Default for PumpConfig {
    fn default() -> Self {
        Self {
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            queue_depth: DEFAULT_QUEUE_DEPTH,
        }
    }
}

impl PumpConfig {
    /// Read both tunables from the environment. A value that is set but does
    /// not parse as a `usize`, or falls outside its bounds, is rejected: the
    /// default applies and the rejection is logged once per process (never
    /// silently, per the standing order on silent failures).
    pub fn from_env() -> Self {
        Self {
            segment_bytes: resolve_env(
                SEGMENT_BYTES_ENV,
                std::env::var(SEGMENT_BYTES_ENV).ok().as_deref(),
                DEFAULT_SEGMENT_BYTES,
                MIN_SEGMENT_BYTES,
                MAX_SEGMENT_BYTES,
                &SEGMENT_BYTES_WARNED,
            ),
            queue_depth: resolve_env(
                QUEUE_DEPTH_ENV,
                std::env::var(QUEUE_DEPTH_ENV).ok().as_deref(),
                DEFAULT_QUEUE_DEPTH,
                MIN_QUEUE_DEPTH,
                MAX_QUEUE_DEPTH,
                &QUEUE_DEPTH_WARNED,
            ),
        }
    }

    /// Round `segment_bytes` up to a multiple of `block`, with a minimum of
    /// one block (`decisions.md` D5): `round_up(max(segment_bytes, block),
    /// block)`. Logs once per process, at INFO, when rounding changes the
    /// configured value — never at every open, or the one line that mattered
    /// would drown in identical repeats.
    pub fn effective_segment(&self, block: usize) -> usize {
        debug_assert!(block > 0, "block must be positive");
        let wanted = self.segment_bytes.max(block);
        let rounded = wanted.div_ceil(block) * block;
        if rounded != self.segment_bytes && !ROUNDING_WARNED.swap(true, Ordering::Relaxed) {
            tracing::info!(
                configured = self.segment_bytes,
                effective = rounded,
                block,
                "write pump segment size rounded up to a block multiple"
            );
        }
        rounded
    }
}

/// One parsed env value: unset (absent, empty, or blank), a `usize` within
/// `[min, max]`, or invalid (non-numeric or out of bounds). Pure — no env
/// access — so the parsing rules are unit-tested directly without
/// `std::env::set_var`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedBound {
    Unset,
    Value(usize),
    Invalid,
}

fn parse_usize_bounded(value: Option<&str>, min: usize, max: usize) -> ParsedBound {
    let Some(text) = value.map(str::trim).filter(|t| !t.is_empty()) else {
        return ParsedBound::Unset;
    };
    match text.parse::<usize>() {
        Ok(n) if n >= min && n <= max => ParsedBound::Value(n),
        _ => ParsedBound::Invalid,
    }
}

/// Resolve one tunable from an already-read env value: `(resolved, rejected)`.
/// `rejected` is true only when the value was set but unusable — unset and
/// empty are not rejections, they are the normal way to ask for the default.
fn resolve_bounded(value: Option<&str>, default: usize, min: usize, max: usize) -> (usize, bool) {
    match parse_usize_bounded(value, min, max) {
        ParsedBound::Value(n) => (n, false),
        ParsedBound::Unset => (default, false),
        ParsedBound::Invalid => (default, true),
    }
}

/// `resolve_bounded` plus the once-per-process WARN, matching the pattern
/// `direct::configured` uses for the O_DIRECT switch: the writer opens a
/// pump per SSTable, so a line per file would bury the one that mattered.
fn resolve_env(
    name: &str,
    value: Option<&str>,
    default: usize,
    min: usize,
    max: usize,
    warned: &AtomicBool,
) -> usize {
    let (resolved, rejected) = resolve_bounded(value, default, min, max);
    if rejected && !warned.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            var = name,
            value = ?value,
            default,
            min,
            max,
            "invalid value for write pump tunable; using the default"
        );
    }
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn pump_primitives_default_config_matches_the_documented_defaults() {
        let config = PumpConfig::default();
        assert_eq!(config.segment_bytes, 1024 * 1024);
        assert_eq!(config.queue_depth, 3);
    }

    /// `unset, empty, garbage, 0, 1, 4095, 4097, above max` for the segment
    /// size (bounds `[1, 16 MiB]`).
    #[test]
    fn pump_primitives_segment_bytes_parse_table() {
        let cases: &[(Option<&str>, ParsedBound)] = &[
            (None, ParsedBound::Unset),
            (Some(""), ParsedBound::Unset),
            (Some("   "), ParsedBound::Unset),
            (Some("garbage"), ParsedBound::Invalid),
            (Some("-1"), ParsedBound::Invalid),
            (Some("0"), ParsedBound::Invalid),
            (Some("1"), ParsedBound::Value(1)),
            (Some("4095"), ParsedBound::Value(4095)),
            (Some("4097"), ParsedBound::Value(4097)),
            (Some("16777216"), ParsedBound::Value(16 * 1024 * 1024)),
            (Some("16777217"), ParsedBound::Invalid),
            (Some("999999999999"), ParsedBound::Invalid),
        ];
        for (input, expected) in cases {
            let got = parse_usize_bounded(*input, MIN_SEGMENT_BYTES, MAX_SEGMENT_BYTES);
            assert_eq!(got, *expected, "input {input:?}");
        }
    }

    /// Same table shape for the queue depth (bounds `[0, 16]`, `0` valid and
    /// meaningful — synchronous mode, not "unset").
    #[test]
    fn pump_primitives_queue_depth_parse_table() {
        let cases: &[(Option<&str>, ParsedBound)] = &[
            (None, ParsedBound::Unset),
            (Some(""), ParsedBound::Unset),
            (Some("   "), ParsedBound::Unset),
            (Some("garbage"), ParsedBound::Invalid),
            (Some("-1"), ParsedBound::Invalid),
            (Some("0"), ParsedBound::Value(0)),
            (Some("1"), ParsedBound::Value(1)),
            (Some("4095"), ParsedBound::Invalid),
            (Some("4097"), ParsedBound::Invalid),
            (Some("16"), ParsedBound::Value(16)),
            (Some("17"), ParsedBound::Invalid),
        ];
        for (input, expected) in cases {
            let got = parse_usize_bounded(*input, MIN_QUEUE_DEPTH, MAX_QUEUE_DEPTH);
            assert_eq!(got, *expected, "input {input:?}");
        }
    }

    #[test]
    fn pump_primitives_resolve_bounded_falls_back_to_default_on_rejection() {
        let (value, rejected) = resolve_bounded(Some("not-a-number"), 1024, 1, 16 * 1024 * 1024);
        assert_eq!(value, 1024);
        assert!(rejected);

        let (value, rejected) = resolve_bounded(None, 1024, 1, 16 * 1024 * 1024);
        assert_eq!(value, 1024);
        assert!(!rejected, "unset is not a rejection");

        let (value, rejected) = resolve_bounded(Some("2048"), 1024, 1, 16 * 1024 * 1024);
        assert_eq!(value, 2048);
        assert!(!rejected);
    }

    #[test]
    fn pump_primitives_effective_segment_rounds_up_with_a_minimum_of_one_block() {
        assert_eq!(PumpConfig::default().effective_segment(4096), 1024 * 1024);
        let small = PumpConfig {
            segment_bytes: 1,
            queue_depth: 3,
        };
        assert_eq!(small.effective_segment(4096), 4096, "minimum one block");
        let exact = PumpConfig {
            segment_bytes: 8192,
            queue_depth: 3,
        };
        assert_eq!(exact.effective_segment(4096), 8192, "already a multiple");
        let over = PumpConfig {
            segment_bytes: 4097,
            queue_depth: 3,
        };
        assert_eq!(over.effective_segment(4096), 8192, "rounds up, not down");
    }

    proptest! {
        /// P7: for any configured size in `1..=16 MiB` and any probed block in
        /// `{4096, 8192, 65536}`, the effective segment is a positive multiple
        /// of the block, at least the block, at least the configured value,
        /// and never overshoots by a whole extra block.
        #[test]
        fn pump_primitives_effective_segment_rounding_property(
            configured in 1usize..=16 * 1024 * 1024,
            block in prop_oneof![Just(4096usize), Just(8192usize), Just(65536usize)],
        ) {
            let config = PumpConfig { segment_bytes: configured, queue_depth: 3 };
            let effective = config.effective_segment(block);
            prop_assert_eq!(effective % block, 0);
            prop_assert!(effective >= block);
            prop_assert!(effective >= configured);
            prop_assert!(effective < configured + block);
        }
    }
}
