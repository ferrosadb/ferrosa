//! Process RAM budget detection and the ORDER BY / spill threshold.
//!
//! A spilling external sort (see [`crate::external_sort`]) must bound its
//! in-memory accumulation to a fraction of the process's real memory budget,
//! then spill sorted runs to disk once that fraction is crossed. This module
//! answers "how many bytes may I hold before I must spill?".
//!
//! # Budget detection (in priority order)
//!
//! 1. **cgroup v2** — `/sys/fs/cgroup/memory.max`
//! 2. **cgroup v1** — `/sys/fs/cgroup/memory/memory.limit_in_bytes`
//! 3. **system total RAM** — `/proc/meminfo` (`MemTotal`)
//!
//! A cgroup value of `"max"` (v2) or an absurdly large sentinel (v1 stores
//! "unlimited" as a near-`u64::MAX` value) is treated as *unlimited*, so the
//! system total is used instead.
//!
//! Detection is **injectable**: [`detect_memory_budget_bytes`] reads a
//! [`BudgetSources`] describing where to read from, so tests exercise the
//! detection logic deterministically without depending on the host's cgroups.
//! Production callers use [`BudgetSources::host`] and cache the result once at
//! startup via [`memory_budget_bytes`].
//!
//! # Spill threshold
//!
//! The threshold defaults to **50%** of the budget. It is tunable:
//!
//! - `FERROSA_RANGE_SPILL_THRESHOLD_BYTES` — absolute byte override (wins).
//! - `FERROSA_RANGE_SPILL_THRESHOLD_PCT` — percent of the budget (default 50).
//!
//! Both env reads and the percentage math are pure given their inputs, so
//! [`spill_threshold_bytes`] is deterministically testable.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Default spill threshold as a percentage of the detected memory budget.
pub const DEFAULT_SPILL_THRESHOLD_PCT: u64 = 50;

/// Env var: absolute spill threshold in bytes (overrides the percentage).
pub const ENV_SPILL_THRESHOLD_BYTES: &str = "FERROSA_RANGE_SPILL_THRESHOLD_BYTES";

/// Env var: spill threshold as a percentage of the memory budget (default 50).
pub const ENV_SPILL_THRESHOLD_PCT: &str = "FERROSA_RANGE_SPILL_THRESHOLD_PCT";

/// A cgroup value at or above this is treated as "unlimited" (fall back to
/// system total). cgroup v1 encodes "no limit" as `PAGE_COUNTER_MAX * PAGE_SIZE`,
/// which lands very close to (but just under) `i64::MAX`. Anything at or above
/// 4 EiB cannot be a real container memory limit, so this threshold catches
/// both the exact sentinel and page-rounded variants of it.
const UNLIMITED_SENTINEL: u64 = 1u64 << 62;

/// A conservative floor used only when *every* detection source is unreadable
/// (e.g. a non-Linux host with no `/proc`, no cgroups). 1 GiB keeps the spill
/// threshold non-zero so the external sort never treats "unknown" as "spill on
/// every row". This path is loud: [`detect_memory_budget_bytes`] returns the
/// source it used so callers can log a fallback.
const UNKNOWN_HOST_FLOOR_BYTES: u64 = 1024 * 1024 * 1024;

/// Where budget detection reads from. Injectable so tests can point the cgroup
/// and meminfo readers at fixtures instead of the real host.
#[derive(Debug, Clone)]
pub struct BudgetSources {
    /// cgroup v2 unified `memory.max` path.
    pub cgroup_v2_max: PathBuf,
    /// cgroup v1 `memory.limit_in_bytes` path.
    pub cgroup_v1_limit: PathBuf,
    /// `/proc/meminfo` path (or a fixture).
    pub meminfo: PathBuf,
}

impl BudgetSources {
    /// The real host paths.
    pub fn host() -> Self {
        Self {
            cgroup_v2_max: PathBuf::from("/sys/fs/cgroup/memory.max"),
            cgroup_v1_limit: PathBuf::from("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            meminfo: PathBuf::from("/proc/meminfo"),
        }
    }
}

/// Which source produced the detected budget. Returned alongside the value so
/// callers (and tests) can log/assert the resolution path rather than guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetSource {
    /// cgroup v2 `memory.max` (a finite limit).
    CgroupV2,
    /// cgroup v1 `memory.limit_in_bytes` (a finite limit).
    CgroupV1,
    /// `/proc/meminfo` `MemTotal`.
    SystemTotal,
    /// No source was readable; the conservative floor was used.
    UnknownFloor,
}

/// Parse a cgroup memory limit file's contents into a finite byte budget.
///
/// Returns `None` when the value is absent, unparseable, the literal `"max"`
/// (cgroup v2 unlimited), or at/above the [`UNLIMITED_SENTINEL`] (cgroup v1
/// unlimited) — in all of which cases the caller must fall through to the next
/// source.
fn parse_cgroup_limit(contents: &str) -> Option<u64> {
    let trimmed = contents.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("max") {
        return None;
    }
    let bytes: u64 = trimmed.parse().ok()?;
    if bytes == 0 || bytes >= UNLIMITED_SENTINEL {
        return None;
    }
    Some(bytes)
}

/// Parse `MemTotal:  16384000 kB` out of `/proc/meminfo` contents into bytes.
fn parse_meminfo_total_bytes(contents: &str) -> Option<u64> {
    for line in contents.lines() {
        let Some(rest) = line.strip_prefix("MemTotal:") else {
            continue;
        };
        // Format is "<num> kB". Take the first numeric token, multiply by 1024.
        let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
        return kb.checked_mul(1024);
    }
    None
}

fn read_to_string(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// Detect the process memory budget from the given sources, in priority order:
/// cgroup v2 → cgroup v1 → system total → conservative floor.
///
/// Returns both the byte budget and which source produced it (for logging).
pub fn detect_memory_budget_bytes(sources: &BudgetSources) -> (u64, BudgetSource) {
    if let Some(bytes) = read_to_string(&sources.cgroup_v2_max).and_then(|c| parse_cgroup_limit(&c))
    {
        return (bytes, BudgetSource::CgroupV2);
    }
    if let Some(bytes) =
        read_to_string(&sources.cgroup_v1_limit).and_then(|c| parse_cgroup_limit(&c))
    {
        return (bytes, BudgetSource::CgroupV1);
    }
    if let Some(bytes) =
        read_to_string(&sources.meminfo).and_then(|c| parse_meminfo_total_bytes(&c))
    {
        return (bytes, BudgetSource::SystemTotal);
    }
    (UNKNOWN_HOST_FLOOR_BYTES, BudgetSource::UnknownFloor)
}

/// Env inputs to the threshold calc, captured so the math is pure/testable.
#[derive(Debug, Clone, Default)]
pub struct ThresholdEnv {
    /// `FERROSA_RANGE_SPILL_THRESHOLD_BYTES` (absolute override).
    pub abs_bytes: Option<String>,
    /// `FERROSA_RANGE_SPILL_THRESHOLD_PCT` (percentage of budget).
    pub pct: Option<String>,
}

impl ThresholdEnv {
    /// Read the two threshold env vars from the process environment.
    pub fn from_process_env() -> Self {
        Self {
            abs_bytes: std::env::var(ENV_SPILL_THRESHOLD_BYTES).ok(),
            pct: std::env::var(ENV_SPILL_THRESHOLD_PCT).ok(),
        }
    }
}

/// Compute the spill threshold in bytes from a budget and env overrides.
///
/// Precedence:
/// 1. A valid `FERROSA_RANGE_SPILL_THRESHOLD_BYTES` (parsed, `> 0`) — used as-is,
///    clamped to at most the budget (never promise to hold more than exists).
/// 2. Otherwise `budget * pct / 100`, where `pct` comes from
///    `FERROSA_RANGE_SPILL_THRESHOLD_PCT` (parsed, `1..=100`) or defaults to 50.
///
/// The result is at least 1 byte so a spilling sort always makes forward
/// progress (it can hold at least one row before spilling), even on a tiny
/// configured budget.
pub fn spill_threshold_bytes(budget_bytes: u64, env: &ThresholdEnv) -> u64 {
    if let Some(abs) = env
        .abs_bytes
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&b| b > 0)
    {
        return abs.min(budget_bytes.max(1));
    }

    let pct = env
        .pct
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&p| (1..=100).contains(&p))
        .unwrap_or(DEFAULT_SPILL_THRESHOLD_PCT);

    // u128 intermediate avoids overflow for large budgets.
    let threshold = (budget_bytes as u128 * pct as u128 / 100) as u64;
    threshold.max(1)
}

/// The process memory budget, detected once from the host and cached.
pub fn memory_budget_bytes() -> u64 {
    static CACHE: OnceLock<u64> = OnceLock::new();
    *CACHE.get_or_init(|| {
        let (bytes, source) = detect_memory_budget_bytes(&BudgetSources::host());
        tracing::debug!(
            budget_bytes = bytes,
            ?source,
            "storage: detected process memory budget for spill threshold"
        );
        bytes
    })
}

/// The effective spill threshold for the running process: the cached memory
/// budget combined with the threshold env overrides read **fresh** on each call.
///
/// The budget detection (cgroup/meminfo reads) is cached because it is fixed for
/// the process lifetime and non-trivial to probe. The env overrides are read
/// per call so an operator (or a test) can change
/// `FERROSA_RANGE_SPILL_THRESHOLD_{PCT,BYTES}` and have it take effect without a
/// restart, and so tests never race a process-global threshold cache.
pub fn process_spill_threshold_bytes() -> u64 {
    let threshold = spill_threshold_bytes(memory_budget_bytes(), &ThresholdEnv::from_process_env());
    tracing::trace!(
        threshold_bytes = threshold,
        "storage: effective ORDER BY spill threshold"
    );
    threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Part A: pure threshold calc (spill_threshold_calc) ----

    #[test]
    fn spill_threshold_calc_defaults_to_50_percent() {
        let budget = 8 * 1024 * 1024 * 1024u64; // 8 GiB
        let env = ThresholdEnv::default();
        assert_eq!(spill_threshold_bytes(budget, &env), budget / 2);
    }

    #[test]
    fn spill_threshold_calc_pct_override() {
        let budget = 10_000u64;
        let env = ThresholdEnv {
            abs_bytes: None,
            pct: Some("25".to_string()),
        };
        assert_eq!(spill_threshold_bytes(budget, &env), 2_500);
    }

    #[test]
    fn spill_threshold_calc_pct_100_is_whole_budget() {
        let budget = 12_345u64;
        let env = ThresholdEnv {
            abs_bytes: None,
            pct: Some("100".to_string()),
        };
        assert_eq!(spill_threshold_bytes(budget, &env), 12_345);
    }

    #[test]
    fn spill_threshold_calc_abs_override_wins_over_pct() {
        let budget = 8 * 1024 * 1024 * 1024u64;
        let env = ThresholdEnv {
            abs_bytes: Some("4096".to_string()),
            pct: Some("90".to_string()), // ignored when abs is present
        };
        assert_eq!(spill_threshold_bytes(budget, &env), 4096);
    }

    #[test]
    fn spill_threshold_calc_abs_clamped_to_budget() {
        // Asking to hold more than the budget is clamped to the budget.
        let budget = 1_000u64;
        let env = ThresholdEnv {
            abs_bytes: Some("999999999".to_string()),
            pct: None,
        };
        assert_eq!(spill_threshold_bytes(budget, &env), 1_000);
    }

    #[test]
    fn spill_threshold_calc_invalid_env_falls_back_to_default_pct() {
        let budget = 2_000u64;
        // Non-numeric / out-of-range values are ignored → default 50%.
        for bad_pct in ["", "0", "101", "abc", "-5", "50.5"] {
            let env = ThresholdEnv {
                abs_bytes: None,
                pct: Some(bad_pct.to_string()),
            };
            assert_eq!(
                spill_threshold_bytes(budget, &env),
                1_000,
                "pct={bad_pct:?} must fall back to 50%"
            );
        }
        // Invalid abs → ignored, falls through to pct default.
        for bad_abs in ["", "0", "xyz", "-1"] {
            let env = ThresholdEnv {
                abs_bytes: Some(bad_abs.to_string()),
                pct: None,
            };
            assert_eq!(
                spill_threshold_bytes(budget, &env),
                1_000,
                "abs={bad_abs:?} must be ignored → 50%"
            );
        }
    }

    #[test]
    fn spill_threshold_calc_is_at_least_one_byte() {
        // A tiny budget with default pct still yields a usable (>=1) threshold.
        assert_eq!(spill_threshold_bytes(1, &ThresholdEnv::default()), 1);
        assert_eq!(spill_threshold_bytes(0, &ThresholdEnv::default()), 1);
    }

    #[test]
    fn spill_threshold_calc_no_overflow_on_huge_budget() {
        let budget = u64::MAX;
        let env = ThresholdEnv {
            abs_bytes: None,
            pct: Some("50".to_string()),
        };
        assert_eq!(spill_threshold_bytes(budget, &env), u64::MAX / 2);
    }

    // ---- Part A: injectable budget detection ----

    fn sources_in(
        dir: &Path,
        v2: Option<&str>,
        v1: Option<&str>,
        meminfo: Option<&str>,
    ) -> BudgetSources {
        let write = |name: &str, contents: Option<&str>| {
            let path = dir.join(name);
            if let Some(c) = contents {
                std::fs::write(&path, c).unwrap();
            }
            path
        };
        BudgetSources {
            cgroup_v2_max: write("memory.max", v2),
            cgroup_v1_limit: write("memory.limit_in_bytes", v1),
            meminfo: write("meminfo", meminfo),
        }
    }

    #[test]
    fn detect_prefers_cgroup_v2() {
        let dir = tempfile::tempdir().unwrap();
        let s = sources_in(
            dir.path(),
            Some("2147483648\n"), // 2 GiB
            Some("4294967296\n"), // 4 GiB (should be ignored)
            Some("MemTotal:       33554432 kB\n"),
        );
        assert_eq!(
            detect_memory_budget_bytes(&s),
            (2 * 1024 * 1024 * 1024, BudgetSource::CgroupV2)
        );
    }

    #[test]
    fn detect_v2_max_falls_through_to_v1() {
        let dir = tempfile::tempdir().unwrap();
        let s = sources_in(
            dir.path(),
            Some("max\n"),       // v2 unlimited
            Some("536870912\n"), // 512 MiB
            Some("MemTotal:       33554432 kB\n"),
        );
        assert_eq!(
            detect_memory_budget_bytes(&s),
            (512 * 1024 * 1024, BudgetSource::CgroupV1)
        );
    }

    #[test]
    fn detect_v1_unlimited_sentinel_falls_through_to_system_total() {
        let dir = tempfile::tempdir().unwrap();
        // A near-i64::MAX v1 value means "no limit" → use system total.
        let s = sources_in(
            dir.path(),
            Some("max\n"),
            Some("9223372036854771712\n"),
            Some("MemTotal:       16384 kB\n"),
        );
        assert_eq!(
            detect_memory_budget_bytes(&s),
            (16384 * 1024, BudgetSource::SystemTotal)
        );
    }

    #[test]
    fn detect_no_sources_uses_floor() {
        let dir = tempfile::tempdir().unwrap();
        let s = sources_in(dir.path(), None, None, None);
        assert_eq!(
            detect_memory_budget_bytes(&s),
            (UNKNOWN_HOST_FLOOR_BYTES, BudgetSource::UnknownFloor)
        );
    }

    #[test]
    fn parse_meminfo_extracts_memtotal_kb() {
        let contents = "MemFree: 100 kB\nMemTotal:       16384000 kB\nBuffers: 5 kB\n";
        assert_eq!(parse_meminfo_total_bytes(contents), Some(16384000 * 1024));
    }
}
