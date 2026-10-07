//! Bounded process-wide runtime tuning for storage flush and compaction work.
//!
//! Values are parsed once, on first use. Every setting has a finite supported
//! range so raising a throughput limit cannot silently create unbounded queues,
//! thread pools, or compaction fan-in. Invalid and non-Unicode values are logged
//! at ERROR and fall back to the documented default.

use std::env;
use std::sync::OnceLock;

const DEFAULT_MAX_AUTOMATIC_FLUSHES_PER_POLL: usize = 8;
const DEFAULT_MAX_COMPACTION_INPUTS_PER_TASK: usize = 64;
const DEFAULT_MAX_SCHEDULED_TABLES_PER_POLL: usize = 8;
const DEFAULT_MAINTENANCE_DRAIN_BATCH: usize = 8;
const DEFAULT_MAX_AGE_FLUSH_FLOOR_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_PER_COMPACTION_MEM_BUDGET_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_AUTO_COMPACTION_PARALLELISM: usize = 8;
const DEFAULT_MAX_FLUSH_PARALLELISM: usize = 64;
const DEFAULT_DIGEST_READ_CHUNK_BYTES: usize = 1 << 20;
const DEFAULT_TASK_QUEUE_CAPACITY_PER_WORKER: usize = 1;
const DEFAULT_RESULT_QUEUE_CAPACITY_PER_WORKER: usize = 2;

/// Pipeline saturation (in-flight tasks / merge capacity) at which the planner
/// stops starting new planning rounds. `1.0` means "only when the pipeline is
/// completely full", which is the safe default: an idle or lightly loaded node
/// is never gated. Lowering it defers planning earlier, trading compaction
/// aggressiveness for a short maintenance tick. `0.0` disables the gate.
const DEFAULT_COMPACTION_BACKPRESSURE_PRESSURE: f64 = 1.0;
/// Upper bound is exactly `1.0`: a value above it could never be reached and
/// would silently disable compaction planning altogether.
const MAX_COMPACTION_BACKPRESSURE_PRESSURE: f64 = 1.0;

// Practical upper bounds keep accepted operator overrides away from channel
// allocation failure, pathological maintenance batches, and excessive worker
// creation. They remain substantially above the defaults for larger hosts.
const MAX_AUTOMATIC_FLUSHES_PER_POLL: usize = 1024;
const MAX_COMPACTION_INPUTS_PER_TASK: usize = 256;
const MAX_SCHEDULED_TABLES_PER_POLL: usize = 1024;
const MAX_MAINTENANCE_DRAIN_BATCH: usize = 1024;
const MAX_AGE_FLUSH_FLOOR_BYTES: u64 = 1 << 40; // 1 TiB
const MAX_PER_COMPACTION_MEM_BUDGET_BYTES: u64 = 1 << 40; // 1 TiB
const MAX_AUTO_COMPACTION_PARALLELISM: usize = 64;
const MAX_FLUSH_PARALLELISM: usize = 256;
const MAX_DIGEST_READ_CHUNK_BYTES: usize = 64 * 1024 * 1024; // 64 MiB
const MAX_TASK_QUEUE_CAPACITY_PER_WORKER: usize = 32;
const MAX_RESULT_QUEUE_CAPACITY_PER_WORKER: usize = 32;

/// SSTable compression codec applied when a table schema does not select one
/// and `FERROSA_SSTABLE_COMPRESSION` is unset. Historical behaviour: LZ4.
const DEFAULT_SSTABLE_COMPRESSION_KIND: SstableCompressionKind = SstableCompressionKind::Lz4;
/// Zstd level used when zstd is selected and no level is configured. Matches
/// the level `compression_from_schema` previously hardcoded.
const DEFAULT_SSTABLE_ZSTD_LEVEL: i32 = 3;
/// Zstd's own supported level range (`ZSTD_minCLevel`..`ZSTD_maxCLevel`). Values
/// outside it are rejected rather than clamped, so an operator typo cannot
/// silently pick a different ratio.
const MIN_SSTABLE_ZSTD_LEVEL: i32 = -7;
const MAX_SSTABLE_ZSTD_LEVEL: i32 = 22;

/// Validated knobs that affect flush/compaction throughput and bounded memory.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StorageRuntimeTuning {
    pub max_automatic_flushes_per_poll: usize,
    pub max_compaction_inputs_per_task: usize,
    pub max_scheduled_tables_per_poll: usize,
    pub max_results_per_maintenance_poll: usize,
    pub max_age_flush_floor_bytes: u64,
    pub per_compaction_mem_budget_bytes: u64,
    pub max_auto_compaction_parallelism: usize,
    pub max_flush_parallelism: usize,
    pub digest_read_chunk_bytes: usize,
    /// Compaction pipeline depth and the planner backpressure threshold.
    pub compaction_pipeline: CompactionPipelineTuning,
    /// SSTable compression codec/level applied when a table schema does not
    /// select one. External knob for a host whose CPU/IOPS balance differs from
    /// the built-in LZ4 default — compress harder where CPU is plentiful and
    /// bandwidth is not (`FERROSA_SSTABLE_COMPRESSION`, `FERROSA_SSTABLE_ZSTD_LEVEL`).
    pub sstable_compression: SstableCompressionTuning,
}

/// SSTable compression defaults applied when a table schema does not select a
/// codec or a zstd level.
///
/// Grouped into their own `Copy` type so `StorageRuntimeTuning` stays under the
/// p0-oom-audit `copy-derive-large-type` field-count threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SstableCompressionTuning {
    pub kind: SstableCompressionKind,
    pub zstd_level: i32,
}

/// Codec selected by `FERROSA_SSTABLE_COMPRESSION` when a table schema does not
/// name one.
///
/// Deliberately carries **no** level: the zstd level comes from
/// [`SstableCompressionTuning::zstd_level`], so one setting cannot embed a level
/// the other setting cannot override.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SstableCompressionKind {
    None,
    Lz4,
    Zstd,
}

/// Compaction pipeline queue depths and the planner backpressure threshold.
///
/// Grouped into their own `Copy` type so `StorageRuntimeTuning` stays under the
/// p0-oom-audit `copy-derive-large-type` field-count threshold, rather than
/// carrying a waiver for the pipeline knobs it accumulated.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CompactionPipelineTuning {
    pub task_queue_capacity_per_worker: usize,
    pub result_queue_capacity_per_worker: usize,
    /// Planner backpressure threshold in `0.0..=1.0`; see the DEFAULT const.
    pub backpressure_pressure: f64,
}

static TUNING: OnceLock<StorageRuntimeTuning> = OnceLock::new();

impl StorageRuntimeTuning {
    fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            max_automatic_flushes_per_poll: read_usize(
                "FERROSA_MAX_AUTOMATIC_FLUSHES_PER_POLL",
                defaults.max_automatic_flushes_per_poll,
                1,
                MAX_AUTOMATIC_FLUSHES_PER_POLL,
            ),
            max_compaction_inputs_per_task: read_usize(
                "FERROSA_MAX_COMPACTION_INPUTS_PER_TASK",
                defaults.max_compaction_inputs_per_task,
                2,
                MAX_COMPACTION_INPUTS_PER_TASK,
            ),
            max_scheduled_tables_per_poll: read_usize(
                "FERROSA_MAX_SCHEDULED_TABLES_PER_POLL",
                defaults.max_scheduled_tables_per_poll,
                1,
                MAX_SCHEDULED_TABLES_PER_POLL,
            ),
            max_results_per_maintenance_poll: read_usize(
                "FERROSA_MAX_RESULTS_PER_MAINTENANCE_POLL",
                defaults.max_results_per_maintenance_poll,
                1,
                MAX_MAINTENANCE_DRAIN_BATCH,
            ),
            max_age_flush_floor_bytes: read_u64(
                "FERROSA_MAX_AGE_FLUSH_FLOOR_BYTES",
                defaults.max_age_flush_floor_bytes,
                1,
                MAX_AGE_FLUSH_FLOOR_BYTES,
            ),
            per_compaction_mem_budget_bytes: read_u64(
                "FERROSA_PER_COMPACTION_MEM_BUDGET_BYTES",
                defaults.per_compaction_mem_budget_bytes,
                1,
                MAX_PER_COMPACTION_MEM_BUDGET_BYTES,
            ),
            max_auto_compaction_parallelism: read_usize(
                "FERROSA_MAX_AUTO_COMPACTION_PARALLELISM",
                defaults.max_auto_compaction_parallelism,
                1,
                MAX_AUTO_COMPACTION_PARALLELISM,
            ),
            max_flush_parallelism: read_usize(
                "FERROSA_MAX_FLUSH_PARALLELISM",
                defaults.max_flush_parallelism,
                1,
                MAX_FLUSH_PARALLELISM,
            ),
            digest_read_chunk_bytes: read_usize(
                "FERROSA_DIGEST_READ_CHUNK_BYTES",
                defaults.digest_read_chunk_bytes,
                1,
                MAX_DIGEST_READ_CHUNK_BYTES,
            ),
            compaction_pipeline: CompactionPipelineTuning {
                task_queue_capacity_per_worker: read_usize(
                    "FERROSA_COMPACTION_TASK_QUEUE_CAPACITY_PER_WORKER",
                    defaults.compaction_pipeline.task_queue_capacity_per_worker,
                    1,
                    MAX_TASK_QUEUE_CAPACITY_PER_WORKER,
                ),
                result_queue_capacity_per_worker: read_usize(
                    "FERROSA_COMPACTION_RESULT_QUEUE_CAPACITY_PER_WORKER",
                    defaults
                        .compaction_pipeline
                        .result_queue_capacity_per_worker,
                    1,
                    MAX_RESULT_QUEUE_CAPACITY_PER_WORKER,
                ),
                backpressure_pressure: read_f64(
                    "FERROSA_COMPACTION_BACKPRESSURE_PRESSURE",
                    defaults.compaction_pipeline.backpressure_pressure,
                    0.0,
                    MAX_COMPACTION_BACKPRESSURE_PRESSURE,
                ),
            },
            sstable_compression: SstableCompressionTuning {
                kind: parse_sstable_compression_env(
                    "FERROSA_SSTABLE_COMPRESSION",
                    env::var("FERROSA_SSTABLE_COMPRESSION"),
                    defaults.sstable_compression.kind,
                ),
                zstd_level: parse_i32_env(
                    "FERROSA_SSTABLE_ZSTD_LEVEL",
                    env::var("FERROSA_SSTABLE_ZSTD_LEVEL"),
                    defaults.sstable_compression.zstd_level,
                    MIN_SSTABLE_ZSTD_LEVEL,
                    MAX_SSTABLE_ZSTD_LEVEL,
                ),
            },
        }
    }
}

impl Default for StorageRuntimeTuning {
    fn default() -> Self {
        Self {
            max_automatic_flushes_per_poll: DEFAULT_MAX_AUTOMATIC_FLUSHES_PER_POLL,
            max_compaction_inputs_per_task: DEFAULT_MAX_COMPACTION_INPUTS_PER_TASK,
            max_scheduled_tables_per_poll: DEFAULT_MAX_SCHEDULED_TABLES_PER_POLL,
            max_results_per_maintenance_poll: DEFAULT_MAINTENANCE_DRAIN_BATCH,
            max_age_flush_floor_bytes: DEFAULT_MAX_AGE_FLUSH_FLOOR_BYTES,
            per_compaction_mem_budget_bytes: DEFAULT_PER_COMPACTION_MEM_BUDGET_BYTES,
            max_auto_compaction_parallelism: DEFAULT_MAX_AUTO_COMPACTION_PARALLELISM,
            max_flush_parallelism: DEFAULT_MAX_FLUSH_PARALLELISM,
            digest_read_chunk_bytes: DEFAULT_DIGEST_READ_CHUNK_BYTES,
            compaction_pipeline: CompactionPipelineTuning {
                task_queue_capacity_per_worker: DEFAULT_TASK_QUEUE_CAPACITY_PER_WORKER,
                result_queue_capacity_per_worker: DEFAULT_RESULT_QUEUE_CAPACITY_PER_WORKER,
                backpressure_pressure: DEFAULT_COMPACTION_BACKPRESSURE_PRESSURE,
            },
            sstable_compression: SstableCompressionTuning {
                kind: DEFAULT_SSTABLE_COMPRESSION_KIND,
                zstd_level: DEFAULT_SSTABLE_ZSTD_LEVEL,
            },
        }
    }
}

/// Return the process-wide settings, parsing the environment only once.
pub(crate) fn storage_runtime_tuning() -> &'static StorageRuntimeTuning {
    TUNING.get_or_init(StorageRuntimeTuning::from_env)
}

pub(crate) fn read_usize(name: &str, default: usize, min: usize, max: usize) -> usize {
    parse_usize_env(name, env::var(name), default, min, max)
}

fn read_u64(name: &str, default: u64, min: u64, max: u64) -> u64 {
    parse_u64_env(name, env::var(name), default, min, max)
}

pub(crate) fn parse_usize_env(
    name: &str,
    value: Result<String, env::VarError>,
    default: usize,
    min: usize,
    max: usize,
) -> usize {
    match value {
        Err(env::VarError::NotPresent) => default,
        Err(error @ env::VarError::NotUnicode(_)) => {
            tracing::error!(setting = name, %error, default, "invalid storage runtime setting; using default");
            default
        }
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(parsed) if (min..=max).contains(&parsed) => parsed,
            _ => {
                tracing::error!(setting = name, value = %raw, min, max, default, "invalid storage runtime setting; using default");
                default
            }
        },
    }
}

fn parse_u64_env(
    name: &str,
    value: Result<String, env::VarError>,
    default: u64,
    min: u64,
    max: u64,
) -> u64 {
    match value {
        Err(env::VarError::NotPresent) => default,
        Err(error @ env::VarError::NotUnicode(_)) => {
            tracing::error!(setting = name, %error, default, "invalid storage runtime setting; using default");
            default
        }
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(parsed) if (min..=max).contains(&parsed) => parsed,
            _ => {
                tracing::error!(setting = name, value = %raw, min, max, default, "invalid storage runtime setting; using default");
                default
            }
        },
    }
}

fn read_f64(name: &str, default: f64, min: f64, max: f64) -> f64 {
    parse_f64_env(name, env::var(name), default, min, max)
}

fn parse_f64_env(
    name: &str,
    value: Result<String, env::VarError>,
    default: f64,
    min: f64,
    max: f64,
) -> f64 {
    match value {
        Err(env::VarError::NotPresent) => default,
        Err(error @ env::VarError::NotUnicode(_)) => {
            tracing::error!(setting = name, %error, default, "invalid storage runtime setting; using default");
            default
        }
        // `parse::<f64>` accepts `NaN`, which would compare false against every
        // threshold and silently disable the gate; reject non-finite values.
        Ok(raw) => match raw.trim().parse::<f64>() {
            Ok(parsed) if parsed.is_finite() && (min..=max).contains(&parsed) => parsed,
            _ => {
                tracing::error!(setting = name, value = %raw, min, max, default, "invalid storage runtime setting; using default");
                default
            }
        },
    }
}

fn parse_i32_env(
    name: &str,
    value: Result<String, env::VarError>,
    default: i32,
    min: i32,
    max: i32,
) -> i32 {
    match value {
        Err(env::VarError::NotPresent) => default,
        Err(error @ env::VarError::NotUnicode(_)) => {
            tracing::error!(setting = name, %error, default, "invalid storage runtime setting; using default");
            default
        }
        Ok(raw) => match raw.trim().parse::<i32>() {
            Ok(parsed) if (min..=max).contains(&parsed) => parsed,
            _ => {
                tracing::error!(setting = name, value = %raw, min, max, default, "invalid storage runtime setting; using default");
                default
            }
        },
    }
}

/// Parse the default SSTable codec selector.
///
/// A value that is *set but empty* is treated as unset and falls back to
/// `default`: `fly machine update --env KEY=` is the only way to clear a Fly
/// variable, so rejecting the empty string would make the knob impossible to
/// remove once set. Only genuinely unrecognized values are rejected loudly.
fn parse_sstable_compression_env(
    name: &str,
    value: Result<String, env::VarError>,
    default: SstableCompressionKind,
) -> SstableCompressionKind {
    match value {
        Err(env::VarError::NotPresent) => default,
        Err(error @ env::VarError::NotUnicode(_)) => {
            tracing::error!(setting = name, %error, "invalid storage runtime setting; using default");
            default
        }
        Ok(raw) => {
            let raw = raw.trim();
            if raw.is_empty() {
                return default;
            }
            match raw.to_ascii_lowercase().as_str() {
                "lz4" => SstableCompressionKind::Lz4,
                "zstd" => SstableCompressionKind::Zstd,
                "none" | "off" | "uncompressed" => SstableCompressionKind::None,
                _ => {
                    tracing::error!(
                        setting = name,
                        value = %raw,
                        "invalid storage runtime setting; expected lz4|zstd|none; using default"
                    );
                    default
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn usize_settings_use_defaults_when_absent_and_accept_bounded_overrides() {
        assert_eq!(
            parse_usize_env("TEST", Err(env::VarError::NotPresent), 8, 1, 64),
            8
        );
        assert_eq!(parse_usize_env("TEST", Ok(" 32 ".into()), 8, 1, 64), 32);
    }

    #[test]
    fn usize_settings_reject_invalid_and_too_low_values() {
        for raw in ["nope", "0", "18446744073709551616"] {
            assert_eq!(parse_usize_env("TEST", Ok(raw.into()), 8, 1, usize::MAX), 8);
        }
    }

    #[test]
    fn integer_settings_fall_back_when_environment_value_is_unreadable() {
        let error = env::VarError::NotUnicode(OsString::from("unreadable"));
        assert_eq!(parse_u64_env("TEST", Err(error), 16, 1, 100), 16);
    }

    #[test]
    fn byte_settings_accept_valid_overrides_and_reject_too_low_values() {
        assert_eq!(
            parse_u64_env("TEST", Ok("67108864".into()), 16, 1, u64::MAX,),
            64 * 1024 * 1024
        );
        assert_eq!(parse_u64_env("TEST", Ok("0".into()), 16, 1, u64::MAX), 16);
        assert_eq!(
            parse_u64_env("TEST", Ok("18446744073709551616".into()), 16, 1, u64::MAX,),
            16
        );
    }

    #[test]
    fn all_runtime_settings_keep_historical_defaults() {
        let tuning = StorageRuntimeTuning::default();
        assert_eq!(
            tuning.max_automatic_flushes_per_poll,
            DEFAULT_MAX_AUTOMATIC_FLUSHES_PER_POLL
        );
        assert_eq!(
            tuning.max_compaction_inputs_per_task,
            DEFAULT_MAX_COMPACTION_INPUTS_PER_TASK
        );
        assert_eq!(
            tuning.max_scheduled_tables_per_poll,
            DEFAULT_MAX_SCHEDULED_TABLES_PER_POLL
        );
        assert_eq!(
            tuning.max_results_per_maintenance_poll,
            DEFAULT_MAINTENANCE_DRAIN_BATCH
        );
        assert_eq!(
            tuning.max_age_flush_floor_bytes,
            DEFAULT_MAX_AGE_FLUSH_FLOOR_BYTES
        );
        assert_eq!(
            tuning.per_compaction_mem_budget_bytes,
            DEFAULT_PER_COMPACTION_MEM_BUDGET_BYTES
        );
        assert_eq!(
            tuning.max_auto_compaction_parallelism,
            DEFAULT_MAX_AUTO_COMPACTION_PARALLELISM
        );
        assert_eq!(tuning.max_flush_parallelism, DEFAULT_MAX_FLUSH_PARALLELISM);
        assert_eq!(
            tuning.digest_read_chunk_bytes,
            DEFAULT_DIGEST_READ_CHUNK_BYTES
        );
        assert_eq!(
            tuning.compaction_pipeline.task_queue_capacity_per_worker,
            DEFAULT_TASK_QUEUE_CAPACITY_PER_WORKER
        );
        assert_eq!(
            tuning.compaction_pipeline.result_queue_capacity_per_worker,
            DEFAULT_RESULT_QUEUE_CAPACITY_PER_WORKER
        );
        assert_eq!(
            tuning.sstable_compression.kind,
            DEFAULT_SSTABLE_COMPRESSION_KIND
        );
        assert_eq!(
            tuning.sstable_compression.zstd_level,
            DEFAULT_SSTABLE_ZSTD_LEVEL
        );
    }

    #[test]
    fn sstable_compression_kind_parses_case_insensitively_and_treats_empty_as_unset() {
        use SstableCompressionKind::*;
        let def = Lz4;
        assert_eq!(
            parse_sstable_compression_env("TEST", Err(env::VarError::NotPresent), def),
            def
        );
        // Empty is "unset", not an error: `fly machine update --env KEY=` is
        // the only way to clear a Fly variable, so empty must fall back.
        for raw in ["", "   "] {
            assert_eq!(
                parse_sstable_compression_env("TEST", Ok(raw.into()), def),
                def
            );
        }
        assert_eq!(
            parse_sstable_compression_env("TEST", Ok("LZ4".into()), def),
            Lz4
        );
        assert_eq!(
            parse_sstable_compression_env("TEST", Ok(" Zstd ".into()), def),
            Zstd
        );
        assert_eq!(
            parse_sstable_compression_env("TEST", Ok("NONE".into()), def),
            SstableCompressionKind::None
        );
        assert_eq!(
            parse_sstable_compression_env("TEST", Ok("off".into()), def),
            SstableCompressionKind::None
        );
    }

    #[test]
    fn sstable_compression_kind_rejects_unknown_values() {
        // An unrecognized codec must not silently become LZ4.
        for raw in ["snappy", "true", "lz4:9"] {
            assert_eq!(
                parse_sstable_compression_env("TEST", Ok(raw.into()), SstableCompressionKind::Zstd),
                SstableCompressionKind::Zstd,
                "{raw} is not a codec name; expected the default"
            );
        }
    }

    #[test]
    fn zstd_level_accepts_the_supported_range_and_rejects_outside_it() {
        assert_eq!(
            parse_i32_env("TEST", Err(env::VarError::NotPresent), 3, -7, 22),
            3
        );
        assert_eq!(parse_i32_env("TEST", Ok(" 9 ".into()), 3, -7, 22), 9);
        assert_eq!(parse_i32_env("TEST", Ok("-7".into()), 3, -7, 22), -7);
        assert_eq!(parse_i32_env("TEST", Ok("22".into()), 3, -7, 22), 22);
        for raw in ["23", "-8", "999", "fast", "", "3.5"] {
            assert_eq!(
                parse_i32_env("TEST", Ok(raw.into()), 3, -7, 22),
                3,
                "{raw} is outside the zstd range and must fall back"
            );
        }
    }

    #[test]
    fn usize_settings_reject_values_above_practical_limits() {
        for (max, raw) in [
            (MAX_AUTOMATIC_FLUSHES_PER_POLL, "1025"),
            (MAX_COMPACTION_INPUTS_PER_TASK, "257"),
            (MAX_SCHEDULED_TABLES_PER_POLL, "1025"),
            (MAX_MAINTENANCE_DRAIN_BATCH, "1025"),
            (MAX_AUTO_COMPACTION_PARALLELISM, "65"),
            (MAX_FLUSH_PARALLELISM, "257"),
            (MAX_DIGEST_READ_CHUNK_BYTES, "67108865"),
            (MAX_TASK_QUEUE_CAPACITY_PER_WORKER, "33"),
            (MAX_RESULT_QUEUE_CAPACITY_PER_WORKER, "33"),
        ] {
            assert_eq!(
                parse_usize_env("TEST", Ok(raw.into()), 1, 1, max),
                1,
                "{raw} must be rejected above {max}"
            );
            assert_eq!(parse_usize_env("TEST", Ok(max.to_string()), 1, 1, max), max);
        }
    }

    #[test]
    fn u64_settings_reject_values_above_practical_limits() {
        for max in [
            MAX_AGE_FLUSH_FLOOR_BYTES,
            MAX_PER_COMPACTION_MEM_BUDGET_BYTES,
        ] {
            assert_eq!(
                parse_u64_env("TEST", Ok((max + 1).to_string()), 1, 1, max),
                1
            );
            assert_eq!(parse_u64_env("TEST", Ok(max.to_string()), 1, 1, max), max);
        }
    }
}
