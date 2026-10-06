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
