//! Module: Select and configure SSTable compaction work.
//! Correctness: Correct when strategy inputs stay bounded and retry settings normalize into safe ranges.
//! Last revised: 2026-09-27
//! Last changed: Add bounded environment-configured retry delays and digest pause limits.
//!
//! Compaction strategy trait and Size-Tiered implementation.
//!
//! The [`CompactionStrategy`] trait defines how to select SSTables for
//! compaction. [`SizeTieredStrategy`] groups SSTables by similar size
//! (within a configurable ratio of the bucket median) and triggers
//! compaction when a bucket reaches `min_threshold`.

use std::path::{Path, PathBuf};

use ferrosa_common::schema::TableSchema;

use super::metadata::{CompactionTask, SSTableMetadata};
use crate::TableId;

/// Selects which SSTables should be compacted together.
pub trait CompactionStrategy: Send + Sync {
    /// Given the current set of SSTables, return compaction tasks (if any).
    fn select(
        &self,
        sstables: &[SSTableMetadata],
        schema: &TableSchema,
        table_id: &TableId,
    ) -> Vec<CompactionTask>;
}

/// Configuration for STCS, populated from `FERROSA_COMPACTION_*` env vars.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum SSTables per bucket to trigger compaction.
    pub min_threshold: usize,
    /// Maximum SSTables per compaction task.
    pub max_threshold: usize,
    /// Maximum bytes of SSTable input selected for one compaction task.
    ///
    /// This caps memory pressure for large fan-in compactions independently of
    /// the input-file count cap.
    pub max_compaction_bytes: u64,
    /// Lower bound of the size ratio for bucket membership.
    pub bucket_low: f64,
    /// Upper bound of the size ratio for bucket membership.
    pub bucket_high: f64,
    /// Directory for compaction output.
    pub output_dir: PathBuf,
    /// Initial delay before retrying a digest or verification failure.
    pub retry_backoff_initial: std::time::Duration,
    /// Maximum delay between digest or verification retries.
    pub retry_backoff_max: std::time::Duration,
    /// Consecutive digest or verification failures that pause table compaction.
    pub retry_digest_failure_limit: u32,
}

impl CompactionConfig {
    /// Reads compaction config from `FERROSA_COMPACTION_*` environment variables.
    pub fn from_env(output_dir: PathBuf) -> Self {
        let min_threshold = std::env::var("FERROSA_COMPACTION_MIN_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4);
        let max_threshold = std::env::var("FERROSA_COMPACTION_MAX_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(32);
        let max_compaction_bytes = std::env::var("FERROSA_COMPACTION_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(512 * 1024 * 1024);
        let bucket_low = std::env::var("FERROSA_COMPACTION_BUCKET_LOW")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.5);
        let bucket_high = std::env::var("FERROSA_COMPACTION_BUCKET_HIGH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1.5);
        let retry_backoff_initial = duration_from_env(
            "FERROSA_COMPACTION_RETRY_BACKOFF_INITIAL_MS",
            1_000,
            1,
            60_000,
        );
        let retry_backoff_max = duration_from_env(
            "FERROSA_COMPACTION_RETRY_BACKOFF_MAX_MS",
            60_000,
            1,
            600_000,
        );
        let retry_backoff_max = if retry_backoff_max < retry_backoff_initial {
            tracing::warn!(
                initial_ms = retry_backoff_initial.as_millis(),
                max_ms = retry_backoff_max.as_millis(),
                "compaction retry max backoff is below initial backoff; normalizing max to initial"
            );
            retry_backoff_initial
        } else {
            retry_backoff_max
        };
        let retry_digest_failure_limit =
            positive_u32_from_env("FERROSA_COMPACTION_DIGEST_FAILURE_LIMIT", 3, 1, 100);

        Self {
            min_threshold,
            max_threshold,
            max_compaction_bytes,
            bucket_low,
            bucket_high,
            output_dir,
            retry_backoff_initial,
            retry_backoff_max,
            retry_digest_failure_limit,
        }
    }
}

fn duration_from_env(name: &str, default_ms: u64, min_ms: u64, max_ms: u64) -> std::time::Duration {
    match std::env::var(name) {
        Ok(value) => duration_from_value(name, &value, default_ms, min_ms, max_ms),
        Err(std::env::VarError::NotPresent) => std::time::Duration::from_millis(default_ms),
        Err(error) => {
            tracing::error!(%error, variable = name, default_ms, "invalid compaction retry configuration; using default");
            std::time::Duration::from_millis(default_ms)
        }
    }
}

fn duration_from_value(
    name: &str,
    value: &str,
    default_ms: u64,
    min_ms: u64,
    max_ms: u64,
) -> std::time::Duration {
    let parsed = match value.parse::<u64>() {
        Ok(parsed) => parsed,
        Err(error) => {
            tracing::error!(%error, variable = name, value, default_ms, "invalid compaction retry configuration; using default");
            return std::time::Duration::from_millis(default_ms);
        }
    };
    let normalized = parsed.clamp(min_ms, max_ms);
    if normalized != parsed {
        tracing::warn!(
            variable = name,
            configured_ms = parsed,
            normalized_ms = normalized,
            "compaction retry configuration was outside its supported range"
        );
    }
    std::time::Duration::from_millis(normalized)
}

fn positive_u32_from_env(name: &str, default: u32, min: u32, max: u32) -> u32 {
    match std::env::var(name) {
        Ok(value) => positive_u32_from_value(name, &value, default, min, max),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => {
            tracing::error!(%error, variable = name, default, "invalid compaction retry configuration; using default");
            default
        }
    }
}

fn positive_u32_from_value(name: &str, value: &str, default: u32, min: u32, max: u32) -> u32 {
    let parsed = match value.parse::<u32>() {
        Ok(parsed) => parsed,
        Err(error) => {
            tracing::error!(%error, variable = name, value, default, "invalid compaction retry configuration; using default");
            return default;
        }
    };
    let normalized = parsed.clamp(min, max);
    if normalized != parsed {
        tracing::warn!(
            variable = name,
            configured = parsed,
            normalized,
            "compaction retry configuration was outside its supported range"
        );
    }
    normalized
}

/// Size-Tiered Compaction Strategy.
///
/// Groups SSTables into buckets by similar size. A bucket is formed when
/// all SSTables in it have sizes within `[bucket_low, bucket_high]` of the
/// bucket's median size. When a bucket has at least `min_threshold` SSTables,
/// a compaction task is emitted.
pub struct SizeTieredStrategy {
    config: CompactionConfig,
}

impl SizeTieredStrategy {
    pub fn new(config: CompactionConfig) -> Self {
        Self { config }
    }

    /// Groups SSTables into size-based buckets.
    ///
    /// Algorithm:
    /// 1. Sort by size ascending.
    /// 2. For each SSTable, check if it fits in the current bucket (within
    ///    `[bucket_low, bucket_high]` of the bucket median).
    /// 3. If not, start a new bucket.
    fn bucket_sstables<'a>(
        &self,
        sstables: &'a [SSTableMetadata],
    ) -> Vec<Vec<&'a SSTableMetadata>> {
        if sstables.is_empty() {
            return Vec::new();
        }

        let mut sorted: Vec<&SSTableMetadata> = sstables.iter().collect();
        sorted.sort_by_key(|s| s.size_bytes);

        let mut buckets: Vec<Vec<&SSTableMetadata>> = Vec::new();
        let mut current_bucket: Vec<&SSTableMetadata> = vec![sorted[0]];

        for sst in &sorted[1..] {
            // `sorted` is ascending and every bucket is a contiguous run of it,
            // so `current_bucket` is itself in ascending size order. Its median
            // is therefore an O(1) index lookup; recomputing it by re-sorting
            // the bucket on every element made a single planning round quadratic.
            let median = median_of_sorted_run(&current_bucket);
            // When median is 0 (size not yet tracked), group all zero-size SSTables
            // together — they are homogeneous and should compact as one bucket.
            let in_bucket = if median == 0.0 {
                sst.size_bytes == 0
            } else {
                let ratio = sst.size_bytes as f64 / median;
                ratio >= self.config.bucket_low && ratio <= self.config.bucket_high
            };

            if in_bucket {
                current_bucket.push(sst);
            } else {
                buckets.push(std::mem::take(&mut current_bucket));
                current_bucket.push(sst);
            }
        }

        if !current_bucket.is_empty() {
            buckets.push(current_bucket);
        }

        buckets
    }
}

impl CompactionStrategy for SizeTieredStrategy {
    fn select(
        &self,
        sstables: &[SSTableMetadata],
        schema: &TableSchema,
        table_id: &TableId,
    ) -> Vec<CompactionTask> {
        let buckets = self.bucket_sstables(sstables);
        let mut tasks = Vec::new();

        for bucket in buckets {
            if bucket.len() >= self.config.min_threshold {
                let mut input_bytes = 0_u64;
                let mut inputs = Vec::new();
                for sstable in bucket {
                    let next_bytes = input_bytes.saturating_add(sstable.size_bytes);
                    if !inputs.is_empty() && next_bytes > self.config.max_compaction_bytes {
                        if inputs.len() >= self.config.min_threshold {
                            tasks.push(CompactionTask {
                                inputs: std::mem::take(&mut inputs),
                                output_dir: self.config.output_dir.join(table_id.to_string()),
                                schema: schema.clone(),
                                table_id: table_id.clone(),
                                purge: None,
                            });
                        } else {
                            inputs.clear();
                        }
                        input_bytes = 0;
                    }

                    input_bytes = input_bytes.saturating_add(sstable.size_bytes);
                    inputs.push(sstable.clone());

                    if inputs.len() >= self.config.max_threshold {
                        tasks.push(CompactionTask {
                            inputs: std::mem::take(&mut inputs),
                            output_dir: self.config.output_dir.join(table_id.to_string()),
                            schema: schema.clone(),
                            table_id: table_id.clone(),
                            purge: None,
                        });
                        input_bytes = 0;
                    }
                }

                if inputs.len() >= self.config.min_threshold {
                    tasks.push(CompactionTask {
                        inputs,
                        output_dir: self.config.output_dir.join(table_id.to_string()),
                        schema: schema.clone(),
                        table_id: table_id.clone(),
                        purge: None,
                    });
                }
            }
        }

        tasks
    }
}

/// Rewrite tasks for legacy-format SSTables, independent of the size-tier
/// strategy (t_a0f922a3).
///
/// A legacy Cassandra-format SSTable (`legacy_format == true`: key bounds do
/// not decode as byte-comparable) stores a wide partition's rows in an order
/// the streaming fragment read path mis-handles — the paged scan silently
/// under-delivers. Size-tiered/UCS bucketing groups by SIZE, so a lone legacy
/// file sits in its own tier and is NEVER selected, leaving the mis-sorted data
/// on disk indefinitely. This selects every legacy-format file for a rewrite
/// regardless of tier: compaction runs `merge::ensure_partition_rows_sorted` on
/// every output partition, so even a single-input rewrite re-sorts each
/// partition and writes byte-comparable, monotonic output.
///
/// Chunked by the SAME bounds as the size-tiered strategy (`max_threshold`
/// files, `max_compaction_bytes` bytes) so a repair never builds one unbounded
/// mega-task, and each chunk is a SEPARATE task so an input-overlap skip in the
/// executor drops only that chunk — never the whole repair set. A single legacy
/// file larger than `max_compaction_bytes` still gets its own task (we never
/// drop a legacy file). Self-terminating: once rewritten, no legacy files
/// remain and no further task is produced.
pub fn legacy_rewrite_tasks(
    sstables: &[SSTableMetadata],
    schema: &TableSchema,
    table_id: &TableId,
    output_dir: &Path,
    max_threshold: usize,
    max_compaction_bytes: u64,
) -> Vec<CompactionTask> {
    let legacy: Vec<&SSTableMetadata> = sstables.iter().filter(|s| s.legacy_format).collect();
    if legacy.is_empty() {
        return Vec::new();
    }
    let file_cap = max_threshold.max(1);
    let mut tasks = Vec::new();
    let mut inputs: Vec<SSTableMetadata> = Vec::new();
    let mut chunk_bytes: u64 = 0;
    let mk = |inputs: Vec<SSTableMetadata>| CompactionTask {
        inputs,
        output_dir: output_dir.join(table_id.to_string()),
        schema: schema.clone(),
        table_id: table_id.clone(),
        purge: None,
    };
    for sst in legacy {
        // Close the current chunk before it would exceed either bound — but
        // only when it already holds a file, so a single oversized legacy file
        // still gets its own (1-input) rewrite rather than being dropped.
        let would_exceed = inputs.len() >= file_cap
            || chunk_bytes.saturating_add(sst.size_bytes) > max_compaction_bytes;
        if !inputs.is_empty() && would_exceed {
            tasks.push(mk(std::mem::take(&mut inputs)));
            chunk_bytes = 0;
        }
        chunk_bytes = chunk_bytes.saturating_add(sst.size_bytes);
        inputs.push((*sst).clone());
    }
    if !inputs.is_empty() {
        tasks.push(mk(inputs));
    }
    tasks
}

/// Computes the median size of a bucket of SSTables.
///
/// The bucket must be in ascending `size_bytes` order, which `bucket_sstables`
/// guarantees: it pushes elements in ascending order and only ever starts a new
/// run. The median is then the middle element (or the mean of the two middle
/// elements) at an O(1) index cost, so a full planning pass stays O(n log n)
/// for the initial sort. The earlier implementation allocated a `Vec<u64>` and
/// `sort_unstable`ed it on every call; because it was called once per element it
/// made a single `select()` quadratic, and `maybe_compact` runs per flush and on
/// the 10 s maintenance tick.
fn median_of_sorted_run(bucket: &[&SSTableMetadata]) -> f64 {
    if bucket.is_empty() {
        return 0.0;
    }
    let mid = bucket.len() / 2;
    if bucket.len().is_multiple_of(2) {
        (bucket[mid - 1].size_bytes + bucket[mid].size_bytes) as f64 / 2.0
    } else {
        bucket[mid].size_bytes as f64
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::schema::TableSchema;
    use std::path::PathBuf;

    fn make_metadata(id: &str, size: u64) -> SSTableMetadata {
        SSTableMetadata {
            id: id.to_string(),
            path: PathBuf::from(format!("/tmp/{id}")),
            size_bytes: size,
            min_token: -100,
            max_token: 100,
            min_timestamp: 1000,
            max_timestamp: 2000,
            partition_count: 10,
            legacy_format: false,
        }
    }

    fn test_config() -> CompactionConfig {
        CompactionConfig {
            min_threshold: 4,
            max_threshold: 32,
            max_compaction_bytes: 512 * 1024 * 1024,
            bucket_low: 0.5,
            bucket_high: 1.5,
            output_dir: PathBuf::from("/tmp/compaction"),
            retry_backoff_initial: std::time::Duration::from_secs(1),
            retry_backoff_max: std::time::Duration::from_secs(60),
            retry_digest_failure_limit: 3,
        }
    }

    fn test_table_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: Default::default(),
        }
    }

    fn test_table_id() -> crate::TableId {
        crate::TableId::new("test_ks", "test_table")
    }

    fn make_legacy(id: &str, size: u64) -> SSTableMetadata {
        SSTableMetadata {
            legacy_format: true,
            ..make_metadata(id, size)
        }
    }

    /// A lone legacy-format SSTable sits in its own size tier, so the size-tiered
    /// strategy never selects it — but it MUST still be scheduled for a rewrite,
    /// or its mis-sorted rows silently break paged reads forever (t_a0f922a3).
    #[test]
    fn lone_legacy_sstable_scheduled_for_rewrite_regardless_of_tier() {
        let strategy = SizeTieredStrategy::new(test_config());
        // One legacy file at a unique size, plus a healthy same-size bucket that
        // does NOT include it. STCS emits a task for the healthy bucket only.
        let sstables = vec![
            make_legacy("legacy", 830_000),
            make_metadata("a", 1000),
            make_metadata("b", 1100),
            make_metadata("c", 900),
            make_metadata("d", 1050),
        ];
        let stcs = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        assert!(
            stcs.iter()
                .all(|t| t.inputs.iter().all(|i| i.id != "legacy")),
            "size-tiered strategy must NOT pull the lone legacy file into a size bucket"
        );

        let rewrite = legacy_rewrite_tasks(
            &sstables,
            &test_table_schema(),
            &test_table_id(),
            &PathBuf::from("/tmp/compaction"),
            32,
            512 * 1024 * 1024,
        );
        assert_eq!(rewrite.len(), 1, "one rewrite task for the legacy file");
        assert_eq!(rewrite[0].inputs.len(), 1);
        assert_eq!(rewrite[0].inputs[0].id, "legacy");
    }

    #[test]
    fn no_legacy_files_produce_no_rewrite_task() {
        let sstables = vec![make_metadata("a", 1000), make_metadata("b", 2000)];
        let rewrite = legacy_rewrite_tasks(
            &sstables,
            &test_table_schema(),
            &test_table_id(),
            &PathBuf::from("/tmp/compaction"),
            32,
            512 * 1024 * 1024,
        );
        assert!(rewrite.is_empty(), "no legacy files → no rewrite task");
    }

    #[test]
    fn multiple_legacy_files_within_bounds_share_one_task() {
        let sstables = vec![
            make_legacy("l1", 800_000),
            make_metadata("healthy", 1000),
            make_legacy("l2", 300_000),
        ];
        let rewrite = legacy_rewrite_tasks(
            &sstables,
            &test_table_schema(),
            &test_table_id(),
            &PathBuf::from("/tmp/compaction"),
            32,
            512 * 1024 * 1024,
        );
        assert_eq!(rewrite.len(), 1);
        let mut ids: Vec<&str> = rewrite[0].inputs.iter().map(|i| i.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec!["l1", "l2"],
            "only legacy inputs, healthy excluded"
        );
    }

    /// Finding 2: legacy rewrites must respect max_threshold / max_compaction_bytes
    /// and split into SEPARATE tasks, so a repair is never one unbounded mega-task
    /// and an executor input-overlap skip drops only one chunk.
    #[test]
    fn legacy_rewrites_chunk_by_count_and_size_bounds() {
        // Count bound: max_threshold=2 over 5 legacy files → 3 tasks (2,2,1).
        let five: Vec<SSTableMetadata> = (0..5)
            .map(|i| make_legacy(&format!("l{i}"), 1000))
            .collect();
        let by_count = legacy_rewrite_tasks(
            &five,
            &test_table_schema(),
            &test_table_id(),
            &PathBuf::from("/tmp/compaction"),
            2,
            512 * 1024 * 1024,
        );
        assert_eq!(by_count.len(), 3, "5 files / cap 2 → 3 tasks");
        assert_eq!(
            by_count.iter().map(|t| t.inputs.len()).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );

        // Byte bound: each file 400 bytes, cap 1000 → 2 files/chunk.
        let four: Vec<SSTableMetadata> =
            (0..4).map(|i| make_legacy(&format!("b{i}"), 400)).collect();
        let by_bytes = legacy_rewrite_tasks(
            &four,
            &test_table_schema(),
            &test_table_id(),
            &PathBuf::from("/tmp/compaction"),
            32,
            1000,
        );
        assert_eq!(by_bytes.len(), 2, "4×400B / 1000B cap → 2 tasks of 2");
        assert!(by_bytes.iter().all(|t| t.inputs.len() == 2));
    }

    /// A single legacy file larger than max_compaction_bytes is never dropped —
    /// it still gets its own 1-input rewrite task.
    #[test]
    fn oversized_lone_legacy_file_still_gets_its_own_task() {
        let sstables = vec![make_legacy("huge", 2 * 1024 * 1024 * 1024)];
        let rewrite = legacy_rewrite_tasks(
            &sstables,
            &test_table_schema(),
            &test_table_id(),
            &PathBuf::from("/tmp/compaction"),
            32,
            512 * 1024 * 1024,
        );
        assert_eq!(rewrite.len(), 1);
        assert_eq!(rewrite[0].inputs[0].id, "huge");
    }

    #[test]
    fn four_similar_sizes_trigger_compaction() {
        let strategy = SizeTieredStrategy::new(test_config());
        let sstables = vec![
            make_metadata("a", 1000),
            make_metadata("b", 1100),
            make_metadata("c", 900),
            make_metadata("d", 1050),
        ];

        let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].inputs.len(), 4);
    }

    /// Regression: one planning round must not recompute the bucket median by
    /// re-sorting the bucket for every element.
    ///
    /// `bucket_sstables` walks the size-sorted SSTables once, so the bucket it
    /// is building is always in ascending size order and its median is an O(1)
    /// index lookup. The previous implementation collected the bucket's sizes
    /// into a fresh `Vec<u64>` and `sort_unstable`ed it on *every* iteration,
    /// making a single round quadratic. Measured in a debug build: 5k SSTables
    /// 110 ms, 10k 429 ms, 20k 1 707 ms, 40k 7 152 ms — 4x per doubling. A table
    /// in the husk state (see `engine.rs` retire comment) reaches tens of
    /// thousands of SSTables, and `maybe_compact` runs after every flush and on
    /// the 10 s maintenance tick, so the planner burns its slice on the thread
    /// that would otherwise submit work.
    #[test]
    fn bucket_sstables_does_not_resort_per_element() {
        let strategy = SizeTieredStrategy::new(test_config());
        const N: usize = 20_000;
        // Sizes within bucket_low..=bucket_high of one another form a single
        // bucket: the worst case for a per-element median recomputation.
        let sstables: Vec<SSTableMetadata> = (0..N)
            .map(|i| make_metadata(&format!("s{i:05}"), 1000 + (i as u64 % 10)))
            .collect();

        let start = std::time::Instant::now();
        let buckets = strategy.bucket_sstables(&sstables);
        let elapsed = start.elapsed();

        assert_eq!(buckets.len(), 1, "similar sizes must form a single bucket");
        assert_eq!(buckets[0].len(), N);
        assert!(
            elapsed < std::time::Duration::from_millis(300),
            "bucket_sstables took {elapsed:?} for {N} SSTables; the bucket median \
             must not be recomputed by re-sorting the bucket for every element"
        );
    }

    /// The linear median lookup must partition exactly as the original
    /// sort-per-element algorithm did, including the zero-size grouping branch
    /// and inputs that straddle several tiers.
    #[test]
    fn bucket_partitioning_matches_reference_algorithm() {
        let config = test_config();
        let strategy = SizeTieredStrategy::new(config.clone());
        let mut rng = 0x2545_F491_4F6C_DD1D_u64;
        let mut multi_bucket_cases = 0;

        for case in 0..200 {
            let count = 1 + (next_rand(&mut rng) % 64) as usize;
            let sstables: Vec<SSTableMetadata> = (0..count)
                .map(|i| {
                    // ~1 in 8 is empty (exercises the median == 0.0 branch);
                    // the rest spread over five decades so buckets split often.
                    let size = if next_rand(&mut rng).is_multiple_of(8) {
                        0
                    } else {
                        let magnitude = next_rand(&mut rng) % 5;
                        10u64.pow(magnitude as u32) * (10 + next_rand(&mut rng) % 90)
                    };
                    make_metadata(&format!("s{i}"), size)
                })
                .collect();

            let got: Vec<Vec<String>> = strategy
                .bucket_sstables(&sstables)
                .into_iter()
                .map(|bucket| bucket.into_iter().map(|s| s.id.clone()).collect())
                .collect();
            let want = reference_bucket_sstables(&config, &sstables);
            if want.len() > 1 {
                multi_bucket_cases += 1;
            }
            assert_eq!(
                got, want,
                "bucketing diverged from the reference in case {case}"
            );
        }

        assert!(
            multi_bucket_cases > 50,
            "the oracle must actually exercise multi-bucket splits, got {multi_bucket_cases}"
        );
    }

    /// Test oracle only: the original algorithm, which sorted the buckets it
    /// built by *size* and recomputed the median from scratch per element.
    fn reference_bucket_sstables(
        config: &CompactionConfig,
        sstables: &[SSTableMetadata],
    ) -> Vec<Vec<String>> {
        if sstables.is_empty() {
            return Vec::new();
        }
        let mut sorted: Vec<&SSTableMetadata> = sstables.iter().collect();
        sorted.sort_by_key(|s| s.size_bytes);
        let mut buckets: Vec<Vec<String>> = Vec::new();
        let mut current: Vec<&SSTableMetadata> = vec![sorted[0]];
        for sst in &sorted[1..] {
            let mut sizes: Vec<u64> = current.iter().map(|s| s.size_bytes).collect();
            sizes.sort_unstable();
            let median = if sizes.is_empty() {
                0.0
            } else {
                let mid = sizes.len() / 2;
                if sizes.len().is_multiple_of(2) {
                    (sizes[mid - 1] + sizes[mid]) as f64 / 2.0
                } else {
                    sizes[mid] as f64
                }
            };
            let in_bucket = if median == 0.0 {
                sst.size_bytes == 0
            } else {
                let ratio = sst.size_bytes as f64 / median;
                ratio >= config.bucket_low && ratio <= config.bucket_high
            };
            if in_bucket {
                current.push(sst);
            } else {
                buckets.push(current.iter().map(|s| s.id.clone()).collect());
                current = vec![sst];
            }
        }
        buckets.push(current.iter().map(|s| s.id.clone()).collect());
        buckets
    }

    fn next_rand(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    #[test]
    fn two_size_groups_two_tasks() {
        let strategy = SizeTieredStrategy::new(test_config());
        let sstables = vec![
            // Small group
            make_metadata("s1", 100),
            make_metadata("s2", 110),
            make_metadata("s3", 90),
            make_metadata("s4", 105),
            // Large group
            make_metadata("l1", 10000),
            make_metadata("l2", 11000),
            make_metadata("l3", 9000),
            make_metadata("l4", 10500),
        ];

        let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        assert_eq!(tasks.len(), 2);
    }

    #[test]
    fn below_threshold_no_tasks() {
        let strategy = SizeTieredStrategy::new(test_config());
        let sstables = vec![
            make_metadata("a", 1000),
            make_metadata("b", 1100),
            make_metadata("c", 900),
        ];

        let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        assert!(tasks.is_empty());
    }

    #[test]
    fn max_threshold_caps_inputs() {
        let config = CompactionConfig {
            min_threshold: 2,
            max_threshold: 3,
            ..test_config()
        };
        let strategy = SizeTieredStrategy::new(config);
        let sstables = vec![
            make_metadata("a", 1000),
            make_metadata("b", 1100),
            make_metadata("c", 900),
            make_metadata("d", 1050),
            make_metadata("e", 950),
        ];

        let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].inputs.len(), 3); // capped at max_threshold
        assert_eq!(tasks[1].inputs.len(), 2); // remaining non-overlapping inputs compact too
    }

    #[test]
    fn large_bucket_is_capped_by_default_compaction_bytes() {
        let strategy = SizeTieredStrategy::new(test_config());
        let sstable_size = 64 * 1024 * 1024;
        let sstables: Vec<_> = (0..20)
            .map(|idx| make_metadata(&format!("sst-{idx}"), sstable_size))
            .collect();

        let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());

        assert!(tasks.len() > 1);
        for task in &tasks {
            let input_bytes: u64 = task.inputs.iter().map(|input| input.size_bytes).sum();
            assert!(
                input_bytes <= 512 * 1024 * 1024,
                "selected {} bytes across {} inputs; compaction must stay below the per-task byte cap to avoid container OOM",
                input_bytes,
                task.inputs.len()
            );
        }
    }

    #[test]
    fn large_bucket_emits_non_overlapping_parallel_tasks() {
        let config = CompactionConfig {
            min_threshold: 2,
            max_threshold: 4,
            ..test_config()
        };
        let strategy = SizeTieredStrategy::new(config);
        let sstables: Vec<_> = (0..8)
            .map(|idx| make_metadata(&format!("sst-{idx}"), 1000 + idx))
            .collect();

        let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());

        assert_eq!(tasks.len(), 2);
        let mut seen = std::collections::HashSet::new();
        for input in tasks.iter().flat_map(|task| task.inputs.iter()) {
            assert!(
                seen.insert(input.id.clone()),
                "input scheduled twice: {}",
                input.id
            );
        }
        assert_eq!(seen.len(), 8);
    }

    #[test]
    fn empty_input_no_tasks() {
        let strategy = SizeTieredStrategy::new(test_config());
        let tasks = strategy.select(&[], &test_table_schema(), &test_table_id());
        assert!(tasks.is_empty());
    }

    #[test]
    fn deterministic_selection() {
        let strategy = SizeTieredStrategy::new(test_config());
        let sstables = vec![
            make_metadata("a", 1000),
            make_metadata("b", 1100),
            make_metadata("c", 900),
            make_metadata("d", 1050),
            make_metadata("e", 5000),
            make_metadata("f", 5500),
        ];

        let tasks1 = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        let tasks2 = strategy.select(&sstables, &test_table_schema(), &test_table_id());

        assert_eq!(tasks1.len(), tasks2.len());
        for (t1, t2) in tasks1.iter().zip(tasks2.iter()) {
            let ids1: Vec<&str> = t1.inputs.iter().map(|i| i.id.as_str()).collect();
            let ids2: Vec<&str> = t2.inputs.iter().map(|i| i.id.as_str()).collect();
            assert_eq!(ids1, ids2);
        }
    }

    #[test]
    fn compaction_backoff_config_invalid_values_use_defaults() {
        assert_eq!(
            duration_from_value("test", "broken", 1_000, 1, 60_000),
            std::time::Duration::from_millis(1_000)
        );
        assert_eq!(positive_u32_from_value("test", "broken", 3, 1, 100), 3);
    }

    #[test]
    fn compaction_backoff_config_normalizes_out_of_range_values() {
        assert_eq!(
            duration_from_value("test", "0", 1_000, 10, 60_000),
            std::time::Duration::from_millis(10)
        );
        assert_eq!(positive_u32_from_value("test", "0", 3, 1, 100), 1);
        assert_eq!(positive_u32_from_value("test", "200", 3, 1, 100), 100);
    }
}
