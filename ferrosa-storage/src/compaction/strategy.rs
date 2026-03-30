//! Compaction strategy trait and Size-Tiered implementation.
//!
//! The [`CompactionStrategy`] trait defines how to select SSTables for
//! compaction. [`SizeTieredStrategy`] groups SSTables by similar size
//! (within a configurable ratio of the bucket median) and triggers
//! compaction when a bucket reaches `min_threshold`.

use std::path::PathBuf;

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
    /// Lower bound of the size ratio for bucket membership.
    pub bucket_low: f64,
    /// Upper bound of the size ratio for bucket membership.
    pub bucket_high: f64,
    /// Directory for compaction output.
    pub output_dir: PathBuf,
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
        let bucket_low = std::env::var("FERROSA_COMPACTION_BUCKET_LOW")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.5);
        let bucket_high = std::env::var("FERROSA_COMPACTION_BUCKET_HIGH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1.5);

        Self {
            min_threshold,
            max_threshold,
            bucket_low,
            bucket_high,
            output_dir,
        }
    }
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
            let median = bucket_median(&current_bucket);
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
                let count = bucket.len().min(self.config.max_threshold);
                let inputs: Vec<SSTableMetadata> =
                    bucket.into_iter().take(count).cloned().collect();
                tasks.push(CompactionTask {
                    inputs,
                    output_dir: self.config.output_dir.clone(),
                    schema: schema.clone(),
                    table_id: table_id.clone(),
                });
            }
        }

        tasks
    }
}

/// Computes the median size of a bucket of SSTables.
fn bucket_median(bucket: &[&SSTableMetadata]) -> f64 {
    if bucket.is_empty() {
        return 0.0;
    }
    let mut sizes: Vec<u64> = bucket.iter().map(|s| s.size_bytes).collect();
    sizes.sort_unstable();
    let mid = sizes.len() / 2;
    if sizes.len().is_multiple_of(2) {
        (sizes[mid - 1] + sizes[mid]) as f64 / 2.0
    } else {
        sizes[mid] as f64
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
        }
    }

    fn test_config() -> CompactionConfig {
        CompactionConfig {
            min_threshold: 4,
            max_threshold: 32,
            bucket_low: 0.5,
            bucket_high: 1.5,
            output_dir: PathBuf::from("/tmp/compaction"),
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
        }
    }

    fn test_table_id() -> crate::TableId {
        crate::TableId::new("test_ks", "test_table")
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
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].inputs.len(), 3); // capped at max_threshold
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
}
