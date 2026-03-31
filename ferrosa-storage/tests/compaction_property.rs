//! Property tests for STCS and UCS compaction strategies.

use std::path::PathBuf;

use proptest::prelude::*;

use ferrosa_common::schema::TableSchema;
use ferrosa_storage::compaction::metadata::SSTableMetadata;
use ferrosa_storage::compaction::strategy::{
    CompactionConfig, CompactionStrategy, SizeTieredStrategy,
};
use ferrosa_storage::compaction::UnifiedCompactionStrategy;
use ferrosa_storage::TableId;

fn default_config() -> CompactionConfig {
    CompactionConfig {
        min_threshold: 4,
        max_threshold: 32,
        bucket_low: 0.5,
        bucket_high: 1.5,
        output_dir: PathBuf::from("/tmp/compaction"),
    }
}

fn make_metadata(id: &str, size: u64) -> SSTableMetadata {
    SSTableMetadata {
        id: id.to_string(),
        path: PathBuf::from(format!("/tmp/{id}")),
        size_bytes: size,
        min_token: -1000,
        max_token: 1000,
        min_timestamp: 0,
        max_timestamp: 1000,
        partition_count: 100,
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

fn test_table_id() -> TableId {
    TableId::new("test_ks", "test_table")
}

// Property: STCS bucket selection is deterministic.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn stcs_deterministic(
        sizes in prop::collection::vec(1_000u64..1_000_000, 4..=20),
    ) {
        let config = default_config();
        let strategy = SizeTieredStrategy::new(config);

        let sstables: Vec<_> = sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| make_metadata(&format!("sst_{i}"), size))
            .collect();

        let tasks1 = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        let tasks2 = strategy.select(&sstables, &test_table_schema(), &test_table_id());

        // Same input → same output.
        prop_assert_eq!(tasks1.len(), tasks2.len());
        for (t1, t2) in tasks1.iter().zip(tasks2.iter()) {
            let ids1: Vec<_> = t1.inputs.iter().map(|m| &m.id).collect();
            let ids2: Vec<_> = t2.inputs.iter().map(|m| &m.id).collect();
            prop_assert_eq!(ids1, ids2);
        }
    }
}

// Property: compaction tasks only include SSTables from the input set.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn tasks_subset_of_input(
        sizes in prop::collection::vec(1_000u64..1_000_000, 4..=20),
    ) {
        let config = default_config();
        let strategy = SizeTieredStrategy::new(config);

        let sstables: Vec<_> = sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| make_metadata(&format!("sst_{i}"), size))
            .collect();

        let all_ids: std::collections::HashSet<_> =
            sstables.iter().map(|m| &m.id).collect();

        let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        for task in &tasks {
            for input in &task.inputs {
                prop_assert!(
                    all_ids.contains(&input.id),
                    "task contains unknown SSTable: {}",
                    input.id
                );
            }
        }
    }
}

// Property: each compaction task has at least min_threshold inputs.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn task_meets_min_threshold(
        sizes in prop::collection::vec(1_000u64..1_000_000, 4..=20),
    ) {
        let config = default_config();
        let min = config.min_threshold;
        let strategy = SizeTieredStrategy::new(config);

        let sstables: Vec<_> = sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| make_metadata(&format!("sst_{i}"), size))
            .collect();

        let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        for task in &tasks {
            prop_assert!(
                task.inputs.len() >= min,
                "task has {} inputs, expected at least {}",
                task.inputs.len(),
                min
            );
        }
    }
}

/// Property: similarly-sized SSTables are grouped together.
#[test]
fn similar_sizes_in_same_bucket() {
    let config = default_config();
    let strategy = SizeTieredStrategy::new(config);

    // Create 4 SSTables of very similar size — should trigger compaction.
    let sstables: Vec<_> = (0..4)
        .map(|i| make_metadata(&format!("sst_{i}"), 10_000 + i * 100))
        .collect();

    let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());
    assert_eq!(
        tasks.len(),
        1,
        "similar-sized SSTables should form one bucket"
    );
    assert_eq!(tasks[0].inputs.len(), 4);
}

/// Property: very different sizes don't get grouped.
#[test]
fn different_sizes_separate_buckets() {
    let config = default_config();
    let strategy = SizeTieredStrategy::new(config);

    // Two pairs of similar sizes, but the pairs are far apart.
    let sstables = vec![
        make_metadata("small1", 1_000),
        make_metadata("small2", 1_100),
        make_metadata("small3", 1_200),
        make_metadata("large1", 1_000_000),
        make_metadata("large2", 1_100_000),
        make_metadata("large3", 1_200_000),
    ];

    let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());
    // Neither group has 4 SSTables (min_threshold), so no compaction.
    assert!(
        tasks.is_empty(),
        "groups of 3 shouldn't trigger with min_threshold=4"
    );
}

// ── UCS property tests ──────────────────────────────────────────────────

fn ucs_strategy(fan_factor: u32) -> UnifiedCompactionStrategy {
    UnifiedCompactionStrategy::new(ferrosa_storage::compaction::UcsConfig::from_params(
        &[("fan_factor".to_string(), fan_factor.to_string())]
            .into_iter()
            .collect(),
        PathBuf::from("/tmp/compaction"),
    ))
}

fn make_ucs_metadata(id: &str, size: u64, min_tok: i64, max_tok: i64) -> SSTableMetadata {
    SSTableMetadata {
        id: id.to_string(),
        path: PathBuf::from(format!("/tmp/{id}")),
        size_bytes: size,
        min_token: min_tok,
        max_token: max_tok,
        min_timestamp: 0,
        max_timestamp: 1000,
        partition_count: 100,
    }
}

// Property: UCS is deterministic — same input always produces same output.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn ucs_deterministic(
        sizes in prop::collection::vec(1_000u64..10_000_000, 2..=15),
    ) {
        let strategy = ucs_strategy(4);

        let sstables: Vec<_> = sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| make_ucs_metadata(&format!("sst_{i}"), size, 0, 1_000_000))
            .collect();

        let tasks1 = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        let tasks2 = strategy.select(&sstables, &test_table_schema(), &test_table_id());

        prop_assert_eq!(tasks1.len(), tasks2.len(), "UCS must be deterministic");
        for (t1, t2) in tasks1.iter().zip(tasks2.iter()) {
            let ids1: Vec<_> = t1.inputs.iter().map(|m| &m.id).collect();
            let ids2: Vec<_> = t2.inputs.iter().map(|m| &m.id).collect();
            prop_assert_eq!(ids1, ids2);
        }
    }
}

// Property: UCS tasks only reference SSTables from the input set.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn ucs_tasks_subset_of_input(
        sizes in prop::collection::vec(1_000u64..10_000_000, 2..=15),
    ) {
        let strategy = ucs_strategy(3);

        let sstables: Vec<_> = sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| make_ucs_metadata(&format!("sst_{i}"), size, 0, 1_000_000))
            .collect();

        let all_ids: std::collections::HashSet<_> =
            sstables.iter().map(|m| &m.id).collect();

        let tasks = strategy.select(&sstables, &test_table_schema(), &test_table_id());
        for task in &tasks {
            for input in &task.inputs {
                prop_assert!(
                    all_ids.contains(&input.id),
                    "UCS task references unknown SSTable: {}",
                    input.id
                );
            }
        }
    }
}
