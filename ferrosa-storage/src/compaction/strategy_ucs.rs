//! Unified Compaction Strategy (UCS) — Cassandra 5.0 CEP-26.
//!
//! Density-based compaction that subsumes STCS, LCS, and TWCS through a
//! single parameterized algorithm. SSTables are assigned to levels based
//! on their density (size / token range fraction), and levels with more
//! than `fan_factor` SSTables trigger compaction.

use std::collections::HashMap;
use std::path::PathBuf;

use super::metadata::{CompactionTask, SSTableMetadata};
use super::strategy::CompactionStrategy;
use ferrosa_common::TableSchema;

use crate::TableId;

/// Total token range size for Murmur3 partitioner: i64::MAX - i64::MIN.
const TOKEN_RANGE_SIZE: f64 = (i64::MAX as f64) - (i64::MIN as f64);

/// Minimum token share to avoid division by zero in density calculation.
const DENSITY_EPSILON: f64 = 1.0 / (1u64 << 48) as f64;

/// Configuration for the Unified Compaction Strategy.
#[derive(Debug, Clone)]
pub struct UcsConfig {
    /// Fan factor (W): maximum SSTables per level before compaction triggers.
    /// W=2 → aggressive (LCS-like), W=4 → balanced, W=32 → lazy (STCS-like).
    /// Minimum 2.
    pub fan_factor: u32,
    /// Minimum SSTable size for density calculation (bytes). SSTables smaller
    /// than this use this value as their effective size to avoid inflated density.
    pub min_sstable_size: u64,
    /// Safety cap on the number of levels.
    pub max_levels: u32,
    /// Output directory for compacted SSTables.
    pub output_dir: PathBuf,
}

impl UcsConfig {
    /// Parse UCS configuration from a DDL `compaction` map.
    pub fn from_params(params: &HashMap<String, String>, output_dir: PathBuf) -> Self {
        let fan_factor = params
            .get("fan_factor")
            .or_else(|| params.get("w"))
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(4)
            .max(2); // Minimum fan factor is 2

        let min_sstable_size = params
            .get("min_sstable_size")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(100 * 1024 * 1024); // 100 MiB default

        let max_levels = params
            .get("max_levels")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(32);

        Self {
            fan_factor,
            min_sstable_size,
            max_levels,
            output_dir,
        }
    }
}

/// Compute the density of an SSTable.
///
/// Density = effective_size / token_share, where token_share is the fraction
/// of the total Murmur3 token range covered by the SSTable.
fn density(sst: &SSTableMetadata, min_sstable_size: u64) -> f64 {
    let effective_size = sst.size_bytes.max(min_sstable_size) as f64;
    let token_span = (sst.max_token as f64) - (sst.min_token as f64);
    let token_share = (token_span / TOKEN_RANGE_SIZE).abs().max(DENSITY_EPSILON);
    effective_size / token_share
}

/// Assign an SSTable to a level based on its density relative to the base density.
///
/// Level N contains SSTables with density in `[base * W^N, base * W^(N+1))`.
fn assign_level(density_val: f64, base_density: f64, fan_factor: u32, max_levels: u32) -> u32 {
    if density_val <= 0.0 || base_density <= 0.0 || !density_val.is_finite() {
        return 0;
    }
    let ratio = density_val / base_density;
    if ratio <= 1.0 {
        return 0;
    }
    let level = ratio.log(fan_factor as f64).floor() as u32;
    level.min(max_levels)
}

/// The Unified Compaction Strategy.
pub struct UnifiedCompactionStrategy {
    config: UcsConfig,
}

impl UnifiedCompactionStrategy {
    pub fn new(config: UcsConfig) -> Self {
        Self { config }
    }
}

impl CompactionStrategy for UnifiedCompactionStrategy {
    fn select(
        &self,
        sstables: &[SSTableMetadata],
        schema: &TableSchema,
        table_id: &TableId,
    ) -> Vec<CompactionTask> {
        if sstables.len() < 2 {
            return vec![];
        }

        // Compute densities
        let densities: Vec<f64> = sstables
            .iter()
            .map(|sst| density(sst, self.config.min_sstable_size))
            .collect();

        // Base density = minimum non-zero density (freshly flushed SSTable)
        let base_density = densities
            .iter()
            .copied()
            .filter(|d| *d > 0.0 && d.is_finite())
            .fold(f64::MAX, f64::min);

        if base_density == f64::MAX || base_density <= 0.0 {
            return vec![];
        }

        // Assign levels
        let levels: Vec<u32> = densities
            .iter()
            .map(|d| {
                assign_level(
                    *d,
                    base_density,
                    self.config.fan_factor,
                    self.config.max_levels,
                )
            })
            .collect();

        // Group SSTables by level
        let mut level_groups: HashMap<u32, Vec<&SSTableMetadata>> = HashMap::new();
        for (i, sst) in sstables.iter().enumerate() {
            level_groups.entry(levels[i]).or_default().push(sst);
        }

        // For each level exceeding fan_factor, emit a compaction task.
        // Prefer lower levels first (smaller, faster compactions).
        let mut sorted_levels: Vec<u32> = level_groups.keys().copied().collect();
        sorted_levels.sort();

        let mut tasks = Vec::new();
        for level in sorted_levels {
            let group = &level_groups[&level];
            if group.len() > self.config.fan_factor as usize {
                // Take fan_factor + 1 SSTables from this level to compact
                let count = (self.config.fan_factor as usize + 1).min(group.len());
                let inputs: Vec<SSTableMetadata> =
                    group.iter().take(count).map(|s| (*s).clone()).collect();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sst(id: &str, size: u64, min_tok: i64, max_tok: i64) -> SSTableMetadata {
        SSTableMetadata {
            id: id.to_string(),
            path: PathBuf::from("/tmp"),
            size_bytes: size,
            min_token: min_tok,
            max_token: max_tok,
            min_timestamp: 0,
            max_timestamp: 0,
            partition_count: 1,
        }
    }

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: Default::default(),
        }
    }

    fn test_table_id() -> TableId {
        TableId::new("ks", "t")
    }

    fn ucs(fan_factor: u32) -> UnifiedCompactionStrategy {
        UnifiedCompactionStrategy::new(UcsConfig {
            fan_factor: fan_factor.max(2),
            min_sstable_size: 1, // 1 byte for test simplicity
            max_levels: 32,
            output_dir: PathBuf::from("/tmp/compaction"),
        })
    }

    // ── WP-006: Config ──────────────────────────────────────────────

    #[test]
    fn ucs_config_from_params_defaults() {
        let config = UcsConfig::from_params(&HashMap::new(), PathBuf::from("/tmp"));
        assert_eq!(config.fan_factor, 4);
        assert_eq!(config.min_sstable_size, 100 * 1024 * 1024);
        assert_eq!(config.max_levels, 32);
    }

    #[test]
    fn ucs_config_from_params_custom() {
        let mut params = HashMap::new();
        params.insert("fan_factor".into(), "8".into());
        params.insert("min_sstable_size".into(), "1048576".into());
        let config = UcsConfig::from_params(&params, PathBuf::from("/tmp"));
        assert_eq!(config.fan_factor, 8);
        assert_eq!(config.min_sstable_size, 1048576);
    }

    #[test]
    fn ucs_config_rejects_fan_factor_below_2() {
        let mut params = HashMap::new();
        params.insert("fan_factor".into(), "1".into());
        let config = UcsConfig::from_params(&params, PathBuf::from("/tmp"));
        assert_eq!(config.fan_factor, 2, "fan_factor must be clamped to >= 2");

        params.insert("fan_factor".into(), "0".into());
        let config = UcsConfig::from_params(&params, PathBuf::from("/tmp"));
        assert_eq!(config.fan_factor, 2);
    }

    // ── WP-007: Density ─────────────────────────────────────────────

    #[test]
    fn density_normal_case() {
        // 1000 bytes covering 10% of token range
        let range = (TOKEN_RANGE_SIZE * 0.1) as i64;
        let s = sst("a", 1000, 0, range);
        let d = density(&s, 1);
        // density = 1000 / 0.1 = 10000
        assert!((d - 10000.0).abs() < 1.0, "expected ~10000, got {d}");
    }

    #[test]
    fn density_zero_token_share_returns_finite() {
        // min_token == max_token → point partition
        let s = sst("a", 1000, 42, 42);
        let d = density(&s, 1);
        assert!(d.is_finite(), "density must be finite, got {d}");
        assert!(d > 0.0, "density must be positive");
    }

    #[test]
    fn density_full_ring() {
        let s = sst("a", 1_000_000, i64::MIN, i64::MAX);
        let d = density(&s, 1);
        // Covers ~100% of ring, density ≈ size
        assert!(
            (d - 1_000_000.0).abs() < 100.0,
            "full ring density should be ~size, got {d}"
        );
    }

    // ── WP-008: Level assignment ────────────────────────────────────

    #[test]
    fn level_assignment_base_density_is_level_zero() {
        let level = assign_level(100.0, 100.0, 4, 32);
        assert_eq!(level, 0, "density == base should be level 0");
    }

    #[test]
    fn level_assignment_4x_base_is_level_one() {
        let level = assign_level(400.0, 100.0, 4, 32);
        assert_eq!(level, 1, "4x base with W=4 should be level 1");
    }

    #[test]
    fn level_assignment_16x_base_is_level_two() {
        let level = assign_level(1600.0, 100.0, 4, 32);
        assert_eq!(level, 2, "16x base with W=4 should be level 2");
    }

    #[test]
    fn level_capped_at_max() {
        let level = assign_level(f64::MAX / 2.0, 1.0, 2, 10);
        assert!(level <= 10, "level must be capped at max_levels");
    }

    // ── WP-009: select() ────────────────────────────────────────────

    #[test]
    fn ucs_empty_no_tasks() {
        let strategy = ucs(4);
        let tasks = strategy.select(&[], &test_schema(), &test_table_id());
        assert!(tasks.is_empty());
    }

    #[test]
    fn ucs_single_sstable_no_tasks() {
        let strategy = ucs(4);
        let sstables = vec![sst("a", 1000, 0, 1000)];
        let tasks = strategy.select(&sstables, &test_schema(), &test_table_id());
        assert!(tasks.is_empty());
    }

    #[test]
    fn ucs_select_triggers_on_fan_factor() {
        let strategy = ucs(2); // W=2 → trigger when >2 at same level
                               // 5 SSTables all covering same range, same size → same level
        let sstables: Vec<_> = (0..5)
            .map(|i| sst(&format!("s{i}"), 1000, 0, 1000))
            .collect();
        let tasks = strategy.select(&sstables, &test_schema(), &test_table_id());
        assert!(
            !tasks.is_empty(),
            "5 SSTables at same level with W=2 should trigger compaction"
        );
        assert!(tasks[0].inputs.len() >= 3); // W+1 = 3
    }

    #[test]
    fn ucs_select_prefers_lower_levels() {
        let strategy = ucs(2);
        // Level 0: 5 small SSTables (low density)
        // Level 1: 5 larger SSTables (higher density, ~4x)
        let range = (TOKEN_RANGE_SIZE * 0.5) as i64;
        let mut sstables = Vec::new();
        for i in 0..5 {
            sstables.push(sst(&format!("low{i}"), 100, 0, range));
        }
        for i in 0..5 {
            sstables.push(sst(&format!("high{i}"), 10000, 0, range));
        }
        let tasks = strategy.select(&sstables, &test_schema(), &test_table_id());
        assert!(
            tasks.len() >= 2,
            "both levels should trigger, got {} tasks",
            tasks.len()
        );
        // First task should be lower level (smaller inputs)
        let first_avg_size: u64 = tasks[0].inputs.iter().map(|s| s.size_bytes).sum::<u64>()
            / tasks[0].inputs.len() as u64;
        let second_avg_size: u64 = tasks[1].inputs.iter().map(|s| s.size_bytes).sum::<u64>()
            / tasks[1].inputs.len() as u64;
        assert!(
            first_avg_size <= second_avg_size,
            "first task should be lower level (smaller), got {first_avg_size} vs {second_avg_size}"
        );
    }

    #[test]
    fn ucs_below_fan_factor_no_tasks() {
        let strategy = ucs(4); // W=4
                               // Only 3 SSTables at same level → below fan_factor
        let sstables: Vec<_> = (0..3)
            .map(|i| sst(&format!("s{i}"), 1000, 0, 1000))
            .collect();
        let tasks = strategy.select(&sstables, &test_schema(), &test_table_id());
        assert!(
            tasks.is_empty(),
            "3 SSTables with W=4 should not trigger compaction"
        );
    }

    #[test]
    fn ucs_w32_stcs_like_no_tasks_with_few_sstables() {
        let strategy = ucs(32); // Very lazy
        let sstables: Vec<_> = (0..20)
            .map(|i| sst(&format!("s{i}"), 1000, 0, 1000))
            .collect();
        let tasks = strategy.select(&sstables, &test_schema(), &test_table_id());
        assert!(
            tasks.is_empty(),
            "20 SSTables with W=32 should not trigger (need >32)"
        );
    }

    #[test]
    fn ucs_same_density_same_level_triggers() {
        let strategy = ucs(4);
        // 8 SSTables all same density → all level 0 → triggers (8 > 4)
        let sstables: Vec<_> = (0..8)
            .map(|i| sst(&format!("s{i}"), 5000, 0, 1000))
            .collect();
        let tasks = strategy.select(&sstables, &test_schema(), &test_table_id());
        assert_eq!(tasks.len(), 1, "all same level, should trigger once");
        assert!(tasks[0].inputs.len() >= 5); // W+1 = 5
    }

    #[test]
    fn ucs_large_fan_factor_no_panic() {
        let strategy = ucs(1000);
        let sstables: Vec<_> = (0..50)
            .map(|i| sst(&format!("s{i}"), 1000, 0, 1000))
            .collect();
        let tasks = strategy.select(&sstables, &test_schema(), &test_table_id());
        assert!(tasks.is_empty(), "50 < 1000, no tasks");
    }

    #[test]
    fn ucs_deterministic() {
        let strategy = ucs(4);
        let sstables: Vec<_> = (0..10)
            .map(|i| {
                sst(
                    &format!("s{i}"),
                    (i + 1) as u64 * 1000,
                    0,
                    (i + 1) as i64 * 1000,
                )
            })
            .collect();
        let tasks1 = strategy.select(&sstables, &test_schema(), &test_table_id());
        let tasks2 = strategy.select(&sstables, &test_schema(), &test_table_id());
        assert_eq!(tasks1.len(), tasks2.len(), "must be deterministic");
        for (t1, t2) in tasks1.iter().zip(tasks2.iter()) {
            assert_eq!(t1.inputs.len(), t2.inputs.len());
            for (i1, i2) in t1.inputs.iter().zip(t2.inputs.iter()) {
                assert_eq!(i1.id, i2.id);
            }
        }
    }
}
