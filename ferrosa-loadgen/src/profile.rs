//! Load test profile configuration.

use std::time::Duration;

/// Configuration for a single load test run.
#[derive(Debug, Clone)]
pub struct LoadProfile {
    /// Human-readable name for this profile.
    pub name: String,
    /// Fraction of operations that are reads (0.0 - 1.0).
    pub read_ratio: f64,
    /// Fraction of operations that are writes (inserts) (0.0 - 1.0).
    pub write_ratio: f64,
    /// Fraction of writes that are updates (overwrite existing key).
    pub update_ratio: f64,
    /// Fraction of writes that are deletes (tombstones).
    pub delete_ratio: f64,
    /// Number of distinct keys in the key space.
    pub key_space_size: usize,
    /// Minimum and maximum value size in bytes.
    pub value_size_range: (usize, usize),
    /// Number of concurrent writer tasks.
    pub num_writers: usize,
    /// Number of concurrent reader tasks.
    pub num_readers: usize,
    /// How long to run the load test.
    pub duration: Duration,
    /// Flush threshold in bytes (smaller = more frequent flushes).
    pub flush_threshold_bytes: u64,
    /// Local cache max bytes (smaller = more S3 eviction).
    pub local_cache_max_bytes: u64,
    /// Total bytes of data to write before stopping (whichever of duration/target
    /// comes first). Set to `u64::MAX` to run for the full duration.
    pub target_data_size_bytes: u64,
    /// UCS fan factor (2 = aggressive compaction, 32 = lazy).
    pub fan_factor: u32,
}

impl LoadProfile {
    /// 90% reads, 10% writes. Exercises read-path merging across memtable + SSTables.
    pub fn read_heavy() -> Self {
        Self {
            name: "read_heavy".into(),
            read_ratio: 0.9,
            write_ratio: 0.1,
            update_ratio: 0.0,
            delete_ratio: 0.0,
            key_space_size: 100_000,
            value_size_range: (1024, 4096),
            num_writers: 4,
            num_readers: 16,
            duration: Duration::from_secs(60),
            flush_threshold_bytes: 256 * 1024,
            local_cache_max_bytes: 10 * 1024 * 1024,
            target_data_size_bytes: u64::MAX,
            fan_factor: 4,
        }
    }

    /// 50/50 read/write mix. Balanced stress on all paths.
    pub fn balanced() -> Self {
        Self {
            name: "balanced".into(),
            read_ratio: 0.5,
            write_ratio: 0.5,
            update_ratio: 0.3,
            delete_ratio: 0.1,
            key_space_size: 50_000,
            value_size_range: (1024, 4096),
            num_writers: 8,
            num_readers: 8,
            duration: Duration::from_secs(60),
            flush_threshold_bytes: 128 * 1024,
            local_cache_max_bytes: 10 * 1024 * 1024,
            target_data_size_bytes: u64::MAX,
            fan_factor: 4,
        }
    }

    /// 10% reads, 90% writes. Maximum compaction and S3 upload pressure.
    pub fn write_heavy() -> Self {
        Self {
            name: "write_heavy".into(),
            read_ratio: 0.1,
            write_ratio: 0.9,
            update_ratio: 0.3,
            delete_ratio: 0.1,
            key_space_size: 200_000,
            value_size_range: (1024, 4096),
            num_writers: 16,
            num_readers: 4,
            duration: Duration::from_secs(120),
            flush_threshold_bytes: 64 * 1024, flush_max_age_secs: 300,
            local_cache_max_bytes: 10 * 1024 * 1024,
            target_data_size_bytes: u64::MAX,
            fan_factor: 4,
        }
    }
    /// 70% updates + 20% deletes + 10% reads. Creates maximum tombstone
    /// and overwrite pressure to stress compaction's ability to reclaim space
    /// without losing data.
    pub fn delete_update_heavy() -> Self {
        Self {
            name: "delete_update_heavy".into(),
            read_ratio: 0.1,
            write_ratio: 0.9,
            update_ratio: 0.7,
            delete_ratio: 0.2,
            key_space_size: 50_000,
            value_size_range: (512, 2048),
            num_writers: 12,
            num_readers: 2,
            duration: Duration::from_secs(300),
            flush_threshold_bytes: 32 * 1024,
            local_cache_max_bytes: 5 * 1024 * 1024,
            target_data_size_bytes: u64::MAX,
            fan_factor: 2, // aggressive compaction to exercise rewrite
        }
    }

    /// Long-running compaction stress: aggressive fan_factor, small flushes,
    /// small cache, high write volume. Designed to run for 5+ minutes and
    /// produce dozens of compaction cycles.
    pub fn compaction_stress() -> Self {
        Self {
            name: "compaction_stress".into(),
            read_ratio: 0.2,
            write_ratio: 0.8,
            update_ratio: 0.5,
            delete_ratio: 0.1,
            key_space_size: 100_000,
            value_size_range: (1024, 4096),
            num_writers: 8,
            num_readers: 4,
            duration: Duration::from_secs(600),
            flush_threshold_bytes: 16 * 1024,
            local_cache_max_bytes: 2 * 1024 * 1024,
            target_data_size_bytes: u64::MAX,
            fan_factor: 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_profile_read_heavy_ratios() {
        let p = LoadProfile::read_heavy();
        assert_eq!(p.read_ratio, 0.9);
        assert_eq!(p.write_ratio, 0.1);
        assert!(p.num_readers > p.num_writers);
    }

    #[test]
    fn load_profile_balanced_ratios() {
        let p = LoadProfile::balanced();
        assert_eq!(p.read_ratio, 0.5);
        assert_eq!(p.write_ratio, 0.5);
        assert_eq!(p.num_writers, p.num_readers);
    }

    #[test]
    fn load_profile_write_heavy_ratios() {
        let p = LoadProfile::write_heavy();
        assert_eq!(p.read_ratio, 0.1);
        assert_eq!(p.write_ratio, 0.9);
        assert!(p.num_writers > p.num_readers);
    }

    #[test]
    fn load_profile_ratios_sum_to_one() {
        for p in [
            LoadProfile::read_heavy(),
            LoadProfile::balanced(),
            LoadProfile::write_heavy(),
        ] {
            assert!(
                (p.read_ratio + p.write_ratio - 1.0).abs() < f64::EPSILON,
                "ratios must sum to 1.0 for profile '{}'",
                p.name
            );
        }
    }

    #[test]
    fn load_profile_duration_is_primary_stop_condition() {
        for p in [
            LoadProfile::read_heavy(),
            LoadProfile::balanced(),
            LoadProfile::write_heavy(),
        ] {
            assert!(
                p.target_data_size_bytes == u64::MAX,
                "profile '{}' should run for full duration (target_data_size_bytes = MAX)",
                p.name
            );
        }
    }

    #[test]
    fn load_profile_fan_factor_valid() {
        for p in [
            LoadProfile::read_heavy(),
            LoadProfile::balanced(),
            LoadProfile::write_heavy(),
        ] {
            assert!(p.fan_factor >= 2, "fan_factor must be >= 2");
        }
    }
}
