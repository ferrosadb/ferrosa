//! Proptest strategies for load test operation generation.

use rand::RngExt;

/// Type of operation in a load test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    Read,
    Write,
    Update,
    Delete,
}

/// Generate a key string for a given index in the key space.
/// Keys are zero-padded for consistent sort order: `k00000000`, `k00000001`, etc.
pub fn make_key_string(index: usize) -> String {
    format!("k{:08}", index)
}

/// Generate a random value of the given length using the provided RNG.
pub fn make_random_value(rng: &mut impl rand::Rng, len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    rng.fill_bytes(&mut buf);
    buf
}

/// Choose an operation type based on profile ratios.
///
/// Within writes, `update_ratio` and `delete_ratio` control how many
/// writes are overwrites vs tombstones vs fresh inserts.
pub fn choose_op(
    rng: &mut impl rand::Rng,
    read_ratio: f64,
    update_ratio: f64,
    delete_ratio: f64,
) -> OpType {
    let r: f64 = rng.random();
    if r < read_ratio {
        OpType::Read
    } else {
        // Within the write portion, pick update/delete/insert.
        let write_r: f64 = rng.random();
        if write_r < delete_ratio {
            OpType::Delete
        } else if write_r < delete_ratio + update_ratio {
            OpType::Update
        } else {
            OpType::Write
        }
    }
}

/// Get the current process RSS in bytes (macOS/Linux).
pub fn process_rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok();
        output
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0)
            * 1024 // ps reports KB on macOS
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
            .unwrap_or(0)
            * 4096 // pages to bytes
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

/// Choose a random value length within the profile's range.
pub fn choose_value_len(rng: &mut impl rand::Rng, min: usize, max: usize) -> usize {
    if min == max {
        min
    } else {
        rng.random_range(min..=max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn make_key_string_zero_padded() {
        assert_eq!(make_key_string(0), "k00000000");
        assert_eq!(make_key_string(42), "k00000042");
        assert_eq!(make_key_string(99_999_999), "k99999999");
    }

    #[test]
    fn make_key_string_sorted_order() {
        let keys: Vec<String> = (0..100).map(make_key_string).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "keys must be naturally sorted");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn random_value_correct_length(len in 64usize..=4096) {
            let mut rng = rand::rng();
            let val = make_random_value(&mut rng, len);
            prop_assert_eq!(val.len(), len);
        }

        #[test]
        fn choose_op_respects_distribution(read_ratio in 0.0f64..=1.0) {
            let mut rng = rand::rng();
            let ops: Vec<OpType> = (0..1000).map(|_| choose_op(&mut rng, read_ratio, 0.0, 0.0)).collect();
            let reads = ops.iter().filter(|o| **o == OpType::Read).count();
            let expected = (read_ratio * 1000.0) as usize;
            // Allow +-15% tolerance for randomness.
            let tolerance = 150;
            prop_assert!(
                reads.abs_diff(expected) < tolerance,
                "expected ~{} reads, got {} (ratio={})",
                expected,
                reads,
                read_ratio
            );
        }
    }

    #[test]
    fn choose_value_len_in_range() {
        let mut rng = rand::rng();
        for _ in 0..100 {
            let len = choose_value_len(&mut rng, 100, 200);
            assert!((100..=200).contains(&len));
        }
    }

    #[test]
    fn choose_value_len_equal_bounds() {
        let mut rng = rand::rng();
        assert_eq!(choose_value_len(&mut rng, 42, 42), 42);
    }
}
