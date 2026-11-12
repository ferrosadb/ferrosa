//! Export production consistency thresholds for comparison with a formal model.
//! Correctness: exact ALL/QUORUM/ONE thresholds; no claim about RPC admission.
//! Last revised: 2026-10-29
//! Last changed: Added a 1/2/3/5/9-replica threshold correspondence probe.

use ferrosa_cluster::ConsistencyLevel;

fn main() {
    println!("FERROSA_CL_POLICY_V1");
    let mut count = 0;
    for replicas in [1, 2, 3, 5, 9] {
        for live in 0..=replicas {
            for (code, level) in [
                ConsistencyLevel::All,
                ConsistencyLevel::Quorum,
                ConsistencyLevel::One,
            ]
            .into_iter()
            .enumerate()
            {
                let needed = level.block_for(replicas);
                println!(
                    "{replicas} {live} {code} {needed} {}",
                    usize::from(live >= needed)
                );
                count += 1;
            }
        }
    }
    println!("END {count}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn degraded_cluster_never_shrinks_the_quorum_denominator() {
        assert_eq!(ConsistencyLevel::Quorum.block_for(2), 2);
        assert_eq!(ConsistencyLevel::Quorum.block_for(3), 2);
        assert_eq!(ConsistencyLevel::Quorum.block_for(5), 3);
        assert!(2 < ConsistencyLevel::Quorum.block_for(5));
    }
}
