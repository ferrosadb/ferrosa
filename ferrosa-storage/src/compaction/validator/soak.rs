//! Compaction soak loop: generate a corpus, compact it for real, and diff the
//! output against the oracle, across many seeds. A divergence or format
//! violation fails loudly with the offending seed so it can be replayed.

use std::path::Path;

use ferrosa_sstable::types::Partition;

use super::auditor::audit_partition;
use super::corpus;
use super::driver::{compact_all, logical_projection};
use super::oracle::oracle_merge_all;
use crate::TableId;

/// Summary of a soak run.
#[derive(Debug, Default, Clone, Copy)]
pub struct SoakReport {
    pub iterations: usize,
    pub cells_checked: usize,
}

/// Run one soak iteration for `seed`, returning the number of logical cells
/// checked. Returns `Err` (naming the seed) on the first divergence or format
/// violation, so the failure is reproducible.
pub fn run_iteration(work_dir: &Path, seed: u64) -> Result<usize, String> {
    let corpus = corpus::generate(seed);
    let groups: Vec<Vec<Partition>> = corpus
        .groups
        .into_iter()
        .filter(|g| !g.is_empty())
        .collect();
    if groups.is_empty() {
        return Ok(0);
    }
    let all: Vec<Partition> = groups.iter().flatten().cloned().collect();

    let dir = work_dir.join(format!("soak_{seed}"));
    std::fs::create_dir_all(&dir).map_err(|e| format!("seed {seed}: mkdir failed: {e}"))?;

    let actual = compact_all(
        &dir,
        &groups,
        &corpus.schema,
        TableId::new("soak", "compaction"),
    );
    let expected = oracle_merge_all(&all);

    let outcome = check(seed, &actual, &expected);
    let _ = std::fs::remove_dir_all(&dir);
    outcome
}

fn check(seed: u64, actual: &[Partition], expected: &[Partition]) -> Result<usize, String> {
    let projected = logical_projection(actual);
    if projected != logical_projection(expected) {
        return Err(format!(
            "seed {seed}: real compaction output diverged from the oracle"
        ));
    }
    for partition in actual {
        let violations = audit_partition(partition);
        if !violations.is_empty() {
            return Err(format!("seed {seed}: format violations: {violations:?}"));
        }
    }
    Ok(projected.len())
}

/// Run `iterations` soak iterations starting at `seed_start`.
pub fn run(work_dir: &Path, seed_start: u64, iterations: usize) -> Result<SoakReport, String> {
    let mut cells_checked = 0;
    for i in 0..iterations {
        cells_checked += run_iteration(work_dir, seed_start + i as u64)?;
    }
    Ok(SoakReport {
        iterations,
        cells_checked,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soak_stays_clean_across_many_seeds() {
        let tmp = tempfile::tempdir().unwrap();
        let report = run(tmp.path(), 1, 48).expect("soak must stay clean");
        assert_eq!(report.iterations, 48);
        assert!(report.cells_checked > 0, "soak must actually check cells");
    }
}
