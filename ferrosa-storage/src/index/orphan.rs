//! SSTables that exist only as metadata, and what a backfill should do about them.
//!
//! # The state this was written from
//!
//! On a live 3-node cluster, 2026-09-13, `agent_memory.entity_store` held per
//! node: 28 SSTables with a `Data.db`, and **35 with a `TOC.txt` and index
//! sidecars but no `Data.db` at all**. The oldest orphan generations date to
//! May; the live ones were from that day. Compaction had removed the data files
//! and left their metadata behind.
//!
//! A backfill enumerates SSTables from that metadata, so it tried to open 21
//! and failed 20 of them with:
//!
//! ```text
//! engine: partition-key index backfill FAILED; rows in this SSTable are not in
//! the index and reads through it will be incomplete
//! e=open data: I/O error: No such file or directory (os error 2)
//! ```
//!
//! Each failure called `mark_failed`, so the index stayed `stale` forever and
//! the server — correctly — refused to read through it:
//!
//! ```text
//! secondary index 'idx_entity_by_tenant' is not current; refusing to return
//! incomplete results while index backfill is pending or failed
//! ```
//!
//! Six indexes were stuck this way for at least a day, with zero progress
//! between samples.
//!
//! # Why retrying cannot fix it
//!
//! `ferrosa-ctl index rebuild` re-runs the same loop. The data files are not
//! coming back — they were compacted away on purpose. Every retry re-reads the
//! same absent files and re-marks the same failure. An automatic rebuild built
//! on retry alone would spin forever and never clear the index.
//!
//! The distinction this module draws is therefore load-bearing: an SSTable
//! whose data is GONE has nothing to index and must not count against
//! coverage, while an SSTable that could not be READ is a real failure and must
//! still be loud.

/// What one SSTable's backfill attempt means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillOutcome {
    /// Indexed.
    Built,
    /// The SSTable's data file is absent. It was compacted away and only its
    /// metadata survives, so there are no rows to index and nothing is missing
    /// from the index by skipping it.
    Vanished,
    /// A real failure: the data is there and could not be indexed. Reads
    /// through this index are genuinely incomplete.
    Failed,
}

/// Whether a build error means the SSTable's data file is simply not there.
///
/// Conservative by construction: anything not recognised as "absent" is a real
/// failure. Being wrong that way costs a loud error about a healthy SSTable.
/// Being wrong the other way silently drops rows from an index and reports the
/// index complete, which is the failure mode this whole area exists to prevent.
///
/// Matching on TEXT because `IndexBuildBackend::build` returns `String`. That
/// is brittle by nature, so the wordings below are pinned by tests rather than
/// assumed, and an unrecognised wording fails safe.
#[must_use]
pub fn data_file_is_absent(error: &str) -> bool {
    let lowered = error.to_lowercase();
    // The canonical io::ErrorKind::NotFound renderings, plus the raw errno.
    // A permission or corruption error must NOT match any of these.
    lowered.contains("no such file or directory")
        || mentions_errno(&lowered, 2)
        || lowered.contains("entity not found")
}

/// Whether the text names exactly this errno, and not one that merely starts
/// with its digits.
///
/// `contains("os error 2")` also matches `os error 21` (EISDIR) and `os error
/// 28` (ENOSPC) — a disk-full error would have been read as "the file is gone"
/// and its SSTable silently dropped from the index. Caught by the test that
/// lists real errors; the digits have to end where the number ends.
fn mentions_errno(lowered: &str, errno: u32) -> bool {
    let needle = format!("os error {errno}");
    let mut from = 0;
    while let Some(at) = lowered[from..].find(&needle) {
        let end = from + at + needle.len();
        match lowered.as_bytes().get(end) {
            Some(next) if next.is_ascii_digit() => from = end,
            _ => return true,
        }
    }
    false
}

/// Classify one attempt.
#[must_use]
pub fn classify(result: Result<(), &str>) -> BackfillOutcome {
    match result {
        Ok(()) => BackfillOutcome::Built,
        Err(error) if data_file_is_absent(error) => BackfillOutcome::Vanished,
        Err(_) => BackfillOutcome::Failed,
    }
}

/// What a rebuild covered, once vanished SSTables are discounted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Coverage {
    /// SSTables actually indexed.
    pub built: usize,
    /// SSTables whose data was gone. Not failures, and not coverage either —
    /// they hold no rows.
    pub vanished: usize,
    /// SSTables that should have been indexed and were not.
    pub failed: usize,
}

impl Coverage {
    /// Fold one outcome in.
    pub fn record(&mut self, outcome: BackfillOutcome) {
        match outcome {
            BackfillOutcome::Built => self.built += 1,
            BackfillOutcome::Vanished => self.vanished += 1,
            BackfillOutcome::Failed => self.failed += 1,
        }
    }

    /// SSTables that actually had rows to index.
    ///
    /// The denominator that matters. Reporting "1 of 21" when 20 of those 21
    /// no longer exist describes the metadata, not the data, and sends someone
    /// looking for twenty missing builds that were never owed.
    #[must_use]
    pub fn indexable(&self) -> usize {
        self.built + self.failed
    }

    /// Whether reads through this index are complete.
    ///
    /// A vanished SSTable cannot make an index incomplete: an index that
    /// covers every SSTable holding rows covers every row.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.failed == 0
    }

    /// What to tell the operator, naming vanished SSTables explicitly.
    ///
    /// They are mentioned even in the success case. Silently discounting
    /// twenty SSTables would leave a rebuild reporting "1 of 1" where the
    /// operator could see twenty-one on disk, and an unexplained gap invites
    /// exactly the wrong conclusion.
    #[must_use]
    pub fn describe(&self) -> String {
        let mut text = if self.is_complete() {
            format!("indexed {} of {} SSTables", self.built, self.indexable())
        } else {
            format!(
                "indexed only {} of {} SSTables; reads through this index are STILL incomplete",
                self.built,
                self.indexable()
            )
        };
        if self.vanished > 0 {
            text.push_str(&format!(
                " ({} more had no data file — compacted away, metadata left behind; \
                 nothing to index there)",
                self.vanished
            ));
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The live error, verbatim from node1 on 2026-09-13.
    const ABSENT: &str = "open data: I/O error: No such file or directory (os error 2)";

    /// The whole point: an SSTable that is gone is not a failure.
    #[test]
    fn a_missing_data_file_is_vanished_not_failed() {
        assert_eq!(classify(Err(ABSENT)), BackfillOutcome::Vanished);
    }

    /// A real failure stays a failure.
    ///
    /// Conservative on purpose. Treating a readable-but-broken SSTable as
    /// "vanished" would drop its rows from the index and then report the index
    /// complete — a silent wrong answer, which is worse than the loud one.
    #[test]
    fn a_real_error_is_never_mistaken_for_a_vanished_sstable() {
        for error in [
            "open data: I/O error: Permission denied (os error 13)",
            "corrupt sstable: checksum mismatch at offset 4096",
            "unexpected end of file while reading Rows.db",
            "decompression failed",
            "",
            "os error 21",
        ] {
            assert_eq!(
                classify(Err(error)),
                BackfillOutcome::Failed,
                "{error:?} must be treated as a real failure"
            );
        }
    }

    /// Success is success.
    #[test]
    fn a_built_sstable_is_built() {
        assert_eq!(classify(Ok(())), BackfillOutcome::Built);
    }

    /// The denominator counts SSTables that HAD rows, not metadata.
    ///
    /// The live rebuild reported "only 1 of 21 SSTables" and exited non-zero.
    /// Twenty of those twenty-one had no data file. The honest reading is
    /// "1 of 1, and twenty sets of leftover metadata".
    #[test]
    fn vanished_sstables_do_not_count_against_coverage() {
        let mut coverage = Coverage::default();
        coverage.record(BackfillOutcome::Built);
        for _ in 0..20 {
            coverage.record(BackfillOutcome::Vanished);
        }

        assert_eq!(coverage.indexable(), 1, "only one SSTable held rows");
        assert!(
            coverage.is_complete(),
            "every SSTable with rows was indexed, so reads are complete"
        );
    }

    /// But one genuine failure still makes it incomplete.
    #[test]
    fn a_single_real_failure_keeps_the_index_incomplete() {
        let mut coverage = Coverage::default();
        coverage.record(BackfillOutcome::Built);
        coverage.record(BackfillOutcome::Vanished);
        coverage.record(BackfillOutcome::Failed);

        assert_eq!(coverage.indexable(), 2);
        assert!(!coverage.is_complete());
    }

    /// Vanished SSTables are REPORTED, never silently discounted.
    ///
    /// An operator who can see twenty-one generations on disk and is told
    /// "1 of 1" will assume the tool is lying. Naming them is what makes the
    /// smaller denominator believable.
    #[test]
    fn the_report_explains_the_missing_sstables() {
        let mut coverage = Coverage::default();
        coverage.record(BackfillOutcome::Built);
        for _ in 0..20 {
            coverage.record(BackfillOutcome::Vanished);
        }

        let text = coverage.describe();
        assert!(text.contains("1 of 1"), "{text}");
        assert!(
            text.contains("20"),
            "the vanished ones must be named: {text}"
        );
        assert!(text.contains("compacted"), "{text}");
    }

    /// And an incomplete rebuild still says so first.
    #[test]
    fn an_incomplete_rebuild_leads_with_the_bad_news() {
        let mut coverage = Coverage::default();
        coverage.record(BackfillOutcome::Failed);
        coverage.record(BackfillOutcome::Vanished);

        let text = coverage.describe();
        assert!(text.contains("STILL incomplete"), "{text}");
    }

    /// An all-vanished table is complete, not failed.
    ///
    /// Every SSTable compacted away and nothing left to index: the index covers
    /// all zero rows that exist. Reporting that as a failure would keep an
    /// index permanently stale over an empty table.
    #[test]
    fn a_table_whose_sstables_all_vanished_is_complete() {
        let mut coverage = Coverage::default();
        for _ in 0..5 {
            coverage.record(BackfillOutcome::Vanished);
        }
        assert_eq!(coverage.indexable(), 0);
        assert!(coverage.is_complete());
    }
}

#[cfg(test)]
mod outcome_tests {
    use crate::engine::RebuildOutcome;

    /// The live case: 21 generations, 20 of them metadata-only.
    ///
    /// Before this, `is_complete` required rebuilt == total, so a fully
    /// repaired index reported failure forever — the twenty data files were
    /// compacted away on purpose and were never coming back. `ferrosa-ctl
    /// index rebuild` exited non-zero every time and the index stayed stale.
    #[test]
    fn an_index_covering_every_live_sstable_is_complete() {
        let outcome = RebuildOutcome {
            sstables_rebuilt: 1,
            sstables_total: 21,
            sstables_vanished: 20,
        };
        assert!(outcome.is_complete());
        assert_eq!(outcome.sstables_indexable(), 1);
    }

    /// A genuine shortfall is still incomplete.
    #[test]
    fn a_real_shortfall_is_still_incomplete() {
        let outcome = RebuildOutcome {
            sstables_rebuilt: 3,
            sstables_total: 21,
            sstables_vanished: 15,
        };
        // 3 built + 15 vanished = 18 of 21; three SSTables hold rows and were
        // not indexed.
        assert!(!outcome.is_complete());
        assert_eq!(outcome.sstables_indexable(), 6);
    }

    /// No vanished SSTables behaves exactly as before.
    #[test]
    fn the_ordinary_case_is_unchanged() {
        assert!(RebuildOutcome {
            sstables_rebuilt: 17,
            sstables_total: 17,
            sstables_vanished: 0,
        }
        .is_complete());
        assert!(!RebuildOutcome {
            sstables_rebuilt: 16,
            sstables_total: 17,
            sstables_vanished: 0,
        }
        .is_complete());
    }
}
