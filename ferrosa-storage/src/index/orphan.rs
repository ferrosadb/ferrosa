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

/// What one SSTable build enumerated versus what it indexed: the row-count
/// reconciliation backstop.
///
/// Classification (`GenerationState`) answers "was this generation read at all".
/// This answers "of what was read, did the index get all of it", independently
/// of how anything was classified. A live index reported `current` while
/// covering 32,632 of 102,840 rows; either check alone would have caught it.
///
/// # Cost and tolerance
///
/// Free: the counters are incremented by the single pass the build already
/// makes, and `partitions_declared` is a footer field. No second scan.
///
/// Tolerance is ZERO, by design:
/// - `partitions_declared` is the partition count the writer recorded in the
///   partition-index footer, so every healthy SSTable scans exactly that many.
///   Fewer means the walk ended early (truncation, a skipped region).
/// - For a partition-key index the indexed value is read from the key, never
///   from a nullable cell, so every partition with a row yields exactly one
///   entry. A shortfall means the value could not be decoded (wrong component,
///   changed key layout) and the index would be silently empty.
///
/// Cell-valued and clustering indexes are deliberately not checked at the
/// entry level: a null cell legitimately produces no entry, so a shortfall
/// there is not evidence. A weaker check presented as strong would be worse
/// than none; for those only the partition walk is reconciled. A full
/// row-by-row comparison against a table scan is the only sound entry-level
/// check for them and is not done on the rebuild path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanTally {
    /// Partition count recorded by the writer in the partition-index footer.
    pub partitions_declared: u64,
    /// Partitions the build actually read.
    pub partitions_scanned: u64,
    /// Partitions the index should hold an entry for, when that is exact
    /// (partition-key index). `None` for index kinds where nulls make it inexact.
    pub entries_expected: Option<u64>,
    /// Entries the build produced.
    pub entries_indexed: u64,
}

impl ScanTally {
    /// Check the tally against zero tolerance.
    ///
    /// # Errors
    ///
    /// Names both numbers so the operator can see how short the index is.
    pub fn verify(&self) -> Result<(), String> {
        if self.partitions_scanned != self.partitions_declared {
            return Err(format!(
                "reconciliation failed: the SSTable declares {} partitions but the build read \
                 {}; the index would not cover {} of them",
                self.partitions_declared,
                self.partitions_scanned,
                self.partitions_declared
                    .saturating_sub(self.partitions_scanned),
            ));
        }
        if let Some(expected) = self.entries_expected {
            if self.entries_indexed != expected {
                return Err(format!(
                    "reconciliation failed: the build enumerated {expected} partitions to index \
                     but produced {} entries; the index would be short by {}",
                    self.entries_indexed,
                    expected.saturating_sub(self.entries_indexed),
                ));
            }
        }
        Ok(())
    }
}

/// What one SSTable's backfill attempt means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillOutcome {
    /// Indexed.
    Built,
    /// The SSTable's data file is absent but its TOC and sidecars survive. It
    /// was compacted away and only its metadata remains, so there are no rows
    /// to index and nothing is missing from the index by skipping it.
    Vanished,
    /// A real failure: the data is (or should be) there and could not be
    /// indexed. Reads through this index are genuinely incomplete.
    Failed,
}

/// What a generation looks like on disk, judged from its files and not from
/// the text of an error.
///
/// "Data.db is absent" has three unrelated causes, and an error string cannot
/// tell them apart (`No such file or directory` is what all three produce):
///
/// 1. compacted away with its metadata left behind: the TOC survives;
/// 2. a stale enumeration or manifest entry: no file of the generation exists;
/// 3. EVICTED to the object store: the data is intact in S3 and a durable
///    `<gen>.evicted` marker records it.
///
/// Only the first holds no rows. Treating the others as the first silently
/// drops live rows from an index and reports it complete, which is the failure
/// mode this module exists to prevent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationState {
    /// `Data.db` is on disk: build from it.
    DataPresent,
    /// No local `Data.db`, but an eviction marker: restore it, then build.
    Evicted,
    /// No `Data.db`, no marker, but the TOC survives: compacted away.
    MetadataOnly,
    /// No `Data.db`, no marker, no TOC: the generation does not exist on disk.
    Absent,
}

impl GenerationState {
    /// Classify from which files exist. The marker outranks the TOC because an
    /// evicted generation may keep its TOC too.
    #[must_use]
    pub fn from_files(data: bool, evicted_marker: bool, toc: bool) -> Self {
        if data {
            Self::DataPresent
        } else if evicted_marker {
            Self::Evicted
        } else if toc {
            Self::MetadataOnly
        } else {
            Self::Absent
        }
    }

    /// The outcome when no build can run for this state, or `None` when one
    /// should (`DataPresent`, and `Evicted` once restored).
    #[must_use]
    pub fn skip_outcome(self) -> Option<BackfillOutcome> {
        match self {
            Self::DataPresent | Self::Evicted => None,
            Self::MetadataOnly => Some(BackfillOutcome::Vanished),
            Self::Absent => Some(BackfillOutcome::Failed),
        }
    }
}

/// What a rebuild covered, once vanished SSTables are discounted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coverage {
    /// SSTables the rebuild was asked to cover (the live set it enumerated).
    pub expected: usize,
    /// SSTables actually indexed.
    pub built: usize,
    /// SSTables whose data was gone but whose metadata survives. Not
    /// failures, and not coverage either: they hold no rows.
    pub vanished: usize,
    /// SSTables that should have been indexed and were not.
    pub failed: usize,
}

impl Coverage {
    /// Start a tally for a rebuild that enumerated `expected` SSTables.
    #[must_use]
    pub fn expecting(expected: usize) -> Self {
        Self {
            expected,
            built: 0,
            vanished: 0,
            failed: 0,
        }
    }

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

    /// Every enumerated SSTable was either built, discounted as vanished, or
    /// failed. Anything else fell through and is unaccounted for.
    #[must_use]
    pub fn unaccounted(&self) -> usize {
        self.expected
            .saturating_sub(self.built + self.vanished + self.failed)
    }

    /// Whether reads through this index are complete.
    ///
    /// Requires BOTH that nothing failed and that every enumerated SSTable is
    /// accounted for. `failed == 0` alone is not enough: it is also true when
    /// generations were skipped for a reason that was never recorded as a
    /// failure, which is how a rebuild reported 1 of 21 SSTables as complete.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.failed == 0 && self.unaccounted() == 0
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
        if self.unaccounted() > 0 {
            text.push_str(&format!(
                " ({} enumerated SSTables were never accounted for)",
                self.unaccounted()
            ));
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tally(built: usize, vanished: usize, failed: usize) -> Coverage {
        let mut coverage = Coverage::expecting(built + vanished + failed);
        for _ in 0..built {
            coverage.record(BackfillOutcome::Built);
        }
        for _ in 0..vanished {
            coverage.record(BackfillOutcome::Vanished);
        }
        for _ in 0..failed {
            coverage.record(BackfillOutcome::Failed);
        }
        coverage
    }

    fn scan(declared: u64, scanned: u64, expected: Option<u64>, indexed: u64) -> ScanTally {
        ScanTally {
            partitions_declared: declared,
            partitions_scanned: scanned,
            entries_expected: expected,
            entries_indexed: indexed,
        }
    }

    #[test]
    fn a_scan_that_matches_its_declaration_and_expectation_verifies() {
        scan(10, 10, Some(10), 10).verify().unwrap();
        scan(0, 0, Some(0), 0).verify().unwrap();
        scan(10, 10, None, 3).verify().unwrap();
    }

    #[test]
    fn a_scan_that_read_fewer_partitions_than_declared_fails_naming_both() {
        let err = scan(100, 32, None, 32).verify().unwrap_err();
        assert!(
            err.contains("declares 100") && err.contains("read 32"),
            "{err}"
        );
        assert!(err.contains("68"), "the shortfall must be stated: {err}");
    }

    #[test]
    fn fewer_entries_than_enumerated_partitions_fails() {
        let err = scan(10, 10, Some(10), 0).verify().unwrap_err();
        assert!(err.contains("10") && err.contains("0 entries"), "{err}");
    }

    /// The live failure: an index reported complete while covering 32,632 of
    /// 102,840 rows (31.7%). Any tolerance wide enough to admit that is not a
    /// guard, so the check is pinned at exactly the live numbers, and at one
    /// short of full on either side of the line.
    #[test]
    fn the_live_shortfall_of_32632_of_102840_is_rejected() {
        let err = scan(102_840, 32_632, None, 32_632).verify().unwrap_err();
        assert!(err.contains("102840") && err.contains("32632"), "{err}");
        let err = scan(102_840, 102_840, Some(102_840), 32_632)
            .verify()
            .unwrap_err();
        assert!(
            err.contains("102840") && err.contains("32632 entries"),
            "{err}"
        );
    }

    /// Where the line is drawn, and why: tolerance is zero. The footer count is
    /// the writer's own, so every healthy SSTable matches it exactly. Exact
    /// equality is accepted; one short is rejected.
    #[test]
    fn the_line_sits_exactly_at_equality() {
        scan(102_840, 102_840, Some(102_840), 102_840)
            .verify()
            .expect("equality is the only accepted partition count");
        scan(102_840, 102_839, None, 102_839)
            .verify()
            .expect_err("one partition short must be rejected");
        scan(102_840, 102_840, Some(102_840), 102_839)
            .verify()
            .expect_err("one entry short must be rejected");
    }

    /// Not a tolerance: for a cell-valued index a null cell yields no entry, so
    /// fewer entries than partitions is legitimate and is NOT checked.
    #[test]
    fn a_cell_index_with_fewer_entries_than_partitions_is_accepted() {
        scan(100, 100, None, 40).verify().unwrap();
    }

    #[test]
    fn a_single_missing_entry_fails_because_tolerance_is_zero() {
        scan(10, 10, Some(10), 9)
            .verify()
            .expect_err("one missing entry is one missing partition");
    }

    /// The legitimate #406 case: the TOC survives, the data file is gone.
    #[test]
    fn metadata_without_a_data_file_is_vanished() {
        let state = GenerationState::from_files(false, false, true);
        assert_eq!(state, GenerationState::MetadataOnly);
        assert_eq!(state.skip_outcome(), Some(BackfillOutcome::Vanished));
    }

    /// The live bug: a generation with no file at all was discounted as
    /// "compacted away" because its open error read "No such file or directory".
    #[test]
    fn a_generation_with_no_files_at_all_is_a_failure_never_vanished() {
        let state = GenerationState::from_files(false, false, false);
        assert_eq!(state, GenerationState::Absent);
        assert_eq!(state.skip_outcome(), Some(BackfillOutcome::Failed));
    }

    /// Eviction leaves the data intact in the object store; it must be
    /// restored and built, never skipped, even if its TOC is also gone.
    #[test]
    fn an_evicted_generation_is_never_skipped() {
        for toc in [true, false] {
            let state = GenerationState::from_files(false, true, toc);
            assert_eq!(state, GenerationState::Evicted);
            assert_eq!(state.skip_outcome(), None);
        }
    }

    /// A generation whose data is on disk is built, whatever else is true.
    #[test]
    fn a_generation_with_data_is_built() {
        for marker in [true, false] {
            for toc in [true, false] {
                let state = GenerationState::from_files(true, marker, toc);
                assert_eq!(state, GenerationState::DataPresent);
                assert_eq!(state.skip_outcome(), None);
            }
        }
    }

    /// The denominator counts SSTables that HAD rows, not metadata.
    #[test]
    fn vanished_sstables_do_not_count_against_coverage() {
        let coverage = tally(1, 20, 0);
        assert_eq!(coverage.indexable(), 1, "only one SSTable held rows");
        assert!(coverage.is_complete());
    }

    /// One genuine failure still makes it incomplete.
    #[test]
    fn a_single_real_failure_keeps_the_index_incomplete() {
        let coverage = tally(1, 1, 1);
        assert_eq!(coverage.indexable(), 2);
        assert!(!coverage.is_complete());
    }

    /// `failed == 0` is not completeness: an enumerated SSTable that was
    /// never recorded in any bucket leaves the index short.
    #[test]
    fn an_unaccounted_sstable_is_not_complete_even_with_no_failures() {
        let mut coverage = Coverage::expecting(21);
        coverage.record(BackfillOutcome::Built);
        assert_eq!(coverage.failed, 0);
        assert_eq!(coverage.unaccounted(), 20);
        assert!(!coverage.is_complete());
        assert!(coverage.describe().contains("never accounted for"));
    }

    /// Vanished SSTables are REPORTED, never silently discounted.
    #[test]
    fn the_report_explains_the_missing_sstables() {
        let text = tally(1, 20, 0).describe();
        assert!(text.contains("1 of 1"), "{text}");
        assert!(text.contains("20"), "{text}");
        assert!(text.contains("compacted"), "{text}");
    }

    /// An incomplete rebuild still says so first.
    #[test]
    fn an_incomplete_rebuild_leads_with_the_bad_news() {
        let text = tally(0, 1, 1).describe();
        assert!(text.contains("STILL incomplete"), "{text}");
    }

    /// An all-vanished table is complete: it holds zero rows.
    #[test]
    fn a_table_whose_sstables_all_vanished_is_complete() {
        let coverage = tally(0, 5, 0);
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
            sstables_failed: 0,
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
            sstables_failed: 3,
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
            sstables_failed: 0,
        }
        .is_complete());
        assert!(!RebuildOutcome {
            sstables_rebuilt: 16,
            sstables_total: 17,
            sstables_vanished: 0,
            sstables_failed: 0,
        }
        .is_complete());
    }
}
