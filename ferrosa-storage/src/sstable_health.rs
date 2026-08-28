//! Module: Decide whether an SSTable generation on disk can serve reads.
//!
//! Correctness: Correct when every way a critical component can fail to serve
//! — absent, empty, or unreadable — withholds the generation from readers, and
//! when a component that is legitimately empty does not.
//!
//! Last revised: 2026-08-28
//! Last changed: New.
//!
//! # Why this is a pure function and not an fs call
//!
//! The decision lives inside `load_existing_sstables_and_sidecars`, which walks
//! a data directory and needs an engine, a manifest and a disk to run at all.
//! The judgement it makes — is this generation servable — needs none of those,
//! and is the part that was wrong. Separated, it is testable without a cluster.

/// What probing one component on disk found.
///
/// `Missing` and `Unreadable` are distinct because they mean different things
/// to an operator: one is a file that is gone, the other is a file that is
/// there and cannot be stat'd. Both withhold the generation; only the message
/// differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComponentProbe {
    /// The component exists and holds this many bytes.
    Present(u64),
    /// The path does not exist.
    Missing,
    /// The path exists but could not be inspected.
    Unreadable,
}

/// Why a critical component cannot serve reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComponentDefect {
    /// Zero bytes. For a critical component this is unrecoverable.
    Empty,
    /// The file the manifest points at is not on disk.
    Missing,
    /// Present, but its metadata could not be read.
    Unreadable,
}

impl ComponentDefect {
    /// How this reads in a log line an operator has to act on.
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Self::Empty => "zero-byte",
            Self::Missing => "missing from disk",
            Self::Unreadable => "unreadable",
        }
    }
}

/// The components a generation cannot serve reads without.
///
/// Rows.db is deliberately absent. The SSTable writer emits a zero-byte
/// Rows.db for simple partitions that need no per-partition row index, and the
/// reader treats a missing or empty Rows.db as "no row index". Treating it as
/// critical would quarantine healthy SSTables.
pub(crate) const CRITICAL_COMPONENTS: [&str; 2] = ["Data.db", "Partitions.db"];

/// The first critical component that cannot serve reads, if any.
///
/// Returns the first rather than all of them because the caller's decision is
/// binary — withhold the generation or do not — and naming one concrete file
/// is what makes the log line actionable. Order follows `probes`, so the
/// message is deterministic when several components are defective.
pub(crate) fn first_unusable_component<'a>(
    probes: &[(&'a str, ComponentProbe)],
) -> Option<(&'a str, ComponentDefect)> {
    probes
        .iter()
        .find_map(|(name, probe)| component_defect(*probe).map(|defect| (*name, defect)))
}

/// Whether one probed component can serve reads.
const fn component_defect(probe: ComponentProbe) -> Option<ComponentDefect> {
    match probe {
        ComponentProbe::Present(0) => Some(ComponentDefect::Empty),
        ComponentProbe::Present(_) => None,
        // A component the manifest references but disk does not have cannot be
        // read, and deferring that to the read path costs an operator a whole
        // table: the failure that reaches them is a bare ENOENT naming no
        // file, no generation and no table.
        ComponentProbe::Missing => Some(ComponentDefect::Missing),
        ComponentProbe::Unreadable => Some(ComponentDefect::Unreadable),
    }
}

/// Probe one component path, mapping io failures onto the states above.
pub(crate) fn probe_component(path: &std::path::Path) -> ComponentProbe {
    match std::fs::metadata(path) {
        Ok(meta) => ComponentProbe::Present(meta.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ComponentProbe::Missing,
        Err(_) => ComponentProbe::Unreadable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probes(
        data: ComponentProbe,
        partitions: ComponentProbe,
    ) -> Vec<(&'static str, ComponentProbe)> {
        vec![("Data.db", data), ("Partitions.db", partitions)]
    }

    /// The ordinary healthy generation.
    #[test]
    fn a_generation_whose_components_all_hold_bytes_is_servable() {
        let probed = probes(ComponentProbe::Present(4_096), ComponentProbe::Present(512));
        assert_eq!(first_unusable_component(&probed), None);
    }

    /// Pins the behaviour that already existed: a zero-byte critical component
    /// is unrecoverable and must be withheld.
    #[test]
    fn a_zero_byte_data_file_is_unusable() {
        let probed = probes(ComponentProbe::Present(0), ComponentProbe::Present(512));
        assert_eq!(
            first_unusable_component(&probed),
            Some(("Data.db", ComponentDefect::Empty))
        );
    }

    /// The defect this module exists for.
    ///
    /// Startup repair used to treat a MISSING component as benign while
    /// treating a zero-byte one as fatal, on the reasoning that it "will fail
    /// in open_sstable_from_dir". It does — as `storage: I/O error: No such
    /// file or directory (os error 2)` with no table, no generation and no
    /// path, failing every read of that table. Missing is strictly worse than
    /// empty and must be caught in the same place.
    #[test]
    fn a_missing_data_file_is_unusable() {
        let probed = probes(ComponentProbe::Missing, ComponentProbe::Present(512));
        assert_eq!(
            first_unusable_component(&probed),
            Some(("Data.db", ComponentDefect::Missing))
        );
    }

    /// The other critical component, so the rule is not accidentally
    /// Data.db-only.
    #[test]
    fn a_missing_partitions_file_is_unusable() {
        let probed = probes(ComponentProbe::Present(4_096), ComponentProbe::Missing);
        assert_eq!(
            first_unusable_component(&probed),
            Some(("Partitions.db", ComponentDefect::Missing))
        );
    }

    /// Present but unstat-able is still unservable, and says so differently.
    #[test]
    fn an_unreadable_component_is_unusable() {
        let probed = probes(ComponentProbe::Unreadable, ComponentProbe::Present(512));
        assert_eq!(
            first_unusable_component(&probed),
            Some(("Data.db", ComponentDefect::Unreadable))
        );
    }

    /// Deterministic reporting: with two defects the message names the first,
    /// so the same broken generation does not log a different file each boot.
    #[test]
    fn the_first_defect_in_order_is_the_one_reported() {
        let probed = probes(ComponentProbe::Missing, ComponentProbe::Present(0));
        assert_eq!(
            first_unusable_component(&probed),
            Some(("Data.db", ComponentDefect::Missing))
        );
    }

    /// Rows.db is legitimately zero-byte for simple partitions, so it must not
    /// be in the critical set — including it would quarantine healthy tables.
    #[test]
    fn rows_db_is_not_a_critical_component() {
        assert!(!CRITICAL_COMPONENTS.contains(&"Rows.db"));
        assert_eq!(CRITICAL_COMPONENTS, ["Data.db", "Partitions.db"]);
    }

    /// The defects an operator reads are distinguishable.
    #[test]
    fn each_defect_describes_itself_distinctly() {
        let all = [
            ComponentDefect::Empty,
            ComponentDefect::Missing,
            ComponentDefect::Unreadable,
        ];
        let described: std::collections::BTreeSet<_> = all.iter().map(|d| d.describe()).collect();
        assert_eq!(described.len(), all.len(), "two defects read the same");
    }
}
