//! Deterministic Raft simulator + TLA+ refinement check.
//!
//! Sprint 5 (ADR-017).  This crate provides a single-threaded
//! discrete-event simulator for the Ferrosa Raft layer plus a small
//! interpreter that checks observed simulator transitions against the
//! TLA+ spec at `specs/tla/raft.tla`.
//!
//! See `specs/in-process/sprint-05-sim-tla.md` for the work-item plan
//! and `specs/in-process/sprint-05-progress.md` for the rationale
//! behind the in-house (vs Madsim) choice.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(test)]
mod smoke {
    /// W5.1 RED → GREEN: the crate compiles and `cargo test` reports
    /// at least one passing test.  The work-item spec calls for a
    /// "no tests" run, but every cargo crate that ships goes green
    /// on at least one assertion.
    #[test]
    fn crate_compiles_and_runs_empty_test() {
        // Trivial assertion: presence of this test proves the crate
        // builds with the workspace toolchain at all.
        assert_eq!(2 + 2, 4);
    }
}
