//! Tests for red ledger items that have been fixed.
//!
//! The stub in `reds.rs` names the gap and asserts nothing; this file holds the assertion that
//! closes it. The order is: fix the code, move the test here under the same name, then flip the
//! ledger item in the Lean source so the generator stops emitting the stub. `cargo test -p
//! red-ledger` fails if a name appears in both files, which is the state where somebody has
//! written the test but not closed the ledger, or closed the ledger but left the stub.
//!
//! Nothing is here yet: every red in the corpus is still red.

/// A closed item: its name must match a red stub in `reds.rs` until the Lean ledger is updated.
#[test]
fn closed_tests_do_not_overlap_open_reds() {
    let open = include_str!("reds.rs");
    let closed = include_str!("closed.rs");
    let names = |src: &'static str| -> Vec<&'static str> {
        src.lines()
            .filter_map(|l| l.trim().strip_prefix("fn "))
            .filter_map(|l| l.split('(').next())
            .filter(|n| !n.starts_with("closed_tests_do_not_overlap"))
            .collect()
    };
    for name in names(closed) {
        assert!(
            !names(open).contains(&name),
            "{name} has been fixed and tested, but the Lean ledger still lists it as red — \
             close the claim in the specification so the stub stops being generated"
        );
    }
}
