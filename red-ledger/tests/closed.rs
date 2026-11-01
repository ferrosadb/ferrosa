//! Reds that have a closing test but are still listed as red.
//!
//! A closing test lives in the crate that owns the code and carries the red's name. Once it exists,
//! the Lean item is flipped to `.met` and the generator drops the row from [`red_ledger::REDS`].
//! `CLOSED` lists the names whose closing tests have landed; a name in both lists is the state where
//! somebody has written the test but not closed the ledger.

use red_ledger::REDS;

/// Names of reds whose closing test has landed in the owning crate. Empty: every red is still red.
const CLOSED: &[&str] = &[];

#[test]
fn a_closed_red_is_no_longer_listed_as_red() {
    for name in CLOSED {
        assert!(
            !REDS.iter().any(|red| red.name == *name),
            "{name} has a closing test, but the Lean ledger still lists it as red — close the \
             claim in the specification so the row leaves the table"
        );
    }
}
