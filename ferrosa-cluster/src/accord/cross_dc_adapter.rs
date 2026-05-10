//! Sprint 7 W7.7 — Cross-DC write adapter (Accord coordination glue).
//!
//! When `route_for_cl` returns the [`CrossDcAccord`] route, the
//! coordinator drives the Accord protocol across both DCs' Raft
//! groups via this adapter. Each DC commits its share via the W7.6
//! [`MembershipChanger::accord_vote_commit`] apply-durability barrier.
//!
//! Today the adapter is intentionally thin — a single entry point
//! ([`CrossDcAccordAdapter::vote_commit_local`]) that the coordinator
//! invokes once Accord pre-accept clears across DCs. Sprint 7 W7.7 wires
//! the routing decision and the metric trace; the full pre-accept /
//! recovery state machine for cross-DC fan-out is the existing
//! `accord/coordinator.rs` plumbing — this adapter is the *glue* that
//! translates an "Accord vote-commit on this DC's Raft" into the
//! durable barrier.
//!
//! The metric counter [`CROSS_DC_VOTE_COMMITS`] is the W7.7 trace: a
//! test asserts it ticks up when a cross-DC write is dispatched.
//!
//! [`CrossDcAccord`]: crate::coordinator::cl_routing::CLRoute::CrossDcAccord
//! [`MembershipChanger::accord_vote_commit`]:
//!     crate::membership::MembershipChanger::accord_vote_commit

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ferrosa_common::{AccordTimestamp, TxnId};

use crate::membership::{MembershipChanger, MembershipError, MembershipNetwork};

/// Process-global counter of cross-DC Accord vote-commits dispatched
/// through this adapter. The W7.7 acceptance test inspects this
/// counter as a trace of the routing path.
pub static CROSS_DC_VOTE_COMMITS: AtomicU64 = AtomicU64::new(0);

/// Snapshot the current cross-DC vote-commit count.
pub fn cross_dc_vote_commit_count() -> u64 {
    CROSS_DC_VOTE_COMMITS.load(Ordering::Relaxed)
}

/// Adapter wrapping a per-DC [`MembershipChanger`] — used by the
/// coordinator's cross-DC write path. The adapter is constructed once
/// per DC (mirroring the per-DC changer scoping in W6.4).
pub struct CrossDcAccordAdapter<N: MembershipNetwork> {
    changer: Arc<MembershipChanger<N>>,
}

impl<N: MembershipNetwork> CrossDcAccordAdapter<N> {
    /// Build an adapter scoped to a single DC's [`MembershipChanger`].
    pub fn new(changer: Arc<MembershipChanger<N>>) -> Self {
        Self { changer }
    }

    /// DC name of the underlying changer (sanity check / metric label).
    pub fn dc_name(&self) -> &str {
        self.changer.dc_name()
    }

    /// Drive the local-DC apply for an Accord-coordinated write.
    /// Bumps [`CROSS_DC_VOTE_COMMITS`] on every successful invocation
    /// so the W7.7 acceptance test can verify the routing path was
    /// taken (vs. the Sprint 6 `NotImplemented` stub).
    ///
    /// Wraps [`MembershipChanger::accord_vote_commit`]; the apply
    /// barrier (W7.6) ensures durability before the call returns.
    pub async fn vote_commit_local(
        &self,
        txn_id: TxnId,
        hlc: AccordTimestamp,
        mutation: Vec<u8>,
    ) -> Result<(), MembershipError> {
        self.changer
            .accord_vote_commit(txn_id, hlc, mutation)
            .await?;
        CROSS_DC_VOTE_COMMITS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Counter is monotonic — `fetch_add(1)` once → +1.
    #[test]
    fn cross_dc_vote_commit_counter_is_monotonic() {
        let before = cross_dc_vote_commit_count();
        CROSS_DC_VOTE_COMMITS.fetch_add(1, Ordering::Relaxed);
        let after = cross_dc_vote_commit_count();
        assert_eq!(after, before + 1);
    }
}
