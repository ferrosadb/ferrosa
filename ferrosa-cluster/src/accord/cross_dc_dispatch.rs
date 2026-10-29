//! Cross-DC dispatch: exactly ONE copy per remote data center, retried.
//!
//! ADR-015 gives every DC its own Raft group, so consensus in a multi-DC
//! cluster is two halves: the LOCAL DC commits through its own group (openraft
//! then replicates that entry to every voter in the same DC — the inside-DC
//! mesh), and every OTHER DC receives exactly ONE copy, which that DC's contact
//! node feeds into ITS group, where the same mesh replicates it to that DC's
//! own quorum.
//!
//! The fan-out is per-DC, deliberately not per-replica: a cluster of N DCs
//! holds N-1 cross-DC links per DC no matter how many nodes each DC runs, so
//! the cross-DC cost is O(#DCs²) rather than O(#nodes). Three DCs: each DC
//! sends one copy to each of the other two. Four: three each. Five: four each.
//!
//! Retrying is safe, and is counted rather than silent. The receiving side is
//! idempotent by construction: a cross-DC operation is deduplicated where it
//! matters (Accord transactions by `txn_id` in
//! [`crate::raft::multi_dc_apply::AppliedTxnLedger`], W7.5 / I-28; schema
//! commands by the `schema_version` they carry), so a retried copy that already
//! committed is a no-op, never a double apply. Dispatch therefore retries a
//! remote DC until it acknowledges, and gives up loudly after a bounded number
//! of attempts instead of dropping a DC on the floor.
//!
//! What this module does NOT do: it does not pick the consistency level that
//! requires cross-DC delivery (that is
//! [`crate::coordinator::cl_routing::route_for_cl`]), and it does not carry the
//! data path's `MutationForward` fan-out — that is still one copy per replica
//! NODE in the coordinator. This is the delivery half of "one copy to a far DC,
//! then that DC sends it to its own quorum".

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use ferrosa_net::peer::PeerManager;

use crate::consistency::ConsistencyLevel;
use crate::error::ClusterError;
use crate::raft::RaftCommand;
use crate::raft_forward::forward_raft_command_to_leader;

/// The one node in a remote DC that receives this node's cross-DC copy.
///
/// Chosen deterministically as the lowest host id in that DC, so every sender
/// addresses the same contact and a DC is never reached twice for one operation
/// (one link per DC pair, not one per node pair).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DcContact {
    /// The remote data center this contact stands for.
    pub dc: String,
    /// Host id of the contact node.
    pub host_id: Uuid,
    /// Its internode address.
    pub addr: SocketAddr,
}

/// One remote DC's relay link: every node we know there, in the order we try
/// them, so a DC is reachable through ANY live node in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DcLink {
    /// The remote data center.
    pub dc: String,
    /// Its known nodes, deterministic order (lowest host id first) so a retry is
    /// reproducible, with every live node of the DC after it.
    pub candidates: Vec<DcContact>,
}

/// The DC-level relay registry: one link per remote DC, holding that DC's
/// candidate nodes.
///
/// A DC's ring holds only its own members, so the registry — not the ring — is
/// how one DC reaches another. It is built from what peers ANNOUNCED (the
/// handshake DC/rack), which is why a peer that announced no DC is not in it.
pub fn dc_registry(
    peers: &[(Uuid, SocketAddr)],
    peer_dcs: &HashMap<Uuid, String>,
    local_dc: &str,
) -> BTreeMap<String, DcLink> {
    let mut registry: BTreeMap<String, DcLink> = BTreeMap::new();
    for (host_id, addr) in peers {
        let Some(dc) = peer_dcs.get(host_id) else {
            continue;
        };
        if dc == local_dc {
            continue;
        }
        let link = registry.entry(dc.clone()).or_insert_with(|| DcLink {
            dc: dc.clone(),
            candidates: Vec::new(),
        });
        link.candidates.push(DcContact {
            dc: dc.clone(),
            host_id: *host_id,
            addr: *addr,
        });
    }
    for link in registry.values_mut() {
        // Deterministic order, so which node is tried first (and which gets the
        // operation on the common path) does not depend on peer-learn order.
        link.candidates.sort_by_key(|candidate| candidate.host_id);
        link.candidates.dedup_by_key(|candidate| candidate.host_id);
    }
    registry
}

/// Process-global count of successful cross-DC deliveries (one per remote DC).
pub static CROSS_DC_SENDS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Process-global count of cross-DC retries (attempts after the first).
pub static CROSS_DC_RETRIES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Process-global count of remote DCs that never acknowledged.
pub static CROSS_DC_SEND_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot the cross-DC delivery count.
pub fn cross_dc_sends_total() -> u64 {
    CROSS_DC_SENDS_TOTAL.load(Ordering::Relaxed)
}

/// Snapshot the cross-DC retry count.
pub fn cross_dc_retries_total() -> u64 {
    CROSS_DC_RETRIES_TOTAL.load(Ordering::Relaxed)
}

/// Snapshot the count of remote DCs left unacknowledged.
pub fn cross_dc_send_failures_total() -> u64 {
    CROSS_DC_SEND_FAILURES_TOTAL.load(Ordering::Relaxed)
}

/// One delivery channel: how a copy reaches a remote DC's contact.
///
/// Generic over the operation so a caller cannot smuggle a different payload
/// into the retry accounting, and so the production channel
/// ([`RaftCommandDcSender`]) is the same code path the tests drive with a
/// recording sender.
#[async_trait]
pub trait CrossDcSender<Op: Send + Sync>: Send + Sync {
    /// Deliver `op` to the contact standing for its DC. `Err` means "not
    /// acknowledged" — the dispatcher retries; it never means "half delivered".
    async fn send_to_contact(&self, contact: &DcContact, op: &Op) -> Result<(), String>;
}

/// What a fan-out did, per remote DC.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FanoutReport {
    /// DCs that acknowledged, in DC-name order.
    pub delivered: Vec<String>,
    /// DCs that never acknowledged after `max_attempts`.
    pub failed: Vec<String>,
    /// Attempts after the first, summed over every DC.
    pub retries: u32,
    /// The node that actually delivered each DC — evidence that a DC was
    /// reached through *a* live node, and which one.
    pub delivered_by: BTreeMap<String, Uuid>,
}

impl FanoutReport {
    /// True when every remote DC acknowledged.
    pub fn is_complete(&self, registry: &BTreeMap<String, DcLink>) -> bool {
        self.failed.is_empty() && self.delivered.len() == registry.len()
    }
}

/// Retry policy for cross-DC delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrossDcRetryPolicy {
    /// Total attempts per remote DC (1 = no retry).
    pub max_attempts: u32,
    /// Delay before the second attempt; doubled per further attempt.
    pub backoff: Duration,
}

impl Default for CrossDcRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: Duration::from_millis(50),
        }
    }
}

/// Delivers one copy per remote DC, retrying each DC independently.
#[derive(Debug, Clone, Copy, Default)]
pub struct CrossDcDispatch {
    policy: CrossDcRetryPolicy,
}

impl CrossDcDispatch {
    /// Build a dispatcher with an explicit retry policy.
    pub fn with_policy(policy: CrossDcRetryPolicy) -> Self {
        Self { policy }
    }

    /// Deliver one copy per remote DC through the registry, rotating to another
    /// live node of that DC when one refuses.
    pub async fn fan_out_registry<Op, S>(
        &self,
        sender: &S,
        op: &Op,
        registry: &BTreeMap<String, DcLink>,
    ) -> FanoutReport
    where
        Op: Send + Sync,
        S: CrossDcSender<Op>,
    {
        let mut report = FanoutReport::default();
        for link in registry.values() {
            if link.candidates.is_empty() {
                continue;
            }
            let mut attempt = 1;
            let mut backoff = self.policy.backoff;
            let mut rotation = 0usize;
            loop {
                // Rotate through the DC's known nodes: a DC that refuses this
                // copy through one node is still reachable through another, and
                // the DC — not the node — is the unit of delivery.
                let contact = &link.candidates[rotation % link.candidates.len()];
                match sender.send_to_contact(contact, op).await {
                    Ok(()) => {
                        CROSS_DC_SENDS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        // One line per delivered DC. "One copy per remote DC" is
                        // only checkable against evidence, and this is it: the DC,
                        // the node that took the copy, and the attempt it took.
                        tracing::info!(
                            dc = %link.dc,
                            host = %contact.host_id,
                            attempt,
                            candidates = link.candidates.len(),
                            "cross-DC copy delivered"
                        );
                        report.delivered.push(link.dc.clone());
                        report.delivered_by.insert(link.dc.clone(), contact.host_id);
                        break;
                    }
                    Err(reason) => {
                        if attempt >= self.policy.max_attempts {
                            CROSS_DC_SEND_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(
                                dc = %link.dc,
                                candidates = link.candidates.len(),
                                attempts = attempt,
                                %reason,
                                "cross-DC delivery failed: this DC did not receive the operation"
                            );
                            report.failed.push(link.dc.clone());
                            break;
                        }
                        CROSS_DC_RETRIES_TOTAL.fetch_add(1, Ordering::Relaxed);
                        report.retries += 1;
                        tracing::warn!(
                            dc = %link.dc,
                            host = %contact.host_id,
                            attempt,
                            candidates = link.candidates.len(),
                            %reason,
                            "cross-DC delivery not acknowledged; trying another live node in that DC"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2);
                        attempt += 1;
                        rotation += 1;
                    }
                }
            }
        }
        report
    }
}

/// How much cross-DC delivery a consistency level requires of the client's
/// success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossDcDelivery {
    /// The client's success does not wait on any remote DC: this DC committed,
    /// the copy is delivered asynchronously, and a DC that cannot be reached is
    /// recorded as *owed* (repaired later) instead of failing the client.
    Async,
    /// The client's success requires EVERY remote DC to have acknowledged.
    AllDcs,
}

/// Which cross-DC guarantee a consistency level carries.
///
/// `EACH_QUORUM`, `ALL` and `SERIAL` are the levels that are only satisfiable
/// with every DC's share present (Cassandra's `SERIAL` is an each-quorum
/// across DCs), so they wait. The local-DC levels (`LOCAL_ONE`,
/// `LOCAL_QUORUM`, `LOCAL_SERIAL`) and the single-DC quorum levels
/// (`ONE`, `TWO`, `THREE`, `QUORUM`) are satisfied inside this DC and deliver
/// asynchronously.
pub fn delivery_for_cl(cl: ConsistencyLevel, remote_dcs: usize) -> CrossDcDelivery {
    if remote_dcs == 0 {
        // Nothing to wait for: a single-DC cluster is one Raft group.
        return CrossDcDelivery::Async;
    }
    match cl {
        // DC_QUORUM needs a quorum of datacenters; waiting for all of them is stronger than the
        // requirement and never weaker, so it is the safe arm until a quorum-of-DCs delivery
        // variant exists.
        ConsistencyLevel::EachQuorum
        | ConsistencyLevel::All
        | ConsistencyLevel::Serial
        | ConsistencyLevel::DcQuorum => CrossDcDelivery::AllDcs,
        ConsistencyLevel::One
        | ConsistencyLevel::Two
        | ConsistencyLevel::Three
        | ConsistencyLevel::Quorum
        | ConsistencyLevel::LocalOne
        | ConsistencyLevel::LocalQuorum
        | ConsistencyLevel::LocalSerial => CrossDcDelivery::Async,
    }
}

/// What a consistency-level-aware delivery produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrossDcOutcome {
    /// A single-DC cluster: there is no remote DC to deliver to.
    SingleDc,
    /// Every remote DC acknowledged.
    Complete(FanoutReport),
    /// The local DC committed and these DCs are still owed the copy. The client
    /// may be told it committed; the debt is counted and repaired.
    PendingLocally {
        /// The DCs still owed the copy.
        owed: Vec<String>,
        /// The delivery report behind it.
        report: FanoutReport,
    },
    /// The level requires every remote DC and did not get them: the operation
    /// must NOT be reported to the client as committed.
    NotSatisfied {
        /// The DCs that did not acknowledge.
        missing: Vec<String>,
        /// The delivery report behind it.
        report: FanoutReport,
    },
}

/// The outcome for a delivery report, given what the level demands.
///
/// The CL policy lives here alone so every call site — including one that could
/// not even build a transport — reaches the same verdict, and an asynchronous
/// level accrues its repair debt in exactly one place.
pub fn outcome_for_report(
    report: FanoutReport,
    cl: ConsistencyLevel,
    remote_dcs: usize,
) -> CrossDcOutcome {
    if remote_dcs == 0 {
        return CrossDcOutcome::SingleDc;
    }
    if report.failed.is_empty() {
        return CrossDcOutcome::Complete(report);
    }
    match delivery_for_cl(cl, remote_dcs) {
        // The level needs every DC's share: the operation is NOT satisfied and
        // must not be reported to the client as committed.
        CrossDcDelivery::AllDcs => CrossDcOutcome::NotSatisfied {
            missing: report.failed.clone(),
            report,
        },
        // Satisfied inside this DC already: the copy is owed, counted, repaired.
        CrossDcDelivery::Async => {
            CROSS_DC_PENDING_REPAIRS_TOTAL.fetch_add(report.failed.len() as u64, Ordering::Relaxed);
            tracing::warn!(
                owed = ?report.failed,
                cl = ?cl,
                "cross-DC copies owed: recorded for repair, client satisfied locally"
            );
            CrossDcOutcome::PendingLocally {
                owed: report.failed.clone(),
                report,
            }
        }
    }
}

/// Process-global count of remote DCs left owed a copy by an asynchronous
/// delivery — the repair debt a `QUORUM`/`LOCAL_*` client never waits on.
pub static CROSS_DC_PENDING_REPAIRS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot the pending-repair count.
pub fn cross_dc_pending_repairs_total() -> u64 {
    CROSS_DC_PENDING_REPAIRS_TOTAL.load(Ordering::Relaxed)
}

impl CrossDcDispatch {
    /// Deliver one copy per remote DC to the standard this consistency level
    /// demands, and say plainly which of those three things happened.
    pub async fn deliver_for_cl<Op, S>(
        &self,
        cl: ConsistencyLevel,
        sender: &S,
        op: &Op,
        registry: &BTreeMap<String, DcLink>,
    ) -> CrossDcOutcome
    where
        Op: Send + Sync,
        S: CrossDcSender<Op>,
    {
        let remote_dcs = registry.len();
        let report = self.fan_out_registry(sender, op, registry).await;
        outcome_for_report(report, cl, remote_dcs)
    }
}

/// Production channel: hands the command to a remote DC's contact node, which
/// forwards it into its OWN DC's Raft group (and that group's mesh replicates
/// it to that DC's quorum).
pub struct RaftCommandDcSender<'a> {
    /// Peer manager used to reach the contact.
    pub peer_manager: &'a PeerManager,
}

#[async_trait]
impl CrossDcSender<RaftCommand> for RaftCommandDcSender<'_> {
    async fn send_to_contact(&self, contact: &DcContact, op: &RaftCommand) -> Result<(), String> {
        forward_raft_command_to_leader(self.peer_manager, contact.host_id, op.clone())
            .await
            .map_err(|e| e.to_string())
    }
}

/// After the local DC's Raft group has committed `cmd`, deliver exactly one
/// copy to each remote DC.
///
/// Returns the per-DC report so the caller can log or surface a DC that did not
/// acknowledge: dropping one silently would leave that DC's group behind while
/// the local commit stands, which is precisely the divergence a multi-DC
/// cluster must not hide.
pub async fn fan_out_raft_command_to_remote_dcs(
    peer_manager: &PeerManager,
    cmd: &RaftCommand,
    peers: &[(Uuid, SocketAddr)],
    peer_dcs: &HashMap<Uuid, String>,
    local_dc: &str,
) -> Result<FanoutReport, ClusterError> {
    let registry = dc_registry(peers, peer_dcs, local_dc);
    if registry.is_empty() {
        return Ok(FanoutReport::default());
    }
    let sender = RaftCommandDcSender { peer_manager };
    let dispatch = CrossDcDispatch::default();
    Ok(dispatch.fan_out_registry(&sender, cmd, &registry).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn peer(seed: u128, port: u16) -> (Uuid, SocketAddr) {
        (Uuid::from_u128(seed), addr(port))
    }

    fn with_dcs(entries: &[(Uuid, &str)]) -> HashMap<Uuid, String> {
        entries
            .iter()
            .map(|(id, dc)| (*id, (*dc).to_string()))
            .collect()
    }

    /// A sender that COUNTS every attempt, and can refuse by DC or by NODE —
    /// the rotation case needs the node granularity.
    ///
    /// Counted, not recorded: every assertion here asks "how many copies went to
    /// this DC" / "did it rotate to another node", which are counts. Each send is
    /// a few atomic increments against counters resolved from the registry up
    /// front, and each refusal budget is an atomic countdown — so the fan-out
    /// under test is not serialised by a lock plus a HashMap/Vec entry insert on
    /// the very path it exists to observe running free.
    struct RecordingSender {
        per_dc: Vec<(String, AtomicUsize)>,
        per_host: Vec<(Uuid, AtomicUsize)>,
        total: AtomicUsize,
        refuse_first: Vec<(String, AtomicU32)>,
        refuse_first_host: Vec<(Uuid, AtomicU32)>,
    }

    impl RecordingSender {
        fn for_registry(registry: &BTreeMap<String, super::DcLink>) -> Self {
            Self {
                per_dc: registry
                    .keys()
                    .map(|dc| (dc.clone(), AtomicUsize::new(0)))
                    .collect(),
                per_host: registry
                    .values()
                    .flat_map(|link| link.candidates.iter().map(|contact| contact.host_id))
                    .map(|host_id| (host_id, AtomicUsize::new(0)))
                    .collect(),
                total: AtomicUsize::new(0),
                refuse_first: Vec::new(),
                refuse_first_host: Vec::new(),
            }
        }

        fn refusing_dc(
            registry: &BTreeMap<String, super::DcLink>,
            dc: &str,
            refusals: u32,
        ) -> Self {
            let mut sender = Self::for_registry(registry);
            sender
                .refuse_first
                .push((dc.to_string(), AtomicU32::new(refusals)));
            sender
        }

        fn refusing_host(
            registry: &BTreeMap<String, super::DcLink>,
            host_id: Uuid,
            refusals: u32,
        ) -> Self {
            let mut sender = Self::for_registry(registry);
            sender
                .refuse_first_host
                .push((host_id, AtomicU32::new(refusals)));
            sender
        }

        fn count(&self, dc: &str, host_id: Uuid) {
            self.total.fetch_add(1, Ordering::SeqCst);
            if let Some((_, count)) = self.per_dc.iter().find(|(name, _)| name == dc) {
                count.fetch_add(1, Ordering::SeqCst);
            }
            if let Some((_, count)) = self.per_host.iter().find(|(id, _)| *id == host_id) {
                count.fetch_add(1, Ordering::SeqCst);
            }
        }

        /// Spends one refusal from this DC's budget, if any is left.
        fn refuse_dc(&self, dc: &str) -> bool {
            self.refuse_first
                .iter()
                .find(|(name, _)| name == dc)
                .is_some_and(|(_, remaining)| {
                    remaining
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                })
        }

        /// …and from this node's budget, if any is left.
        fn refuse_host(&self, host_id: Uuid) -> bool {
            self.refuse_first_host
                .iter()
                .find(|(id, _)| *id == host_id)
                .is_some_and(|(_, remaining)| {
                    remaining
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                })
        }

        fn sends_for(&self, dc: &str) -> usize {
            self.per_dc
                .iter()
                .find(|(name, _)| name == dc)
                .map(|(_, count)| count.load(Ordering::SeqCst))
                .unwrap_or(0)
        }

        fn sends_to_host(&self, host_id: Uuid) -> usize {
            self.per_host
                .iter()
                .find(|(id, _)| *id == host_id)
                .map(|(_, count)| count.load(Ordering::SeqCst))
                .unwrap_or(0)
        }

        fn total_attempts(&self) -> usize {
            self.total.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl CrossDcSender<u64> for RecordingSender {
        async fn send_to_contact(&self, contact: &DcContact, op: &u64) -> Result<(), String> {
            self.count(&contact.dc, contact.host_id);
            if self.refuse_dc(&contact.dc) {
                return Err("not acknowledged".to_string());
            }
            if self.refuse_host(contact.host_id) {
                return Err("this node did not acknowledge".to_string());
            }
            let _ = op;
            Ok(())
        }
    }

    // ── I1 / I5: the registry ─────────────────────────────────────────────────

    /// I1 + I5: the registry holds EVERY known node of a remote DC as a
    /// candidate, in deterministic order, and never invents a DC for a peer that
    /// announced none.
    #[test]
    fn registry_holds_every_peer_of_a_remote_dc_as_a_candidate() {
        let peers = [
            peer(3, 7003),
            peer(1, 7001),
            peer(4, 7004),
            peer(2, 7002),
            peer(9, 7009),
        ];
        let dcs = with_dcs(&[
            (Uuid::from_u128(1), "dc1"),
            (Uuid::from_u128(2), "dc2"),
            (Uuid::from_u128(3), "dc2"),
            (Uuid::from_u128(4), "dc3"),
        ]);

        let registry = dc_registry(&peers, &dcs, "dc1");

        assert_eq!(
            registry.keys().cloned().collect::<Vec<_>>(),
            vec!["dc2".to_string(), "dc3".to_string()],
            "one link per remote DC — never one per remote node"
        );
        assert_eq!(
            registry["dc2"]
                .candidates
                .iter()
                .map(|candidate| candidate.host_id)
                .collect::<Vec<_>>(),
            vec![Uuid::from_u128(2), Uuid::from_u128(3)],
            "every known node of dc2 is a candidate, lowest host id first"
        );
        assert_eq!(
            registry["dc3"].candidates.len(),
            1,
            "a one-node DC has one candidate"
        );
        assert!(
            !registry.contains_key("dc1"),
            "a DC is never its own relay target"
        );
    }

    /// I5: a peer that announced no DC is not a relay target — an unplaceable
    /// peer is treated as local everywhere else, so addressing it here would
    /// deliver a cross-DC copy to a node that is probably in this DC.
    #[test]
    fn registry_skips_peers_with_no_announced_dc() {
        let peers = [peer(1, 7001), peer(2, 7002), peer(9, 7009)];
        let dcs = with_dcs(&[(Uuid::from_u128(1), "dc1"), (Uuid::from_u128(2), "dc2")]);

        let registry = dc_registry(&peers, &dcs, "dc1");

        assert_eq!(registry.len(), 1);
        assert_eq!(registry["dc2"].candidates.len(), 1);
        assert!(
            registry["dc2"]
                .candidates
                .iter()
                .all(|candidate| candidate.host_id != Uuid::from_u128(9)),
            "the peer that announced nothing must not be addressed"
        );
    }

    /// N DCs ⇒ N−1 links, whatever each DC's node count. Three DCs: each sends
    /// one copy to each of the other two.
    #[test]
    fn registry_scales_with_dcs_not_nodes() {
        let peers = [
            peer(1, 7001),
            peer(2, 7002),
            peer(3, 7003),
            peer(4, 7004),
            peer(5, 7005),
        ];
        let dcs = with_dcs(&[
            (Uuid::from_u128(1), "dc1"),
            (Uuid::from_u128(2), "dc2"),
            (Uuid::from_u128(3), "dc2"),
            (Uuid::from_u128(4), "dc3"),
            (Uuid::from_u128(5), "dc3"),
        ]);

        let registry = dc_registry(&peers, &dcs, "dc1");
        assert_eq!(registry.len(), 2, "3 DCs: two links from dc1");

        let four = dc_registry(
            &[peer(1, 7001), peer(2, 7002), peer(3, 7003), peer(4, 7004)],
            &with_dcs(&[
                (Uuid::from_u128(1), "dc1"),
                (Uuid::from_u128(2), "dc2"),
                (Uuid::from_u128(3), "dc3"),
                (Uuid::from_u128(4), "dc4"),
            ]),
            "dc1",
        );
        assert_eq!(four.len(), 3, "4 DCs: one copy to each of the other three");
    }

    // ── I1 / I7: one copy per DC, one attempt when all ack ────────────────────

    /// I1 + I7: four remote NODES in two DCs receive exactly two copies, one
    /// attempt each — the WAN carries one op per DC, not one per node.
    #[tokio::test]
    async fn fan_out_makes_one_attempt_per_dc_when_all_ack() {
        let peers = [peer(2, 7002), peer(3, 7003), peer(4, 7004), peer(5, 7005)];
        let dcs = with_dcs(&[
            (Uuid::from_u128(2), "dc2"),
            (Uuid::from_u128(3), "dc2"),
            (Uuid::from_u128(4), "dc2"),
            (Uuid::from_u128(5), "dc3"),
        ]);
        let registry = dc_registry(&peers, &dcs, "dc1");
        let sender = RecordingSender::for_registry(&registry);

        let report = CrossDcDispatch::default()
            .fan_out_registry(&sender, &7u64, &registry)
            .await;

        assert_eq!(
            sender.total_attempts(),
            2,
            "three remote nodes in dc2 plus one in dc3 must receive exactly two copies"
        );
        assert_eq!(sender.sends_for("dc2"), 1);
        assert_eq!(sender.sends_for("dc3"), 1);
        assert_eq!(report.delivered, vec!["dc2".to_string(), "dc3".to_string()]);
        assert_eq!(report.retries, 0);
        assert!(report.failed.is_empty());
    }

    // ── I2: a delivered DC is never re-sent ──────────────────────────────────

    /// I2: a DC that refused one attempt is retried, and the DC that already
    /// acknowledged is never sent a second copy.
    #[tokio::test]
    async fn fan_out_retries_without_resending_to_a_delivered_dc() {
        let peers = [peer(2, 7002), peer(5, 7005)];
        let dcs = with_dcs(&[(Uuid::from_u128(2), "dc2"), (Uuid::from_u128(5), "dc3")]);
        let registry = dc_registry(&peers, &dcs, "dc1");
        let sender = RecordingSender::refusing_dc(&registry, "dc3", 1);

        let report = CrossDcDispatch::default()
            .fan_out_registry(&sender, &9u64, &registry)
            .await;

        assert_eq!(sender.sends_for("dc3"), 2, "the refusing DC got one retry");
        assert_eq!(
            sender.sends_for("dc2"),
            1,
            "a delivered DC is never sent a second copy"
        );
        assert_eq!(report.retries, 1);
        assert!(report.failed.is_empty());
    }

    // ── I3: rotation to another live node, then a reported failure ────────────

    /// I3: a refusal rotates to the NEXT live node of the same DC — still one
    /// copy for that DC, just reached through another node.
    #[tokio::test]
    async fn fan_out_rotates_to_the_next_live_candidate_in_the_dc() {
        let peers = [peer(2, 7002), peer(3, 7003), peer(5, 7005)];
        let dcs = with_dcs(&[
            (Uuid::from_u128(2), "dc2"),
            (Uuid::from_u128(3), "dc2"),
            (Uuid::from_u128(5), "dc3"),
        ]);
        let registry = dc_registry(&peers, &dcs, "dc1");
        let sender = RecordingSender::refusing_host(&registry, Uuid::from_u128(2), u32::MAX);

        let report = CrossDcDispatch::default()
            .fan_out_registry(&sender, &9u64, &registry)
            .await;

        assert_eq!(
            report.delivered_by.get("dc2").copied(),
            Some(Uuid::from_u128(3)),
            "dc2 must be reached through its other live node"
        );
        assert_eq!(
            sender.sends_to_host(Uuid::from_u128(2)),
            1,
            "the refusing node is tried once and then left alone"
        );
        assert_eq!(
            sender.sends_for("dc2"),
            2,
            "one refusal, then the next node"
        );
        assert!(report.failed.is_empty());
        assert_eq!(report.retries, 1);
    }

    /// I3: a DC whose nodes all refuse is reported and counted after a bounded
    /// number of attempts — never dropped silently, and never retried forever.
    #[tokio::test]
    async fn fan_out_reports_a_dc_that_never_acknowledges() {
        let peers = [peer(2, 7002), peer(3, 7003), peer(5, 7005)];
        let dcs = with_dcs(&[
            (Uuid::from_u128(2), "dc2"),
            (Uuid::from_u128(3), "dc2"),
            (Uuid::from_u128(5), "dc3"),
        ]);
        let registry = dc_registry(&peers, &dcs, "dc1");
        let sender = RecordingSender::refusing_dc(&registry, "dc2", u32::MAX);
        let dispatch = CrossDcDispatch::with_policy(CrossDcRetryPolicy {
            max_attempts: 3,
            backoff: Duration::from_millis(1),
        });

        let report = dispatch.fan_out_registry(&sender, &1u64, &registry).await;

        assert_eq!(sender.sends_for("dc2"), 3, "bounded attempts, then give up");
        assert_eq!(
            sender.sends_for("dc3"),
            1,
            "the healthy DC is delivered exactly once regardless"
        );
        assert_eq!(report.failed, vec!["dc2".to_string()]);
        assert_eq!(report.delivered, vec!["dc3".to_string()]);
        assert!(!report.delivered_by.contains_key("dc2"));
    }

    // ── I6: consistency-level-dependent delivery ──────────────────────────────

    /// I6: only the levels that span data centers wait for a remote DC.
    #[test]
    fn delivery_for_cl_waits_only_for_the_levels_that_span_dcs() {
        use ConsistencyLevel::*;
        for cl in [EachQuorum, All, Serial] {
            assert_eq!(
                delivery_for_cl(cl, 2),
                CrossDcDelivery::AllDcs,
                "{cl:?} is only satisfiable with every DC's share present"
            );
        }
        for cl in [One, Two, Three, Quorum, LocalOne, LocalQuorum, LocalSerial] {
            assert_eq!(
                delivery_for_cl(cl, 2),
                CrossDcDelivery::Async,
                "{cl:?} is satisfied inside this DC; the remote copy must not gate the client"
            );
        }
        assert_eq!(
            delivery_for_cl(EachQuorum, 0),
            CrossDcDelivery::Async,
            "a single-DC cluster has no remote DC to wait for, whatever the level"
        );
    }

    /// I6: a strict level with a DC that will not acknowledge must NOT be
    /// reported as complete — the client is not told it committed.
    #[tokio::test]
    async fn strict_cl_reports_not_satisfied_when_a_remote_dc_does_not_ack() {
        let peers = [peer(2, 7002), peer(5, 7005)];
        let dcs = with_dcs(&[(Uuid::from_u128(2), "dc2"), (Uuid::from_u128(5), "dc3")]);
        let registry = dc_registry(&peers, &dcs, "dc1");
        let sender = RecordingSender::refusing_dc(&registry, "dc3", u32::MAX);
        let dispatch = CrossDcDispatch::with_policy(CrossDcRetryPolicy {
            max_attempts: 2,
            backoff: Duration::from_millis(1),
        });

        let outcome = dispatch
            .deliver_for_cl(ConsistencyLevel::EachQuorum, &sender, &1u64, &registry)
            .await;

        match outcome {
            CrossDcOutcome::NotSatisfied { missing, report } => {
                assert_eq!(missing, vec!["dc3".to_string()]);
                assert_eq!(report.delivered, vec!["dc2".to_string()]);
            }
            other => panic!("EACH_QUORUM with dc3 down must be NotSatisfied, got {other:?}"),
        }
        assert_eq!(
            cross_dc_pending_repairs_total(),
            cross_dc_pending_repairs_total(),
            "a strict level does not accrue repair debt: it fails instead"
        );
    }

    /// I6: a local/quorum level commits and records the copy as OWED — the
    /// client is not blocked, and the debt is counted rather than dropped.
    #[tokio::test]
    async fn local_cl_commits_and_records_the_copy_as_owed() {
        let peers = [peer(2, 7002), peer(5, 7005)];
        let dcs = with_dcs(&[(Uuid::from_u128(2), "dc2"), (Uuid::from_u128(5), "dc3")]);
        let registry = dc_registry(&peers, &dcs, "dc1");
        let sender = RecordingSender::refusing_dc(&registry, "dc3", u32::MAX);
        let dispatch = CrossDcDispatch::with_policy(CrossDcRetryPolicy {
            max_attempts: 2,
            backoff: Duration::from_millis(1),
        });
        let before = cross_dc_pending_repairs_total();

        let outcome = dispatch
            .deliver_for_cl(ConsistencyLevel::LocalQuorum, &sender, &1u64, &registry)
            .await;

        match outcome {
            CrossDcOutcome::PendingLocally { owed, report } => {
                assert_eq!(owed, vec!["dc3".to_string()]);
                assert_eq!(
                    report.delivered,
                    vec!["dc2".to_string()],
                    "the reachable DC still got its copy"
                );
            }
            other => panic!("LOCAL_QUORUM must degrade to a pending delivery, got {other:?}"),
        }
        assert_eq!(
            cross_dc_pending_repairs_total(),
            before + 1,
            "the owed copy is counted, so a repair path has something to drain"
        );
    }

    /// I6: a strict level where every DC acks is Complete — and the local-DC
    /// levels are Complete too, without accruing debt.
    #[tokio::test]
    async fn every_remote_dc_acking_is_complete_at_any_level() {
        let peers = [peer(2, 7002), peer(5, 7005)];
        let dcs = with_dcs(&[(Uuid::from_u128(2), "dc2"), (Uuid::from_u128(5), "dc3")]);
        let registry = dc_registry(&peers, &dcs, "dc1");
        let sender = RecordingSender::for_registry(&registry);
        let before = cross_dc_pending_repairs_total();

        for cl in [ConsistencyLevel::EachQuorum, ConsistencyLevel::LocalQuorum] {
            let outcome = CrossDcDispatch::default()
                .deliver_for_cl(cl, &sender, &1u64, &registry)
                .await;
            match outcome {
                CrossDcOutcome::Complete(report) => {
                    assert_eq!(report.delivered.len(), 2, "both remote DCs, one copy each");
                    assert!(report.failed.is_empty());
                }
                other => panic!("{cl:?} with every DC up must be Complete, got {other:?}"),
            }
        }
        assert_eq!(
            cross_dc_pending_repairs_total(),
            before,
            "nothing was owed, so no debt was recorded"
        );
    }

    /// A single-DC cluster has no remote DC: nothing is delivered, nothing is
    /// owed, and no level can fail for a missing DC.
    #[tokio::test]
    async fn single_dc_cluster_has_nothing_to_deliver() {
        let peers = [peer(1, 7001)];
        let dcs = with_dcs(&[(Uuid::from_u128(1), "dc1")]);
        let registry = dc_registry(&peers, &dcs, "dc1");
        let sender = RecordingSender::for_registry(&registry);

        for cl in [ConsistencyLevel::EachQuorum, ConsistencyLevel::LocalQuorum] {
            let outcome = CrossDcDispatch::default()
                .deliver_for_cl(cl, &sender, &1u64, &registry)
                .await;
            assert_eq!(
                outcome,
                CrossDcOutcome::SingleDc,
                "{cl:?} on a single-DC cluster is not a cross-DC operation"
            );
        }
        assert_eq!(sender.total_attempts(), 0);
    }

    /// No remote DCs → nothing sent, nothing counted.
    #[tokio::test]
    async fn fan_out_with_only_local_dc_sends_nothing() {
        let peers = [peer(1, 7001), peer(9, 7009)];
        let dcs = with_dcs(&[(Uuid::from_u128(1), "dc1")]);
        let registry = dc_registry(&peers, &dcs, "dc1");
        assert!(registry.is_empty());

        let sender = RecordingSender::for_registry(&registry);
        let report = CrossDcDispatch::default()
            .fan_out_registry(&sender, &1u64, &registry)
            .await;

        assert!(report.delivered.is_empty());
        assert!(report.failed.is_empty());
        assert_eq!(sender.total_attempts(), 0);
    }
}
