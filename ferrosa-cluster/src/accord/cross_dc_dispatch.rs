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

/// Resolve ONE contact per remote DC from the peers this node knows.
///
/// Peers whose announced DC is unknown (an older build, or a peer that
/// announced nothing) are NOT targets: an unplaceable peer is treated as local
/// everywhere else in the codebase, and inventing a remote DC for it here would
/// send a cross-DC copy to a node that is probably in this very DC. Local-DC
/// peers are the mesh, not the fan-out.
pub fn dc_contacts(
    peers: &[(Uuid, SocketAddr)],
    peer_dcs: &HashMap<Uuid, String>,
    local_dc: &str,
) -> BTreeMap<String, DcContact> {
    let mut contacts: BTreeMap<String, DcContact> = BTreeMap::new();
    for (host_id, addr) in peers {
        let Some(dc) = peer_dcs.get(host_id) else {
            continue;
        };
        if dc == local_dc {
            continue;
        }
        let candidate = DcContact {
            dc: dc.clone(),
            host_id: *host_id,
            addr: *addr,
        };
        match contacts.get(&candidate.dc) {
            Some(existing) if existing.host_id <= candidate.host_id => {}
            _ => {
                contacts.insert(candidate.dc.clone(), candidate);
            }
        }
    }
    contacts
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
}

impl FanoutReport {
    /// True when every remote DC acknowledged.
    pub fn is_complete(&self, contacts: &BTreeMap<String, DcContact>) -> bool {
        self.failed.is_empty() && self.delivered.len() == contacts.len()
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

    /// Send `op` to every remote DC in `contacts`, at most once per attempt per
    /// DC, retrying a DC that fails until it acknowledges or the attempt budget
    /// runs out. A DC that acknowledges is never sent a second copy.
    pub async fn fan_out<Op, S>(
        &self,
        sender: &S,
        op: &Op,
        contacts: &BTreeMap<String, DcContact>,
    ) -> FanoutReport
    where
        Op: Send + Sync,
        S: CrossDcSender<Op>,
    {
        let mut report = FanoutReport::default();
        for contact in contacts.values() {
            let mut attempt = 1;
            let mut backoff = self.policy.backoff;
            loop {
                match sender.send_to_contact(contact, op).await {
                    Ok(()) => {
                        CROSS_DC_SENDS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        report.delivered.push(contact.dc.clone());
                        break;
                    }
                    Err(reason) => {
                        if attempt >= self.policy.max_attempts {
                            CROSS_DC_SEND_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(
                                dc = %contact.dc,
                                host = %contact.host_id,
                                attempts = attempt,
                                %reason,
                                "cross-DC delivery failed: this DC did not receive the operation"
                            );
                            report.failed.push(contact.dc.clone());
                            break;
                        }
                        CROSS_DC_RETRIES_TOTAL.fetch_add(1, Ordering::Relaxed);
                        report.retries += 1;
                        tracing::warn!(
                            dc = %contact.dc,
                            host = %contact.host_id,
                            attempt,
                            %reason,
                            "cross-DC delivery not acknowledged; retrying"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2);
                        attempt += 1;
                    }
                }
            }
        }
        report
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
    let contacts = dc_contacts(peers, peer_dcs, local_dc);
    if contacts.is_empty() {
        return Ok(FanoutReport::default());
    }
    let sender = RaftCommandDcSender { peer_manager };
    let dispatch = CrossDcDispatch::default();
    Ok(dispatch.fan_out(&sender, cmd, &contacts).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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

    /// A sender that records every attempt: `(dc, attempt)` in call order.
    #[derive(Default)]
    struct RecordingSender {
        attempts: Mutex<Vec<(String, String)>>,
        /// How many leading attempts a DC refuses before it acknowledges.
        refuse_first: Mutex<HashMap<String, u32>>,
    }

    impl RecordingSender {
        fn with_refusals(dc: &str, refusals: u32) -> Self {
            let s = Self::default();
            s.refuse_first
                .lock()
                .unwrap()
                .insert(dc.to_string(), refusals);
            s
        }

        fn sends_for(&self, dc: &str) -> usize {
            self.attempts
                .lock()
                .unwrap()
                .iter()
                .filter(|(d, _)| d == dc)
                .count()
        }
    }

    #[async_trait]
    impl CrossDcSender<u64> for RecordingSender {
        async fn send_to_contact(&self, contact: &DcContact, op: &u64) -> Result<(), String> {
            self.attempts
                .lock()
                .unwrap()
                .push((contact.dc.clone(), op.to_string()));
            let mut refusals = self.refuse_first.lock().unwrap();
            if let Some(remaining) = refusals.get_mut(&contact.dc) {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err("not acknowledged".to_string());
                }
            }
            Ok(())
        }
    }

    /// The link count is per DC, not per node: 2 DCs → 1 contact, 3 → 2,
    /// 4 → 3; adding nodes to a DC never adds a contact.
    #[test]
    fn dc_contacts_hold_exactly_one_contact_per_remote_dc() {
        let peers = [
            peer(1, 7001), // dc1 (local) — never a target
            peer(2, 7002), // dc2
            peer(3, 7003), // dc2
            peer(4, 7004), // dc2
            peer(5, 7005), // dc3
            peer(6, 7006), // dc3
        ];
        let dcs = with_dcs(&[
            (Uuid::from_u128(1), "dc1"),
            (Uuid::from_u128(2), "dc2"),
            (Uuid::from_u128(3), "dc2"),
            (Uuid::from_u128(4), "dc2"),
            (Uuid::from_u128(5), "dc3"),
            (Uuid::from_u128(6), "dc3"),
        ]);

        let contacts = dc_contacts(&peers, &dcs, "dc1");

        assert_eq!(
            contacts.keys().cloned().collect::<Vec<_>>(),
            vec!["dc2".to_string(), "dc3".to_string()],
            "a 3-DC cluster holds exactly two cross-DC contacts: one per other DC"
        );
        assert_eq!(
            contacts["dc2"].host_id,
            Uuid::from_u128(2),
            "the contact is deterministic — the lowest host id in that DC"
        );
        assert_eq!(contacts["dc2"].addr, addr(7002));
        assert_eq!(contacts["dc3"].host_id, Uuid::from_u128(5));

        // 2 DCs → 1 contact; 4 DCs → 3 contacts.
        let two_dc = dc_contacts(
            &[peer(1, 7001), peer(2, 7002)],
            &with_dcs(&[(Uuid::from_u128(1), "dc1"), (Uuid::from_u128(2), "dc2")]),
            "dc1",
        );
        assert_eq!(two_dc.len(), 1, "2 DCs: each DC has one link to the other");

        let peers4 = [peer(1, 7001), peer(2, 7002), peer(3, 7003), peer(4, 7004)];
        let dcs4 = with_dcs(&[
            (Uuid::from_u128(1), "dc1"),
            (Uuid::from_u128(2), "dc2"),
            (Uuid::from_u128(3), "dc3"),
            (Uuid::from_u128(4), "dc4"),
        ]);
        let four_dc = dc_contacts(&peers4, &dcs4, "dc1");
        assert_eq!(
            four_dc.len(),
            3,
            "4 DCs: each DC gets one copy from each of the other three"
        );
        assert!(
            !four_dc.contains_key("dc1"),
            "a DC never dispatches to itself"
        );
    }

    /// A peer whose DC was never announced must not become a cross-DC target:
    /// unplaceable peers are treated as local everywhere else.
    #[test]
    fn dc_contacts_skip_peers_with_no_announced_dc() {
        let peers = [peer(1, 7001), peer(2, 7002), peer(9, 7009)];
        let dcs = with_dcs(&[(Uuid::from_u128(1), "dc1"), (Uuid::from_u128(2), "dc2")]);

        let contacts = dc_contacts(&peers, &dcs, "dc1");

        assert_eq!(
            contacts.len(),
            1,
            "only the announced remote DC is a target"
        );
        assert!(contacts.contains_key("dc2"));
    }

    /// One copy per remote DC per attempt — never one per remote node.
    #[tokio::test]
    async fn fan_out_sends_exactly_one_copy_per_remote_dc() {
        let peers = [peer(2, 7002), peer(3, 7003), peer(4, 7004), peer(5, 7005)];
        let dcs = with_dcs(&[
            (Uuid::from_u128(2), "dc2"),
            (Uuid::from_u128(3), "dc2"),
            (Uuid::from_u128(4), "dc2"),
            (Uuid::from_u128(5), "dc3"),
        ]);
        let contacts = dc_contacts(&peers, &dcs, "dc1");
        let sender = RecordingSender::default();

        let report = CrossDcDispatch::default()
            .fan_out(&sender, &7u64, &contacts)
            .await;

        assert_eq!(
            sender.attempts.lock().unwrap().len(),
            2,
            "four remote NODES in two DCs must receive exactly two copies"
        );
        assert_eq!(sender.sends_for("dc2"), 1);
        assert_eq!(sender.sends_for("dc3"), 1);
        assert_eq!(report.delivered, vec!["dc2".to_string(), "dc3".to_string()]);
        assert_eq!(report.retries, 0);
        assert!(report.is_complete(&contacts));
    }

    /// A DC that refuses an attempt is retried, and the DC that already
    /// acknowledged is not sent a second copy.
    #[tokio::test]
    async fn fan_out_retries_a_refusing_dc_without_resending_to_a_delivered_dc() {
        let peers = [peer(2, 7002), peer(5, 7005)];
        let dcs = with_dcs(&[(Uuid::from_u128(2), "dc2"), (Uuid::from_u128(5), "dc3")]);
        let contacts = dc_contacts(&peers, &dcs, "dc1");
        let sender = RecordingSender::with_refusals("dc3", 1);

        let report = CrossDcDispatch::default()
            .fan_out(&sender, &9u64, &contacts)
            .await;

        assert_eq!(sender.sends_for("dc3"), 2, "the refusing DC got one retry");
        assert_eq!(
            sender.sends_for("dc2"),
            1,
            "a delivered DC is never sent a second copy"
        );
        assert_eq!(report.retries, 1);
        assert_eq!(report.failed, Vec::<String>::new());
        assert!(report.is_complete(&contacts));
    }

    /// A DC that never acknowledges is reported, not silently dropped, and the
    /// other DCs are still delivered.
    #[tokio::test]
    async fn fan_out_reports_a_dc_that_never_acknowledges() {
        let peers = [peer(2, 7002), peer(5, 7005)];
        let dcs = with_dcs(&[(Uuid::from_u128(2), "dc2"), (Uuid::from_u128(5), "dc3")]);
        let contacts = dc_contacts(&peers, &dcs, "dc1");
        let sender = RecordingSender::with_refusals("dc3", u32::MAX);
        let dispatch = CrossDcDispatch::with_policy(CrossDcRetryPolicy {
            max_attempts: 3,
            backoff: Duration::from_millis(1),
        });

        let report = dispatch.fan_out(&sender, &1u64, &contacts).await;

        assert_eq!(sender.sends_for("dc3"), 3, "bounded attempts, then give up");
        assert_eq!(report.failed, vec!["dc3".to_string()]);
        assert_eq!(report.delivered, vec!["dc2".to_string()]);
        assert!(!report.is_complete(&contacts));
    }

    /// No remote DCs → nothing to do, and nothing counted.
    #[tokio::test]
    async fn fan_out_with_only_local_dc_sends_nothing() {
        let peers = [peer(1, 7001), peer(9, 7009)];
        let dcs = with_dcs(&[(Uuid::from_u128(1), "dc1")]);
        let contacts = dc_contacts(&peers, &dcs, "dc1");
        assert!(contacts.is_empty());

        let sender = RecordingSender::default();
        let report = CrossDcDispatch::default()
            .fan_out(&sender, &1u64, &contacts)
            .await;

        assert!(report.delivered.is_empty());
        assert!(report.failed.is_empty());
        assert_eq!(sender.attempts.lock().unwrap().len(), 0);
    }
}
