//! Module: the interim jsonb gate on leaving standalone (T-300, D24, D15a).
//!
//! Responsibility: the DDL half of the gate lives in
//! `ferrosa_schema::jsonb_rules` (one rule for both wires, checked at entry and
//! apply). This module is the transition half: while any table holds jsonb, a
//! node may not leave `Standalone`, at startup (peers/seeds configured or a
//! former cluster member) or at runtime (a handshake that would move it to
//! `Pair`, `Forming` or `Cluster`). The controller's mode cell is the schema's
//! mode cell, so the DDL gate always sees the mode this controller holds.
//!
//! This module also computes and publishes the DDL gate's verdict (D32): the
//! cluster's jsonb readiness is read from the live member view and each member's
//! advertised capability bits, and stored in the schema's gate cell whenever the
//! membership changes. Fail-closed throughout — an unread member, or a member set
//! that is not established, is unknown, and unknown is never capable.
//!
//! Correctness: a refusal names every table holding jsonb and D15a, never
//! suggests a bypass, and there is none. A schema that cannot be scanned is
//! refused too (fail closed, FMEA SCH-T300-06). Refusals are logged as edges:
//! one ERROR when refusing starts, one INFO when it clears.
//! Last revised: 2026-09-28
//! Last changed: New module (T-300).

use std::sync::atomic::Ordering;

pub use ferrosa_schema::jsonb_rules::{jsonb_ddl_permitted, JsonbClusterGate, JsonbDdlRefused};

use ferrosa_schema::system::peers::ClusterState as _;
use std::sync::Arc;

/// The view this module's rule reads until the controller publishes the real one
/// (D32): [`JsonbClusterGate::Unknown`] keeps the transition rule fail-closed, so a
/// node holding jsonb still may not leave standalone. Wiring the controller's peer
/// view in here is the follow-up slice, not a silent loosening now.
const TRANSITION_VIEW: JsonbClusterGate = JsonbClusterGate::Unknown;

use super::{DeploymentMode, ModeController};

/// Why a node may not leave (or start outside) standalone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonbTransitionRefused {
    /// These tables hold jsonb.
    Tables {
        target: DeploymentMode,
        tables: Vec<String>,
    },
    /// The schema could not be scanned, so absence of jsonb is unproven.
    Unverifiable {
        target: DeploymentMode,
        reason: String,
    },
}

impl std::fmt::Display for JsonbTransitionRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tables { target, tables } => write!(
                f,
                "refusing to leave standalone for {target} mode: jsonb columns exist in {} and \
                 are allowed on a standalone node only until the capability ledger (D15a) lands",
                tables.join(", ")
            ),
            Self::Unverifiable { target, reason } => write!(
                f,
                "refusing to leave standalone for {target} mode: the schema could not be \
                 scanned for jsonb columns ({reason}); jsonb requires the capability ledger \
                 (D15a) outside standalone"
            ),
        }
    }
}

impl std::error::Error for JsonbTransitionRefused {}

impl ModeController {
    /// Refuse a move from a jsonb-permitting mode (standalone) to `target`
    /// while any table holds jsonb (SCH-T300-05). Pure: no logging, no state.
    pub(super) fn check_leaving_standalone(
        &self,
        target: DeploymentMode,
    ) -> Result<(), JsonbTransitionRefused> {
        let current = self.mode();
        if jsonb_ddl_permitted(current, &TRANSITION_VIEW).is_err()
            || jsonb_ddl_permitted(target, &TRANSITION_VIEW).is_ok()
        {
            return Ok(());
        }
        self.check_jsonb_absent(target)
    }

    /// The startup check (SCH-T300-05): a node that is not standalone, or is
    /// configured with seeds and so will not stay standalone, must not carry
    /// jsonb columns. The caller exits non-zero on `Err`.
    pub fn check_startup_jsonb(&self) -> Result<(), JsonbTransitionRefused> {
        let current = self.mode();
        let target = if jsonb_ddl_permitted(current, &TRANSITION_VIEW).is_err() {
            current
        } else if !self.net_config.seeds.is_empty() {
            DeploymentMode::Pair
        } else {
            return Ok(());
        };
        self.check_jsonb_absent(target)
    }

    fn check_jsonb_absent(&self, target: DeploymentMode) -> Result<(), JsonbTransitionRefused> {
        match self.schema.tables_with_jsonb() {
            Ok(tables) if tables.is_empty() => Ok(()),
            Ok(tables) => Err(JsonbTransitionRefused::Tables { target, tables }),
            Err(e) => Err(JsonbTransitionRefused::Unverifiable {
                target,
                reason: e.to_string(),
            }),
        }
    }

    /// Whether the node may move to `target`; logs the edges only. Called at
    /// the top of every transition out of standalone, before any write path
    /// is touched, and again inside `try_transition_mode` as a backstop.
    pub(super) fn leaving_standalone_permitted(&self, target: DeploymentMode) -> bool {
        match self.check_leaving_standalone(target) {
            Ok(()) => {
                if self.jsonb_gate_refusing.swap(false, Ordering::Relaxed) {
                    tracing::info!(%target, "jsonb gate cleared: leaving standalone is allowed again");
                }
                true
            }
            Err(refused) => {
                if !self.jsonb_gate_refusing.swap(true, Ordering::Relaxed) {
                    tracing::error!(%refused, "jsonb gate: staying standalone (D24, D15a)");
                }
                false
            }
        }
    }

    /// True while a departure from standalone is being refused (for readiness).
    pub fn jsonb_gate_refusing(&self) -> bool {
        self.jsonb_gate_refusing.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// D32: computing and publishing the verdict
// ---------------------------------------------------------------------------

/// How many times the refresh re-reads member capabilities before publishing
/// "unknown", and the gap between attempts. A member whose handshake has not
/// completed is unknown, not incapable, so a single unlucky read must not refuse
/// jsonb DDL until the next membership event.
const CAPABILITY_READ_ATTEMPTS: u32 = 5;
const CAPABILITY_READ_BACKOFF: std::time::Duration = std::time::Duration::from_millis(200);

/// The verdict for one member view (D32).
///
/// `members` pairs each member's display name with the capability bits it advertised on
/// its live connection, `None` when it could not be read. Every rule is fail-closed: an
/// unread member — a peer with no live connection, or one whose handshake has not
/// completed — is unknown, and unknown is never capable. Pure, so the decision is tested
/// with no sockets, no timers and no peers.
pub(crate) fn verdict_from_member_caps<S: AsRef<str>>(
    members: impl IntoIterator<Item = (S, Option<u32>)>,
) -> JsonbClusterGate {
    let members: Vec<(S, Option<u32>)> = members.into_iter().collect();
    if members.is_empty() {
        // The member set is not established, so nothing can be claimed about it.
        return JsonbClusterGate::Unknown;
    }
    if members.iter().any(|(_, caps)| caps.is_none()) {
        return JsonbClusterGate::Unknown;
    }
    let lacking: Vec<String> = members
        .iter()
        .filter(|(_, caps)| !caps.is_some_and(|c| c & ferrosa_net::handshake::CAP_JSONB_V1 != 0))
        .map(|(name, _)| name.as_ref().to_string())
        .collect();
    if lacking.is_empty() {
        JsonbClusterGate::Capable
    } else {
        JsonbClusterGate::Incapable { members: lacking }
    }
}

/// Compute the cluster's jsonb readiness from the live member view and publish it into
/// the schema's gate cell — the cell the DDL gate reads (D32).
///
/// `members` is the member view as the controller holds it, `(display name, host id)`;
/// `self_caps` is added as a member when the view does not already contain this node.
/// A view that is empty publishes unknown: the member set is not established.
pub(crate) async fn publish_jsonb_cluster_gate(
    schema: &ferrosa_schema::Schema,
    peer_manager: Option<&ferrosa_net::peer::PeerManager>,
    members: &[(String, uuid::Uuid)],
    self_host_id: uuid::Uuid,
) -> JsonbClusterGate {
    let mut rows: Vec<(String, Option<u32>)> = vec![(
        format!("{self_host_id} (this node)"),
        Some(ferrosa_net::handshake::LOCAL_CAPABILITIES),
    )];
    let mut verdict = JsonbClusterGate::Unknown;
    for attempt in 0..CAPABILITY_READ_ATTEMPTS {
        rows.truncate(1);
        for (name, host_id) in members {
            if *host_id == self_host_id {
                continue; // already present as this node
            }
            let caps = match peer_manager {
                Some(pm) => pm.peer_capabilities(*host_id).await,
                None => None,
            };
            rows.push((name.clone(), caps));
        }
        verdict = verdict_from_member_caps(rows.iter().map(|(n, c)| (n.as_str(), *c)));
        if !matches!(verdict, JsonbClusterGate::Unknown) || attempt + 1 == CAPABILITY_READ_ATTEMPTS
        {
            break;
        }
        tokio::time::sleep(CAPABILITY_READ_BACKOFF).await;
    }
    schema.set_jsonb_cluster_gate(verdict.clone());
    verdict
}

impl crate::controller::ModeController {
    /// Recompute and publish this node's jsonb readiness from the live member view
    /// (D32). Called whenever the membership changes — a member connecting, leaving,
    /// or reconnecting with another build — so the gate cannot lag the view for longer
    /// than the read takes.
    ///
    /// A view that is empty publishes unknown immediately rather than waiting on a peer
    /// manager with nothing to read: a cluster-mode node that cannot see its member set
    /// must refuse, not assume.
    pub(crate) fn spawn_jsonb_cluster_gate_refresh(&self) {
        let schema = Arc::clone(&self.schema);
        let self_host_id = self.local_host_id;
        let members: Vec<(String, uuid::Uuid)> = self
            .cluster_state
            .load()
            .peers()
            .into_iter()
            .map(|p| (format!("{}:{}", p.peer, p.peer_port), p.host_id))
            .collect();
        if members.is_empty() {
            schema.set_jsonb_cluster_gate(JsonbClusterGate::Unknown);
            return;
        }
        let peer_manager = self.peer_manager.load().clone();
        self.spawn_tracked(async move {
            let _ = publish_jsonb_cluster_gate(
                &schema,
                peer_manager.as_deref(),
                &members,
                self_host_id,
            )
            .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pure decision (D32), with no sockets, peers or timers. Written with the
    /// implementation rather than before it: a function that does not exist cannot
    /// fail to compile in a way that documents its own rule, so these are regression
    /// guards. Their control is the pair of `Unknown` cases below — treat an unread
    /// member as capable and they fail.
    #[test]
    fn a_view_whose_members_all_advertise_jsonb_is_capable() {
        let v = verdict_from_member_caps([
            ("10.0.0.1:7100", Some(ferrosa_net::handshake::CAP_JSONB_V1)),
            ("10.0.0.2:7100", Some(ferrosa_net::handshake::CAP_JSONB_V1)),
        ]);
        assert_eq!(v, JsonbClusterGate::Capable);
    }

    #[test]
    fn a_view_with_a_member_that_lacks_jsonb_names_it() {
        let v = verdict_from_member_caps([
            ("10.0.0.1:7100", Some(ferrosa_net::handshake::CAP_JSONB_V1)),
            ("10.0.0.2:7100", Some(0)),
        ]);
        let JsonbClusterGate::Incapable { members } = v else {
            panic!("expected a refusal naming the member, got {v:?}");
        };
        assert_eq!(members, vec!["10.0.0.2:7100".to_string()]);
    }

    /// A member whose capabilities could not be read is unknown, never capable: it may
    /// be an older build and there is no way to tell.
    #[test]
    fn an_unreadable_member_makes_the_whole_view_unknown() {
        let v = verdict_from_member_caps([
            ("10.0.0.1:7100", Some(ferrosa_net::handshake::CAP_JSONB_V1)),
            ("10.0.0.2:7100", None),
        ]);
        assert_eq!(v, JsonbClusterGate::Unknown);
    }

    /// A member set that is not established is unknown too, and unknown is not capable.
    #[test]
    fn an_unestablished_view_is_unknown() {
        assert_eq!(
            verdict_from_member_caps(Vec::<(String, Option<u32>)>::new()),
            JsonbClusterGate::Unknown
        );
    }

    /// Advertising some OTHER capability is not advertising jsonb: the test is the bit,
    /// not "the peer sent something".
    #[test]
    fn another_capability_does_not_stand_in_for_jsonb() {
        let v = verdict_from_member_caps([("10.0.0.9:7100", Some(1))]);
        assert!(matches!(v, JsonbClusterGate::Incapable { .. }), "{v:?}");
    }

    /// The glue publishes what it decides, into the very cell the DDL gate reads. This
    /// build understands jsonb, so a view containing only this node is capable.
    #[tokio::test]
    async fn the_published_verdict_reaches_the_schema_gate_cell() {
        let schema = crate::controller::tests::test_schema();
        let me = uuid::Uuid::new_v4();
        let v = publish_jsonb_cluster_gate(&schema, None, &[], me).await;
        assert_eq!(v, JsonbClusterGate::Capable);
        assert_eq!(
            schema.jsonb_cluster_gate(),
            JsonbClusterGate::Capable,
            "the decision must reach the cell the gate reads"
        );
    }

    /// A member this node has no live connection to is unknown, so nothing is admitted
    /// against a view that cannot be read — and the cell says so.
    #[tokio::test]
    async fn an_unreadable_member_publishes_unknown_to_the_cell() {
        let schema = crate::controller::tests::test_schema();
        let me = uuid::Uuid::new_v4();
        let other = uuid::Uuid::new_v4();
        let v =
            publish_jsonb_cluster_gate(&schema, None, &[("10.0.0.2:7100".to_string(), other)], me)
                .await;
        assert_eq!(v, JsonbClusterGate::Unknown);
        assert_eq!(schema.jsonb_cluster_gate(), JsonbClusterGate::Unknown);
    }

    /// The transition half's rule, evaluated with the view it still uses
    /// (`TRANSITION_VIEW` = unknown): leaving standalone while a table holds jsonb stays
    /// refused. Wiring the controller's real view in here is a separate slice.
    #[test]
    fn jsonb_ddl_permitted_only_standalone() {
        for mode in [
            DeploymentMode::Standalone,
            DeploymentMode::Pair,
            DeploymentMode::Forming,
            DeploymentMode::Cluster,
            DeploymentMode::DegradedPair,
            DeploymentMode::DegradedCluster,
        ] {
            match jsonb_ddl_permitted(mode, &TRANSITION_VIEW) {
                Ok(()) => assert_eq!(mode, DeploymentMode::Standalone),
                Err(refused) => {
                    assert_ne!(mode, DeploymentMode::Standalone);
                    assert_eq!(refused.mode, mode);
                    assert!(refused.to_string().contains("D15a"), "{refused}");
                }
            }
        }
    }
}
