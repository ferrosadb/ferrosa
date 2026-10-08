//! Phase 5 — ReplaySchema.
//!
//! Pre-condition: leader elected (WaitLeader post-condition holds).
//! Post-condition: every node in the cluster reports the same
//! `state.schema_version` as the leader.  Schema replay is a Raft
//! `client_write` driven from the leader; followers converge through
//! AppendEntries.
//!
//! The check is on `schema_version: Uuid` rather than the full schema
//! payload because Uuid equality is a sufficient witness once Raft has
//! committed: every entry replayed under the same Uuid will produce
//! the same applied state.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;
use std::time::Duration;

use ferrosa_net::peer::PeerManager;
use uuid::Uuid;

use super::phase::{BootstrapError, BootstrapPhase};
use crate::error::ClusterError;
use crate::pair::ddl::DdlOperation;
use crate::raft::FerrosRaft;

/// Bounded retry of the local-schema replay (t_0b4e9b99).
///
/// The replay runs right after an election. When the leader that was just
/// elected steps down (a rolling restart reaches it), every forward fails
/// with "lane is reconnecting" or "leader unknown" until the next election.
/// Each failed operation is re-sent to whichever node leads at the time,
/// at most `max_rounds` rounds in all, with exponential backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForwardRetryPolicy {
    pub max_rounds: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for ForwardRetryPolicy {
    fn default() -> Self {
        Self {
            max_rounds: 8,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(8),
        }
    }
}

/// What to do after a replay round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardRoundOutcome {
    /// Nothing is pending: every operation was applied.
    Converged,
    /// Re-send the retryable pending operations after this delay.
    RetryAfter(Duration),
    /// Stop: rounds are exhausted, or nothing pending could succeed on retry.
    GiveUp,
}

/// Decide what follows replay round `round` (1-based) that left `pending`
/// operations unapplied, `retryable` of which failed transiently.
pub fn after_round(
    policy: &ForwardRetryPolicy,
    round: u32,
    pending: usize,
    retryable: usize,
) -> ForwardRoundOutcome {
    debug_assert!(round >= 1, "rounds are 1-based");
    debug_assert!(retryable <= pending, "retryable is a subset of pending");
    if pending == 0 {
        return ForwardRoundOutcome::Converged;
    }
    if retryable == 0 || round >= policy.max_rounds {
        return ForwardRoundOutcome::GiveUp;
    }
    let factor = 1u32 << (round - 1).min(16);
    ForwardRoundOutcome::RetryAfter(
        policy
            .base_delay
            .saturating_mul(factor)
            .min(policy.max_delay),
    )
}

/// Whether a replay failure can succeed when re-sent once a leader is
/// elected and its lane is up. Encoding failures and refusals cannot.
pub fn is_retryable_forward_error(error: &ClusterError) -> bool {
    match error {
        ClusterError::Net(_)
        | ClusterError::NotLeader { .. }
        | ClusterError::Overloaded(_)
        | ClusterError::RaftError(_)
        | ClusterError::Internal(_)
        | ClusterError::Unavailable { .. }
        | ClusterError::WriteTimeout { .. }
        | ClusterError::ReadTimeout { .. }
        | ClusterError::ReplicationFailed(_)
        | ClusterError::CatchUpRequired => true,
        ClusterError::Storage(_)
        | ClusterError::PairWriteUnavailable
        | ClusterError::NotPrimary
        | ClusterError::ModeTransitionRejected(_)
        | ClusterError::NotApproved(_)
        | ClusterError::NotImplemented { .. }
        | ClusterError::DataMovementUnverified(_) => false,
    }
}

/// One local schema object to replay, with a name for the logs.
#[derive(Clone, Debug)]
pub struct LabeledDdl {
    pub label: String,
    pub op: DdlOperation,
}

/// Where a replay round sends its operations.
pub struct ReplayTarget<'a> {
    pub raft: &'a FerrosRaft,
    pub peer_manager: &'a PeerManager,
    pub node_map: &'a RwLock<HashMap<u64, Uuid>>,
    pub local_node_id: u64,
}

/// Operations left unapplied after a round, split by whether a retry can help.
#[derive(Debug, Default)]
pub struct RoundFailures {
    pub retryable: Vec<(LabeledDdl, ClusterError)>,
    pub permanent: Vec<(LabeledDdl, ClusterError)>,
}

impl RoundFailures {
    fn pending(&self) -> usize {
        self.retryable.len() + self.permanent.len()
    }

    fn first_error(&self) -> String {
        self.retryable
            .first()
            .or(self.permanent.first())
            .map(|(ddl, e)| format!("{}: {e}", ddl.label))
            .unwrap_or_default()
    }
}

/// Send each operation to the node that leads now: through Raft directly
/// when that is this node, else forwarded. No per-operation logging; the
/// caller reports the round.
pub async fn replay_round(target: &ReplayTarget<'_>, ops: Vec<LabeledDdl>) -> RoundFailures {
    let mut failures = RoundFailures::default();
    let leader = target.raft.current_leader().await;
    let leader_uuid = leader.and_then(|lid| {
        target
            .node_map
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&lid)
            .copied()
    });
    for ddl in ops {
        let result = match (leader, leader_uuid) {
            (None, _) => Err(ClusterError::NotLeader { leader_id: None }),
            (Some(lid), _) if lid == target.local_node_id => {
                crate::ddl_path::execute_via_raft(target.raft, ddl.op.clone())
                    .await
                    .map(|_| ())
            }
            (Some(lid), None) => Err(ClusterError::Internal(format!(
                "leader node_id {lid} missing from node_map"
            ))),
            // Bootstrap schema hand-off, not a client DDL: no
            // read-your-writes wait needed.
            (Some(_), Some(uuid)) => crate::ddl_path::forward_ddl_to_leader(
                None,
                target.peer_manager,
                uuid,
                ddl.op.clone(),
            )
            .await
            .map(|_| ()),
        };
        if let Err(e) = result {
            if is_retryable_forward_error(&e) {
                failures.retryable.push((ddl, e));
            } else {
                failures.permanent.push((ddl, e));
            }
        }
    }
    failures
}

/// Report a finished replay round on its edges and decide the next step.
/// Returns the operations to re-send and the delay, or `None` when done.
pub fn report_round(
    policy: &ForwardRetryPolicy,
    round: u32,
    total: usize,
    failures: RoundFailures,
) -> Option<(Vec<LabeledDdl>, Duration)> {
    match after_round(policy, round, failures.pending(), failures.retryable.len()) {
        ForwardRoundOutcome::Converged => {
            if round > 1 {
                tracing::info!(
                    total,
                    rounds = round,
                    "schema replay: recovered; every local schema object reached the leader"
                );
            } else {
                tracing::info!(
                    total,
                    "schema replay: every local schema object reached the leader"
                );
            }
            None
        }
        ForwardRoundOutcome::RetryAfter(delay) => {
            if round == 1 {
                tracing::warn!(
                    total,
                    pending = failures.pending(),
                    first_error = %failures.first_error(),
                    retry_in_ms = delay.as_millis() as u64,
                    max_rounds = policy.max_rounds,
                    "schema replay: local schema objects did not reach the leader; \
                     re-sending once a leader is reachable"
                );
            }
            for (ddl, e) in &failures.permanent {
                tracing::error!(object = %ddl.label, error = %e,
                    "schema replay: a local schema object cannot be replayed; not retrying it");
            }
            Some((
                failures.retryable.into_iter().map(|(ddl, _)| ddl).collect(),
                delay,
            ))
        }
        ForwardRoundOutcome::GiveUp => {
            tracing::error!(
                total,
                pending = failures.pending(),
                rounds = round,
                first_error = %failures.first_error(),
                "schema replay: gave up; the local schema objects listed next are not in the \
                 cluster schema unless another node already replicated them"
            );
            // Terminal edge: name each missing object once, so an operator
            // knows exactly what to re-create.
            for (ddl, e) in failures.retryable.iter().chain(&failures.permanent) {
                tracing::warn!(object = %ddl.label, error = %e,
                    "schema replay: local schema object never reached the leader");
            }
            None
        }
    }
}

/// Schema-replay snapshot.
#[derive(Clone, Debug)]
pub struct ReplaySchemaState {
    pub leader_node_id: u64,
    /// Schema version observed on each node (including the leader).
    pub node_schema_versions: BTreeMap<u64, Uuid>,
}

pub fn precondition(state: &ReplaySchemaState) -> Result<(), BootstrapError> {
    if !state
        .node_schema_versions
        .contains_key(&state.leader_node_id)
    {
        return Err(BootstrapError::phase(
            BootstrapPhase::ReplaySchema,
            format!(
                "leader node_id {} missing from schema-version map",
                state.leader_node_id
            ),
        ));
    }
    Ok(())
}

pub fn postcondition(state: &ReplaySchemaState) -> Result<(), BootstrapError> {
    let leader_version = state
        .node_schema_versions
        .get(&state.leader_node_id)
        .copied()
        .ok_or_else(|| {
            BootstrapError::phase(
                BootstrapPhase::ReplaySchema,
                "leader version vanished between pre and post",
            )
        })?;
    let mut divergent: Vec<(u64, Uuid)> = Vec::new();
    for (node, ver) in &state.node_schema_versions {
        if *ver != leader_version {
            divergent.push((*node, *ver));
        }
    }
    if divergent.is_empty() {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::ReplaySchema,
            format!(
                "{n} node(s) diverged from leader: {divergent:?}",
                n = divergent.len()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_postcondition_holds_when_all_nodes_match_leader() {
        let v = Uuid::from_bytes([7; 16]);
        let mut node_schema_versions = BTreeMap::new();
        node_schema_versions.insert(1, v);
        node_schema_versions.insert(2, v);
        node_schema_versions.insert(3, v);
        let state = ReplaySchemaState {
            leader_node_id: 1,
            node_schema_versions,
        };
        precondition(&state).expect("leader present → pre ok");
        postcondition(&state).expect("all match → post ok");
    }

    #[test]
    fn replay_postcondition_flags_divergent_follower() {
        let v_leader = Uuid::from_bytes([7; 16]);
        let v_follower = Uuid::from_bytes([8; 16]);
        let mut m = BTreeMap::new();
        m.insert(1, v_leader);
        m.insert(2, v_follower);
        let state = ReplaySchemaState {
            leader_node_id: 1,
            node_schema_versions: m,
        };
        let err = postcondition(&state).expect_err("divergence → err");
        assert_eq!(err.name(), BootstrapPhase::ReplaySchema);
    }

    fn policy() -> ForwardRetryPolicy {
        ForwardRetryPolicy {
            max_rounds: 4,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(2),
        }
    }

    /// t_0b4e9b99: "lane is reconnecting; retry later" must actually be
    /// retried. A round with transient failures is followed by another,
    /// with exponential backoff capped at `max_delay`.
    #[test]
    fn transient_failures_are_retried_with_capped_backoff() {
        let p = policy();
        assert_eq!(
            after_round(&p, 1, 304, 304),
            ForwardRoundOutcome::RetryAfter(Duration::from_millis(500))
        );
        assert_eq!(
            after_round(&p, 2, 10, 10),
            ForwardRoundOutcome::RetryAfter(Duration::from_secs(1))
        );
        assert_eq!(
            after_round(&p, 3, 10, 10),
            ForwardRoundOutcome::RetryAfter(Duration::from_secs(2))
        );
    }

    #[test]
    fn retries_are_bounded() {
        assert_eq!(after_round(&policy(), 4, 3, 3), ForwardRoundOutcome::GiveUp);
        let huge = ForwardRetryPolicy {
            max_rounds: u32::MAX,
            ..policy()
        };
        assert_eq!(
            after_round(&huge, 40, 1, 1),
            ForwardRoundOutcome::RetryAfter(Duration::from_secs(2)),
            "the backoff shift saturates instead of overflowing"
        );
    }

    #[test]
    fn nothing_pending_converges_and_nothing_retryable_gives_up() {
        assert_eq!(
            after_round(&policy(), 1, 0, 0),
            ForwardRoundOutcome::Converged
        );
        assert_eq!(
            after_round(&policy(), 3, 0, 0),
            ForwardRoundOutcome::Converged
        );
        assert_eq!(after_round(&policy(), 1, 2, 0), ForwardRoundOutcome::GiveUp);
    }

    /// The failures seen in the rolling restart are transient; an operation
    /// that cannot even be encoded is not.
    #[test]
    fn election_window_failures_are_retryable_and_encoding_failures_are_not() {
        let reconnecting = ClusterError::Net(ferrosa_net::error::NetError::Timeout(
            "lane is reconnecting; retry later".into(),
        ));
        assert!(is_retryable_forward_error(&reconnecting));
        assert!(is_retryable_forward_error(&ClusterError::NotLeader {
            leader_id: None
        }));
        assert!(is_retryable_forward_error(&ClusterError::NotLeader {
            leader_id: Some(2)
        }));
        assert!(!is_retryable_forward_error(&ClusterError::Storage(
            ferrosa_common::Error::InvalidFormat("bad op".into())
        )));
    }

    /// Edges, not events: a round that converges returns nothing to resend;
    /// a failed first round hands back exactly the retryable operations.
    #[test]
    fn report_round_resends_only_the_retryable_operations() {
        let ddl = |label: &str| LabeledDdl {
            label: label.into(),
            op: DdlOperation::DropTable {
                keyspace: "ks".into(),
                table: label.into(),
            },
        };
        let failures = RoundFailures {
            retryable: vec![(ddl("a"), ClusterError::NotLeader { leader_id: None })],
            permanent: vec![(
                ddl("b"),
                ClusterError::Storage(ferrosa_common::Error::InvalidFormat("x".into())),
            )],
        };
        let (resend, delay) = report_round(&policy(), 1, 2, failures).expect("retry");
        assert_eq!(delay, Duration::from_millis(500));
        let labels: Vec<_> = resend.iter().map(|d| d.label.as_str()).collect();
        assert_eq!(labels, vec!["a"]);
        assert!(report_round(&policy(), 2, 2, RoundFailures::default()).is_none());
    }

    #[test]
    fn precondition_requires_leader_in_map() {
        let state = ReplaySchemaState {
            leader_node_id: 1,
            node_schema_versions: BTreeMap::new(),
        };
        assert!(precondition(&state).is_err());
    }
}
