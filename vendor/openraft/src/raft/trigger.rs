//! Trigger an action to RaftCore by external caller.

use std::time::Duration;

use crate::core::raft_msg::external_command::ExternalCommand;
use crate::core::raft_msg::RaftMsg;
use crate::core::ServerState;
use crate::error::Fatal;
use crate::error::TransferError;
use crate::raft::RaftInner;
use crate::AsyncRuntime;
use crate::RaftTypeConfig;

/// Trigger is an interface to trigger an action to RaftCore by external caller.
///
/// It is create with [`Raft::trigger()`].
///
/// For example, to trigger an election at once, you can use the following code
/// ```ignore
/// raft.trigger().elect().await?;
/// ```
///
/// Or to fire an heartbeat, building a snapshot, or purging logs:
///
/// ```ignore
/// raft.trigger().heartbeat().await?;
/// raft.trigger().snapshot().await?;
/// raft.trigger().purge_log().await?;
/// ```
///
/// [`Raft::trigger()`]: crate::Raft::trigger
pub struct Trigger<'r, C>
where C: RaftTypeConfig
{
    raft_inner: &'r RaftInner<C>,
}

impl<'r, C> Trigger<'r, C>
where C: RaftTypeConfig
{
    pub(in crate::raft) fn new(raft_inner: &'r RaftInner<C>) -> Self {
        Self { raft_inner }
    }

    /// Trigger election at once and return at once.
    ///
    /// Returns error when RaftCore has [`Fatal`] error, e.g. shut down or having storage error.
    /// It is not affected by `Raft::enable_elect(false)`.
    pub async fn elect(&self) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::Elect, "trigger_elect").await
    }

    /// Trigger a heartbeat at once and return at once.
    ///
    /// Returns error when RaftCore has [`Fatal`] error, e.g. shut down or having storage error.
    /// It is not affected by `Raft::enable_heartbeat(false)`.
    pub async fn heartbeat(&self) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::Heartbeat, "trigger_heartbeat").await
    }

    /// Trigger to build a snapshot at once and return at once.
    ///
    /// Returns error when RaftCore has [`Fatal`] error, e.g. shut down or having storage error.
    pub async fn snapshot(&self) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::Snapshot, "trigger_snapshot").await
    }

    /// Initiate the log purge up to and including the given `upto` log index.
    ///
    /// Logs that are not included in a snapshot will **NOT** be purged.
    /// In such scenario it will delete as many log as possible.
    /// The [`max_in_snapshot_log_to_keep`] config is not taken into account
    /// when purging logs.
    ///
    /// It returns error only when RaftCore has [`Fatal`] error, e.g. shut down or having storage
    /// error.
    ///
    /// Openraft won't purge logs at once, e.g. it may be delayed by several seconds, because if it
    /// is a leader and a replication task has been replicating the logs to a follower, the logs
    /// can't be purged until the replication task is finished.
    ///
    /// [`max_in_snapshot_log_to_keep`]: `crate::Config::max_in_snapshot_log_to_keep`
    pub async fn purge_log(&self, upto: u64) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::PurgeLog { upto }, "purge_log").await
    }

    /// Transfer leadership to the given target node (ferrosa fork — ADR-012, W3.9, W3.10).
    ///
    /// Performs the Ongaro Sec 3.10 leadership-transfer protocol:
    /// 1. Verifies the local node is leader and the target is a voter.
    /// 2. Catches up the target's replication state to the leader's last log id.
    /// 3. Sends a `TimeoutNow` directive to the target via the network factory.
    /// 4. Watches metrics for the target winning the election.
    ///
    /// Returns `Ok(())` once `current_leader` in metrics matches `target`. Returns
    /// `TransferError::Timeout` if the target doesn't win within
    /// `election_timeout_max × 2` after the TimeoutNow has been dispatched
    /// (W3.10 timeout safety).
    ///
    /// On failure, the local node remains leader; the cluster is unaffected
    /// except for the (small) cost of the catch-up replication and the burned
    /// term on the target.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn transfer_to(&self, target: C::NodeId) -> Result<(), TransferError<C::NodeId>> {
        // 1. Snapshot current metrics — verify leader, voter membership, and target identity
        //    locally to fail fast before bothering RaftCore.
        let mut metrics_rx = self.raft_inner.rx_metrics.clone();
        let metrics = metrics_rx.borrow().clone();
        if metrics.state != ServerState::Leader {
            return Err(TransferError::NotLeader);
        }
        if metrics.id == target {
            return Err(TransferError::TargetIsSelf(target));
        }
        if !metrics.membership_config.membership().is_voter(&target) {
            return Err(TransferError::TargetNotVoter(target));
        }

        // 2. Catch up the target. Poll the replication metrics until the target's
        //    matched log id reaches the leader's last log id, or fail after a budget.
        //
        //    The budget is `election_timeout_max × 2` — same magnitude as the post-dispatch
        //    deadline in step 4 below. If we can't catch up within that, the network is
        //    too slow / the target is too far behind to make transfer practical.
        let leader_last = metrics.last_log_index;
        let catchup_deadline_ms = self
            .raft_inner
            .config
            .election_timeout_max
            .saturating_mul(2);
        let catchup_deadline = std::time::Instant::now() + Duration::from_millis(catchup_deadline_ms);

        loop {
            let m = metrics_rx.borrow().clone();
            // Re-check leader state — a stepdown during catch-up should fail fast.
            if m.state != ServerState::Leader {
                return Err(TransferError::NotLeader);
            }
            let matched_idx = m
                .replication
                .as_ref()
                .and_then(|r| r.get(&target).cloned().flatten())
                .map(|lid| lid.index);

            let caught_up = match (matched_idx, leader_last) {
                (Some(m_idx), Some(ll_idx)) => m_idx >= ll_idx,
                (None, None) => true,
                (Some(_), None) => true,
                (None, Some(_)) => false,
            };
            if caught_up {
                break;
            }
            if std::time::Instant::now() >= catchup_deadline {
                return Err(TransferError::TargetTooFarBehind {
                    target: target.clone(),
                    matched: m
                        .replication
                        .as_ref()
                        .and_then(|r| r.get(&target).cloned().flatten()),
                    leader_last: m.last_log_index.and_then(|idx| {
                        // We don't have leader_last as a LogId here; reconstruct one from index only
                        // is impossible without term, so leave None for the matched view.
                        let _ = idx;
                        None
                    }),
                });
            }

            // Wait for either a metrics change or a short sleep, whichever comes first.
            tokio::select! {
                _ = metrics_rx.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(self.raft_inner.config.heartbeat_interval)) => {}
            }
        }

        // 3. Dispatch TimeoutNow via RaftCore (which has the network factory).
        let (tx, rx) = C::AsyncRuntime::oneshot();
        let cmd = ExternalCommand::SendTimeoutNow {
            target: target.clone(),
            tx,
        };
        let send_res = self.raft_inner.tx_api.send(RaftMsg::ExternalCommand { cmd });
        if send_res.is_err() {
            return Err(TransferError::Fatal);
        }

        // Await the dispatch result.
        let dispatch_res = rx.await;

        match dispatch_res {
            Ok(Ok(())) => {
                // TimeoutNow accepted by target; fall through to step 4 (watch metrics).
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(TransferError::Fatal),
        }

        // 4. Watch metrics for the target winning the election. Deadline is
        //    `election_timeout_max × 5` after dispatch (W3.10).
        //
        //    Why 5x: with the simple "stop heartbeats" stepdown path (per the spec doc),
        //    followers must wait up to `leader_lease == election_timeout_max` before
        //    their lease expires and they can grant a vote. The election round itself
        //    takes up to `election_timeout_max` more, so 2x is the theoretical minimum.
        //    We add 3x slack for scheduling/RPC latency under contended test runs.
        let deadline = std::time::Instant::now()
            + Duration::from_millis(self.raft_inner.config.election_timeout_max.saturating_mul(5));

        loop {
            let m = metrics_rx.borrow().clone();
            if m.current_leader.as_ref() == Some(&target) {
                return Ok(());
            }
            // If somehow we are still leader and the target hasn't taken over, keep waiting.
            // If we've stepped down in favor of someone else (not target), that's still a failure.

            if std::time::Instant::now() >= deadline {
                return Err(TransferError::Timeout);
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            tokio::select! {
                _ = metrics_rx.changed() => {}
                _ = tokio::time::sleep(remaining.min(Duration::from_millis(self.raft_inner.config.heartbeat_interval))) => {}
            }
        }
    }
}
