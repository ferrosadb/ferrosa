//! The repair installer: how a quarantined Raft group leaves quarantine.
//!
//! `transition_to_cluster` prepares a repair when the recovery journal says an
//! apply group was interrupted mid-effect. Preparation drains the group, fences
//! its physical workers and records `Repairing` with the committed target and
//! purge point (`RepairPreparationState::Prepared`). Nothing runs Raft for the
//! group while it is quarantined, so this module does the rest:
//!
//! 1. [`ModeController::spawn_raft_repair_driver`] asks each same-DC peer that
//!    advertises `CAP_RAFT_REPAIR_SNAPSHOT` for a committed snapshot through at
//!    least [`ActiveRaftRepair::minimum_index`], retrying with capped backoff
//!    and logging only the edges of a failing streak.
//! 2. [`ModeController::complete_raft_repair`] verifies the cut, installs it
//!    authoritatively (refusing one that would orphan local rows), fsyncs the
//!    snapshot file, purges the local log through it, records
//!    `SnapshotSynced` in the journal, lifts the fence, releases the admission
//!    fault at the prepared epoch and re-drives the cluster transition, which
//!    now starts Raft from the installed snapshot.
//!
//! The order is the crash story. A crash before the journal write leaves the
//! journal `Repairing` and the next start prepares again from the larger purge
//! point; a crash after it starts healthy from the fsynced snapshot. The
//! design source is `specs/lean4/Ferrosa/Raft/Quarantine.lean`.

use std::sync::Arc;
use std::time::Duration;

use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, SnapshotMeta};

use super::ModeController;
use crate::raft::recovery_session::{ActiveRaftRepair, RepairPreparationState};
use crate::raft::repair_snapshot::{RepairSnapshotRequest, RepairSnapshotResponse};
use crate::raft::state_machine::{FerrosStateMachine, RepairInstallError};
use crate::raft::RaftGroupId;

/// The admission fault an interrupted apply raises (`recover_from_persisted_snapshot`).
const LOCAL_RAFT_FAULT: &str = "local-raft";
/// How long one peer has to answer, including building a fresh snapshot.
const PEER_ANSWER_TIMEOUT: Duration = Duration::from_secs(60);
const FIRST_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

impl ModeController {
    fn local_raft_group(&self) -> RaftGroupId {
        RaftGroupId::for_dc(&self.config.data_center)
    }

    /// The repair currently prepared for this node's group, if any.
    fn prepared_raft_repair(&self) -> Option<Arc<ActiveRaftRepair>> {
        match self
            .repair_preparations
            .lock()
            .get(&self.local_raft_group())
        {
            Some(RepairPreparationState::Prepared(active)) => Some(Arc::clone(active)),
            _ => None,
        }
    }

    /// Start the task that fetches and installs a peer snapshot for the
    /// prepared repair. It holds only a weak handle between attempts, so a
    /// dropped controller ends it, as do shutdown and the preparation leaving
    /// `Prepared`.
    pub(super) fn spawn_raft_repair_driver(&self) {
        let Some(weak) = self.self_weak.get().cloned() else {
            tracing::error!(
                "Raft repair is prepared but this controller has no self handle; \
                 the group stays quarantined"
            );
            return;
        };
        let cancel = self.cancel.clone();
        ferrosa_net::task_pool::TaskPool::current("raft-repair-install").spawn(async move {
            let mut backoff = FIRST_BACKOFF;
            let mut failing_since: Option<String> = None;
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(backoff) => {}
                }
                let Some(controller) = weak.upgrade() else {
                    return;
                };
                match controller.attempt_raft_repair().await {
                    Ok(()) => {
                        if failing_since.is_some() {
                            tracing::info!("Raft repair recovered from its failing streak");
                        }
                        return;
                    }
                    Err(error) if error.is_retryable() => {
                        if failing_since.is_none() {
                            tracing::warn!(%error, "Raft repair attempt failed; retrying with backoff");
                            failing_since = Some(error.to_string());
                        }
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                    Err(error) => {
                        tracing::error!(%error, "Raft repair cannot proceed; the group stays quarantined");
                        controller.fail_raft_repair(error.to_string());
                        return;
                    }
                }
            }
        });
    }

    fn fail_raft_repair(&self, reason: String) {
        let group = self.local_raft_group();
        let mut preparations = self.repair_preparations.lock();
        if matches!(
            preparations.get(&group),
            Some(RepairPreparationState::Prepared(_))
        ) {
            preparations.insert(group, RepairPreparationState::Failed(reason));
        }
        drop(preparations);
        self.repair_preparation_changed.notify_waiters();
    }

    /// One pass over the capable same-DC peers. `Ok` when the repair
    /// completed or is no longer prepared.
    async fn attempt_raft_repair(&self) -> Result<(), RepairInstallError> {
        let Some(active) = self.prepared_raft_repair() else {
            tracing::info!("Raft repair is no longer prepared; its driver exits");
            return Ok(());
        };
        let minimum = active.minimum_index();
        drop(active);
        let Some(peer_manager) = self.peer_manager.load().as_ref().clone() else {
            return Err(RepairInstallError::Insufficient(
                "no peer manager is set to reach a repair source".into(),
            ));
        };
        let connected = self.connected_peers.load_full();
        let dcs = self.peer_dcs_snapshot();
        let (same_dc, _) =
            super::cluster::partition_peers_by_dc(&connected, &dcs, &self.config.data_center);
        let request = RepairSnapshotRequest {
            group: self.config.data_center.clone(),
            minimum_index: minimum,
        }
        .encode()
        .map_err(RepairInstallError::Local)?;
        let mut refusals = Vec::new();
        for (peer, _) in same_dc {
            let capable = peer_manager
                .peer_capabilities(peer)
                .await
                .is_some_and(|caps| caps & ferrosa_net::handshake::CAP_RAFT_REPAIR_SNAPSHOT != 0);
            if !capable {
                refusals.push(format!("{peer}: does not serve repair snapshots"));
                continue;
            }
            let reply = peer_manager
                .send_with_timeout(
                    peer,
                    request.clone(),
                    crate::raft::snapshot_transport::snapshot_lane(),
                    PEER_ANSWER_TIMEOUT,
                )
                .await;
            let answer = match reply
                .map_err(|e| e.to_string())
                .and_then(RepairSnapshotResponse::decode)
            {
                Ok(answer) => answer,
                Err(error) => {
                    refusals.push(format!("{peer}: {error}"));
                    continue;
                }
            };
            match answer {
                RepairSnapshotResponse::Snapshot { meta, bytes } => {
                    let source = crate::raft::uuid_to_node_id(peer);
                    match self.complete_raft_repair(meta, bytes, source).await {
                        Ok(()) => return Ok(()),
                        Err(error) if error.is_retryable() => {
                            refusals.push(format!("{peer}: {error}"))
                        }
                        Err(error) => return Err(error),
                    }
                }
                RepairSnapshotResponse::Refused { reason } => {
                    refusals.push(format!("{peer}: {reason}"))
                }
            }
        }
        Err(RepairInstallError::Insufficient(format!(
            "no same-DC peer served a snapshot through {minimum}: [{}]",
            refusals.join("; ")
        )))
    }

    /// Install a verified peer cut and leave quarantine. See the module doc
    /// for the order and why it is crash-safe.
    pub(crate) async fn complete_raft_repair(
        &self,
        meta: SnapshotMeta<u64, BasicNode>,
        bytes: Vec<u8>,
        source: u64,
    ) -> Result<(), RepairInstallError> {
        let group = self.local_raft_group();
        let active = self
            .prepared_raft_repair()
            .ok_or_else(|| RepairInstallError::Local("no repair is prepared".into()))?;
        let installed = meta.last_log_id.ok_or_else(|| {
            RepairInstallError::Invalid("the snapshot has no log position".into())
        })?;
        active
            .covers(installed)
            .map_err(RepairInstallError::Insufficient)?;
        let exclusive = active.try_begin_install().ok_or_else(|| {
            RepairInstallError::Invalid("another repair completion is running".into())
        })?;
        let released = self
            .physical_raft_workers
            .release_retired_log(group, active.preparation.retirement_scope)
            .map_err(RepairInstallError::Local)?;
        tracing::debug!(released, "retired Raft owners released the group's log");
        let raft_dir = self.local_raft_dir();
        let schema = Arc::clone(&self.schema);
        let storage = Arc::clone(&self.storage);
        let installer = Arc::clone(&active);
        let mut log = ferrosa_net::task_pool::TaskPool::current("raft-repair-install")
            .spawn_blocking(move || {
                let log = super::cluster::open_raft_log_store_with_recovery(&raft_dir)
                    .map_err(|e| RepairInstallError::Local(format!("open the Raft log: {e}")))?;
                let mut machine = FerrosStateMachine::for_repair(
                    schema,
                    storage,
                    raft_dir.join("state-machine.snapshot.bin"),
                    installer.session(),
                );
                machine.set_durable_applied_handle(log.durable_applied_handle());
                machine.install_repair_snapshot(&meta, &bytes, installer.minimum_index())?;
                Ok::<_, RepairInstallError>(log)
            })
            .await
            .await
            .map_err(|e| RepairInstallError::Local(format!("install task: {e}")))??;
        log.purge(installed)
            .await
            .map_err(|e| RepairInstallError::Local(format!("purge the Raft log: {e}")))?;
        let purged = log
            .last_purged_log_id()
            .map_err(|e| RepairInstallError::Local(format!("read the purge point: {e}")))?;
        if purged != Some(installed) {
            return Err(RepairInstallError::Local(format!(
                "the Raft log purged to {purged:?}, not the installed snapshot {installed}"
            )));
        }
        drop(log);
        let scope = active.preparation.retirement_scope;
        active
            .session()
            .repair_installed(scope, installed, source)
            .map_err(|e| RepairInstallError::Local(format!("record the repair: {e}")))?;
        self.physical_raft_workers
            .finish_recovery(group, scope)
            .map_err(RepairInstallError::Local)?;
        self.recovery_admission
            .release(LOCAL_RAFT_FAULT, active.epoch)
            .map_err(|e| RepairInstallError::Local(format!("release admission: {e}")))?;
        {
            let mut preparations = self.repair_preparations.lock();
            if matches!(preparations.get(&group), Some(RepairPreparationState::Prepared(held)) if Arc::ptr_eq(held, &active))
            {
                preparations.remove(&group);
            }
        }
        drop(exclusive);
        drop(active);
        self.repair_preparation_changed.notify_waiters();
        tracing::info!(%installed, source, "Raft repair complete; restarting the group from the installed snapshot");
        let peers = self.connected_peers.load_full().as_ref().clone();
        let _guard = self.transition_guard.lock();
        if !matches!(self.mode(), super::DeploymentMode::Cluster) {
            self.transition_to_cluster(peers);
        }
        Ok(())
    }
}
