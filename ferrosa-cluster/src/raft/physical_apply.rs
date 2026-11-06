//! Own and drain Raft physical effects before replacing their authority.
//!
//! Correctness: closing the registry excludes new Raft owners, existing owners
//! drain their committed queue, and detached blocking jobs retain admission.
//! Last revised: 2026-11-05.
//! Last changed: Track owner lifetime separately from Core shutdown and job count.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use openraft::LogId;
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::log_store::CommittedLogReader;
use super::{FerrosRaft, RaftGroupId};

#[derive(Default)]
struct Activity {
    owner_ended: bool,
    closed: bool,
    active: usize,
    applied: Option<LogId<u64>>,
    failure: Option<String>,
}

/// One state-machine owner plus every physical job it has submitted.
pub(crate) struct PhysicalApplyLifecycle {
    activity: Mutex<Activity>,
    changed: Notify,
}

impl PhysicalApplyLifecycle {
    pub(crate) fn new_owner() -> (Arc<Self>, PhysicalApplyOwner) {
        let lifecycle = Arc::new(Self {
            activity: Mutex::default(),
            changed: Notify::new(),
        });
        let owner = PhysicalApplyOwner(Arc::clone(&lifecycle));
        (lifecycle, owner)
    }

    /// The same mutex orders final closure against submission. No check-before-
    /// spawn gap exists: the returned guard must move into the queued job.
    pub(crate) fn admit(self: &Arc<Self>) -> Result<Arc<PhysicalApplyLease>, String> {
        let mut activity = self.activity.lock();
        if activity.closed {
            return Err("Raft physical owner has retired".into());
        }
        activity.active += 1;
        Ok(Arc::new(PhysicalApplyLease(Arc::clone(self))))
    }

    pub(crate) fn applied(&self, log_id: Option<LogId<u64>>) {
        self.activity.lock().applied = log_id;
        self.changed.notify_waiters();
    }

    pub(crate) fn fail(&self, reason: impl ToString) {
        self.activity
            .lock()
            .failure
            .get_or_insert_with(|| reason.to_string());
        self.changed.notify_waiters();
    }

    /// A zero job count is insufficient: the SM loop may still own queued
    /// commands. Wait for that owner to end, then for detached jobs, before
    /// atomically denying any later snapshot-builder/job admission.
    pub(crate) async fn drain(&self) -> Result<Option<LogId<u64>>, String> {
        let drained = self.drain_for_recovery().await;
        match drained.failure {
            Some(reason) => Err(reason),
            None => Ok(drained.applied),
        }
    }

    async fn drain_for_recovery(&self) -> DrainedOwner {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut activity = self.activity.lock();
                if activity.owner_ended && activity.active == 0 {
                    activity.closed = true;
                    return DrainedOwner {
                        applied: activity.applied,
                        failure: activity.failure.clone(),
                    };
                }
            }
            notified.await;
        }
    }
}

struct DrainedOwner {
    applied: Option<LogId<u64>>,
    failure: Option<String>,
}

/// Observed positions of an owner that has actually stopped and drained.
#[derive(Clone, Debug)]
pub struct RetiredRaftOwner {
    pub applied: Option<LogId<u64>>,
    pub committed: Option<LogId<u64>>,
    pub purged: Option<LogId<u64>>,
    pub failure: Option<String>,
    pub shutdown_error: Option<String>,
}

/// Group-scoped quiescence evidence, never an adoption or admission receipt.
/// There is deliberately no clear/release operation without verified storage activation.
#[derive(Debug)]
pub struct RetiredRaftGroup {
    scope: uuid::Uuid,
    group: RaftGroupId,
    owners: Vec<RetiredRaftOwner>,
}

impl RetiredRaftGroup {
    pub fn scope(&self) -> uuid::Uuid {
        self.scope
    }
    pub fn group(&self) -> RaftGroupId {
        self.group
    }
    pub fn owners(&self) -> &[RetiredRaftOwner] {
        &self.owners
    }
}

/// Only the actual SM owns this token; staged copies/builders must not clone it.
pub(crate) struct PhysicalApplyOwner(Arc<PhysicalApplyLifecycle>);
impl Drop for PhysicalApplyOwner {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.fail("Raft state-machine owner panicked");
        }
        self.0.activity.lock().owner_ended = true;
        self.0.changed.notify_waiters();
    }
}

pub(crate) struct PhysicalApplyLease(Arc<PhysicalApplyLifecycle>);

impl PhysicalApplyLease {
    /// The per-job sentinel observes unwind even if another async share keeps
    /// the admission lease alive until after the blocking thread has unwound.
    pub(crate) fn into_job(self: Arc<Self>) -> PhysicalApplyJob {
        PhysicalApplyJob(self)
    }
}

pub(crate) struct PhysicalApplyJob(Arc<PhysicalApplyLease>);
impl Drop for PhysicalApplyJob {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0 .0.fail("Raft physical job panicked");
        }
    }
}

impl Drop for PhysicalApplyLease {
    fn drop(&mut self) {
        self.0.activity.lock().active -= 1;
        self.0.changed.notify_waiters();
    }
}

#[derive(Default)]
struct Registry {
    sealed: bool,
    groups: Vec<Arc<PhysicalRaftRegistration>>,
    recovering: HashMap<RaftGroupId, uuid::Uuid>,
}

/// Controller-side ownership includes starting groups, not only published ones.
#[derive(Default)]
pub(crate) struct PhysicalRaftWorkers {
    registry: Mutex<Registry>,
}

pub(crate) struct PhysicalRaftRegistration {
    group: RaftGroupId,
    lifecycle: Arc<PhysicalApplyLifecycle>,
    committed: Mutex<Option<CommittedLogReader>>,
    raft: Mutex<Option<Arc<FerrosRaft>>>,
    shutdown_error: Mutex<Option<String>>,
    registry: Weak<PhysicalRaftWorkers>,
}

impl PhysicalRaftWorkers {
    pub(crate) fn reserve(
        self: &Arc<Self>,
        group: RaftGroupId,
        lifecycle: Arc<PhysicalApplyLifecycle>,
    ) -> Result<Arc<PhysicalRaftRegistration>, String> {
        let mut registry = self.registry.lock();
        if registry.sealed {
            return Err("Raft worker registration is sealed for retirement".into());
        }
        if registry.recovering.contains_key(&group) {
            return Err("Raft group is fenced for verified replacement".into());
        }
        let registration = Arc::new(PhysicalRaftRegistration {
            group,
            lifecycle,
            committed: Mutex::new(None),
            raft: Mutex::new(None),
            shutdown_error: Mutex::new(None),
            registry: Arc::downgrade(self),
        });
        registry.groups.push(Arc::clone(&registration));
        Ok(registration)
    }

    pub(crate) async fn stop_group_for_recovery(
        &self,
        group: RaftGroupId,
        published: &HashMap<RaftGroupId, Arc<FerrosRaft>>,
    ) -> Result<Arc<RetiredRaftGroup>, String> {
        let (scope, owners) = {
            let mut registry = self.registry.lock();
            if registry.sealed {
                return Err("global Pair retirement has already sealed Raft".into());
            }
            let owners: Vec<_> = registry
                .groups
                .iter()
                .filter(|owner| owner.group == group)
                .cloned()
                .collect();
            if owners.is_empty() {
                return Err(format!(
                    "Raft group {group:?} has no tracked physical owner"
                ));
            }
            if let Some(raft) = published.get(&group) {
                if !owners.iter().any(|owner| {
                    owner
                        .raft
                        .lock()
                        .as_ref()
                        .is_some_and(|owned| Arc::ptr_eq(owned, raft))
                }) {
                    return Err(format!("Raft group {group:?} published an untracked Core"));
                }
            }
            let scope = *registry
                .recovering
                .entry(group)
                .or_insert_with(uuid::Uuid::new_v4);
            (scope, owners)
        };
        let mut shutdown_errors = Vec::with_capacity(owners.len());
        for owner in &owners {
            let raft = owner.raft.lock().clone();
            // A joined Core may report its original fatal error. Still wait for
            // the actual SM owner and detached jobs; preserve that error below.
            let error = match raft {
                Some(raft) => raft.shutdown().await.err().map(|error| error.to_string()),
                None => None,
            };
            if let Some(error) = error {
                owner.shutdown_error.lock().get_or_insert(error);
            }
            shutdown_errors.push(owner.shutdown_error.lock().clone());
        }
        let mut reports = Vec::with_capacity(owners.len());
        for (owner, shutdown_error) in owners.into_iter().zip(shutdown_errors) {
            let drained = owner.lifecycle.drain_for_recovery().await;
            let reader = owner
                .committed
                .lock()
                .clone()
                .ok_or_else(|| "retired owner has no bound log metadata".to_string())?;
            let (committed, purged) =
                ferrosa_net::task_pool::TaskPool::current("raft-group-retirement")
                    .spawn_blocking(move || {
                        Ok::<_, String>((
                            reader.read().map_err(|error| error.to_string())?,
                            reader.read_purged()?,
                        ))
                    })
                    .await
                    .await
                    .map_err(|error| error.to_string())??;
            reports.push(RetiredRaftOwner {
                applied: drained.applied,
                committed,
                purged,
                failure: drained.failure,
                shutdown_error,
            });
        }
        Ok(Arc::new(RetiredRaftGroup {
            scope,
            group,
            owners: reports,
        }))
    }

    /// Cancellation retains the sealed registry, every owner, and every Raft
    /// handle. A later caller joins the same retirement rather than losing work.
    pub(crate) async fn stop_and_drain(
        &self,
        published: &HashMap<RaftGroupId, Arc<FerrosRaft>>,
    ) -> Result<(), String> {
        let groups = {
            let mut registry = self.registry.lock();
            if !registry.recovering.is_empty() {
                return Err(
                    "Raft group replacement is still fenced; refusing Pair retirement".into(),
                );
            }
            registry.sealed = true;
            for (id, raft) in published {
                if !registry.groups.iter().any(|group| {
                    group.group == *id
                        && group
                            .raft
                            .lock()
                            .as_ref()
                            .is_some_and(|owned| Arc::ptr_eq(owned, raft))
                }) {
                    return Err(format!("Raft group {id:?} has no tracked physical owner"));
                }
            }
            registry.groups.clone()
        };
        for group in &groups {
            let raft = group.raft.lock().clone();
            if let Some(raft) = raft {
                raft.shutdown()
                    .await
                    .map_err(|error| format!("stop Raft group {:?}: {error}", group.group))?;
            }
        }
        for group in groups {
            let applied = group.lifecycle.drain().await?;
            let committed = group.committed.lock().clone();
            if let Some(reader) = committed {
                let target = ferrosa_net::task_pool::TaskPool::current("raft-physical-retirement")
                    .spawn_blocking(move || reader.read())
                    .await
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())?;
                if let Some(target) = target {
                    if applied.is_none_or(|applied| {
                        applied.index < target.index
                            || (applied.index == target.index && applied != target)
                    }) {
                        return Err(format!("Raft group {:?} physical retirement applied {applied:?}, below committed {target:?}", group.group));
                    }
                }
            }
        }
        Ok(())
    }
}

impl PhysicalRaftRegistration {
    /// Bind after any startup log reset so this cannot keep an obsolete sled
    /// database alive and prevent the replacement from obtaining its lock.
    pub(crate) fn bind_log(&self, reader: CommittedLogReader) {
        *self.committed.lock() = Some(reader);
    }

    /// A group whose asynchronous creation finishes after sealing must shut its
    /// Core down itself. Its owner remains registered until the SM loop ends.
    pub(crate) fn install_raft(&self, raft: Arc<FerrosRaft>) -> bool {
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let state = registry.registry.lock();
        if state.sealed || state.recovering.contains_key(&self.group) {
            return false;
        }
        *self.raft.lock() = Some(raft);
        true
    }

    /// The registry lock orders publication against sealing; a separate boolean
    /// check before publishing would let a stopped group reappear afterwards.
    pub(crate) fn publish(&self, publish: impl FnOnce()) -> bool {
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let state = registry.registry.lock();
        if state.sealed || state.recovering.contains_key(&self.group) {
            return false;
        }
        publish();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Start a pool job and hand back a handle that detaches on drop.
    ///
    /// `spawn_blocking` admits against the pool *before* it hands the closure to
    /// tokio, so the job is only queued once its future is polled. Production code
    /// polls and awaits it in place; the tests below drop the handle on purpose to
    /// prove the work survives its waiter, which needs the future driven by a task.
    fn start_job<T, F>(
        pool: &ferrosa_net::task_pool::TaskPool,
        work: F,
    ) -> tokio::task::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // Cloned so the returned task owns everything it needs; a `TaskPool` is a
        // name plus an optional runtime handle, so this is cheap.
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.spawn_blocking(work)
                .await
                .await
                .expect("blocking job handle was dropped before it was dispatched")
        })
    }

    use crate::raft::log_store::SledLogStore;
    use crate::raft::state_machine::FerrosStateMachine;
    use crate::raft::FerrosRaftConfig;
    use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
    use openraft::network::RPCOption;
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, Vote};

    #[derive(Debug)]
    struct NoPeers;
    impl std::fmt::Display for NoPeers {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("test node has no peers")
        }
    }
    impl std::error::Error for NoPeers {}

    struct NoPeersNetwork;

    impl RaftNetworkFactory<FerrosRaftConfig> for NoPeersNetwork {
        type Network = NoPeersNetwork;
        async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
            NoPeersNetwork
        }
    }

    impl RaftNetwork<FerrosRaftConfig> for NoPeersNetwork {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<FerrosRaftConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
            Err(RPCError::Unreachable(Unreachable::new(&NoPeers)))
        }

        async fn install_snapshot(
            &mut self,
            _rpc: InstallSnapshotRequest<FerrosRaftConfig>,
            _option: RPCOption,
        ) -> Result<
            InstallSnapshotResponse<u64>,
            RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
        > {
            Err(RPCError::Unreachable(Unreachable::new(&NoPeers)))
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<u64>,
            _option: RPCOption,
        ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
            Err(RPCError::Unreachable(Unreachable::new(&NoPeers)))
        }
    }

    struct OwnedGroup {
        raft: Arc<FerrosRaft>,
        lifecycle: Arc<PhysicalApplyLifecycle>,
        _directory: tempfile::TempDir,
    }

    async fn owned_group(workers: &Arc<PhysicalRaftWorkers>, name: &str, node: u64) -> OwnedGroup {
        let directory = tempfile::tempdir().unwrap();
        let log = SledLogStore::new(directory.path()).unwrap();
        let machine = FerrosStateMachine::new();
        let lifecycle = machine.physical_lifecycle();
        let registration = workers
            .reserve(RaftGroupId::for_dc(name), Arc::clone(&lifecycle))
            .unwrap();
        registration.bind_log(log.committed_reader());
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let raft = Arc::new(
            FerrosRaft::new(node, config, NoPeersNetwork, log, machine)
                .await
                .unwrap(),
        );
        assert!(registration.install_raft(Arc::clone(&raft)));
        OwnedGroup {
            raft,
            lifecycle,
            _directory: directory,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_recovery_retirement_does_not_stop_other_cores_or_seal_registry() {
        let workers = Arc::new(PhysicalRaftWorkers::default());
        let first = owned_group(&workers, "first", 1).await;
        let second = owned_group(&workers, "second", 2).await;
        let vote = || VoteRequest {
            vote: Vote::new(1, 99),
            last_log_id: None,
        };
        assert!(second.raft.vote(vote()).await.is_ok());
        let published = HashMap::from([
            (RaftGroupId::for_dc("first"), Arc::clone(&first.raft)),
            (RaftGroupId::for_dc("second"), Arc::clone(&second.raft)),
        ]);
        workers
            .stop_group_for_recovery(RaftGroupId::for_dc("first"), &published)
            .await
            .unwrap();
        assert!(
            workers.stop_and_drain(&published).await.is_err(),
            "Pair handoff must not bypass a group repair fence"
        );
        let second_still_serves = second.raft.vote(vote()).await.is_ok();
        let (third_lifecycle, _owner) = PhysicalApplyLifecycle::new_owner();
        let other_start_allowed = workers
            .reserve(RaftGroupId::for_dc("third"), third_lifecycle)
            .is_ok();
        second.raft.shutdown().await.unwrap();
        assert!(
            second_still_serves,
            "recovering one group stopped a different real Core"
        );
        assert!(
            other_start_allowed,
            "group recovery sealed the global Pair registry"
        );
        assert!(first.lifecycle.activity.lock().owner_ended);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_recovery_retirement_drains_but_does_not_erase_failed_owner() {
        let workers = Arc::new(PhysicalRaftWorkers::default());
        let failed = owned_group(&workers, "failed", 1).await;
        failed.lifecycle.fail("actual owner projection failure");
        let published = HashMap::from([(RaftGroupId::for_dc("failed"), Arc::clone(&failed.raft))]);
        let retired = workers
            .stop_group_for_recovery(RaftGroupId::for_dc("failed"), &published)
            .await;
        assert!(failed.lifecycle.activity.lock().owner_ended);
        assert_eq!(failed.lifecycle.activity.lock().active, 0);
        assert_eq!(
            failed.lifecycle.activity.lock().failure.as_deref(),
            Some("actual owner projection failure")
        );
        let retired = retired.expect("a fully drained failed owner needs a fenced repair token, not global permanent retirement refusal");
        assert_eq!(
            retired.owners()[0].failure.as_deref(),
            Some("actual owner projection failure")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_recovery_retirement_cancel_keeps_scope_until_real_job_finishes() {
        struct Release(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for Release {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        let workers = Arc::new(PhysicalRaftWorkers::default());
        let owner = owned_group(&workers, "cancel", 1).await;
        let group = RaftGroupId::for_dc("cancel");
        let published = HashMap::from([(group, Arc::clone(&owner.raft))]);
        let lease = owner.lifecycle.admit().unwrap();
        let path = owner._directory.path().join("owned-job-result");
        let result_path = path.clone();
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release_tx, released) = std::sync::mpsc::channel();
        let release = Release(Some(release_tx));
        let pool = ferrosa_net::task_pool::TaskPool::current("retirement-cancel-test");
        let job = start_job(&pool, move || {
            let _owned = lease.into_job();
            started.send(()).unwrap();
            released.recv().unwrap();
            std::fs::write(path, b"finished before replacement").unwrap();
        });
        entered.await.unwrap();
        let mut first = Box::pin(workers.stop_group_for_recovery(group, &published));
        assert!(futures::poll!(first.as_mut()).is_pending());
        let scope = workers.registry.lock().recovering[&group];
        drop(first);
        let (new_lifecycle, _new_owner) = PhysicalApplyLifecycle::new_owner();
        assert!(
            workers.reserve(group, new_lifecycle).is_err(),
            "cancelled waiter reopened group starts"
        );
        let mut retry = Box::pin(workers.stop_group_for_recovery(group, &published));
        assert!(
            futures::poll!(retry.as_mut()).is_pending(),
            "retirement ignored the held real job"
        );
        drop(release);
        job.await.unwrap();
        let retired = retry.await.unwrap();
        assert_eq!(
            retired.scope(),
            scope,
            "retry changed retirement generation"
        );
        assert_eq!(retired.group(), group);
        assert_eq!(
            std::fs::read(result_path).unwrap(),
            b"finished before replacement"
        );
        assert!(
            owner.lifecycle.admit().is_err(),
            "retired old views still admit physical jobs"
        );
        assert!(
            {
                let (replacement, _owner) = PhysicalApplyLifecycle::new_owner();
                workers.reserve(group, replacement).is_err()
            },
            "quiescence alone admitted an unverified replacement"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_waiter_retains_running_physical_work() {
        let (lifecycle, owner) = PhysicalApplyLifecycle::new_owner();
        let lease = lifecycle.admit().unwrap();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let pool = ferrosa_net::task_pool::TaskPool::current("physical-drain-test");
        let job = start_job(&pool, move || {
            let _lease = lease.into_job();
            entered.send(()).unwrap();
            released
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        });
        started.await.unwrap();
        drop(job); // Tokio's blocking job is detached, not cancelled.
        drop(owner);
        let mut drain = Box::pin(lifecycle.drain());
        assert!(futures::poll!(drain.as_mut()).is_pending());
        release.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), None);
        assert!(lifecycle.admit().is_err());
    }

    #[test]
    fn cancelled_waiter_retains_queued_physical_work() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (release, released) = std::sync::mpsc::channel();
            let (entered, started) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                released
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            });
            started.await.unwrap();
            let (lifecycle, owner) = PhysicalApplyLifecycle::new_owner();
            let lease = lifecycle.admit().unwrap();
            let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let written = Arc::clone(&ran);
            let pool = ferrosa_net::task_pool::TaskPool::current("physical-queued-drain-test");
            let job = start_job(&pool, move || {
                let _lease = lease.into_job();
                written.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            drop(job);
            drop(owner);
            let mut drain = Box::pin(lifecycle.drain());
            assert!(futures::poll!(drain.as_mut()).is_pending());
            assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
            release.send(()).unwrap();
            blocker.await.unwrap();
            drain.await.unwrap();
            assert!(ran.load(std::sync::atomic::Ordering::SeqCst));
            assert!(lifecycle.admit().is_err());
        });
    }

    #[tokio::test]
    async fn failed_physical_owner_cannot_confirm_retirement() {
        let (lifecycle, owner) = PhysicalApplyLifecycle::new_owner();
        lifecycle.fail("owned fixture persistence refusal");
        drop(owner);
        assert_eq!(
            lifecycle.drain().await.unwrap_err(),
            "owned fixture persistence refusal"
        );
        assert!(lifecycle.admit().is_err());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn orphaned_panicking_physical_job_refuses_retirement() {
        let (lifecycle, owner) = PhysicalApplyLifecycle::new_owner();
        let lease = lifecycle.admit().unwrap();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let pool = ferrosa_net::task_pool::TaskPool::current("physical-panic-drain-test");
        let job = start_job(&pool, move || {
            let _lease = lease.into_job();
            entered.send(()).unwrap();
            released
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            panic!("owned orphaned physical job panic");
        });
        started.await.unwrap();
        drop(job);
        drop(owner);
        release.send(()).unwrap();
        assert!(
            lifecycle.drain().await.is_err(),
            "an orphaned panic is not successful physical retirement"
        );
        assert!(lifecycle.admit().is_err());
    }
    #[tokio::test]
    async fn physical_job_panic_is_recorded_before_last_async_share_drops() {
        let (lifecycle, owner) = PhysicalApplyLifecycle::new_owner();
        let caller_share = lifecycle.admit().unwrap();
        let job_lease = Arc::clone(&caller_share).into_job();
        let pool = ferrosa_net::task_pool::TaskPool::current("physical-shared-panic-test");
        let job = start_job(&pool, move || {
            let _job = job_lease;
            panic!("owned job panics while async share remains");
        });
        assert!(job.await.unwrap_err().is_panic());
        drop(caller_share); // This final shared drop is not unwinding.
        drop(owner);
        assert!(lifecycle.drain().await.is_err());
    }
}
