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
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut activity = self.activity.lock();
                if activity.owner_ended && activity.active == 0 {
                    activity.closed = true;
                    return match &activity.failure {
                        Some(reason) => Err(reason.clone()),
                        None => Ok(activity.applied),
                    };
                }
            }
            notified.await;
        }
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
        let registration = Arc::new(PhysicalRaftRegistration {
            group,
            lifecycle,
            committed: Mutex::new(None),
            raft: Mutex::new(None),
            registry: Arc::downgrade(self),
        });
        registry.groups.push(Arc::clone(&registration));
        Ok(registration)
    }

    /// Cancellation retains the sealed registry, every owner, and every Raft
    /// handle. A later caller joins the same retirement rather than losing work.
    pub(crate) async fn stop_and_drain(
        &self,
        published: &HashMap<RaftGroupId, Arc<FerrosRaft>>,
    ) -> Result<(), String> {
        let groups = {
            let mut registry = self.registry.lock();
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
        if state.sealed {
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
        if state.sealed {
            return false;
        }
        publish();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_waiter_retains_running_physical_work() {
        let (lifecycle, owner) = PhysicalApplyLifecycle::new_owner();
        let lease = lifecycle.admit().unwrap();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let job = ferrosa_net::task_pool::TaskPool::current("physical-drain-test").spawn_blocking(
            move || {
                let _lease = lease.into_job();
                entered.send(()).unwrap();
                released
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            },
        );
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
            let job = ferrosa_net::task_pool::TaskPool::current("physical-queued-drain-test")
                .spawn_blocking(move || {
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
        let job = ferrosa_net::task_pool::TaskPool::current("physical-panic-drain-test")
            .spawn_blocking(move || {
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
        let job = ferrosa_net::task_pool::TaskPool::current("physical-shared-panic-test")
            .spawn_blocking(move || {
                let _job = job_lease;
                panic!("owned job panics while async share remains");
            });
        assert!(job.await.unwrap_err().is_panic());
        drop(caller_share); // This final shared drop is not unwinding.
        drop(owner);
        assert!(lifecycle.drain().await.is_err());
    }
}
