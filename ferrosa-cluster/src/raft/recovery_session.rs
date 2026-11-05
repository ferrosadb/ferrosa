//! A durable pre-effect guard shared by the live machine and snapshot builders.
use super::recovery_journal::{RecoveryJournal, RecoveryPhase, RecoveryRecord};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Mutex,
};

pub(crate) struct RecoverySession {
    path: PathBuf,
    group: String,
    state: Mutex<SessionState>,
}

#[derive(Default)]
struct SessionState {
    journal: Option<RecoveryJournal>,
    record: Option<RecoveryRecord>,
    reopened_unsafe: bool,
    highest_started: u64,
}

pub(crate) struct PreparedRaftRepair {
    pub(crate) retirement_scope: uuid::Uuid,
    pub(crate) required_snapshot_index: u64,
    pub(crate) committed_target: Option<super::recovery_journal::RecoveryLogPosition>,
}

pub(crate) struct ActiveRaftRepair {
    pub(crate) preparation: PreparedRaftRepair,
    pub(crate) epoch: u64,
    session: std::sync::Arc<RecoverySession>,
    _retired: std::sync::Arc<super::physical_apply::RetiredRaftGroup>,
}
impl ActiveRaftRepair {
    pub(crate) fn new(
        preparation: PreparedRaftRepair,
        epoch: u64,
        session: std::sync::Arc<RecoverySession>,
        retired: std::sync::Arc<super::physical_apply::RetiredRaftGroup>,
    ) -> Self {
        Self {
            preparation,
            epoch,
            session,
            _retired: retired,
        }
    }
    pub(crate) fn record(&self) -> io::Result<Option<RecoveryRecord>> {
        self.session.record()
    }
}
pub(crate) enum RepairPreparationState {
    Pending,
    Prepared(std::sync::Arc<ActiveRaftRepair>),
    Failed(String),
}

impl RecoverySession {
    pub(crate) fn for_snapshot(path: &Path) -> Self {
        let group_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let group = group_dir
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        Self {
            // Outside the directory that Raft reset rotates.
            path: {
                let mut path = group_dir.as_os_str().to_os_string();
                path.push(".recovery.json");
                PathBuf::from(path)
            },
            group,
            state: Mutex::new(SessionState::default()),
        }
    }

    fn initialize(&self, state: &mut SessionState) -> io::Result<()> {
        if state.journal.is_none() {
            if let Some(parent) = self.path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let (journal, record) = RecoveryJournal::open(self.path.clone(), self.group.clone())?;
            state.reopened_unsafe = record.as_ref().is_some_and(|r| {
                !matches!(
                    r.phase,
                    RecoveryPhase::SnapshotSynced | RecoveryPhase::Initialized
                )
            });
            state.highest_started = record.as_ref().map_or(0, |r| r.synced);
            state.record = record;
            state.journal = Some(journal);
        }
        Ok(())
    }

    pub(crate) fn startup_reason(&self) -> io::Result<Option<String>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("recovery session poisoned"))?;
        self.initialize(&mut state)?;
        Ok(state.reopened_unsafe.then(|| {
            state.record.as_ref().map_or_else(
                || "unfinished Raft session".into(),
                |r| format!("{} (failed index {:?})", r.reason, r.failed_at),
            )
        }))
    }

    pub(crate) fn before_apply(&self, index: u64, previous: u64) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("recovery session poisoned"))?;
        self.initialize(&mut state)?;
        if state.reopened_unsafe {
            return Err(io::Error::other(
                "Raft group requires verified online repair",
            ));
        }
        let journal = state
            .journal
            .as_ref()
            .ok_or_else(|| io::Error::other("missing recovery journal"))?;
        let record = match state.record.as_ref() {
            None => journal.arm_session(previous, 0)?,
            Some(record)
                if matches!(
                    record.phase,
                    RecoveryPhase::SnapshotSynced | RecoveryPhase::Initialized
                ) =>
            {
                let mut record = record.clone();
                record.phase = RecoveryPhase::SessionArmed;
                record.reason = "unclean Raft apply session".into();
                journal.persist(&record)?;
                record
            }
            Some(record) => record.clone(),
        };
        state.record = Some(record);
        state.highest_started = state.highest_started.max(index);
        Ok(())
    }

    pub(crate) fn execute<T, E>(
        &self,
        index: u64,
        previous: u64,
        on_failure: impl FnOnce(&str),
        transition: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<io::Error> + std::fmt::Display,
    {
        if let Err(error) = self.before_apply(index, previous) {
            on_failure(&error.to_string());
            return Err(error.into());
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(transition)) {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                let reason = error.to_string();
                on_failure(&reason);
                if let Err(journal_error) = self.failed(index, &reason) {
                    tracing::error!(%journal_error, "pre-effect guard retained after failure-reason update failed");
                }
                Err(error)
            }
            Err(payload) => {
                let reason = format!("owned physical apply group through {index} panicked");
                on_failure(&reason);
                if let Err(journal_error) = self.failed(index, &reason) {
                    tracing::error!(%journal_error, "pre-effect guard retained after panic-reason update failed");
                }
                std::panic::resume_unwind(payload)
            }
        }
    }

    pub(crate) fn prepare_from_retired(
        &self,
        retired: &super::physical_apply::RetiredRaftGroup,
    ) -> io::Result<PreparedRaftRepair> {
        if retired.group() != super::RaftGroupId::for_dc(&self.group) || retired.owners().is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "retirement scope does not match this Raft group",
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("recovery session poisoned"))?;
        self.initialize(&mut state)?;
        if !state.reopened_unsafe {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "healthy session cannot prepare quarantine repair",
            ));
        }
        let mut record = state
            .record
            .clone()
            .ok_or_else(|| io::Error::other("quarantine record is missing"))?;
        let mut committed = record.committed_target.clone();
        for owner in retired.owners() {
            if let Some(purged) = owner.purged {
                record.purge_point = record.purge_point.max(purged.index);
            }
            if let Some(target) = owner.committed {
                let candidate = super::recovery_journal::RecoveryLogPosition {
                    term: target.leader_id.term,
                    node_id: target.leader_id.node_id,
                    index: target.index,
                };
                if let Some(previous) = &committed {
                    if previous.index == candidate.index && previous != &candidate {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "conflicting committed log identity during repair",
                        ));
                    }
                }
                if committed
                    .as_ref()
                    .is_none_or(|old| old.index < candidate.index)
                {
                    committed = Some(candidate);
                }
            }
        }
        record.phase = RecoveryPhase::Repairing;
        record.retirement_scope = Some(retired.scope());
        record.committed_target = committed.clone();
        let required_snapshot_index = record.synced.max(record.purge_point);
        state
            .journal
            .as_ref()
            .ok_or_else(|| io::Error::other("missing recovery journal"))?
            .persist(&record)?;
        state.record = Some(record);
        Ok(PreparedRaftRepair {
            retirement_scope: retired.scope(),
            required_snapshot_index,
            committed_target: committed,
        })
    }

    pub(crate) fn record(&self) -> io::Result<Option<RecoveryRecord>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("recovery session poisoned"))?;
        self.initialize(&mut state)?;
        Ok(state.record.clone())
    }

    pub(crate) fn failed(&self, index: u64, reason: &str) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("recovery session poisoned"))?;
        self.initialize(&mut state)?;
        let mut record = state
            .record
            .clone()
            .ok_or_else(|| io::Error::other("failure before journal armed"))?;
        record.phase = RecoveryPhase::Quarantined;
        record.reason = reason.into();
        record.failed_at = Some(index);
        state.reopened_unsafe = true;
        state.record = Some(record.clone());
        // Failed reason persistence leaves the pre-existing armed guard intact.
        state
            .journal
            .as_ref()
            .ok_or_else(|| io::Error::other("missing recovery journal"))?
            .persist(&record)
    }

    pub(crate) fn snapshot_synced(&self, index: u64) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("recovery session poisoned"))?;
        self.initialize(&mut state)?;
        if state.reopened_unsafe || state.highest_started > index {
            return Ok(());
        }
        let Some(mut record) = state.record.clone() else {
            return Ok(());
        };
        record.phase = RecoveryPhase::SnapshotSynced;
        record.synced = record.synced.max(index);
        state
            .journal
            .as_ref()
            .ok_or_else(|| io::Error::other("missing recovery journal"))?
            .persist(&record)?;
        state.record = Some(record);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runtime_review_dotted_group_names_have_independent_sessions() {
        let directory = tempfile::tempdir().unwrap();
        let first = RecoverySession::for_snapshot(
            &directory.path().join("dc.alpha/state-machine.snapshot.bin"),
        );
        let second = RecoverySession::for_snapshot(
            &directory.path().join("dc.beta/state-machine.snapshot.bin"),
        );
        first.before_apply(1, 0).unwrap();
        second
            .before_apply(1, 0)
            .expect("different full group names must not share a lock or journal");
    }
    #[test]
    fn runtime_review_panicked_worker_persists_fault_without_waiter() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dc-a/state-machine.snapshot.bin");
        let session = RecoverySession::for_snapshot(&path);
        let closed = std::sync::atomic::AtomicBool::new(false);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            session.execute::<(), io::Error>(
                7,
                2,
                |_| closed.store(true, std::sync::atomic::Ordering::SeqCst),
                || panic!("injected physical worker panic"),
            )
        }));
        assert!(outcome.is_err(), "preserve the original worker panic");
        assert!(
            closed.load(std::sync::atomic::Ordering::SeqCst),
            "the owning job must close admission without depending on its waiter"
        );
        drop(session);
        let reopened = RecoverySession::for_snapshot(&path);
        let reason = reopened
            .startup_reason()
            .unwrap()
            .expect("panic survives cold restart");
        assert!(
            reason.contains("Some(7)"),
            "durable fault identifies the submitted group: {reason}"
        );
    }
}
