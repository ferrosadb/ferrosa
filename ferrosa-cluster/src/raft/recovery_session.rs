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
    reopened_unclean: bool,
    highest_started: u64,
}

pub(crate) struct PreparedRaftRepair {
    pub(crate) retirement_scope: uuid::Uuid,
    pub(crate) required_snapshot_index: u64,
    pub(crate) committed_target: Option<super::recovery_journal::RecoveryLogPosition>,
}

/// Request bound by the caller to a selected authenticated, healthy peer.
pub(crate) struct RepairSnapshotRequest {
    pub(crate) group: super::RaftGroupId,
    pub(crate) source: u64,
    pub(crate) epoch: u64,
}

/// Candidate identity and position, without owning any snapshot or log payload.
pub(crate) struct RepairSnapshotMetadata<'a> {
    pub(crate) group: super::RaftGroupId,
    pub(crate) source: u64,
    pub(crate) epoch: u64,
    pub(crate) retirement_scope: uuid::Uuid,
    pub(crate) last_log: Option<&'a super::recovery_journal::RecoveryLogPosition>,
}

impl PreparedRaftRepair {
    /// Check metadata against the request and current repair ownership.
    ///
    /// The caller selects the healthy peer; matching its identity here does not
    /// establish peer health or verify physical data. Eligibility changes no
    /// admission, journal, storage, or Raft state and does not authorize activation.
    pub(crate) fn snapshot_is_eligible(
        &self,
        request: &RepairSnapshotRequest,
        current_epoch: u64,
        current_scope: uuid::Uuid,
        candidate: &RepairSnapshotMetadata<'_>,
    ) -> bool {
        if candidate.group != request.group
            || candidate.source != request.source
            || candidate.epoch != request.epoch
            || current_epoch != request.epoch
            || candidate.retirement_scope != self.retirement_scope
            || current_scope != self.retirement_scope
        {
            return false;
        }
        let Some(last_log) = candidate.last_log else {
            return false;
        };
        last_log.index >= self.required_snapshot_index
            && self
                .committed_target
                .as_ref()
                .is_none_or(|committed| last_log.index > committed.index || last_log == committed)
    }
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
            state.reopened_unclean = record.as_ref().is_some_and(|r| !r.phase.is_clean());
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
        Ok(state.reopened_unclean.then(|| {
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
        if state.reopened_unclean {
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
            Some(record) if record.phase.is_clean() => {
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
            Ok(Ok(value)) => {
                if let Err(error) = self.settled(index) {
                    // The effects are complete but the guard stays armed, so the
                    // next restart quarantines. That is fail-closed, and loud.
                    let reason = format!("cannot settle apply group through {index}: {error}");
                    tracing::error!(%reason, "Raft recovery guard left armed");
                    on_failure(&reason);
                    return Err(error.into());
                }
                Ok(value)
            }
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

    /// Disarm the guard once the group through `index` completed every effect.
    ///
    /// Only the group that armed it may settle it, and a quarantined session
    /// never settles: its repair, not a later success, clears it.
    fn settled(&self, index: u64) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("recovery session poisoned"))?;
        self.initialize(&mut state)?;
        if state.reopened_unclean {
            return Err(io::Error::other("a quarantined Raft session cannot settle"));
        }
        let mut record = state
            .record
            .clone()
            .ok_or_else(|| io::Error::other("settle before journal armed"))?;
        if record.phase != RecoveryPhase::SessionArmed || state.highest_started != index {
            return Err(io::Error::other(format!(
                "settle of group {index} does not match the armed group {} ({:?})",
                state.highest_started, record.phase
            )));
        }
        record.phase = RecoveryPhase::Settled;
        record.reason = "every armed apply group completed".into();
        state
            .journal
            .as_ref()
            .ok_or_else(|| io::Error::other("missing recovery journal"))?
            .persist(&record)?;
        state.record = Some(record);
        Ok(())
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
        if !state.reopened_unclean {
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
        state.reopened_unclean = true;
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
        if state.reopened_unclean || state.highest_started > index {
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
    fn prepared_quarantined_repair_checks_snapshot_eligibility_without_activation() {
        use super::{RepairSnapshotMetadata, RepairSnapshotRequest};
        use crate::raft::{quarantine::RecoveryAdmission, RaftGroupId};

        // API-absence RED: these production metadata types and the read-only
        // predicate do not exist yet. Eligibility cannot verify physical rows
        // or authorize activation, journal cleanup, or clearing quarantine.
        use crate::raft::recovery_journal::RecoveryLogPosition;

        struct Case {
            name: &'static str,
            group: RaftGroupId,
            source: u64,
            epoch_delta: u64,
            scope: uuid::Uuid,
            current_scope: uuid::Uuid,
            fresh_fault: bool,
            watermark: u64,
            committed: Option<RecoveryLogPosition>,
            last_log: Option<RecoveryLogPosition>,
            eligible: bool,
        }

        let group = RaftGroupId::for_dc("dc-a");
        let scope = uuid::Uuid::from_u128(1);
        let other_scope = uuid::Uuid::from_u128(2);
        let position = |term, node_id, index| RecoveryLogPosition {
            term,
            node_id,
            index,
        };
        let matching = || Case {
            name: "matching committed identity",
            group,
            source: 11,
            epoch_delta: 0,
            scope,
            current_scope: scope,
            fresh_fault: false,
            watermark: 40,
            committed: Some(position(7, 11, 50)),
            last_log: Some(position(7, 11, 50)),
            eligible: true,
        };
        let cases = [
            matching(),
            Case {
                name: "later snapshot",
                last_log: Some(position(8, 12, 51)),
                ..matching()
            },
            Case {
                name: "wrong group",
                group: RaftGroupId::for_dc("dc-b"),
                eligible: false,
                ..matching()
            },
            Case {
                name: "wrong source",
                source: 12,
                eligible: false,
                ..matching()
            },
            Case {
                name: "wrong candidate epoch",
                epoch_delta: 1,
                eligible: false,
                ..matching()
            },
            Case {
                name: "fresh fault invalidates prepared epoch",
                fresh_fault: true,
                eligible: false,
                ..matching()
            },
            Case {
                name: "wrong candidate retirement scope",
                scope: other_scope,
                eligible: false,
                ..matching()
            },
            Case {
                name: "lost current retirement ownership",
                current_scope: other_scope,
                eligible: false,
                ..matching()
            },
            Case {
                name: "missing snapshot position",
                last_log: None,
                eligible: false,
                ..matching()
            },
            Case {
                name: "below committed target but above watermark",
                last_log: Some(position(7, 11, 49)),
                eligible: false,
                ..matching()
            },
            Case {
                name: "equal committed index with different term",
                last_log: Some(position(8, 11, 50)),
                eligible: false,
                ..matching()
            },
            Case {
                name: "equal committed index with different node",
                last_log: Some(position(7, 12, 50)),
                eligible: false,
                ..matching()
            },
            Case {
                name: "below synced or purge watermark despite committed coverage",
                watermark: 51,
                eligible: false,
                ..matching()
            },
            Case {
                name: "exactly at higher watermark",
                watermark: 51,
                last_log: Some(position(8, 12, 51)),
                ..matching()
            },
            Case {
                name: "no committed target still accepts exact watermark",
                committed: None,
                last_log: Some(position(6, 11, 40)),
                ..matching()
            },
            Case {
                name: "no committed target still rejects below watermark",
                committed: None,
                last_log: Some(position(6, 11, 39)),
                eligible: false,
                ..matching()
            },
        ];

        for case in cases {
            // Given a prepared quarantined repair and a request bound to the
            // selected authenticated healthy peer (not the payload's claim).
            let admission = RecoveryAdmission::new();
            let epoch = admission.quarantine("dc-a", "interrupted physical effects");
            let request = RepairSnapshotRequest {
                group,
                source: 11,
                epoch,
            };
            let prepared = PreparedRaftRepair {
                retirement_scope: scope,
                required_snapshot_index: case.watermark,
                committed_target: case.committed,
            };
            if case.fresh_fault {
                admission.quarantine("dc-a", "new fault during repair");
            }
            let current_epoch = admission.epoch();
            let reason = admission.reason();
            let candidate = RepairSnapshotMetadata {
                group: case.group,
                source: case.source,
                epoch: epoch + case.epoch_delta,
                retirement_scope: case.scope,
                last_log: case.last_log.as_ref(),
            };

            // When checking eligibility, borrow only immutable metadata;
            // there is no snapshot/log payload to clone or storage to adopt.
            assert_eq!(
                prepared.snapshot_is_eligible(
                    &request,
                    current_epoch,
                    case.current_scope,
                    &candidate,
                ),
                case.eligible,
                "{}",
                case.name
            );

            // Then both accepted and rejected metadata leave quarantine closed.
            assert!(
                admission.admits().is_err(),
                "{} cleared quarantine",
                case.name
            );
            assert_eq!(admission.epoch(), current_epoch, "{}", case.name);
            assert_eq!(admission.reason(), reason, "{}", case.name);
            assert_eq!(prepared.retirement_scope, scope, "{}", case.name);
            assert_eq!(
                prepared.required_snapshot_index, case.watermark,
                "{}",
                case.name
            );
        }
    }

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
    fn a_completed_group_settles_and_an_interrupted_one_does_not() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dc-a/state-machine.snapshot.bin");
        let session = RecoverySession::for_snapshot(&path);
        session
            .execute::<(), io::Error>(3, 2, |_| {}, || Ok(()))
            .unwrap();
        drop(session);
        let reopened = RecoverySession::for_snapshot(&path);
        assert_eq!(
            reopened.startup_reason().unwrap(),
            None,
            "a group whose effects all completed must not quarantine a restart"
        );

        // Arm the next group without completing it: the crash window.
        reopened.before_apply(4, 3).unwrap();
        drop(reopened);
        let crashed = RecoverySession::for_snapshot(&path);
        assert!(
            crashed.startup_reason().unwrap().is_some(),
            "a group interrupted mid-effect must still quarantine"
        );
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
