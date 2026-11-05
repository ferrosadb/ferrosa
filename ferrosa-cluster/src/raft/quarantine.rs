//! Shared recovery admission for inconsistent Raft state.
//! Correctness: publishers serialize; detection never waits for publisher I/O.
//! Last revised: 2026-11-05
//! Last changed: close all consumers through one epoch-bound recovery gate.

use parking_lot::Mutex;
use std::cell::RefCell;

thread_local! { static PUBLICATIONS: RefCell<std::collections::BTreeSet<usize>> = const { RefCell::new(std::collections::BTreeSet::new()) }; }
struct PublicationScope(usize);
impl Drop for PublicationScope {
    fn drop(&mut self) {
        PUBLICATIONS.with(|active| {
            active.borrow_mut().remove(&self.0);
        });
    }
}
use std::{collections::BTreeMap, fmt};

#[derive(Debug, Clone)]
pub struct RecoveryRefusal {
    pub epoch: u64,
    pub reason: String,
}
impl fmt::Display for RecoveryRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "recovery quarantine (epoch {}): {}",
            self.epoch, self.reason
        )
    }
}
impl std::error::Error for RecoveryRefusal {}

#[derive(Default)]
struct AdmissionState {
    epoch: u64,
    faults: BTreeMap<String, String>,
}

/// Every client/readiness/snapshot/vote/ack consumer shares this node gate.
/// Per-group recovery producers must not clear another group's fault.
#[derive(Default)]
pub struct RecoveryAdmission {
    transition: Mutex<AdmissionState>,
    publication: Mutex<()>,
}
impl RecoveryAdmission {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn epoch(&self) -> u64 {
        self.transition.lock().epoch
    }
    pub fn reason(&self) -> Option<String> {
        self.transition
            .lock()
            .faults
            .iter()
            .next()
            .map(|(group, reason)| format!("{group}: {reason}"))
    }
    pub fn admits(&self) -> Result<(), RecoveryRefusal> {
        let state = self.transition.lock();
        Self::check(&state, None)
    }
    pub fn admits_epoch(&self, epoch: u64) -> Result<(), RecoveryRefusal> {
        let state = self.transition.lock();
        Self::check(&state, Some(epoch))
    }
    /// Close in memory before any durable progress I/O or peer repair work.
    pub fn quarantine(&self, group: &str, reason: impl Into<String>) -> u64 {
        let reason = reason.into();
        let mut state = self.transition.lock();
        state.faults.insert(group.into(), reason);
        Self::advance(&mut state)
    }
    /// Serialize publishers, but never hold the fault-state lock across caller
    /// code. Detection and admission remain callable from the closure. Recursive
    /// publication on this gate is refused rather than waiting on itself.
    /// A changed epoch rejects the result; already-admitted I/O is not rolled back.
    pub fn with_admission_epoch<T>(
        &self,
        epoch: u64,
        publish: impl FnOnce() -> T,
    ) -> Result<T, RecoveryRefusal> {
        let key = self as *const Self as usize;
        if PUBLICATIONS.with(|active| active.borrow().contains(&key)) {
            return Err(RecoveryRefusal {
                epoch: self.epoch(),
                reason: "recursive recovery publication".into(),
            });
        }
        let _publisher = self.publication.lock();
        PUBLICATIONS.with(|active| {
            active.borrow_mut().insert(key);
        });
        let _scope = PublicationScope(key);
        self.admits_epoch(epoch)?;
        let result = publish();
        self.admits_epoch(epoch)?;
        Ok(result)
    }

    /// Once quarantined, wait for old admitted publication I/O before fixing the
    /// persisted purge floor and verifying a replacement checkpoint.
    pub fn drain_publications(&self) -> Result<(), RecoveryRefusal> {
        let key = self as *const Self as usize;
        if PUBLICATIONS.with(|active| active.borrow().contains(&key)) {
            return Err(RecoveryRefusal {
                epoch: self.epoch(),
                reason: "recursive recovery publication drain".into(),
            });
        }
        let _publisher = self.publication.lock();
        Ok(())
    }

    fn check(state: &AdmissionState, epoch: Option<u64>) -> Result<(), RecoveryRefusal> {
        let reason = state
            .faults
            .iter()
            .next()
            .map(|(group, why)| format!("{group}: {why}"));
        match reason {
            Some(reason) => Err(RecoveryRefusal {
                epoch: state.epoch,
                reason,
            }),
            None if epoch.is_some_and(|value| value != state.epoch) => Err(RecoveryRefusal {
                epoch: state.epoch,
                reason: "admission epoch changed".into(),
            }),
            None => Ok(()),
        }
    }
    fn advance(state: &mut AdmissionState) -> u64 {
        match state.epoch.checked_add(1) {
            Some(next) => state.epoch = next,
            None => {
                state
                    .faults
                    .insert("epoch".into(), "recovery epoch exhausted".into());
            }
        }
        state.epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn healthy_gate_opens_all_shared_checks() {
        let gate = RecoveryAdmission::new();
        let epoch = gate.epoch();
        assert!(gate.admits().is_ok());
        assert!(gate.admits_epoch(epoch).is_ok());
        assert_eq!(gate.with_admission_epoch(epoch, || 7).unwrap(), 7);
    }
    #[test]
    fn publication_holds_the_transition_lease() {
        let gate = RecoveryAdmission::new();
        gate.with_admission_epoch(gate.epoch(), || {
            assert!(gate.publication.try_lock().is_none());
        })
        .unwrap();
    }
    #[test]
    fn recursive_checks_and_fault_detection_do_not_wait_on_publication() {
        let gate = RecoveryAdmission::new();
        let epoch = gate.epoch();
        let result = gate.with_admission_epoch(epoch, || {
            assert!(gate.admits().is_ok());
            assert!(gate.with_admission_epoch(epoch, || ()).is_err());
            assert!(gate.drain_publications().is_err());
            gate.quarantine("dc1", "fault during publication");
            assert!(gate.admits().is_err());
        });
        assert!(result.is_err());
        assert!(gate.reason().unwrap().contains("fault during publication"));
    }
    #[test]
    fn detection_closes_publication_and_retains_group_reason() {
        let gate = RecoveryAdmission::new();
        let epoch = gate.epoch();
        gate.quarantine("dc1", "projection index 5 failed");
        assert!(gate.admits().is_err());
        assert!(gate.admits_epoch(epoch).is_err());
        assert!(gate
            .with_admission_epoch(epoch, || panic!("closed gate published"))
            .is_err());
        assert!(gate.reason().unwrap().contains("index 5"));
    }
    #[test]
    fn recursive_publication_is_refused_before_waiting() {
        let gate = RecoveryAdmission::new();
        let epoch = gate.epoch();
        gate.with_admission_epoch(epoch, || {
            assert!(
                gate.with_admission_epoch(epoch, || ()).is_err(),
                "nested publication must be refused rather than overlap the current publisher"
            );
        })
        .unwrap();
    }
}
