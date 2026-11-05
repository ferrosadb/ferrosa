//! Shared recovery admission for inconsistent Raft state.
//! Correctness: detection and synchronous publication share one transition lock.
//! Last revised: 2026-11-05
//! Last changed: close all consumers through one epoch-bound recovery gate.

use parking_lot::RwLock;
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
    transition: RwLock<AdmissionState>,
}
impl RecoveryAdmission {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn epoch(&self) -> u64 {
        self.transition.read().epoch
    }
    pub fn reason(&self) -> Option<String> {
        self.transition
            .read()
            .faults
            .iter()
            .next()
            .map(|(group, reason)| format!("{group}: {reason}"))
    }
    pub fn admits(&self) -> Result<(), RecoveryRefusal> {
        let state = self.transition.read();
        Self::check(&state, None)
    }
    pub fn admits_epoch(&self, epoch: u64) -> Result<(), RecoveryRefusal> {
        let state = self.transition.read();
        Self::check(&state, Some(epoch))
    }
    /// Close in memory before any durable progress I/O or peer repair work.
    pub fn quarantine(&self, group: &str, reason: impl Into<String>) -> u64 {
        let mut state = self.transition.write();
        state.faults.insert(group.into(), reason.into());
        Self::advance(&mut state)
    }
    /// The closure is synchronous, short, and must not call this gate recursively.
    /// It linearizes snapshot slot/watermark publication with detection.
    pub fn with_admission_epoch<T>(
        &self,
        epoch: u64,
        publish: impl FnOnce() -> T,
    ) -> Result<T, RecoveryRefusal> {
        let state = self.transition.read();
        Self::check(&state, Some(epoch))?;
        Ok(publish())
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
            assert!(gate.transition.try_write().is_none());
        })
        .unwrap();
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
}
