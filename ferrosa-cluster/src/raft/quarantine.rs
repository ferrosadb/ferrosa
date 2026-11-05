//! Shared recovery admission for inconsistent Raft state.
//! Correctness: publishers serialize; detection never waits for publisher I/O.
//! Last revised: 2026-11-05
//! Last changed: refuse busy publication and drain only a quarantined epoch.

use parking_lot::{Mutex, RwLock};
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
    publication: Mutex<()>,
    epoch_changed: tokio::sync::Notify,
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
        let reason = reason.into();
        let mut state = self.transition.write();
        state.faults.insert(group.into(), reason);
        let epoch = Self::advance(&mut state);
        drop(state);
        self.epoch_changed.notify_waiters();
        epoch
    }
    /// Register before checking the epoch so a transition cannot be lost between
    /// the check and suspension. No watch read guard is held by the consumer.
    pub async fn changed_since(&self, seen: u64) -> u64 {
        loop {
            let changed = self.epoch_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let epoch = self.epoch();
            if epoch != seen {
                return epoch;
            }
            changed.await;
        }
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
        let _publisher = self.publication.try_lock().ok_or_else(|| RecoveryRefusal {
            epoch: self.epoch(),
            reason: "recovery publication is busy".into(),
        })?;
        self.admits_epoch(epoch)?;
        let result = publish();
        self.admits_epoch(epoch)?;
        Ok(result)
    }

    /// Prove no old publisher remains only for the exact quarantined epoch.
    /// Busy publication is refused, never waited upon: a cross-thread nested
    /// caller must not create a dependency cycle with the current publisher.
    pub fn drain_publications(&self, expected_epoch: u64) -> Result<(), RecoveryRefusal> {
        let _publisher = self.publication.try_lock().ok_or_else(|| RecoveryRefusal {
            epoch: self.epoch(),
            reason: "recovery publication is still active".into(),
        })?;
        let state = self.transition.read();
        if state.epoch != expected_epoch || state.faults.is_empty() {
            return Err(RecoveryRefusal {
                epoch: state.epoch,
                reason: "publication drain requires the current quarantined epoch".into(),
            });
        }
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
    fn publication_holds_the_publisher_lease() {
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
            assert!(gate.drain_publications(epoch).is_err());
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
    #[test]
    fn cross_thread_nested_publication_refuses_instead_of_waiting() {
        let gate = std::sync::Arc::new(RecoveryAdmission::new());
        let (send, receive) = std::sync::mpsc::channel();
        let mut child = None;
        let mut nested_result = None;
        gate.with_admission_epoch(gate.epoch(), || {
            let nested = std::sync::Arc::clone(&gate);
            child = Some(std::thread::spawn(move || {
                let result = nested.with_admission_epoch(nested.epoch(), || ());
                send.send(result).unwrap();
            }));
            // Cleanup deadline only: the held publication makes the blocked
            // dependency deterministic; release it before joining on failure.
            nested_result = receive.recv_timeout(std::time::Duration::from_secs(5)).ok();
        })
        .unwrap();
        child.unwrap().join().unwrap();
        assert!(
            matches!(nested_result, Some(Err(_))),
            "nested cross-thread publisher waited on its parent"
        );
    }

    #[test]
    fn drain_requires_current_quarantined_epoch() {
        let gate = RecoveryAdmission::new();
        let healthy = gate.epoch();
        assert!(gate.drain_publications(healthy).is_err());
        let closed = gate.quarantine("group", "projection failure");
        assert!(gate.drain_publications(healthy).is_err());
        assert!(gate.drain_publications(closed).is_ok());
    }
    #[tokio::test]
    async fn epoch_change_wakes_registered_and_late_waiters() {
        use std::{future::Future, task::Poll};
        let gate = RecoveryAdmission::new();
        let initial = gate.epoch();
        let mut wait = Box::pin(gate.changed_since(initial));
        std::future::poll_fn(|context| {
            assert!(wait.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        let closed = gate.quarantine("group", "projection failure");
        assert_eq!(wait.await, closed);
        assert_eq!(gate.changed_since(initial).await, closed);
    }
}
