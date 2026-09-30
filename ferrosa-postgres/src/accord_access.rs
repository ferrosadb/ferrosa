//! How the PostgreSQL front-end reaches Accord, without depending on
//! `ferrosa-cluster`.
//!
//! The binary (`ferrosa`) owns the `SessionCore` and therefore the live node
//! state; this crate owns the wire protocol. [`AccordAccess`] is the seam
//! between them: it carries two things the front-end needs, both supplied by the
//! binary and neither naming a cluster type here.
//!
//! ## Why the committer is a *factory*, not a value
//!
//! A front-end listener is constructed at startup, before the node has connected
//! to its seeds — so before it has formed a cluster. `ferrosa/src/main.rs` binds
//! the PostgreSQL listener at step 11b while seed connection (and therefore
//! formation) begins at step 12. A committer snapshotted at that moment would be
//! `None` for a node that becomes a Raft cluster a second later, permanently
//! disabling PostgreSQL transaction ordering on every real cluster.
//!
//! So this holds a *function* that re-answers "what can this node offer now?"
//! and every statement asks it. A node that is still a singleton gets `None`
//! (its per-key Accord resolver cannot place a key, and offering a committer
//! would fail every table statement with `no replicas resolved … cluster mode
//! required`); the same node acquires a committer the moment the controller
//! installs the cluster write path.
//!
//! ## Why observer registration is *separate* from the committer
//!
//! The MVCC observer must be installed even before the node is a cluster, or a
//! cluster that forms later would apply Accord writes with PostgreSQL MVCC
//! visibility silently disabled. It therefore targets the node's Accord-state
//! slot, which retains a registered observer until formation attaches it —
//! not the committer, which is `None` at that point.

use std::sync::Arc;

use ferrosa_storage::accord::{PostgresMvccApplyObserver, TransactionCommitter};

/// A committer the node can offer *right now*, or `None` if it is not a Raft
/// cluster (or has lost its write path).
type CommitterFactory = dyn Fn() -> Option<Arc<dyn TransactionCommitter>> + Send + Sync;

/// Installs the PostgreSQL MVCC observer on this node's local Accord apply path.
type ObserverSink = dyn Fn(Arc<dyn PostgresMvccApplyObserver>) -> Result<(), String> + Send + Sync;

/// The PostgreSQL front-end's handle on Accord.
#[derive(Clone, Default)]
pub struct AccordAccess {
    committer: Option<Arc<CommitterFactory>>,
    observer_sink: Option<Arc<ObserverSink>>,
}

impl AccordAccess {
    /// Accord is not reachable from this front-end (standalone-only deployments
    /// and unit tests that do not exercise transactions).
    pub fn disabled() -> Self {
        Self::default()
    }

    /// A committer that is always offered, plus observer registration on that
    /// same committer.
    ///
    /// For tests that inject their own Accord plumbing (an in-process state
    /// machine, a mock, or a two-node harness): the fixture *is* the cluster, so
    /// there is no live node state to gate on and gating would only make the
    /// fixture harder to drive.
    pub fn fixed(committer: Arc<dyn TransactionCommitter>) -> Self {
        let factory_source = Arc::clone(&committer);
        let factory: Arc<CommitterFactory> = Arc::new(move || Some(Arc::clone(&factory_source)));
        let sink_committer = committer;
        let sink: Arc<ObserverSink> = Arc::new(move |observer| {
            sink_committer
                .register_postgres_mvcc_observer(observer)
                .map_err(|error| error.to_string())
        });
        Self {
            committer: Some(factory),
            observer_sink: Some(sink),
        }
    }

    /// Production wiring: `committer` re-answers against the node's live state on
    /// every statement, and `register_observer` survives cluster formation.
    pub fn live<C, R>(committer: C, register_observer: R) -> Self
    where
        C: Fn() -> Option<Arc<dyn TransactionCommitter>> + Send + Sync + 'static,
        R: Fn(Arc<dyn PostgresMvccApplyObserver>) -> Result<(), String> + Send + Sync + 'static,
    {
        Self {
            committer: Some(Arc::new(committer)),
            observer_sink: Some(Arc::new(register_observer)),
        }
    }

    /// The committer this node can offer for the statement about to run.
    ///
    /// Re-evaluated on every call by design — see the module docs. `None` means
    /// the front-end must take its local path (and must reject anything that was
    /// begun expecting cluster-wide ordering).
    pub fn committer(&self) -> Option<Arc<dyn TransactionCommitter>> {
        self.committer.as_ref().and_then(|factory| factory())
    }

    /// Install the PostgreSQL MVCC observer on the node's local Accord apply
    /// path, if this deployment has one at all.
    pub fn register_observer(
        &self,
        observer: Arc<dyn PostgresMvccApplyObserver>,
    ) -> Result<(), String> {
        match &self.observer_sink {
            Some(sink) => sink(observer),
            None => Ok(()),
        }
    }
}
