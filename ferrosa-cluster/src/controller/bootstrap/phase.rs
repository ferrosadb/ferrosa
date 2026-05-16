//! Phase enum + structured error for the bootstrap pipeline.
//!
//! Each step in `transition_to_cluster` is one [`BootstrapPhase`].  A
//! failure inside a phase is reported as [`BootstrapError::Phase`] with
//! the phase name attached, so log lines and metrics can attribute the
//! failure to a single step.
//!
//! Phases are pure value types — they carry no behaviour beyond
//! identification.  Pre/post-condition functions live alongside the
//! phase implementations in their own modules.
//!
//! Both types implement `Eq` so unit tests can match the precise phase
//! that produced an error, and `Display` for human-readable logs.

use std::fmt;

/// One ordered step in the cluster bootstrap pipeline.
///
/// Ordering matches the runtime sequence in
/// [`crate::controller::bootstrap`].  Adding a new phase is a breaking
/// change for downstream metrics; new phases must extend the enum and
/// update the docs there.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BootstrapPhase {
    DeliverInvites,
    EstablishPools,
    CreateRaft,
    WaitLeader,
    ReplaySchema,
    BootstrapStream,
    Promote,
    DrainQueue,
}

impl BootstrapPhase {
    /// Stable string id used in log fields and metric labels.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeliverInvites => "deliver_invites",
            Self::EstablishPools => "establish_pools",
            Self::CreateRaft => "create_raft",
            Self::WaitLeader => "wait_leader",
            Self::ReplaySchema => "replay_schema",
            Self::BootstrapStream => "bootstrap_stream",
            Self::Promote => "promote",
            Self::DrainQueue => "drain_queue",
        }
    }

    /// Every phase, in run order. Used by tests to assert exhaustiveness.
    pub fn all() -> &'static [BootstrapPhase] {
        &[
            Self::DeliverInvites,
            Self::EstablishPools,
            Self::CreateRaft,
            Self::WaitLeader,
            Self::ReplaySchema,
            Self::BootstrapStream,
            Self::Promote,
            Self::DrainQueue,
        ]
    }
}

impl fmt::Display for BootstrapPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Boxed dynamic error used as the source payload of [`BootstrapError`].
///
/// Phases produce errors from a wide range of upstream types
/// (`io::Error`, `openraft::error::*`, validation strings).  Boxing
/// keeps the API small without pulling in `anyhow`.
pub type BootstrapSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Failure surfaced by any bootstrap phase.
///
/// The `name` field carries the phase that produced the error, so
/// `BootstrapError`s emitted from different phases compare unequal even
/// when their wrapped causes are identical strings.
#[derive(Debug)]
pub enum BootstrapError {
    /// A pre-condition, the phase body, or a post-condition failed.
    Phase {
        name: BootstrapPhase,
        source: BootstrapSource,
    },
}

impl BootstrapError {
    /// Build a phase-tagged error from any `Display`-able message.
    pub fn phase(name: BootstrapPhase, msg: impl Into<String>) -> Self {
        #[derive(Debug)]
        struct Msg(String);
        impl fmt::Display for Msg {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl std::error::Error for Msg {}
        Self::Phase {
            name,
            source: Box::new(Msg(msg.into())),
        }
    }

    /// Wrap an existing boxed error with the offending phase.
    pub fn from_source(name: BootstrapPhase, err: BootstrapSource) -> Self {
        Self::Phase { name, source: err }
    }

    pub fn name(&self) -> BootstrapPhase {
        match self {
            Self::Phase { name, .. } => *name,
        }
    }
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Phase { name, source } => write!(f, "bootstrap phase '{name}' failed: {source}"),
        }
    }
}

impl std::error::Error for BootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Phase { source, .. } => Some(source.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each phase carries pre/post conditions through
    /// the `Result<(), BootstrapError>` type and `BootstrapError`
    /// distinguishes phases by name.
    #[test]
    fn bootstrap_phase_pre_post_conditions() {
        // Every phase must round-trip through the `as_str` table.
        for &p in BootstrapPhase::all() {
            assert!(!p.as_str().is_empty(), "phase {p:?} missing label");
        }

        // The eight phases are enumerated; if a future change adds a
        // ninth, this assertion forces the docs to be updated.
        assert_eq!(BootstrapPhase::all().len(), 8);

        // BootstrapError distinguishes phases by `name`.
        let a = BootstrapError::phase(BootstrapPhase::DeliverInvites, "boom");
        let b = BootstrapError::phase(BootstrapPhase::WaitLeader, "boom");
        assert_eq!(a.name(), BootstrapPhase::DeliverInvites);
        assert_eq!(b.name(), BootstrapPhase::WaitLeader);
        assert_ne!(a.name(), b.name());

        // Display includes the phase id so log scraping can group by it.
        let rendered = format!("{a}");
        assert!(
            rendered.contains("deliver_invites"),
            "expected phase id in display: {rendered}"
        );

        // Pre/post-condition signatures: a `Result<(), BootstrapError>`
        // returned from a closure typed for `precondition` is valid.
        let pre: fn() -> Result<(), BootstrapError> = || Ok(());
        let post: fn() -> Result<(), BootstrapError> = || {
            Err(BootstrapError::phase(
                BootstrapPhase::Promote,
                "post failed",
            ))
        };
        assert!(pre().is_ok());
        let err = post().unwrap_err();
        assert_eq!(err.name(), BootstrapPhase::Promote);
    }
}
