//! Bootstrap phase-runner skeleton.
//!
//! The live `transition_to_cluster` path still executes most phase bodies inline,
//! but it now consumes this named runner seam for the canonical ordering. Follow-up
//! cards can move one phase body at a time behind this API while preserving logs,
//! metrics labels, and tests.

use super::BootstrapPhase;

#[derive(Debug, Clone, Copy, Default)]
pub struct BootstrapPhaseRunner;

impl BootstrapPhaseRunner {
    pub fn canonical() -> Self {
        Self
    }

    pub fn phase_order(self) -> &'static [BootstrapPhase] {
        BootstrapPhase::all()
    }
}
