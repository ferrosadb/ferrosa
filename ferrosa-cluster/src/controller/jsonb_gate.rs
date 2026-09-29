//! Module: the interim jsonb gate on leaving standalone (T-300, D24, D15a).
//!
//! Responsibility: the DDL half of the gate lives in
//! `ferrosa_schema::jsonb_rules` (one rule for both wires, checked at entry and
//! apply). This module is the transition half: while any table holds jsonb, a
//! node may not leave `Standalone`, at startup (peers/seeds configured or a
//! former cluster member) or at runtime (a handshake that would move it to
//! `Pair`, `Forming` or `Cluster`). The controller's mode cell is the schema's
//! mode cell, so the DDL gate always sees the mode this controller holds.
//!
//! Correctness: a refusal names every table holding jsonb and D15a, never
//! suggests a bypass, and there is none. A schema that cannot be scanned is
//! refused too (fail closed, FMEA SCH-T300-06). Refusals are logged as edges:
//! one ERROR when refusing starts, one INFO when it clears.
//! Last revised: 2026-09-28
//! Last changed: New module (T-300).

use std::sync::atomic::Ordering;

pub use ferrosa_schema::jsonb_rules::{jsonb_ddl_permitted, JsonbDdlRefused};

use super::{DeploymentMode, ModeController};

/// Why a node may not leave (or start outside) standalone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonbTransitionRefused {
    /// These tables hold jsonb.
    Tables {
        target: DeploymentMode,
        tables: Vec<String>,
    },
    /// The schema could not be scanned, so absence of jsonb is unproven.
    Unverifiable {
        target: DeploymentMode,
        reason: String,
    },
}

impl std::fmt::Display for JsonbTransitionRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tables { target, tables } => write!(
                f,
                "refusing to leave standalone for {target} mode: jsonb columns exist in {} and \
                 are allowed on a standalone node only until the capability ledger (D15a) lands",
                tables.join(", ")
            ),
            Self::Unverifiable { target, reason } => write!(
                f,
                "refusing to leave standalone for {target} mode: the schema could not be \
                 scanned for jsonb columns ({reason}); jsonb requires the capability ledger \
                 (D15a) outside standalone"
            ),
        }
    }
}

impl std::error::Error for JsonbTransitionRefused {}

impl ModeController {
    /// Refuse a move from a jsonb-permitting mode (standalone) to `target`
    /// while any table holds jsonb (SCH-T300-05). Pure: no logging, no state.
    pub(super) fn check_leaving_standalone(
        &self,
        target: DeploymentMode,
    ) -> Result<(), JsonbTransitionRefused> {
        let current = self.mode();
        if jsonb_ddl_permitted(current).is_err() || jsonb_ddl_permitted(target).is_ok() {
            return Ok(());
        }
        self.check_jsonb_absent(target)
    }

    /// The startup check (SCH-T300-05): a node that is not standalone, or is
    /// configured with seeds and so will not stay standalone, must not carry
    /// jsonb columns. The caller exits non-zero on `Err`.
    pub fn check_startup_jsonb(&self) -> Result<(), JsonbTransitionRefused> {
        let current = self.mode();
        let target = if jsonb_ddl_permitted(current).is_err() {
            current
        } else if !self.net_config.seeds.is_empty() {
            DeploymentMode::Pair
        } else {
            return Ok(());
        };
        self.check_jsonb_absent(target)
    }

    fn check_jsonb_absent(&self, target: DeploymentMode) -> Result<(), JsonbTransitionRefused> {
        match self.schema.tables_with_jsonb() {
            Ok(tables) if tables.is_empty() => Ok(()),
            Ok(tables) => Err(JsonbTransitionRefused::Tables { target, tables }),
            Err(e) => Err(JsonbTransitionRefused::Unverifiable {
                target,
                reason: e.to_string(),
            }),
        }
    }

    /// Whether the node may move to `target`; logs the edges only. Called at
    /// the top of every transition out of standalone, before any write path
    /// is touched, and again inside `try_transition_mode` as a backstop.
    pub(super) fn leaving_standalone_permitted(&self, target: DeploymentMode) -> bool {
        match self.check_leaving_standalone(target) {
            Ok(()) => {
                if self.jsonb_gate_refusing.swap(false, Ordering::Relaxed) {
                    tracing::info!(%target, "jsonb gate cleared: leaving standalone is allowed again");
                }
                true
            }
            Err(refused) => {
                if !self.jsonb_gate_refusing.swap(true, Ordering::Relaxed) {
                    tracing::error!(%refused, "jsonb gate: staying standalone (D24, D15a)");
                }
                false
            }
        }
    }

    /// True while a departure from standalone is being refused (for readiness).
    pub fn jsonb_gate_refusing(&self) -> bool {
        self.jsonb_gate_refusing.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonb_ddl_permitted_only_standalone() {
        for mode in [
            DeploymentMode::Standalone,
            DeploymentMode::Pair,
            DeploymentMode::Forming,
            DeploymentMode::Cluster,
            DeploymentMode::DegradedPair,
            DeploymentMode::DegradedCluster,
        ] {
            match jsonb_ddl_permitted(mode) {
                Ok(()) => assert_eq!(mode, DeploymentMode::Standalone),
                Err(refused) => {
                    assert_ne!(mode, DeploymentMode::Standalone);
                    assert_eq!(refused.mode, mode);
                    assert!(refused.to_string().contains("D15a"), "{refused}");
                }
            }
        }
    }
}
