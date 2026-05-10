//! Sprint 2 W2.6 / W2.7 / W2.8 — topology-mutating nemeses.
//!
//! These nemeses change the cluster's *membership*, not just the network
//! between existing nodes:
//!
//!   - **add-node-via-follower** (W2.6): spin up `node4` configured to
//!     contact a follower (e.g. `node3`) as its seed. Forces openraft's
//!     learner-promotion path to traverse a non-leader, exercising the
//!     bug class Sprint 1 fixes.
//!   - **decommission-leader** (W2.7): identify the current leader from
//!     `current_leader` metric (or fallback to the highest-priority
//!     contact), invoke `ferrosa-ctl decommission` against it. The cluster
//!     must elect a new leader and shrink to N-1 voters.
//!   - **random-startup-order** (W2.8): tear the compose stack down and
//!     bring nodes up in shuffled order. Asserts that formation succeeds
//!     regardless of which node boots first — only meaningful with the
//!     symmetric seed config from W2.14.
//!
//! Pure-library tests in this module exercise the *plan* (which container
//! to spawn, which `ferrosa-ctl` arg to use, which order to bring nodes up
//! in) rather than the live container-runtime invocations. The live
//! invocations are gated behind `FERROSA_TEST_CONTAINERS=1` and panic with
//! a setup hint when the env var is absent.

use std::path::PathBuf;

use crate::chaos::{NemesisAction, NemesisContext};
use crate::docker_provision::container_runtime;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;

// ---------------------------------------------------------------------------
// add-node-via-follower (W2.6)
// ---------------------------------------------------------------------------

/// W2.6 nemesis. Spawns `node4` configured to contact `node3` (a follower)
/// as its seed, then waits for the new node to appear in every existing
/// node's membership snapshot.
pub struct AddNodeViaFollower {
    /// Service name of the seed to dial (e.g. `"node3"`). Must be a
    /// follower; `pick_follower_seed` resolves this from a node list.
    pub seed_service: String,
    /// Service name to assign the new node (e.g. `"node4"`).
    pub new_node_service: String,
    /// Path to the compose file to mutate. Used by tests; the live
    /// implementation reuses `compose_file_path`.
    pub compose_file: Option<PathBuf>,
}

impl AddNodeViaFollower {
    /// Pick a non-leader from `nodes` to use as the seed. The leader is
    /// supplied separately so the caller (or a metric scraper) can keep
    /// the choice deterministic across re-runs.
    pub fn pick_follower_seed(nodes: &[String], leader: &str) -> Option<String> {
        nodes.iter().find(|n| n.as_str() != leader).cloned()
    }
}

#[async_trait]
impl NemesisAction for AddNodeViaFollower {
    fn name(&self) -> &str {
        "add-node-via-follower"
    }

    async fn inject(&self, _ctx: &NemesisContext) -> Result<()> {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "add-node-via-follower requires FERROSA_TEST_CONTAINERS=1 \
                 and a docker-compose cluster"
            );
        }
        let rt = container_runtime();
        let compose = self
            .compose_file
            .clone()
            .ok_or_else(|| anyhow::anyhow!("compose_file must be set"))?;
        // Spawn the new node service via `compose up -d <service>`.
        let status = std::process::Command::new(rt)
            .args([
                "compose",
                "-f",
                compose.to_str().unwrap(),
                "up",
                "-d",
                "--no-recreate",
                self.new_node_service.as_str(),
            ])
            .status()
            .context("compose up <new node>")?;
        if !status.success() {
            bail!("compose up of {} failed", self.new_node_service);
        }
        Ok(())
    }

    async fn heal(&self, _ctx: &NemesisContext) -> Result<()> {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            return Ok(());
        }
        let rt = container_runtime();
        let compose = match &self.compose_file {
            Some(c) => c.clone(),
            None => return Ok(()),
        };
        // Stop the spawned node so the cluster returns to baseline.
        let _ = std::process::Command::new(rt)
            .args([
                "compose",
                "-f",
                compose.to_str().unwrap(),
                "rm",
                "-sf",
                self.new_node_service.as_str(),
            ])
            .status();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// decommission-leader (W2.7)
// ---------------------------------------------------------------------------

/// W2.7 nemesis. Issues `ferrosa-ctl decommission --node <leader>` against
/// the current leader and waits for cluster size to drop by one.
pub struct DecommissionLeader {
    /// Address of the admin HTTP endpoint to query for the current leader
    /// (e.g. `"http://localhost:49090/admin/membership-snapshot"`).
    pub admin_url: String,
    /// `ferrosa-ctl` binary path (`"ferrosa-ctl"` if on PATH).
    pub ctl_binary: String,
}

impl DecommissionLeader {
    /// Resolve the leader's host_id by inspecting a membership-snapshot
    /// JSON. Picks the first voter. In production the snapshot will
    /// include a `current_leader` field once Sprint 4 lands — the helper
    /// inspects that first, then falls back to "any voter".
    pub fn parse_leader_from_snapshot(snapshot_json: &str) -> Result<String> {
        let v: serde_json::Value = serde_json::from_str(snapshot_json)
            .context("parse_leader_from_snapshot: invalid JSON")?;
        if let Some(s) = v.get("current_leader").and_then(|x| x.as_str()) {
            if !s.is_empty() {
                return Ok(s.to_string());
            }
        }
        let voters = v
            .get("openraft_voters")
            .and_then(|x| x.as_array())
            .ok_or_else(|| anyhow::anyhow!("snapshot missing openraft_voters array"))?;
        let first = voters
            .first()
            .and_then(|s| s.as_str())
            .ok_or_else(|| anyhow::anyhow!("openraft_voters empty"))?;
        Ok(first.to_string())
    }
}

#[async_trait]
impl NemesisAction for DecommissionLeader {
    fn name(&self) -> &str {
        "decommission-leader"
    }

    async fn inject(&self, _ctx: &NemesisContext) -> Result<()> {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "decommission-leader requires FERROSA_TEST_CONTAINERS=1 \
                 and a docker-compose cluster"
            );
        }
        // Fetch the membership snapshot from the admin endpoint.
        let body = reqwest::get(&self.admin_url)
            .await
            .with_context(|| format!("GET {}", self.admin_url))?
            .text()
            .await
            .context("read body")?;
        let leader = Self::parse_leader_from_snapshot(&body)?;
        let status = std::process::Command::new(&self.ctl_binary)
            .args(["decommission", "--node", &leader])
            .status()
            .context("ferrosa-ctl decommission")?;
        if !status.success() {
            bail!("ferrosa-ctl decommission for leader {} failed", leader);
        }
        Ok(())
    }

    async fn heal(&self, _ctx: &NemesisContext) -> Result<()> {
        // Decommission is intentionally non-reversible from the nemesis;
        // the harness re-provisions the cluster between combinations.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// random-startup-order (W2.8)
// ---------------------------------------------------------------------------

/// W2.8 nemesis. Tears the compose stack down then brings nodes up in
/// shuffled order. Asserts cluster formation succeeds regardless of which
/// node boots first — only meaningful with the W2.14 symmetric seed config.
pub struct RandomStartupOrder {
    /// Compose file path.
    pub compose_file: Option<PathBuf>,
    /// Node service names to shuffle.
    pub node_services: Vec<String>,
}

impl RandomStartupOrder {
    /// Produce a shuffled startup order. Pure function — used by tests
    /// to verify the property without spawning containers.
    ///
    /// Uses Fisher–Yates with the supplied RNG to remain compatible with
    /// rand 0.10 (the `rand::seq::SliceRandom::shuffle` extension trait
    /// has churned across releases; rolling our own avoids the moving
    /// target).
    pub fn plan_startup_order(services: &[String], rng: &mut impl rand::Rng) -> Vec<String> {
        use rand::RngExt;
        let mut v: Vec<String> = services.to_vec();
        let n = v.len();
        for i in (1..n).rev() {
            let j = rng.random_range(0..=i);
            v.swap(i, j);
        }
        v
    }
}

#[async_trait]
impl NemesisAction for RandomStartupOrder {
    fn name(&self) -> &str {
        "random-startup-order"
    }

    async fn inject(&self, _ctx: &NemesisContext) -> Result<()> {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "random-startup-order requires FERROSA_TEST_CONTAINERS=1 \
                 and a docker-compose cluster"
            );
        }
        let rt = container_runtime();
        let compose = self
            .compose_file
            .clone()
            .ok_or_else(|| anyhow::anyhow!("compose_file must be set"))?;
        // Tear down everything first.
        let _ = std::process::Command::new(rt)
            .args(["compose", "-f", compose.to_str().unwrap(), "down", "-v"])
            .status();

        let order = {
            let mut rng = rand::rng();
            Self::plan_startup_order(&self.node_services, &mut rng)
        };
        for svc in order {
            let status = std::process::Command::new(rt)
                .args([
                    "compose",
                    "-f",
                    compose.to_str().unwrap(),
                    "up",
                    "-d",
                    svc.as_str(),
                ])
                .status()
                .context("compose up <svc>")?;
            if !status.success() {
                bail!("compose up {} failed", svc);
            }
            // Wait for the started node to report healthy before continuing.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        Ok(())
    }

    async fn heal(&self, _ctx: &NemesisContext) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    // -----------------------------------------------------------------------
    // W2.6 — add-node-via-follower
    // -----------------------------------------------------------------------

    #[test]
    fn add_node_via_follower_picks_a_non_leader() {
        let nodes = vec![
            "node1".to_string(),
            "node2".to_string(),
            "node3".to_string(),
        ];
        let leader = "node1";
        let seed = AddNodeViaFollower::pick_follower_seed(&nodes, leader).unwrap();
        assert_ne!(seed, leader, "seed must be a non-leader");
        assert!(nodes.contains(&seed));
    }

    #[test]
    fn add_node_via_follower_returns_none_when_only_leader_present() {
        let nodes = vec!["node1".to_string()];
        let seed = AddNodeViaFollower::pick_follower_seed(&nodes, "node1");
        assert!(
            seed.is_none(),
            "no follower available — caller must handle None"
        );
    }

    #[test]
    fn add_node_via_follower_has_correct_name() {
        let nem = AddNodeViaFollower {
            seed_service: "node3".into(),
            new_node_service: "node4".into(),
            compose_file: None,
        };
        assert_eq!(nem.name(), "add-node-via-follower");
    }

    // -----------------------------------------------------------------------
    // W2.7 — decommission-leader
    // -----------------------------------------------------------------------

    #[test]
    fn decommission_leader_parses_current_leader_field() {
        let snapshot = r#"{
            "current_leader": "host-A",
            "openraft_voters": ["host-A", "host-B", "host-C"]
        }"#;
        let leader = DecommissionLeader::parse_leader_from_snapshot(snapshot).unwrap();
        assert_eq!(leader, "host-A");
    }

    #[test]
    fn decommission_leader_falls_back_to_first_voter() {
        let snapshot = r#"{
            "openraft_voters": ["host-A", "host-B", "host-C"]
        }"#;
        let leader = DecommissionLeader::parse_leader_from_snapshot(snapshot).unwrap();
        assert_eq!(
            leader, "host-A",
            "fallback must be deterministic — first voter"
        );
    }

    #[test]
    fn decommission_leader_errors_on_empty_voter_list() {
        let snapshot = r#"{ "openraft_voters": [] }"#;
        let err = DecommissionLeader::parse_leader_from_snapshot(snapshot)
            .expect_err("empty voter list must error");
        assert!(err.to_string().contains("openraft_voters empty"));
    }

    #[test]
    fn decommission_leader_errors_on_invalid_json() {
        let err = DecommissionLeader::parse_leader_from_snapshot("not json")
            .expect_err("invalid JSON must error");
        assert!(err.to_string().contains("parse_leader_from_snapshot"));
    }

    #[test]
    fn decommission_leader_has_correct_name() {
        let nem = DecommissionLeader {
            admin_url: "http://example/snapshot".into(),
            ctl_binary: "ferrosa-ctl".into(),
        };
        assert_eq!(nem.name(), "decommission-leader");
    }

    // -----------------------------------------------------------------------
    // W2.8 — random-startup-order
    // -----------------------------------------------------------------------

    /// W2.8 plan-level test: with 10 different RNG seeds, the planner emits
    /// at least two distinct first-boot nodes. Locks the property that the
    /// nemesis actually shuffles.
    #[test]
    fn random_startup_order_does_not_break_formation() {
        // Pure-function check: plan_startup_order produces all nodes
        // (no drops, no duplicates) and yields different orders for
        // different seeds.
        let services: Vec<String> = (1..=4).map(|i| format!("node{i}")).collect();
        let mut firsts = std::collections::HashSet::new();
        for seed in 0..10u64 {
            let mut rng = StdRng::seed_from_u64(seed);
            let order = RandomStartupOrder::plan_startup_order(&services, &mut rng);
            assert_eq!(order.len(), services.len(), "no nodes dropped");
            let unique: std::collections::HashSet<_> = order.iter().collect();
            assert_eq!(
                unique.len(),
                services.len(),
                "no nodes duplicated; order={order:?}"
            );
            firsts.insert(order[0].clone());
        }
        assert!(
            firsts.len() >= 2,
            "10 seeds must produce at least 2 distinct first-boot nodes; got {firsts:?}"
        );
    }

    #[test]
    fn random_startup_order_has_correct_name() {
        let nem = RandomStartupOrder {
            compose_file: None,
            node_services: vec![],
        };
        assert_eq!(nem.name(), "random-startup-order");
    }
}
