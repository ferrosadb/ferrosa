//! The pair-mode smoke stack must elect the same primary on every run.
//!
//! `tests/docker-smoke.sh` drives all pair-mode CQL against **node1** as the
//! primary and promotes **node2** in its Phase 3 operator-promotion step. That
//! only holds if the primary election is deterministic.
//!
//! `ModeController::choose_pair_role` awards `Primary` to the **lowest**
//! host_id (`local_host_id <= peer_host_id`). A node with no `FERROSA_HOST_ID`
//! generates a fresh random UUID on first boot, so with unpinned ids node1 won
//! that comparison only about half the time. On the runs it lost, node1 came up
//! a secondary — which refuses every client CQL connection while still
//! reporting healthy on `/readyz` — and the smoke test sat in `wait_cql` until
//! it timed out. That coin flip is what made the nightly Docker smoke test
//! intermittently red.
//!
//! These tests read the committed compose stacks and assert the ordering the
//! election depends on, so removing or reordering a pin fails here rather than
//! in a nightly run hours later. The election rule itself is pinned separately
//! by `ferrosa-cluster`'s `lower_host_id_takes_primary_role`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use uuid::Uuid;

/// Absolute path to a file in the workspace root.
fn workspace_file(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("ferrosa crate always has a workspace-root parent")
        .join(name)
}

/// Pinned `FERROSA_HOST_ID` for every node service in a compose file.
///
/// Returns a map of service name to parsed UUID. A service without a pinned
/// host_id is absent from the map — the caller decides whether that is fatal,
/// which keeps the "pin is missing" failure message specific.
fn pinned_host_ids(compose_path: &Path) -> BTreeMap<String, Uuid> {
    let raw = std::fs::read_to_string(compose_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", compose_path.display()));
    let doc: serde_yaml::Value = serde_yaml::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} is not valid YAML: {e}", compose_path.display()));

    let services = doc
        .get("services")
        .and_then(|s| s.as_mapping())
        .unwrap_or_else(|| panic!("{} has no `services` mapping", compose_path.display()));

    let mut pins = BTreeMap::new();
    for (name, service) in services {
        let Some(name) = name.as_str() else { continue };
        // Stacks namespace their services (`node1`, `ct-node1`, ...), so match
        // on the substring rather than a prefix.
        if !name.contains("node") {
            continue;
        }
        let Some(raw_id) = service
            .get("environment")
            .and_then(|env| env.get("FERROSA_HOST_ID"))
            .and_then(|id| id.as_str())
        else {
            continue;
        };
        let id = Uuid::parse_str(raw_id).unwrap_or_else(|e| {
            panic!("{name} has an unparseable FERROSA_HOST_ID {raw_id:?}: {e}")
        });
        pins.insert(name.to_string(), id);
    }
    pins
}

/// Every node in the pair-mode compose stack must pin its host_id.
///
/// Without a pin the node generates a random UUID per run and the election
/// outcome becomes a coin flip.
#[test]
fn pair_smoke_stack_pins_every_node_host_id() {
    let compose = workspace_file("docker-compose.yml");
    let pins = pinned_host_ids(&compose);

    for node in ["node1", "node2", "node3"] {
        assert!(
            pins.contains_key(node),
            "{node} in {} must pin FERROSA_HOST_ID — an unpinned node generates a \
             random UUID each run, making the pair primary election a coin flip",
            compose.display()
        );
    }
}

/// node1 must hold the lowest host_id, so it always takes the primary role.
///
/// `choose_pair_role` compares `local_host_id <= peer_host_id`, so the lowest
/// id wins. `tests/docker-smoke.sh` depends on that being node1.
#[test]
fn node1_is_deterministically_the_pair_primary() {
    let compose = workspace_file("docker-compose.yml");
    let pins = pinned_host_ids(&compose);

    let node1 = pins["node1"];
    for peer in ["node2", "node3"] {
        let peer_id = pins[peer];
        assert_ne!(
            node1, peer_id,
            "node1 and {peer} must not share a host_id — a duplicate identity \
             breaks peer bookkeeping as well as the election"
        );
        assert!(
            node1 < peer_id,
            "node1 ({node1}) must sort below {peer} ({peer_id}): \
             choose_pair_role gives Primary to the lowest host_id, and \
             tests/docker-smoke.sh drives pair-mode CQL against node1 as the primary"
        );
    }
}

/// The multi-node cluster stacks must also pin ids, so a Raft cluster that
/// passes through pair mode during formation lands on a stable primary.
#[test]
fn cluster_stacks_pin_every_node_host_id() {
    for stack in [
        "tests/docker-compose.cluster.yml",
        "tests/docker-compose.cluster.w3-standalone.yml",
        "tests/docker-compose.compaction-test.yml",
    ] {
        let compose = workspace_file(stack);
        let pins = pinned_host_ids(&compose);
        assert!(
            !pins.is_empty(),
            "{stack} must pin FERROSA_HOST_ID for its nodes so formation is reproducible"
        );
    }
}
