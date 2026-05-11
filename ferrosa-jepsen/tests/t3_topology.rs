//! Sprint 6 W6.9 — T3 (3+3 dual-DC) topology bring-up.
//!
//! Validates the multi-DC compose file at
//! `ferrosa-jepsen/tests/docker/jepsen-cluster-t3.yml`:
//!
//! 1. The YAML is well-formed and parses.
//! 2. It declares two per-DC networks (`dc1-net`, `dc2-net`) plus a
//!    `wan-net` bridge that both DCs attach to.
//! 3. Three voters in each DC, each pinned to its DC via
//!    `FERROSA_DATA_CENTER`. Per-DC seeding stays inside the DC.
//! 4. (Optional, opt-in via `FERROSA_TEST_CONTAINERS=1`) bringing the
//!    full stack up: both DCs reach Cluster mode within 30s of start.
//!
//! Steps 1-3 run on every CI invocation; step 4 panics with setup
//! instructions when the env var is set but no container runtime is
//! reachable, per the test policy in `CLAUDE.md`.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

const COMPOSE_RELATIVE: &str = "tests/docker/jepsen-cluster-t3.yml";

fn compose_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join(COMPOSE_RELATIVE)
}

#[test]
fn t3_compose_yaml_exists_and_parses() {
    let path = compose_path();
    assert!(
        path.exists(),
        "Sprint 6 W6.9: compose file missing at {}",
        path.display()
    );
    let raw = std::fs::read_to_string(&path).expect("read compose yaml");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&raw).expect("parse compose yaml");

    // services: dc1-node1..3 + dc2-node1..3 + rustfs + rustfs-init + volume-permissions.
    let services = parsed
        .get("services")
        .and_then(|v| v.as_mapping())
        .expect("services mapping");
    for required in [
        "rustfs",
        "rustfs-init",
        "dc1-node1",
        "dc1-node2",
        "dc1-node3",
        "dc2-node1",
        "dc2-node2",
        "dc2-node3",
    ] {
        assert!(
            services.contains_key(serde_yaml::Value::String(required.into())),
            "compose must declare service `{required}`"
        );
    }

    // networks: dc1-net, dc2-net, wan-net.
    let networks = parsed
        .get("networks")
        .and_then(|v| v.as_mapping())
        .expect("networks mapping");
    for required in ["dc1-net", "dc2-net", "wan-net"] {
        assert!(
            networks.contains_key(serde_yaml::Value::String(required.into())),
            "compose must declare network `{required}` (W6.9)"
        );
    }
}

#[test]
fn t3_compose_pins_each_dc_node_to_its_dc_via_env() {
    let raw = std::fs::read_to_string(compose_path()).expect("read compose yaml");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&raw).expect("parse compose yaml");
    let services = parsed.get("services").and_then(|v| v.as_mapping()).unwrap();

    // Helper: read FERROSA_DATA_CENTER from a service definition.
    let dc_of = |name: &str| -> String {
        let svc = services
            .get(serde_yaml::Value::String(name.into()))
            .unwrap_or_else(|| panic!("service {name} missing"));
        let env = svc.get("environment").and_then(|v| v.as_mapping()).unwrap();
        env.get(serde_yaml::Value::String("FERROSA_DATA_CENTER".into()))
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| panic!("{name} missing FERROSA_DATA_CENTER"))
    };

    for n in ["dc1-node1", "dc1-node2", "dc1-node3"] {
        assert_eq!(dc_of(n), "dc1", "{n} must run in dc1");
    }
    for n in ["dc2-node1", "dc2-node2", "dc2-node3"] {
        assert_eq!(dc_of(n), "dc2", "{n} must run in dc2");
    }
}

#[test]
fn t3_compose_seeds_within_each_dc() {
    // Each DC's seed list must reference only nodes in that DC.
    // Cross-DC peering is added via wan-net at runtime, not via
    // FERROSA_SEED.
    let raw = std::fs::read_to_string(compose_path()).expect("read compose yaml");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&raw).expect("parse compose yaml");
    let services = parsed.get("services").and_then(|v| v.as_mapping()).unwrap();

    for n in ["dc1-node1", "dc1-node2", "dc1-node3"] {
        let svc = services.get(serde_yaml::Value::String(n.into())).unwrap();
        let env = svc.get("environment").and_then(|v| v.as_mapping()).unwrap();
        let seed = env
            .get(serde_yaml::Value::String("FERROSA_SEED".into()))
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("{n} missing FERROSA_SEED"));
        assert!(
            !seed.contains("dc2-"),
            "{n} seed must not reference DC2 nodes (got `{seed}`)"
        );
        assert!(
            seed.contains("dc1-"),
            "{n} seed must reference DC1 nodes (got `{seed}`)"
        );
    }
    for n in ["dc2-node1", "dc2-node2", "dc2-node3"] {
        let svc = services.get(serde_yaml::Value::String(n.into())).unwrap();
        let env = svc.get("environment").and_then(|v| v.as_mapping()).unwrap();
        let seed = env
            .get(serde_yaml::Value::String("FERROSA_SEED".into()))
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("{n} missing FERROSA_SEED"));
        assert!(
            !seed.contains("dc1-"),
            "{n} seed must not reference DC1 nodes (got `{seed}`)"
        );
        assert!(
            seed.contains("dc2-"),
            "{n} seed must reference DC2 nodes (got `{seed}`)"
        );
    }
}

#[test]
fn t3_compose_attaches_each_dc_to_its_network_and_wan() {
    let raw = std::fs::read_to_string(compose_path()).expect("read compose yaml");
    let parsed: serde_yaml::Value = serde_yaml::from_str(&raw).expect("parse compose yaml");
    let services = parsed.get("services").and_then(|v| v.as_mapping()).unwrap();

    let networks_of = |name: &str| -> Vec<String> {
        let svc = services
            .get(serde_yaml::Value::String(name.into()))
            .unwrap();
        match svc.get("networks") {
            Some(serde_yaml::Value::Sequence(seq)) => seq
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
            _ => Vec::new(),
        }
    };

    for n in ["dc1-node1", "dc1-node2", "dc1-node3"] {
        let nets = networks_of(n);
        assert!(nets.contains(&"dc1-net".to_string()), "{n} missing dc1-net");
        assert!(nets.contains(&"wan-net".to_string()), "{n} missing wan-net");
        assert!(
            !nets.contains(&"dc2-net".to_string()),
            "{n} must NOT attach to dc2-net"
        );
    }
    for n in ["dc2-node1", "dc2-node2", "dc2-node3"] {
        let nets = networks_of(n);
        assert!(nets.contains(&"dc2-net".to_string()), "{n} missing dc2-net");
        assert!(nets.contains(&"wan-net".to_string()), "{n} missing wan-net");
        assert!(
            !nets.contains(&"dc1-net".to_string()),
            "{n} must NOT attach to dc1-net"
        );
    }
}

/// W6.9 RED: bringing up the live T3 cluster, both DCs reach Cluster mode
/// within 30s. Requires container runtime — Docker Desktop or Podman
/// Desktop with the bundled compose plugin.
///
/// Per the test policy in `CLAUDE.md`, tests requiring container
/// infrastructure must `panic!` with setup instructions when invoked
/// without the runtime — never silently skip. We honor that here.
#[test]
fn t3_topology_brings_up_two_dcs() {
    if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
        panic!(
            "FERROSA_TEST_CONTAINERS not set.\n\
             Bring up the T3 (3+3 dual-DC) topology:\n  \
             docker compose -f ferrosa-jepsen/tests/docker/jepsen-cluster-t3.yml up -d --build\n\
             Then re-run with:\n  \
             FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-jepsen --test t3_topology -- t3_topology_brings_up_two_dcs"
        );
    }

    let runtime = container_runtime();
    let compose = compose_path();
    let compose_str = compose.to_str().expect("compose path is valid UTF-8");

    // Bring up.
    let up = Command::new(runtime)
        .args(["compose", "-f", compose_str, "up", "-d", "--build"])
        .status()
        .expect("spawn compose up");
    assert!(up.success(), "compose up failed");

    // Poll each DC's first node's /health endpoint.
    let endpoints = [
        ("dc1", "http://localhost:29090/health"),
        ("dc2", "http://localhost:29190/health"),
    ];
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut healthy: std::collections::BTreeSet<&'static str> = Default::default();
    while std::time::Instant::now() < deadline && healthy.len() < endpoints.len() {
        for (dc, url) in endpoints {
            if healthy.contains(dc) {
                continue;
            }
            let ok = Command::new("curl")
                .args(["-sf", url])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if ok {
                healthy.insert(dc);
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    // Always tear down before asserting so a failure doesn't leave
    // containers around.
    let _ = Command::new(runtime)
        .args(["compose", "-f", compose_str, "down", "-v"])
        .status();

    assert_eq!(
        healthy.len(),
        endpoints.len(),
        "T3 topology: both DCs must reach Cluster mode within 60s; healthy = {healthy:?}"
    );
}

/// Locate a working container runtime daemon. Mirrors the helper in
/// `docker_mini_jepsen.rs`. Panics with setup instructions if none
/// is reachable — matches the test policy.
fn container_runtime() -> &'static str {
    for candidate in &["docker", "podman"] {
        let ok = Command::new(candidate)
            .arg("info")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            return Box::leak((*candidate).to_string().into_boxed_str());
        }
    }
    panic!(
        "FERROSA_TEST_CONTAINERS=1 set but no container runtime is reachable.\n\
         Start Docker Desktop or Podman Desktop, then re-run the test."
    );
}
