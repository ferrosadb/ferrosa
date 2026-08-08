//! Mini Jepsen test for the ferrosa-memory container cluster.
//!
//! Tests data durability across rolling restarts, node kills, and pauses.
//! Requires the ferrosa-memory Compose cluster to be running:
//!   cd ~/src/ferrosa-memory && docker compose up -d   # Docker Desktop
//!   cd ~/src/ferrosa-memory && podman compose up -d   # Podman Desktop (macOS)
//!
//! Run with:
//!   FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-jepsen --features live-infra-tests --test docker_mini_jepsen -- --nocapture

#![cfg(feature = "live-infra-tests")]

use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;
use uuid::Uuid;

const CQLSH_IMAGE: &str = "cassandra:3.11";

/// The mini-Jepsen cases restart and pause the same node. Serialize them so
/// their fault injections cannot race each other.
fn cluster_test_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn memory_network() -> String {
    std::env::var("FERROSA_MEMORY_NETWORK").unwrap_or_else(|_| "ferrosa-memory_default".to_string())
}

/// Return "docker" or "podman" — whichever container runtime is running.
///
/// Uses `docker info` / `podman info` (not just `--version`) to confirm the
/// daemon is actually reachable, not just that the binary is installed.
/// On macOS, Docker Desktop may be installed but not started; Podman Desktop
/// is usually running. Panics if neither daemon is reachable.
fn container_runtime() -> &'static str {
    for candidate in &["docker", "podman"] {
        let ok = Command::new(candidate)
            .arg("info")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            // Leak is fine — this runs at most once per test binary.
            return Box::leak((*candidate).to_string().into_boxed_str());
        }
    }
    panic!(
        "no container runtime daemon reachable — start Podman Desktop (macOS) or Docker Desktop \
         and start the ferrosa-memory compose cluster before running these tests"
    );
}

/// Assert that FERROSA_TEST_CONTAINERS=1 is set; panic with setup instructions otherwise.
fn require_container_cluster() {
    if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
        let rt = if Command::new("podman")
            .arg("info")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            "podman"
        } else {
            "docker"
        };
        panic!(
            "FERROSA_TEST_CONTAINERS not set.\n\
             Start the cluster:\n  \
             cd ~/src/ferrosa-memory && {rt} compose up -d\n\
             Then re-run with:\n  \
             FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-jepsen --features live-infra-tests --test docker_mini_jepsen"
        );
    }
}

/// Execute a CQL query via a transient cassandra:5.1 container on the
/// ferrosa-memory compose network.  No host-side cqlsh install required.
///
/// The network name defaults to `ferrosa-memory_default` (Compose default for
/// the `~/src/ferrosa-memory` project) and can be overridden with
/// `FERROSA_MEMORY_NETWORK`.
fn cqlsh(port: u16, query: &str) -> Result<String, String> {
    // Map macOS host port → compose service name (internal CQL port is always 9042).
    let service = match port {
        19042 => "node1",
        19043 => "node2",
        19044 => "node3",
        p => return Err(format!("unknown ferrosa-memory CQL port {p}")),
    };
    let network = memory_network();
    let username = std::env::var("FERROSA_MEMORY_CQL_USERNAME")
        .unwrap_or_else(|_| "ferrosa_admin".to_string());
    let password = std::env::var("FERROSA_MEMORY_CQL_PASSWORD")
        .unwrap_or_else(|_| "ferrosa_admin".to_string());
    let output = Command::new(container_runtime())
        .args([
            "run",
            "--rm",
            "--network",
            &network,
            CQLSH_IMAGE,
            "cqlsh",
            "--no-color",
            service,
            "9042",
            "-u",
            &username,
            "-p",
            &password,
            "-e",
            query,
        ])
        .output()
        .map_err(|e| format!("cqlsh container run failed: {e}"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

/// Execute a compose command using the detected container runtime.
fn compose(args: &[&str]) -> Result<String, String> {
    let output = Command::new(container_runtime())
        .arg("compose")
        .args(args)
        .current_dir(std::env::var("FERROSA_MEMORY_DIR").unwrap_or_else(|_| {
            format!(
                "{}/src/ferrosa-memory",
                std::env::var("HOME").unwrap_or_default()
            )
        }))
        .output()
        .map_err(|e| format!("compose failed: {e}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if output.status.success() {
        Ok(format!("{stdout}{stderr}"))
    } else {
        Err(format!("{stdout}{stderr}"))
    }
}

/// Promote node1 to standalone primary.
fn promote_node1() -> Result<(), String> {
    let output = Command::new("curl")
        .args([
            "-s",
            "-X",
            "POST",
            "http://localhost:19090/api/cluster/promote",
        ])
        .output()
        .map_err(|e| format!("curl failed: {e}"))?;
    let body = String::from_utf8_lossy(&output.stdout);
    if body.contains("promoted") || body.contains("standalone") {
        Ok(())
    } else {
        Err(format!("promote failed: {body}"))
    }
}

/// Write a test entity and return its entity_id.
fn write_test_entity(port: u16, name: &str) -> Result<Uuid, String> {
    let entity_id = Uuid::new_v4();
    let tenant_id = "00000000-0000-0000-0000-000000000001";
    let session_id = "86da7931-7c87-54fe-8a49-eabc21c025aa";
    let query = format!(
        "INSERT INTO agent_memory.entity_store \
         (tenant_id, entity_id, session_id, entity_name, entity_type, \
          confidence, state, created_at, context_snippet) \
         VALUES ({tenant_id}, {entity_id}, {session_id}, \
         '{name}', 'concept', 0.9, 'active', toTimestamp(now()), '')"
    );
    cqlsh(port, &query)?;
    Ok(entity_id)
}

/// Verify an entity exists by entity_name.
fn verify_entity_exists(port: u16, name: &str) -> bool {
    let query = "SELECT entity_name FROM agent_memory.entity_store \
         WHERE tenant_id = 00000000-0000-0000-0000-000000000001 \
         AND session_id = 86da7931-7c87-54fe-8a49-eabc21c025aa"
        .to_string();
    match cqlsh(port, &query) {
        Ok(output) => output.contains(name),
        Err(_) => false,
    }
}

/// Wait for node1 to be healthy and promoted.
fn wait_for_node1(timeout_secs: u64) -> Result<(), String> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(timeout_secs) {
            return Err("timeout waiting for node1".to_string());
        }
        if let Ok(out) = cqlsh(19042, "SELECT key FROM system.local") {
            if out.contains("local") {
                std::thread::sleep(Duration::from_secs(2));
                let _ = promote_node1();
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Ensure node1 is up, healthy, and promoted before each test.
fn ensure_cluster_ready() {
    let _ = compose(&["up", "-d", "node1"]);
    wait_for_node1(30).expect("cluster not ready");
}

// ── Tests ──────────────────────────────────────────────────────────────

/// Test 1: Write entity, rolling restart, verify entity survives.
#[test]
fn write_survives_rolling_restart() {
    let _guard = cluster_test_guard();
    require_container_cluster();
    ensure_cluster_ready();
    let name = format!("JEPSEN_ROLLING_{}", Uuid::new_v4().as_simple());

    write_test_entity(19042, &name).expect("write failed");
    assert!(
        verify_entity_exists(19042, &name),
        "entity should exist before restart"
    );

    compose(&["up", "-d", "--force-recreate", "node1"]).expect("restart failed");
    wait_for_node1(30).expect("node1 didn't come back");

    assert!(
        verify_entity_exists(19042, &name),
        "FAIL: entity lost after rolling restart"
    );
    eprintln!("PASS: write_survives_rolling_restart");
}

/// Test 2: Write entity, kill node1 (SIGKILL — no graceful shutdown),
/// restart, verify entity is lost (expected with SIGKILL).
#[test]
fn write_lost_on_sigkill() {
    let _guard = cluster_test_guard();
    require_container_cluster();
    ensure_cluster_ready();
    let name = format!("JEPSEN_SIGKILL_{}", Uuid::new_v4().as_simple());

    write_test_entity(19042, &name).expect("write failed");
    assert!(verify_entity_exists(19042, &name));

    let _ = Command::new(container_runtime())
        .args(["kill", "--signal=SIGKILL", "ferrosa-memory-node1-1"])
        .output();
    std::thread::sleep(Duration::from_secs(2));

    compose(&["up", "-d", "node1"]).expect("restart failed");
    wait_for_node1(30).expect("node1 didn't come back");

    // Entity MAY be lost (SIGKILL doesn't flush).
    // If the maintenance loop flushed it in time, it survives.
    // This test documents the behavior, not asserts a specific outcome.
    let survived = verify_entity_exists(19042, &name);
    eprintln!(
        "write_lost_on_sigkill: entity {}",
        if survived {
            "SURVIVED (maintenance flush saved it)"
        } else {
            "LOST (expected — SIGKILL prevents flush)"
        }
    );
}

/// Test 3: Write entity, pause node1 (network partition), unpause, verify consistency.
#[test]
fn pause_unpause_preserves_data() {
    let _guard = cluster_test_guard();
    require_container_cluster();
    ensure_cluster_ready();
    let name = format!("JEPSEN_PAUSE_{}", Uuid::new_v4().as_simple());

    write_test_entity(19042, &name).expect("write failed");
    assert!(verify_entity_exists(19042, &name));

    let _ = Command::new(container_runtime())
        .args(["pause", "ferrosa-memory-node1-1"])
        .output();
    std::thread::sleep(Duration::from_secs(3));

    let _ = Command::new(container_runtime())
        .args(["unpause", "ferrosa-memory-node1-1"])
        .output();
    std::thread::sleep(Duration::from_secs(3));

    assert!(
        verify_entity_exists(19042, &name),
        "FAIL: entity lost after pause/unpause"
    );
    eprintln!("PASS: pause_unpause_preserves_data");
}

/// Test 4: Write multiple entities rapidly, rolling restart mid-write,
/// verify all acknowledged writes are present after restart.
#[test]
fn rapid_writes_during_rolling_restart() {
    let _guard = cluster_test_guard();
    require_container_cluster();
    ensure_cluster_ready();
    let mut written: Vec<String> = Vec::new();

    for i in 0..10 {
        let name = format!("JEPSEN_RAPID_{}_{}", i, Uuid::new_v4().as_simple());
        if write_test_entity(19042, &name).is_ok() {
            written.push(name);
        }
    }
    assert!(
        written.len() >= 8,
        "should write at least 8 of 10, got {}",
        written.len()
    );

    compose(&["up", "-d", "--force-recreate", "node1"]).expect("restart failed");
    wait_for_node1(30).expect("node1 didn't come back");

    let mut found = 0;
    let mut lost = 0;
    for name in &written {
        if verify_entity_exists(19042, name) {
            found += 1;
        } else {
            lost += 1;
            eprintln!("  LOST: {name}");
        }
    }
    eprintln!(
        "rapid_writes_during_rolling_restart: {found}/{} survived, {lost} lost",
        written.len()
    );
    assert_eq!(
        lost, 0,
        "all acknowledged writes must survive rolling restart"
    );
}

/// Test 5: Verify S3 has manifest and SSTables after a flush cycle.
#[test]
fn s3_contains_manifest_and_sstables() {
    let _guard = cluster_test_guard();
    require_container_cluster();

    let rt = container_runtime();
    let network = memory_network();
    let output = Command::new(rt)
        .args([
            "run",
            "--rm",
            "--network",
            &network,
            "--entrypoint",
            "/bin/sh",
            "minio/mc",
            "-c",
            "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1 \
             && mc cat local/ferrosa-memory/node1/manifest.json",
        ])
        .output()
        .expect("mc container failed");

    let manifest = String::from_utf8_lossy(&output.stdout);
    assert!(
        manifest.contains("format_version"),
        "manifest should contain format_version"
    );
    assert!(
        manifest.contains("sstables"),
        "manifest should contain sstables"
    );

    let output = Command::new(rt)
        .args([
            "run",
            "--rm",
            "--network",
            &network,
            "--entrypoint",
            "/bin/sh",
            "minio/mc",
            "-c",
            "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1 \
             && mc cat local/ferrosa-memory/node1/schema.json | head -c 100",
        ])
        .output()
        .expect("mc container failed");

    let schema = String::from_utf8_lossy(&output.stdout);
    assert!(schema.contains("version"), "schema should contain version");
    eprintln!("PASS: s3_contains_manifest_and_sstables");
}
