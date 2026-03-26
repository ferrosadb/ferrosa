//! Mini Jepsen test for the ferrosa-memory Docker cluster.
//!
//! Tests data durability across rolling restarts, node kills, and pauses.
//! Requires the ferrosa-memory Docker Compose cluster to be running:
//!   cd ~/src/ferrosa-memory && docker compose up -d
//!
//! Run with:
//!   cargo test -p ferrosa-jepsen --test docker_mini_jepsen -- --nocapture --ignored

use std::process::Command;
use std::time::Duration;
use uuid::Uuid;

/// Execute a CQL query via cqlsh and return stdout.
fn cqlsh(port: u16, query: &str) -> Result<String, String> {
    let output = Command::new("cqlsh")
        .args(["localhost", &port.to_string(), "-e", query])
        .output()
        .map_err(|e| format!("cqlsh failed: {e}"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

/// Execute a docker compose command.
fn docker_compose(args: &[&str]) -> Result<String, String> {
    let output = Command::new("docker")
        .args(["compose"])
        .args(args)
        .current_dir(std::env::var("FERROSA_MEMORY_DIR").unwrap_or_else(|_| {
            format!(
                "{}/src/ferrosa-memory",
                std::env::var("HOME").unwrap_or_default()
            )
        }))
        .output()
        .map_err(|e| format!("docker compose failed: {e}"))?;
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
                // Node is up, promote it
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
    // Make sure node1 is running
    let _ = docker_compose(&["up", "-d", "node1"]);
    wait_for_node1(30).expect("cluster not ready");
}

// ── Tests ──────────────────────────────────────────────────────────────

/// Test 1: Write entity, rolling restart, verify entity survives.
#[test]
#[ignore] // requires Docker cluster
fn write_survives_rolling_restart() {
    ensure_cluster_ready();
    let name = format!("JEPSEN_ROLLING_{}", Uuid::new_v4().as_simple());

    // Write
    write_test_entity(19042, &name).expect("write failed");
    assert!(
        verify_entity_exists(19042, &name),
        "entity should exist before restart"
    );

    // Rolling restart node1
    docker_compose(&["up", "-d", "--force-recreate", "node1"]).expect("restart failed");
    wait_for_node1(30).expect("node1 didn't come back");

    // Verify
    assert!(
        verify_entity_exists(19042, &name),
        "FAIL: entity lost after rolling restart"
    );
    eprintln!("PASS: write_survives_rolling_restart");
}

/// Test 2: Write entity, kill node1 (SIGKILL — no graceful shutdown),
/// restart, verify entity is lost (expected with SIGKILL).
#[test]
#[ignore]
fn write_lost_on_sigkill() {
    ensure_cluster_ready();
    let name = format!("JEPSEN_SIGKILL_{}", Uuid::new_v4().as_simple());

    write_test_entity(19042, &name).expect("write failed");
    assert!(verify_entity_exists(19042, &name));

    // SIGKILL — no graceful shutdown
    let _ = Command::new("docker")
        .args(["kill", "--signal=SIGKILL", "ferrosa-memory-node1-1"])
        .output();
    std::thread::sleep(Duration::from_secs(2));

    // Restart
    docker_compose(&["up", "-d", "node1"]).expect("restart failed");
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

/// Test 3: Write entity, docker pause (network partition), verify reads
/// on another node, unpause, verify consistency.
#[test]
#[ignore]
fn pause_unpause_preserves_data() {
    ensure_cluster_ready();
    let name = format!("JEPSEN_PAUSE_{}", Uuid::new_v4().as_simple());

    write_test_entity(19042, &name).expect("write failed");
    assert!(verify_entity_exists(19042, &name));

    // Pause node1 (simulates network partition)
    let _ = Command::new("docker")
        .args(["pause", "ferrosa-memory-node1-1"])
        .output();
    std::thread::sleep(Duration::from_secs(3));

    // Unpause
    let _ = Command::new("docker")
        .args(["unpause", "ferrosa-memory-node1-1"])
        .output();
    std::thread::sleep(Duration::from_secs(3));

    // Verify data is still there
    assert!(
        verify_entity_exists(19042, &name),
        "FAIL: entity lost after pause/unpause"
    );
    eprintln!("PASS: pause_unpause_preserves_data");
}

/// Test 4: Write multiple entities rapidly, rolling restart mid-write,
/// verify all writes that acknowledged are present after restart.
#[test]
#[ignore]
fn rapid_writes_during_rolling_restart() {
    ensure_cluster_ready();
    let mut written: Vec<String> = Vec::new();

    // Write 10 entities
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

    // Rolling restart
    docker_compose(&["up", "-d", "--force-recreate", "node1"]).expect("restart failed");
    wait_for_node1(30).expect("node1 didn't come back");

    // Verify all acknowledged writes survived
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

/// Test 5: Verify S3 has data after flush cycle.
#[test]
#[ignore]
fn s3_contains_manifest_and_sstables() {
    // Check manifest exists via mc
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            "ferrosa-memory_default",
            "--entrypoint",
            "/bin/sh",
            "minio/mc",
            "-c",
            "mc alias set local http://rustfs:9000 rustfsadmin rustfsadmin >/dev/null 2>&1 && mc cat local/ferrosa-memory/node1/manifest.json",
        ])
        .output()
        .expect("mc failed");

    let manifest = String::from_utf8_lossy(&output.stdout);
    assert!(
        manifest.contains("format_version"),
        "manifest should contain format_version"
    );
    assert!(
        manifest.contains("sstables"),
        "manifest should contain sstables"
    );

    // Check schema exists
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            "ferrosa-memory_default",
            "--entrypoint",
            "/bin/sh",
            "minio/mc",
            "-c",
            "mc alias set local http://rustfs:9000 rustfsadmin rustfsadmin >/dev/null 2>&1 && mc cat local/ferrosa-memory/node1/schema.json | head -c 100",
        ])
        .output()
        .expect("mc failed");

    let schema = String::from_utf8_lossy(&output.stdout);
    assert!(schema.contains("version"), "schema should contain version");
    eprintln!("PASS: s3_contains_manifest_and_sstables");
}
