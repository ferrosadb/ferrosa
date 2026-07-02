//! Gated live-infra entry for the fly.io multi-node streaming-scan memory
//! harness (spec: `specs/proposed/multi-node-streaming-test-harness.md`,
//! Part A/B; tracks `t_3fc6be3c` + `t_ee98faa0`).
//!
//! This drives REAL fly machines (distinct private IPs, real network streaming)
//! — the environment the parked-replica in-process tests structurally cannot
//! reproduce. It provisions N>=3 ferrosa nodes at the intentional 2 GiB cap,
//! seeds a large `entity_store`, runs the Part B probe suite (FTS content scan,
//! multi-page projected scan, abandoned-page cancel, slow consumer, viz
//! SnapshotStreamEnd, integrity, ORDER-BY-spill cancel), asserts every node
//! stays under 2 GiB, and tears down.
//!
//! Gating (repo test policy):
//!   * behind the `live-infra-tests` crate feature, so the default `cargo test`
//!     never compiles/reports this body, AND
//!   * behind `FERROSA_TEST_FLY=1`; with the feature ON but the env unset (or
//!     `flyctl` missing) it `panic!`s with setup instructions rather than
//!     silently passing.
//!
//! It NEVER provisions autonomously in CI: it shells out to
//! `deploy/fly-stream-scan/run-all.sh --i-will-pay`, which itself only bills when
//! that explicit flag is present. The in-process
//! `replica_scan_serialization_memory_bound.rs` test is what gates the fix in CI;
//! this is the on-demand live confirmation for PR #237.
//!
//! Manual run:
//! ```bash
//! FERROSA_TEST_FLY=1 cargo test -p ferrosa-cluster --features live-infra-tests \
//!   --test fly_stream_scan_live -- --nocapture
//! ```

#[cfg(feature = "live-infra-tests")]
#[test]
fn fly_multi_node_streaming_scan_stays_under_2gib() {
    use std::path::PathBuf;
    use std::process::Command;

    // Panic-on-missing-infra: feature ON but no explicit opt-in => loud setup
    // instructions, never a false pass.
    if std::env::var("FERROSA_TEST_FLY").as_deref() != Ok("1") {
        panic!(
            "live-infra-tests is enabled but FERROSA_TEST_FLY is not set to 1.\n\
             This test provisions REAL fly.io machines and BILLS MONEY. To run it:\n\
             1. Install + auth flyctl (https://fly.io/docs/flyctl/install/).\n\
             2. Review deploy/fly-stream-scan/README.md and config.env.\n\
             3. FERROSA_TEST_FLY=1 cargo test -p ferrosa-cluster \\\n\
                  --features live-infra-tests --test fly_stream_scan_live -- --nocapture\n\
             The harness NEVER raises the 2 GiB per-node mem_limit — bounded memory is the assertion."
        );
    }

    if which_flyctl().is_none() {
        panic!(
            "FERROSA_TEST_FLY=1 but flyctl is not on PATH. Install it: \
             https://fly.io/docs/flyctl/install/"
        );
    }

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = manifest
        .join("..")
        .join("deploy")
        .join("fly-stream-scan")
        .join("run-all.sh");
    assert!(
        script.exists(),
        "harness orchestrator missing: {}",
        script.display()
    );

    // --i-will-pay: the explicit billing opt-in. run-all.sh provisions -> seeds
    // -> probes -> tears down, and returns non-zero if any node exceeds 2 GiB or
    // any probe hangs/fails. Teardown always runs (trap in run-all.sh).
    let status = Command::new("bash")
        .arg(&script)
        .arg("--i-will-pay")
        .status()
        .expect("failed to spawn fly stream-scan harness");

    assert!(
        status.success(),
        "fly streaming-scan harness failed (a node exceeded 2 GiB, a probe hung, or teardown \
         failed). Inspect the harness output above and confirm all machines were destroyed."
    );
}

#[cfg(feature = "live-infra-tests")]
fn which_flyctl() -> Option<std::path::PathBuf> {
    let out = std::process::Command::new("bash")
        .arg("-lc")
        .arg("command -v flyctl")
        .output()
        .ok()?;
    if out.status.success() {
        let p = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if p.is_empty() {
            None
        } else {
            Some(std::path::PathBuf::from(p))
        }
    } else {
        None
    }
}
