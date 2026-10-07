//! T3 — assert the nightly's *permitted exclusions* and nothing else.
//!
//! The defect that prompted this work: `nightly-fuzz.yml` carried a
//! hand-maintained `--skip` list that drifted from `ci.yml`'s. When
//! `replicated_jsonb_index_builds_on_every_node` landed its skip was added to
//! `ci.yml` only, so the nightly built that test with `--all-features`, hit its
//! own live-infra guard and died at ~test 40 of ~500 — for 8 consecutive nights.
//! The fuzz session silently stopped running, and nobody noticed because a
//! permanently-red workflow is noise, not signal.
//!
//! This test makes the set of tests the nightly *cannot* run explicit and
//! asserted. The contract it enforces:
//!
//! 1. The comprehensive run step has **no `--exclude`** at all, and its
//!    `--skip` tokens are **exactly a subset** of [`PERMITTED_EXCLUSIONS`].
//!    Any other `--skip` — the old convenience skips, or a fresh
//!    make-it-green skip — fails this test. (Control (a).)
//! 2. Every permitted entry names its **blocker** and a **`file:line`**, and
//!    the entry must still match a **real test**: the cited file must exist and
//!    still declare `fn <name>`. A permitted entry whose test is gone (its
//!    blocker fixed) is a **stale entry** and fails. (Control (c).)
//! 3. No `#[ignore]` anywhere in the workspace is used as an *environment
//!    gate*. `CLAUDE.md` forbids ignoring a test for being slow or
//!    env-dependent; the only legitimate `#[ignore]` is the pre-existing
//!    cluster-gated one, and its reason must not name a `FERROSA_TEST_*`
//!    prerequisite.
//!
//! Removing a `--skip` for a still-blocked test does **not** fail here on
//! purpose: that test then runs, hits its `panic!`-with-setup-instructions
//! guard, and turns the nightly **red**. That is control (b) — the loud failure
//! belongs to the run, not to this assertion. So this test pins the *ceiling*
//! (what may be skipped) and the *freshness* (each skip still has a live
//! blocker), never the *floor*.

use std::path::{Path, PathBuf};

/// The exact set of tests the comprehensive nightly may skip, each with the
/// physical blocker that makes it impossible on a GitHub-hosted runner (or, for
/// `tier_multi_dc_one_hour_bank_workload`, the structural defect that makes it
/// unrunnable outside its own workflow), the source that proves it, and the
/// marker that must still be present there.
///
/// Verified against the tree that carries T4's LazyFS retirement. The former
/// `promote_dir_fsync_lazyfs_crash_loses_neither_copy` entry is **absent**: T4
/// deleted the test (window E' is closed by the T-022 intent protocol and the
/// LazyFS probe was vacuous), so any token naming it is a stale entry.
struct Exclusion {
    /// The libtest filter token as it appears after `--skip` in the workflow.
    token: &'static str,
    /// Why it cannot run. Shown on failure.
    blocker: &'static str,
    /// The source file that declares the test / holds the blocker.
    evidence_file: &'static str,
    /// A `file:line` pointer for the reader, recorded as documentation.
    evidence_line: u32,
    /// A marker that must still exist in `evidence_file`; for env-gated tests
    /// this is the `FERROSA_TEST_*` variable, so a test that quietly loses its
    /// gate (and could start silently passing) is caught.
    blocker_marker: &'static str,
}

const PERMITTED_EXCLUSIONS: &[Exclusion] = &[
    // ── Firecracker / VM: hosted runners have no KVM ──────────────────────
    Exclusion {
        token: "ssh_execute_command",
        blocker: "needs a pre-provisioned Firecracker VM with sshd (KVM); hosted runners have none",
        evidence_file: "ferrosa-jepsen/src/ssh.rs",
        evidence_line: 237,
        blocker_marker: "FERROSA_TEST_FIRECRACKER",
    },
    Exclusion {
        token: "ssh_upload_file",
        blocker: "needs a pre-provisioned Firecracker VM with sshd (KVM); hosted runners have none",
        evidence_file: "ferrosa-jepsen/src/ssh.rs",
        evidence_line: 269,
        blocker_marker: "FERROSA_TEST_FIRECRACKER",
    },
    Exclusion {
        token: "provision_single_vm",
        blocker: "needs the Firecracker binary + root + KVM; hosted runners have none",
        evidence_file: "ferrosa-jepsen/src/firecracker.rs",
        evidence_line: 327,
        blocker_marker: "FERROSA_TEST_FIRECRACKER",
    },
    Exclusion {
        token: "provision_t1_cluster",
        blocker: "needs a pre-provisioned Firecracker cluster (KVM); hosted runners have none",
        evidence_file: "ferrosa-jepsen/src/cluster.rs",
        evidence_line: 222,
        blocker_marker: "FERROSA_TEST_FIRECRACKER",
    },
    // ── Live nemesis trio: inject faults over SSH into Firecracker nodes ──
    // NOTE: these three tokens collide by exact name with hermetic in-process
    // tests in `ferrosa-cluster/tests/accord_nemesis.rs`; the nightly's
    // name-collision guard step re-runs that binary with no exclusions.
    Exclusion {
        token: "disk_fail_no_phantom_commits",
        blocker: "live variant injects disk faults over SSH into Firecracker nodes; its hermetic twin is re-run by the name-collision guard",
        evidence_file: "ferrosa-jepsen/tests/nemesis_correctness.rs",
        evidence_line: 37,
        blocker_marker: "FERROSA_TEST_FIRECRACKER",
    },
    Exclusion {
        token: "packet_reorder_linearizability",
        blocker: "live variant injects network faults over SSH into Firecracker nodes; its hermetic twin is re-run by the name-collision guard",
        evidence_file: "ferrosa-jepsen/tests/nemesis_correctness.rs",
        evidence_line: 87,
        blocker_marker: "FERROSA_TEST_FIRECRACKER",
    },
    Exclusion {
        token: "lwt_batch_atomicity_all_nemeses",
        blocker: "live variant injects SSH faults into Firecracker nodes; its hermetic twin is re-run by the name-collision guard",
        evidence_file: "ferrosa-jepsen/tests/nemesis_correctness.rs",
        evidence_line: 134,
        blocker_marker: "FERROSA_TEST_FIRECRACKER",
    },
    // ── Structural defect, not a capability gap ───────────────────────────
    Exclusion {
        token: "tier_multi_dc_one_hour_bank_workload",
        blocker: "STRUCTURAL DEFECT: panics unconditionally outside its own 1h workflow (jepsen-multi-dc-nightly.yml); it is not a runner-capability gap and should be feature-gated/relocated so the generic run cannot reach it",
        evidence_file: "ferrosa-jepsen/tests/tier_multi_dc.rs",
        evidence_line: 62,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    // ── ferrosa-memory compose stack (19042-19044) not provisioned here ───
    Exclusion {
        token: "write_survives_rolling_restart",
        blocker: "needs the ferrosa-memory compose cluster (19042-19044), which this job does not provision",
        evidence_file: "ferrosa-jepsen/tests/docker_mini_jepsen.rs",
        evidence_line: 224,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "write_lost_on_sigkill",
        blocker: "needs the ferrosa-memory compose cluster (19042-19044), which this job does not provision",
        evidence_file: "ferrosa-jepsen/tests/docker_mini_jepsen.rs",
        evidence_line: 249,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "pause_unpause_preserves_data",
        blocker: "needs the ferrosa-memory compose cluster (19042-19044), which this job does not provision",
        evidence_file: "ferrosa-jepsen/tests/docker_mini_jepsen.rs",
        evidence_line: 282,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "rapid_writes_during_rolling_restart",
        blocker: "needs the ferrosa-memory compose cluster (19042-19044), which this job does not provision",
        evidence_file: "ferrosa-jepsen/tests/docker_mini_jepsen.rs",
        evidence_line: 311,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "s3_contains_manifest_and_sstables",
        blocker: "needs the ferrosa-memory compose cluster (19042-19044), which this job does not provision",
        evidence_file: "ferrosa-jepsen/tests/docker_mini_jepsen.rs",
        evidence_line: 354,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    // ── RustFS S3 on :29000, not provisioned here ─────────────────────────
    Exclusion {
        token: "ucs_load_s3_write_heavy",
        blocker: "needs RustFS S3 on :29000 (tests/docker-compose.compaction-test.yml), which this job does not provision",
        evidence_file: "ferrosa-loadgen/tests/ucs_load_s3_test.rs",
        evidence_line: 81,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "ucs_load_s3_balanced",
        blocker: "needs RustFS S3 on :29000 (tests/docker-compose.compaction-test.yml), which this job does not provision",
        evidence_file: "ferrosa-loadgen/tests/ucs_load_s3_test.rs",
        evidence_line: 122,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "ucs_load_s3_read_heavy",
        blocker: "needs RustFS S3 on :29000 (tests/docker-compose.compaction-test.yml), which this job does not provision",
        evidence_file: "ferrosa-loadgen/tests/ucs_load_s3_test.rs",
        evidence_line: 155,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    // ── PostgreSQL fault schedule lives in ci.yml's dedicated job ─────────
    Exclusion {
        token: "postgres_transactions_are_strictly_serializable",
        blocker: "needs FERROSA_TEST_POSTGRES_URLS + the fault schedule that only ci.yml's postgres-jepsen-fault job provides",
        evidence_file: "ferrosa-jepsen/tests/postgres_strict_serializable.rs",
        evidence_line: 44,
        blocker_marker: "FERROSA_TEST_POSTGRES_URLS",
    },
    // ── Tests that would tear down / rebuild the shared harness ───────────
    Exclusion {
        token: "t3_topology_brings_up_two_dcs",
        blocker: "brings up and tears down the shared T3 dual-DC stack the job already owns",
        evidence_file: "ferrosa-jepsen/tests/t3_topology.rs",
        evidence_line: 257,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "orchestrator_docker_cluster_provision",
        blocker: "provisions a competing 3-node compose cluster on the same shared ports",
        evidence_file: "ferrosa-jepsen/src/docker_provision.rs",
        evidence_line: 596,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "orchestrator_cluster_teardown",
        blocker: "tears down the shared compose cluster (compose down -v) other tests in the run depend on",
        evidence_file: "ferrosa-jepsen/src/docker_provision.rs",
        evidence_line: 623,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "cassandra_reads_compacted_sstable_from_s3",
        blocker: "needs MinIO + Cassandra 5; compaction-cassandra.yml binds host:9043, already owned by node2 in this job",
        evidence_file: "ferrosa-storage/src/engine.rs",
        evidence_line: 29451,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    Exclusion {
        token: "compaction_end_to_end_pipeline",
        blocker: "needs MinIO + Cassandra 5; compaction-cassandra.yml binds host:9043, already owned by node2 in this job",
        evidence_file: "ferrosa-storage/src/engine.rs",
        evidence_line: 29815,
        blocker_marker: "FERROSA_TEST_CONTAINERS",
    },
    // ── Captured SSTables are explicitly not checked in ───────────────────
    Exclusion {
        token: "real_typed_edges_paged_scan_delivers_every_distinct_row",
        blocker: "needs captured agent_memory.typed_edges SSTables (FERROSA_TEST_TYPED_EDGES_DIR), explicitly not checked in",
        evidence_file: "ferrosa-cluster/src/coordinator/range_read_stream.rs",
        evidence_line: 3804,
        blocker_marker: "FERROSA_TEST_TYPED_EDGES_DIR",
    },
    Exclusion {
        token: "count_range_metadata_merger_dedups_real_typed_edges_sstables",
        blocker: "needs captured agent_memory.typed_edges SSTables (FERROSA_TEST_TYPED_EDGES_DIR), explicitly not checked in",
        evidence_file: "ferrosa-storage/src/store.rs",
        evidence_line: 12312,
        blocker_marker: "FERROSA_TEST_TYPED_EDGES_DIR",
    },
    // ── Provisions real Fly.io machines and bills money ───────────────────
    Exclusion {
        token: "fly_multi_node_streaming_scan_stays_under_2gib",
        blocker: "provisions REAL Fly.io machines and bills money (FERROSA_TEST_FLY); never autonomous",
        evidence_file: "ferrosa-cluster/tests/fly_stream_scan_live.rs",
        evidence_line: 34,
        blocker_marker: "FERROSA_TEST_FLY",
    },
];

/// The only file allowed to declare a real `#[ignore]` attribute. It is
/// pre-existing, cluster-gated (not an env gate), and PR CI's `integration` job
/// runs it via `--ignored` while `nightly-slow-tests.yml` re-runs it.
const ALLOWED_IGNORE_FILES: &[&str] = &["ferrosa-cql/tests/fts_live_cluster.rs"];

/// Convenience skips that used to hide coverage and must never return. Listed
/// explicitly so a regression reads clearly even though subset-equality against
/// `PERMITTED_EXCLUSIONS` already catches them.
const FORBIDDEN_SKIP_FRAGMENTS: &[&str] = &[
    "::slow::",
    "binary_",
    "flush_2000",
    "many_flushes",
    "differential_oracle",
    "replicated_jsonb_index_builds_on_every_node",
];

fn repo_root() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .expect("CARGO_MANIFEST_DIR must be set under cargo test");
    manifest_dir
        .parent()
        .expect("ferrosa-jepsen must live under the repo root")
        .to_path_buf()
}

fn nightly_yaml() -> String {
    let path = repo_root().join(".github/workflows/nightly-fuzz.yml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The body of the named step, up to the next `- name:` / `- uses:`.
fn step_body<'a>(yaml: &'a str, step_name: &str) -> &'a str {
    let marker = format!("- name: {step_name}");
    let start = yaml
        .find(&marker)
        .unwrap_or_else(|| panic!("nightly-fuzz.yml has no step `{step_name}`"));
    let rest = &yaml[start..];
    let end = rest
        .find("\n      - name:")
        .into_iter()
        .chain(rest.find("\n      - uses:"))
        .min()
        .unwrap_or(rest.len());
    &rest[..end]
}

/// Every `--skip <token>` token in a step body.
fn skip_tokens(step: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = step;
    while let Some(pos) = rest.find("--skip ") {
        let after = &rest[pos + "--skip ".len()..];
        let token: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == ':')
            .collect();
        rest = &after[token.len()..];
        if !token.is_empty() {
            out.push(token);
        }
    }
    out
}

fn exclude_tokens(step: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = step;
    while let Some(pos) = rest.find("--exclude ") {
        let after = &rest[pos + "--exclude ".len()..];
        let token: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
            .collect();
        rest = &after[token.len()..];
        if !token.is_empty() {
            out.push(token);
        }
    }
    out
}

/// Recursively collect `*.rs` paths under `root`, skipping dot-directories
/// (`.git`, `.target-*`), `target/`, and any nested checkout (a subdirectory
/// that is itself a git worktree / repo, e.g. `.wt-*` or `ferrosa-*-worktrees/`).
/// A fresh CI checkout has none of these, but the primary dev checkout does, and
/// descending into a sibling worktree would report its copies as this tree's.
fn collect_rs_files(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if name.starts_with('.') || name == "target" || name == "node_modules" {
                continue;
            }
            // Do not descend into a nested checkout / worktree.
            if path.join(".git").exists() {
                continue;
            }
            collect_rs_files(&path, out);
        } else if name.ends_with(".rs") {
            out.push(path);
        }
    }
}

#[test]
fn the_comprehensive_run_step_skips_only_permitted_exclusions() {
    let yaml = nightly_yaml();
    let step = step_body(&yaml, "Run the comprehensive workspace test suite");

    // The scope contract: the whole workspace, every feature, every target,
    // reporting every failure — this is what makes it the most thorough run.
    for required in [
        "--all-features",
        "--workspace",
        "--all-targets",
        "--no-fail-fast",
    ] {
        assert!(
            step.contains(required),
            "the nightly run step must keep `{required}` (it is the comprehensive run); step was:\n{step}"
        );
    }

    // No package may be excluded: ferrosa-jepsen and ferrosa-loadgen are the
    // whole point of this job (PR CI excludes them).
    let excludes = exclude_tokens(step);
    assert!(
        excludes.is_empty(),
        "the comprehensive run step must not `--exclude` any package; found: {excludes:?}. \
         ferrosa-jepsen and ferrosa-loadgen are the crates this job exists to run."
    );

    let permitted: std::collections::BTreeSet<&str> =
        PERMITTED_EXCLUSIONS.iter().map(|e| e.token).collect();
    let actual: std::collections::BTreeSet<String> = skip_tokens(step).into_iter().collect();

    // (Control a) Any `--skip` not on the permitted list fails here. This is
    // the drift the whole redesign exists to prevent.
    let unexplained: Vec<&String> = actual
        .iter()
        .filter(|t| !permitted.contains(t.as_str()))
        .collect();
    assert!(
        unexplained.is_empty(),
        "the nightly skips tests that are not on the permitted-exclusion list: {unexplained:?}\n\
         Every `--skip` must name a physical blocker (or, for \
         tier_multi_dc_one_hour_bank_workload, a structural defect) recorded in \
         PERMITTED_EXCLUSIONS. A skip added to make the job green hides a real \
         failure — give the test what it needs instead."
    );

    // The old convenience skips: they all run now that the cluster + PostgreSQL
    // are provisioned. Named explicitly so a regression is unmistakable.
    for forbidden in FORBIDDEN_SKIP_FRAGMENTS {
        assert!(
            !actual.iter().any(|t| t.contains(forbidden)),
            "the convenience skip `{forbidden}` reappeared in the nightly run step; \
             those tests run now that the cluster + PostgreSQL are provisioned"
        );
    }

    // The three colliding tokens would silently drop their hermetic twins in
    // ferrosa-cluster/tests/accord_nemesis.rs (libtest `--skip` matches by name
    // across every selected binary). The job must therefore re-run that binary
    // with no exclusions.
    for collider in [
        "disk_fail_no_phantom_commits",
        "packet_reorder_linearizability",
        "lwt_batch_atomicity_all_nemeses",
    ] {
        assert!(
            actual.contains(collider),
            "expected `{collider}` to be in the permitted set"
        );
    }
    let guard = step_body(
        &yaml,
        "Re-run hermetic accord_nemesis tests (name-collision guard)",
    );
    assert!(
        guard.contains("--test accord_nemesis") && !guard.contains("--skip"),
        "the name-collision guard step must re-run ferrosa-cluster's accord_nemesis \
         WITH NO exclusions (three `--skip` tokens share exact names with its \
         hermetic tests); step was:\n{guard}"
    );
}

#[test]
fn every_permitted_exclusion_still_names_a_live_blocker_and_a_real_test() {
    let root = repo_root();

    for e in PERMITTED_EXCLUSIONS {
        let path = root.join(e.evidence_file);
        let src = std::fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!(
                "permitted exclusion `{}` names {}:{} but the file is gone or unreadable ({err}). \
                 A stale entry is a defect — its blocker may have been fixed. \
                 Drop it from PERMITTED_EXCLUSIONS and from the nightly run step.",
                e.token, e.evidence_file, e.evidence_line
            )
        });

        // The test must still exist under its exact name in the cited file.
        let fn_marker = format!("fn {}", e.token);
        assert!(
            src.contains(&fn_marker),
            "permitted exclusion `{}` no longer matches a real test: `{}` not found in {}. \
             If the test was deleted or the blocker was fixed, this entry is stale — \
             remove it from PERMITTED_EXCLUSIONS and from the nightly run step. \
             (blocker on record: {})",
            e.token,
            fn_marker,
            e.evidence_file,
            e.blocker
        );

        // The blocker must still be present: an env gate that vanished would
        // mean the test could start silently passing.
        assert!(
            src.contains(e.blocker_marker),
            "permitted exclusion `{}` claims blocker `{}` (marker `{}`) but {} no longer \
             contains that marker; the blocker may have changed",
            e.token,
            e.blocker,
            e.blocker_marker,
            e.evidence_file
        );

        // The `file:line` must point inside the file.
        let line_count = src.lines().count();
        assert!(
            (e.evidence_line as usize) <= line_count && e.evidence_line > 0,
            "permitted exclusion `{}` cites {}:{} but that file has only {line_count} lines",
            e.token,
            e.evidence_file,
            e.evidence_line
        );
    }
}

#[test]
fn no_ignore_is_used_as_an_environment_gate() {
    let root = repo_root();
    let mut files = Vec::new();
    collect_rs_files(&root, &mut files);
    assert!(
        files.len() > 100,
        "expected to walk the workspace, found only {} .rs files under {} — the walker is broken",
        files.len(),
        root.display()
    );

    let mut offenders: Vec<String> = Vec::new();
    let mut unknown_ignore_files: Vec<String> = Vec::new();
    for path in &files {
        let src = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(_) => continue,
        };
        for line in src.lines() {
            let trimmed = line.trim_start();
            if !trimmed.starts_with("#[ignore") {
                continue;
            }
            let rel = path
                .strip_prefix(&root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            // A real `#[ignore]` attribute must be in the allowlist and must
            // not name a `FERROSA_TEST_*` prerequisite.
            if !ALLOWED_IGNORE_FILES.contains(&rel.as_str()) {
                unknown_ignore_files.push(format!("{rel}: {trimmed}"));
            }
            if trimmed.contains("FERROSA_TEST_") {
                offenders.push(format!("{rel}: {trimmed}"));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "`#[ignore]` is being used as an environment gate (it names a FERROSA_TEST_* \
         prerequisite). ferrosa/CLAUDE.md forbids ignoring a test for being slow or \
         env-dependent — such a test must panic loudly instead:\n{}",
        offenders.join("\n")
    );
    assert!(
        unknown_ignore_files.is_empty(),
        "a new `#[ignore]` appeared outside the pre-existing allowlist. If it is \
         cluster-gated rather than env-gated it must be added to ALLOWED_IGNORE_FILES \
         deliberately; silently ignoring a test for being slow/env-dependent is \
         forbidden by ferrosa/CLAUDE.md:\n{}",
        unknown_ignore_files.join("\n")
    );
}
