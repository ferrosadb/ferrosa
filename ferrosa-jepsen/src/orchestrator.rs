use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::chaos::{NemesisAction, NemesisContext, NemesisRegistry};
use crate::checker::{check_linearizability, CheckResult};
use crate::config::{Concurrency, RunConfig, Tier, Topology};
use crate::cql_session::ScyllaCqlSession;
use crate::docker_provision::{provision_docker_cluster, teardown_docker_cluster, ClusterInfo};
use crate::driver::DriverRegistry;
use crate::history::HistoryRecorder;
use crate::report::RunReport;
use crate::workload::{CqlSession, MockCqlSession, WorkloadRegistry};

/// Where the CQL session for a single combination came from.
///
/// Used by the orchestrator to decide whether to dial the real cluster or
/// the in-process mock. The variant is exposed for unit tests so they can
/// assert the wiring without spinning a Docker cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionSource {
    /// In-process mock — no I/O. Used when no cluster has been provisioned
    /// or when running unit-style combinations.
    Mock,
    /// Real CQL session against the contact points listed.
    Real(Vec<String>),
}

/// Resolve the session source for a single combination.
///
/// When a real cluster has been provisioned (`cluster.is_some()`), we route
/// queries through `ScyllaCqlSession` against the cluster's contact points.
/// Otherwise (unit-level invocations, in-process orchestrator runs without
/// containers), we fall back to `MockCqlSession`.
///
/// Prior to Sprint 2, this function did not exist: `run_single_combination`
/// took an `Option<&ClusterInfo>` argument that it ignored, so the real
/// cluster path was unreachable. See `specs/in-process/sprint-02-jepsen-reactivation.md`
/// W2.1 / W2.2.
pub(crate) fn resolve_session_source(cluster: Option<&ClusterInfo>) -> SessionSource {
    match cluster {
        Some(c) => {
            let addrs: Vec<String> = c.nodes.iter().map(|n| n.cql_address()).collect();
            SessionSource::Real(addrs)
        }
        None => SessionSource::Mock,
    }
}

/// Build a non-owning cluster description from caller-provisioned CQL contact
/// points. This is the live path for Fly: the workload runner may dial the
/// machines but must never create or tear down their lifecycle.
fn cluster_from_preprovisioned_nodes(
    topology: Topology,
    contact_points: &str,
) -> Result<ClusterInfo> {
    let nodes = contact_points
        .split(',')
        .map(str::trim)
        .filter(|point| !point.is_empty())
        .map(|point| {
            let (host, port) = point
                .rsplit_once(':')
                .ok_or_else(|| anyhow::anyhow!("CQL contact point `{point}` must be host:port"))?;
            let host = host.trim_matches(['[', ']']);
            if host.is_empty() {
                bail!("CQL contact point `{point}` has an empty host");
            }
            let cql_port = port.parse::<u16>().map_err(|e| {
                anyhow::anyhow!("CQL contact point `{point}` has invalid port: {e}")
            })?;
            Ok(crate::docker_provision::NodeInfo {
                host: host.to_string(),
                cql_port,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if nodes.len() != topology.node_count() {
        bail!(
            "{:?} requires exactly {} CQL contact points, got {}",
            topology,
            topology.node_count(),
            nodes.len()
        );
    }
    Ok(ClusterInfo::external(nodes))
}

/// Build the command transport for a live nemesis run. Fly machine IDs are
/// required when the caller selects the Fly transport; otherwise this retains
/// the direct-SSH path used by Firecracker-based tests.
fn nemesis_context_for_cluster(cluster: &ClusterInfo) -> Result<NemesisContext> {
    let node_ips = cluster.nodes.iter().map(|node| node.host.clone()).collect();
    if let Ok(app_name) = std::env::var("FERROSA_JEPSEN_FLY_APP") {
        let machine_ids = std::env::var("FERROSA_JEPSEN_FLY_MACHINE_IDS")
            .map_err(|_| {
                anyhow::anyhow!(
                    "FERROSA_JEPSEN_FLY_APP is set but FERROSA_JEPSEN_FLY_MACHINE_IDS is missing"
                )
            })?
            .split(',')
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .collect();
        return NemesisContext::fly(node_ips, app_name, machine_ids);
    }

    if std::env::var("FERROSA_TEST_CONTAINERS").is_ok() {
        let container_names: Vec<String> = [
            "ferrosa-jepsen-t3-dc1-node1",
            "ferrosa-jepsen-t3-dc1-node2",
            "ferrosa-jepsen-t3-dc1-node3",
            "ferrosa-jepsen-t3-dc2-node1",
            "ferrosa-jepsen-t3-dc2-node2",
            "ferrosa-jepsen-t3-dc2-node3",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        // The DC-partition nemesis installs iptables DROP rules *inside* the
        // containers, so it must target each node's real cross-DC (WAN) network
        // address. The outer `node_ips` come from the host-mapped CQL contacts
        // (`localhost:2904x`); using those would make the partition drop
        // loopback and silently sever nothing. Resolve the container WAN IPs.
        let node_ips = resolve_container_wan_ips(&container_names)?;
        return Ok(NemesisContext {
            node_ips,
            ssh_user: "root".to_string(),
            ssh_key_path: std::path::PathBuf::new(),
            ssh_port: 0,
            executor: crate::chaos::NemesisExecutor::Docker { container_names },
        });
    }

    Ok(NemesisContext {
        node_ips,
        ssh_user: std::env::var("FERROSA_SSH_USER").unwrap_or_else(|_| "root".to_string()),
        ssh_key_path: std::env::var("FERROSA_SSH_KEY")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from("rootfs/test_key")),
        ssh_port: std::env::var("FERROSA_SSH_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(22),
        executor: crate::chaos::NemesisExecutor::Ssh,
    })
}

/// Resolve each container's WAN-network IP (the address cross-DC peers actually
/// use), in the same order as `containers`. Container mode otherwise has no
/// route to the internal IPs — the CQL contacts are host-mapped `localhost`
/// ports — so the DC-partition nemesis needs this to drop real inter-DC traffic
/// instead of loopback.
fn resolve_container_wan_ips(containers: &[String]) -> Result<Vec<String>> {
    let runtime = crate::docker_provision::container_runtime();
    containers
        .iter()
        .map(|c| {
            let out = std::process::Command::new(runtime)
                .args([
                    "inspect",
                    c,
                    "--format",
                    "{{json .NetworkSettings.Networks}}",
                ])
                .output()
                .with_context(|| format!("failed to inspect container {c}"))?;
            if !out.status.success() {
                bail!(
                    "{runtime} inspect {c} failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            let networks: serde_json::Value = serde_json::from_slice(&out.stdout)
                .with_context(|| format!("parsing networks JSON for {c}"))?;
            let obj = networks
                .as_object()
                .with_context(|| format!("networks for {c} not a JSON object"))?;
            // Prefer the shared WAN network (the only one spanning both DCs);
            // fall back to the first network if the naming differs.
            let ip = obj
                .iter()
                .find(|(name, _)| name.contains("wan"))
                .or_else(|| obj.iter().next())
                .and_then(|(_, v)| v.get("IPAddress"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .with_context(|| format!("no IPAddress for container {c}"))?;
            Ok(ip.to_string())
        })
        .collect()
}

/// Inject one fault window and always heal it before returning. The workload
/// runs concurrently with this cycle, so a named Jepsen nemesis is evidence,
/// not merely a label in the final report.
async fn run_nemesis_cycle(
    nemesis: &dyn NemesisAction,
    ctx: &NemesisContext,
    inject_duration: Duration,
) -> Result<()> {
    nemesis.inject(ctx).await?;
    tokio::time::sleep(inject_duration).await;
    nemesis.heal(ctx).await
}

/// Result of a single test combination (one workload + one nemesis).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CombinationResult {
    pub workload: String,
    pub nemesis: String,
    pub topology: String,
    pub concurrency: String,
    pub driver: String,
    pub passed: bool,
    pub linearizability: Vec<CheckResult>,
    pub invariant_passed: bool,
    pub invariant_error: Option<String>,
    /// Set when the combination never RAN to completion — cluster provisioning,
    /// DDL, or driver setup failed — so no history was produced and no invariant
    /// was ever evaluated.
    ///
    /// This is deliberately distinct from `invariant_error`. Both used to be
    /// reported as "violated an invariant", which announced a flaky harness or
    /// infrastructure failure as a CORRECTNESS regression (see issue #303: a
    /// schema-agreement timeout at `CREATE KEYSPACE` was reported that way, and
    /// the bank invariant it named had never been tested).
    #[serde(default)]
    pub setup_error: Option<String>,
    pub duration_secs: f64,
    pub op_count: usize,
}

impl CombinationResult {
    /// True when this combination failed before producing any history — the
    /// failure says nothing about the database's correctness.
    pub fn is_setup_failure(&self) -> bool {
        self.setup_error.is_some()
    }
}

/// Run the full test suite per the config.
pub async fn run(config: &RunConfig) -> Result<RunReport> {
    let topologies = config.topologies();
    let concurrency_levels = config.concurrency_levels();

    let nemesis_reg = resolve_nemesis_registry(config.tier);
    let nemesis_names: Vec<String> = match &config.nemesis {
        Some(n) => vec![n.clone()],
        None => nemesis_reg.names(),
    };

    let workload_names: Vec<String> = match &config.pattern {
        Some(p) => vec![p.clone()],
        None => {
            let wr = crate::workload::WorkloadRegistry::phase1();
            wr.names()
        }
    };

    let driver_reg = resolve_driver_registry(config.tier);
    let driver_names: Vec<String> = match &config.driver {
        Some(d) => vec![d.clone()],
        None => driver_reg.names(),
    };

    let mut results = Vec::new();

    for topology in &topologies {
        tracing::info!(?topology, "Provisioning cluster");

        // Provision the cluster for this topology.
        // For single-DC topologies (T1/T2) we use Docker Compose when
        // FERROSA_TEST_CONTAINERS is set; otherwise we fall back to a
        // no-op cluster (placeholder) so the orchestrator can still run
        // unit-level combinations without container infrastructure.
        let cluster_opt: Option<ClusterInfo> = if let Ok(contact_points) =
            std::env::var("FERROSA_TEST_CLUSTER_NODES")
        {
            Some(cluster_from_preprovisioned_nodes(
                *topology,
                &contact_points,
            )?)
        } else if std::env::var("FERROSA_TEST_CONTAINERS").is_ok() && topology.node_count() <= 3 {
            match provision_docker_cluster(*topology).await {
                Ok(c) => Some(c),
                Err(e) => {
                    tracing::error!(
                        ?topology,
                        error = %e,
                        "Docker cluster provisioning failed; skipping topology"
                    );
                    continue;
                }
            }
        } else {
            None
        };

        if matches!(config.tier, Tier::MultiDc) && cluster_opt.is_none() {
            bail!(
                "tier multi-dc requires a preprovisioned six-node cluster; set \
                 FERROSA_TEST_CLUSTER_NODES=host1:9042,...,host6:9042"
            );
        }

        for concurrency in &concurrency_levels {
            for driver_name in &driver_names {
                for nemesis_name in &nemesis_names {
                    for workload_name in &workload_names {
                        let result = run_single_combination(
                            *topology,
                            nemesis_name,
                            workload_name,
                            driver_name,
                            *concurrency,
                            config,
                            cluster_opt.as_ref(),
                        )
                        .await;

                        match result {
                            Ok(r) => results.push(r),
                            Err(e) => {
                                tracing::error!(
                                    workload = workload_name.as_str(),
                                    nemesis = nemesis_name.as_str(),
                                    driver = driver_name.as_str(),
                                    error = %e,
                                    "Combination failed to run (setup/execution error — no \
                                     invariant was evaluated)"
                                );
                                results.push(CombinationResult {
                                    workload: workload_name.clone(),
                                    nemesis: nemesis_name.clone(),
                                    topology: format!("{topology:?}"),
                                    concurrency: format!("{concurrency:?}"),
                                    driver: driver_name.clone(),
                                    passed: false,
                                    linearizability: vec![],
                                    // The combination never ran, so no invariant
                                    // was checked — do NOT claim one was violated.
                                    invariant_passed: false,
                                    invariant_error: None,
                                    setup_error: Some(e.to_string()),
                                    duration_secs: 0.0,
                                    op_count: 0,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Tear down the cluster after all combinations for this topology.
        if let Some(cluster) = cluster_opt {
            if let Err(e) = teardown_docker_cluster(cluster).await {
                tracing::warn!(?topology, error = %e, "cluster teardown failed");
            }
        }
    }

    let report = RunReport::from_results(&config.run_id, results);

    // Write report
    let report_dir = config.output_dir.join(&config.run_id);
    std::fs::create_dir_all(&report_dir)?;
    report.write_json(&report_dir.join("results.json"))?;
    report.write_html(&report_dir.join("report.html"))?;

    Ok(report)
}

/// Resolve nemesis registry based on tier.
fn resolve_nemesis_registry(tier: Tier) -> NemesisRegistry {
    match tier {
        Tier::Smoke => NemesisRegistry::phase1(),
        Tier::Standard => NemesisRegistry::phase2(),
        Tier::Full | Tier::Endurance => NemesisRegistry::full(),
        // Sprint 7 W7.11: tier-multi-dc uses the full registry
        // which includes the W7.11 dc-partition+dc-slow composed
        // nemesis required by the headline run.
        Tier::MultiDc => NemesisRegistry::full(),
    }
}

/// Resolve driver registry based on tier.
fn resolve_driver_registry(tier: Tier) -> DriverRegistry {
    match tier {
        Tier::Smoke => DriverRegistry::phase1(),
        _ => DriverRegistry::phase1(), // Phase 2: switch to phase2() once drivers are ready
    }
}

/// Run a single workload+nemesis combination.
///
/// Wires the full pipeline:
/// 1. Resolve workload from registry.
/// 2. Set up schema via CQL session.
/// 3. Run workload, recording history.
/// 4. Check linearizability.
/// 5. Check workload-specific invariants.
///
/// Uses `MockCqlSession` when no real cluster contact points are provided.
/// `cluster` is `Some` when a real Docker cluster has been provisioned.
async fn run_single_combination(
    topology: Topology,
    nemesis_name: &str,
    workload_name: &str,
    driver_name: &str,
    concurrency: Concurrency,
    config: &RunConfig,
    cluster: Option<&crate::docker_provision::ClusterInfo>,
) -> Result<CombinationResult> {
    let start = Instant::now();

    tracing::info!(
        workload = workload_name,
        nemesis = nemesis_name,
        driver = driver_name,
        ?concurrency,
        ?topology,
        "Running combination"
    );

    // Resolve workload.
    let registry = WorkloadRegistry::phase1();
    let workload = registry
        .get(workload_name)
        .ok_or_else(|| anyhow::anyhow!("unknown workload: {workload_name}"))?;

    // Resolve session source: real cluster when provisioned, mock otherwise.
    // Sprint 2 W2.2 — formerly the cluster argument was discarded and
    // `MockCqlSession` was used unconditionally.
    let session: Box<dyn CqlSession> = match resolve_session_source(cluster) {
        SessionSource::Real(addrs) => {
            tracing::info!(?addrs, "dialing real CQL cluster");
            Box::new(ScyllaCqlSession::connect(&addrs).await?)
        }
        SessionSource::Mock => {
            tracing::debug!("using MockCqlSession (no cluster provisioned)");
            Box::new(MockCqlSession)
        }
    };

    // Set up schema.
    workload.setup(session.as_ref()).await?;

    // Run the workload while the selected fault is actually active. `noop`
    // remains the control case; every other name must execute and heal rather
    // than merely appearing in the report label.
    let run_duration = Duration::from_secs(config.run_duration_secs());
    let mut recorder = HistoryRecorder::new(&format!("{driver_name}-{workload_name}"));
    if nemesis_name == "noop" {
        workload
            .run(session.as_ref(), &mut recorder, run_duration)
            .await?;
    } else {
        let cluster = cluster.ok_or_else(|| {
            anyhow::anyhow!(
                "nemesis `{nemesis_name}` requires a live cluster; refusing to label a mock run as faulted"
            )
        })?;
        let nemesis_registry = resolve_nemesis_registry(config.tier);
        let nemesis = nemesis_registry
            .get(nemesis_name)
            .ok_or_else(|| anyhow::anyhow!("unknown nemesis `{nemesis_name}`"))?;
        let context = nemesis_context_for_cluster(cluster)?;
        let inject_duration = run_duration.min(Duration::from_secs(60));
        let (workload_result, nemesis_result) = tokio::join!(
            workload.run(session.as_ref(), &mut recorder, run_duration),
            run_nemesis_cycle(nemesis, &context, inject_duration),
        );
        workload_result?;
        nemesis_result?;
    }
    let history = recorder.finish();

    // Write history JSONL to output directory.
    let run_dir = config.output_dir.join(&config.run_id);
    std::fs::create_dir_all(&run_dir)?;
    let history_path = run_dir.join(format!("{topology:?}-{nemesis_name}-{workload_name}.jsonl"));
    history.to_jsonl(&history_path)?;

    // Check linearizability — only for workloads whose history is a set of
    // single-value register operations (register/LWT). Transactional workloads
    // like `bank` opt out (`register_linearizable() == false`): their delta
    // writes and multi-key snapshot reads cannot be modelled as a single-value
    // register, and per-key linearizability is not a guarantee the
    // eventually-consistent base makes. Their safety is judged by
    // `check_invariant` (conservation) and, for transactions, strict
    // serializability (Elle). An empty result vector is vacuously all-linear.
    let linearizability = if workload.register_linearizable() {
        check_linearizability(&history)
    } else {
        Vec::new()
    };
    let all_linear = linearizability.iter().all(|r| r.valid);

    // Check workload-specific invariants.
    let invariant_result = workload.check_invariant(&history);
    let invariant_passed = invariant_result.is_ok();
    let invariant_error = invariant_result.err().map(|e| e.to_string());

    let duration = start.elapsed();

    Ok(CombinationResult {
        workload: workload_name.into(),
        nemesis: nemesis_name.into(),
        topology: format!("{topology:?}"),
        concurrency: format!("{concurrency:?}"),
        driver: driver_name.into(),
        passed: all_linear && invariant_passed,
        linearizability,
        invariant_passed,
        invariant_error,
        setup_error: None,
        duration_secs: duration.as_secs_f64(),
        op_count: history.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combination_result_serialization() {
        let r = CombinationResult {
            workload: "register".into(),
            nemesis: "partition-halves".into(),
            topology: "T1".into(),
            concurrency: "low".into(),
            driver: "rust".into(),
            passed: true,
            linearizability: vec![],
            invariant_passed: true,
            invariant_error: None,
            setup_error: None,
            duration_secs: 1.5,
            op_count: 42,
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: CombinationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.workload, "register");
        assert!(back.passed);
    }

    #[test]
    fn workload_names_from_registry() {
        let wr = crate::workload::WorkloadRegistry::phase1();
        let names = wr.names();
        assert!(!names.is_empty());
        assert!(names.contains(&"register".to_string()));
        assert!(names.contains(&"bank".to_string()));
    }

    // -----------------------------------------------------------------------
    // W2.1 / W2.2 — orchestrator must wire the real cluster when one is provided.
    //
    // Before Sprint 2 the orchestrator discarded its `_cluster` argument and
    // always used `MockCqlSession`, even when callers had spun up a real
    // Docker cluster. The bug lived at `orchestrator.rs:203`. The two tests
    // below pin the resolution helper that drives the wiring.
    // -----------------------------------------------------------------------

    /// W2.2: when a cluster is provided, the orchestrator must dial it.
    #[test]
    fn orchestrator_uses_real_cluster_when_provided() {
        use crate::docker_provision::NodeInfo as JepsenNodeInfo;

        let cluster = ClusterInfo {
            nodes: vec![
                JepsenNodeInfo {
                    host: "localhost".to_string(),
                    cql_port: 49042,
                },
                JepsenNodeInfo {
                    host: "localhost".to_string(),
                    cql_port: 49043,
                },
                JepsenNodeInfo {
                    host: "localhost".to_string(),
                    cql_port: 49044,
                },
            ],
            compose_file: Some(std::path::PathBuf::from("/tmp/fake.yml")),
        };

        let source = resolve_session_source(Some(&cluster));
        match source {
            SessionSource::Real(addrs) => {
                assert_eq!(
                    addrs,
                    vec![
                        "localhost:49042".to_string(),
                        "localhost:49043".to_string(),
                        "localhost:49044".to_string(),
                    ],
                    "real session must use the cluster's CQL contact points"
                );
            }
            SessionSource::Mock => panic!(
                "orchestrator must NOT use MockCqlSession when a real cluster is provided \
                 (Sprint 2 W2.2 — see specs/in-process/sprint-02-jepsen-reactivation.md)"
            ),
        }
    }

    /// W2.1: when no cluster is provided, the orchestrator falls back to mock.
    #[test]
    fn orchestrator_uses_mock_when_no_cluster_provided() {
        let source = resolve_session_source(None);
        assert_eq!(source, SessionSource::Mock);
    }

    #[test]
    fn multi_dc_uses_preprovisioned_fly_nodes_as_real_cql_contacts() {
        let cluster = cluster_from_preprovisioned_nodes(
            Topology::T3,
            "fdaa:0:abcd::1:9042,fdaa:0:abcd::2:9042,fdaa:0:abcd::3:9042,\
             fdaa:0:dcba::1:9042,fdaa:0:dcba::2:9042,fdaa:0:dcba::3:9042",
        )
        .expect("six Fly nodes are a valid T3 cluster");

        assert_eq!(cluster.nodes.len(), 6);
        assert_eq!(
            resolve_session_source(Some(&cluster)),
            SessionSource::Real(vec![
                "[fdaa:0:abcd::1]:9042".to_string(),
                "[fdaa:0:abcd::2]:9042".to_string(),
                "[fdaa:0:abcd::3]:9042".to_string(),
                "[fdaa:0:dcba::1]:9042".to_string(),
                "[fdaa:0:dcba::2]:9042".to_string(),
                "[fdaa:0:dcba::3]:9042".to_string(),
            ]),
            "multi-DC runs must dial the supplied Fly cluster rather than use MockCqlSession"
        );
    }

    #[tokio::test]
    async fn run_single_combination_completes() {
        // Verify that run_single_combination runs the full pipeline (workload setup,
        // run, linearizability check, invariant check) without panicking.
        //
        // Uses MockCqlSession so no real cluster is needed. The mock returns empty
        // rows for all queries, which means reads return Value(None). This is
        // consistent with a never-written register (model starts at None), so a
        // history with only reads would be linearizable. With writes also present,
        // the mock history may not be linearizable — the important invariant is
        // that the pipeline runs end-to-end without error, not that it passes.
        let dir = tempfile::tempdir().unwrap();
        let config = RunConfig {
            tier: Tier::Smoke,
            topology: None,
            nemesis: None,
            pattern: None,
            driver: None,
            concurrency: None,
            run_id: "test-run".into(),
            output_dir: dir.path().to_path_buf(),
            fly_regions: vec![],
            alert_webhook: None,
            output_json: false,
        };

        let result = run_single_combination(
            Topology::T1,
            "noop",
            "register",
            "rust",
            Concurrency::Low,
            &config,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.workload, "register");
        assert_eq!(result.nemesis, "noop");
        assert_eq!(result.driver, "rust");
        // History file should exist.
        let history_path = dir.path().join("test-run").join("T1-noop-register.jsonl");
        assert!(history_path.exists(), "history JSONL must be written");
    }

    #[tokio::test]
    async fn selected_nemesis_is_injected_and_healed() {
        use std::sync::{Arc, Mutex};

        struct RecordingNemesis(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait::async_trait]
        impl crate::chaos::NemesisAction for RecordingNemesis {
            fn name(&self) -> &str {
                "recording"
            }

            async fn inject(&self, _ctx: &crate::chaos::NemesisContext) -> anyhow::Result<()> {
                self.0.lock().unwrap().push("inject");
                Ok(())
            }

            async fn heal(&self, _ctx: &crate::chaos::NemesisContext) -> anyhow::Result<()> {
                self.0.lock().unwrap().push("heal");
                Ok(())
            }
        }

        let events = Arc::new(Mutex::new(Vec::new()));
        let nemesis = RecordingNemesis(Arc::clone(&events));
        let ctx = crate::chaos::NemesisContext {
            node_ips: vec!["127.0.0.1".into()],
            ssh_user: "root".into(),
            ssh_key_path: std::path::PathBuf::from("/tmp/not-used"),
            ssh_port: 22,
            executor: crate::chaos::NemesisExecutor::Ssh,
        };

        run_nemesis_cycle(&nemesis, &ctx, Duration::ZERO)
            .await
            .expect("nemesis cycle succeeds");
        assert_eq!(*events.lock().unwrap(), ["inject", "heal"]);
    }

    #[tokio::test]
    async fn named_nemesis_refuses_to_run_against_the_mock_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let config = RunConfig {
            tier: Tier::Smoke,
            topology: None,
            nemesis: None,
            pattern: None,
            driver: None,
            concurrency: None,
            run_id: "mock-fault".into(),
            output_dir: dir.path().to_path_buf(),
            fly_regions: vec![],
            alert_webhook: None,
            output_json: false,
        };

        let error = run_single_combination(
            Topology::T1,
            "partition-halves",
            "register",
            "rust",
            Concurrency::Low,
            &config,
            None,
        )
        .await
        .expect_err("a named fault must never silently run against MockCqlSession");
        assert!(error.to_string().contains("requires a live cluster"));
    }
}
