# Ferrosa Coverage Gap Report — Top-Level Synthesis

> Generated: 2026-04-18
> Zone inputs read: 6 of 10
> Inputs used: storage-coverage.md, cluster-coverage.md, cql-coverage.md, multimodel-coverage.md, ops-coverage.md, testing-infra-coverage.md
> Authoritative supplemental inputs: specs/status.md, specs/gaps-2026-04-03.mc, specs/project-plan-gap-closure.md, specs/testing.md

**Pending zones** — rerun coverage agent for each before re-generating this file:
- `specs/coverage/accord-coverage.md` — not yet produced
- `specs/coverage/indexing-coverage.md` — not yet produced
- `specs/coverage/schema-auth-net-coverage.md` — not yet produced
- `ferrosa-memory/specs/coverage/coverage.md` — not yet produced

---

## 1. Executive Summary

- **Storage and SSTable are the best-documented zones.** Architecture specs, FMEA, and tests closely match what the specs promise. The main gaps are two post-incident correctness invariants (Gate A/B clustering validation and startup quarantine) that exist only in ARCHITECTURE.md footnotes, not in the primary specs where engineers look during maintenance.
- **The live-cluster testing layer is the single largest structural gap in the project.** Every zone that has automated testing reports the same pattern: unit and deterministic in-process coverage is strong; real multi-node correctness assertions are either `todo!` stubs, skipped in every CI workflow, or only exercise harness plumbing rather than named invariants. This is not a zone-specific gap — it is a systemic condition across cluster, Accord, PITR, and driver testing.
- **Auth and access control is under-documented across every zone that was reviewed.** `FERROSA_AUTH_DISABLED=true` bypasses all CQL and web API auth in every deployed configuration. The auth middleware has zero tests. There is no unified auth architecture spec, no cross-zone threat model for auth surfaces, and no live integration test that exercises auth across CQL, graph HTTP, and SPARQL simultaneously.
- **The multi-model zone (graph and SPARQL) has two P0 correctness gaps unrelated to documentation:** MERGE is not implemented (blocking safe migration of ferrosa-memory writes to the graph API), and ferrosa-memory bypasses the graph adjacency index on every edge write via direct CQL. These are code gaps, not just missing specs.
- **Two CI workflows have defects that cause every release build and nightly fuzz run to fail** on infra-gated panics: `release.yml` includes `ferrosa-jepsen` and `ferrosa-loadgen` without excluding them, and `nightly-fuzz.yml` excludes `ferrosa-jepsen` but not `ferrosa-loadgen`.

---

## 2. Coverage by Zone

| Zone | Features inventoried | P0 gaps | P1 gaps | P2 gaps | Coverage-% (est.) | Input status |
|------|---------------------|---------|---------|---------|-------------------|-------------|
| Storage (`ferrosa-storage`, `ferrosa-sstable`) | 60 | 2 (Gate A/B spec missing, quarantine not in storage.md invariant) | 2 (PendingUploadsLog, CdcReader unspecced) | 3 (jemalloc runbook, timeseries arch, compaction readback) | ~75% | complete |
| Cluster / Raft (`ferrosa-cluster` ex-Accord) | 55 | 2 (hints arch missing, repair trigger unspecced) | 3 (batchlog protocol, SystemTableWriter, FormationState not implemented) | 3 (streaming protocol, ClusterConfig reference, NTS per-DC) | ~60% | complete |
| Accord (`ferrosa-cluster::accord`) | unknown | unknown | unknown | unknown | unknown | **not yet produced** |
| CQL (`ferrosa-cql`) | 50 | 1 (EVENT push not wired after REGISTER) | 1 (auth disabled end-to-end, no auth test) | 3 (stub virtual tables, stale ADR-006, no compression integration test) | ~70% | complete |
| Multi-model (graph, SPARQL, UDF) | 53 | 2 (MERGE unimplemented, ferrosa-memory write bypass) | 3 (RDF* stub, Turtle format violation, no SPARQL client in ferrosa-memory) | 2 (Bolt untested, reverse edge index missing) | ~50% | complete |
| Indexing (`ferrosa-index`, `ferrosa-index-builder`) | unknown | unknown | unknown | unknown | unknown | **not yet produced** |
| Schema / Auth / Net (`ferrosa-schema`, `ferrosa-net`) | unknown | unknown | unknown | unknown | unknown | **not yet produced** |
| Ops (`ferrosa` binary, `ferrosa-ctl`, `ferrosa-worker`) | 26 endpoints + 15 ctl cmds | 2 (auth middleware zero tests, restore no E2E test) | 2 (flamechart undocumented, ctl web commands no live HTTP test) | 2 (force-compact unspecced, WebSocket no spec) | ~55% | complete |
| Testing infra (ferrosa-jepsen, ferrosa-loadgen, CI) | 18 Jepsen workloads, 5 load profiles, 7 workflows | 2 (release.yml fails on infra panics, nightly fails on loadgen panics) | 2 (no container tests in any workflow, Jepsen unit tests excluded from CI) | 2 (no sanitizer runs, no loadgen smoke in nightly) | ~50% | complete |
| ferrosa-memory | unknown | unknown | unknown | unknown | unknown | **not yet produced** |

---

## 3. Top 20 Gaps Across the System

| Rank | ID | Zone | Description | Recommended action | Effort |
|------|----|------|-------------|-------------------|--------|
| 1 | G-01 | Testing infra | `release.yml` runs `cargo test --workspace` with no exclusions — jepsen and loadgen infra tests `panic!` on every tagged release | Add `--exclude ferrosa-jepsen --exclude ferrosa-loadgen` to release.yml test step | S |
| 2 | G-02 | Testing infra | `nightly-fuzz.yml` excludes jepsen but not loadgen — nightly suite fails on loadgen panics, producing spurious regression PRs | Add `--exclude ferrosa-loadgen` to nightly-fuzz.yml | S |
| 3 | G-03 | CQL / Ops | `FERROSA_AUTH_DISABLED=true` in all deployments; auth middleware has zero tests; no integration test verifies 401 on unauthenticated request | Add auth middleware tests; execute design-cql-role-auth-rollout.md Sprint A | M |
| 4 | G-04 | Multi-model | Cypher `MERGE` not implemented — blocks migration of ferrosa-memory writes from direct CQL to graph API; adjacency invariants violated on every edge write | Implement MERGE: AST, parser, physical planner, executor | M |
| 5 | G-05 | Multi-model | ferrosa-memory bypasses graph adjacency index with direct CQL writes to 7 edge tables, silently violating graph engine invariants | Resolve after G-04; migrate ferrosa-memory to Cypher MERGE | M |
| 6 | G-06 | CQL | EVENT push not wired after REGISTER — drivers relying on schema-change events to invalidate prepared statements silently fail | Subscribe connection to event_sender after REGISTER; push EVENT frames in select! loop (~30 lines) | S |
| 7 | G-07 | Storage | Gate A (clustering-shape validation) and Gate B (self-readback) are load-bearing correctness invariants absent from `specs/sstable.md` and `specs/storage.md` | Add §SSTableWriter Invariants to specs/sstable.md; add Gate A/B to specs/storage.md §Flush Path | S |
| 8 | G-08 | Ops | `POST /api/restore` and `ferrosa-ctl restore` have no E2E integration test; `--point-in-time` path entirely untested | Create restore E2E test in ferrosa/tests/ or ferrosa-ctl/tests/restore_e2e.rs | M |
| 9 | G-09 | Cluster | Hinted handoff has no architecture spec — hint file format, capacity eviction, delivery ordering, `needs_repair` semantics are undocumented | Create specs/hints-architecture.md | S |
| 10 | G-10 | Cluster | Repair triggering policy is unspecced — Merkle primitives exist but no caller is defined; anti-entropy has no trigger mechanism | Create specs/anti-entropy-architecture.md covering trigger, range selection, exchange protocol | M |
| 11 | G-11 | Multi-model | RDF* query execution is a stub returning empty annotations unconditionally; queries silently return no results | Implement edge_annotations table; wire evaluate_rdf_star_pattern | M |
| 12 | G-12 | Multi-model | Turtle serializer outputs N-Triples content with text/turtle content-type — silent format violation that passes existing tests | Implement proper Turtle serialization (~50 lines + prefix handling) | S |
| 13 | G-13 | Storage | Startup quarantine invariant documented only in a bug spec; OPS-3 S3-marker mitigation (quarantine marker in S3 to prevent 404 storms) is unimplemented and unspecced | Add §Startup Quarantine to specs/storage.md; create S3-marker mitigation spec | M |
| 14 | G-14 | Cluster | Batchlog two-phase protocol (703 lines) has no spec covering failure modes, double-apply risk on batchlog-delete failure, or consistency guarantees | Create specs/batchlog-coordinator.md | M |
| 15 | G-15 | Cluster | `DeploymentMode` is 3-variant (Standalone/Pair/Cluster); architecture spec requires FormationState with Forming and Degraded variants; degraded handling resets to Standalone losing peer context | Create specs/in-process/gap-formation-state-machine.md; implement FormationState | L |
| 16 | G-16 | Testing infra | ferrosa-jepsen's 53+ pure unit tests (linearizability checker, workload registry) are excluded from every CI workflow despite requiring no infrastructure | Add `cargo test -p ferrosa-jepsen --lib` step to ci.yml | S |
| 17 | G-17 | CQL | Several system_observability virtual tables (billing, alerts, full_scan_reasons, query_fingerprints) are stubs returning empty/static data, violating the "fail loud" convention | Audit each stub; implement or remove; update cql.md | M |
| 18 | G-18 | Ops | Flamechart endpoint `check_ip_whitelist` always receives None for remote_ip — the designed threat-model mitigation (OBS-T1) is inert | Pass ConnectInfo<SocketAddr> extractor to flamechart_handler | S |
| 19 | G-19 | Testing infra | `FERROSA_TEST_CONTAINERS=1` is never set in any GitHub Actions workflow — S3-backed compaction tests never run automatically | Add container-integration job to nightly-fuzz.yml with MinIO via Docker Compose | M |
| 20 | G-20 | Storage | `specs/storage.md` §Not Yet Implemented table is stale — lists four completed features as unimplemented | Update §Follow-on Work table in specs/storage.md to reflect current status | S |

---

## 4. Cross-Cutting Themes

**Auth and access control** appears in every zone reviewed but is documented coherently in none. The SASL PLAIN path is tested in isolation, but `FERROSA_AUTH_DISABLED=true` bypasses auth at both the web middleware layer and the CQL connection layer in all known deployments. The auth middleware has zero tests. There is no unified auth architecture spec, no cross-zone threat model for SASL/TLS vs. HTTP Basic vs. Bolt auth, and no live integration test exercising auth across all three protocol surfaces. Auth is addressed in `design-cql-role-auth-rollout.md` for CQL and `threat-model.md` for web, but these are not connected to each other or to the graph/SPARQL surfaces.

**Live-cluster correctness vs. deterministic coverage** is the most pervasive theme. The codebase has excellent unit and in-process Jepsen coverage, but real multi-node correctness assertions are uniformly thin: cluster coordinator tests are skipped in CI, Accord live-cluster tests are `todo!` stubs, the Jepsen smoke tier is excluded from every workflow, and the driver test harness targets a single node. C4 (live Jepsen) and C8 (all-driver cluster) remain unprovisioned in any CI workflow.

**Spec staleness and reconciliation debt** is systemic. At least four specs contain materially wrong information: `specs/storage.md` lists implemented features as unimplemented; `specs/testing.md` describes Suites 5 and 6 as future work when both are complete; ADR-006 states ALLOW FILTERING should be rejected when code accepts it; `specs/project-plan-gap-closure.md` Sprint 3 marks Accord wiring as unstarted when `status.md` claims Accord complete. This creates misleading signals for agents and new contributors.

**Observability breadth without test depth.** Extensive implementation (25+ spans, 13 virtual tables, alert evaluator), but multiple virtual tables are stubs returning empty data, the flamechart endpoint's designed IP-whitelist mitigation is inert, and `FERROSA_AUTH_DISABLED` exposes all observability endpoints without auth in all known configurations.

**Direct storage reads bypassing WritePath in cluster mode.** Graph executor, SPARQL executor, and ferrosa-memory edge writes all bypass the cluster routing layer, returning stale or incomplete data in cluster mode. Gap-closure Sprint 2 addresses graph/SPARQL reads. ferrosa-memory write bypass requires MERGE first. No spec documents the intended routing architecture for non-CQL data paths in cluster mode.

---

## 5. Spec-Hygiene Issues

- **`specs/storage.md` §Not Yet Implemented** lists four completed features (S3 upload wiring, Manifest CAS, recovery, S3 integrity verification) as unimplemented. Misleads contributors into re-implementing completed work.
- **`specs/testing.md`** is dated 2026-03-22. Suites 5 (secondary indexes) and 6 (Accord) are described as future work; both are complete with hundreds of tests.
- **ADR-006 §3** states ALLOW FILTERING should return `ERROR(Invalid)`. Both `cql.md` and `router.rs` explicitly support it. The ADR has no addendum noting the reversal.
- **`specs/project-plan-gap-closure.md` Sprint 3** (Accord wiring) is listed as high-risk/unstarted. `specs/status.md` states Accord is complete. One of them is wrong.
- **`specs/gaps-2026-04-03.mc`** uses a `.mc` file extension. This is not rendered by GitHub or most documentation tooling. Should be renamed to `.md`.
- **`specs/status.md`** references spec paths under `superpowers/specs/` that do not exist in the current directory tree. These links are broken.
- **`specs/sstable.md` §Phase 2** marks `ferrosa-sstable-dump` and `ferrosa-sstable-import` as deferred. Both tools exist in `ferrosa-sstable/src/bin/`.
- **Gate A and Gate B** (clustering-shape validation and self-readback) are critical correctness invariants noted only in `specs/ARCHITECTURE.md`. Absent from `specs/sstable.md` and `specs/storage.md`, which are the specs maintainers consult when modifying the flush path.

---

## 6. Recommended Next 3 Sprints of Documentation Work

### Doc Sprint D1 (1 week): CI health and auth — immediate risk reduction

1. Fix `release.yml` CI defect: add `--exclude ferrosa-jepsen --exclude ferrosa-loadgen` to the test step (G-01). Removes spurious failures on every release tag.
2. Fix `nightly-fuzz.yml` CI defect: add `--exclude ferrosa-loadgen` (G-02). Stops spurious nightly regression PRs.
3. Add ferrosa-jepsen unit tests to ci.yml: `cargo test -p ferrosa-jepsen --lib` (G-16). Zero infrastructure cost, immediate regression protection for linearizability checker.
4. Create `specs/auth-architecture.md` — unified model covering CQL SASL, graph HTTP auth, SPARQL auth, column-level permissions, role hierarchy, GRANT/REVOKE, and the `FERROSA_AUTH_DISABLED` semantics. Cross-reference `design-cql-role-auth-rollout.md` and the existing threat models. (~500 words)
5. Add auth middleware tests to `ferrosa/src/web/auth.rs` — at minimum: no-auth returns 401, valid credential returns 200, non-admin returns 403 (G-03).
6. Rename `specs/gaps-2026-04-03.mc` to `specs/gaps-2026-04-03.md`.

### Doc Sprint D2 (1 week): Stale spec reconciliation and storage invariants

1. Update `specs/storage.md` — (a) replace §Not Yet Implemented with accurate status (G-20); (b) add §Startup Quarantine invariant covering `{table_dir}/quarantine/` mechanic and the open OPS-3 S3-marker gap (G-13); (c) add §PendingUploadsLog and §CdcReader subsections; (d) add §Gate A and Gate B with failure modes (G-07). Single highest-leverage file edit in the project — one file closes four gaps.
2. Update `specs/sstable.md` — add §SSTableWriter Invariants (Gate A, Gate B, WriteOptions.verify_output semantics); move CLI tools from Phase 2 to complete (G-07, hygiene).
3. Update `specs/testing.md` — mark Suites 5 and 6 as complete; document actual live-cluster coverage gaps.
4. Add addendum to ADR-006 §3 noting the ALLOW FILTERING reversal and rationale (hygiene).
5. Reconcile `specs/project-plan-gap-closure.md` Sprint 3 with `specs/status.md` Accord completion claim (hygiene).
6. Create `specs/cluster-routing-multimodel.md` — document the intended WritePath routing for graph executor and SPARQL reads in cluster mode (G-04/G-05 prerequisite).

### Doc Sprint D3 (1 week): Cluster architecture documentation

1. Create `specs/hints-architecture.md` — hint file format, segment rotation, per-peer capacity, delivery ordering, `needs_repair` semantics, token-remapping fix for topology change (G-09). Unblocks the topology-change bug fix and FMEA F19 repair trigger.
2. Create `specs/anti-entropy-architecture.md` — repair trigger conditions, initiating node selection, Merkle exchange protocol, result feedback into StreamSender (G-10).
3. Create `specs/batchlog-coordinator.md` — two-phase protocol, batchlog replica selection, write ordering, cleanup on success, double-apply risk on partial failure (G-14).
4. Create `specs/in-process/gap-formation-state-machine.md` — track FormationState implementation (Forming, Degraded variants); link specific files to change; define acceptance criteria per state transition (G-15).
5. Create `specs/write-correctness-invariants.md` — full correctness gate chain: Gate A → Gate B → compaction readback → startup quarantine → S3 SHA-256. Include 2026-04-17 incident timeline so future contributors understand why these gates exist and what breaks if disabled.
