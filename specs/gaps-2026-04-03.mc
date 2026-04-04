# Testing Gaps — 2026-04-03

## Scope

This review compared the current test suite against spec documents that either:

- define explicit test cases or named tests,
- claim a feature is implemented and covered by tests, or
- describe failure-mode / system-level validation that should map to automated coverage.

Pure ADRs and roadmap-only docs were not treated as coverage obligations unless they named concrete tests.

## High-confidence gaps

### 1. Accord live-cluster correctness coverage is still incomplete

The largest gap is between the Accord system/correctness specs and the tests that currently exist.

- `specs/accord-test-system.md` explicitly says these checks require a real multi-node cluster and "cannot be run in the TestCluster deterministic scheduler".
- Several named correctness tests are still placeholders in [ferrosa-cluster/tests/correctness.rs](/Users/bkearns/src/ferrosa/ferrosa-cluster/tests/correctness.rs): `batch_atomicity_kill_coordinator`, `recovery_coordinator_activation`, `recovery_coordinator_resolves_inflight`, and `pause_resume_state_convergence` all end in `todo!`.
- The storage-side counterparts in [ferrosa-storage/tests/correctness.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/tests/correctness.rs) also leave `dep_wait_ordering_under_partition` and another `batch_atomicity_kill_coordinator` as `todo!`.
- Some tests with spec names exist, but only in the deterministic harness, e.g. `jepsen_full_nemesis_suite` in [ferrosa-cluster/src/accord/jepsen_nemesis.rs](/Users/bkearns/src/ferrosa/ferrosa-cluster/src/accord/jepsen_nemesis.rs). That does not satisfy the system-spec requirement for real network, process, and timing behavior.
- The live-cluster nemesis tests in [ferrosa-jepsen/tests/nemesis_correctness.rs](/Users/bkearns/src/ferrosa/ferrosa-jepsen/tests/nemesis_correctness.rs) currently validate inject/heal/reachability, but they do not actually assert the correctness properties named in the spec such as "no phantom commits", "linearizability under packet reorder", or "LWT batch atomicity under all nemeses".

Gap summary: the unit and deterministic layers are strong, but multiple Phase C4/C6 system-level correctness gates are not yet backed by completed live-cluster assertions.

### 2. Multi-driver cluster-mode coverage is missing

The driver tests exist, but they do not match the cluster-mode coverage promised in the specs.

- `specs/project-plan-correctness-sprints.md` requires 3-node cluster smoke, token-aware routing for all drivers, prepared-statement invalidation across schema change, and `jepsen_standard_tier_all_drivers`.
- [tests/drivers/docker-compose.drivers.yml](/Users/bkearns/src/ferrosa/tests/drivers/docker-compose.drivers.yml) starts a single Ferrosa node, not a 3-node cluster.
- The driver tests in `tests/drivers/{python,go,node,java,csharp,rust}` do basic CRUD/prepared/ALTER TABLE smoke coverage, but there is no all-driver cluster test for token ownership or coordinator selection.
- The Python driver test explicitly disables token metadata in [tests/drivers/python/test_cql.py](/Users/bkearns/src/ferrosa/tests/drivers/python/test_cql.py), which means it cannot validate token-aware routing at all.
- No implementation was found for `token_aware_routing_all_drivers`, `prepared_stmt_cache_invalidation_all_drivers`, or `jepsen_standard_tier_all_drivers`.

Gap summary: current driver coverage is mostly single-node compatibility smoke; the cluster-behavior assertions from C8 are still missing.

### 3. PITR integration and end-to-end coverage is materially thinner than the PITR FMEA/test plan

PITR has solid unit coverage for individual components, but many of the spec-promised integration/E2E tests are not present.

- Unit coverage exists in [ferrosa-storage/src/commitlog/archiver.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/src/commitlog/archiver.rs), [ferrosa-storage/src/snapshot/manager.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/src/snapshot/manager.rs), [ferrosa-storage/src/restore/manager.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/src/restore/manager.rs), and [ferrosa-storage/src/restore/validation.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/src/restore/validation.rs).
- `specs/pitr-fmea.md` calls for integration tests like high write rate with slow S3 (T3), concurrent compaction + snapshot (T9), restore detects archive gap (T11), GC respects snapshot manifests (T15), retention respects snapshot boundaries (T16), crash recovery during archive (T17), and restore retry on S3 error (T18).
- I did not find corresponding integration tests under `ferrosa-storage/tests/` or top-level `tests/`.
- The end-to-end tests listed in `specs/pitr-fmea.md` (`E1` full PITR cycle, `E2` restore after compaction, `E3` multi-table restore) also do not currently exist as automated tests.

Gap summary: PITR primitives are tested, but the cross-component restore/archive/snapshot workflows described by the FMEA are still under-covered.

### 4. Accord durability / ExclusiveSyncPoint tests are model-level only

The names from the system spec exist, but the coverage is much narrower than the spec text implies.

- `specs/accord-test-system.md` describes `exclusive_sync_point_*` and `durability_service_*` as cluster-level tests involving protocol-log GC, sidecar GC, shard coordination, and stall detection over time.
- The current implementations live in [ferrosa-cluster/src/accord/durability.rs](/Users/bkearns/src/ferrosa/ferrosa-cluster/src/accord/durability.rs).
- Those tests only exercise an in-memory `ExclusiveSyncPoint`/`DurabilityService` bookkeeping model. They do not validate real protocol-log reclamation, sidecar cleanup, background scheduling, or multi-node recovery behavior.

Gap summary: the spec-required lifecycle is only covered by local state-machine tests, not by the storage/cluster integration tests the spec describes.

### 5. Cluster-formation hazard coverage still has open holes

This area has a lot of tests and a large shell-based smoke script, but several hazards called out in the current specs still do not map to dedicated automated checks.

- `specs/hazards-cluster-formation.md` lists still-open items such as:
  - P0-1: DDL applied during the Forming window is not replicated
  - P1-10: RangeRead truncates large tables at a hard-coded limit
  - P2-5: `system.peers` can transiently return empty on lock contention
  - P2-6: ClusterInvite re-broadcast depends on a fixed delay
- [ferrosa-cluster/src/controller/tests.rs](/Users/bkearns/src/ferrosa/ferrosa-cluster/src/controller/tests.rs) covers join approval, progressive join, and bootstrap streaming.
- [tests/docker-smoke.sh](/Users/bkearns/src/ferrosa/tests/docker-smoke.sh) covers useful lifecycle smoke scenarios like add-node, bootstrap, decommission, rebalance, and some FMEA checks.
- I did not find focused automated tests that exercise the still-open hazard cases above.

Gap summary: formation coverage is broad, but it does not yet close the highest-value remaining hazard cases documented in the latest formation scan.

### 6. Some spec-promised live correctness tests are present only as "panic with setup instructions"

A recurring pattern across the repo is that infra-gated tests are counted in plans/specs before they are fully implemented.

- [ferrosa-cluster/tests/correctness.rs](/Users/bkearns/src/ferrosa/ferrosa-cluster/tests/correctness.rs)
- [ferrosa-storage/tests/correctness.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/tests/correctness.rs)
- [ferrosa-jepsen/tests/nemesis_correctness.rs](/Users/bkearns/src/ferrosa/ferrosa-jepsen/tests/nemesis_correctness.rs)

In these files, several tests either:

- immediately panic unless special env vars are set, and/or
- reach `todo!` after the environment check, and/or
- only prove harness plumbing instead of the behavioral invariant named by the spec.

Gap summary: the repo has strong scaffolding for infra-gated validation, but some specs currently overstate how much of that validation is actually executable end-to-end.

## Areas that look comparatively well-covered

These are not gaps, but they help bound the report.

- Storage engine basics from `specs/storage.md` are covered well by [ferrosa-storage/tests/integration.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/tests/integration.rs), [ferrosa-storage/tests/engine_integration.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/tests/engine_integration.rs), [ferrosa-storage/tests/pipeline_integration.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/tests/pipeline_integration.rs), and multiple property suites.
- Compaction S3 pipeline coverage is better than the plans imply: [ferrosa-storage/src/engine.rs](/Users/bkearns/src/ferrosa/ferrosa-storage/src/engine.rs) contains tests for output upload, manifest update, input deletion, metrics, Cassandra-readback, and an end-to-end compaction pipeline.
- Accord deterministic Jepsen/register/bank coverage exists in [ferrosa-cluster/tests/jepsen/](/Users/bkearns/src/ferrosa/ferrosa-cluster/tests/jepsen/) and [ferrosa-cluster/src/accord/jepsen_bank.rs](/Users/bkearns/src/ferrosa/ferrosa-cluster/src/accord/jepsen_bank.rs), even where live-cluster coverage is still missing.

## Recommended next test additions

If this is going to be turned into execution work, the highest-value additions are:

1. Finish the `todo!` live-cluster Accord correctness tests before adding more deterministic ones.
2. Add a real 3-node driver test harness and implement all-driver token-aware / prepared-invalidation tests.
3. Add PITR integration/E2E tests for archive gaps, slow S3, retention boundaries, and full restore workflows.
4. Convert the remaining cluster-formation hazards into focused automated tests rather than leaving them in shell smoke coverage only.
