# Ferrosa Specs

> Last updated: 2026-06-04
> Status: Internal evidence index, not public release guarantees

These documents separate implemented evidence from proposals, active work, and
verification plans. Public-facing docs live under [`docs/`](../docs/) and should
keep a developer-preview posture unless a claim has current source/test evidence.

## Directory Structure

```text
specs/
  *.md                  Current architecture, threat, FMEA, DSM, and roadmap docs
  proposed/             Draft designs and investigations; not implemented claims
  todo/                 Open bugs and feature work awaiting implementation or triage
  in-process/           Actively owned work only
  implemented/          Implementation evidence awaiting verification/archive
  verified-test-plan/   Ambiguous items that need live verification before closure
  coverage/             Source/spec/test coverage audits
  decisions/            Architecture Decision Records (ADRs)
  archive/              Historical plans, analyses, and verified bugs
    project-plans/      Completed project plans, compiled plans, TDD plans
    analysis/           Completed FMEA, DSM, threat models, test specs, evaluations
    bugs-verified/      Fixed bugs with retained repro/evidence notes
```

## Public Claim Rules

Do not present these as public guarantees until linked evidence exists:

- Jepsen-verified correctness or production-ready cluster mode.
- Full Cassandra/CQL or full Redis/RESP compatibility.
- Arbitrary-query `SUBSCRIBE`/CDC delivery.
- Complete observability table backing.
- Binary vector sidecars for HNSW/IVFFlat; the current vector sidecars are JSON.

Keep unsupported engineering topics in `proposed/`, `todo/`, or
`verified-test-plan/` rather than documenting them as completed behavior.

## Architecture Specs (Current)

| Spec | Description |
|------|-------------|
| [Overview](overview.md) | High-level system overview and design principles |
| [Architecture](ARCHITECTURE.md) | One-page contributor codebase map |
| [Components](components.md) | Crate architecture, dependency graph, responsibilities |
| [Data Flow](data-flow.md) | Write path, read path, compaction, S3 lifecycle |
| [SSTable](sstable.md) | BTI format, trie encoding, I/O traits, compression |
| [Storage](storage.md) | Storage engine: memtable, flush, compaction, S3, cache |
| [CQL](cql.md) | CQL native protocol v4/v5, parser, query routing, LWT, pagination |
| [Testing](testing.md) | Test infrastructure and suites |
| [Cancel Safety](cancel-safety-conventions.md) | Async cancel safety conventions |

## Feature Specs and Proposals

| Spec | Description | Status |
|------|-------------|--------|
| [Secondary Index Pipeline](secondary-index-pipeline.md) | Query integration, sidecar persistence, vector indexes | Implemented evidence |
| [Full-Text Indexing](fulltext-index-architecture.md) | Inverted index sidecars, analyzer pipeline, BM25, `fts_match()` | Implemented evidence |
| [Anti-Entropy Repair](anti-entropy-repair-architecture.md) | Merkle-then-stream repair with byte+partition-bounded fetch and bounded-storage memory model | Implemented evidence (v0.13.0, operator-initiated) |
| [Remote Index Build Backend](remote-index-build-backend.md) | Standalone `ferrosa-index-builder` binary and backend modes | Design / open work tracked in `todo/` |
| [Hierarchical Vector Quantization](proposed/hierarchical-vector-quantization.md) | Quantized NVMe-resident ANN design with CockroachDB C-SPANN lessons and scope outlines | Proposed; current HNSW/IVFFlat sidecars are JSON |
| [HVQ / C-SPANN Implementation Blueprint](in-process/hvq-cspann-implementation-blueprint.md) | Multi-agent implementation blueprint and TDD acceptance spec for quantized prefix-scoped ANN | In-process blueprint pending owner decisions |
| [HVQ C-SPANN Implementation Plan](plans/hvq-cspann-implementation-plan.md) | Work-packet DAG for Kanban/worktree implementation | Plan pending owner decisions |
| [Bounded SSTable Reader Memory](proposed/p0-bounded-sstable-reader-design.md) | Descriptor-backed views, engine-wide reader pool, streaming range/repair reads, compaction gate | Implemented in 0.13 hardening set; repair full-overlap fan-in remains a gate |
| [Repair Fuzz Harness](proposed/repair-fuzz-harness-design.md) | Shared generators and proptest harness for repair/storage convergence and memory bounds | Proposed; integration and CI wiring pending |
| [Self-Healing Controller](proposed/self-healing-controller-design.md) | Deterministic autonomous repair controller over bounded remediation primitives | Proposed; depends on repair fan-in hardening |
| [UCS Compaction](ucs-compaction-architecture.md) | Unified Compaction Strategy | Active design/implementation |
| [Cluster Formation](cluster-formation-architecture.md) | Cluster formation state machine and protocol | Active hardening |
| [Observability](observability-architecture.md) | Metrics, tracing, telemetry pipeline | Active; not complete public claim |
| [Runtime Isolation](runtime-isolation-architecture.md) | Tokio runtime separation for latency-sensitive paths | Active |
| [SPARQL Endpoint](sparql-endpoint-architecture.md) | SPARQL 1.1 query endpoint over graph data | Active |
| [Jepsen E2E](jepsen-e2e-test-plan.md) | Verification plan for distributed behavior | Test plan, not completed evidence |
| [UCS Load Test](ucs-load-test-architecture.md) | Load testing framework for UCS compaction | Proposed/active |
| [PITR Branch/Copy](pitr-branch-copy-architecture.md) | Copy-on-write object-store branches from PITR checkpoints | Draft |

## Threat Models

| Spec | Scope |
|------|-------|
| [System-Wide](threat-model.md) | Ferrosa STRIDE analysis |
| [CQL B/C](threat-model-cql-bc.md) | CQL parser, routing, prepared cache |
| [Net/Cluster](threat-model-net-cluster.md) | Internode protocol, Raft, pair mode |
| [Cluster Formation](threat-model-cluster-formation.md) | Formation protocol security |
| [Graph](threat-model-graph.md) | Graph engine, HTTP endpoint |
| [Observability](observability-threat-model.md) | Telemetry pipeline security |

## Failure Mode Analysis and DSM

| Spec | Scope |
|------|-------|
| [Cluster Formation FMEA](fmea-cluster-formation.md) | Formation protocol failure modes |
| [Observability FMEA](observability-fmea.md) | Telemetry pipeline failure modes |
| [Cluster Formation DSM](dsm-cluster-formation.md) | Formation module dependencies |
| [Controller Refactor DSM](dsm-controller-refactor.md) | Controller module restructuring |
| [UCS Compaction Analysis](ucs-compaction-analysis.md) | Compaction subsystem analysis |

## Roadmaps and Project Plans

| Plan | Scope | Status |
|------|-------|--------|
| [Next Sprints](project-plan-next-sprints.md) | Correctness, repair, Jepsen, and follow-up hardening | Active/open |
| [UCS Compaction](project-plan-ucs-compaction.md) | UCS implementation plan | Active/open |
| [Cluster Formation](project-plan-cluster-formation.md) | Formation state machine implementation | Active/open |
| [Unified Roadmap](project-plan-unified.md) | Ferrosa ecosystem roadmap | Mixed roadmap; verify before public claims |

## Work Item Buckets

| Directory | Meaning |
|-----------|---------|
| [proposed/](proposed/) | Design proposals and investigations |
| [todo/](todo/) | Open bugs/features awaiting implementation or triage |
| [in-process/](in-process/) | Actively owned work only |
| [implemented/](implemented/) | Implementation evidence awaiting verification/archive |
| [verified-test-plan/](verified-test-plan/) | Verification plans for ambiguous claims/fixes |
| [archive/bugs-verified/](archive/bugs-verified/) | Fixed bugs retained with repro/evidence notes |

## Architecture Decision Records

| ADR | Decision |
|-----|----------|
| [001](decisions/001-write-behind-s3.md) | Write-behind async S3 storage model |
| [002](decisions/002-cql-only-compat.md) | CQL client compat only, own internode protocol |
| [003](decisions/003-raft-metadata.md) | Raft for metadata, tunable CL for data |
| [004](decisions/004-layered-sstable.md) | Layered SSTable: read Big+BTI, write BTI, future native |
| [005](decisions/005-rust-native-crates.md) | Rust-native crates + Java as behavioral oracle |
| [006](decisions/006-auth-first-schema.md) | Auth-first schema design |
| [006b](decisions/006-cql-architecture.md) | CQL architecture |
| [007](decisions/007-configurable-password-hashing.md) | Configurable password hashing (bcrypt/argon2id) |
| [008](decisions/008-audit-first-schema.md) | Audit-first schema design |
| [009](decisions/009-pluggable-secrets-provider.md) | Pluggable secrets provider (env/AWS SM/Vault) |
| [010](decisions/010-production-mode.md) | Production mode — mandatory encryption, fail-closed |
| [011](decisions/011-s3-native-pitr.md) | S3-native PITR — metadata snapshots + commit log archiving |
| [012](decisions/012-pitr-branches-copy-on-write.md) | PITR branches — copy-on-write object references |

## Archive

Completed work is preserved for auditability under `archive/project-plans/`,
`archive/analysis/`, and `archive/bugs-verified/`.
