# Ferrosa Specs

> Last updated: 2026-04-09

## Directory Structure

```
specs/
  *.md                  Current architecture and feature specs
  decisions/            Architecture Decision Records (ADRs)
  todo/                 Work items awaiting implementation
  in-process/           Work items being actively implemented
  implemented/          Work items done, awaiting verification
  verified/             Work items verified
  archive/              Completed plans, fixed bugs, historical analysis
    project-plans/      Completed project plans, compiled plans, TDD plans
    analysis/           Completed FMEA, DSM, threat models, test specs
    bugs-verified/      Fixed and verified bugs
```

---

## Architecture Specs (Current)

| Spec | Description |
|------|-------------|
| [Overview](overview.md) | High-level system overview and design principles |
| [Components](components.md) | Crate architecture, dependency graph, responsibilities |
| [Data Flow](data-flow.md) | Write path, read path, compaction, S3 lifecycle |
| [SSTable](sstable.md) | BTI format, trie encoding, I/O traits, compression |
| [Storage](storage.md) | Storage engine: memtable, flush, compaction, S3, cache |
| [CQL](cql.md) | CQL native protocol v5, parser, query routing, LWT, pagination |
| [Testing](testing.md) | Test infrastructure, suites, Jepsen, performance detection |
| [Cancel Safety](cancel-safety-conventions.md) | Async cancel safety conventions |

## Feature Specs (Active)

| Spec | Description | Status |
|------|-------------|--------|
| [Secondary Index Pipeline](secondary-index-pipeline.md) | Query integration, sidecar persistence, vector indexes | Implemented |
| [Full-Text Indexing](fulltext-index-architecture.md) | Inverted index sidecars, analyzer pipeline, BM25, fts_match() | Implemented |
| [Remote Index Build Backend](remote-index-build-backend.md) | Standalone `ferrosa-index-builder` binary, engine backend modes (local/remote/off) | Draft |
| [Hierarchical Vector Quantization](hierarchical-vector-quantization.md) | S3-durable, NVMe-cached tiered Q1/Q2/Q4/Q8/F32 vector search design for HNSW/IVFFlat | Draft |
| [UCS Compaction](ucs-compaction-architecture.md) | Unified Compaction Strategy: density-based levels, fan factor, per-table DDL | New |
| [Cluster Formation](cluster-formation-architecture.md) | Cluster formation state machine and protocol | Active |
| [Observability](observability-architecture.md) | Metrics, tracing, telemetry pipeline | Active |
| [Runtime Isolation](runtime-isolation-architecture.md) | Tokio runtime separation for latency-sensitive paths | Active |
| [SPARQL Endpoint](sparql-endpoint-architecture.md) | SPARQL 1.1 query endpoint over graph data | Active |
| [Jepsen E2E](jepsen-e2e-test-plan.md) | Accord transaction verification: topologies, nemeses, workloads | Approved |
| [UCS Load Test](ucs-load-test-architecture.md) | Load testing framework for UCS compaction | New |

## Threat Models

| Spec | Scope |
|------|-------|
| [System-Wide](threat-model.md) | Ferrosa STRIDE analysis |
| [CQL B/C](threat-model-cql-bc.md) | CQL parser, routing, prepared cache |
| [Net/Cluster](threat-model-net-cluster.md) | Internode protocol, Raft, pair mode |
| [Cluster Formation](threat-model-cluster-formation.md) | Formation protocol security |
| [Graph](threat-model-graph.md) | Graph engine, HTTP endpoint |
| [Observability](observability-threat-model.md) | Telemetry pipeline security |

## Failure Mode Analysis (FMEA)

| Spec | Scope |
|------|-------|
| [Cluster Formation](fmea-cluster-formation.md) | Formation protocol failure modes |
| [HVQ S3 Spill Tier](fmea-hvq-s3-spill-tier.md) | Quantized vector artifact persistence, read-through cache, remote builder failures |
| [Observability](observability-fmea.md) | Telemetry pipeline failure modes |

## DSM Analysis

| Spec | Scope |
|------|-------|
| [Cluster Formation](dsm-cluster-formation.md) | Formation module dependencies |
| [Controller Refactor](dsm-controller-refactor.md) | Controller module restructuring |
| [UCS Compaction](ucs-compaction-analysis.md) | Compaction subsystem: 15 modules, 10 STRIDE threats, 15 FMEA modes |

## Active Project Plans

| Plan | Scope | Status |
|------|-------|--------|
| [Next Sprints](project-plan-next-sprints.md) | S1-S4: hazard fixes, NTS read, correctness, repair, Jepsen | Active |
| [HVQ S3 Spill Tier](project-plan-hvq-s3-spill-tier.md) | S3-durable hierarchical vector quantization with bounded NVMe cache | Draft |
| [UCS Compaction](project-plan-ucs-compaction.md) | 4 sprints: metadata, UCS strategy, integration, equivalence | New |
| [Cluster Formation](project-plan-cluster-formation.md) | Formation state machine implementation | Active |
| [Unified Roadmap](project-plan-unified.md) | Ferrosa ecosystem: core DB, memory, dbaas, Temporal | Active |

## Supporting Docs

| Doc | Purpose |
|-----|---------|
| [Cluster Formation State Machine](cluster-formation-state-machine.md) | State machine diagrams |
| [Cluster Formation Hazards](hazards-cluster-formation.md) | Known hazards and mitigations |

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

## Work Item Pipeline

| Directory | Contents |
|-----------|----------|
| [todo/](todo/) | 4 bugs, 16 feature items pending implementation |
| [in-process/](in-process/) | 5 active bug investigations |
| [implemented/](implemented/) | Awaiting verification |
| [verified/](verified/) | Verified complete |

## Archive

Completed work preserved for reference: `archive/project-plans/`, `archive/analysis/`, `archive/bugs-verified/`.
