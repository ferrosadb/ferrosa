# Testing Strategy

> Last updated: 2026-03-11
> Status: Approved

## Overview

Ferrosa uses on-demand Sprite/fly.io clusters for integration testing and Hunter (DataStax) for automated performance regression detection. The Cassandra Java baseline is the floor to beat — Ferrosa should always be faster.

## Test Infrastructure

```mermaid
flowchart LR
    subgraph "CI (GitHub Actions)"
        Push[Push to branch]
        Unit[cargo test]
        Lint[clippy + fmt]
        Push --> Unit --> Lint
    end

    subgraph "Nightly"
        Spin[Spin up Sprites]
        Run[Run suites 1-4]
        Collect[Collect metrics → S3]
        Hunt[Hunter: analyze CSV]
        Tear[Tear down Sprites]
        Spin --> Run --> Collect --> Hunt --> Tear
    end

    Lint -.->|merge to main| Spin
    Hunt -.->|significant change| Alert[Alert]
```

### Budget: less than $50/month

All clusters are on-demand — spin up for test runs, tear down after.

| Purpose | Nodes | Spec | Est. Cost |
|---------|-------|------|-----------|
| Correctness (Cassandra baseline) | 3 | shared-cpu-2x, 1GB | ~$3/mo |
| Correctness (Ferrosa) | 3 | shared-cpu-2x, 1GB | ~$3/mo |
| Performance baseline | 3 | shared-cpu-4x, 2GB | ~$5/mo |
| Chaos / node kill | 3-5 | shared-cpu-1x, 256MB | ~$1-2/run |

## Performance Philosophy

```mermaid
flowchart TD
    Cassandra[Cassandra Baseline<br/>on same Sprite hardware] -->|floor to beat| Compare
    Ferrosa[Ferrosa Current<br/>nightly run] --> Compare{Hunter:<br/>significant change?}
    Compare -->|no change| OK[Continue]
    Compare -->|improvement| Record[Record new baseline]
    Compare -->|regression| Investigate[Investigate immediately]
    Compare -->|slower than Cassandra| Bug[File as bug]
```

- Cassandra baseline is the **floor**, not the target
- Regressions measured against **Ferrosa's own prior performance**
- No hardcoded threshold — Hunter detects statistically significant changes
- 5% relative change filter for noise (from Hunter paper)
- Need 30+ data points for reliable detection

### Metrics Collected

- Throughput (ops/sec)
- p50, p99, p999 latency
- S3 upload lag (Ferrosa-specific)
- Cache hit ratio (Ferrosa-specific)

## Suite 1: Data Integrity

No data loss under any condition. This is the #1 requirement.

| Test | Validates | Method |
|------|-----------|--------|
| Write + read back | Readable at all CL levels | YCSB workload A |
| Node kill during write | Survives single node death | QUORUM write, kill mid-stream |
| Node kill + recovery | Complete data from S3 | Kill, replace, verify |
| Multi-node failure | Survives RF-1 failures | Sequential kill |
| Compaction correctness | No data loss or corruption | Write, compact, verify all rows |
| S3 upload verification | S3 matches local | Checksum comparison |
| Cold start from S3 | Serves correctly with no local data | Wipe local, restart |

## Suite 2: Performance Baselines

YCSB workloads on identical Sprite hardware:

| Workload | Mix | Models |
|----------|-----|--------|
| A | 50% read, 50% update | Session store |
| B | 95% read, 5% update | Photo tagging |
| C | 100% read | User profile cache |
| D | 95% read, 5% insert | Latest status |
| F | 50% read, 50% read-modify-write | User database |

## Suite 3: Chaos / Failure Injection

Sprites are ideal — Firecracker VMs with fast spin-up/kill.

| Scenario | Action | Verify |
|----------|--------|--------|
| Node crash | Kill Sprite VM | Cluster continues, data intact |
| Network partition | iptables isolation | Correct CL behavior |
| Slow node | tc qdisc latency | No cluster degradation |
| Disk full | Fill ephemeral storage | S3 data accessible |
| S3 unavailable | Block S3 endpoint | Local writes continue |
| Rolling restart | One node at a time | Zero downtime |
| Multi-kill | Kill 2 of 3 nodes | Sub-quorum writes rejected |

## Suite 4: CQL Compatibility

- **Drivers**: DataStax Java/Python/Go, gocql, scylla-rust-driver
- **Protocol**: All CQL v5 message types and error responses
- **DDL**: CREATE/ALTER/DROP keyspaces, tables, indexes
- **DML**: INSERT, UPDATE, DELETE, SELECT at all CL levels
- **Types**: All CQL types including collections, UDTs, tuples
- **cqlsh**: Connects and operates normally

## Pre-1.0 Test Backlog

Required before declaring production readiness:

```mermaid
mindmap
  root((Pre-1.0))
    Data Model
      Tombstone & TTL handling
      Large partition handling
      Counter correctness
      Timestamp conflict resolution
    Distributed
      Hinted handoff
      Read repair verification
      Anti-entropy repair
      Range queries & pagination
    S3 Failure Modes
      Throttling / 429
      LIST eventual consistency
      Partial upload failure
      Cost profiling
    Operational
      Rolling upgrade
      Schema evolution under load
      Compaction under heavy writes
      Backup/restore from S3
      Soak test 24-72hr
      Memory pressure / OOM
    Migration
      SSTable import correctness
```

Sprites' small memory footprint naturally triggers memory pressure issues that would take deliberate effort to reproduce on larger instances.

## Related Specs

- [Data Flow](data-flow.md) — write/read paths and durability mitigations
- [Components](components.md) — crate architecture
