# ADR-003: Raft for Metadata, Tunable CL for Data, Transactions Deferred

> Date: 2026-03-11
> Status: Accepted

## Context

Cassandra evolved through three consensus approaches: gossip + hinted handoff, Paxos (LWT), and Accord (5.x). Ferrosa needs a consensus model for metadata and consistency for data operations.

## Decision

- Raft (via `openraft`) for metadata consensus: schema, topology, token assignment
- Cassandra-compatible tunable consistency levels for data operations
- Distributed transactions (Accord-like) deferred as a research item

## Rationale

- Raft is battle-tested (TiKV, CockroachDB, etcd) with mature Rust implementations
- Replaces Cassandra's most complex subsystem (TCM/Paxos) with something well-understood
- Tunable CL is core to Cassandra's value proposition — must preserve
- Most Cassandra workloads don't use LWT — transactions can come later
- Single cluster-wide Raft group is sufficient for metadata (infrequent relative to data ops)

## Consequences

- No lightweight transactions (LWT / IF NOT EXISTS) initially
- Must clearly communicate what's missing to potential users
- Raft log persistence on ephemeral nodes requires S3 snapshot backup
- 3-5 Raft voter nodes; remaining nodes are learners

## Research Items

- Accord protocol for distributed transactions
- Tempo / Janus as alternative consensus
- EPaxos for leaderless consensus
- HLC / TrueTime-like clock synchronization for cross-DC
