# ADR-002: CQL Client Compatibility Only

> Date: 2026-03-11
> Status: Accepted

## Context

Cassandra uses multiple internal protocols (gossip, internode messaging, streaming, TCM, Accord). Full wire compatibility would allow Ferrosa nodes to join Cassandra clusters for rolling migration.

## Decision

Implement CQL native protocol v5 for client compatibility. Design Ferrosa's own internode protocol. Ferrosa clusters are standalone — they do not join Cassandra clusters.

## Rationale

- CQL protocol is well-documented and tractable
- Implementing all Cassandra internode protocols (especially TCM + Accord) would be years of work
- Ties Ferrosa to Cassandra's internal protocol evolution
- SSTable import tools handle data migration without mixed clusters
- Freedom to use modern networking (custom binary protocol, potential QUIC)

## Consequences

- Migration from Cassandra requires SSTable import or dual-write, not rolling node replacement
- Harder sell for risk-averse operators who want zero-downtime migration
- Must build migration tooling (`ferrosa-sstable-import`)

## Alternatives Rejected

- **Full wire compat**: Multiple engineer-years for gossip, TCM, Accord, streaming protocols. Bug-for-bug compatibility required.
- **Progressive** (CQL now, internode later): Effectively the same as this decision — "later" would likely never materialize, and designing abstractions for it adds complexity without value.
