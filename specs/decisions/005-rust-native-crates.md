# ADR-005: Rust-Native Crates with Java as Behavioral Oracle

> Date: 2026-03-11
> Status: Accepted

## Context

Ferrosa is informed by Cassandra's architecture but must avoid inheriting its accidental complexity. Three approaches were considered: strangler fig (replace Java components via FFI), parallel build (clean Rust, Java as oracle), and bottom-up libraries (independent crates).

## Decision

Hybrid of parallel build + bottom-up crates. Build independent Rust crates with clean architecture, using the refactored Java Cassandra as a behavioral oracle and test reference. The Java phase is analysis only, not a deliverable.

## Rationale

- Clean Rust architecture with proper ownership boundaries, not a Java transliteration
- Each crate is independently testable and usable
- `ferrosa-sstable` is immediately useful as a standalone migration tool
- Java analysis identifies what's essential vs. accidental complexity before writing Rust
- Characterization tests validate both implementations against the same behavioral spec
- No FFI complexity or impedance mismatch between GC'd and non-GC'd worlds

## Consequences

- Longer time to first complete system (must build all crates)
- Must deeply understand Cassandra's behavior without mechanically copying its code
- Risk of subtle semantic divergence — mitigated by characterization test suite
- Two parallel workstreams require coordination (Track 1 findings feed Track 2 design)

## Track 1 Analysis Outputs

- DSM analysis → module map, dead code, dependency cycles
- Behavioral characterization → edge cases at every CL level
- "What we wouldn't do" ADR → scope reduction
- SSTable format deep dive → byte-level spec
- CQL protocol spec → message-level documentation
