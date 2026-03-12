# ADR-008: Audit-First Schema Design

> Date: 2026-03-12
> Status: Accepted

## Context

Audit logging is typically bolted on after the core system ships. This leads to incomplete coverage — some code paths emit events, others don't, and operators can't trust that the audit trail is complete. The same pattern played out with authentication in many databases, which is why Ferrosa adopted auth-first design (ADR-006).

The ferrosa-schema crate is the authority for all schema mutations and authentication. Every DDL operation and every login attempt flows through its registry. This makes it the natural single point for audit event emission — if the audit hook is in the registry from day one, there is no code path that can modify schema or authenticate without producing a record.

## Decision

Audit logging is a first-class concern in ferrosa-schema Chunk A:

- **`AuditEvent` enum**: Covers all auth events (success, failure, throttle, lockout, password change) and all DDL events (create/alter/drop keyspace, table, role; grant/revoke permissions)
- **`AuditSink` trait**: Pluggable destination — one method (`emit`), no error return, must not block or fail schema operations
- **Default sink**: `LogAuditSink` emits structured JSON via the `tracing` crate at INFO level, target `ferrosa::audit`
- **Emit points**: Every registry method that modifies state or checks credentials calls `self.audit_sink.emit()` after the operation
- **`TestAuditSink`**: In-memory collector for testing — asserts that every operation produces the expected audit event
- **Source address**: `AuditContext` carries optional `SocketAddr` set by the CQL layer; the schema crate propagates it into events without depending on network concepts

## Rationale

- **No audit gaps**: The type system and API design ensure every mutation path emits an event. Adding a new registry method without an audit emit point is a visible omission in code review.
- **Same pattern as auth**: Auth-first (ADR-006) proved that baking security into the API from the start prevents bypass paths. Audit-first applies the same principle to observability.
- **Pluggable from day one**: The `AuditSink` trait means the Chunk A implementation (structured logging) works immediately, while future chunks can add richer backends (system keyspace table, S3 archival) without changing the registry.
- **Zero-cost for operators who don't need it**: The `LogAuditSink` is a structured log line — operators who don't need audit can filter it out via `RUST_LOG`. No database overhead, no extra storage.
- **Compliance-ready**: Structured JSON audit events with timestamp, actor, source IP, and operation details are the foundation for SOC 2, HIPAA, and PCI DSS audit trail requirements.

## Consequences

- Chunk A scope increases by ~200 lines (event types, trait, log sink, test sink)
- Every registry method has an `emit()` call — ~1 line per method, but must be reviewed for completeness
- `Schema::new()` now takes `Box<dyn AuditSink>` — all callers (including tests) must provide a sink
- The `tracing` crate becomes a direct dependency of ferrosa-schema (already indirect via ferrosa-storage)
- Future audit sinks (system table, S3) must implement a simple trait rather than needing to instrument the registry

## Alternatives Considered

- **Audit as a wrapper/middleware**: Wrap the `Schema` struct in an `AuditedSchema` decorator that intercepts method calls. Risk: decorator must be kept in sync with every new method; easy to miss one. The baked-in approach is safer.
- **Audit in the CQL layer only**: The CQL layer has more context (client IP, query text) but doesn't cover internal operations (bootstrap, Raft-driven schema replication). The schema crate is the single chokepoint for all mutations.
- **Audit in Chunk B or later**: Defers the work but creates the same retrofit problem as deferred auth — code paths accumulate without audit hooks, and adding them later requires touching every method.
