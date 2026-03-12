# ADR-006: Auth-First Schema Design

> Date: 2026-03-12
> Status: Accepted

## Context

Most databases bolt authentication and authorization on after the core schema system exists. This leads to code paths that bypass auth checks, internal operations that assume no auth, and a constant game of patching security holes. Cassandra itself added auth as a separate subsystem with its own persistence path, and many internal operations bypass it.

Ferrosa has the opportunity to design auth into the schema registry from day one.

## Decision

Authentication and authorization are part of Chunk A (the first implementation chunk) of `ferrosa-schema`. Every mutating operation on the schema registry requires an `AuthContext` parameter. There is no API path that modifies schema without auth.

Specifically:

- The `Schema` registry API requires `&AuthContext` on every create/alter/drop/grant/revoke operation
- `AuthContext` is obtained by calling `Schema::authenticate(username, password)`
- Permission checking walks the role hierarchy (direct grants, resource inheritance, role inheritance)
- Superusers bypass permission checks but still require an `AuthContext`
- Column masking (Cassandra 5.x) is included in the column metadata model from the start

## Rationale

- **No auth-bypass paths**: The type system enforces that callers pass auth context. You can't accidentally write an unauthenticated code path.
- **Security by design**: Auth is not a feature to be added later — it's a structural property of the API.
- **Column masking for PII**: Built into `ColumnMetadata` from day one, so the data model never needs retrofitting.
- **Cassandra compatibility**: Matches the `system_auth` keyspace that CQL drivers expect.

## Consequences

- Chunk A is larger than it would be without auth (roles, permissions, password hashing, system_auth tables)
- All tests must create an `AuthContext` even for simple schema operations (superuser context for convenience)
- Password hashing adds dependencies (`bcrypt`, `argon2`)
- The auth system is in-memory only until Raft persistence (ferrosa-cluster) is implemented

## Alternatives Considered

- **Auth-aware interfaces in Chunk A, implementation in Chunk B**: Defines the `AuthContext` trait early but ships a `NoAuth` stub. Risk: the stub becomes the de facto path and auth is never properly integrated.
- **Auth as a later chunk**: Simpler initial implementation but creates auth-bypass code paths that must be retrofitted.
