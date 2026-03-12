# ADR-010: Production Mode — Mandatory Encryption

> Date: 2026-03-12
> Status: Accepted

## Context

Ferrosa has multiple encryption and security controls that can be individually disabled for development convenience: plaintext S3 via `FERROSA_S3_ALLOW_HTTP`, no CQL TLS, no internode TLS, unencrypted local disk, default superuser credentials, env-var secrets. Each flag is reasonable in isolation, but in production every one of these relaxations is a security risk.

Operators need a single switch that says "enforce everything" — and a startup that refuses to run if any requirement is missing. Individual flag discipline doesn't scale; one misconfigured env var in a deployment manifest shouldn't silently downgrade security.

## Decision

Ferrosa supports `FERROSA_MODE=production|development`:

**Production mode** refuses to start unless:

1. CQL TLS is configured (certificate + key)
1. Internode mutual TLS is configured
1. S3 endpoint uses HTTPS (`FERROSA_S3_ALLOW_HTTP=true` is rejected)
1. Local data directory is on an encrypted filesystem
1. Default superuser password is not in use (`FERROSA_SUPERUSER_PASSWORD` required)
1. Secrets provider is not `env` (warning, not hard block)

**Development mode** (default) runs the same checks but logs warnings instead of refusing to start.

A `validate_production_requirements()` function runs at startup and returns a typed list of `ProductionViolation` values. In production mode, any violation is fatal.

## Rationale

- **Fail-closed in production**: Better to refuse to start than to run insecurely. A node that won't start is immediately visible in monitoring; a node running without TLS is silently vulnerable.
- **Development ergonomics**: Default mode is permissive — developers don't need to configure TLS and encrypted disks for local testing.
- **Single enforcement point**: One env var controls everything. No need to audit 6 separate flags across deployment manifests.
- **Incremental adoption**: Checks are added as features land. Chunk A validates S3 HTTP, local disk, superuser password, and secrets. CQL TLS and internode TLS checks arrive with those crates.
- **Escape hatch**: `FERROSA_ALLOW_UNENCRYPTED_DISK=true` exists for environments where encryption is handled at a layer Ferrosa can't detect (hardware encryption, hypervisor-level encryption).

## Consequences

- Development default means production mode is opt-in — operators must explicitly set `FERROSA_MODE=production`
- Startup validation adds a few hundred ms (filesystem encryption detection, S3 endpoint check)
- The validation function needs access to config from multiple crates — lives in ferrosa-schema but accepts config from ferrosa-cql and ferrosa-net as they're implemented
- Docker/Kubernetes deployment templates must document `FERROSA_MODE=production` as required for production use

## Configuration

| Env Var | Values | Default |
|---------|--------|---------|
| `FERROSA_MODE` | `development`, `production` | `development` |
| `FERROSA_ALLOW_UNENCRYPTED_DISK` | `true`, `false` | `false` |
