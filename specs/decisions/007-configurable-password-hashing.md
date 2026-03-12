# ADR-007: Configurable Password Hashing

> Date: 2026-03-12
> Status: Accepted

## Context

Password hashing for database authentication requires balancing security (resistance to offline brute-force) against operational performance (login latency, CPU cost). Different deployment environments have different threat models — a database behind a VPN in a private cloud has different needs than one exposed to the internet.

bcrypt is the most widely deployed password hashing algorithm and is used by Cassandra. It is adequate but showing its age: it has a 72-byte input limit, and it is more GPU-friendly than newer memory-hard algorithms like argon2id.

## Decision

Password hashing in ferrosa-schema is configurable:

- **Default**: bcrypt with cost 12 — battle-tested, well-understood, good baseline
- **Configurable**: `FERROSA_AUTH_HASHER=argon2id` switches to argon2id (memory-hard, OWASP recommended) for new password hashes
- **Self-describing hashes**: The hash string embeds its algorithm (`$2b$...` for bcrypt, `$argon2id$...` for argon2id), so verification auto-detects the algorithm regardless of configuration
- **Auto-upgrade on login**: When a user authenticates successfully and the stored hash uses a different algorithm than the configured hasher, the password is re-hashed with the configured algorithm and saved. This allows seamless migration by changing a config value.

## Rationale

- **Pragmatic default**: bcrypt is good enough for most deployments and avoids surprising operators with unfamiliar algorithms
- **Simple upgrade path**: Change one environment variable, and all users migrate to argon2id on their next login — no batch migration needed
- **No breaking changes**: Existing bcrypt hashes keep working regardless of config
- **Operator control**: If auth is a performance bottleneck (high-throughput environments), operators can tune the cost parameter or choose the faster algorithm. The trade-off (reduced offline brute-force resistance) is explicit.

## Consequences

- Two password hashing dependencies: `bcrypt` and `argon2` crates
- Hash verification must inspect the hash prefix to select the correct algorithm
- Auto-upgrade means `authenticate()` can trigger a schema mutation (re-hashing), which is a write operation during a read-like call
- Argon2id parameters (memory, iterations, parallelism) need sensible defaults and documentation

## Configuration

| Env Var | Values | Default |
|---------|--------|---------|
| `FERROSA_AUTH_HASHER` | `bcrypt`, `argon2id` | `bcrypt` |
| `FERROSA_AUTH_BCRYPT_COST` | 4-31 | 12 |
| `FERROSA_AUTH_ARGON2_MEMORY_KIB` | integer | 65536 (64 MB) |
| `FERROSA_AUTH_ARGON2_ITERATIONS` | integer | 3 |
| `FERROSA_AUTH_ARGON2_PARALLELISM` | integer | 4 |
