# ADR-009: Pluggable Secrets Provider

> Date: 2026-03-12
> Status: Accepted

## Context

Ferrosa requires secrets at startup (S3 credentials, superuser password, TLS certificates) and potentially at runtime (credential rotation). The current approach — reading everything from environment variables — works for development but is problematic in production:

- Environment variables are visible through `/proc/<pid>/environ`, container inspection (`docker inspect`), and crash dumps
- They don't support rotation without process restart
- They encourage operators to embed secrets in deployment manifests, CI/CD pipelines, and shell history
- Multi-cloud deployments use different secrets backends (AWS Secrets Manager, HashiCorp Vault, Kubernetes secrets volumes)

## Decision

Ferrosa defines a `SecretsProvider` trait that abstracts secret retrieval:

- **Trait**: `SecretsProvider` with a single method `get_secret(key) -> Result<Option<String>>`
- **Default**: `EnvSecretsProvider` reads from environment variables (backward compatible, zero configuration)
- **Selection**: `FERROSA_SECRETS_PROVIDER=env|aws-secrets-manager|vault|file` selects the active provider
- **Bootstrap**: The provider is consulted at startup for initial secrets; future providers may support rotation
- **Future providers**: AWS Secrets Manager, HashiCorp Vault, filesystem-mounted secrets (Kubernetes)

## Rationale

- **Backward compatible**: `EnvSecretsProvider` is the default — existing deployments work without changes
- **Single integration point**: All secret access goes through one trait, so adding a new backend doesn't touch core code
- **Minimal bootstrap paradox**: The provider itself is selected by a single env var (`FERROSA_SECRETS_PROVIDER`), and each provider needs only 1-2 env vars to connect to its backend (e.g., `FERROSA_SECRETS_ARN` for AWS). This avoids a chicken-and-egg problem where you need secrets to access the secrets provider.
- **Rotation-ready**: The trait interface supports rotation-aware backends (TTL-based caching, periodic refresh) without changing callers
- **On-prem friendly**: Vault and file providers cover air-gapped and on-prem deployments where AWS services aren't available

## Consequences

- Chunk A ships with only `EnvSecretsProvider` — additional providers are follow-on work
- `Schema::new()` takes `&dyn SecretsProvider` as a parameter — all callers must provide one
- The trait is synchronous (`fn get_secret`) — async backends must block or pre-cache. This is acceptable because secrets are resolved at startup, not on the hot path.
- Provider-specific configuration (Vault address, AWS region, file path) is still read from environment variables — this is the minimal bootstrap needed to reach the secrets backend

## Configuration

| Env Var | Values | Default |
|---------|--------|---------|
| `FERROSA_SECRETS_PROVIDER` | `env`, `aws-secrets-manager`, `vault`, `file` | `env` |
| `FERROSA_SECRETS_ARN` | AWS Secrets Manager ARN | (required for `aws-secrets-manager`) |
| `FERROSA_VAULT_ADDR` | Vault server URL | (required for `vault`) |
| `FERROSA_VAULT_AUTH_METHOD` | `token`, `kubernetes`, `iam` | `token` |
| `FERROSA_SECRETS_DIR` | Mounted secrets directory path | (required for `file`) |

## Secret Keys

| Key | Purpose |
|-----|---------|
| `superuser_password` | Bootstrap superuser password |
| `s3.access_key_id` | S3 access credentials |
| `s3.secret_access_key` | S3 secret credentials |
