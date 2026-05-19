---
type: todo
priority: P3
status: draft
created: 2026-05-16
updated: 2026-05-16
affected-versions: ferrosa-memory main as of 2026-05-16
---

# Bug: `smoke-18765.sh` defaults to a TLS cert path that does not exist

## Why this is a Ferrosa bug

`ferrosa-memory/scripts/smoke-18765.sh` is the project's primary
"is the cluster healthy" smoke test. Out of the box it fails on a fresh
clone before reaching any actual probe:

```
$ bash scripts/smoke-18765.sh
FAIL: TLS cert not readable: /home/bkearns/src/ferrosa-suite/ferrosa-memory/.runtime/tls.crt
```

The cert pair actually lives at `.runtime/tls/mcp.crt` and
`.runtime/tls/mcp.key`. The smoke script's default
`FERROSA_MEMORY_TLS_CACERT="${REPO_ROOT}/.runtime/tls.crt"` is stale.

## Observed on

- `ferrosa-memory` repo at `~/src/ferrosa-suite/ferrosa-memory/`, 2026-05-16.
- `scripts/smoke-18765.sh` lines 18-19.

## Suspected scope

The smoke script was written when the cert lived at the .runtime root.
At some point the cert pair was moved into `.runtime/tls/` (likely
when both `mcp.crt` and `mcp.key` were generated as a pair — the
filename also changed from `tls.crt` to `mcp.crt`).

## Fix shape

One of:

1. Update the default in the smoke script:
   `FERROSA_MEMORY_TLS_CACERT="${REPO_ROOT}/.runtime/tls/mcp.crt"`.
2. Symlink `.runtime/tls.crt` → `.runtime/tls/mcp.crt` for backwards
   compatibility, and update the script default in the next release.
3. Add a fallback chain in the script: try `.runtime/tls/mcp.crt` first,
   then `.runtime/tls.crt`, then fail with a clear message listing
   both paths checked.

Option 1 is cleanest if no other tooling depends on the old path.

## Secondary issue: TLS scheme assumption

The script also defaults `BASE_URL=https://127.0.0.1:18765`, but the
in-tree `ferrosa-memory-http-podman.toml` runtime config sets
`transport = "http"` and `require_tls = false`, so the MCP server
listens with plain HTTP on 18765. Smoke users must override with
`FERROSA_MEMORY_BASE_URL=http://127.0.0.1:18765` AND
`FERROSA_MEMORY_TLS_CACERT=insecure` to get past the TLS handshake.
Consider matching the script default to the in-tree compose config, or
adding a guard that detects the scheme from the running endpoint.
