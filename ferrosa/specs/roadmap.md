---
crate: ferrosa
doc: roadmap
last_updated: 2026-08-27
---

# ferrosa — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), the in-code `TODO`
(`web/api.rs:475`), and the composition review of `main.rs`.

## Recently addressed

- **One crash-safe local schema owner (FMEA FE-11).** `schema.json` now has a
  discriminator and one bounded streaming publication path. Startup validates
  it before storage opens, quarantines unreadable/legacy-array evidence, and
  returns an error instead of starting with an empty registry. Three verified
  generations are retained; storage's table-only recovery file has a different
  name and cannot win a format race.

- **Preserve TOML internode broadcast advertisements (FMEA FE-10).** A launchd
  three-node cluster used correct, distinct `[internode].broadcast` ports, but
  the binary populated only the resolved `broadcast_addr`; handshakes therefore
  advertised `None`. The loader now preserves the exact configured endpoint.
  Focused unit coverage is green; rebuilding the live cluster is the remaining
  verification gate.

## Now (highest value)

- **Fail loud on partial boot (FMEA FE-1).** When an *enabled* listener
  (Postgres, Flight, graph HTTP, Bolt, SPARQL) fails to bind, the node must
  either crash or report not-ready via `/readyz` — not log-and-continue. Add a
  startup readiness aggregate that distinguishes "disabled by config" from
  "enabled but failed to bind."
- **Default-closed auth in production mode (FMEA FE-3).** When
  `DeploymentMode::Production`, refuse to start (or default `auth_enabled=true`)
  unless the operator made an explicit auth decision, because the same
  `auth_disabled` flag also opens the web `/admin/*` cluster-control surface.

## Next

- **Guard test for startup ordering (FMEA FE-2).** Extract the boot sequence
  into an asserted, testable form (or add an integration test) so a re-ordering
  that breaks recovery (engine/CdcBus, table-registration/replay,
  PeerManager/SessionCore) fails in CI rather than in a live cold start.
- **Readiness gate for bootstrap integrity (FMEA FE-8).** Surface "S3 bootstrap
  failed / index reload failed / UDT-UDF replay failed" as a not-ready signal so
  a node that came up with incomplete schema is visibly degraded.
- **Refuse ephemeral Flight key in production (FMEA FE-4).** With the `flight`
  feature and `DeploymentMode::Production`, require `FERROSA_FLIGHT_SIGNING_KEY`.
- **Wire the deferred web API handler** (`web/api.rs:475` TODO — storage/schema/
  peer_manager/local_node_id into that endpoint).

## Later

- **Configurable, observable shutdown drain (FMEA FE-7).** Make the 30 s drain
  timeout configurable and emit a metric/alert when a flush is left incomplete.
- **Maintenance-loop escalation (FMEA FE-9).** Track consecutive flush/S3-sync
  failures and escalate (metric + alert) instead of silently retrying each tick.
- **Split `main.rs`.** ~2.8k LoC in one file; extract the bootstrap, listener
  wiring, and maintenance loop into focused modules to keep each unit reviewable.

## Non-goals

- Re-documenting or re-implementing subsystem internals (storage, schema,
  cluster, transport, query front-ends). Those live in their own crates with
  their own specs — this crate only composes them.
