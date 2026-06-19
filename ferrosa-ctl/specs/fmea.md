---
crate: ferrosa-ctl
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-ctl — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). `ferrosa-ctl` is an operator tool, so the worst outcomes are
**destructive recovery commands run against the wrong target** and **commands
that silently look successful when the node side does nothing**.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| CTL-1 | `raft log-truncate` run with the wrong `--from` (or on a node that didn't need it) | Permanent loss of committed metadata ops `>= from` on that node | 10 | 3 | 5 | 150 | `--from` is mandatory (clap-required); mutation needs `--yes`; `--dry-run` previews; `log-inspect` prints the exact `--from` to use. **Residual gap:** no cross-node consistency check — the operator must repeat correctly on every damaged node. |
| CTL-2 | Unwired HTTP endpoint (`raft transfer-leader`, `cluster add-learner/promote/demote`, `snapshot`, `restore`) treated by the operator as a working operation | Operator believes a membership/PITR change happened when it didn't | 7 | 5 | 3 | 105 | Commands surface upstream `404`/`501` with a spec pointer (fail-loud). **Residual gap:** `snapshot`/`restore` POST to endpoints the node does not yet serve, so they only "work" once the node side lands; status is not obvious from `--help`. |
| CTL-3 | `sstable s3-clean --apply` deletes object-store generations on a misread `<keyspace>.<table>` dir name | Durable S3 copies of (mis)identified gens deleted; cold restart can't recover them | 9 | 2 | 5 | 90 | Dry-run default; only CORRUPT gens (per the engine smoke test) are targeted; reuses engine `FERROSA_S3_*` config. **Residual gap:** deletion is irreversible and depends on the dir being named exactly `<keyspace>.<table>`. |
| CTL-4 | `auth set-password` quoting/escaping bug lets a crafted role/password break out of the `ALTER ROLE` statement | CQL injection in an admin path | 9 | 1 | 4 | 36 | `escape_double_quotes`/`escape_single_quotes` applied; dedicated unit tests for `'` in password and `"` in role. Low occurrence; server also validates. |
| CTL-5 | `--ssl` requested for `auth set-password` | Operator expects an encrypted admin connection; cleartext password would otherwise traverse the network | 8 | 2 | 2 | 32 | `CqlClient` has no TLS yet, so `--ssl` **errors out** rather than silently connecting in cleartext. **Open gap:** no TLS for any CQL command — admin password crosses the wire in cleartext on non-localhost. |
| CTL-6 | `sstable reingest --apply` re-inserts salvaged rows but some rows are unrecoverable / silently dropped | Partial recovery presented without a clear "what was lost" tally | 6 | 4 | 5 | 120 | Resilient salvage recovers header + leading rows; per-row insert failures are counted (`failed`) not fatal; `--limit` enables a controlled first run; dry-run default. **Residual gap:** rows beyond the first decode failure in a partition are not recovered and the yield is an estimate. |
| CTL-7 | Web-API command output is the raw server body; a partial/odd JSON shape is printed verbatim | Operator misreads ring/repair/restore result | 4 | 4 | 4 | 64 | `ring` does best-effort column extraction; `repair` pretty-prints JSON when parseable else dumps raw. Non-2xx always becomes an error. Cosmetic, not data-affecting. |
| CTL-8 | `cluster bootstrap-dc` looks like it created a per-DC Raft group | Operator assumes a DC group exists | 5 | 3 | 3 | 45 | Output explicitly says "Sprint 6 scaffolding — the live HTTP wire-up lands in Sprint 7"; only derives + prints the `RaftGroupId`. |

## Top risks to act on

1. **CTL-1 (RPN 150)** — `raft log-truncate` is the most dangerous command:
   irreversible committed-data loss, and correctness depends on the operator
   applying the right `--from` on every damaged node. Mitigations are good but
   there is no automated multi-node guard.
2. **CTL-6 (RPN 120)** — `sstable reingest` yield is best-effort; the gap
   between "rows salvaged" and "rows that actually existed" is not surfaced,
   so a partial recovery can read as complete.
3. **CTL-2 / CTL-5** — several lifecycle commands target endpoints the node
   doesn't serve yet, and no CQL command uses TLS (admin password in cleartext
   off-localhost). Both are fail-loud today but are real functional gaps.

## Detection assets

- `tests/integration.rs` — boots a real CQL server + `system_observability.*`
  virtual tables and exercises the queries the observability commands issue.
- `src/main.rs` parsing tests (46) pin every subcommand's flags, defaults, and
  the `--from`-required / dry-run-default invariants.
- `src/auth.rs` tests cover password validation and `ALTER ROLE` escaping.
- `src/commands/sstable.rs` tests run against genuine BTI fixtures via
  `ferrosa-storage`'s `test-support` feature.
