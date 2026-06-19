# Ferrosa Specs

> Last rebuilt: 2026-06-19
> Status: Internal evidence index, not public release guarantees

This `specs/` tree was rebuilt mechanically from the 2026-06-19 spec audit, which
classified all 196 prior specs against the actual code on `origin/main`. The
previous taxonomy (`proposed/`, `in-process/`, `verified-test-plan/`, etc.) had
drifted: ~30 specs were misfiled and the security docs were systematically
optimistic. The new taxonomy reflects implemented reality.

- **Rebuild rationale**: [`../specs-audit/SPEC-AUDIT.md`](../specs-audit/SPEC-AUDIT.md)
- **Historical archive**: [`../specs-legacy/`](../specs-legacy/) holds every dropped,
  obsolete, duplicate, superseded, or aspirational-no-code spec, plus the prior
  `archive/`, `coverage/`, `plans/`, and project-plans. Nothing was deleted —
  history is preserved there. (The active `postgres-frontend` design subtree was
  carried into `todo/postgres-frontend/`.)

## Directory Structure

```text
specs/
  implemented/   Specs whose feature/fix is implemented in code (KEEP + audit MOVE→implemented)
  reference/     Living architecture, DSM, (reliability) FMEA, hazard, and process docs
  todo/          Genuinely open + partial work with remaining implementation
  decisions/     Architecture Decision Records (ADRs), carried over wholesale
  security/      Threat models — re-status against code before citing as evidence
  README.md          This index
  ROADMAP.md         Now / Next / Later, grounded in the audit
  SECURITY-STATUS.md Honest security posture (deferred / unconfirmed items)
```

## Taxonomy

| Dir | Meaning | Count |
|-----|---------|-------|
| [`implemented/`](implemented/) | Implementation present in code; some still need a live-infra verification run | see `find specs/implemented -type f` |
| [`reference/`](reference/) | Descriptive, living docs: ARCHITECTURE, components, data-flow, storage, sstable, cql, DSMs, reliability FMEAs, raft invariants, *-architecture, hazards, release-process, roadmap | — |
| [`todo/`](todo/) | Open bugs and features with real remaining work, including audit PARTIAL and VERIFY-RUN-with-no-locatable-fix items | — |
| [`decisions/`](decisions/) | ADRs 001–020 + role-auth rollout | — |
| [`security/`](security/) | STRIDE threat models. Flagged optimistic by the audit — see `SECURITY-STATUS.md` | — |

## Important caveats

- **Security docs are not evidence until re-statused.** Several threat-model items
  read as "mitigated" but are Phase-2/deferred. See [`SECURITY-STATUS.md`](SECURITY-STATUS.md).
- **`implemented/` is not "verified".** A subset (cluster/streaming bugs) needs a
  live-infra run to confirm; the audit flagged these VERIFY-RUN. Presence of a spec
  here means code exists, not that a Jepsen/cluster run has signed off.
- **Public claim rules still apply.** Do not present as public guarantees:
  Jepsen-verified correctness, full Cassandra/CQL or Redis compatibility,
  arbitrary-query `SUBSCRIBE`/CDC, complete observability backing, or binary
  vector sidecars (current HNSW/IVFFlat sidecars are JSON).
