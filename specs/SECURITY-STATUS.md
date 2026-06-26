# Security Status — honest posture

> Rebuilt: 2026-06-19
> Source: [`../specs-audit/SPEC-AUDIT.md`](../specs-audit/SPEC-AUDIT.md), cross-cutting risk #1
> Detail: per-item threat models in [`security/`](security/)

The spec audit's single highest-value finding was that **the threat-model / FMEA
docs are systematically optimistic**: multiple items read as "APPROVED" or
"mitigated" but are actually Phase-2 / deferred and **unconfirmed in code**. This
is a trust/correctness gap, not cosmetics.

**Do not cite any item below as a mitigated control until it has been re-statused
against the code and a verifying test exists.** Each is marked DEFERRED (known
not-yet-implemented) or UNCONFIRMED (claimed but no located implementation/test).

## Items the threat models claim mitigated but are not confirmed

| ID / Area | Source doc | Status | Note |
|-----------|-----------|--------|------|
| T02 — TLS (client/transport) | `security/threat-model.md` | UNCONFIRMED | Reads as mitigated; not confirmed in code. |
| T08 — TLS (related transport item) | `security/threat-model.md` | UNCONFIRMED | Same TLS gap as T02. |
| Internode mTLS | `security/threat-model-net-cluster.md` | DEFERRED | Mutual TLS between nodes not confirmed implemented. |
| Cluster-formation admin-API auth | `security/threat-model-cluster-formation.md` | DEFERRED | Formation/admin API authentication is Phase-2. |
| Flamechart / telemetry endpoint auth | `security/observability-threat-model.md` | UNCONFIRMED | Flamechart auth claimed; not confirmed. |
| WASM UDF URL allowlist | `security/threat-model-rrd-wasm-timeseries.md` | DEFERRED | Inline-language → WASM URL allowlist deferred. |
| CQL T5 | `security/threat-model-cql-bc.md` | UNCONFIRMED | Listed as mitigated; not confirmed against parser/routing code. |
| CQL T6 | `security/threat-model-cql-bc.md` | UNCONFIRMED | As above. |
| CQL T10 | `security/threat-model-cql-bc.md` | UNCONFIRMED | As above. |
| CQL T11 | `security/threat-model-cql-bc.md` | UNCONFIRMED | As above. |

Additional graph threat-model items (`security/threat-model-graph.md`, T12–T16)
were also flagged as over-optimistic and should be re-statused in the same pass.

## What "DEFERRED" vs "UNCONFIRMED" means here

- **DEFERRED** — the threat model itself (or the audit) acknowledges the control
  is Phase-2 / future work. Honest gap; track it as open work.
- **UNCONFIRMED** — the threat model presents the control as done, but the audit
  could not locate the implementation or a verifying test. Treat as not-mitigated
  until proven otherwise (fail loud, do not assume).

## Action

Per the audit's cross-cutting risk #1: **re-status every security doc against the
code before any are cited as evidence.** Until then, this file is the canonical
"what is actually true" pointer; the documents in [`security/`](security/) retain
their original (optimistic) wording for historical comparison and were not edited
during the rebuild.
