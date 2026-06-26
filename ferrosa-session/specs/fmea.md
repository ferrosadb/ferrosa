---
crate: ferrosa-session
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-session — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). The crate is tiny and data-only, so most risk is in *extraction
completeness* and *coupling* rather than runtime logic.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| SE-1 | **No constructor** — `SessionCore` is built by literal at 12+ call sites (`ferrosa::main`, `ferrosa-cql` server/test_util/connection/subscribe/router, `ferrosa-ctl` + `ferrosa-cql` tests) | A new field, or a changed invariant (e.g. `accord_clock`↔`peer_manager`), must be fixed at every site; drift is easy and silent | 7 | 6 | 6 | 252 | **Open gap.** Add a `SessionCore::new(...)` / builder that centralizes construction and enforces invariants. See roadmap (Now). |
| SE-2 | **Zero in-crate tests** — no `#[test]`/`mod tests` in `ferrosa-session` | A change here can pass `cargo test -p ferrosa-session` (which runs nothing) while breaking real wiring; green build is not a safety signal | 7 | 5 | 7 | 245 | **Open gap.** Add at least `accord_enabled()` truth-table tests + an invariant test once a constructor exists. |
| SE-3 | **`accord_clock`↔`peer_manager` invariant unenforced** — doc says clock is `None` whenever peer manager is `None`, but nothing checks it; a literal could set clock without a peer manager | `accord_enabled()` is reliable, but downstream code assuming "clock present ⇒ peer manager present" could panic/misroute | 6 | 3 | 6 | 108 | Documented invariant only. A constructor (SE-1) would make it structural; until then it is convention. |
| SE-4 | **Extraction incomplete — coupling stays in `ferrosa-cql`** | `SharedState`, all handlers, and the `Deref` bridge still live in `ferrosa-cql`; `ferrosa-postgres` cannot yet reuse anything beyond the field bundle, so the D10 goal is only partially realized | 5 | 5 | 3 | 75 | **Known, by design (in progress).** Tracked as "land ferrosa-session extraction as standalone soaked PR." Detectable: the crate is visibly thin. |
| SE-5 | **`accord_enabled()` has no callers** — dead-on-arrival API | The method could rot or drift from the Postgres front-end's actual D11 check before anything exercises it | 4 | 4 | 5 | 80 | Documented as a forward-looking precondition for `ferrosa-postgres`. Add a test (SE-2) to pin its semantics now. |
| SE-6 | **Wide dependency fan-in for a "neutral" crate** — depends on 6 ferrosa crates (cluster, common, net, schema, storage, udf) | Any breaking change in those crates' re-exported types (`WritePath`, `DdlPath`, `ClusterStateHolder`, `PeerManager`, …) ripples through `SessionCore` and every call site | 4 | 4 | 4 | 64 | Inherent to being the shared engine-state bundle. Bounded by keeping the struct field-only (no logic to break). |
| SE-7 | **Scope creep — a CQL/PG-specific field added here** | Re-tangles the front-ends and defeats the D10 separation | 6 | 2 | 5 | 60 | Invariant #2 in [overview.md](overview.md): "would a second front-end need this with identical semantics?" Enforced by review only. |

## Top risks to act on

1. **SE-1 (RPN 252)** — the missing constructor is the highest-leverage gap:
   12+ duplicated literals mean every future field or invariant change is a
   multi-site edit with no compiler help on the invariant. Add `SessionCore::new`
   (or a builder) and migrate the call sites.
2. **SE-2 (RPN 245)** — the crate has no tests of its own, so its build is not a
   real safety signal. Add `accord_enabled()` truth-table tests and (after SE-1)
   a constructor-invariant test.

## Detection assets

- **None in-crate.** `SessionCore` is exercised only transitively by
  `ferrosa-cql` / `ferrosa` / `ferrosa-ctl` integration and unit tests that
  construct it. There is no `cargo test -p ferrosa-session` coverage today —
  this is precisely SE-2.
