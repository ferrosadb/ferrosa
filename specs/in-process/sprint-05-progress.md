---
type: progress
sprint: 5
created: 2026-05-10
last-updated: 2026-05-10
---

# Sprint 5 Progress — Deterministic simulation harness + TLA+ skeleton

## Approach

Strict TDD throughout. Each WI lands as a focused commit on the
`sprint-05-sim-tla` worktree branch. CI gates from Sprint 1 plus
`cargo fmt --check` and `cargo clippy --all-targets -- -D warnings`
green every commit.

## Decision: in-house simulator over Madsim

ADR-017 grants the implementer authority to fall back from Madsim if
integration friction is too high. Sprint 5 elects the fallback path
**up-front**, before W5.2 turns into a multi-day refactor of every
`tokio::time::Instant::now()` call site in ferrosa-cluster. Rationale:

- Madsim shims `tokio` via a feature flag, but openraft 0.9 + sled +
  the existing `FerrosRaft` pull in network and disk paths that are
  outside Madsim's well-trodden coverage. The Sprint 4 audit lists 50+
  call sites of wall-clock APIs across `ferrosa-cluster` alone.
- The headline goal of Sprint 5 is the **TLA+ refinement check**
  (W5.10): observed simulator transitions checked against a TLA+ spec.
  That goal does not require running the *entire* ferrosa-cluster
  binary under sim — it requires a **state machine** that exhibits the
  same protocol transitions the TLA+ spec describes, fed by a
  deterministic event loop.
- TigerBeetle and FoundationDB simulate at the protocol level — they
  rewrote their networking and storage to be deterministic. Madsim is
  a quicker path *if* your code is greenfield; ferrosa is not.

The in-house sim therefore models the **Raft protocol** (term, vote,
log, state) directly, with a deterministic event loop and seeded RNG.
Bootstrap-phase transitions (W5.6) ride on top of the protocol
simulator: each bootstrap phase becomes a sim event that must satisfy
its precondition before firing, exactly like the typed phases in
`ferrosa-cluster/src/controller/bootstrap/`.

Madsim adoption remains an option for a future sprint if the
in-process integration tests in `ferrosa-cluster/tests/` grow
hard-to-reproduce flakes.

## Per-work-item status

| WI    | Status | Commit prefix | Tests added | Notes |
|-------|--------|---------------|-------------|-------|
| W5.1  | **Done** | feat(sim): W5.1 | 1 (`crate_compiles_and_runs_empty_test`) | New crate `ferrosa-sim` added to workspace; deps `serde`, `tracing`, dev `proptest`. |

## Final commit count

TBD.
