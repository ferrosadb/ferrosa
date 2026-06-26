---
crate: ferrosa-session
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-session — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), the code review (one 69-LoC
module, no constructor, no tests), and the board item "land ferrosa-session
extraction as standalone soaked PR." There are no in-code TODO/FIXME markers in
the crate.

## Now (highest value)

- **Add a `SessionCore::new` / builder** (FMEA SE-1). Replace the 12+ duplicated
  struct literals in `ferrosa`, `ferrosa-cql`, and `ferrosa-ctl` with a single
  constructor that takes the handles and **enforces the `accord_clock` ⇔
  `peer_manager` invariant** (both `Some` together, or both `None`). This makes
  the invariant structural instead of convention.
- **Add in-crate tests** (FMEA SE-2 / SE-5). At minimum: an `accord_enabled()`
  truth table over the four `(peer_manager, accord_clock)` combinations, and —
  once the constructor exists — a test that the constructor rejects the illegal
  "clock without peer manager" combination. Gives `cargo test -p ferrosa-session`
  real meaning.

## Next

- **Finish the soaked extraction PR.** Land `ferrosa-session` as the standalone,
  soaked PR it is tracked as, so the neutral state is a stable shared dependency
  before `ferrosa-postgres` builds on it.
- **Wire `accord_enabled()` into `ferrosa-postgres`** (FMEA SE-5). Have the
  Postgres front-end's D11 transaction-routing check actually call it, so the
  method stops being dead-on-arrival and its semantics are pinned by a real
  consumer.

## Later

- **Evaluate moving the `Deref` bridge pattern into a shared trait.** If
  `ferrosa-postgres` ends up duplicating `SharedState`'s `Deref<Target =
  SessionCore>` shape, consider a small shared accessor trait so both front-ends
  reach neutral fields uniformly (FMEA SE-4 follow-up). Only do this once a
  second front-end actually exists and the shape is proven.
- **Tighten the dependency fan-in** (FMEA SE-6). If a re-exported type from
  `ferrosa-cluster` / `ferrosa-net` churns, consider whether `SessionCore` should
  hold a narrower interface than the concrete type.

## Non-goals

- Request handling, protocol framing, query planning, prepared-statement caching,
  EVENT channels, or metrics — those are front-end concerns and stay in
  `ferrosa-cql` / `ferrosa-postgres`, never here (overview.md invariant #2).
- Becoming a dumping ground for "shared-ish" state. A field belongs here only if
  a second front-end needs it with identical semantics.
