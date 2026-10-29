# ADR-023: Commons boundaries and the shape of the front-end refactor

Status: accepted
Date: 2026-10-29
Context: DSM analysis of the workspace (27 crates, 121 dependency edges, propagation
cost 29.9%, 0 cycles, cluster quality 51.3%)

## The finding that drives this

`ferrosa-common` is a god element: **fan-in 22, fan-out 1, instability 0.04**. Nearly
every crate already depends on it, so "move the shared concerns into commons" cannot
mean "move them into `ferrosa-common`" — that would grow the one module that is
already the bottleneck.

`ferrosa-cql` is the coupling hotspot in the other direction: **fan-out 13**, the
highest in the workspace, instability 0.76. It reaches into everything, which is why
CQL restated the flow-control policy rather than importing it.

`ferrosa-sql` has **fan-in 1** and fan-out 3: a shared layer that almost nothing uses.
It is the natural home for plan and expression semantics once it has consumers.

## Decisions

### 1. One PR for now

The Lean corpus, ADR-022, `count.rs` and the DC_QUORUM test alignment stack into PR
#634. The commons split continues there and is split out later if it grows past
reviewability.

### 2. The value/type concern gets its own crate

A new `ferrosa-value` crate owns the canonical value and type representation plus the
coercion table. `ferrosa-common`, `ferrosa-schema` and `ferrosa-row-bridge` re-export
from it so call sites do not churn.

The three existing homes split the concern by accident, not by design: `common`
(fan-in 22), `schema` (fan-in 13) and `row-bridge` (3 in / 4 out) each hold part of it.
Re-exporting keeps the blast radius small while giving the concern exactly one place
to optimise.

### 3. The coverage gate is a differential harness

One statement corpus, executed through CQL native, PostgreSQL wire, Bolt and Flight,
asserting **identical rows and identical error classes** across surfaces.

This is the gate that protects the user-facing invariant — *no user silently gets
incomplete data* — because the failure mode that invariant describes is precisely two
surfaces disagreeing while both answering confidently. Golden frames per surface are
useful for protocol conformance but would not catch it.

### 4. Breaking changes are allowed

The crates are pre-1.0 and internal. The split may break APIs outright and all 27 call
sites get fixed, rather than accumulating compatibility shims that would defeat the
purpose of the refactor.

## Consequences

- `ferrosa-common` must shrink, not grow. Any new shared concern lands in a narrow
  crate; if it cannot justify more than one consumer it does not belong in commons at
  all (`every_commons_target_is_shared_by_at_least_two_front_ends` in
  `specs/lean4/Ferrosa/Commons/FrontEnd.lean` is the machine-checked version of this
  test).
- The differential harness must land before the value crate is split, so the split has
  a behavioural baseline rather than only a compilation baseline.
- The twelve concerns inventoried in `Commons/FrontEnd.lean` map onto crates as
  follows: value → `ferrosa-value` (new), plan/expressions → `ferrosa-sql`,
  parameters → `ferrosa-session`, stream/gap → `Commons.Stream` in the spec and
  `ferrosa-cdc` narrowed in code, flow → `Commons.Flow`, consistency →
  `ferrosa-cluster` (already the right layer).
