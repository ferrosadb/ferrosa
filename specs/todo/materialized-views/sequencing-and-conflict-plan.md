---
title: Materialized Views — Sequencing & Conflict-Avoidance Plan
status: draft
created: 2026-06-19
work_item: materialized-views
branch: feature/materialized-views
executive_summary: >
  Sequencing plan that parks the materialized-views branch cleanly while the
  postgres foundational work is in flight, to avoid hard merge conflicts. The MV
  branch footprint today is tiny and additive — the new ferrosa-view crate, six
  spec docs, and one line in the root Cargo.toml — and touches none of the shared
  crates the postgres work edits (ferrosa-schema, ferrosa-cql, ferrosa-storage).
  Conflicts therefore do not exist now; they would only arise on the eventual
  rebase, and only in the not-yet-started integration steps (E6 schema, E11/E12
  CQL, E7/E8 storage), each of which edits a postgres-overlapping crate. The plan:
  pause all MV coding now; keep the branch parked and periodically rebased (cheap,
  since only Cargo.toml + specs drift); resume integration only after the postgres
  foundation merges to main, re-reading each shared file before editing and
  preferring additive edits (new fields, new match arms, new modules) over
  refactors so the rebase stays mechanical. The Accord step (E9) is notably NOT in
  the postgres blast radius, but it depends on the storage steps that are.
---

# Materialized Views — Sequencing & Conflict-Avoidance Plan

> Context: postgres foundational work is in flight and edits shared crates
> `ferrosa-schema`, `ferrosa-cql`, `ferrosa-storage` (confirmed). To avoid hard
> conflicts, MV implementation is **paused**. This doc records the current state,
> the conflict surface, and the resume sequence.

## 1. Current branch footprint (parked state)

`feature/materialized-views` (worktree `/private/tmp/ferrosa-mv`), based on
`main@db849cf5` (a clean ancestor of current `origin/main`). Real footprint
(`git diff db849cf5..HEAD`) — **12 files, additive only**:

| Path | Kind | Shared-crate edit? |
|------|------|--------------------|
| `ferrosa-view/` (Cargo.toml + 4 src files) | NEW crate, nothing imports it | No — island |
| `specs/materialized-views/` (6 docs + this) | NEW spec dir | No |
| root `Cargo.toml` | +1 line in `members` | **Only** shared-file edit (trivial) |

`ferrosa-view` contents: `metadata` (ViewMetadata/ViewKind/ColumnSource),
`validate_view_def` (architecture §4 rules), `compute_view_delta` (§6.3 state
machine). 25 unit tests, fmt + clippy clean.

**Consequence:** there are no conflicts with the postgres work *now*. The branch
can sit indefinitely with near-zero maintenance.

## 2. Conflict surface — only the deferred integration steps

Postgres blast radius: **ferrosa-schema, ferrosa-cql, ferrosa-storage** (not
ferrosa-cluster). Mapping each remaining MV step to its target crate and overlap:

| Step | Element | Target crate(s) | Overlaps postgres? | Disposition |
|------|---------|-----------------|--------------------|-------------|
| Finish island | E1/E4/E5 extras: proptest oracle, UDF projection, D4 predicate eval | `ferrosa-view` (+dev deps on udf/index) | **No** | Conflict-free, but paused by request |
| E6 | `SchemaSnapshot.views` + Raft DDL replication | `ferrosa-schema`, `ferrosa-cluster` | **Yes (schema)** | Defer; rebase first |
| E11 | populate `system_schema.views` (+ driver-shape fix, G7) | `ferrosa-cql` (router.rs) | **Yes** | Defer; main already changed router.rs |
| E12 | `CREATE/ALTER/DROP MV` parse → ViewMetadata | `ferrosa-cql` (parser/ast/router) | **Yes** | Defer |
| E7 | view `TableStore` lifecycle | `ferrosa-storage` (store/engine) | **Yes** | Defer |
| E8 | observer hook base→delta | `ferrosa-storage` (observer/engine) | **Yes** | Defer |
| E9 | Accord base+view commit | `ferrosa-cluster` (coordinator/write) | **No** | Defer anyway — depends on E7/E8 |

**Every integration step touches a postgres-overlapping crate, or depends on one
that does.** There is no integration work that can safely proceed in parallel.
Only the `ferrosa-view` island is conflict-free — and that is paused by request.

## 3. The plan

### Phase 0 — Pause (now)

- **Stop all MV coding.** No further commits to `ferrosa-view` or any crate.
- Park the branch. This plan + the board tasks are the durable resume record.
- **Low-cost keep-warm (optional):** periodically `git rebase origin/main` on the
  parked branch. Because the footprint is only `ferrosa-view` + `specs/` + one
  Cargo.toml line, each rebase is near-instant and conflict-free. Doing this
  occasionally keeps the eventual integration rebase from accumulating drift.

### Phase 1 — Resume trigger & rebase (when postgres foundation merges to main)

1. `git rebase origin/main`. Expected conflicts: the Cargo.toml `members` line
   (trivial) and possibly `specs/` placement (see §4). `ferrosa-view` is isolated
   and should rebase clean.
2. Re-run `cargo test -p ferrosa-view` (the 25 tests) + fmt + clippy — must stay
   green on the new base.
3. **Re-read the now-postgres-modified shared files before writing any
   integration code** — `ferrosa-schema/registry.rs`, `ferrosa-cql/router.rs`
   + `parser.rs`, `ferrosa-storage/observer.rs` + `store.rs`. Postgres may have
   changed their shapes; the DSM forbidden-edge assumptions and the seam APIs
   must be re-validated against the settled code, not the pre-postgres snapshot
   this blueprint was written against.

### Phase 2 — Integration in band order (against settled main)

Follow `dsm-proposed.md` build bands, each preceded by a re-read of the target:

1. **E6** — `SchemaSnapshot.views` + Raft replication (ferrosa-schema). Add the
   `views` field in the **shape postgres settled on** — coordinate so CQL
   incremental MVs and postgres snapshot MVs share one `ViewMetadata` via the
   `view_kind` discriminator (D1).
2. **E12 + E11** — CQL DDL parse/translate + `system_schema.views` population
   (ferrosa-cql). Fold in the existing driver-shape fix (board task `t_8152cf35`,
   gate G7). Note main already edited `router.rs` since the blueprint base.
3. **E7 + E8** — view `TableStore` + observer hook (ferrosa-storage).
4. **E9** — Accord base+view commit (ferrosa-cluster). Not in the postgres blast
   radius, but gated on E7/E8.

Island work (proptest oracle, UDF projection, D4 predicate eval in `ferrosa-view`)
can slot in at any point once coding resumes — it never conflicts.

## 4. Merge-strategy rules (keep the rebase mechanical)

- **`ferrosa-view` stays 100% additive** (it already is) — it never conflicts.
- **Prefer additive edits to shared files** over refactors: add a `views` field,
  add new AST variants + new match arms, add a new observer impl — do not
  restructure existing functions postgres is also editing. Additive hunks rebase
  far more cleanly than rewrites.
- **Do E6 only after seeing postgres's final schema/DDL-replication shape**, so
  the `views` representation matches rather than fights it.
- **Coordinate on the schema representation** with whoever owns the postgres
  foundational work — the `view_kind` discriminator (D1) is the explicit shared
  contract; the snapshot engine (board task `t_02a5a95c`) is the postgres side.

## 5. Spec relocation note (rebase action)

`origin/main` has since reorganized `specs/` (e.g., `postgres-frontend` moved to
`specs/proposed/`). On the Phase-1 rebase, evaluate moving
`specs/materialized-views/` → `specs/proposed/materialized-views/` to match the
new convention for not-yet-implemented work. Mechanical `git mv`; flagged here so
it is not missed.

## 6. What "done with pausing" looks like

- Branch parked, 25 tests green, footprint = 12 additive files + 1 Cargo.toml
  line.
- Board has the resume gate + per-crate integration tasks, each tagged with its
  postgres-overlap status (§2) and blocked on the postgres-foundation merge.
- Resumption cost: one rebase + re-read of three shared files, then band-order
  integration — no re-derivation required (this plan + the six specs carry it).
