---
title: Postgres Front-End — Phase 0 Decision Log
status: proposed (Phase 0 complete — D1–D11 locked)
executive_summary: >
  Decision records captured during blueprint Phase 0 (plan interrogation) for the
  Postgres wire-protocol front-end. Each entry is a locked constraint that shapes
  later architecture, threat-model, FMEA, and test-spec phases.
---

# Phase 0 Decision Log — Postgres Front-End

Feature: a Postgres wire-protocol front-end for ferrosa, mirroring the existing
CQL / Bolt / SPARQL listeners. Goal v1: a working subset of ANSI SQL plus DESCRIBE,
full Postgres wire protocol, driver-compatible, transactions on Accord, TDD throughout,
on a feature branch.

Grounding facts (from recon):

- The CQL `router` layer is protocol-agnostic: it accepts a parsed `Statement` AST +
  `RequestContext` and dispatches against a shared `SharedState` (storage, schema, auth,
  WritePath, Accord clock). New protocols mirror CQL: own wire codec + own parser → router.
- There is **no** SQL/relational engine today — only CQL (partition-keyed, wide-column).
  No joins, no relational planner, no multi-table query.
- Accord (`ferrosa-cluster/src/accord/`) provides **strict serializability**, invoked today
  only for CQL LWT. `TxnId`/`HybridLogicalClock` live in `ferrosa-common/src/accord.rs`.
- Auth is bcrypt against `ferrosa-schema::Schema`. Postgres drivers default to SCRAM-SHA-256.

---

## D1 — Consistency / isolation model

**Decision:** Eventual-by-default, opt-in strong.

Default session isolation is ferrosa tunable consistency (may be eventual / stale reads).
A client explicitly raises isolation to strict-serializable (Accord) per session via a
session GUC, e.g. `SET ferrosa.isolation = 'accord'`.

**Consequences / constraints (accepted):**

- This is in known tension with the "compatible with the drivers" goal. Many drivers/ORMs
  (psycopg, JDBC, pgx, node-postgres) assume read-your-writes by default. Read-after-write
  staleness on the default path is **expected behavior, not a bug** — the driver-compat
  test matrix must encode this.
- The wire-protocol transaction-status byte (`I`/`T`/`E`) and `BEGIN/COMMIT/ROLLBACK`
  semantics must still be correct regardless of the underlying CL.
- Documentation must loudly state the default is eventual and how to opt into Accord.

**Opt-in surfaces (all set the same session GUC `ferrosa.isolation`):**

1. **Connection-time (preferred)** — via the `StartupMessage` parameters, using a
   namespaced/dotted custom GUC `ferrosa.isolation=accord`. Driver-portable: libpq/psql
   `options='-c ferrosa.isolation=accord'`, psycopg `options=...`, asyncpg
   `server_settings={...}`, pgx `RuntimeParams`, pgjdbc `options=`. Lets a connection pool
   pin every connection to Accord with no extra round-trip.
2. **Per-session** — `SET ferrosa.isolation = 'accord';` after connect.
3. **Optional alias** — `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` → Accord, for
   ORMs/drivers that only speak standard SQL isolation (still an open follow-up).

**Implementation constraints this imposes:**

- The StartupMessage parser MUST accept and retain unknown dotted parameters (custom GUCs)
  rather than rejecting the connection — a common naive-implementation failure.
- Startup-packet GUCs are applied at session init **after auth**, before the first query.
- Transaction-mode connection poolers (e.g. PgBouncer) multiplex sessions and may not
  preserve a startup-time GUC per logical client — encode as a known test-matrix row.

_Open follow-up:_ exact GUC values (`accord` vs `serializable` vs `strong`) and whether
`SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` aliases to Accord. (Deferred — Q-TBD.)

**Refined by D11:** explicit BEGIN...COMMIT blocks always use Accord; the GUC is no longer the only path — autocommit stays eventual.

---

## D2 — SQL relational scope (v1)

**Decision:** Real relational — multi-table JOINs, subqueries, CTEs, GROUP BY/aggregates.
A genuine relational query engine over ferrosa's wide-column storage, not a thin CRUD
shim onto the CQL router.

**Consequences / constraints:**

- This is the dominant subsystem of the whole feature — far larger than the wire protocol.
  v1 timeline grows ~10x versus a single-table CRUD shim.
- Strongly implies embedding an existing Rust query engine (DataFusion) rather than
  hand-rolling a planner/optimizer/join-operators/spill. (Confirmed separately — see D3.)
- The existing protocol-agnostic CQL `router` is no longer sufficient for reads: SELECTs
  flow through the relational engine, not `router::route()`. Writes may still reuse
  WritePath. Expect a **hybrid** execution model (read path ≠ write path).
- Storage must expose a scan/range interface the engine can pull from (a `TableProvider`
  equivalent), including predicate/projection pushdown to avoid full scans on a
  partition-keyed store.
- "Eventual by default" (D1) now also governs what the relational engine sees mid-query:
  a multi-operator plan reads a non-snapshot view unless the session opted into Accord.

---

## D3 — Relational engine: build vs embed

**Decision:** Bespoke engine — own planner, optimizer, operators (joins, aggregation,
sort, spill), tuned to wide-column storage and Accord. No DataFusion / Arrow dependency.

**Consequences / constraints (accepted, high-risk):**

- This is a multi-person-year subsystem and is the single largest schedule risk in the
  project. A near-term "working v1" must therefore be scoped as a **thin but real**
  vertical (e.g. single-table scan + one join type) and grown; "real relational" (D2) is
  the destination, not the first milestone. The project plan must stage this explicitly.
- Pro retained: exact Postgres semantics and native Postgres type system end-to-end
  (no Arrow↔OID impedance), and an Accord-aware planner.
- Con accepted: we re-derive decades of query-engine engineering. Test burden is large —
  TDD (required) means a correctness-oriented operator test suite from day one, plus a
  conformance corpus against real Postgres output.
- Storage still needs a pull-based scan interface with predicate/projection pushdown for
  the bespoke operators to avoid full scans on a partition-keyed store.

_Risk flag for Phase 6 (project plan):_ recommend a milestone gate after the first
end-to-end slice (one join, real driver) to re-confirm bespoke vs. embed with evidence
before committing the full optimizer build.

---

## D4 — Authentication

**Decision:** Add a SCRAM-SHA-256 verifier alongside the existing bcrypt hash in the
shared role store. Postgres clients authenticate with driver-default SCRAM.

**Consequences / constraints:**

- `ferrosa-schema` role records gain an optional `scram_sha256` field
  `{salt, iterations, stored_key, server_key}`. This is a **shared-store change**, not a
  Postgres-only one.
- The verifier can only be derived from the cleartext password at set-time. Therefore
  **every password-set path must populate it** — `CREATE ROLE / ALTER ROLE ... PASSWORD`
  over CQL *and* over Postgres — so a role created via CQL can log in via Postgres and
  vice-versa. (Unification constraint; touches the CQL auth path too.)
- Existing bcrypt-only roles have no verifier and **cannot authenticate over Postgres
  until their password is reset** (or a migration tool backfills verifiers — but PBKDF2
  needs the cleartext, so a true backfill requires capturing it at next login/reset).
  The dev seed creds (used by loadgen) must be seeded with a SCRAM verifier.
- SCRAM exchange lives in the Postgres wire layer; channel binding
  (`SCRAM-SHA-256-PLUS`) is desirable under TLS but can be a follow-up.
- Bcrypt remains the CQL/Bolt/SPARQL path; the two verifiers coexist per role.

---

## D5 — Namespace mapping  _(SUPERSEDED by D8 — single-database assumption replaced by multi-database)_

> D5 still holds that **keyspace = schema**. What changed in D8: there is **no longer a single
> logical database**. Databases are a real, first-class layer above schemas, backed by a
> keyspace↔database mapping table, and joins are now bounded to a single database.

**Decision:** keyspace = Postgres schema. One logical Postgres database (`ferrosa`);
each ferrosa keyspace is exposed as a schema; `ks.tbl` → schema `ks`, table `tbl`.
`search_path` selects the active keyspace; schema-qualified names address others.

**Consequences / constraints:**

- Preserves cross-keyspace JOINs from D2 (everything is one database, so joins across
  schemas are legal Postgres — the database-as-keyspace option would have made them
  illegal cross-database joins).
- The StartupMessage `database` parameter is expected to be `ferrosa` (or a configured
  single name); connecting with an arbitrary dbname must either map to the one logical DB
  or error clearly — decide behavior (lenient vs strict) in design phase.
- `pg_catalog` + `information_schema` must be emulated well enough that drivers introspect
  and `psql \d` / DESCRIBE work: at minimum `pg_namespace` (from keyspaces), `pg_class`
  (tables), `pg_attribute` (columns), `pg_type` (type OIDs), `pg_proc` stubs,
  `current_schema()`, `search_path`, `SHOW`. Build these on ferrosa's existing
  **virtual-table** mechanism in `ferrosa-schema`. This is mandatory, not optional, for
  driver-compat — stage it early.
- Requires a CQL-type ↔ Postgres-type-OID mapping table (mechanical but must be complete
  enough that drivers receive valid RowDescription OIDs).

---

## D6 — First shippable milestone (M1)

**Decision:** M1 = **first JOIN end-to-end.** A real driver (psql/psycopg) completes SCRAM
auth over the full handshake; `\dn`/`\d` introspect keyspaces-as-schemas; and a two-table
`JOIN ... WHERE pk=$1` is planned by the bespoke engine and returned correctly.

**Rationale / constraints:**

- Deliberately front-loads the highest architectural risk (bespoke planner + join operator,
  per D3's milestone-gate flag) into milestone 1, so the bespoke-vs-embed bet is validated
  on real evidence before the full optimizer is built.
- M1 still implies the entire spine: wire handshake + SCRAM (D4), extended-query enough to
  carry `$1` parameters, catalog emulation (D5), type/OID mapping, scan + join operators,
  and the read path through the bespoke engine.
- "Full wire protocol" and "real relational" remain the destination; later milestones add
  aggregates/GROUP BY/sort/subqueries/CTEs, COPY, LISTEN/NOTIFY, cursors, writes through
  Accord opt-in, and the full driver conformance matrix.

## D7 — How we proceed now

**Decision:** Finish full blueprint planning first (Phases 1–12, specs only). **No
implementation code** until the blueprint is complete and approved.

**Scope/branch notes (defaults, confirm at implementation time):**

- New crate **`ferrosa-postgres`** mirroring `ferrosa-cql`'s structure (server, frame codec,
  connection state machine), plus a bespoke relational engine (likely its own crate, e.g.
  `ferrosa-sql` / `ferrosa-relational`, to keep the planner separable from the wire layer).
- Default port **5432**; listener spawned in `ferrosa/src/main.rs` alongside CQL/Bolt/SPARQL,
  sharing `SharedState`.
- Feature branch (e.g. `feature/postgres-frontend`) in the `ferrosa` repo only; no other
  sub-repo is affected by v1.

---

---

## D8 — Multi-database model, RBAC, and CQL interop _(revises D5)_

Postgres namespacing becomes genuinely 3-level: **database → schema (= keyspace) → table**.
A connection binds to one database (Postgres-standard). Three sub-decisions:

### D8a — keyspace↔database cardinality + join boundary

**Decision:** A keyspace can be **attached to many databases** (many-to-many). Schema names
must be unique within a given database. **JOINs remain database-bounded** (Postgres-correct:
no cross-database joins). To make two keyspaces joinable, attach both to the same database.

- Backed by a **mapping table** (`database` ↔ `keyspace`, many-to-many).
- Join reach (D2) is now "whatever keyspaces are co-located in the connected database," not
  "all keyspaces." This re-bounds D2 deliberately and keeps driver behavior standard.

### D8b — Grant model: unified across paths

**Decision:** One permission model gates **all** paths. `GRANT ... ON SCHEMA` maps onto the
existing keyspace-level permissions; a new **database-level grant** (`CONNECT`/`USAGE ON
DATABASE`) is a coarse gate that applies on **both Postgres and CQL**. Single source of
truth; no Postgres-vs-CQL bypass.

- "Everything is flat through CQL" = **namespace** flatness only. CQL still addresses
  keyspaces directly with no database/schema hierarchy in names — but a keyspace that lives
  in database `D` requires the role to hold the database-connect grant on `D`, **even via
  CQL**. Permission ≠ namespace.
- **Backward-compat constraint (must handle at rollout):** existing CQL roles today have
  keyspace/table perms but no database grant. With unification they would lose access unless
  granted connect on their keyspaces' database. Mitigation: treat the default `ferrosa`
  database (see D8c) as implicitly connectable by any role with the underlying keyspace
  perms, OR auto-grant existing roles `CONNECT ON DATABASE ferrosa` during migration. Decide
  in design; **fail loud** if a role is denied — never silently widen access.

### D8c — Unmapped keyspaces

**Decision:** A keyspace with no explicit database attachment **auto-lands in a default
database `ferrosa`**, so CQL-created keyspaces are always reachable from Postgres without
admin action. Explicit attachment (D8a) is additive; once a keyspace has ≥1 explicit
attachment, whether it still also appears in `ferrosa` is a minor design detail (lean:
explicit attachments replace the implicit default; document the rule).

### Storage / catalog consequences

- New control/system tables (home: extend `system_auth` for grants, add a small
  `system_pg`-style registry): a **database registry**, the **keyspace↔database mapping**
  (many-to-many), and **database/schema grant** rows (or folded into `system_auth`).
- `pg_catalog` virtual tables grow: **`pg_database`** now lists multiple databases from the
  registry; `pg_namespace`/`pg_class`/`pg_attribute` are filtered by the connected database's
  attached keyspaces and the caller's grants.
- The unified grant check is a single enforcement point that **both** the Postgres engine and
  the CQL router must consult — a high-value correctness/security target (add to FMEA: a
  divergence here is a privilege bug). Enforce once, share it. (home resolved by D10:
  `ferrosa-schema`)
- CQL DDL (`CREATE KEYSPACE`) and the new Postgres `CREATE DATABASE` / attach operations both
  mutate the same registry; DDL broadcast must cover the new tables.

---

## D9 — Strict TDD execution ordering _(refines D7)_

**Decision:** Test infrastructure precedes production code. Once the analysis/blueprint is
approved, the implementation order for every unit of work is:

1. **Build the test harness** (containers, differential-vs-real-Postgres oracle, driver
   matrix, unified-authz differential rig, fixtures, CI/Makefile wiring) — see
   `test-harness.md`.
2. **Generate failing (RED) tests** from `test-specification.md` for the slice in scope.
3. **Write production code** until the tests pass (GREEN).
4. **Refactor** with the tests as the safety net.

**Consequences / constraints:**

- Phases 8 (test generation) and 9 (test harness) are **no longer deferred to "after code"**
  — they are the **first** implementation sprint, ahead of any wire/engine code. (This
  corrects the earlier deferral note that paused them until code existed.)
- The harness is a hard prerequisite, not optional: several test classes (differential vs
  real Postgres, driver-matrix conformance, authz parity across paths) **cannot be authored**
  until the harness containers/oracles exist. Harness-first is therefore on the critical path.
- No production module is written before a failing test exists for it. The repo's
  `/tdd` discipline and test policy (no `#[ignore]`, `live-infra-tests` feature, panic on
  missing infra, `container_runtime()`) apply from the first commit.
- The compiled project plan (Phase 10) must sequence harness → red tests → code per sprint;
  the project plan's Sprint 0 is re-cast as "harness + red tests" preceding the spine code.

## D10 — Crate boundaries: extract `ferrosa-session` _(from DSM, corrects architecture.md)_

**Decision:** Do **not** make `ferrosa-postgres` depend on `ferrosa-cql`. Extract the neutral
shared state + protocol-agnostic write/DDL contract into a new **`ferrosa-session`** crate
that sits above `ferrosa-cluster` and below both `ferrosa-cql` and `ferrosa-postgres`. Each
protocol composes its private fields on top of the shared core.

**Rationale / constraints (from DSM validation against the real workspace graph):**

- `SharedState` currently lives in `ferrosa-cql/src/router.rs` inside a ~54k-LOC crate.
  `ferrosa-postgres → ferrosa-cql` would pull that whole crate + ~8 transitive deps for a
  struct, and becomes a hard cycle the moment any Postgres-originated concern needs to enter
  the shared contract. Extraction removes both problems.
- The D8b unified `authorize()`, the database registry, the keyspace↔database mapping, and
  the `pg_catalog`/`information_schema` virtual tables go in **`ferrosa-schema`** — already
  the home of `check_permission` and the virtual-table machinery, and cycle-safe (it depends
  only on common/index/sstable). Keep it **pure over a metadata snapshot** so no
  `schema → engine` back-edge forms. One implementation serves both the CQL router and the PG
  engine.
- Fact corrections to fold into `architecture.md`: `WritePath`/`DdlPath` live in
  `ferrosa-cluster` (not implied home); the engine read/scan path must resolve whether it
  pulls from `ferrosa-storage` directly or via `ferrosa-cluster::WritePath` (open item).
- Hard rule: **forbid `ferrosa-sql → ferrosa-postgres`** (engine must not depend on the wire
  layer). Dependency direction: `ferrosa-postgres → {ferrosa-sql, ferrosa-session,
  ferrosa-schema, ferrosa-net}`; `ferrosa-sql → {ferrosa-session, ferrosa-schema,
  ferrosa-storage, ferrosa-common}`.
- **Sequencing:** the `ferrosa-session` extraction is Sprint-0 work (before any PG code), and
  is itself a refactor of existing CQL internals that must keep CQL green — TDD/regression on
  the existing CQL suite gates it.

See `dsm.md` for the dependency matrix, Mermaid graph, and option analysis.

---

## D11 — Accord engages on the transaction block _(refines D1; resolves risk-register R1/R2)_

**Decision:** An **explicit transaction block** (`BEGIN … COMMIT`, or any multi-statement
transaction) runs on **Accord = strict-serializable**, with read-your-writes inside it.
**Autocommit / bare statements stay eventual-by-default.** The `ferrosa.isolation=accord`
GUC remains available to additionally force Accord on autocommit reads, but is **not required**
for the common case.

**Rationale / constraints:**

- Directly honors the user's original intent ("transactions should use Accord") **and** the
  "eventual is OK" allowance — isolation is bound to the **transaction block**, not a session
  GUC that ORMs never set and transaction-mode poolers strip (risk-register **R1**).
- Fixes the concrete driver/ORM breakage (risk-register **R2**): read-after-write,
  `RETURNING`, `SELECT … FOR UPDATE`, and multi-statement snapshot expectations all occur
  **inside** transactions, which now get strict semantics for free.
- The transaction-status byte (`I`/`T`/`E`) already tracks block boundaries; entering a block
  (`T`) is the trigger to route through the Accord coordinator. `FM-22` ("Accord opt-in
  silently not engaging") is re-scoped: the test is now "a `BEGIN…COMMIT` block engages Accord
  **without** any GUC," which is far more observable.
- Autocommit single statements keep the cheap eventual/tunable path (D1 default unchanged).
- Open detail: behavior of `SELECT … FOR UPDATE` **outside** an explicit block (implicit
  one-statement txn) — lean: treat as its own Accord txn. Confirm in design.

**Propagation:** updates D1's framing, `architecture.md` §5 + the read/write flow, the
isolation tests in `test-specification.md`/`test-harness.md` (H5), `fmea.md` FM-22, and marks
risk-register R1/R2 resolved.

---

_Phase 0 (grill-me) complete: 10 questions asked across two rounds, 10 user decisions,
0 deferred to defaults. Open follow-ups: isolation GUC naming (Q1), lenient-vs-strict dbname
handling (Q2 — now richer under multi-db), legacy bcrypt-role migration (Q3), SCRAM channel
binding (Q4), CQL grant backward-compat at rollout (D8b), implicit-vs-explicit default-db
listing (D8c)._
