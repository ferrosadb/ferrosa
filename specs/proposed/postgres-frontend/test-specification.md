---
title: Postgres Front-End — Layered Test Specification
status: proposed (TDD-first — these tests are written before implementation)
executive_summary: >
  Seven-layer test specification for the new Postgres wire-protocol front-end
  (decisions D1–D8 in decisions.md). The whole feature is built test-first
  (red-green-refactor): every test named here is authored RED before its
  production code exists. The centerpiece (Layer 3) is DIFFERENTIAL TESTING of
  the bespoke relational engine against a real PostgreSQL container — the only
  reliable way to catch silently-wrong query results. A SECOND differential
  centerpiece arrives with D8 (multi-database / unified RBAC): the SAME grant
  fixtures are replayed through BOTH the Postgres path AND the CQL path, and any
  divergence in allow/deny outcome is a privilege bug. Layers run from pure unit
  codec checks up through real multi-driver conformance, isolation proofs
  (explicit BEGIN..COMMIT block engages Accord without a GUC; autocommit
  eventual; GUC can force Accord on autocommit), full-server integration, and
  load/soak. Honors the ferrosa test policy: no `#[ignore]`, no silent returns,
  live-infra behind the `live-infra-tests` cargo feature with `panic!` setup
  instructions when prerequisites are absent, `container_runtime()` helper,
  `FERROSA_TEST_CONTAINERS=1`. Failure modes are sourced from `fmea.md` and
  attack surfaces from `threat-model.md` (siblings) and are not duplicated here.
---

# Layered Test Specification — Postgres Front-End

> Companion to [`decisions.md`](./decisions.md). Failure modes referenced as
> **FM-n** live in [`fmea.md`](./fmea.md); threats referenced as **T-n** live in
> [`threat-model.md`](./threat-model.md). This document defines *tests*, not
> implementation, and is the source of the RED tests that must exist before any
> production code is written.
>
> **D8 note:** decision **D8** (multi-database model, unified RBAC, CQL interop)
> *revises D5*: namespacing is now 3-level (**database → schema = keyspace →
> table**), a connection binds to one database, JOINs are **database-bounded**,
> and a *single* grant model gates **both** the Postgres and CQL paths. Tests
> that previously assumed "one logical db `ferrosa`, all keyspaces joinable"
> (notably Layer 6 cross-keyspace JOIN and Layer 3 cross-schema cases) are
> re-scoped accordingly below; new D8 coverage is added in Layers 1, 3, 4 and 6.
> The D8 work items live in
> [`todo/multi-database-control-plane.md`](./todo/multi-database-control-plane.md).

## 0. Ground rules (binding on every layer)

These are non-negotiable and apply to all tests below. They restate the
[`ferrosa/CLAUDE.md`](../../../CLAUDE.md) test policy as it applies to this
feature; any test that violates them is itself a bug.

1. **TDD / red-green-refactor.** Each test is written and observed to FAIL
   (RED) before the production code that satisfies it exists. CI snapshots the
   RED commit (test added, code absent/stub) so the discipline is auditable.
   No production code lands without a test that exercised it RED first.
2. **No `#[ignore]`.** Zero ignored tests. A test that cannot run yet does not
   get committed yet.
3. **No silent returns.** Never `if !precondition { return; }` in a test body.
   A missing precondition is either (a) a unit/property test with no external
   dependency — then it always runs — or (b) a live-infra test — then it
   `panic!`s with setup instructions (rule 4).
4. **Live-infra opt-in.** Any test needing a container (PostgreSQL, MinIO,
   ferrosa server, a real driver runtime) is gated behind the crate feature
   `live-infra-tests` and, once that feature is enabled, MUST `panic!` with
   explicit setup instructions when its environment prerequisite is absent —
   never skip, never pass vacuously.
   - `FERROSA_TEST_CONTAINERS=1` — Docker/Podman compose (PostgreSQL + MinIO).
   - `FERROSA_TEST_CLUSTER_NODES=<addr>` — pre-provisioned ferrosa cluster.
   - Container engine resolved via the `container_runtime()` helper, never a
     hardcoded `"docker"`.
   - Local form, e.g.:
     `FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-postgres --features live-infra-tests pg_diff_ -- --nocapture`
5. **Fail loud.** A wrong-result or degraded-mode outcome must surface as a
   test failure with diagnostic context (the SQL, both result sets, the OID
   map), not a quietly-swallowed mismatch. This is the whole point of Layer 3.
6. **Determinism.** Differential and property tests pin seeds, fix table data,
   and `ORDER BY` (or canonicalize) result sets so a mismatch is reproducible
   and never a flaky-ordering artifact.

### Crate / module layout assumed by these tests

Per D7: new crate **`ferrosa-postgres`** (wire codec, connection state machine,
SCRAM, catalog emulation) and a bespoke relational engine crate (referred to
here as **`ferrosa-sql`**), both depending on the new neutral **`ferrosa-session`**
crate extracted from `ferrosa-cql` per D10 (the shared `SharedState` core; its
CQL regression suite stays green). Tests live as follows:

| Layer | Location |
|-------|----------|
| 1 Unit | `ferrosa-postgres/src/**` `#[cfg(test)]`, `ferrosa-sql/src/**` `#[cfg(test)]`, `ferrosa-session/src/**` `#[cfg(test)]` |
| 2 Property | `ferrosa-postgres/tests/prop_*.rs`, `ferrosa-sql/tests/prop_*.rs` (proptest) |
| 3 Differential | `ferrosa-sql/tests/pg_diff_*.rs` (feature `live-infra-tests`) |
| 4 Conformance | `ferrosa-postgres/tests/driver_conformance/` (feature `live-infra-tests`; polyglot drivers via container) |
| 5 Isolation | `ferrosa-postgres/tests/isolation_*.rs` (feature `live-infra-tests`) |
| 6 Integration | `ferrosa-postgres/tests/system_*.rs` (feature `live-infra-tests`) |
| 7 Load/soak | `ferrosa-loadgen` profiles + `ferrosa-postgres/tests/soak_*.rs` (feature `live-infra-tests`) |

---

## Layer 1 — Unit (wire codec, SCRAM, OID map)

Pure, in-process, no feature gate, always run. These are the first RED tests of
the project.

### 1.1 Message framing encode/decode round-trips

For **every** frontend and backend protocol message type, a round-trip property
holds at the unit level (exhaustive enumeration of variants; property-fuzzed in
Layer 2):

- Frontend: `StartupMessage`, `SSLRequest`, `PasswordMessage`/`SASLInitialResponse`/`SASLResponse`,
  `Query`, `Parse`, `Bind`, `Describe`, `Execute`, `Sync`, `Flush`, `Close`,
  `CancelRequest`, `Terminate`, `CopyData`/`CopyDone`/`CopyFail`.
- Backend: `AuthenticationOk`/`AuthenticationSASL`/`SASLContinue`/`SASLFinal`,
  `ParameterStatus`, `BackendKeyData`, `ReadyForQuery`, `RowDescription`,
  `DataRow`, `CommandComplete`, `EmptyQueryResponse`, `ParseComplete`,
  `BindComplete`, `ParameterDescription`, `NoData`, `CloseComplete`,
  `PortalSuspended`, `ErrorResponse`, `NoticeResponse`, `CopyInResponse`/`CopyOutResponse`.

Tests:
- `encode(decode(bytes)) == bytes` for a captured-from-real-libpq corpus of each
  message (golden bytes checked in).
- `decode(encode(msg)) == msg` for constructed values.
- Length-prefix self-consistency: declared length equals actual body length.

### 1.2 Bounded message-length cap (FM: oversized-frame OOM → see fmea.md)

- A message header declaring a body larger than the configured cap
  (`MAX_MESSAGE_LEN`) is rejected with a protocol error **before** any
  allocation of the declared size — assert no large allocation occurs and the
  connection is closed with a clear error. (Drives the bounded-reader contract;
  mirrors T-flood / FM-oversized-frame.)
- A `StartupMessage` whose declared length exceeds the startup cap is rejected
  pre-auth (Layer 7 floods this; here we unit-prove the guard).

### 1.3 StartupMessage parameter handling (D1, D5)

- Unknown **dotted** GUCs (e.g. `ferrosa.isolation=accord`) are **retained**,
  not rejected (D1 explicitly calls naive rejection a failure mode).
- `database`, `user`, `application_name`, `client_encoding` parsed correctly.
- Empty/duplicate parameter keys handled deterministically (last-wins, encoded
  as a known decision).

### 1.4 SCRAM-SHA-256 step vectors (D4)

Using RFC 5802 / RFC 7677 known-answer vectors plus locally generated ones:
- `client-first` / `server-first` / `client-final` / `server-final` byte-exact
  against fixtures.
- `SaltedPassword`, `ClientKey`, `StoredKey`, `ServerKey`, `ClientSignature`,
  `ServerSignature` intermediate values match vectors.
- Verifier derivation from cleartext `{salt, iterations, stored_key, server_key}`
  matches what a real `libpq` client computes (cross-checked against a captured
  exchange).
- **Wrong password** → server rejects, no `SASLFinal` success emitted.
- **Nonce mismatch / replayed client nonce** → rejected (T-scram-replay).
- Channel-binding flag `n`/`y`/`p` parsed; `SCRAM-SHA-256-PLUS` is a documented
  follow-up — a `p` request without TLS support returns a clear error, never a
  silent downgrade (fail-loud).

### 1.5 CQL ↔ Postgres OID mapping (D5)

- Each supported CQL type maps to exactly one Postgres type OID and back where
  reversible; the table is total over the supported set (exhaustive match, no
  wildcard fallthrough).
- `RowDescription` for a known table yields OIDs a real driver accepts (the
  byte-level assertion is unit; the driver actually accepting them is Layer 4).
- Unmapped CQL types produce an explicit "unsupported type" error, never OID 0
  / `unknown` smuggled through silently (fail-loud).

### 1.6 Multi-database control-plane model & DDL (D8 — unit, always-on)

Pure, in-process tests of the database registry, the keyspace↔database mapping,
and the unified grant model — no container. These RED tests gate the D8
data-model and the single shared grant checkpoint before any wire/integration
test can exercise it. (D8a/D8b/D8c; FMEA grant-divergence row.)

- **Mapping cardinality (D8a):** the keyspace↔database mapping is many-to-many;
  the model accepts one keyspace attached to N databases and one database holding
  N keyspaces. Attaching the *same* schema name twice within one database is
  rejected (schema-name uniqueness *within* a database), while the same keyspace
  name may legally appear in two different databases.
- **Default-db rule (D8c):** a keyspace with **no** explicit attachment resolves
  to database `ferrosa`. Once it gains ≥1 explicit attachment, the documented
  rule (lean: explicit replaces implicit) is enforced *deterministically* — the
  resolver returns exactly the documented set, never a silent union/empty.
- **Unified grant decision function (D8b) — pure core:** the single
  `authorize(role, action, object)` decision function (the one checkpoint both
  paths must call) is unit-tested over a fixture grant set:
  - `CONNECT/USAGE ON DATABASE` is required to reach *any* keyspace in that db,
    `ON SCHEMA` maps onto the existing keyspace-level permission, and the two
    compose (db-connect AND schema-usage AND table-priv) as documented.
  - Denials return a typed deny *with reason*, never a bare `false` that a caller
    could mistake for "not yet evaluated" (fail-loud).
  - The function is **pure and path-agnostic**: it takes the same inputs whether
    the caller is the Postgres engine or the CQL router — this purity is what
    makes the Layer-3 differential equality (3.4) meaningful rather than two
    re-implementations that merely happen to agree.
- **DDL / wire-verb parse (D8):** `CREATE DATABASE`, attach/detach-keyspace,
  `GRANT/REVOKE {CONNECT,USAGE} ON DATABASE`, and `GRANT/REVOKE ... ON SCHEMA`
  parse to the correct AST; malformed forms return a typed parse error (no
  silent partial parse). CQL-side `GRANT ... ON DATABASE` parses to the *same*
  internal grant mutation as the Postgres form (one model, two front-ends).

---

## Layer 2 — Property-based (proptest)

Generators + invariants. Seeds pinned and printed on failure.

### 2.1 Wire-message round-trip (generalizes 1.1)

- `forall msg: decode(encode(msg)) == msg` across generated values of every
  message type, including boundary field sizes, empty strings, max-arity
  parameter lists, and multi-byte UTF-8 in text fields.
- `forall bytes: decode` either returns a well-typed message or a typed
  protocol error — **never panics, never over-reads** (fuzz the byte stream).

### 2.2 SQL parser round-trip / fuzz (D2)

- `parse(unparse(ast)) ≅ ast` (AST-equivalence modulo formatting) for generated
  ASTs over the supported grammar (SELECT, JOIN, WHERE, GROUP BY, ORDER BY,
  params `$n`, subqueries/CTEs as they land).
- `parse` on arbitrary token streams never panics; returns a typed parse error.
- Differential parser oracle (optional, Layer-3-adjacent): a generated query
  that real Postgres accepts is either accepted by ferrosa or rejected with a
  clear "unsupported" error — never a silent partial parse.

### 2.3 Planner equivalence properties (D3 — bespoke engine)

Algebraic invariants the bespoke optimizer must preserve. Each is a property
over generated logical plans + small generated table instances:
- **Join commutativity:** `A ⋈ B` and `B ⋈ A` produce the same multiset of rows
  (modulo column order).
- **Predicate pushdown soundness:** pushing a filter below a join/projection
  yields the same result multiset as evaluating it above.
- **Projection pruning:** dropping unreferenced columns does not change row
  count or surviving column values.
- **Aggregate/group invariance:** `GROUP BY` result is invariant under input row
  permutation.
- **Sort stability + spill equivalence:** an external (spilled) sort returns the
  same ordered sequence as an in-memory sort over the same input (Layer 7
  bounds the spill; here we prove correctness of the spilled path).

> These are *internal* equivalences. Layer 3 proves the engine matches the
> *external* oracle (real Postgres). Both are required: 2.3 catches optimizer
> rewrite bugs; Layer 3 catches semantic bugs the optimizer and the naive plan
> share.

---

## Layer 3 — Engine correctness: DIFFERENTIAL TESTING vs real PostgreSQL (CENTERPIECE)

> **Why this is the centerpiece.** D3 commits to a *bespoke* relational engine —
> we re-derive decades of query-engine semantics (NULL three-valued logic, join
> semantics, type coercion, ordering, aggregate edge cases). The dominant risk
> (D3) is not a crash but a **silently wrong result**. The only reliable oracle
> is real PostgreSQL. We run identical SQL against a Postgres container and
> against ferrosa and assert identical results. A mismatch is a hard failure
> with both result sets dumped.

Gated behind `live-infra-tests` + `FERROSA_TEST_CONTAINERS=1`. Engine via
`container_runtime()`; absent prerequisite → `panic!` with setup instructions.

### 3.1 Harness shape

```
ferrosa-sql/tests/pg_diff_<area>.rs
    │
    ├─ fixture: load identical schema + identical seed data into BOTH:
    │     • PostgreSQL container (oracle)
    │     • ferrosa (keyspace == schema, D5)
    │   from one shared DDL+DML corpus so divergence cannot come from setup.
    │
    ├─ for each case (SQL string + tags):
    │     r_pg      = run on Postgres via a pinned pg driver  (LC_COLLATE=C, COLLATE "C")
    │     r_ferrosa = run on ferrosa via the SAME driver, different port (COLLATE "C")
    │     verdict   = oracle(r_pg, r_ferrosa) ∈ { Match, Mismatch, OutOfScope }
    │       where canonicalize = (column-order-normalize, type-normalize,
    │                             float/numeric canonical-render, NULL sentinel,
    │                             and ORDER BY-or-sort to kill ordering noise)
    │       Match     ⇒ canonicalize(r_pg) == canonicalize(r_ferrosa) → PASS
    │       Mismatch  ⇒ canonicalized rows differ                     → FAIL (loud)
    │       OutOfScope⇒ case uses a feature ferrosa intentionally does
    │                   not support OR a locale-dependent behavior the
    │                   v1 (COLLATE "C") oracle cannot compare          → RECORDED, not failed
    │
    └─ on Mismatch: fail loud — print SQL, both row sets, both RowDescriptions
        (names + OIDs), and the first differing row. OutOfScope is recorded
        (logged + counted), never silently skipped and never a PASS-by-default.
```

**Three verdicts, not pass/fail.** The differential oracle returns **Match |
Mismatch | OutOfScope**:
- **Match** — both sides return and canonicalize equal. PASS.
- **Mismatch** — both sides return but canonicalized results differ. Hard FAIL,
  dumped loud. This is the bug the centerpiece exists to catch.
- **OutOfScope** — the case uses a feature ferrosa **intentionally does not
  support** (the differential oracle cannot meaningfully compare a query ferrosa
  restricts), OR a **locale/collation-dependent behavior the v1 oracle cannot
  compare** (see COLLATE "C" below). OutOfScope is **recorded** (counted, logged
  with the reason) — it is **not** a failure and **not** a silent skip. The
  restricted-query *rejection* oracle (3.1a) is what actually exercises the
  out-of-scope/restricted paths for fail-loud rejection; the differential oracle
  simply does not score them as result-correctness pass/fail.

Design constraints on the harness:
- **One corpus, two targets.** Schema and seed data are authored once and
  loaded into both engines, so a difference is always an engine difference.
- **Same client.** Use one pg driver pointed at two ports; this also exercises
  the wire layer end-to-end, so a RowDescription/type-encoding bug shows up
  here too, not just in Layer 4.
- **v1 collation = `C` only.** Both sides run under `COLLATE "C"` /
  `LC_COLLATE=C` (byte ordering): the Postgres container is initialized with
  `LC_COLLATE=C`/`LC_CTYPE=C` and text comparisons/`ORDER BY` in the corpus pin
  `COLLATE "C"`. This removes false ordering mismatches that are collation
  differences, not engine bugs (ferrosa has no ICU/libc collation machinery).
  **Limitation (DEFERRED):** locale/ICU collation parity is explicitly out of
  scope for v1 — any non-`C` collation case is verdict **OutOfScope**, and the
  catalog must report `C` collation so apps know. Follow-up: implement/compare
  ICU/libc collation parity — tracked in
  [`todo/open-follow-ups.md`](./todo/open-follow-ups.md).
- **Ordering discipline.** Cases either carry an explicit `ORDER BY` (then order
  is asserted) or are compared as multisets. Never compare unordered output
  positionally.
- **Type-aware comparison with float/numeric canonicalization.** Compare decoded
  values by Postgres type, not raw text. As part of the **Match** comparison,
  float/numeric values are rendered to a **canonical form** (e.g. normalize
  `1.0` vs `1`, `-0` vs `0`, exponent/scale text rendering) so that text-format
  differences in float/numeric output do not false-trigger a Mismatch. The
  canonicalizer normalizes *rendering* only; it MUST compare decoded typed values
  and MUST NOT round or drop precision (that would hide FM-17) — it canonicalizes
  presentation, never magnitude.
- **Determinism.** No `now()`/random()/volatile functions in the corpus unless
  the case explicitly pins them.

### 3.1a Restricted-query REJECTION oracle (`reject_oracle`) — the fail-loud counterpart

The differential oracle (3.1) has a structural blind spot: it can only score
queries ferrosa *accepts* and that are *in scope* to compare. Every query ferrosa
**intentionally does not support** (cross-database joins per D8a, unsupported
types per the type-support matrix, `SELECT ... FOR UPDATE`, unsupported SQL/
features) is **outside** the differential oracle by construction — yet those are
exactly the paths where a half-supported query could return *plausible wrong rows*
instead of a clean error. The differential oracle marks them **OutOfScope**; this
**separate** oracle proves they fail loud.

`reject_oracle(sql)`: for every query in a **restricted-query corpus** (queries
ferrosa does NOT support), assert that ferrosa returns a **clean typed ERROR with a
real SQLSTATE** (e.g. `0A000 feature_not_supported`, `42P01 undefined_table`,
`3D000 invalid_catalog_name` for a cross-database reference, `42704`/`42883` for an
unsupported type/function as appropriate) and **NEVER emits unproven or partial
rows**. This is the fail-loud counterpart to the differential oracle and covers its
structural blind spot: a restricted query that returns rows at all is a hard
failure, regardless of whether those rows happen to look correct.

```
reject_oracle: for each restricted-query case:
    result = run on ferrosa
    assert result is Err(ErrorResponse) with a non-XX000 SQLSTATE in the expected class
    assert NO DataRow was emitted (zero rows AND an error — not "zero rows, no error")
on a returned row (or a missing/`XX000` error): fail loud — print SQL, the rows or
    the bare/internal error. No tolerance, no skip.
```

Restricted-query corpus families (grow over time): cross-database JOIN (D8a),
unsupported types (per the closed type-support matrix), `FOR UPDATE`/`FOR SHARE`,
unsupported SQL constructs/functions, and any non-`C` collation operation the v1
oracle declares out of scope. A query that moves from "restricted" to "supported"
migrates from this corpus into the differential corpus (3.2).

> **Generator scoping.** The SQLancer-style randomized generator (FM-12) is
> constrained to ferrosa's **supported** grammar/types/collation so its budget
> tests result correctness via the differential oracle, not the rejection path.
> Queries outside that grammar are routed to `reject_oracle`, not the differential
> oracle — so the two oracles partition the query space rather than both crying
> wolf on restricted queries.

### 3.2 Corpus coverage (the cases)

Each tag is a sub-file. Coverage is the contract; cases grow over time but these
families must exist:

| Area | Cases (examples) | Catches |
|------|------------------|---------|
| **Joins** (D2/D6/D8a) | INNER, LEFT, RIGHT, FULL, self-join, multi-table chain, cross-schema join *within one database* (D8a-legal) | join-semantics & null-padding bugs |
| **NULLs** | `NULL = NULL`, `IS NULL`, `IS NOT DISTINCT FROM`, NULLs in aggregates, NULLs in GROUP BY, NULL ordering (`NULLS FIRST/LAST`) | three-valued-logic bugs (highest-risk for a hand-rolled engine) |
| **Aggregates** | COUNT(*), COUNT(col), SUM/AVG/MIN/MAX, GROUP BY single/multi-key, HAVING, empty-group, all-NULL group, DISTINCT agg | aggregate edge-case bugs |
| **Ordering / LIMIT** | ORDER BY multi-key asc/desc, NULLS FIRST/LAST, LIMIT/OFFSET, ties | sort correctness + the spilled-sort path |
| **Types & coercion** | int/bigint/numeric/text/bool/timestamp/uuid/bytea, implicit/explicit casts, comparison across numeric types | OID/coercion mismatch (D5) |
| **Predicates** | `=,<>,<,>,BETWEEN,IN,LIKE,AND/OR`, predicate on join key (pushdown path), param `$1` | predicate eval + pushdown soundness |
| **Subqueries / CTE** (later milestones) | scalar subquery, `IN (subquery)`, `WITH` | correlated/uncorrelated eval |

### 3.3 Invariant

For every differential corpus case the oracle returns **Match | Mismatch |
OutOfScope** (3.1): a **Match** (`canonicalize(r_pg) == canonicalize(r_ferrosa)`
under `COLLATE "C"`, with float/numeric canonical rendering) PASSes; a **Mismatch**
FAILs loud, printing enough to reproduce, never tolerated or rounded away; an
**OutOfScope** case is recorded (counted + logged with reason), neither passed nor
failed by the differential oracle. For every restricted-query corpus case (3.1a)
the **`reject_oracle`** invariant holds: ferrosa returns a clean typed ERROR and
emits **no rows**. Together these make "silently wrong query results" (Mismatch
caught) AND "plausible wrong rows from a query ferrosa should reject" (reject_oracle
caught) impossible to ship green.

> **v1 limitations (declared, not hidden):** locale/ICU collation parity is
> DEFERRED — v1 compares under `COLLATE "C"` only and the catalog reports `C`;
> non-`C` collation cases are OutOfScope (follow-up in
> [`todo/open-follow-ups.md`](./todo/open-follow-ups.md)). Float/numeric comparison
> canonicalizes *rendering* only and never rounds away precision (so FM-17 stays
> catchable).

### 3.4 UNIFIED RBAC differential testing — Postgres path vs CQL path (D8 CENTERPIECE)

> **Why this is a centerpiece, parallel to 3.1–3.3.** D8b commits to *one*
> permission model enforced at a *single* checkpoint that **both** the Postgres
> engine and the CQL router must consult. The dominant risk (D8b, and the FMEA
> grant-divergence mode routed from
> [`todo/multi-database-control-plane.md`](./todo/multi-database-control-plane.md))
> is not a crash but a **silent privilege divergence**: a role allowed on one
> path and denied on the other. Where 3.1–3.3 use *real PostgreSQL* as the oracle
> for *query results*, 3.4 uses **the two ferrosa paths as oracles for each
> other** for *authorization outcomes*. Identical grant fixtures, identical
> requested action, **identical allow/deny** is the invariant. Any divergence is
> a hard failure and a privilege bug.

Gated `live-infra-tests` + `FERROSA_TEST_CONTAINERS=1`; `container_runtime()`;
absent prerequisite → `panic!` with setup instructions. Lives in
`ferrosa-sql/tests/pg_diff_rbac_*.rs` (or `ferrosa-postgres/tests/`, wherever the
two listeners are reachable in one process).

#### 3.4.1 Harness shape

```
one grant fixture set  ──loaded once into the shared role/grant store──┐
                                                                       │
for each (role, action, object) probe in the fixture matrix:          │
    allow_pg  = attempt the action over the Postgres listener (SCRAM auth as role)
    allow_cql = attempt the SAME action over the CQL listener  (auth as same role)
    assert allow_pg == allow_cql            ← the unified-RBAC invariant
    and (when an oracle expectation is pinned) assert allow_pg == expected
on divergence: fail loud — print role, action, object, the resolved grant chain
    (db-connect / schema-usage / table-priv), and which path allowed vs denied.
    No tolerance, no skip.
```

- **One fixture, two paths.** Grants are authored once and written through the
  single model; a difference is therefore an *enforcement* difference, never a
  setup difference — exactly mirroring 3.1's "one corpus, two targets."
- **Probe matrix** covers each grant axis independently and in combination:
  database `CONNECT`/`USAGE`, schema `USAGE`, and table-level
  `SELECT`/`INSERT`/`UPDATE`/`DELETE`, plus the negative space (missing
  db-connect, missing schema-usage, missing table-priv, role with none).
- Both `==` directions matter: a Postgres **allow** that CQL **denies** is a
  Postgres over-grant; a CQL **allow** that Postgres **denies** is a CQL
  over-grant. Both are failures.

#### 3.4.2 Cases (the contract)

| Probe family | Fixture | Asserts |
|--------------|---------|---------|
| **db-connect gate** | role lacks `CONNECT ON DATABASE D`; holds keyspace perms | DENIED on **both** Postgres and CQL — "permission ≠ namespace" (D8b): even CQL, which addresses the keyspace directly, is gated by the db-connect grant |
| **schema/keyspace usage** | role has db-connect but no schema usage | DENIED identically on both paths |
| **table privilege** | role has db-connect + schema usage, lacks `SELECT` on table | DENIED identically; with `SELECT` granted → ALLOWED identically |
| **compose** | full chain (connect + usage + select) | ALLOWED identically; revoke any one link → DENIED identically |
| **cross-path GRANT/REVOKE** | `GRANT` issued over Postgres, then probed over CQL (and vice-versa) | the grant takes effect on the *other* path too — single source of truth, no per-path cache divergence |

#### 3.4.3 Invariant

For every probe: `authorize_via_postgres(role, action, object) ==
authorize_via_cql(role, action, object)`. A divergence is a privilege bug, fails
loud with the resolved grant chain, and is never tolerated. This is what makes a
"silent grant divergence between the two front-ends" impossible to ship green —
the security analogue of 3.3.

---

## Layer 4 — Protocol conformance (real driver matrix)

> Proves we speak the protocol the way real clients expect — independent of
> result correctness (Layer 3). Drives a polyglot driver matrix against a live
> ferrosa Postgres listener in a container. Gated `live-infra-tests` +
> `FERROSA_TEST_CONTAINERS=1`; `container_runtime()`; absent → `panic!`.

### 4.1 Driver matrix

| Driver | Lang | Why in the matrix |
|--------|------|-------------------|
| libpq / `psql` | C | reference client; `\d`, `\dn` meta-commands |
| psycopg3 | Python | extended-query heavy, async, `server_settings` GUC |
| asyncpg | Python | binary protocol, prepared-statement cache, `server_settings` |
| pgx | Go | `RuntimeParams`, binary, connection pool |
| pgjdbc | Java | enterprise ORM default, distinct prepared-statement behavior |
| node-postgres | Node | text protocol default, widely used |

Each driver runs the same conformance script (container per driver runtime) so a
protocol bug specific to one client surfaces.

### 4.2 Conformance checks (per driver where applicable)

- **Handshake**: StartupMessage → AuthenticationSASL → SCRAM (D4) → AuthenticationOk
  → ParameterStatus set (`server_version`, `client_encoding`, `DateStyle`,
  `standard_conforming_strings`, `integer_datetimes`) → BackendKeyData → ReadyForQuery.
- **SCRAM**: success path, wrong-password rejection, nonce integrity (T-scram-replay).
- **Simple query** (`Query`): single statement, multi-statement, empty query
  (`EmptyQueryResponse`), DDL/DML CommandComplete tags (`SELECT n`, `INSERT 0 n`).
- **Extended query**: full `Parse`/`Bind`/`Describe`/`Execute`/`Sync` cycle;
  `ParameterDescription` + `RowDescription` from `Describe`; `NoData` for
  param-only describe.
- **Prepared statements**: named + unnamed; re-bind a prepared statement with
  new params; `Close` of statement/portal; pgjdbc/asyncpg server-side cache
  behavior.
- **Parameter binding**: text and binary parameter formats; result formats text
  and binary; `$1..$n` typed via `ParameterDescription`; NULL parameter.
- **Error / notice fields**: `ErrorResponse` carries `S`(severity),
  `C`(SQLSTATE), `M`(message), and where relevant `D`/`H`/`P`. A parse error
  returns a plausible SQLSTATE (e.g. `42601`), not a generic blob. `NoticeResponse`
  surfaces non-fatal warnings. (Drivers parse these strictly — a missing field
  breaks them.)
- **Transaction-status byte**: `ReadyForQuery` reports `I` (idle), `T` (in tx),
  `E` (failed tx) correctly across BEGIN/COMMIT/ROLLBACK and after an error
  inside a transaction. (D1: must be correct regardless of underlying CL.)
- **Cancellation**: `CancelRequest` with `BackendKeyData` cancels an in-flight
  query; cancel of an idle/unknown key is a no-op, not a crash (T-cancel-forge).
- **Introspection**: `psql \d <table>`, `\dn` (schemas == keyspaces, D5),
  `current_schema()`, `SHOW search_path`, `SET search_path`, and the catalog
  queries each driver issues on connect must all return driver-parseable results.
- **Multi-database introspection (D8)**: `psql \l` / `\dn` and the `pg_database`,
  `pg_namespace`, `pg_class` queries reflect the **connected** database only —
  `\l` lists the registry's databases; `\dn` lists *only* the connected db's
  attached keyspaces, further filtered by the caller's grants. Connecting with a
  `database` parameter that is not in the registry returns a clear connection
  error (no silent fallthrough to `ferrosa`). No cross-db catalog rows ever leak.
- **GRANT/REVOKE over the wire (D8b)**: `GRANT/REVOKE {CONNECT,USAGE} ON
  DATABASE` and `... ON SCHEMA` issued through a real driver return a correct
  CommandComplete tag and a parseable result; a subsequent connect/query by the
  affected role observes the change. A `CONNECT`-less role attempting to connect
  to a database is refused at connect time with a clear `ErrorResponse`
  (SQLSTATE `42501` insufficient_privilege / `3D000` invalid_catalog_name as
  appropriate) — the CONNECT gate is enforced over the wire, not just internally.
- **COPY** (later milestone): `COPY ... TO/FROM STDOUT/STDIN`,
  CopyInResponse/CopyOutResponse/CopyData/CopyDone. Tagged as a later-milestone
  conformance set; its tests are written when COPY lands (still TDD, still no
  `#[ignore]` — they simply aren't committed until then).

---

## Layer 5 — Transaction / isolation (D1, refined by D11)

> Proves the central D11 bet: an explicit `BEGIN … COMMIT` block engages Accord
> **without any GUC** (strict-serializable, read-your-writes inside); autocommit
> stays eventual-by-default; and the GUC can still force Accord on autocommit.
> The difference is observable and documented — staleness on the autocommit path
> is asserted as *expected*, not treated as a failure.
> Gated `live-infra-tests` + `FERROSA_TEST_CLUSTER_NODES`/`FERROSA_TEST_CONTAINERS`;
> absent → `panic!`.

### 5.1 An explicit transaction block engages Accord WITHOUT a GUC (centerpiece, D11)

- **Explicit block ⇒ Accord, no GUC**: `BEGIN; … ; COMMIT;` on a plain session
  (no `ferrosa.isolation` set) routes the block's reads/writes through Accord:
  read-your-writes holds inside (5.3) and the run is strict-serializable.
  Entering the block (status byte `T`) is the trigger. Asserted via a behavioral
  probe **and** a server-side metric/log that the Accord path was taken
  (fail-loud: we don't trust the absence of staleness alone; we confirm the
  path — observe a `TxnId`/HLC). (D11.)
- **GUC forces Accord on autocommit (optional)**: connecting with
  `options='-c ferrosa.isolation=accord'` (and per-driver equivalents: psycopg
  `options=`, asyncpg `server_settings`, pgx `RuntimeParams`, pgjdbc `options=`),
  or `SET ferrosa.isolation = 'accord';` after connect, additionally routes
  **autocommit** reads through Accord — not required for the explicit-block case
  above. (D1 opt-in surfaces 1 & 2, now optional under D11.)
- **Unknown/invalid GUC value** → clear error, session left on the documented
  default, never a silent wrong mode (fail-loud).

### 5.2 Autocommit is eventual-by-default, with documented staleness (EXPECTED behavior)

- An **autocommit** (bare-statement) session may observe **read-after-write
  staleness**; the test asserts this is *tolerated* (a stale read is a PASS on the
  autocommit path) and documents it, rather than asserting read-your-writes. This
  encodes D1's "expected behavior, not a bug" directly into the matrix.
- An explicit `BEGIN … COMMIT` block (5.1/5.3) and a GUC-forced autocommit session
  must **not** be stale — so the rows together prove the trigger does something.

| Session mode | Read-after-write | Test asserts |
|--------------|------------------|--------------|
| autocommit (eventual) | may be stale | stale read is ACCEPTED (expected) |
| explicit `BEGIN … COMMIT` block (no GUC) | must be fresh | read-your-writes REQUIRED (D11) |
| autocommit + `ferrosa.isolation=accord` | must be fresh | read-your-writes REQUIRED |

### 5.3 Read-your-writes inside an explicit Accord transaction (no GUC)

- With **no GUC set**, `BEGIN; INSERT ...; SELECT ...;` sees its own write within
  the same transaction (read-your-writes) because the explicit block engages Accord
  (D11), and a concurrent autocommit reader is allowed to lag.
- Strict-serializable ordering across two Accord transactions (a classic
  write-skew / lost-update probe) holds — leans on the existing Accord test
  vocabulary in `ferrosa-cluster`.

### 5.4 BEGIN / COMMIT / ROLLBACK status correctness (independent of CL)

- `BEGIN` → status `T`; `COMMIT` → `I`; `ROLLBACK` → `I`.
- Error inside a transaction → status `E`; subsequent statements rejected with
  "current transaction is aborted" until `ROLLBACK`; then `I`.
- These hold on **both** the eventual and Accord paths (D1: the status byte and
  BEGIN/COMMIT semantics must be correct regardless of underlying CL).

### 5.5 Pooler caveat (documented matrix row)

- A transaction-mode pooler (e.g. PgBouncer) may not preserve a startup-time GUC
  per logical client. This is captured as a **known** matrix row: the test
  documents the behavior (per-session `SET` required behind such a pooler) and
  asserts ferrosa does not silently appear to honor a startup GUC it can't see.
  (D1 implementation constraint.)

---

## Layer 6 — Integration / system

> Full server over containers, real catalog introspection on connect,
> multi-keyspace-as-schema queries. Gated `live-infra-tests` +
> `FERROSA_TEST_CONTAINERS=1`; `container_runtime()`; absent → `panic!`.

- **Listener spin-up**: ferrosa boots with the Postgres listener on 5432
  alongside CQL/Bolt/SPARQL sharing `SharedState` (D7); a driver connects and
  completes a query end-to-end.
- **Introspection on connect**: each Layer-4 driver's *automatic* startup
  catalog queries succeed (the queries psql/psycopg/pgjdbc fire before the user
  types anything) — `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
  `pg_proc` stubs, `current_schema()`, `search_path` (D5). A driver that errors
  on connect is a hard failure.
- **Keyspace == schema mapping**: `\dn` lists keyspaces as schemas; `ks.tbl`
  resolves; `search_path` selects the active keyspace; schema-qualified names
  address others (D5, within the connected database — D8a).
- **Database-bounded JOIN (D8a)**: a JOIN across two keyspaces **co-located in
  the connected database** SUCCEEDS and returns correct rows (also differential-
  checked in Layer 3). A JOIN that references a keyspace **not attached** to the
  connected database ERRORS clearly (schema/relation does not exist in this
  database) — **never** a silent cross-database join, never a silent empty
  result. Both the success and the clear-error case are asserted; the negative
  case checks the error text/SQLSTATE names the unreachable schema (fail-loud).
- **Missing table errors; empty table returns zero rows (FM-41, R15)**: a query
  against a **non-existent table** (not in the catalog, or catalog-known but
  **not registered in storage**) ERRORS with a clear `NoSuchTable` →
  SQLSTATE `42P01` (`undefined_table`) / `3D000` as appropriate — **never** an
  empty result set. A query against a **legitimately empty (registered)** table
  returns **zero rows with no error**. The two outcomes are asserted as
  **distinct**: this proves the engine does not inherit
  `ferrosa-storage::range_iter_projected`'s silent empty-stream-on-missing-table
  fallback (fail-loud). A JOIN whose one side resolves to a catalog-known but
  storage-unregistered table ERRORS — it never silently drops that side and
  returns plausible-but-wrong rows. Ties into the FM-36 mapping-race coverage and
  the [`todo/storage-scan-fail-loud.md`](./todo/storage-scan-fail-loud.md) audit.
- **Keyspace attached to many databases (D8a)**: a keyspace attached to two
  databases `D1` and `D2` is visible and joinable in **each** (connect to `D1`,
  join works; connect to `D2`, join works); schema-name uniqueness is enforced
  *within* a database; and a role's grants apply **per database** — holding
  `CONNECT ON D1` but not `D2` reaches the keyspace only via `D1`.
- **Default database reachability (D8c)**: a keyspace created via **CQL**
  `CREATE KEYSPACE` (no explicit attach) auto-appears in database `ferrosa` and
  is reachable from a Postgres connection to `ferrosa` with **no admin action** —
  it shows in `\dn`/`pg_namespace` and `ks.tbl` resolves and queries end-to-end.
  Asserts the auto-registration path is live, not a manual step.
- **Filtered catalog — no cross-db leakage (D8a/D8b)**: from a connection to
  database `D`, `pg_database` lists the registry but `pg_namespace`/`pg_class`/
  `pg_attribute` expose **only** `D`'s attached keyspaces, **further filtered by
  the caller's grants** — a keyspace the role lacks usage on does not appear, and
  a keyspace attached only to another database never appears. Driver introspection
  on connect sees exactly this filtered view; no row from another database or
  another role's reach leaks into the catalog.
- **CONNECT gate end-to-end (D8b)**: a role without `CONNECT ON DATABASE D`
  cannot open a session to `D` over Postgres **and** cannot reach `D`'s keyspaces
  over CQL — same denial, both paths (the Layer-3 3.4 invariant, observed here at
  the full-server level).
- **Rollout / backward-compat migration (D8b)** — effective-permission diff:
  - **No silent revoke**: an existing CQL role that today holds keyspace/table
    perms on a keyspace in default db `ferrosa` **retains** access after
    unification (via the implicit-connect-or-auto-grant mitigation in D8b). The
    test computes the role's effective-permission set before and after the
    migration and asserts the post-migration set is a **superset-or-equal** on
    the keyspaces it legitimately held — a dropped permission fails loud.
  - **No silent widen**: a role that should **not** reach a given keyspace/database
    does **not** gain access through the migration. The effective-permission diff
    asserts no permission appeared that the role did not earn — an added
    permission fails loud.
  - Both directions are asserted from the *same* effective-permission diff, so the
    migration can neither quietly drop nor quietly grant. (D8b: "fail loud if a
    role is denied — never silently widen access.")
- **Auth unification (D4)**: a role created via **CQL** `CREATE ROLE ... PASSWORD`
  can authenticate over Postgres SCRAM (verifier populated on the CQL set path),
  and a role created/altered over Postgres can authenticate over CQL bcrypt.
  Confirms "every password-set path populates the SCRAM verifier."
- **Dev seed creds (D4)**: the loadgen/dev seed role is seeded with a SCRAM
  verifier and can complete the Postgres handshake (otherwise loadgen can't
  connect — mirrors the existing CQL-auth loadgen fix).

---

## Layer 7 — Load / soak

> Resource-bounds and resilience under adversarial input. Gated `live-infra-tests`;
> driven from `ferrosa-loadgen` profiles + soak tests; absent prerequisite → `panic!`.
> Specific thresholds and failure modes come from [`fmea.md`](./fmea.md); this
> layer names the test shapes, not the numbers.

- **Pre-auth flood resistance** (T-flood / FM-preauth-flood): thousands of
  connections that open and send oversized/garbage StartupMessages without
  authenticating must not OOM or exhaust the accept loop. Asserts the
  bounded-message cap (1.2) and a pre-auth connection/time budget hold; the
  listener stays responsive to a legitimate client throughout. (Mirrors the
  observed CQL 1,536-session storm class.)
- **Query-of-death / cartesian-join spill bounds** (FM-cartesian-spill): a
  deliberately huge cross/cartesian join and a large external sort must spill to
  bounded resources (peak memory and temp footprint capped), then either
  complete or fail loud with a resource-limit error — never silently OOM the
  node it serves. Validates the spilled-sort/join path proven correct in 2.3/3.2.
- **Prepared-statement cache pressure** (FM-prepared-cache-growth): a driver
  (asyncpg/pgjdbc) churning many distinct prepared statements must not grow the
  server-side statement cache unbounded; eviction is bounded and observable, and
  correctness of still-live prepared statements is preserved.
- **Soak**: a multi-hour mixed read/write workload (default + Accord sessions)
  holds steady — no connection/statement/memory leak, transaction-status byte
  stays correct, and the Accord path stays strongly consistent throughout.

---

## Traceability matrix (decision / risk → test layer → milestone)

| Decision / Risk | Primary layer(s) | Key tests | Milestone |
|-----------------|------------------|-----------|-----------|
| **D1/D11** explicit txn block ⇒ Accord (no GUC); autocommit eventual; GUC forces Accord on autocommit | 5 (1.3 startup GUC, 4 status byte) | 5.1, 5.2, 5.3, 5.4; 1.3 GUC retention | M1 (handshake+status), M2 (full isolation proof) |
| **D1** staleness EXPECTED on autocommit path | 5 | 5.2 (stale read = PASS on autocommit) | M2 |
| **D2** real relational (joins/agg/sort) | 3, 2 | 3.2 corpus, 2.2/2.3 | M1 (join slice), grows after |
| **D3** bespoke engine = silent-wrong-result risk | **3** (centerpiece), 2.3 | pg_diff_* differential corpus | M1 (join), ongoing |
| **R10** sound oracle: 3 verdicts (Match/Mismatch/OutOfScope) + COLLATE "C" v1 + reject_oracle + float canonicalization | **3** (3.1, 3.1a, 3.3) | pg_diff_* (Match/Mismatch/OutOfScope); pg_reject_* (restricted-query rejection) | M1 (join + restricted slice), ongoing |
| **D4** SCRAM alongside bcrypt, populated everywhere | 1.4, 4, 6 | 1.4 step vectors; 4 SCRAM handshake; 6 auth-unification + dev seed | M1 (SCRAM handshake), M2 (full unification) |
| **D5** keyspace=schema, pg_catalog emulation, OID map | 1.5, 4, 6 | 1.5 OID map; 4 introspection; 6 `\dn`/cross-keyspace | M1 (\dn,\d, OIDs), grows |
| **D6** first JOIN end-to-end over a real driver | 3, 4, 6 | see M1 set below | **M1** |
| **D8a** keyspace↔db many-to-many; schema-name uniqueness within db | 1.6, 6 | 1.6 mapping cardinality; 6 keyspace-attached-to-many-databases | M1 (single-db slice), M2 (many-to-many) |
| **D8a** database-bounded JOIN (no cross-db join) | 3, 6 | 3.2 join row "within one database"; 6 database-bounded JOIN (success + clear error) | **M1** (correctness gate) |
| **D8b** unified RBAC — one model, both paths | **3.4** (centerpiece), 1.6, 4, 6 | 3.4 pg-vs-cql differential authz; 1.6 pure decision fn; 4 GRANT/REVOKE over wire; 6 CONNECT gate e2e | M1 (CONNECT gate), M2 (full probe matrix) |
| **D8b** CONNECT gate enforced (Postgres + CQL) | 3.4, 4, 6 | 3.4 db-connect probe; 4 connect-time refusal; 6 CONNECT gate e2e | **M1** |
| **D8b** rollout migration: no silent revoke / no silent widen | 6 | 6 rollout effective-permission diff (both directions) | M2 |
| **D8c** default-db `ferrosa` auto-reachability | 1.6, 6 | 1.6 default-db resolver rule; 6 CQL-created keyspace reachable from Postgres | **M1** |
| **D8** filtered `pg_database`/`pg_namespace`/`pg_class` (no cross-db catalog leak) | 4, 6 | 4 multi-db introspection; 6 filtered catalog | M2 (M1 needs only single-db `ferrosa` view) |
| FMEA: grant divergence (role allowed on one path, denied on the other) | **3.4**, 1.6 | 3.4 pg-vs-cql allow/deny equality invariant | M1 (CONNECT-gate slice), M2 (full matrix) — see [`fmea.md`](./fmea.md) |
| FMEA: silent cross-database join | 3, 6 | 6 database-bounded JOIN clear-error case | M1 — see [`fmea.md`](./fmea.md) |
| FMEA FM-41: scan returns empty stream on missing table (table-absent vs table-empty) | 6 | 6 missing-table-errors / empty-table-zero-rows (distinct outcomes) | M1 — see [`fmea.md`](./fmea.md), [`todo/storage-scan-fail-loud.md`](./todo/storage-scan-fail-loud.md) |
| FMEA: rollout silent revoke/widen | 6 | 6 effective-permission diff | M2 — see [`fmea.md`](./fmea.md) |
| Risk: oversized-frame OOM | 1.2, 7 | 1.2 cap; 7 pre-auth flood | M1 (unit cap), M2 (flood) |
| Risk: cartesian/spill OOM | 2.3, 7 | 2.3 spill equiv; 7 spill bounds | M2 |
| Risk: wire-fuzz panic | 2.1 | byte-stream fuzz | M1 |
| Risk: prepared-cache growth | 7 | 7 cache pressure | M2 |
| Threats (STRIDE) | 1, 4, 5, 7 | per [`threat-model.md`](./threat-model.md) | per threat |
| Failure modes (RPN-ranked) | 1.2, 2.3, 7 | per [`fmea.md`](./fmea.md) | per FM |

---

## Milestone 1 minimal test set (first JOIN end-to-end)

M1 (D6) = a real driver completes SCRAM auth, introspects keyspaces-as-schemas,
and a two-table `JOIN ... WHERE pk=$1` is planned by the bespoke engine and
returned correctly. Under D8, M1 is scoped to the **single default database
`ferrosa`**, but it must already honor the new namespace/RBAC spine: at minimum
M1 needs **default-db `ferrosa` reachability** (a CQL-created keyspace is reachable
from Postgres with no admin action, D8c), the **CONNECT gate** (a role without
db-connect is refused, identically on both paths, D8b), and **database-bounded
JOIN correctness** (a JOIN within `ferrosa` succeeds; a reference to a keyspace
not in the connected database errors clearly, D8a). The full many-to-many model,
the complete pg-vs-cql probe matrix, the filtered catalog, and the rollout
migration diff are M2+. The following tests MUST be GREEN for M1 (and were each
written RED first). Everything else in this spec is M2+.

**Layer 1 (unit, always-on):**
- 1.1 round-trips for the M1 message subset: StartupMessage, SASL* (SCRAM),
  Query, Parse/Bind/Describe/Execute/Sync, RowDescription, DataRow,
  CommandComplete, ReadyForQuery, ErrorResponse.
- 1.2 bounded message-length cap (oversized-frame guard).
- 1.3 StartupMessage retains dotted GUCs (`ferrosa.isolation`) — even if M1 only
  exercises the default path, the parser must not reject it.
- 1.4 SCRAM-SHA-256 step vectors incl. wrong-password rejection.
- 1.5 OID map total over the M1 type set (the join's column types).
- 1.6 (D8 slice for M1): the default-db resolver returns `ferrosa` for an
  unattached keyspace (D8c), and the unified `authorize()` decision function
  enforces the **db-connect gate** path-agnostically (D8b) — the pure core behind
  the M1 CONNECT-gate and default-db requirements.

**Layer 2 (property, always-on):**
- 2.1 wire round-trip + byte-stream fuzz never panics.
- 2.3 join commutativity + predicate-pushdown soundness (the M1 join uses a
  pushed-down `WHERE pk=$1`).

**Layer 3 (differential — the M1 correctness gate):**
- `pg_diff_joins`: INNER JOIN of two tables with `WHERE pk=$1` returns identical
  results vs real PostgreSQL — verdict **Match** under `COLLATE "C"` (3.1).
- `pg_diff_types`: the M1 column types round-trip identically (RowDescription
  OIDs + decoded values match the oracle), with float/numeric canonical rendering.
- `pg_reject_*` (3.1a): the M1 restricted-query slice — a cross-database JOIN
  reference (D8a) and an unsupported-type/feature query each return a clean typed
  ERROR with no rows via `reject_oracle` (the fail-loud counterpart to the
  differential gate).

**Layer 4 (conformance — minimal driver):**
- psql/libpq **and** psycopg3 (or asyncpg): SCRAM handshake → extended-query
  `Parse/Bind/Describe/Execute/Sync` carrying `$1` → correct RowDescription and
  DataRows → `ReadyForQuery` status byte `I`. ErrorResponse carries a valid
  SQLSTATE on a bad query.

**Layer 6 (integration):**
- Listener boots on 5432 sharing `SharedState`; driver connects to database
  `ferrosa`, the driver's automatic startup catalog queries succeed, `\dn` lists
  keyspaces-as-schemas, `\d <table>` describes the join's tables, and the
  end-to-end `JOIN ... WHERE pk=$1` returns correct rows over the wire.
- Auth path: a role usable for the M1 connection authenticates via SCRAM with a
  populated verifier (D4) — at minimum the dev seed role.
- D8c default-db reachability: a keyspace created via CQL `CREATE KEYSPACE` is
  reachable from the Postgres `ferrosa` connection with no admin action (auto-
  registration is live).
- D8b CONNECT gate: a role lacking `CONNECT ON DATABASE ferrosa` is refused at
  connect time over Postgres and denied the keyspace over CQL — same denial, both
  paths (the M1 slice of 3.4).
- D8a database-bounded JOIN: the M1 join across two keyspaces co-located in
  `ferrosa` succeeds; a JOIN referencing a keyspace not attached to `ferrosa`
  errors clearly (named unreachable schema), never a silent cross-db join.

**Explicitly deferred past M1** (tests exist in this spec, written when their
code lands, still no `#[ignore]`): COPY (4 COPY set), full aggregate/GROUP BY/
HAVING differential corpus, subqueries/CTEs, full isolation proof 5.2/5.3 beyond
the GUC-retention check, full driver matrix (pgx/pgjdbc/node-postgres), Layer 7
load/soak, `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` alias, and the D8 M2+
set: the full keyspace↔database many-to-many model (3.4 full pg-vs-cql probe
matrix, keyspace-attached-to-many-databases), the filtered multi-db catalog
(`pg_database` + per-db/per-grant `pg_namespace`/`pg_class`), and the rollout
backward-compat effective-permission diff (no silent revoke / no silent widen).

---

_This specification defines the tests that gate the feature; it does not define
implementation. Detailed failure modes: [`fmea.md`](./fmea.md). Threat surfaces:
[`threat-model.md`](./threat-model.md). Locked constraints: [`decisions.md`](./decisions.md)._
