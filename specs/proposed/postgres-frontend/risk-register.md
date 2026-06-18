---
title: Postgres Front-End — Adversarial Risk Register
status: proposed (independent review)
executive_summary: >
  Adversarial feasibility and risk review of the postgres-frontend blueprint (README, D1–D10,
  architecture, threat-model, FMEA, test-spec, test-harness, DSM, project-plan, todo/*). The
  blueprint is technically literate and unusually thorough on security and silent-correctness,
  but it has three load-bearing problems the sibling docs do not confront head-on. (1) SEMANTIC
  DRIFT FROM THE USER'S ASK: the original intent was "transactions use Accord"; D1 quietly
  demoted that to eventual-by-default with Accord as an opt-in GUC. Under the stated
  "compatible with the drivers" requirement this is not a tuning knob — it breaks the default
  contract every Postgres ORM is built on (read-your-writes, RETURNING, SELECT ... FOR UPDATE,
  serializable-by-default), and the per-session GUC does not rescue them because the ORMs never
  set it and poolers strip it. (2) M1 IS NOT A SMALL SLICE: "first JOIN end-to-end over a
  bespoke engine" transitively requires a SQL parser, binder, planner, two join operators with
  spill, a type/OID system, NUMERIC/timestamptz binary encoders, catalog emulation, SCRAM, the
  extended-query protocol, AND a ferrosa-session refactor of a 53,947-LOC crate — realistically
  a multi-quarter effort for a small team, not a milestone. (3) THE DIFFERENTIAL ORACLE, named
  as the top control for the dominant FMEA risk, is itself unsound as specified: collation,
  float/numeric formatting, and ordering differences between ferrosa and real Postgres will
  produce false mismatches, while queries ferrosa intentionally restricts produce coverage
  HOLES the oracle cannot see — so the control both cries wolf and has blind spots. Underneath
  these sit a long tail of under-specified wire surface (cursors/streaming vs ferrosa's
  per-partition scan model, COPY, SQLSTATE mapping, server_version gating, NOTICE channel,
  prepared-statement lifecycle) and a real schedule hazard in sequencing a 54k-LOC refactor as
  Sprint-0 foundation. This register does not relitigate the locked decisions; it surfaces what
  will actually hurt and names the decision needed for each. References the sibling FMEA/threat
  tables by ID rather than duplicating them.
---

# Postgres Front-End — Adversarial Risk Register

> Independent review companion to [`decisions.md`](./decisions.md),
> [`architecture.md`](./architecture.md), [`threat-model.md`](./threat-model.md),
> [`fmea.md`](./fmea.md), [`test-specification.md`](./test-specification.md),
> [`test-harness.md`](./test-harness.md), [`dsm.md`](./dsm.md),
> [`project-plan.md`](./project-plan.md). Failure modes are referenced as **FM-n** and threats
> as **S/T/I/D/E-n** from the siblings; their tables are not duplicated here. This document
> challenges the design; it does not re-decide it. Each risk: title, severity, why it bites,
> and the decision/recommendation needed.

Ground-truth checks run against the workspace on branch
`fix/p0-read-compaction-get-error-race` (used below): `ferrosa-cql` is **53,947 LOC**; the
storage read surface is `StorageEngine::range_iter_projected(table_id, wanted, partition_limit,
start, end) -> Stream<Partition>` — **token/`DecoratedKey`-keyed and partition-oriented**, and
returns an **empty stream on a missing table** (a silent fallback); `WritePath::range_read_
projected_*` lives in `ferrosa-cluster` and **errors in degraded mode**; Accord is wired only
through `ferrosa-cql/src/router.rs` for LWT today; there is **no collation/locale/Collator code**
anywhere in `ferrosa-storage`/`ferrosa-common`. These facts sharpen several risks below.

---

## 1. Semantic contradictions in the decisions

### R1 — The user asked for "transactions use Accord"; D1 delivered "eventual unless you opt in" — Critical

> **RESOLVED (D11):** explicit `BEGIN … COMMIT` blocks now always run on Accord
> (strict-serializable, read-your-writes inside) with no GUC required; autocommit single
> statements keep the eventual default. This is exactly the R1 recommendation below and
> honors the literal "transactions use Accord" intent. Analysis retained for context.

**Why it bites.** The blueprint's own framing (README, D6, Phase-0 grounding) records the
original goal as "transactions on Accord." D1 then inverts the default: the *standard* path is
ferrosa tunable/eventual consistency, and Accord is reached only by setting
`ferrosa.isolation=accord`. That is not a faithful encoding of "transactions use Accord" — it
is the opposite default, with the user's stated intent relegated to an opt-in that, by D1's own
admission, most drivers will never set. A reviewer reading only the README would believe
transactions are strict-serializable; they are not, unless a human remembers a GUC. The
sibling docs treat this as a settled tuning decision (FM-09, FM-22 are scored as "documented
expected behavior"), which launders a product-intent reversal into a test-matrix row.

There is a real defensibility argument for eventual-by-default (it matches ferrosa's substrate
and avoids forcing Accord cost on every read). But that argument was never made *against the
user's words*; D1 records it as a free choice, not as a deviation requiring sign-off.

**Decision needed.** Re-confirm with the requester, explicitly, that "eventual-by-default,
Accord-opt-in" is an acceptable reinterpretation of "transactions use Accord" — OR flip the
default so that **explicit `BEGIN ... COMMIT` transactions run on Accord** (read-your-writes
inside a txn) while only autocommit single statements use the eventual path. The latter
honors the literal ask and is closer to Postgres semantics: a `BEGIN` block is exactly where a
client signals it wants transactional guarantees. This is the single most important unresolved
question in the blueprint and should gate approval, not design phase.

### R2 — "Compatible with the drivers" vs eventual-by-default: the GUC does not rescue the ORMs — Critical

> **RESOLVED (D11):** binding strict semantics to the explicit transaction block (not the GUC
> the ORMs never set and poolers strip) fixes the concrete ORM breakage — read-after-write,
> `RETURNING`, `SELECT … FOR UPDATE`, and multi-statement snapshot expectations all occur
> *inside* transactions, which now get Accord for free. The autocommit eventual default and its
> documented staleness remain; option (a) below is the chosen path. Analysis retained for context.

**Why it bites.** D1 asserts the opt-in GUC reconciles eventual-default with driver
compatibility. It does not, for concrete, enumerable reasons. The default contract every
mainstream Postgres ORM is built on is **read-committed with read-your-writes**, and several
behaviors will silently break on the eventual path:

- **Read-after-write (RYW).** `INSERT ... RETURNING id` then `SELECT ... WHERE id=?` is the
  single most common ORM pattern (Django `save()` then re-fetch, Rails, SQLAlchemy
  `flush()`+`refresh()`, Prisma `create` then `findUnique`). On the eventual path this can
  return zero rows. FM-09 scores this severity-10 and calls it "expected" — but "expected by
  the spec author" is not "expected by the ORM," which treats a missing row as a logic error
  and may raise, retry, or corrupt state. This is the worst class of failure: looks like
  success, returns wrong data.
- **`RETURNING`.** Postgres `RETURNING` is defined to return the post-mutation tuple from the
  *same* statement. The blueprint never says how `INSERT/UPDATE ... RETURNING` is implemented
  over an eventual WritePath — is the returned row the value just written (correct) or a
  read-back (potentially stale)? If read-back, `RETURNING` is broken on the default path. This
  is unaddressed.
- **`SELECT ... FOR UPDATE` / `FOR SHARE`.** Row-level locking has no meaning on an eventual
  store and no Accord mapping is specified. ORMs use this for optimistic-lock and queue
  patterns. Silently ignoring the lock clause (returning rows as if locked) is a correctness
  trap; erroring is safer but breaks the ORM. Undecided.
- **Serializable-by-default expectations.** Some frameworks and test suites assume that two
  statements in a transaction see a consistent snapshot. On the eventual path with no snapshot
  (D2 explicitly notes a multi-operator plan reads a non-snapshot view), even a single
  multi-join SELECT can observe a torn read across operators. This is a *new* anomaly the
  blueprint acknowledges in one line (D2) but never tests.
- **The GUC is unreachable in practice.** ORMs do not emit `SET ferrosa.isolation`. Connection
  poolers in transaction/statement mode strip or cross-contaminate startup GUCs (the
  threat-model's own cross-cutting note on PgBouncer). So the "opt-in" is, for the dominant
  deployment shape (app + pooler + ORM), effectively unavailable — which means the eventual
  default is the *only* path most real workloads will ever see.

**Decision needed.** Either (a) make explicit transactions Accord-by-default (R1), which fixes
RYW, RETURNING, and FOR UPDATE inside transactions at a stroke; or (b) define and TEST each of
the four ORM behaviors above as first-class matrix rows with a documented, *driver-observable*
failure mode (not a silent stale row), and publish a "supported ORM operations" compatibility
statement so "compatible with the drivers" is scoped honestly rather than claimed broadly.
RETURNING and FOR UPDATE semantics must be specified before M2.

### R3 — "Full wire + real relational + bespoke engine" vs "working subset first": M1 is not a slice, it is most of a database — High

**Why it bites.** D6 frames M1 ("first JOIN end-to-end over a real driver") as a thin
vertical that front-loads risk. But because D3 forbids DataFusion, the *first* JOIN drags in
the entire spine, and the bespoke choice means none of it is borrowed. M1's transitive
requirements, taken from the blueprint's own Sprint F/0/1/2 contents:

- a hand-written SQL lexer + recursive-descent parser (subset, but real);
- a binder + name resolution over a catalog that does not exist yet;
- a logical plan + at least predicate/projection pushdown into a partition-keyed scan that
  does not expose a relational interface (ground truth: `range_iter_projected` is
  partition/token-oriented, not a table scan);
- two physical join operators (`HashJoin` + `NestedLoopJoin`) with bounded buffers and spill;
- a type/OID mapping plus text **and binary** encoders, including the genuinely fiddly ones
  if any join column is numeric/timestamp (FM-17 NUMERIC base-10000, FM-29 2000-epoch tz);
- `pg_catalog`/`information_schema` emulation good enough that psql AND psycopg complete their
  automatic connect-time introspection (FM-11) — this alone is a large, driver-specific corpus;
- full SCRAM-SHA-256 server exchange (FM-06) with verifier population on every password-set
  path across CQL and Postgres (FM-25);
- the extended-query protocol (Parse/Bind/Describe/Execute/Sync) carrying `$1` (FM-02/03);
- the D8 spine: database registry, default-db auto-landing, the unified `authorize()`
  checkpoint, and the CONNECT gate enforced on BOTH paths (the test-spec puts all of this in
  the M1 set);
- and *before any of that*, the `ferrosa-session` extraction (R12).

That is not "one join." That is a working SQL front-end minus aggregates, sort, subqueries,
and most types. The "milestone gate to re-confirm bespoke vs embed" (D3/project-plan S2) is
sound in spirit but arrives only after the team has already paid for the parser, binder,
planner, type system, catalog, SCRAM, and the session refactor — i.e. after most of the
sunk-cost is incurred, which blunts the gate's purpose (you cannot cheaply walk back to
DataFusion at that point because the type system and catalog are already bespoke).

**Decision needed.** Either (a) accept that M1 is a multi-quarter effort and rename it
accordingly (it is "v0.5," not "milestone 1"), or (b) move the bespoke-vs-embed gate
*earlier* by spiking a DataFusion-backed read path behind the same `ferrosa-sql` trait in
Sprint 1 — proving the wire/catalog/SCRAM spine against a borrowed engine first, then
swapping in bespoke operators incrementally. Option (b) de-risks the schedule and makes the D3
gate real, at the cost of a temporary Arrow dependency the blueprint wants to avoid. Decide
which cost you prefer; the current plan pays the bespoke cost up front and gates after.

---

## 2. Under-specified / missing wire & engine surface

### R4 — Cursors and large-result streaming vs ferrosa's per-partition scan model — High

**Why it bites.** Postgres clients stream: psql, JDBC `setFetchSize`, psycopg server-side
cursors, and the extended-query `Execute` row-count limit all assume the server can produce a
result incrementally and suspend (`PortalSuspended`). The architecture defers cursors
(`DECLARE`/`FETCH`) to post-M1, but *implicit* streaming via `Execute(max_rows)` and
`PortalSuspended` is part of the base extended-query protocol that M1 claims, and FM-28 already
flags portal interleaving. Meanwhile ground truth shows the engine produces `Stream<Partition>`
keyed by token — a per-partition lazy iterator, not a row cursor with a stable resumable
position across a join. Building a suspendable, back-pressured portal over a hash-join (whose
build side is fully materialized) is non-trivial: a suspended portal over a `HashJoin` pins the
entire build side for the portal's lifetime, interacting badly with D-6 (portal accumulation)
and D-5 (spill quotas). The blueprint never reconciles "Postgres clients expect streaming" with
"the engine's unit of work is a partition" and "joins materialize."

**Decision needed.** Specify the portal/streaming contract for M1 explicitly: does `Execute`
with a row limit suspend mid-join, or does M1 only support unsuspended execution (return all
rows, ignore the limit) with a documented limitation? If the latter, ensure drivers that send a
non-zero `Execute` max-row count still work (most send 0 = unlimited, but JDBC with fetch size
does not). Add a portal-suspension-over-join test to the FM-28 set. Decide before M1.

### R5 — Error-code mapping (internal/CQL errors → Postgres SQLSTATE) is asserted but not specified — High

**Why it bites.** Drivers branch on the 5-character SQLSTATE, not the message. The test-spec
sprinkles expected codes (`26000`, `42601`, `42501`, `3D000`, `53200/53400`, `22003`) but there
is **no mapping table** from ferrosa's internal error taxonomy (and the CQL/storage errors that
will surface through the shared paths) to SQLSTATE. Ground truth: storage returns
`ferrosa_common::Result` errors and `range_read_projected` returns a "degraded mode" error
string — what SQLSTATE does a degraded-mode write-path error become? An unmapped internal error
defaulting to `XX000 internal_error` will cause ORMs to treat transient backpressure as a fatal
bug, and a wrong class (e.g. returning `42xxx` syntax for a `53xxx` resource condition) makes
clients retry-vs-fail incorrectly. This is exactly the silent-degradation the project's
fail-loud rule forbids, but at the protocol layer.

**Decision needed.** Add an explicit `error.rs` SQLSTATE map (engine error variant → class) to
`ferrosa-sql` and a CQL/storage-error → SQLSTATE map at the dispatch boundary, with the
fail-loud rule applied: an unmapped error is `XX000` AND logged loudly with the source error, so
gaps are visible. Make it a Layer-1 unit test (every engine error variant has a non-`XX000`
SQLSTATE or an explicit justification). Specify before Sprint 1.

### R6 — Type-system completeness is hand-waved; the hard types are the common ones — High

**Why it bites.** D5/FM-10/17/19/20/29 acknowledge OID mapping, NUMERIC binary, typmod, and
collections, but the blueprint treats the type system as "mechanical but must be complete." It
is not mechanical for the types apps actually use: **numeric/decimal** (base-10000 binary,
arbitrary precision — ferrosa's CQL `decimal` is a different representation), **timestamptz**
(2000-epoch + timezone semantics ferrosa's `timestamp` does not carry), **uuid** (text vs
binary), **json/jsonb** (no CQL analog; the blueprint never says whether jsonb is supported),
**arrays** (CQL list/set → PG array OID is lossy on null/nesting), and **NULL vs empty** (FM-18,
length -1). Each is a correctness surface with a driver that will reject a wrong encoding.
"Complete enough that drivers receive valid OIDs" (D5) is far weaker than "round-trips
correctly," and the gap between them is where FM-10/17/29 live. The blueprint also never states
the v1 *supported type set* — without a closed list, the differential oracle's coverage is
undefined (R8).

**Decision needed.** Publish a closed, versioned **type support matrix** (CQL type → PG OID →
text encoder → binary encoder → typmod rule → null handling), mark unsupported types as
explicit fail-loud errors (FM-20 already wants this), and gate M2 on numeric+timestamptz+uuid
round-tripping through the driver matrix. Decide the json/jsonb and array story explicitly —
"unsupported in v1" is an acceptable answer; silence is not.

### R7 — Transaction isolation actually achievable over an eventual store is overstated — High

**Why it bites.** Layer 5 and D1 promise BEGIN/COMMIT/ROLLBACK and a correct `I/T/E` status
byte "regardless of CL," plus read-your-writes under Accord. But on the **eventual default
path** a multi-statement transaction has *no* isolation: statement 2 can see a different view
than statement 1, a `SELECT` inside the txn can observe other committers, and there is no atomic
commit across statements (each write hits WritePath independently). The status byte will say
`T`, implying transactional semantics that do not exist. This is a subtler version of R1: the
protocol *claims* a transaction is open while the store provides none of the guarantees a
transaction implies. The Accord path provides real semantics, but per R2 most workloads never
reach it. Calling the eventual path's BEGIN/COMMIT "correct because the status byte is right" is
correct at the wire layer and misleading at the semantic layer.

**Decision needed.** Define what `BEGIN ... COMMIT` *means* on the eventual path: is it (a) a
no-op autocommit-per-statement with a cosmetic status byte (must be documented loudly as "no
isolation, no atomicity"), or (b) does opening a transaction implicitly upgrade the session to
Accord (the R1 recommendation, which makes the status byte honest)? Pick one and test it. The
current "status byte correct regardless of CL" framing should not be allowed to stand in for
"transactions work."

### R8 — Multi-statement simple-query, NOTICE channel, and server_version gating are missing — Med

**Why it bites.** Three concrete driver-facing gaps the blueprint underplays:
- **Multi-statement simple Query.** libpq's simple-query protocol allows `a; b; c` in one
  message, executed as an implicit transaction, returning multiple result sets. psql and many
  migration tools rely on it. The codec lists `Query` but the execution semantics for
  multi-statement (implicit BEGIN, all-or-nothing, multiple CommandComplete) are unspecified.
- **NOTICE/warning channel.** `NoticeResponse` is listed in the message set but no source of
  notices is defined. ORMs and psql surface notices; more importantly, the *honest* way to tell
  a client "you are on the eventual path, this read may be stale" (R2) is a NOTICE — yet the
  blueprint's fail-loud posture has no wire mechanism wired up for it.
- **`server_version` gating.** Drivers gate features on the reported `server_version`: pgjdbc,
  psycopg, and pgx enable/disable protocol features, prepared-statement behavior, and SQL
  syntax based on it. I5 treats `server_version` only as a fingerprinting concern and says
  "report a minimal intentional string." But reporting too *low* a version disables extended
  features the drivers would otherwise use; too *high* makes them send syntax ferrosa cannot
  parse. The chosen version string is a compatibility decision, not just a security one.

**Decision needed.** Specify multi-statement simple-query semantics (or explicitly reject `;`
-separated batches with a clear error for v1); wire `NoticeResponse` and use it to surface
eventual-path staleness honestly; and choose `server_version` deliberately against the driver
matrix (test which features each driver enables at the chosen version), documenting the choice
in I5's acceptance gate rather than treating it as purely cosmetic.

### R9 — Collation and sort/ordering parity will make the differential oracle produce FALSE failures — High

**Why it bites.** This is both a missing-surface and an oracle-soundness problem, so it
straddles sections 2 and 3. Ground truth: **ferrosa has no collation/locale machinery.** Real
Postgres `ORDER BY text` uses the database/column collation (libc or ICU), which orders strings
differently from ferrosa's byte/UTF-8 ordering for anything beyond ASCII, and even differs for
case and punctuation. The test-spec's Layer-3 `ORDER BY` corpus (3.2) and FM-15 will therefore
report mismatches that are *not engine bugs* — they are collation differences — unless every
text ordering case is pinned to `COLLATE "C"` (byte order), which ferrosa can match but which is
not what apps use. The same class of false-failure hits float/numeric *formatting* (the
canonicalizer claims type-aware compare, but `1.0` vs `1`, `-0`, and numeric scale rendering are
exactly where FM-17 lives — the canonicalizer is being asked to paper over the very bug it
should catch).

**Decision needed.** State ferrosa's collation story explicitly: v1 supports **only `"C"`
collation** (byte ordering) and reports it as such in the catalog, OR it implements ICU/libc
collation (large). Constrain the differential corpus to the supported collation and assert the
catalog *reports* that collation so apps know. For numeric/float, the canonicalizer must compare
*decoded typed values*, never text, and must NOT round or normalize away precision — otherwise it
will hide FM-17. Document the canonicalization rules as a tested contract, because a too-loose
canonicalizer turns the centerpiece control into a rubber stamp.

---

## 3. Engine-correctness / oracle soundness

### R10 — The differential oracle has structural blind spots the FMEA treats as covered — Critical

> **RESOLVED (spec updated):** three-verdict oracle + COLLATE C v1 + restricted-query
> rejection oracle + float canonicalization. The differential oracle now returns
> **Match | Mismatch | OutOfScope** (Mismatch fails; OutOfScope is recorded, not
> failed); both sides run under `COLLATE "C"` / `LC_COLLATE=C` (locale/ICU collation
> parity DEFERRED as a declared limitation + follow-up), removing false ordering
> mismatches; float/numeric values are canonically rendered in the **Match**
> comparison (rendering only — never rounding away precision, so FM-17 stays
> catchable); and a **separate restricted-query rejection oracle** (`reject_oracle`)
> asserts that queries ferrosa does not support return a clean typed ERROR and emit
> NO rows — the fail-loud counterpart covering the differential oracle's structural
> blind spot. See [`test-specification.md`](./test-specification.md) §3.1/3.1a/3.3 and
> [`test-harness.md`](./test-harness.md) H2. Analysis retained for context.

**Why it bites.** FM-12/14 (RPN 420) name differential-vs-real-Postgres as *the* control, and
the whole test-spec leans on it. But the oracle is unsound in two structural ways the siblings
do not confront:

1. **False positives (cry wolf):** collation (R9), float/numeric formatting, NULL ordering
   defaults, and any nondeterministic plan ordering produce mismatches that are not engine bugs.
   A control that fires on non-bugs gets its failures triaged-away by humans, which is exactly
   how a *real* FM-12 mismatch slips through ("oh, that's just another ordering diff"). The
   harness's "no tolerance, no skip" rule (3.1) is in direct tension with the inevitability of
   benign diffs — something has to give, and if it's the discipline, the control is worthless.
2. **Coverage holes (blind spots):** the oracle can only test queries ferrosa *accepts*. Every
   query ferrosa intentionally restricts (cross-database joins per D8a, unsupported types per
   R6, FOR UPDATE per R2, unsupported SQL) is *outside* the oracle by construction — yet those
   restricted paths are where silent-wrong-vs-error decisions live (does ferrosa return wrong
   rows or a clean error for a query it half-supports?). Real Postgres accepts a superset of
   what ferrosa does, so a generator seeded from "queries Postgres accepts" (the SQLancer-style
   plan) spends most of its budget on queries ferrosa rejects, testing the *rejection* path, not
   the *result* path. The blind spot is precisely the partially-supported query that returns
   plausible wrong rows.

**Recommendation / decision needed.** Refine the oracle into three distinct verdicts, not a
binary match: **MATCH** (both return, equal), **BENIGN-DIFF** (both return, differ only in a
declared-acceptable dimension — must be an *enumerated, justified* allowlist, e.g. ordering
under non-`COLLATE "C"`, reviewed per-entry, never a tolerance), and **MISMATCH** (hard fail).
Separately, add a **restricted-query oracle**: for every query ferrosa rejects, assert it
rejects with a *clean typed error*, never wrong rows (this catches the blind-spot class without
real Postgres). And constrain the SQLancer generator to ferrosa's *supported* grammar/types/
collation so its budget tests result correctness, not rejection. Without these three changes the
top FMEA control is weaker than its RPN-mitigation claims. This is the most important technical
refinement in this register.

### R11 — Property-test "internal equivalences" (Layer 2.3) can be self-consistently wrong — Med

**Why it bites.** Layer 2.3 proves join commutativity, pushdown soundness, etc. as algebraic
properties over the engine's *own* operators. These catch optimizer *rewrite* bugs but, as the
test-spec itself notes, cannot catch a semantic bug the naive and optimized plans *share* (e.g. a
three-valued-logic bug in the join's null handling that is consistent across both plan shapes).
The blueprint correctly says "both are required" — but then leans the M1 correctness gate almost
entirely on Layer 3 (the oracle), which R10 shows has holes. The two controls have *correlated*
blind spots: a NULL-semantics bug that the oracle's benign-diff triage dismisses AND that the
internal properties preserve is invisible to both. NULL/3VL is explicitly called the
highest-risk area for a hand-rolled engine.

**Recommendation.** Add a dedicated **NULL/3VL conformance corpus** authored from the SQL
standard and Postgres docs (not generated, not differential) — known-answer vectors for `NULL =
NULL`, `IS NOT DISTINCT FROM`, NULL in aggregates, NULL in join keys, NULL ordering — so the
highest-risk semantics have a *self-contained* oracle independent of both the property tests and
the differential harness. Cheap, high-value, closes the correlated blind spot.

---

## 4. Dependency / sequencing risk

### R12 — `ferrosa-session` extraction is a 54k-LOC refactor on the critical path of Sprint 0 — High

**Why it bites.** D10/DSM correctly identify that depending on all of `ferrosa-cql` for
`SharedState` is wrong and that extraction into `ferrosa-session` is the clean fix. The
*architecture* is right. The *schedule* is the risk: this is a refactor of the struct at the
center of a **53,947-LOC** crate (ground-truth confirmed), touching `router.rs:860`, the
`main.rs:1217` construction site, and every call site that reads `SharedState` fields — and it
must keep the **entire existing CQL suite green** as a precondition for *any* Postgres work. The
plan (Sprint F) treats this as "moderate, mechanical, test-covered." Extractions of a
god-struct from a large crate are rarely mechanical: `SharedState` mixes neutral handles with
CQL-only fields (`prepared_cache`, `cql_metrics`, `event_sender`, trackers, `topology_policy`),
and the composition boundary (CQL composes `core` + private fields) will ripple through every
method that today reaches a field directly. If this refactor destabilizes the CQL path — the
*shipping* product — it blocks the Postgres work AND risks regressing live functionality, which
is a far worse outcome than a late Postgres feature.

**Recommendation / decision needed.** (a) Land `ferrosa-session` as a **standalone PR on its
own branch, merged and soaked, BEFORE the Postgres feature branch starts** — decouple its risk
from the new feature's timeline entirely. (b) Budget it as real work (estimate it independently;
do not bury it inside "Sprint F"). (c) Add an explicit regression gate beyond "tests green": run
the existing CQL load/race-stress nightly against the post-extraction build, since struct-sharing
changes can introduce contention/aliasing bugs that unit tests miss. (d) Keep `udf_executor`
composed in CQL, not in the neutral core (DSM §7.7 already leans this way), to avoid forcing a
`ferrosa-session → ferrosa-udf` edge for a dependency Postgres does not need at the wire layer.

### R13 — Harness-first (D9) + extraction-first (D10) stack two foundations before any value ships — Med

**Why it bites.** D9 mandates harness → RED tests → code, and the harness for the *centerpiece*
(H2 differential oracle) requires a running Postgres container, the `differential!` helper, AND a
working ferrosa Postgres listener to point it at — but the listener does not exist until Sprint
0. So the M1 differential tests cannot actually be RED-then-GREEN in a tight loop; they are RED
(no listener) for the entire Sprint 0/1 duration, which is a long time to hold "failing for the
right reason" across a moving target. Stacking the session extraction (R12) AND the full harness
AND the D8 control-plane interfaces all *before* the first wire byte means a long runway with no
end-to-end signal. This is a process risk, not a correctness one: it delays the first moment the
team learns whether the bespoke bet is even tractable.

**Recommendation.** Allow a thin **walking-skeleton exception** to strict D9 for Sprint 0 only:
stand up a trivial Postgres listener that answers `SELECT 1` end-to-end (handshake → SCRAM →
one hardcoded row) *before* the full harness, so the differential/conformance loop has a live
target and the team gets an end-to-end heartbeat in week 1. Keep strict TDD for all engine and
authz logic. This trades a small purity compromise for a large reduction in integration risk.

---

## 5. Operational / security residue (not already covered in threat-model/fmea)

### R14 — Reads depend on write-path health; degraded-mode read errors need a defined client contract — Med

**Why it bites.** Ground truth: `WritePath::range_read_projected_*` returns
`"range_read_projected unavailable: write path is in degraded mode"`. If the engine's read path
routes through the cluster write path (DSM §7.5 leaves this **explicitly unresolved**), then a
Postgres `SELECT` can fail purely because the *write* path is degraded — an operational coupling
no Postgres client expects, with no defined SQLSTATE (R5) and no documented retry guidance. The
threat-model and FMEA cover query-of-death and spill DoS but not this availability coupling.

**Recommendation / decision needed.** Resolve DSM §7.5 (does `physical/scan.rs` pull from
`ferrosa-storage` directly or via `ferrosa-cluster::WritePath`?) *before* M1 — it changes the
`ferrosa-sql` dependency edge and the read-availability story. If reads can hit degraded-mode
write-path errors, map them to a retryable SQLSTATE (`57P0x`/`53xxx` class) and document it.

### R15 — `range_iter_projected` returns an empty stream on a missing table — a silent fallback the engine must not inherit — Med

> **RESOLVED (spec + todo):** the architecture now mandates an explicit
> **table-absent-vs-empty** scan contract (table-absent ⇒ `Err`/`NoSuchTable`;
> table-empty ⇒ `Ok` zero rows), citing the fail-loud rule, and forbids the bespoke
> scan/join from inheriting the silent empty-stream fallback
> ([`architecture.md`](./architecture.md) §3.3). A new FMEA row **FM-41** (RPN 288,
> P1) scores the silent-wrong-result mode; a Layer-6 test asserts missing-table ⇒
> error (`42P01`/`3D000`) vs empty-table ⇒ zero rows as *distinct* outcomes
> ([`test-specification.md`](./test-specification.md)); and a todo audit
> ([`todo/storage-scan-fail-loud.md`](./todo/storage-scan-fail-loud.md)) fixes
> `ferrosa-storage` and gates the bespoke read path. Analysis retained for context.

**Why it bites.** Ground truth: `StorageEngine::range_iter_projected` returns
`futures::stream::empty()` when the table is not registered, and `range_read_projected` is
documented to return `Ok(vec![])` for an unknown table. That is a textbook silent-fallback (the
project's own safety rules forbid "return empty when the operation could not be performed"). If
the bespoke binder/scan layer trusts this, a query against a table that exists in the catalog but
is *not yet registered in storage* (timing, partial DDL broadcast, the D8 mapping race FM-36)
returns **zero rows instead of an error** — indistinguishable from a legitimately empty table.
For a JOIN, this silently drops one side. This is a fresh instance of the dominant FM-12 class,
introduced not by the join algorithm but by the storage contract beneath it.

**Recommendation.** The binder must resolve table existence against the catalog and assert
storage registration *before* scanning; a catalog-present/storage-absent table is a fail-loud
error (likely `57xxx`/`XX000` with context), never an empty scan. Add a test: query a
catalog-known but storage-unregistered table and assert an error, not empty rows. Wire this into
the FM-36 (mapping-race) coverage.

### R16 — CancelRequest over a shared cancel-key space across protocols, and pre-auth SCRAM CPU cost — Med

**Why it bites.** Two operational residues:
- **Cross-protocol cancel keys.** S3 covers `BackendKeyData` entropy for the Postgres path, but
  if cancellation routes a cancel signal into the *shared* `SharedState` (architecture's cancel
  arrow does), the cancel-key table is a new shared structure. Its lifecycle, eviction, and
  whether a Postgres cancel can ever reach a CQL/other operation (it must not) are unspecified.
- **SCRAM iteration cost as a pre-auth amplifier.** SCRAM-SHA-256 with a high iteration count is
  CPU-work the server performs *per connection attempt*, pre-auth. D-2 covers connection
  flooding by count, but a flood of *complete-looking* SCRAM client-first messages forces
  server-side PBKDF2/HMAC work per attempt — an asymmetric CPU DoS distinct from raw connection
  count. The iteration count is a security/DoS tradeoff the blueprint sets implicitly.

**Recommendation.** Specify the cancel-key table as Postgres-private (not shared with CQL) with a
bounded size and TTL; assert no cross-protocol cancel reach. Pin and document the SCRAM iteration
count as a deliberate DoS/security tradeoff, and add a per-IP pre-auth *CPU/attempt* budget on top
of D-2's connection-count budget.

### R17 — D8c default-db auto-landing + a public 5432 listener is a discoverability/blast-radius change — Med

**Why it bites.** D8c auto-lands every CQL-created keyspace into `ferrosa`, and the architecture
binds 5432 to `0.0.0.0` by default. Independently, each is defended (I7 gates data behind schema
grants; S1/I2 cover auth/TLS). Together they change operational posture: a brand-new Postgres
listener on a well-known port, reachable by every existing role that gains default-db connect
(E6), exposing *catalog existence* of every CQL keyspace by default. The threat-model covers the
authz correctness; the *operational* surprise — "we turned on Postgres and now every keyspace's
name/columns are enumerable to every connectable role" — is a deployment-default decision, not
just an authz one.

**Recommendation.** Default `FERROSA_POSTGRES_BIND` to `127.0.0.1:5432` (opt-in to public),
default the listener **off** until explicitly enabled, and require `require_tls` (I2) for any
non-loopback bind. Make "enable Postgres" a deliberate operator action with a documented
exposure note, not an on-by-default consequence of upgrading.

---

## Top 5 things to resolve before writing code

1. **R1/R2 — The Accord-vs-eventual default.** Get explicit requester sign-off that
   eventual-by-default reinterprets "transactions use Accord," OR flip explicit `BEGIN...COMMIT`
   transactions to run on Accord. Until this is settled, the product's core consistency contract
   — and the meaning of "compatible with the drivers" — is undefined. Specify RETURNING and
   FOR UPDATE semantics as part of this.
2. **R10/R9 — Make the differential oracle sound.** Three verdicts (MATCH/BENIGN-DIFF-allowlist/
   MISMATCH), a separate restricted-query rejection oracle, a generator constrained to ferrosa's
   supported grammar/types/collation, and a `"C"`-collation-only v1 story. The top FMEA control
   does not currently do what its RPN claims.
3. **R3 — Right-size M1 and move the bespoke-vs-embed gate earlier.** Either rename M1 to reflect
   its true multi-quarter scope, or spike a borrowed-engine read path behind the `ferrosa-sql`
   trait so the wire/catalog/SCRAM spine is proven before the bespoke operator cost is sunk.
4. **R12 — De-risk the `ferrosa-session` extraction.** Land it as a standalone, independently
   estimated, merged-and-soaked PR (with the CQL race-stress nightly as a gate) *before* the
   Postgres feature branch — do not let a 54k-LOC refactor's risk ride on the new feature's
   timeline or destabilize the shipping CQL path.
5. **R5/R6 — Pin the error-SQLSTATE map and the closed type-support matrix.** Drivers branch on
   both; both are currently asserted-but-unspecified, and the fail-loud rule has to be enforced
   at the protocol boundary (unmapped error/type → loud error, never a silent default).

## Effort / timeline reality check (honest)

**M1 (first JOIN end-to-end):** the blueprint frames this as a slice; it is most of a SQL
front-end. Realistically M1 requires the session extraction (a refactor risk in its own right),
a hand-written parser+binder+planner, two join operators with spill, a type/OID system with text
and binary encoders, catalog emulation broad enough for two real drivers' connect introspection,
full SCRAM with cross-protocol verifier population, the extended-query protocol, AND the D8
control-plane spine (registry, default-db landing, unified `authorize()` on both paths). For a
small team building bespoke (no DataFusion), this is on the order of **3–5 months**, not a
sprint or two — and that estimate assumes the differential oracle is made sound early (R10),
because without it the team cannot trust any "green." If the oracle's blind spots are discovered
late, add re-work.

**v1 ("full Postgres wire + real relational over a bespoke engine"):** D3 itself calls this a
"multi-person-year subsystem," and that is the honest figure — re-deriving a planner/optimizer,
the full type system (numeric/timestamptz/json/arrays/collation), aggregates/sort/subqueries/
CTEs with spill, COPY, cursors, the full driver matrix, and the D8 multi-database RBAC across
both protocols is **well over a person-year, plausibly 2–3**, dominated by the bespoke engine and
the long tail of driver-conformance and type-correctness edges that only surface against real
clients. The blueprint's security and silent-correctness analysis is strong and worth keeping;
the schedule framing (M1 as a small milestone, the session refactor as mechanical, the
differential oracle as a sufficient correctness backstop) is the part that will hurt if taken at
face value. Treat M1 as a *feasibility spike with a real go/no-go gate*, and make that gate cheap
enough to actually exercise (R3) before committing to the multi-year bespoke build.
