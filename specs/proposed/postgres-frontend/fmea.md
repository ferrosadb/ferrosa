---
title: "FMEA — Postgres Wire-Protocol Front-End for Ferrosa"
status: proposed
executive_summary: |
  Failure Mode and Effects Analysis for a NEW Postgres wire-protocol front-end on the
  ferrosa database (bespoke relational engine over wide-column storage). Scope covers
  wire framing/state-machine, the extended-query (portal/prepared-statement) lifecycle,
  SCRAM-SHA-256 auth, the BESPOKE planner/executor (join/aggregate/sort/spill), type/OID
  mapping, catalog emulation, eventual-consistency vs ORM expectations, Accord opt-in
  engagement, cross-protocol password-set verifier population, and result encoding edge cases.

  DOMINANT RISK: the bespoke engine returning SILENTLY-WRONG join/aggregate results
  (high severity, low detection). FM-12 (wrong JOIN) and FM-14 (wrong aggregate) are the
  worst outcomes and carry the highest RPNs. The primary control is DIFFERENTIAL TESTING
  against a real PostgreSQL of identical query+data, byte-comparing result sets. This honors
  the project "fail loud, never fake" rule: when the engine cannot prove a result correct it
  must crash/error, never emit plausible-but-wrong rows.

  D8 (multi-database / unified-RBAC) ADDS A SECOND CLASS of silent-but-wrong failures on the
  AUTHORIZATION surface (FM-33..FM-40). The new dominant risk on this surface is GRANT-CHECK
  DIVERGENCE between the Postgres and CQL enforcement paths (FM-33): a role denied on one path
  is allowed on the other — silent privilege escalation, same spirit as FM-12/14 (high severity,
  low detection). Its twin is the rollout migration SILENTLY WIDENING access (FM-34). The
  primary controls mirror the engine surface: a SINGLE shared enforcement point consulted by
  both paths, DIFFERENTIAL AUTHZ tests running the SAME grant fixtures through Postgres and CQL,
  and an explicit fail-loud migration with a before/after effective-permission audit diff.

  P1 WORK ITEMS (RPN >= 200): FM-12 (wrong JOIN results), FM-14 (wrong aggregate results),
  FM-08 (planner resource blowup / OOM — Power-of-10 bounded loops & allocation), FM-22
  (explicit `BEGIN…COMMIT` block silently not engaging Accord → wrong isolation), FM-25 (cross-protocol password-set
  does not populate SCRAM verifier), FM-33 (grant-check divergence Postgres-vs-CQL → privilege
  escalation), FM-34 (rollout migration silently widens access), FM-41 (storage scan returns an
  empty stream on a missing table → JOIN silently wrong/empty instead of erroring). Severity-10
  items: FM-12, FM-14, FM-22, FM-09 (silent read-after-write staleness presented as success),
  FM-33 (silent privilege escalation).

  All rows with RPN >= 50 carry a test-case note (differential / detection / exploitation /
  recovery). Milestone 1 (D6: first JOIN end-to-end over SCRAM with a real driver) is gated
  on FM-12 and FM-25 controls being in place.

  D8 SCOPE (FM-33..FM-40): unified database/schema RBAC enforced across BOTH Postgres and CQL
  (D8b), keyspace↔database many-to-many mapping with database-bounded joins (D8a), default-db
  auto-landing of unmapped keyspaces (D8c), the `pg_database` / filtered-`pg_namespace` catalog,
  and the CQL backward-compat rollout migration. See specs decisions.md (D8a/D8b/D8c) and
  todo/multi-database-control-plane.md.
---

# FMEA — Postgres Wire-Protocol Front-End for Ferrosa

## 1. Scope

A new Postgres wire-protocol (v3) front-end is added to ferrosa so that standard Postgres
drivers and ORMs connect to the existing wide-column engine. The front-end terminates the
Postgres protocol, authenticates with SCRAM-SHA-256, parses SQL, and runs queries through a
**bespoke** relational engine (own planner / optimizer / join / aggregate / sort / spill) on
top of ferrosa wide-column storage. There is no DataFusion.

### Locked design constraints (designed-to)

- **D1** — Eventual-consistency default. Strict-serializable (Accord) is opt-in via session GUC
  `ferrosa.isolation=accord` (connection-time via StartupMessage `options`, or per-session `SET`).
- **D2/D3** — Bespoke relational engine. Highest schedule + correctness risk.
- **D4** — SCRAM-SHA-256 verifier stored alongside bcrypt in the shared `ferrosa-schema` role
  store; populated at every password-set path (CQL and Postgres) so roles work across
  protocols. Legacy bcrypt-only roles cannot log in via Postgres until reset.
- **D5** — keyspace = Postgres schema; `pg_catalog` / `information_schema` emulated; CQL-type ↔
  PG-OID mapping table required. _(single-database assumption SUPERSEDED by D8)_
- **D6** — Milestone 1 = first JOIN end-to-end with a real driver over SCRAM.
- **D8** — Multi-database model (database → schema=keyspace → table). **D8a:** keyspace↔database
  is many-to-many via a mapping table; JOINs are database-bounded (no cross-database joins).
  **D8b:** ONE unified grant model — database-level `CONNECT`/`USAGE ON DATABASE` plus
  `GRANT ON SCHEMA` (mapped onto keyspace perms) gate **both** Postgres and CQL via a single
  shared enforcement point; rollout must not silently revoke or widen existing CQL roles' access.
  **D8c:** a keyspace with no explicit attachment auto-lands in the default database `ferrosa`.
- **D10** — the single `authorize()` lives in `ferrosa-schema` (pure over a metadata snapshot);
  both front-ends reach the shared core via the neutral `ferrosa-session` crate (no
  `postgres → cql` dependency edge).

### Functions under analysis

1. Wire framing & connection state machine (startup, simple query, extended query, Sync, Terminate).
2. Extended-query lifecycle: Parse/Bind/Describe/Execute/Close + portal & prepared-statement state.
3. Transaction-status byte (`I` idle / `T` in-transaction / `E` failed) reporting in `ReadyForQuery`.
4. SCRAM-SHA-256 authentication exchange.
5. Bespoke planner/optimizer + executor (join, aggregate, sort, spill).
6. Type / OID mapping (CQL type ↔ PG OID) and text/binary result encoding.
7. Catalog emulation (`pg_catalog`, `information_schema`) for driver/ORM introspection.
8. Consistency semantics surfaced to ORMs (read-after-write).
9. Accord isolation opt-in engagement.
10. Cross-protocol password-set → SCRAM verifier population.
11. Unified database/schema RBAC enforced across both Postgres and CQL (D8b) — single shared check.
12. keyspace↔database mapping & registry, database-bounded join visibility/binding (D8a).
13. Default-database auto-landing of unmapped keyspaces (D8c).
14. `pg_database` + database-filtered `pg_namespace`/`pg_class`/`pg_attribute` catalog.
15. CQL backward-compat rollout migration (database-connect grant backfill) under D8b.

### Operating conditions

Normal (single client), concurrent (many portals/sessions), overloaded (large joins/sorts →
spill), degraded (S3 write-behind lag, replica staleness, partial catalog coverage).

## 2. Rating scales

Standard FMEA 1–10 scales for Severity (impact), Occurrence (likelihood of cause), and
Detection (1 = caught by type system/protocol, 10 = silent in production). **RPN = S × O × D.**
Thresholds: **>= 200 = P1 work item (must address before ship)**; 100–199 = High (this release);
50–99 = Medium (plan mitigation); < 50 = Low (accept/defer). Any Severity-10 item is treated as
critical regardless of RPN.

## 3. FMEA table

| # | Component | Failure Mode | Effect | Cause | S | O | D | RPN | Mitigation / Detection control |
|----|-----------|--------------|--------|-------|---|---|---|-----|--------------------------------|
| FM-01 | Wire framing | Message length/body desync (frame boundary lost) | Connection wedges or misreads next message → protocol corruption | Off-by-one in length-prefix parse; partial read not buffered to full frame | 8 | 4 | 3 | 96 | Length-prefixed reader with exact-fill; fuzz the framer; assert remaining-bytes == 0 after each message decode (fail loud) |
| FM-02 | Extended query | Prepared-statement / portal name desync (Bind references unknown/stale statement) | Driver gets `ErrorResponse` mid-pipeline or executes wrong plan | Statement/portal maps not keyed per-connection; reuse-after-Close; unnamed-statement overwrite semantics wrong | 7 | 5 | 4 | 140 | Per-connection name→object maps; explicit lifecycle state machine; reject Bind to missing statement with `26000`; lifecycle unit tests + driver pipeline test |
| FM-03 | Extended query | Sync / error-recovery not draining to next Sync | After an error, server processes queued messages instead of discarding until Sync → cascade of spurious errors | Missing "skip until Sync" state after ErrorResponse | 7 | 5 | 4 | 140 | Implement error→skip-until-Sync per spec; emit single ErrorResponse then ReadyForQuery only on Sync; pipeline-after-error integration test |
| FM-04 | Txn status byte | `ReadyForQuery` reports wrong status (`I`/`T`/`E`) | ORM transaction tracking corrupted; ORM commits/rolls back at wrong time → data integrity surprise | Transaction state not updated on BEGIN/COMMIT/ROLLBACK/error; failed-txn `E` not latched until ROLLBACK | 8 | 4 | 5 | 160 | Single source of truth for txn state; latch `E` on first error until ROLLBACK; assert state transitions; driver-level BEGIN/error/ROLLBACK test |
| FM-05 | Wire framing | Unsupported/unknown message type handled silently | Driver hangs waiting for response, or undefined behavior | Catch-all match arm that ignores unknown frontend messages | 6 | 3 | 4 | 72 | Exhaustive match on message tag; unknown tag → ErrorResponse + close (fail loud, never ignore); negative test sends bogus tag |
| FM-06 | SCRAM auth | SCRAM exchange aborts mid-flow (client-final / server-final mismatch) | Client cannot connect; auth loop or hang | Nonce concatenation, channel-binding flag, or base64 of proof computed incorrectly | 7 | 4 | 3 | 84 | Use vetted SCRAM-SHA-256 impl; RFC 5802 test vectors; real-driver auth integration test; explicit error on proof mismatch |
| FM-07 | SCRAM auth | Channel-binding negotiation mishandled (`p=` vs `n=`/`y=`) | Driver requiring `SCRAM-SHA-256-PLUS` fails or downgrades silently | gs2 header handling incomplete; advertise PLUS but don't bind | 6 | 4 | 5 | 120 | Advertise only mechanisms actually supported; reject mismatch loudly; test with psql/JDBC channel-binding=require |
| FM-08 | Planner/executor | Resource blowup — unbounded intermediate state / spill failure → OOM | Process OOM-kills, all sessions die (availability) | Hash-join build side or sort buffer unbounded; spill-to-disk not triggered or fails silently; Power-of-10 bounded-allocation violated | 9 | 5 | 5 | **225** | **P1.** Hard memory budget per query; bounded operators with mandatory spill; cap intermediate cardinality; fail query with `53200`/`53400` (out of memory / config limit) BEFORE OOM; spill-fault injection test; loadgen large-join stress |
| FM-09 | Consistency | Read-after-write staleness presented to ORM as success | ORM reads its own just-written row as absent/old → app logic breaks; looks like success (silent) | D1 eventual-consistency default; S3 write-behind / replica lag; ORM assumes RYW | 10 | 6 | 6 | **360** | **P1.** Document eventual default loudly; offer read-your-writes mode / `ferrosa.isolation=accord` for RYW; ORM-pattern test (insert→select) on default vs accord; surface staleness in session, never fake freshness |
| FM-10 | Type/OID mapping | Wrong PG OID emitted in RowDescription | Driver decode error or silent type coercion garbage | CQL→OID table incomplete/incorrect; collection/UDT mapped to wrong OID | 8 | 5 | 4 | 160 | Authoritative mapping table with tests per type; RowDescription OID asserted against driver-expected; round-trip decode test per type |
| FM-11 | Catalog emulation | `pg_catalog`/`information_schema` gap fails driver introspection at connect | Connection fails before first query; ORM unusable | ORM/driver queries a catalog relation/function not emulated (e.g., `pg_type`, `pg_namespace`, `format_type`) | 8 | 6 | 4 | 192 | Capture real driver introspection queries (psql, JDBC, psycopg, SQLAlchemy, Prisma); emulate required relations/functions; connect-handshake test per driver; missing-catalog → explicit error not empty fake |
| FM-12 | Bespoke planner | **JOIN returns silently-wrong rows** (missing/extra/mispaired) | Application reads incorrect data believing it correct — worst outcome | Bespoke join algorithm bug: predicate pushdown, null-handling in equi-join, multi-column key, hash collision, outer-join padding | 10 | 6 | 7 | **420** | **P1 / DOMINANT.** Differential testing vs real Postgres: identical schema+data+query, byte-compare result sets; randomized join query generator (SQLancer-style); fail loud — if engine can't prove correctness, error rather than emit rows. Gates Milestone 1 (D6) |
| FM-13 | Bespoke planner | Outer/anti/semi-join null-padding or filter-placement wrong | Wrong result set for LEFT/RIGHT/FULL/NOT EXISTS | WHERE vs ON predicate placement; null-extended rows filtered incorrectly | 9 | 5 | 7 | 315 | Covered by FM-12 differential harness with explicit outer-join corpus; dedicated null-padding cases |
| FM-14 | Bespoke planner | **Aggregate returns wrong value** (SUM/COUNT/AVG/GROUP BY) | Silently wrong analytics/totals — financial/decision impact | Grouping-key hash bug; NULL-in-aggregate semantics (COUNT(*) vs COUNT(col)); numeric overflow/rounding; HAVING placement | 10 | 6 | 7 | **420** | **P1.** Differential testing vs real Postgres incl. NULL/empty-group/overflow; property tests (sum of partition sums == total); reject on overflow rather than wrap |
| FM-15 | Bespoke planner | ORDER BY / sort produces wrong order or drops rows under spill | Paginated/ordered results wrong; ORM `LIMIT/OFFSET` returns wrong page | External merge-sort bug; unstable sort where stability assumed; collation mismatch vs PG | 8 | 5 | 6 | 240 | Differential ordering tests incl. spilled sort, ties, collation; assert row count preserved across spill (fail loud on count mismatch) |
| FM-16 | Result encoding | Binary vs text format mismatch for a column | Driver decode error or garbled value | Result-format codes (per-column in Bind) ignored; value encoded text when binary requested (or vice versa) | 8 | 5 | 4 | 160 | Honor per-column format codes from Bind; encode matrix test (text+binary) per type; driver binary-mode integration test |
| FM-17 | Result encoding | NUMERIC/DECIMAL binary wire format wrong | Driver throws or silently wrong number | PG NUMERIC base-10000 digit encoding (weight/scale/sign) implemented incorrectly | 9 | 5 | 5 | 225 | Implement against PG NUMERIC binary spec; vector tests incl. negatives, scale, zero; round-trip vs psycopg/JDBC BigDecimal |
| FM-18 | Result encoding | NULL encoded as empty value (length 0) instead of -1 | Driver reads `''` instead of NULL → wrong data | DataRow column length field uses 0 instead of -1 for NULL | 9 | 4 | 4 | 144 | NULL → length -1 always; explicit NULL-vs-empty-string test per type; assert in encoder |
| FM-19 | Type/typmod | typmod / precision-scale not surfaced (varchar(n), numeric(p,s)) | Driver/ORM schema reflection wrong; truncation surprises | RowDescription/`pg_attribute` typmod hardcoded -1 | 6 | 5 | 5 | 150 | Compute typmod from schema; surface in RowDescription and catalog; reflection test vs ORM migration tooling |
| FM-20 | Type mapping | CQL collection / UDT / blob mapped to a PG type the driver can't decode | Query on a table with collections fails at decode | No clean PG analog for `list/set/map/UDT`; mapped to bytea/text without driver agreement | 7 | 5 | 6 | 210 | Define explicit mapping (array OID for list/set, jsonb/composite for map/UDT); document unsupported; error loudly on unmapped type rather than emit garbage; per-type driver decode test |
| FM-21 | Planner | Predicate not pushed to storage → full-scan / timeout | Query slow or times out; looks like hang | Optimizer fails to translate WHERE into partition/clustering-key restriction | 6 | 5 | 5 | 150 | Cost/restriction translation tests; assert partition-key restriction generated for eligible predicates; slow-query log + EXPLAIN |
| FM-22 | Accord on txn block | A `BEGIN…COMMIT` block silently does NOT engage Accord (runs eventual) | App believes an explicit transaction is strict-serializable with read-your-writes; actually eventual → invisible correctness/isolation violation | Entering the explicit block (status byte `T`) not threaded to the Accord coordinator; GUC-forced autocommit path diverges; transaction-status transition not wired to isolation selection | 10 | 5 | 8 | **400** | **P1.** Route explicit blocks through Accord on entering `T`; test that a `BEGIN…COMMIT` block engages Accord **without** any GUC (read-your-writes inside; strict-serializable, observe TxnId); on unknown/unsupported GUC value → ERROR (never silently default); expose effective isolation via `SHOW ferrosa.isolation`; fail loud on mismatch |
| FM-23 | Accord opt-in | StartupMessage `options` parsing drops/ignores the GUC | Connection-time isolation request lost → defaults to eventual silently | `options` startup param not parsed, or only `SET` path wired | 9 | 4 | 6 | 216 | Parse `options` GUCs at startup; assert effective value matches requested; connect-with-options integration test asserting `SHOW` echoes accord |
| FM-24 | SCRAM / roles | Legacy bcrypt-only role attempts Postgres login | User locked out of Postgres until password reset | D4: SCRAM verifier absent for pre-existing roles | 5 | 7 | 3 | 105 | Documented behavior; clear error `password authentication failed (no SCRAM verifier — reset password)`; admin report of roles missing SCRAM verifier; test login attempt → explicit actionable error |
| FM-25 | Cross-protocol auth | Password-set path does NOT populate SCRAM verifier | Role can't log in via Postgres after a CQL/other password change → broken auth, support load | D4 not enforced at every set-password path; CQL `ALTER ROLE ... PASSWORD` writes bcrypt only | 8 | 6 | 5 | **240** | **P1.** Centralize password-set so EVERY path writes both bcrypt + SCRAM verifier; schema invariant test: after any set-password, both verifiers present; cross-protocol test (set via CQL → login via PG) |
| FM-26 | Catalog emulation | Emulated catalog returns stale/empty after DDL | ORM migration/reflection sees wrong schema → broken migrations | Catalog views not reflecting live schema metadata; cache not invalidated on DDL | 7 | 4 | 5 | 140 | Back catalog views with live schema store; invalidate on DDL; reflection-after-DDL test; never serve empty as "no columns" silently |
| FM-27 | Planner | Spill-to-disk path corrupts/loses rows under memory pressure | Wrong result set (subset) returned, looks successful | Spill serialization bug; partial spill file read; row-count not verified across spill boundary | 9 | 4 | 7 | 252 | Row-count + checksum across spill boundary (fail loud on mismatch); spill fault-injection; differential vs PG on spilled queries; ties to FM-08/FM-15 |
| FM-28 | Wire / concurrency | Concurrent portals on one connection interleave state | Cross-talk between portals; wrong rows to wrong Execute | Shared mutable portal state without proper per-portal isolation | 7 | 3 | 5 | 105 | Per-portal cursors/state; suspended-portal (`PortalSuspended`) handling; multi-portal interleave test |
| FM-29 | Result encoding | Date/time/timestamptz epoch & timezone encoding wrong | Off-by-timezone or epoch-base errors in datetime values | PG uses 2000-01-01 epoch for timestamp binary; tz handling vs CQL `timestamp` (ms since 1970) | 8 | 5 | 5 | 200 | PG epoch/tz conversion vectors; differential vs PG for timestamptz; round-trip driver test across timezones |
| FM-30 | SCRAM auth | Verifier stored with wrong iteration count / salt format | All Postgres logins fail or weakened security | SCRAM verifier serialization (`SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>`) malformed | 7 | 3 | 4 | 84 | Store canonical PG verifier string; parse/serialize round-trip test; psql login against a verifier built by our set-password path |
| FM-31 | Planner | Integer / NUMERIC overflow in aggregate wraps silently | Wrong totals, no error (fail-quiet) | Native add without overflow check in SUM | 9 | 4 | 7 | 252 | Checked arithmetic; on overflow raise `22003 numeric_value_out_of_range` (fail loud, never wrap); overflow property test |
| FM-32 | Wire framing | Large message / parameter exceeds limit unhandled | OOM or truncation on huge Bind parameter | No cap on parameter/message size | 6 | 3 | 4 | 72 | Enforce max message size; reject oversized with ErrorResponse; fuzz oversized Bind |
| FM-33 | Unified RBAC (D8b) | **Grant-check DIVERGENCE between Postgres path and CQL path** — role denied on one path is allowed on the other | Silent privilege escalation: attacker uses the permissive path to read/write data the policy denies — worst authz outcome | Two enforcement points (Postgres engine vs CQL router) implement the grant check independently and drift; database-connect/schema grant evaluated in one path but not the other; ordering/precedence of database vs keyspace perms differs | 10 | 6 | 8 | **480** | **P1 / DOMINANT (authz).** SINGLE shared enforcement point (`authorize(role, db, schema/keyspace, action)`) that BOTH the Postgres engine and CQL router call — no path-local copy. Differential authz tests: drive the SAME grant fixtures through both paths and assert identical allow/deny. Fail loud: unknown/unmapped grant → deny + error, never default-allow |
| FM-34 | Rollout migration (D8b) | Migration SILENTLY WIDENS access — auto-grant `CONNECT ON DATABASE` reaches keyspaces a role should not | Role gains access to data it never had; silent over-grant looks like a clean migration | Blanket auto-grant of `CONNECT ON DATABASE ferrosa` (or implicit-connect) sweeps in keyspaces the role lacked perms on; default-db (D8c) aggregates more keyspaces than the role's prior reach | 9 | 5 | 8 | **360** | **P1.** Explicit, scoped migration (grant connect only where the role already held underlying keyspace perms); FAIL LOUD on any role whose effective set would change unexpectedly; compute an AUDIT DIFF of effective permissions before/after and require sign-off; test: post-migration effective-perm set ⊆ documented expansion |
| FM-35 | Rollout migration (D8b) | Migration SILENTLY REVOKES access — existing CQL role loses keyspace access after unification | Availability/functional failure: existing CQL workloads start getting permission-denied on keyspaces they legitimately used | Unification now requires a database-connect grant the legacy role never had; migration fails to backfill connect for that role's keyspaces' database | 8 | 5 | 6 | 240 | Backfill database-connect for every (role, keyspace→database) the role already had keyspace perms on; before/after audit diff must show NO net revocation; pre-flight test: replay existing CQL role perms post-migration and assert no previously-allowed action becomes denied; fail loud on any drop |
| FM-36 | Mapping table (D8a) | keyspace↔database mapping inconsistent across cluster (DDL broadcast race) | Keyspace visible in a database on some nodes but not others; the SAME join succeeds on one node, errors on another — non-deterministic correctness | `CREATE DATABASE`/attach and CQL `CREATE KEYSPACE` mutate the registry; DDL broadcast doesn't cover the new tables or races against query routing; no version/epoch on the mapping | 8 | 5 | 6 | 240 | Broadcast the new registry/mapping tables through the SAME DDL/metadata propagation as schema; version the mapping and gate query planning on a converged epoch; differential test: attach on node A, immediately query node B, assert consistent visibility or explicit retry — never silent divergence |
| FM-37 | Default-db (D8c) | Auto-landing exposes a freshly CQL-created keyspace to all default-db-connect roles before intended | Newly created keyspace's data readable by every role holding `CONNECT ON DATABASE ferrosa` the instant it lands — premature/over-broad exposure | D8c auto-lands unmapped keyspaces into `ferrosa`; schema-level grants not yet set, but database-connect roles see it immediately via the default-db aggregation | 7 | 5 | 6 | 210 | Auto-landing grants VISIBILITY only via the database gate, but schema/keyspace-level grants still required for data access (database connect ≠ table read); assert default-deny at schema level for newly landed keyspaces; test: create keyspace via CQL, connect as default-db role, assert no row access without explicit schema grant |
| FM-38 | Binder / visibility (D8a) | Connection bound to database A references keyspaces only attached to B | Cross-database join that should be illegal SUCCEEDS — correctness + tenant isolation break | Binder/visibility resolves schema names against the global keyspace set instead of the connected database's attached set; database-bounded join check missing or evaluated after name resolution | 9 | 5 | 7 | 315 | Binder restricts visible schemas to the connected database's attached keyspaces ONLY; cross-database reference → error `3D000`/`42P01` clearly; differential test: bind to DB A, reference a B-only keyspace, assert error not silent rows; assert join reach == connected-db attachment set |
| FM-39 | Catalog filter (D8a) | `pg_database` / filtered `pg_namespace` returns keyspaces from the WRONG database | Driver/ORM introspection shows foreign or wrong schemas; reflection/migrations target the wrong database's objects | Catalog virtual tables not filtered by the connected database's attached keyspaces; `pg_database` lists registry rows the caller can't connect to; filter applied to `pg_namespace` but not `pg_class`/`pg_attribute` | 7 | 5 | 5 | 175 | Filter ALL catalog views (`pg_namespace`/`pg_class`/`pg_attribute`) by the connected database AND caller grants, consistently; `pg_database` lists only connectable databases; driver-introspection test per database asserts no foreign schema leaks; assert catalog filter == binder visibility set |
| FM-40 | Many-attach semantics (D8a) | Keyspace attached to many databases — dropping/perms applied against the wrong attachment | Detaching from one database silently affects another (data/visibility loss), or per-database perms differ and the WRONG one is applied (over- or under-grant) | Many-to-many attachment treated as a single shared object on drop; permission resolution doesn't key on (database, keyspace) pair so it picks an arbitrary/foreign grant set | 8 | 4 | 6 | 192 | Detach is per-(database,keyspace) and never cascades to other attachments; permission resolution keyed on the CONNECTED database's (database,keyspace) grant; test: attach KS to A and B with differing grants, assert each connection sees its own database's grant; detach from A leaves B intact (fail loud if a drop would orphan B) |
| FM-41 | Storage scan contract | **Scan returns an EMPTY STREAM on a missing table** instead of erroring — JOIN silently produces wrong/empty result | A query against a catalog-known but storage-unregistered table returns zero rows indistinguishable from a legitimately empty table; a JOIN silently drops one side → silently-wrong result presented as success | `ferrosa-storage::range_iter_projected` returns `futures::stream::empty()` (and `range_read_projected` returns `Ok(vec![])`) on an unregistered table — a silent fallback the project's fail-loud rule forbids; the bespoke binder/scan inherits it (timing / partial DDL broadcast / D8 mapping race FM-36) | 9 | 4 | 8 | **288** | **P1.** Fix the scan contract so **table-absent is an explicit `Err` (`NoSuchTable`)**, distinct from **table-empty (`Ok`, zero rows)**; the binder resolves existence against the catalog AND asserts storage registration BEFORE scanning; a catalog-present/storage-absent table is fail-loud (`3D000`/`42P01` or `XX000` with context), never an empty scan. Test: query a catalog-known but storage-unregistered table → error, not empty rows; wired into the FM-36 mapping-race coverage. See `todo/storage-scan-fail-loud.md` |

## 4. Test-case notes (every row with RPN >= 50)

Each note follows the adversarial triad: **D** = detection test, **X** = exploitation test (try to
trigger), **R** = recovery test. Differential tests compare ferrosa output byte-for-byte against a
reference PostgreSQL given identical schema, data, and query.

- **FM-01 (96):** D — assert decoder leaves 0 bytes after each message. X — fuzz frame lengths
  (short, long, split across reads). R — connection error is clean, next connection works.
- **FM-02 (140):** D — Bind to unknown statement → `26000`. X — Close then Bind same name; unnamed
  statement overwrite mid-pipeline. R — Sync recovers, subsequent statements work.
- **FM-03 (140):** D — after forced error, only one ErrorResponse then RFQ at Sync. X — pipeline
  Parse/Bind/Execute where Bind errors; verify later messages skipped. R — next Sync resumes clean.
- **FM-04 (160):** D — `ReadyForQuery` byte asserted after BEGIN (`T`), after error (`E`), after
  ROLLBACK (`I`). X — error inside txn then issue command; must reject until ROLLBACK. R — ROLLBACK
  clears to `I`.
- **FM-05 (72):** D — bogus message tag → ErrorResponse+close. X — send unknown tag. R — new
  connection unaffected.
- **FM-06 (84):** D — RFC 5802 test vectors pass. X — corrupt client proof → auth fails loudly.
  R — retry with correct creds succeeds.
- **FM-07 (120):** D — driver with `channel_binding=require` connects or gets explicit reject.
  X — advertise PLUS, attempt bind mismatch. R — fallback path documented, no silent downgrade.
- **FM-08 (225, P1):** D — query exceeding memory budget → `53200`/`53400` before OOM. X — loadgen
  join/sort sized to exceed budget; spill-disk-full injection. R — failed query frees memory, session
  survives, next query works. Power-of-10: assert bounded build side & sort buffer.
- **FM-09 (360, P1):** D — insert→immediate select on default shows documented eventual behavior;
  on accord shows RYW. X — write then read across replica under induced lag. R — accord/RYW mode
  returns own write deterministically. Never fake freshness.
- **FM-10 (160):** D — RowDescription OID per type == driver-expected. X — query every CQL type.
  R — round-trip decode succeeds per type.
- **FM-11 (192):** D — capture & replay each real driver's connect introspection (psql, JDBC,
  psycopg, SQLAlchemy, Prisma). X — connect with each driver. R — missing catalog object → explicit
  error logged, not silent empty.
- **FM-12 (420, P1, DOMINANT):** D — differential harness byte-compares JOIN result vs real PG.
  X — randomized join-query generator (multi-key, null, outer, large) against shared dataset.
  R — on any mismatch the harness fails the build; engine errors rather than emit unproven rows.
  **Gates Milestone 1 (D6).**
- **FM-13 (315):** D — outer/anti/semi-join corpus differential vs PG. X — generate LEFT/RIGHT/FULL/
  NOT EXISTS with NULLs and WHERE-vs-ON predicates. R — mismatch fails build.
- **FM-14 (420, P1):** D — differential aggregate (SUM/COUNT/AVG/GROUP BY/HAVING) vs PG incl. NULL,
  empty group, overflow. X — randomized aggregate generator; property test sum-of-parts. R — overflow
  → `22003`, never wrap.
- **FM-15 (240):** D — differential ORDER BY incl. ties, collation, spilled sort. X — sort dataset
  larger than memory budget. R — assert row count preserved across spill; mismatch fails loud.
- **FM-16 (160):** D — encode matrix text+binary per type matches driver. X — Bind requesting binary
  for every column. R — driver decodes without error.
- **FM-17 (225):** D — NUMERIC binary vectors (neg/scale/zero) match PG. X — round-trip BigDecimal via
  JDBC/psycopg. R — values equal after round-trip.
- **FM-18 (144):** D — NULL DataRow column length == -1. X — query rows with NULL and empty-string in
  same column. R — driver distinguishes NULL from ''.
- **FM-19 (150):** D — typmod surfaced for varchar(n)/numeric(p,s). X — ORM schema reflection / migration
  diff. R — reflected schema matches DDL.
- **FM-20 (210):** D — per-type driver decode for list/set/map/UDT/blob. X — query table with each
  collection type. R — unmapped type → explicit error, never garbage bytes.
- **FM-21 (150):** D — assert partition-key restriction generated for eligible WHERE. X — query with
  pushable predicate; verify no full scan via EXPLAIN. R — slow-query log fires on miss.
- **FM-22 (400, P1):** D — query-path assertion that an explicit `BEGIN…COMMIT` block invokes
  Accord (observe TxnId/HLC); `SHOW ferrosa.isolation` echoes effective mode. X — run a
  `BEGIN…COMMIT` block with **no GUC** and assert read-your-writes + strict-serializable, not
  eventual; run conflicting explicit txns. Unknown GUC value → ERROR. R — invalid value rejected
  at set time.
- **FM-23 (216):** D — connect with `options=-c ferrosa.isolation=accord`; `SHOW` echoes accord.
  X — both StartupMessage and per-session SET paths. R — mismatch between requested and effective →
  loud error.
- **FM-24 (105):** D — bcrypt-only role login via PG → actionable error naming "reset password".
  X — attempt login pre-reset. R — after reset, SCRAM login succeeds.
- **FM-25 (240, P1):** D — schema invariant: after ANY set-password path, both bcrypt + SCRAM
  verifiers present. X — set password via CQL `ALTER ROLE`, then log in via Postgres. R — login
  succeeds cross-protocol; missing verifier fails the invariant test loudly.
- **FM-26 (140):** D — catalog reflects schema after CREATE/ALTER/DROP. X — DDL then immediate
  reflection. R — invalidation refreshes; never serves stale empty as truth.
- **FM-27 (252):** D — row-count + checksum verified across spill boundary. X — induce spill with
  fault injection (truncated spill file). R — corruption detected → query errors, not partial result.
- **FM-28 (105):** D — two portals on one connection return correct independent rows. X — interleave
  Execute on portal A and B. R — `PortalSuspended` resumes correctly.
- **FM-29 (200):** D — timestamptz binary vectors match PG (2000-epoch, tz). X — round-trip datetimes
  across timezones via driver. R — values equal after round-trip.
- **FM-30 (84):** D — verifier string parse/serialize round-trip. X — build verifier via our
  set-password, log in with psql. R — login succeeds with canonical format.
- **FM-31 (252):** D — SUM overflow → `22003`. X — aggregate values summing past i64/numeric range.
  R — error returned, no wrapped value emitted.
- **FM-32 (72):** D — oversized Bind parameter rejected. X — fuzz with > max-size parameter.
  R — connection survives or closes cleanly, no OOM.
- **FM-33 (480, P1, DOMINANT authz):** D — DIFFERENTIAL AUTHZ harness: load one grant-fixture
  corpus (role × database-connect × schema/keyspace × action) and assert Postgres-path and
  CQL-path return identical allow/deny for every cell. X — for each fixture deny one path,
  attempt the action via the other path; any allow that the policy denies fails the build.
  R — both paths route through the single `authorize()` checkpoint; an unmapped grant denies and
  errors loudly, never default-allows. Mirrors FM-12 (silently-wrong → fail loud).
- **FM-34 (360, P1):** D — compute effective-permission AUDIT DIFF before vs after migration;
  assert post ⊆ documented-expansion set (no surprise widening). X — run migration on a fixture
  with a role lacking keyspace perms; assert it does NOT gain connect-reach to those keyspaces.
  R — migration is explicit/scoped and FAILS LOUD (aborts) on any role whose effective set would
  change beyond the documented rule; sign-off gate on the diff.
- **FM-35 (240):** D — replay every existing CQL role's prior allowed actions post-migration;
  assert NONE becomes denied (no net revocation). X — legacy bcrypt/CQL role with keyspace perms
  but no database grant; connect+query post-migration. R — backfill connect for the role's
  keyspaces' database; audit diff shows zero revocations; fail loud on any drop.
- **FM-36 (240):** D — attach keyspace→database on node A, immediately query node B; assert
  consistent visibility (or explicit converged-epoch retry), never silent divergence. X — race
  attach against concurrent joins across nodes. R — mapping versioned; planning gated on
  converged epoch; the same join yields the same allow/error on every node.
- **FM-37 (210):** D — create keyspace via CQL (auto-lands in `ferrosa`), connect as a
  default-db-connect role, assert NO row access without an explicit schema/keyspace grant
  (database connect ≠ table read). X — race read against the auto-landing moment. R — default-deny
  at schema level for freshly landed keyspaces; visibility ≠ data access.
- **FM-38 (315):** D — bind connection to database A, reference a keyspace attached only to B;
  assert clear error (`3D000`/`42P01`), never silent rows. X — construct a cross-database join
  A↔B-only; assert it is rejected as illegal. R — binder visibility == connected-db attachment
  set; join reach asserted equal to that set.
- **FM-39 (175):** D — per-database driver introspection: `pg_database` lists only connectable
  databases; `pg_namespace`/`pg_class`/`pg_attribute` filtered to the connected database's
  keyspaces + caller grants; assert no foreign schema appears. X — connect to A, query catalog,
  scan for B-only objects. R — catalog filter set asserted equal to binder visibility set (FM-38).
- **FM-40 (192):** D — attach one keyspace to databases A and B with DIFFERING grants; from each
  connection assert the CONNECTED database's grant is applied (not the other's). X — detach the
  keyspace from A; assert B's attachment + data remain intact (no silent cascade). R — perms keyed
  on (database, keyspace); detach is per-attachment; fail loud if a drop would orphan B.
- **FM-41 (288, P1):** D — query a table that is **present in the catalog but not registered in
  storage**; assert the scan/JOIN ERRORS (`NoSuchTable` → `3D000`/`42P01`), never returns empty
  rows. Distinguish from a legitimately EMPTY table, which returns zero rows with no error.
  X — race a query against a partial DDL broadcast / D8 mapping convergence (FM-36) so the catalog
  knows the table before storage does; assert error, not a silently-dropped JOIN side. R — binder
  asserts storage registration before scanning; `range_iter_projected` returns `Err` (not an empty
  stream) for an unregistered table; fix is the `todo/storage-scan-fail-loud.md` audit.

## 5. Summary — top RPN items and P1 work items

### Top RPN items (descending)

| Rank | # | Failure Mode | S | O | D | RPN | P1? |
|------|----|--------------|---|---|---|-----|-----|
| 1 | FM-33 | Grant-check divergence Postgres-vs-CQL → privilege escalation (DOMINANT authz) | 10 | 6 | 8 | **480** | ✅ |
| 2 | FM-12 | JOIN returns silently-wrong rows (DOMINANT engine) | 10 | 6 | 7 | **420** | ✅ |
| 2 | FM-14 | Aggregate returns wrong value | 10 | 6 | 7 | **420** | ✅ |
| 4 | FM-22 | Explicit `BEGIN…COMMIT` block silently not engaging Accord | 10 | 5 | 8 | **400** | ✅ |
| 5 | FM-09 | Read-after-write staleness presented as success | 10 | 6 | 6 | **360** | ✅ |
| 5 | FM-34 | Rollout migration silently WIDENS access | 9 | 5 | 8 | **360** | ✅ |
| 7 | FM-13 | Outer/anti/semi-join null-padding wrong | 9 | 5 | 7 | 315 | — |
| 7 | FM-38 | Connection bound to DB A references DB-B-only keyspace (illegal cross-db join succeeds) | 9 | 5 | 7 | 315 | — |
| 9 | FM-41 | Storage scan returns empty stream on missing table → JOIN silently wrong/empty | 9 | 4 | 8 | **288** | ✅ |
| 10 | FM-27 | Spill-to-disk drops/corrupts rows | 9 | 4 | 7 | 252 | — |
| 10 | FM-31 | Aggregate integer/NUMERIC overflow wraps silently | 9 | 4 | 7 | 252 | — |
| 12 | FM-15 | ORDER BY / sort wrong under spill | 8 | 5 | 6 | 240 | — |
| 12 | FM-25 | Cross-protocol password-set misses SCRAM verifier | 8 | 6 | 5 | **240** | ✅ |
| 12 | FM-35 | Rollout migration silently REVOKES existing CQL access | 8 | 5 | 6 | 240 | — |
| 12 | FM-36 | keyspace↔database mapping inconsistent across cluster (DDL race) | 8 | 5 | 6 | 240 | — |
| 16 | FM-08 | Planner resource blowup / OOM | 9 | 5 | 5 | **225** | ✅ |
| 16 | FM-17 | NUMERIC binary wire format wrong | 9 | 5 | 5 | 225 | — |
| 18 | FM-23 | StartupMessage `options` GUC dropped | 9 | 4 | 6 | 216 | — |
| 19 | FM-20 | Collection/UDT mapped to undecodable PG type | 7 | 5 | 6 | 210 | — |
| 19 | FM-37 | Default-db auto-landing exposes fresh keyspace prematurely | 7 | 5 | 6 | 210 | — |
| 21 | FM-29 | Date/time epoch & timezone encoding wrong | 8 | 5 | 5 | 200 | — |
| 22 | FM-40 | Keyspace attached to many DBs — wrong-database perms / drop affects another | 8 | 4 | 6 | 192 | — |
| 23 | FM-39 | `pg_database` / filtered `pg_namespace` returns wrong-database keyspaces | 7 | 5 | 5 | 175 | — |

### P1 work items (RPN >= 200) → go to project plan

| # | Title | RPN | Why P1 |
|----|-------|-----|--------|
| FM-33 | Single shared grant-enforcement point + differential authz tests (both paths) | 480 | Silent privilege escalation if Postgres/CQL checks diverge; dominant authz risk |
| FM-12 | Differential-tested correct JOINs (dominant engine risk) | 420 | Silently wrong data; gates Milestone 1 (D6) |
| FM-14 | Differential-tested correct aggregates | 420 | Silently wrong totals/analytics |
| FM-22 | Explicit `BEGIN…COMMIT` block actually engages Accord (no GUC needed) | 400 | Silent wrong isolation; explicit block must be strict-serializable; unknown GUC value must ERROR |
| FM-09 | Read-after-write expectation handled (RYW/accord mode) | 360 | ORMs break silently on eventual default |
| FM-34 | Explicit fail-loud rollout migration + before/after audit diff | 360 | Silent access widening; migration must never over-grant unnoticed |
| FM-13 | Outer/anti/semi-join correctness (under FM-12 harness) | 315 | Silently wrong result sets |
| FM-41 | Fail-loud storage scan contract (table-absent `Err` vs table-empty `Ok`) | 288 | Missing-table empty stream → JOIN silently drops a side; silent-wrong-result class beneath the engine |
| FM-27 | Spill row-count/checksum integrity | 252 | Silent subset results under memory pressure |
| FM-31 | Checked aggregate arithmetic (no silent overflow) | 252 | Fail-loud on overflow per project rule |
| FM-15 | Sort correctness under spill | 240 | Wrong order / dropped rows |
| FM-25 | Centralized password-set populates SCRAM verifier | 240 | Broken cross-protocol auth (D4) |
| FM-08 | Bounded planner allocation + mandatory spill (Power-of-10) | 225 | OOM kills all sessions |
| FM-17 | Correct NUMERIC binary encoding | 225 | Driver decode failure / wrong numbers |
| FM-23 | StartupMessage `options` GUC parsing | 216 | Connection-time isolation lost silently |
| FM-20 | Explicit CQL→PG type mapping (collections/UDT) | 210 | Undecodable rows; error loudly when unmapped |
| FM-29 | Correct datetime/timestamptz encoding | 200 | Off-by-timezone/epoch data errors |

### Cross-cutting controls (apply across many rows)

1. **Differential testing against real PostgreSQL** — the single most important control. A reference
   PG runs the same schema+data+query; ferrosa output is byte-compared. Covers FM-12, FM-13, FM-14,
   FM-15, FM-17, FM-29 and more. Pair with a randomized query generator (SQLancer-style) to surface
   join/aggregate divergence the team would never hand-write.
2. **Fail loud, never fake (global rule)** — when the engine cannot prove a result correct (overflow,
   spill checksum mismatch, unmapped type, unknown isolation GUC), it MUST raise a Postgres
   `ErrorResponse` with a real SQLSTATE rather than emit plausible-but-wrong rows or silently default.
   This converts the highest-severity *silent* failures (FM-12/14/22/09/27/31) into loud,
   detectable ones — lowering effective Detection risk.
3. **Power-of-10 bounded loops & allocation** — every planner operator (hash build, sort, group)
   carries a hard cardinality/memory budget and a mandatory spill path; no `while true` without a cap;
   exceed → error before OOM (FM-08, FM-15, FM-27, FM-32).
4. **Centralized auth invariant** — one password-set function writes both bcrypt + SCRAM verifier;
   a schema invariant test enforces it after every path (FM-24, FM-25, FM-30).
5. **Driver-matrix connect test** — psql, JDBC, psycopg, SQLAlchemy, Prisma each complete connect +
   introspection + a JOIN over SCRAM; this is the executable definition of Milestone 1 (D6) and
   exercises FM-02/03/04/06/07/10/11/16/26.
6. **Single shared authorization checkpoint + differential authz testing (D8b)** — exactly one
   `authorize(role, database, schema/keyspace, action)` function; BOTH the Postgres engine and the
   CQL router call it (no path-local copy). A grant-fixture corpus is driven through both paths and
   allow/deny asserted identical (FM-33). Visibility (binder) and catalog filtering resolve against
   the SAME connected-database attachment set, so the set the planner can join (FM-38) equals the set
   the catalog reveals (FM-39). This is the authz analogue of control 1 (differential testing) and
   control 2 (fail loud) — an unmapped/unknown grant DENIES and errors, never default-allows.
7. **Explicit fail-loud RBAC migration with effective-permission audit diff (D8b rollout)** — the
   CQL backward-compat migration is scoped, not blanket: it computes each role's effective
   permissions before and after, requires the post-set to differ only by the documented expansion,
   and ABORTS (fail loud) on any unexpected widen (FM-34) or revoke (FM-35). Database-connect ≠ table
   read: a default-db-connect grant gives schema VISIBILITY only; data access still needs the
   schema/keyspace grant (FM-37, FM-40). The keyspace↔database mapping propagates through the same
   versioned DDL/metadata broadcast as schema so node visibility cannot diverge (FM-36).

### Milestone-1 (D6) gate

Milestone 1 — first JOIN end-to-end with a real driver over SCRAM — is **gated** on:
FM-25 (SCRAM verifier populated so the driver can authenticate), FM-11 (catalog emulation so the
driver completes connect introspection), and FM-12 (the JOIN result proven correct by the
differential harness). Ship M1 only when those three controls are green.
