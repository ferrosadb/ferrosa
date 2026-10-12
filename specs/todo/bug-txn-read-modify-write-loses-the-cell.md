---
title: A read-modify-write inside a transaction loses the committed cell
status: todo
priority: P1
source: isolation-probe
created: 2026-10-11
updated: 2026-10-11
component: transaction-manager
executive_summary: >
  A write whose VALUE depends on a read inside a transaction commits with the row present and
  the updated column NULL. Isolated on the live cluster to a single variable: a concurrent
  read-modify-write pair loses the survivor's cell, while a concurrent BLIND-write pair carries
  it. The fix starts with the parse-time form - Cassandra's LET block, where the read is
  declared in the statement - and then moves to MVCC with strict ordering and PostgreSQL
  conflict-resolution semantics so that transactions behave, from the front end, the way
  PostgreSQL transactions do.
---

# A read-modify-write inside a transaction loses the committed, cell

## Symptom (measured on the live three-node cluster)

Two simultaneous transactions, both COMMITs in flight, one variable changed between runs:

| shape | refused peer | survivor reports | survivor's cell |
|---|---|---|---|
| concurrent READ-MODIFY-WRITE (`SET v = v + 2`) | refused `40001` (loud) | COMMIT success | **NULL** - value lost |
| concurrent BLIND write (`SET v = 222`) | refused `40001` (loud) | COMMIT success | **222** - value carried |

The refused peer correctly did not apply its write in either shape. So the trigger is the read the
UPDATE performs inside the transaction - not concurrency alone, not the statement shape.

Reproduced three times on the live cluster (probe `txn-iso2-probe.sh`, V1). NOT reproducible at two
local levels: the in-process two-node fixture is green 3/3, and three real local node processes are
green 1/1. **The reproducer is therefore the live probe**; a local green is not evidence about this
defect.

## Not the cause (ruled out with evidence)

- Concurrency alone: blind writes survive it (V2 above).
- The statement shape: the identical single-transaction form is correct (V0 control).
- The harness: a positive control (one transaction's committed value read back by value through a
  fresh connection) passes on every run.

## Phase order (requester's direction)

1. **Parse-time form first - Cassandra's `LET` block.** The simpler case, because the read is
   declared in the statement instead of being an implicit consequence of an open transaction:
   `BEGIN TRANSACTION; LET cur = SELECT v FROM t WHERE aid = 7; UPDATE t SET v = cur + 1 WHERE
   aid = 7; COMMIT TRANSACTION;` The read-set is then explicit at PARSE time.
2. **Then MVCC with strict ordering and PostgreSQL conflict-resolution semantics**, so transactions
   behave from the front end the way PostgreSQL transactions do.

## Test list

- [ ] I1 parse: `BEGIN TRANSACTION ... LET cur = SELECT v FROM t WHERE aid = 7 ... COMMIT TRANSACTION`
      produces a transaction whose read-set is exactly `(ks, t, aid=7)` and whose staged write carries
      the update bound to `cur` - not an implicit storage read inside the write.
- [ ] I2 effect: after COMMIT the row reads back BY VALUE through a fresh connection, equal to
      old + 1. (Proceeds is not takes-effect.)
- [ ] I3 (the defect): a transaction whose write VALUE depends on a read carries that value on commit.
      Judged by the live probe; a local level cannot fail on it.
- [ ] I4 refusal: a `LET` over a read that cannot be served exactly (a scan, a non-key predicate) is
      REFUSED BY NAME inside a transaction, never served silently from the local store.
- [ ] I5 read-your-write: a later statement referencing the bound variable sees the transaction's own
      value.
- [ ] I6 PG semantics (phase 2): two concurrent transactions - a refusal is loud and named (40001),
      every peer reporting success carries its value, and the assertions are stated over ALL
      outcomes rather than asserting which side of the race wins.

## Implementation plan (recon complete, 2026-10-11)

GREEN for I1 touches two seams, and the repo already supplies the precedent for both:

1. **AST (`ferrosa-cql/src/ast.rs`)** — add `pub struct LetBinding { pub name: String,
   pub stmt_index: usize }` and `bindings: Vec<LetBinding>` on `Statement::TransactionBlock`.
   Keep both `pub`: the variant is public, so a public field is reachable and does NOT trip
   clippy's dead-field lint — no `_unused` parameter, no lint suppression.
   `TransactionBlock` already exists (ast.rs:166) and both construct sites are in the parser
   (parser.rs:669 and :676), which is why this is a two-call-site change rather than a
   workspace-wide one. Every `match` on the variant uses `..`, so no match arm breaks.

2. **Parser (`ferrosa-cql/src/parser.rs`)** — intercept `LET` at the top of the
   `parse_transaction_block` loop (parser.rs:663), BEFORE `parse_statement()`. `LET` must be a
   **soft keyword** (a bare identifier), exactly as `TIMEOUT` is handled at parser.rs:721-736:
   `TokenKind::Ident(s) | QuotedIdent(s)` compared with `eq_ignore_ascii_case("let")`, so a
   table or column named `let` keeps working. A single-token lookahead suffices (the same note
   the `IN TRANSACTION` suffix parser makes at parser.rs:746-750). Then parse
   `<ident> = <SELECT ...>`, push the SELECT into `statements`, and record
   `LetBinding { name, stmt_index: statements.len() - 1 }`.

3. **Wiring the read-set** — `transaction_keys::extract_where_keys` already exists and is
   consumed; `session.rs:304 stage_read` already stages a `TransactionRead`. The binding's
   SELECT feeds those, so the read-set is populated at parse time rather than inferred.

The red that must go green is `router::tests::a_let_block_declares_its_read_at_parse_time`,
currently uncommitted and failing with
`SyntaxError("unexpected token Ident(\"LET\") at position 18")`.

## State of the cycle

- I1 RED is written and uncommitted (the waiver guard forbids the `#[ignore]` holding pattern,
  so the working tree is the breadcrumb). PR 548's head is untouched and green.
- I2-I6 remain unwritten. I3 can only be judged by the live probe.

## Cycle state (2026-10-11, updated)

**DONE — I1, committed `427c5c77` and pushed to `feat/read-in-transaction` (PR 548).** A `LET` block
parses into a `TransactionBlock` whose read is explicit at PARSE time: `LetBinding` carried on the
variant, `LET` as a SOFT keyword on the `TIMEOUT` precedent (so a table named `let` keeps working),
interception at the top of the block loop before `parse_statement`, and a binding that is not a
SELECT refused by name. Red first on `SyntaxError(unexpected token Ident("LET"))`. Gates: ferrosa-cql
lib 1244 passed / 0 failed, workspace clippy clean under `-D warnings`, fmt clean.

**NEXT — I2: resolve the binding into the consuming statement's value.** `route_transaction_block`
loops the block's statements through `route_in_transaction` and IGNORES `bindings` (deferred on
purpose), so `cur` in `SET v = cur + 1` remains an unresolved identifier. The red asserts the bound
name resolves to the value read in the same transaction, observable at the CQL entry point.

**TRAP, do not write this test the obvious way.** The `dml_client_with_committer` fixture in
`ferrosa-postgres/tests` installs a `MockTransactionCommitter`, which records whatever it is handed
and reports `Committed`. A value assertion against it passes for a reason unrelated to any defect:
it is green by construction, and it is precisely why the cell loss is invisible in-process at every
level. The claim that a committed value SURVIVES belongs to I3 only.

**THEN — I3:** the live probe (`txn-iso2-probe.sh`, V1 read-modify-write). The only level that can
judge the lost cell - and the reason this work exists.

**THEN — phase 2:** MVCC with strict ordering and PostgreSQL conflict-resolution semantics, so
transactions behave from the front end the way PostgreSQL's do.

## I2 status correction (2026-10-11)

My first I2 test was WRONG and is relabelled. I assumed the grammar refuses a bound name as the
UPDATE operand; it does not. `SET v = cur + 1` parses today — the counter-increment path takes the
assignment target from the column and discards the left-hand term, and a bare word is accepted as a
term. So that test was green on arrival and could not fail on I2; keeping it as an "I2 red" would
have been a green test implying coverage.

**I2's real red is at RUNTIME, not at parse time.** `route_transaction_block` iterates the block
through `route_in_transaction` and ignores `bindings`, so the bound name resolves to nothing when
the write is staged: the value that reaches the committer is not the value this transaction read.
Witnessing that requires an engine-backed fixture (the SELECT must actually read), and it is OWED.

### I2 red: the fixture gap it is blocked on (2026-10-11)

The red belongs at `route_transaction_block` and needs a committer that RECORDS what it is handed.
The instrument exists: `MockTransactionCommitter::committed() -> Vec<Vec<TransactionWrite>>` and
`.reads() -> Vec<Vec<TransactionRead>>`
(`ferrosa-storage/src/accord/transaction_committer.rs:446` and `:453`). For THIS claim the mock is
the right instrument - the question is what value reached the committer, not whether a cell survives
(latter is I3, live only).

What blocks it is a FIXTURE gap, not a product mystery: the CQL router takes its committer from
`state.accord_transaction_committer()` (`ferrosa-cql/src/router.rs:3202`) and **no CQL-side fixture
installs one** - `setup()` and `setup_with_engine_overrides` do not expose it, and no test in
`ferrosa-cql` references `MockTransactionCommitter`. The seam for installing one is
`AccordAccess::fixed(committer)` (`ferrosa-postgres/src/accord_access.rs:65`), the same shape the
PostgreSQL-side fixture already uses.

**Next increment:** add a CQL-side fixture whose `SharedState` carries an `AccordAccess` with a
recording `MockTransactionCommitter`, then write the red - a `LET` block whose bound read must feed
the value that reaches the committer.

### CORRECTION: the committer is NOT installable — the fixture must run CLUSTER mode (2026-10-11)

My previous note named `AccordAccess::fixed(committer)` as the seam. **That is wrong for this path**
and I am retracting it: it is the PostgreSQL front-end seam. On the session/CQL side the committer is
not stored anywhere — `SessionCore::accord_transaction_committer()`
(`ferrosa-session/src/lib.rs:112`) **builds it on demand** from the live `write_path` + `peer_manager`
+ `accord_clock`, deliberately, so every front-end obtains it the same way and nothing is threaded
through `SharedState`.

Consequences for the I2 red:

1. **A recording mock cannot be installed here.** `MockTransactionCommitter` is reachable in
   `ferrosa-session`'s type space but nothing stores it; the accessor ignores any such field because
   there is none.
2. **The fixture must run `DeploymentMode::Cluster`** — `ferrosa-session`'s own
   `core(DeploymentMode::Cluster, true)` shows the shape, and that accessor's tests
   (`cluster_node_still_yields_an_accord_committer`) assert a real committer is produced. The CQL
   fixture's `setup()` is Standalone, which is exactly why the sibling test is named
   `..._fails_commit_in_standalone`.
3. **So the assertion must be the EFFECT, not a recording**: after the block commits, read the row
   back and assert the value. That is a stronger assertion than the recorded-call version would have
   been, and it is the same shape I3 uses — the difference is only the level (local cluster-mode
   process vs live cross-host cluster).

This also matches the requester's requirement directly: the CQL front end must run the same
cluster-mode commit path the PostgreSQL front end does, so that transactions behave the same way from
either front end.

### I2 effect-level fixture: attempted, REVERTED, and the exact gaps (2026-10-11)

I built the cluster-mode CQL fixture. The structure was right: `SessionCore`'s twelve fields are all
public and `SharedState.core` is public, so a CQL test CAN construct a cluster core (one-node ring →
`WritePath::cluster` + peer manager + clock) and install it on the state, which is what makes a real
commit reachable from this crate.

The type paths were not, and I **reverted it** rather than leave a non-compiling test carrying a
placeholder assertion in the tree. Behaviour unchanged.

What the compiler rejected — this is the worklist for the next attempt, and resolving it is
mechanical:

1. `ferrosa_cluster::PeerManager`, `NetConfig`, `HybridLogicalClock`, `NoopListener` do NOT live in
   `ferrosa_cluster`; `ferrosa-session`'s own test helper imports them, so read its import block.
2. `ferrosa_common::NodeConfig` is not the right path for `NodeConfig`.
3. The row read-back is not `StorageEngine::read_row` with `RowKey` / `CellValue` / `Timestamp`;
   those names are not in `ferrosa_storage`. Find the engine's actual point-read API.
4. `route_select_raw`'s call shape did not match (E0308) for a non-transactional SELECT.

Next attempt: take those four from `ferrosa-session/src/lib.rs`'s test module imports and the
engine's read API, then the fixture is a copy of that helper. The assertion stays the EFFECT: the
value in the row after the block commits — never a mock recording, because the committer here is
built on demand and cannot be replaced.

### I2 RED ACHIEVED — and the product's answer is a named refusal (2026-10-11)

The fixture landed and the test runs against a REAL commit path: a cluster-mode CQL core built from
`SessionCore`'s public fields (one-node ring → `WritePath::Cluster` + peer manager + Accord clock),
installed on the state, with `accord_transaction_committer()` asserted `Some` before anything else so
the test cannot pass by quietly having no commit path at all. Read-back is the EFFECT through the
front end (`route_select_raw` → `SelectRawResult.rows`), never a recorded call.

Result, verbatim:

    the LET block must commit, not fail: column 'v': unsupported collection assignment:
    Add of a non-collection value — not supported inside a transaction (BEGIN…COMMIT) or logged
    batch; use a non-transactional UPDATE

So read-modify-write inside a transaction is **not silently wrong on the CQL front end — it is
refused**, by name and with the remedy. Deliberate unsupported-feature refusal, not a defect.

**The consequence that matters:** the two front ends DISAGREE about the same operation.

- CQL: read-modify-write in a transaction → refused loudly (this message).
- PG (live, RT-3): read-modify-write in a transaction → commits, and silently leaves the cell NULL.

That is exactly the requirement this work serves — transactions must behave the same way from either
front end. Neither is the target, but they are different failures, and the fix must reconcile them:
support the read-in-transaction form and make the written value come from the bound read.

The red is preserved as `let-i2-red.patch` in the session scratch rather than committed, because a red
test on the PR branch breaks the branch's gate. It gets committed together with its green.

Next: the GREEN. Two named gaps to close — (1) permit a read-modify-write assignment inside a
transaction, (2) resolve the bound name to the value this transaction read so the write carries it.

### CQL green LANDED; PG is a DIFFERENT mechanism (2026-10-11)

**CQL is green.** `a transaction resolves the read it declared` (3a49ac1d): the block's LET reads run
up front, and each `Add`/`Sub` naming a binding is rewritten into the concrete assignment it means, so
the write carries the value this transaction read. The old refusal is gone and the block reaches
COMMIT. Gate: fmt/clippy clean, 1247 passed / 0 failed.

**PG is NOT the same defect, and this matters for the fix.** The live probe showed the PostgreSQL
front end COMPUTES a read-modify-write correctly (I-A PASS, v=105) - its value is right. What it loses
is the APPLICATION: the committed cell comes back NULL for the survivor of a concurrent pair. So PG
does not have CQL's gap (never computing the read) at all; it has the write-set materialization seam
(`fix(accord): never materialize the PostgreSQL transaction write-set`, b839ef04).

Consequences:
1. Porting the CQL rewrite to PG would fix nothing: PG already resolves the read.
2. The PG half is unverifiable locally by construction. It was NOT reproduced in-process (3/3 green) nor
   at three real processes (1/1 green); the only judge is the live probe (txn-iso2-probe.sh V1).
3. So "green on both front ends" for PG means: rebuild with the write-set fix, deploy, and re-run the
   live isolation probe. Anything short of that is a claim without evidence.

### The PG write-set path, mapped from code (2026-10-11)

Reconnaissance done while a rebuild/deploy/probe round runs, because the probe MEASURES and this is
what a fix will need. All four are read off the code, not inferred from the symptom.

1. **The PostgreSQL write-set is an ENVELOPE**, not a plain mutation:
   `encode_mutation(storage_mutation: &[u8], mvcc_metadata: &[u8])` at
   `ferrosa-storage/src/accord/postgres_mvcc.rs:10` frames magic + length + mvcc_metadata + storage
   mutation. So a PG transaction travels as two payloads, and the second one (the row images) is the
   part a read-modify-write depends on.

2. **An EMPTY metadata SILENTLY skips the whole PostgreSQL apply** —
   `ferrosa-cluster/src/accord/apply.rs:1300`: `if !postgres_mvcc_metadata.is_empty() { ... }`. No
   error, no log, no counter. A transaction that wrote PG rows but whose metadata never got built
   leaves no PG-visible history at all, and the front end reads through MVCC, so it sees nothing
   where it should see the write. This is a fail-SILENT on the one path where a committed write can
   vanish, in a repo whose stated rule is fail-loud.

3. **The metadata is built on the PG side** (ferrosa-postgres) and applied by
   `MvccManager::prepare_postgres_apply` (`ferrosa-postgres/src/mvcc.rs:636`), which
   `decode_row_changes(metadata)?` and inserts history at the transaction's `t`.

4. **This is the same CLASS as the CQL bug just fixed** (3a49ac1d): a value that depends on a read
   inside the transaction fails to reach the write that must carry it. The CQL front end at least
   refused the form BY NAME; the PG front end does it silently.

The rule this touches, already standing: hot-path serialization must be capnp, and JSON row images
riding inside the Accord write-set are a bug. Point 1 is exactly where those row images live.

**Next, when the probe speaks:** read the metadata builder on the PG side and assert that a
read-modify-write's computed value is IN the metadata before the commit is proposed. If it is not,
the fix is the PG mirror of 3a49ac1d. If it is, the loss is downstream of it and the probe's V1 line
says where.

### RT-2 ROOT CAUSE — the conflict predicate is per TABLE, not per KEY (2026-10-11)

`validate_snapshot` (`ferrosa-postgres/src/mvcc.rs:725`) decides a serialization conflict from the
TABLE's write epoch alone:

    if read_tables.iter().any(|table| {
        state.table_epochs.get(table).is_some_and(|epoch| *epoch > snapshot.read_ts)
    }) { return Err(MvccCommitError::SerializationFailure); }

`read_tables` is a `HashSet<String>` of table NAMES. So ANY concurrent write to a table this
transaction read from refuses it, however unrelated the rows are. PostgreSQL raises a serialization
failure on read-set / write-set OVERLAP, never on table identity — which is why every concurrent pair
on one table loses a side.

**A deterministic IN-PROCESS reproducer exists** (RT-3 never had one):
`transactions_on_disjoint_keys_do_not_conflict` — two serializable snapshots, A writes `d1`, B writes
`d2`, B is refused `40001` in 0.05 s with no cluster. Verbatim:

    [ErrorResponse { fields: [(83,"ERROR"), (67,"40001"), (77,"could not serialize PostgreSQL transaction")] }]

Its negative control runs FIRST: two transactions writing the SAME key must have exactly one refused.
That case PASSES today, so the control is non-vacuous and a "fix" that simply never reports a conflict
cannot go green.

**The fix is feature-shaped. This half is NOT done:** the read-set must record the ROWS (or key
ranges) a transaction read, not only the tables, and the conflict test must intersect that set with
the concurrent commit's write set. The signature IS the defect — `read_tables: &HashSet<String>` cannot
express a disjoint-key pair, so no honest unit red exists against it, which is why the red above is
taken through the commit path instead.

Red held out as `rt2-disjoint-keys.patch` in the scratch dir: a red inside a module cannot park its
file, so the patch is the breadcrumb. It lands with its green.

### CORRECTION: RT-3 IS reproducing — the old probe mislabelled it (2026-10-11)

I previously reported "RT-3 did not reproduce on the settled build". **That was wrong, and the cause
was the instrument.** The old probe printed that same outcome as "REPRODUCED" in a branch whose logic
was inverted, so the run read as a false conflict rather than as a lost value. With the corrected
judge (which decides each side by whether it was refused), the same shape reports precisely:

    V1 read-modify-write:  A refused 40001; B SUCCEEDED and its value is EMPTY
        after: aid=1 v='777'   aid=2 v='<empty>'
        FAIL V1: aid=2 got '<empty>' want '202' (refused A=1 B=0)
        NOTE: one side refused 40001 on DISJOINT keys - RT-2 false conflict
    V2 blind write:        A refused 40001; B SUCCEEDED and its value is KEPT
        after: aid=1 v='777'   aid=2 v='222'
        PASS V2: no value lost (refused A=1 B=0)

**So RT-3 is ALIVE**: a transaction reported success and its committed cell reads back EMPTY. And the
single-variable isolation still holds — **V1 (with the read) loses the value, V2 (blind, no read of v)
keeps it** — so the loss tracks the READ inside the transaction, as originally isolated.

It is INTERMITTENT, not fixed: the preceding settled run of the same case showed `aid=2 v='200'`
(the value simply unchanged). One clean run was never a green here, and the earlier report that
treated it as one came from reading a mislabelled probe.

**Mechanism now consistent with the code**: a successful transaction whose PostgreSQL metadata never
materialised is SILENTLY skipped by the apply (`ferrosa-cluster/src/accord/apply.rs:1300`,
`if !postgres_mvcc_metadata.is_empty()`), so no PG-visible history is written and the read comes back
empty rather than with the value. That is the "never materialise the write-set" seam, and it is the
leading hypothesis — stated as a hypothesis, not as a measurement.

**Standing correction:** the corrected probe is the durable instrument and belongs in the skill's
`scripts/` (`scripts/txn-iso-probe.sh` currently carries the mislabelled V1/V2). Until that lands, any
run of the old probe's V1/V2 lines is not evidence in either direction.

### RT-2 FIX — the design, the ONE decision it needs, and the exact first step (2026-10-11)

The fix is small at its core, because the machinery already exists. `State.versions` is a per-ROW
`BTreeMap<commit_seq, Row>` (`ferrosa-postgres/src/mvcc.rs`), already pruned by `prune_versions`. So
the row-granular predicate is:

    conflict  <=>  some row in THIS transaction's read-set has a latest version sequence
                   (state.versions[key] or state.distributed_versions[key], .keys().next_back())
                   greater than snapshot.read_ts

Today's predicate compares the TABLE instead — `table_epochs: HashMap<String, u64>`, bumped on every
apply — which is exactly why disjoint rows collide.

**The one decision it needs: what a SCAN's read-set is.** A point read knows its key; a scan does not
enumerate its rows. Dropping the table-level check outright would silently LOSE phantom protection (a
row inserted into a scanned range must still conflict). PostgreSQL answers this with predicate/SIREAD
locks. The real options are:

  (a) row-level for point reads, table-level RETAINED for scans — fixes this red, keeps the false
      conflict for scan readers (an honest increment with a named residual);
  (b) SIREAD-style range predicates — correct, and feature-sized;
  (c) row-level only — fast, and silently drops phantom protection.

This is an unmade DESIGN decision, not a fixture gap, so it is not something to guess at.

**First step for whoever lands it** (six call sites, all listed so no recon is needed): change the
read-set to carry rows — `txn_read_tables: HashSet<String>` at `extended.rs:94`, filled at
`server.rs:715-722`, taken at `server.rs:883`, consumed at `query.rs:1789/1796` and
`mvcc.rs:460/463/524/528` — to also carry the `RowKey` of each point read; apply the predicate above;
and keep the table check for scans only, per whichever option is chosen.

The red `transactions_on_disjoint_keys_do_not_conflict` is parked as `rt2-disjoint-keys.patch` and
turns GREEN under (a) or (b). Under (a) it is green while scan-readers still collide — say so when it
lands.

### RT-2 fix: the read site, and why this is deeper than "the predicate is small" (2026-10-11)

Recon for option (a). The read-set is filled in **`track_select_reads`** (`ferrosa-postgres/src/server.rs:709`),
which is called with the whole `ferrosa_sql::SelectStmt` — so the WHERE predicate IS in scope, and the
function currently records only the table name from `select.from` / `select.join`.

What that means for sizing: option (a) needs the **RowKey of a point read**, and obtaining it means
deriving the storage key from the predicate — a real piece of work, not a one-liner alongside the
predicate change. The engine already resolves exactly that key in order to execute the read, so the
fix should REUSE that resolution (record the key it resolved) rather than re-deriving it from the
`SelectStmt`; deriving it a second time would be a second implementation of key resolution that can
disagree with the first.

So the honest size is: predicate change (~15 lines) + read-set type change across the six call sites
already listed + reuse of the engine's key resolution at the read site + the gate. That is not a
turn-ending increment, and I did not start it — the red stays parked and the tree stays clean.

**Status: OWED, blocked on the one design decision (scan read-sets) plus this key-resolution reuse.** The
decision went to the user and timed out unanswered; the recommendation stands at (a), the conservative
increment, which keeps phantom protection for scans and names its residual.

### DECISION TAKEN: option (a) — implementation spec, ready to land (2026-10-11)

The requestor chose the recommendation: **(a) row-level for point reads; the table check is RETAINED
for scans.** The blocking decision is resolved. Residual to state when it lands: a transaction that
read by SCAN still collides on table identity — only (b)/SIREAD removes that.

The predicate, checked against BOTH cases the red asserts, so no further design is needed:

    conflict  <=>  some key in (rows this txn READ by key) has
                   latest_seq(key) > snapshot.read_ts

    latest_seq(key) = max over state.versions[key]            (HashMap<RowKey, BTreeMap<u64, Option<Row>>>,
                                                              keyed by commit_seq, already pruned)
                             and state.distributed_versions[key]  (BTreeMap<Timestamp, ...>, the
                                                              Accord-applied history — the live case
                                                              runs through THIS map)

Why it satisfies both. DISJOINT keys: neither transaction's read row is touched by the other, so both
commit. SAME key: A commits first, which inserts a version for that row, so B's read-set then sees a
sequence greater than B's snapshot and exactly ONE is refused. That second property comes from the
ASYMMETRY of comparing against COMMITTED versions rather than against the other transaction's reads —
which is why no symmetric read-read machinery is needed.

Remaining recon is two lookups and nothing else: `struct RowKey` is NOT `pub struct` (a
visibility-qualified grep finds it nowhere `pub`), so its path and key type come from its definition;
and `MvccSnapshot.read_ts`'s type — already used against a u64 commit_seq by the current comparison, so
it is sequence-comparable as it stands.

Then: carry the point-read RowKey in the read-set (`extended.rs:94` + `server.rs:709/715/883` +
`query.rs:1789` + `mvcc.rs:460/463/524/528`), apply the predicate above, and keep the table check for
scans. The parked red `transactions_on_disjoint_keys_do_not_conflict` goes GREEN, control included.

### RT-2 spec COMPLETE — both last unknowns resolved (2026-10-11)

    struct RowKey { .. }   ferrosa-postgres/src/mvcc.rs:233   — private to mvcc.rs, which is why a
                                                                `pub struct RowKey` grep found nothing.
                                                                Becomes `pub(crate)` so the read set in
                                                                extended.rs/server.rs can hold one.
    read_ts: u64           mvcc.rs:249 (and 283). Decisive: mvcc.rs:430 is `let read_ts =
                           state.commit_seq;` — so read_ts IS a commit_seq, and comparing it against
                           the BTreeMap<u64> keys of `versions` is sequence-correct by construction.
    mvcc.rs:511            already reads history as `versions.range(..=snapshot.read_ts).next_back()`
                           — the exact idiom the new row predicate uses, inverted: a row conflicts
                           when a version exists with key > read_ts.

So the predicate is now fully determined, and the ONE piece still requiring a judgement call is the
point-read key: `track_select_reads` (server.rs:709) is handed the SelectStmt, so the key COULD be
derived from its WHERE clause — but that would be a second implementation of key resolution, and the
engine already resolves that exact key to execute the read. The fix must record the key the read
resolved, not re-derive it. Locating that resolution is the first step of the next pass; everything
after it is mechanical (the six call sites are listed, the predicate is written above).

NOT landed: the code. This pass resolved the design and both remaining unknowns; the change itself did
not land, and the parked red stays parked.

### RT-2 (a) — COMPLETE implementation spec; every shape resolved (2026-10-11)

Nothing below requires further recon.

1. `mvcc.rs:233`  `struct RowKey { table: String, key: Vec<Value> }` — private. Make the struct AND both
   fields `pub(crate)`. `key` is `Vec<Value>` — COLUMN VALUES, not storage bytes — so a point read's
   key is directly expressible from the predicate, with no storage-key encoding involved.

2. `mvcc.rs:725`  `validate_snapshot(state, snapshot, read_tables)` gains a 4th param
   `read_rows: &HashSet<RowKey>`, and the row check runs BESIDE the existing table check:

       if read_rows.iter().any(|row| {
           state.versions.get(row).or_else(|| state.distributed_versions.get(row))
               .is_some_and(|versions| {
                   versions.range((Excluded(snapshot.read_ts), Unbounded)).next().is_some()
               })
       }) { return Err(MvccCommitError::SerializationFailure); }

   `read_ts` is `u64` and `mvcc.rs:430` sets it from `state.commit_seq`, so comparing it against the
   version maps' keys is sequence-correct by construction. `mvcc.rs:511` already uses this range idiom.

3. `mvcc.rs:460/463` and `:524/528`  the two callers pass the new set through;
   `MvccManager::commit` (`mvcc.rs:521`) gains `read_rows` too.

4. `extended.rs:94`  add `txn_read_rows: HashSet<RowKey>` beside `txn_read_tables`, with
   `txn_read_rows_mut()` / `take_txn_read_rows()` mirroring `:411`/`:415`.

5. `server.rs:709` `track_select_reads` — after the table insert, add the point-read branch:

       if let Some(ferrosa_sql::Expr::Compare { left: Operand::Column(col), op: CmpOp::Eq,
                                                  value: Term::Literal(v) }) = &select.filter
           && <col.name is select.from.table's partition key column>
       { session.txn_read_rows_mut().insert(RowKey { table: "schema.table", key: vec![v.clone()] });
         return; }   // a point read is row-granular: do NOT also record the table

   `CmpOp` is at `ferrosa-sql/src/exec.rs:626` (NOT ast.rs). The PK column comes from table metadata the
   front end already holds — `server.rs:1952` shows `partition_key: vec!["k".to_string()]`.

6. `server.rs:883` `take_txn_read_rows()` beside `take_txn_read_tables()`;
   `query.rs:1789/1796` pass it into the mvcc commit.

Then re-apply `rt2-disjoint-keys.patch` and the red goes GREEN with its control.

RESIDUAL, to state when it lands: a transaction that read by SCAN keeps the table-level check, so scan
readers still collide on table identity. Only (b)/SIREAD removes that.

### RT-2 (a): predicate + plumbing WRITTEN, saved as a patch; not landed (2026-10-11)

Written and nearly compiling, saved as `rt2-predicate-inprogress.patch` (121 lines) rather than
committed, because the piece that makes the red GREEN was not reached:

- `RowKey` -> `pub(crate)` (struct + both fields).
- `validate_snapshot` gains `read_rows: &HashSet<RowKey>` and checks ROW-granular FIRST: local
  (`state.versions`, `BTreeMap<u64>` against `snapshot.read_ts`) and distributed
  (`state.distributed_versions`, `BTreeMap<Timestamp>` against `snapshot.cluster_ts` — `ActiveSnapshot`
  carries it as `Option<Timestamp>`). The TABLE-granular check is RETAINED for scans, per option (a).
- `validate_commit`, `MvccManager::commit` and `query::commit_mutations` thread `read_rows` through.

The compiler listed the remaining callers as SEVEN one-line updates: `copy_stdin.rs:489`;
`query.rs:1577`, `:1663`, `:4556`; `server.rs:893`, `:983`, `:1091`.

NOT reached — step 5, the piece that turns the red green: the point-read branch in
`track_select_reads` (`server.rs:709`), which needs the PK column of the read table. The metadata type
is `ferrosa-schema/src/metadata/table.rs:23` (`partition_key: Vec<String>`) and `jsonb_rules.rs:234`
already performs exactly the needed check (`table.partition_key.contains(&col.name)`); what is NOT yet
resolved is the accessor by which the front end reaches that table metadata from `ctx.schema`.

Tree: GREEN and verified (`cargo build -p ferrosa-postgres` -> `Finished`) after the revert, which is
why the partial work was parked as a patch rather than committed. Nothing semi-landed.

### RT-2 (a): the change is SMALLER than "seven sites" — three, not seven (2026-10-11)

Re-applied the parked predicate patch and finished the call-site recon. Two findings shrink the job:

1. **`TableMetadata` is `ferrosa_schema::TableMetadata`** (imported at the crate root —
   `ferrosa-postgres/src/catalog.rs:34`), carrying `partition_key: Vec<String>`.

2. **Five of the seven "call sites" need NO change at all.** `query.rs:1577` and `:1663` pass
   `&std::collections::HashSet::new()` for the read set TODAY, because they are the **autocommit
   path** (`None => match mvcc` — not inside a transaction). A transaction-less write has no read-set
   by definition, so an empty rows set is CORRECT there, not a placeholder. `copy_stdin.rs:489` and
   `query.rs:4556` are the same shape. Only the three sites that run the TRANSACTION path need the
   real rows: **`server.rs:893` and `:983` (which call `validate_commit`) and `server.rs:1091` (which
   calls `commit_mutations`)** — and those are exactly the sites the red exercises.

So the remaining work is: the `ctx.schema` -> `TableMetadata` accessor (still unlocated; the
`Schema`/`SchemaSnapshot` types are both imported at catalog.rs:34, so it is a snapshot lookup), the
`extended.rs` read-rows field plus its two accessors, the point-read branch in `track_select_reads`,
and the three `server.rs` sites. `read_rows` defaults to empty everywhere else, which is correct.

Tree restored and verified green after the revert (`cargo build -p ferrosa-postgres` -> Finished);
behaviour unchanged.

### RT-2 (a): the shape with NO signature ripple — record the read row on the MANAGER (2026-10-11)

Every attempt so far was blocked by the same thing: threading `read_rows` as a PARAMETER ripples through
~10 coupled call sites. It does not need a parameter. The manager already owns the per-snapshot
lifecycle (`active_snapshots: HashMap<u64, ActiveSnapshot>`), so the read set belongs THERE:

    State:            read_rows: HashMap<u64 /*snapshot id*/, HashSet<RowKey>>

    new method:       MvccManager::record_point_read(&self, snapshot: &MvccSnapshot, row: RowKey)
                      -> inserts into state.read_rows[snapshot.id] after confirming the snapshot is
                         still active

    validate_snapshot: reads state.read_rows.get(&snapshot.id) — SIGNATURE UNCHANGED

    the read site:    track_select_reads (server.rs:709) calls ctx.mvcc.record_point_read(snapshot, row)

Consequences:
- No ripple: 1 file (`mvcc.rs`) + the new method + the point-read branch + 1 call site. The three
  `server.rs` sites and the two `validate_commit`/`commit` signatures are NOT touched at all.
- The set is keyed by snapshot id, so it is bounded by ACTIVE transactions and dropped with the
  snapshot — which satisfies the standing rule that nothing unbounded is added on this path.

The parked `rt2-predicate-inprogress.patch` already contains the predicate itself; its parameter-based
plumbing should be REPLACED by this lookup rather than extended.

### RT-2 (a) LANDED — row-granular point reads, table-granular scans (2026-10-11)

The parked red `transactions_on_disjoint_keys_do_not_conflict` is GREEN with its negative control,
committed together with the fix. Verbatim before (red, 0.05 s, no cluster):

    B writes d2 only - a DISJOINT key - so refusing it is the false conflict:
    [ErrorResponse { fields: [(83,"ERROR"), (67,"40001"), (77,"could not serialize PostgreSQL transaction")] }]

After: `ok` — both transactions COMMIT and both written VALUES read back.

**Read side (the design as given).** `State` gains `read_rows: HashMap<u64 /*snapshot id*/,
HashSet<RowKey>>` (`mvcc.rs`), pruned in `prune_versions` — which every site that removes a snapshot
(`SnapshotLease::drop`, `expire_snapshots_before`) already calls, so nothing leaks.
`MvccManager::record_point_read(&snapshot, RowKey)` inserts under the live snapshot;
`validate_snapshot` reads it ROW-granular FIRST (local `versions` keyed by `commit_seq` against
`read_ts`; distributed `distributed_versions` against `cluster_ts` when present) then falls through to
the EXISTING table check, signature unchanged. `RowKey` and both fields are `pub(crate)`.
`track_select_reads` (`server.rs`) records a `WHERE <partition-key> = <literal>` read as its ROW and
does NOT record the table; the PK column is read from `ctx.schema.snapshot().tables`
(`TableMetadata::partition_key`).

**Write side — the design's "three `server.rs` sites are NOT touched" was wrong, and the red proved
it.** `commit_txn` folded every WRITTEN table into the set handed to `validate_commit`, so a
transaction was still refused on the table epoch of a table it merely WROTE (the instrumentation
showed the false refusal came from `tables={"public.kv"}` / `epochs={"public.kv": 6}`, not the read
set). `commit_txn` now validates the SCANNED tables only; a write is judged by the ROWS it wrote,
recorded in `commit_mutations` (`query.rs`) through the same `record_point_read` — so same-key
write-write still conflicts while disjoint writes do not.

**Two pre-statement liveness probes had to stop using `validate_commit`.** `expired_transaction_error`
(`server.rs`) and `load_catalog_with_mvcc` (`query.rs`) called `validate_commit(snapshot, &empty)` as
an "is the snapshot usable" check. With an empty read set that could only ever return
`SnapshotExpired`; once `validate_snapshot` also checks read rows, a live snapshot with a changed read
row was mis-reported EXPIRED and a transaction could no longer read its own BEGIN snapshot. Both now
call the new `MvccManager::snapshot_is_active`: only EXPIRY aborts a later statement, and a read-set
conflict stays a COMMIT-time decision.

RESIDUAL (unchanged): a transaction that read by SCAN keeps the table-level check, so scan readers
still collide on table identity — only (b)/SIREAD removes that.

Gates: `cargo fmt --all` clean; `cargo clippy -p ferrosa-postgres --all-targets -- -D warnings`
clean; `cargo test -p ferrosa-postgres` green — lib 361 passed / 0 failed (360 + the new red), every
integration target green.

### LAYER 2 DIAGNOSIS: the row check is INERT on the live path (cluster_ts) — 2026-10-12

Settled re-run (V0-a PASS, so the cluster was healthy): V1 and V2 both still report "one side refused
40001 on DISJOINT keys". The fix does not reach the live path. Cause, read off the code:

1. The live commit goes through Accord — `server.rs:1026`, `if let Some(committer) = ctx.accord.committer()`
   — so its writes are recorded by `record_applied_accord_commit` into `state.distributed_versions`,
   NOT into `state.versions`.
2. The new row check's distributed arm runs only `if let Some(cluster_ts) = snapshot.cluster_ts`.
3. The live snapshot comes from `ctx.mvcc.snapshot()` (`server.rs:926`), which carries NO cluster
   timestamp; `snapshot_with_cluster_ts(..)` is used only by tests.

=> Live, the row check examines `versions` (empty — nothing writes it on the Accord path) and SKIPS
   `distributed_versions` (no cluster_ts). It can NEVER detect a row conflict. So the TABLE check still
   decides and the false conflict persists, exactly as observed.

Corroboration from the node logs: no `40001`, no abandon, no "serialize" line. The Accord abandon path
DOES log ("abandoning the transaction (NOT committed; safe to retry)"), so its absence rules
`TxnAbandoned` out; the MVCC path maps `SerializationFailure` to the wire silently, which is why the
emitter leaves no trace.

**THE FIX (not done):** make the row check see the distributed map on the Accord path — either give the
commit-path snapshot the transaction's Accord timestamp, or compare `distributed_versions` against the
timestamp the commit already holds (`prepare_postgres_apply` receives `apply_t`).

**Residual, unchanged:** scans keep the table check, and this machinery still REFUSES where PostgreSQL
would WAIT — at the default isolation level that is the deeper gap.

Also confirmed on a settled cluster: V1's B value EMPTY, V2's B value KEPT (222) — RT-3's
read-discriminator still holds.

### ROOT CAUSE CORRECTED — it was a CLUSTER-GLOBAL LOCK, not the table check (2026-10-12)

**My layer-2 diagnosis was WRONG, and the evidence that refutes it is simple.** For these transactions
`read_tables` AND `read_rows` are BOTH EMPTY — a plain `BEGIN` stores isolation `None`, and the
non-serializable path records nothing — so `mvcc::validate_snapshot` always returned `Ok`. It was never
the emitter. (The row check being inert on the Accord path is still true; it was just irrelevant here.)

**The real cause:** every PostgreSQL commit bumps a single cluster-global
`POSTGRES_TRANSACTION_MARKER_KEY` high-water mark, and the Accord pre-accept snapshot-stale check
refused ANY transaction whose snapshot predated that mark — regardless of which rows it touched. That is
a CLUSTER-GLOBAL LOCK: every concurrent pair conflicts on a shared protocol key, which is the
"4 clients = 0 tps".

**The fix: `77cdf369`** — "judge PostgreSQL snapshot staleness on the transaction's own keys, not the
global marker" (`ferrosa-cluster/src/accord/state_machine.rs`, +106/-13): staleness is now judged
against the keys the transaction itself WROTE, excluding the two shared protocol keys (the data-commit
marker and the BEGIN/COMMIT barrier). Two new tests plus the reworked stale test; 1463 ferrosa-cluster
lib tests pass; fmt/clippy clean.

**Verified live on pgwire-77cdf369** — probe output verbatim:

    V1:  A: (no error)  B: (no error)   after: aid=1 v='778'  aid=2 v='<empty>'
         FAIL V1: aid=2 got '<empty>' want '202'   (refused A=0 B=0)
    V2:  A: (no error)  B: (no error)   after: aid=1 v='111'  aid=2 v='222'
         PASS V2: no value lost                    (refused A=0 B=0)

`refused A=0 B=0` on BOTH pairs: no 40001 anywhere. A cold cluster can make failures APPEAR; it cannot
make a refusal DISAPPEAR, so the global lock is gone. Caveat recorded honestly: V0-a FAILED with the
post-deploy cold-read timeout, so this run is inadmissible for a formal pass and a CLEAN SETTLED
RE-RUN IS OWED before calling RT-2 done.

**RT-3 is now cleanly separated from RT-2**: V1's B value reads back EMPTY (a successful transaction
losing its value) while V2's blind B KEEPS its value (222) — the read-discriminator still holds, on the
same run. RT-3 is the remaining transaction defect.

**Also found on the CQL side (needs recording/follow-up):**
- `UPDATE ... SET v = v + N` inside a CQL transaction is refused with 0x2200 ("unsupported collection
  assignment: Add of a non-collection value ... inside a transaction"). This is the KNOWN gap: the CQL
  fix resolves a BOUND operand (`v = cur + N`); a self-increment names the column, so it fails the
  binding lookup. NOT yet recorded anywhere else.
- The LIVE node rejected the documented LET block (`unexpected token Ident("LET") at position 19`)
  while this tree's parser accepts it in-process. The image was mid-roll at that moment, so this is
  UNRESOLVED and needs one clean re-run on a settled 77cdf369.

## Notes

- The statement-level red is legitimate for a new syntax: the operation is refused today, and the
  assertion is on the user-visible outcome (the value read back), so it survives the implementation.
  A red that fails on a missing internal symbol would not be.
- Do not assert which peer wins a race. That is scheduling; the invariant has to hold either way.
