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

## Notes

- The statement-level red is legitimate for a new syntax: the operation is refused today, and the
  assertion is on the user-visible outcome (the value read back), so it survives the implementation.
  A red that fails on a missing internal symbol would not be.
- Do not assert which peer wins a race. That is scheduling; the invariant has to hold either way.
