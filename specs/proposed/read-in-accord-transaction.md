# Read-in-transaction: `SELECT` inside an Accord `BEGIN … COMMIT`

**Status:** Proposed. Motivated by strict-serializability validation (Elle
`list-append`), which requires transactions that both read and write and return
the reads.

## Problem

Ferrosa's Accord transactions are **write-only** today. `route_transactional`
(`ferrosa-cql/src/router.rs:1698`) buffers only `INSERT/UPDATE/DELETE`; a
`SELECT` inside a transaction falls to `_ => None`, and `COMMIT` returns
`encode_void()`. The commit seam
(`ferrosa-storage/src/accord/transaction_committer.rs:59`,
`TransactionCommitter::commit(writes)`) carries no read-set and returns no rows.

This blocks the gold-standard Elle checker (`list-append`), which needs a
transaction to read a key's value and return it. Without transactional reads we
are limited to Elle `rw-register` with a `:linearizable-keys?` **assumption**
(built + validated in `scratchpad/sr-elle`), which is weaker than a direct
list-append proof.

## What already exists (engine)

The Accord coordinator already performs a **single-key, linearizable read-at-`t`**:
`ReadPredicate::ReadRow { keyspace, table }` (`ferrosa-cluster/src/accord/wire.rs`),
the F+1 agreement in `agreed_row` (`coordinator.rs:515`), and `last_read_row`
(`coordinator.rs:453`). But it is (1) single representative-key, (2) fused to a
conditional write (LWT `IF`), and (3) consumed internally by `condition_gate` —
never surfaced through the commit seam. `transaction_keys.rs` already extracts a
read-set but is **dead code** (no consumers).

## Design

Add a read-set to the transaction; execute those reads at the transaction's
agreed commit timestamp across all read keys; return the rows from `COMMIT`.

### Seam change (additive, low blast radius)

Keep `commit(writes)` and add, on `TransactionCommitter`:

```rust
async fn commit_with_reads(
    &self,
    writes: Vec<TransactionWrite>,
    reads: Vec<TransactionRead>,   // { keyspace, table, key }
) -> Result<(CommitOutcome, CommitReads), CommitError> {
    // default: commit writes, return no rows (front-ends/committers without
    // transactional-read support are unaffected — e.g. Postgres front-end)
    Ok((self.commit(writes).await?, CommitReads::default()))
}
```

`CommitReads { rows: Vec<Option<Vec<u8>>> }` carries the agreed row bytes per
read (positional). The CQL front-end calls `commit_with_reads`; the Postgres
front-end is untouched.

## Phased TDD plan

**Slice 1 — CQL plumbing (proves the path, mock engine). Small.**
- `session.rs CqlTransaction`: add a `reads: Vec<TransactionRead>` buffer +
  `stage_read`; include in cap/poison accounting.
- `router.rs route_transactional`: add a `Statement::Select(_) if txn.is_open()`
  arm that buffers the read (new `build_transaction_read`, reusing
  `transaction_keys::extract_where_keys`).
- `router.rs commit_cql_transaction`: call `commit_with_reads`; on `Committed`,
  decode the returned row bytes (mirror `decode_agreed_row_to_map`) + project the
  SELECT columns + `result::encode_rows` instead of `encode_void`.
- `MockTransactionCommitter`: override `commit_with_reads` to echo canned rows.
- **Failing test** (model on `route_transactional_handles_begin_rollback_and_fails_commit_in_standalone`,
  `router.rs:13505`): `BEGIN; SELECT; UPDATE; COMMIT` → assert the COMMIT
  `RouteResult::Result` body decodes to a Rows body (kind `0x0002`) with the
  expected values, not void. No dispatch/wire change (`connection.rs:1605`
  already forwards `RouteResult::Result`).

**Slice 2 — engine multi-key read-at-`t` (the hard, correctness-critical part).
Large.**
- `coordinator.rs run_transaction` + read-vote loop (`:1140`): generalize the
  single representative-key read-vote to iterate the transaction's **read-set**,
  collect each key's `agreed_row` at the committed `t`, and thread
  `Vec<(key, Option<row_bytes>)>` out.
- `AccordTransactionCommitter::commit_with_reads`
  (`transaction_commit.rs:70`): drive reads via the generalized path (not
  `ReadPredicate::Always`, which skips reads); return `CommitReads`.
- Tests: model on `transaction_commit.rs` (`RecordingApplier`/`NoPeersTransport`,
  single replica) and `driver_cross_shard_e2e.rs:233`. Assert a read observes a
  write from the same transaction and reads are at the agreed `t` (snapshot
  isolation across the read-set). This is consensus-path work — do NOT rush it.

**Slice 3 — Elle `list-append` validation.**
- Clojure/Rust `list-append` workload: `BEGIN; SELECT v; UPDATE v = v+[elem];
  COMMIT` (append via read-modify-write in one txn), values unique.
- Run against the live T3 cluster under the real `dc-partition+dc-slow` nemesis
  (the container-mode partition fix from PR #274).
- Elle `list-append/check {:consistency-models [:strict-serializable]}` must
  return `:valid? true` (no G0/G1a/G1b/G1c/G2/G-single). No inference
  assumptions. Do not claim strict-serializable until this passes.

## Notes / risks

- Multi-`SELECT` transactions: CQL returns one result body per response — support
  one SELECT-in-txn first (or the last), decide multi-SELECT semantics later.
- Slice 2 changes core Accord consensus code; each change needs its own failing
  test and careful review. This warrants its own PR(s) separate from the CQL
  plumbing.
- Fallback if slice 2 is deferred: rw-register + `:linearizable-keys?`
  (validated) gives a weaker-but-real signal now.
