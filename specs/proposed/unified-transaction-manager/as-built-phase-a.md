---
title: Unified Transaction Manager — Phase A As-Built
status: implemented
component: transaction-manager
executive_summary: >
  As-built description of Phase A: a connection-independent, txn-id-keyed CQL
  transaction manager that replaces the fragile per-connection transaction buffer.
  A server-wide TransactionRegistry (keyed by an opaque UUID) holds each open
  transaction's staged write-set, owner, and deadline; the id rides the CQL surface
  (BEGIN returns it; `... IN TRANSACTION <id>`; `COMMIT|ROLLBACK TRANSACTION <id>`)
  so the statements of one transaction need not share a TCP connection. Commit routes
  the whole write-set through the existing Accord committer (strict-serializable). This
  deletes the connection-affinity desync that produced the ~24% :info in the Elle
  list-append cert. MVCC, multi-node forwarding, and a Postgres front-end are later
  phases; Phase A stages the write-set in RAM and pins the coordinator (single node).
last_revised: 2026-07-21
---

# Unified Transaction Manager — Phase A As-Built

## 1. The problem it solves

Before Phase A, CQL transaction state was a **per-TCP-connection**
`Arc<Mutex<CqlTransaction>>`. `BEGIN`/`UPDATE`/`COMMIT` were three separate
statements whose correctness depended on all three landing on the same connection
*and* being processed strictly serially. Neither holds in practice: the scylla
driver pools connections, and the server processes pipelined requests on concurrent
spawned tasks. So `COMMIT` could reach a connection with no open transaction
("COMMIT outside of a transaction") or a second `BEGIN` could arrive mid-transaction
("nested transactions"). In the Elle list-append certification this surfaced as
**~24% `:info`** (indeterminate) operations — not a database correctness bug, a
**client↔server affinity** bug in the transaction plumbing.

## 2. The core idea

Move transaction state off the connection and into a **server-wide registry keyed
by an opaque transaction id**, and put that id on the CQL wire so the client threads
it through every statement of the transaction. Now it does not matter which
connection (or which concurrent task) each statement lands on — they all address the
same registry entry by id.

```
Client                         Node (one TransactionRegistry, shared by all connections)
  BEGIN TRANSACTION      ──►    mint id, open entry{owner, deadline, buffer}, return id row
  UPDATE … IN TRANSACTION <id> ─► look up <id>, authorize owner, stage the write
  COMMIT TRANSACTION <id> ──►   take entry out, drive Accord commit of the buffered set
```

## 3. Components (all in `ferrosa-cql`)

**`CqlTxnId`** (`txn_registry.rs`) — an opaque 128-bit id, a v4 UUID rendered as
canonical text. Unguessable (defends against cross-client hijack) with negligible
collision odds; parses from a bare UUID token or a quoted string on the wire.

**`TransactionEntry`** — one open transaction's server-side state: the staged
write-set (the existing `CqlTransaction` buffer, reused unchanged), the
**authenticated owner** (principal/role), an absolute **deadline**, and the timeout
budget.

**`TransactionRegistry`** — `HashMap<CqlTxnId, TransactionEntry>` plus bounds. Lives
on `SharedState`, so it is **per-node and shared across every connection** (that
sharing is the whole point). Lifecycle methods:
- `begin(owner, now, timeout?) -> CqlTxnId` — capacity-checked (fails loud at the
  10 000-entry cap), timeout resolved (override bounded by a 600 s hard max, never
  silently clamped), mints an id, inserts an open entry.
- `stage(id, owner, now, write)` — authorize (own it, not expired), then buffer the
  write. A build/stage failure **poisons** the transaction so a later commit refuses
  a partial write-set.
- `take_for_commit(id, owner, now) -> CqlTransaction` — authorize, then **remove**
  the entry and hand the owned buffer to the caller.
- `abort(id, owner)` — `ROLLBACK`; drop the entry.
- `ensure_active(id, owner, now)` — validate for a `SELECT … IN TRANSACTION` without
  staging (Phase A reads committed state; MVCC isolation is Phase D).
- `reap_expired(now) -> Vec<CqlTxnId>` — evict every past-deadline entry.

**Reaper** (`spawn_transaction_reaper`) — a background task that calls
`reap_expired(Instant::now())` every second, so an abandoned transaction is aborted
within ~one interval of its deadline **without any client statement**. Default open
timeout is **10 s** (overridable per transaction via `USING TIMEOUT`, up to the hard
max). This bounds RAM and, in later phases, bounds MVCC version retention.

## 4. The CQL surface (wire-compatible)

Every extension is ordinary CQL text in a standard QUERY frame, so **stock Cassandra
drivers work unmodified**:

| Action | CQL | Server behavior |
|---|---|---|
| Begin | `BEGIN TRANSACTION [USING TIMEOUT <ms>]` | mint id; return a one-row Rows RESULT `[[txn_id]]` |
| Write | `UPDATE/INSERT/DELETE … IN TRANSACTION <id>` | stage the write under `<id>` |
| Read | `SELECT … IN TRANSACTION <id>` | validate `<id>`; read committed state (Phase A) |
| Commit | `COMMIT TRANSACTION <id>` | Accord-commit the buffered write-set |
| Rollback | `ROLLBACK TRANSACTION <id>` | discard the entry |

`TIMEOUT` is a *soft* keyword (a bare identifier), so it does not reserve the word
`timeout` for table/column names. The `IN TRANSACTION` suffix is disambiguated from a
`WHERE … IN (…)` list because it only binds at the end of a fully-parsed DML
statement. `BEGIN`'s result is a normal Rows set — a legacy client that ignores it
still works via the shim (below).

## 5. Commit path & strict serializability

Phase A **reuses the existing Accord committer unchanged**. On `COMMIT`, the router
`take_for_commit`s the owned buffer out of the registry, releases the registry lock,
and drives `AccordTransactionCommitter::commit(write_set)`. Accord provides the
strict-serializable, all-or-nothing multi-key commit; the transaction manager's job
is only to *assemble the right write-set under the right id and hand it over intact*.

Strict serializability therefore rests on two things working together:
1. **This layer** — the write-set is assembled correctly and atomically per
   transaction, connection-independently (no torn/duplicated/dropped statements).
2. **The Accord execution-timestamp ordering fixes** (branch `feat/crdt-collections-dwrite`
   / PR #283): cell-path rebind to the execution ts, the shared-HLC witness, and the
   per-key execution-ts high-water-mark — which fixed the *write-side ordering* that
   Elle's `list-append` checks. Phase A removes the *client-affinity* noise that was
   masking whether that ordering actually holds under load.

## 6. Concurrency & lock discipline

The registry is a synchronous `parking_lot::Mutex`. Every registry operation is
synchronous **except** commit, and commit is deliberately structured so the async
Accord round-trip runs **without the registry lock held**: `take_for_commit` removes
the entry and returns the owned buffer, the lock drops, then the `.await` happens. If
the lock were held across the await, every transaction on the node would serialize on
it. Per-connection there is a tiny `Option<CqlTxnId>` "shim" slot (below); the
registry itself carries no per-connection coupling.

## 7. Backward compatibility (the shim)

A bare `BEGIN` still works: it mints an id, returns it, **and** binds it to the
connection's shim slot, so a later bare `COMMIT`/`ROLLBACK` (or bare in-transaction
DML) resolves to it. This keeps legacy single-connection-serial clients working and
logs a deprecation hint. The shim is per-connection and does **not** provide the
connection-independence guarantee — explicit ids do. It is slated for removal in
Phase C once clients migrate.

## 8. Safety properties (fail-loud, bounded)

- **Auth scope** — an entry records its owner; a statement on a non-owned id fails
  loud (`Unauthorized`). One client cannot touch another's transaction.
- **Bounded** — the registry count is hard-capped (fails loud at capacity); ids are
  minted with bounded retries; no unbounded server-side growth.
- **Time-bounded** — every open transaction has a deadline, enforced both lazily (on
  the next stage/commit) and actively (the reaper). A timed-out transaction is
  aborted and persists nothing.
- **No partial commits** — a failed staged statement poisons the transaction; the
  next commit refuses a partial write-set. A failed commit surfaces as an error and
  is never acked as success.
- **Testable time** — the deadline logic takes `now: Instant` as a parameter, so it
  is unit-tested deterministically with no wall-clock dependence.

## 9. Explicit non-goals for Phase A (deferred, on the board)

- **MVCC** (versioned rows, write intents, snapshot reads, read-your-writes) — Phase
  D (`t_c8c1b043`). Phase A stages in RAM and reads committed state.
- **NVMe temp-table staging** — Phase D. Phase A's write-set is RAM-bounded.
- **Multi-node txn-id forwarding** — Phase B (`t_d999f32b`). Phase A pins the
  coordinator (single node), which is exactly the Elle cert topology.
- **Postgres/SQL front-end, durable/recoverable txn state, isolation levels,
  savepoints** — Phases E/F (`t_90b7bee3`, `t_3251fe75`).

## 10. Verification status — CERTIFIED

- Unit + integration: `ferrosa-cql` 1035 lib tests + integration + doc-tests green;
  `ferrosa-cluster` green; `ferrosa` binary builds; `clippy --all-targets` clean.
- **Live acceptance gate — PASSED.** Fresh-build RF=3 Elle `list-append`
  certification via `deploy/fly-accord-elle/certify.sh`: **`valid? true`,
  `anomaly-types: nil`, checker exit 0, `1600 :ok / 0 :fail / 0 :info`.** The
  `:info` connection-affinity desync went from ~24% → **0**. Every append committed
  and the history is strictly serializable — serializable *and* usable.
- **Bug found + fixed during certification:** the first fresh-build run was
  `valid? true` but rejected ~84% of BEGINs as "nested transactions" (all clean
  `:fail`). The nested-BEGIN guard was a vestige of the per-connection model — in
  the txn-id model a client legitimately holds several independent transactions at
  once, so a fresh BEGIN never rejects. `begin_transaction` now mints a fresh id and
  rebinds the shim last-wins (no guard); regression test
  `router::tests::consecutive_begins_on_one_connection_never_report_nested`. Re-cert
  after the fix: `0 :fail`.
- **Tooling fix:** `certify.sh` labeled the fly image by `git HEAD` only, so
  uncommitted code silently certified a stale cached image — now a dirty-aware
  deterministic content-hash label + a build-log `Compiling ferrosa` check.
