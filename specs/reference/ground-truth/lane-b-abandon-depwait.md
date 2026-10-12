# Ground truth: ABANDON and DEPENDENCY-WAIT machinery (read-only recon)

Repo: `/Users/bkearns/src/ferrosa-suite/.wt-int2`
Branch: `feat/read-in-transaction` @ `32fc001a`
Method: code-inferred from source at the revision above. **No source file was changed.**
Every claim carries a `file:line` citation. Contradictions are recorded as contradictions,
unresolved. All paths are relative to `ferrosa-cluster/src/` unless stated otherwise.

Primary files:
- `accord/dep_wait.rs` (924 lines) — `DepWaitGraph`
- `accord/apply.rs` (4422 lines) — `DepWaitApplier` (owns the graph)
- `accord/coordinator.rs` (7906 lines) — `AccordCoordinatorDriver`, `AccordDriverError`
- `accord/abandon_durability.rs` (328 lines) — the abandon invariant test
- `accord/handlers.rs` — read-vote / per-txn apply waits
- `accord/state_machine.rs` — the timeout constants, apply path, maintenance prune
- `ferrosa-postgres/src/server.rs`, `ferrosa-cql/src/router.rs` — client-visible mapping

---

## (1) The 10-second constant(s), the waits they govern, and the emitting branch

### 1a. There is more than one distinct "10-second" constant, and more than one wait.

| Constant / source | Value | Citation |
|---|---|---|
| `dep_wait::DEP_WAIT_TIMEOUT` | `Duration::from_secs(10)` | `accord/dep_wait.rs:25` |
| `epoch_drain::DEFAULT_TXN_TIMEOUT` | `Duration::from_secs(10)` | `accord/epoch_drain.rs:43` |
| `state_machine::DEFAULT_TXN_TIMEOUT` (re-export of the above) | same 10 s | `accord/state_machine.rs:1556-1557` |
| `state_machine::DEFAULT_BARRIER_TIMEOUT` ("the historical dependency-wait value") | `from_secs(5)` — **not** 10 s | `accord/state_machine.rs:1559-1560` |
| `Lane::Data` transport timeout | `Duration::from_secs(10)` | `ferrosa-net/src/pool.rs:707` |

So three different mechanisms default to 10 s: the graph's own timer, the operator
"apply/txn" bound, and the data-lane RPC default. The graph's `DEP_WAIT_TIMEOUT` and the
operator bound are unrelated code paths that merely happen to share the value 10 s.

### 1b. Waits governed by the operator bound (`configured_txn_timeout()`)

`configured_txn_timeout()` (`accord/state_machine.rs:1644-1646`) resolves
`FERROSA_ACCORD_TXN_TIMEOUT_SECS` (`accord/state_machine.rs:1537`) once, process-wide, via
`DEFAULT_TXN_TIMEOUT` = 10 s (`accord/state_machine.rs:1556-1557`). It has exactly two
production callers (verified by whole-workspace grep of `configured_txn_timeout`):

1. **The coordinator apply phase.** `apply_phase` passes the resolved bound into
   `apply_phase_within(..)` (`accord/coordinator.rs:2527-2538`, bound at `:2535`). Inside,
   the bound is used three ways:
   - as the **Apply fan-out per-peer RPC timeout** — `quorum_broadcast_within(.., bound)`
     for the single-key case (`:2745`) and `apply_fanout_bounded_within(.., bound)` for the
     multi-key case (`:2751`); both reach `send_with_timeout(.., Lane::Data, timeout)`
     (`:3237` in `quorum_broadcast_per_peer`; `:3177` in `apply_fanout_bounded_within`);
   - as the **coordinator's own local apply wait** — `local_apply_wait` calls
     `await_txn_applied_within(&state, txn_id, bound)` (`:2719-2726`, call at `:2722`);
   - as the **value carried in the abandon error** — `TxnAbandoned { timeout: bound }`
     (`:2790`, `:2799`).
2. **Every inbound apply's ack wait.** `await_txn_applied` defaults its bound to
   `configured_txn_timeout()` (`accord/handlers.rs:346-353`, call at `:350`). Used by
   `apply_writeset_and_ack` (`accord/handlers.rs:156-169`, call at `:159`), which returns
   `None` (withholds the `ApplyOK`) if the txn does not reach `Applied` within the bound.

### 1c. Waits governed by the graph's own `DEP_WAIT_TIMEOUT` (10 s) — **no production caller**

`DepWaitGraph::timeout` is set to `DEP_WAIT_TIMEOUT` in `new()` (`accord/dep_wait.rs:112`) and
is read only by `check_timeouts()` (`:360-369`) and `abort_timeout()` (`:374-385`). Whole-workspace
grep shows **no caller of either outside `dep_wait.rs`'s own `#[cfg(test)]` module**
(`check_timeouts`: only `dep_wait.rs:658`; `abort_timeout`: only `dep_wait.rs:666` plus an
unrelated `two_phase_ddl::abort_timeout` at `accord/two_phase_ddl.rs:243`). The module header's
claim — "If a transaction waits longer than 10 seconds for a dependency, it is aborted with a
timeout error" (`accord/dep_wait.rs:14-17`) — therefore describes a path with **no production
caller**. The graph's 10 s timer is dead code in production.

`break_cycle()` (`:175-190`, the only writer of the `aborted` set besides `abort_timeout`) is
likewise called only from `dep_wait.rs` tests (`:622`, `:716`, `:765`). So the `aborted` map is
never populated on a production path.

### 1d. The client message: CONFIRMED emitted from the apply-quorum branch, and the word "dependency-wait" is a misnomer

**Confirmed.** `TxnAbandoned` has exactly one construction site in production:
`accord/coordinator.rs:2799`, inside the `if !apply_ok { .. }` branch opened at `:2783`. The
message the client sees is `Display for AccordDriverError` (`:563-585`):

```
"abandoned: transaction exceeded the {}s dependency-wait bound and was \
 rolled back (NOT committed); safe to retry"      // coordinator.rs:577-582
```

`apply_ok` is the result of `remote_apply` (`:2772`), which is the Apply quorum decision
(`:2733-2754`). The branch that emits the message is therefore the **apply-quorum-not-reached**
branch, **not** the dep-wait graph. Two tests drive exactly this: an empty-body `ApplyOK`
(`:6044-6068`) and a mismatched-`txn_id` `ApplyOK` (`:6074-6096`) both assert
`Err(AccordDriverError::TxnAbandoned { .. })` — i.e. they reach the message via the quorum
predicate, proving the message does not require the graph timer to fire.

**The truth precisely:** the bound named in the message is `configured_txn_timeout()`
(`FERROSA_ACCORD_TXN_TIMEOUT_SECS`, default 10 s, `state_machine.rs:1537,1556-1557`), used as
the Apply fan-out RPC bound (`coordinator.rs:2745/2751/3237/3177`) and the local apply wait
(`:2722`) — **not** `dep_wait::DEP_WAIT_TIMEOUT`. The phrase "dependency-wait bound" is legacy:
`state_machine.rs:1559` labels `DEFAULT_BARRIER_TIMEOUT` as "the historical dependency-wait
value". The code itself records that this string previously named a bound that could not
expire: the test doc at `coordinator.rs:7497-7506` ("the message named a bound that never
expired") and `:2899-2902`.

### 1e. Client-visible mapping of each outcome

- `TxnAbandoned` → PostgreSQL SQLSTATE **40001** (serialization_failure), message
  `"transaction was abandoned and NOT committed: {error}"` —
  `ferrosa-postgres/src/server.rs:1202-1213` (match at `:1208`, `msg.contains("abandoned:")`).
  The `abandoned:` prefix is load-bearing and asserted at `coordinator.rs:571-576, 3419-3432`.
  This is exactly the string in the reported failure.
- CQL front end → `CqlError::ServerError("Accord transaction abandoned after the
  dependency-wait bound — rolled back and NOT applied; safe to retry")` —
  `ferrosa-cql/src/router.rs:3931-3936`.
- The **local apply** failing after the quorum is secured → `AccordDriverError::Network`
  (non-retryable), **not** an abandon — `coordinator.rs:2809-2853` (emit at `:2849-2853`),
  asserted by `coordinator.rs:5916-5975`.
- The read-vote dep wait timing out → **abstain** (returns `false`), logged at
  `accord/handlers.rs:314-331`; this is not an abandon and carries no `abandoned:` prefix.

---

## (2) The wait graph: structures, writers, readers, bounds, and prune key

`DepWaitGraph` fields (`accord/dep_wait.rs:86-101`):

| Structure | Field / line | Written by | Read by | Bound / prune | Keyed by |
|---|---|---|---|---|---|
| forward edges | `waiting_on` `:88` | `register_wait` `:142`; `mark_applied` `:204-209,219`; `clear_waits` `:230`; `waive` `:260,276`; `remove_txn` `:452,466` | `detect_cycle`/`dfs_cycle` `:433`; `deps_of` `:354`; `is_waiting` `:349`; `check_timeouts` `:365` | entry removed when emptied (`:154-156,206-213,234,276`); **no time prune** | identity |
| reverse edges | `waited_by` `:90` | `register_wait` `:143`; `mark_applied` `:202`; `clear_waits` `:232`; `waive` `:265`; `remove_txn` `:454,464` | `waiters_of` `:245`; `mark_applied` `:202`; `remove_txn` `:464`; `clear_waits` `:232` | entry removed when emptied (`:160-162,235,268,457`) | identity |
| wait start times | `wait_start` `:92` | `register_wait` `:146` | `check_timeouts` `:362`; `abort_timeout` `:375` | removed when waiter unblocks (`:214,220,240,277,473`); **no prune** | identity |
| applied set | `applied` `:95` | `mark_applied` `:197`; `forget` (remove) `:339`; `prune` (retain) `:312-313` | `register_wait` `:137`; `is_applied` `:284`; `prune` `:311` | **`prune` = TIME bound (`max_age`), `forget` = IDENTITY bound** | see below |
| aborted set | `aborted` `:98` | `break_cycle` `:187`; `abort_timeout` `:382`; `forget` (remove) `:342`; `prune` (retain) `:315` | `is_aborted` `:289`; `aborted_count` `:299` | same two mechanisms | see below |
| timeout | `timeout` `:100` | `new`/`with_timeout` `:112,124` | `check_timeouts` `:365`; `abort_timeout` `:379` | n/a | n/a |

**Prune is keyed by TIME; forget is keyed by IDENTITY.**

- `prune(max_age)` (`:309-318`) retains `applied`/`aborted` entries whose insertion time is
  within `max_age` — a pure TIME bound, keyed on `Instant`, not on transaction identity.
- `forget(&[TxnId])` (`:336-345`) removes exactly the named ids — an IDENTITY bound.

### 2a. Can the call at `apply.rs:1468` (`prune(max_age)`) forget a dependency whose `TxnState` is still live?

**The doc-comment claims it can; the claim is a true property of `prune()`, but the call at
`apply.rs:1468` is NOT a live production call.** `apply.rs:1464-1469`:

```
1464| /// Prune the dep-wait graph's `applied`/`aborted` sets. Test-only: production
1465| /// prunes the graph from its maintenance loop.
1466| #[cfg(test)]
1467| pub fn prune_graph_for_test(&self, max_age: std::time::Duration) -> usize {
1468|     self.graph.lock().prune(max_age)
1469| }
```

`apply.rs:1468` is inside a `#[cfg(test)]` method (`:1466`). Whole-workspace grep confirms **no
production caller of `DepWaitGraph::prune(max_age)` anywhere** (the only other `.prune(` hits
are `ferrosa/src/web/auth_cache.rs:211,382`, an unrelated cache). The production bound is
`forget()`, driven by `forget_graph_applied` (`apply.rs:1601-1608`, delegating to `forget` at
`:1607`), which is called from `state_machine::prune_applied` on exactly the ids whose
`TxnState` that prune just dropped:

```
1415| // The dep-wait graph's own `applied`/`aborted` records are forgotten on
1416| // the SAME state watermark, by exact id: the transactions whose `TxnState`
1417| // this prune just dropped. Its `prune(max_age)` is test-only, so without
1418| // this those sets also grew for the life of the process.
1419| self.apply_engine.forget_graph_applied(&applied_ids);
```

(`accord/state_machine.rs:1415-1419`; `applied_ids` is collected at `:1377-1382`, `TxnState`
removed at `:1385`.)

The doc-comment's hazard is real *as a property of `prune()`* (`dep_wait.rs:320-335`): if the
graph dropped an `applied` record by age while the dep's `TxnState` is still live, a later
waiter would see `!graph.is_applied(dep)` at `:137`, register an edge, and never be woken
(the dep already applied and is never re-marked). But in this revision:

- `prune(max_age)` has no production caller (`state_machine.rs:1417` states this explicitly);
- the production path is identity-keyed and lockstep with the state prune
  (`state_machine.rs:1419`);
- and the apply path **filters dependencies to those with live `TxnState`** before the graph
  ever sees them — `ordered_deps` (`state_machine.rs:1056-1074`), filter at `:1072`:
  `.filter(|dep| !self.executes_after(dep, t) && self.txn_states.contains_key(dep))`. So a dep
  with no live state can never be registered as a wait at all.

**Reachability verdict:** the specific hazard is **not reachable in production** in this
revision, because the age-keyed call is test-only and the production bound is identity-keyed
in lockstep with the state watermark. It is reachable **only** if someone wires
`prune(max_age)` back into the maintenance loop — which `state_machine.rs:1417` says
production does not do.

**What test would prove it:** two separate tests, because they prove opposite things:
1. *Proves the hazard exists (would be a red-first failing test today):* at the `DepWaitGraph`
   unit level — `mark_applied(dep)`; `prune(Duration::ZERO)` to drop the graph's `applied`
   record; then `register_wait(waiter, dep)` and assert the waiter is left waiting with no
   wake (`waiting_count()==1` and `mark_applied` is never called again). The existing tests do
   **not** cover this: the `forget` test asserts the *opposite* — that a still-live id's record
   survives (`dep_wait.rs:790-810`, esp. `:802-805`), and the `prune` tests only check set
   sizes (`:724-785`).
2. *Proves production cannot hit it:* an integration test through `AccordStateMachine` that
   (a) applies a dep, (b) runs `prune_applied` so the dep's `TxnState` is gone and
   `forget_graph_applied` has run, (c) registers a fresh waiter naming that dep, and asserts the
   waiter does **not** park. This is closely related to the existing test "a dependency this
   replica holds no live state for must not park its waiter" (`state_machine.rs:1836-1885`),
   which is the closest existing evidence and exercises the `ordered_deps` filter.

---

## (3) The two hand-maintained edge maps (`waiting_on` / `waited_by`)

Every mutator that touches either map, and whether it keeps them consistent
(`accord/dep_wait.rs`):

| Mutator | `waiting_on` | `waited_by` | Consistent? |
|---|---|---|---|
| `register_wait` `:135-169` | adds `waiter→dep` `:142` | adds `dep→waiter` `:143` | Yes — both added; on a detected cycle both are rolled back `:152-163` |
| `mark_applied` `:196-223` | removes `txn_id` from each waiter's deps `:204-205`; removes `txn_id`'s own forward edges `:219` | removes the whole `waited_by[txn_id]` set `:202` | **PARTIAL — see below** |
| `clear_waits` `:229-241` | removes `waiting_on[waiter]` `:230` | removes `waiter` from each dep's set `:232-236` | Yes |
| `waive` `:257-280` | removes `waiter→dep` `:260` | removes `waiter` from `waited_by[dep]` `:265-270` | Yes |
| `remove_txn` `:450-474` | removes `waiting_on[txn_id]` `:452`; removes `txn_id` from each waiter's deps `:466` | removes `waited_by[txn_id]` `:464`; removes `txn_id` from each dep's set `:454-455` | **PARTIAL — see below** |

### 3a. `mark_applied` updates one map without the other (production-reachable)

`mark_applied` removes `waited_by[txn_id]` (`:202`) and then removes `txn_id` from each waiter's
`waiting_on` set (`:204-205`), but its own forward-edge cleanup at `:219`
(`self.waiting_on.remove(&txn_id);`) does **not** remove `txn_id` from `waited_by[dep]` for each
`dep` that `txn_id` was waiting on. Result: a stale reverse edge (`waited_by[dep]` still lists
`txn_id`) with no matching forward edge. `mark_applied` is on the production cascade path —
called from `cascade` (`apply.rs:1781-1789`, `:1785`) and `apply_unblocked`
(`apply.rs:1814-1850`, `:1845`) — so this asymmetry is production-reachable *in code*.
Practical impact is limited because the stale entry is removed the next time `waited_by.remove`
runs for that dep (`:202`, whole-set removal) and because `waive_dependency` on a stale waiter
is a no-op (`apply.rs:1804-1807` returns early when `waive` returns `false`, `dep_wait.rs:262-264`),
but `waiters_of(dep)` (`:244-249`, surfaced as `apply.rs:1792-1794 waiters_on`) can transiently
report a phantom waiter. **Whether the normal flow ever reaches `:219` with a non-empty forward
set is not proven either way** — see "Could not determine".

### 3b. `remove_txn` leaves empty `waiting_on` entries (not production-reachable in this revision)

`remove_txn` (`:450-474`) removes `waited_by[txn_id]` and strips `txn_id` from each waiter's
`waiting_on` deps, but its own comment says "Don't remove the waiter entry here -- they may
have other deps" (`:468`). A waiter left with an empty set stays in `waiting_on` and in
`wait_start` (never cleared), so `check_timeouts` (`:365` gates on
`waiting_on.contains_key`) would keep treating it as an active waiter. `remove_txn` is reached
only via `break_cycle` (`:186`) and `abort_timeout` (`:381`), and both have **no production
caller** (see §1c), so this asymmetry is not on a production path here.

### 3c. Swallowed errors / `let _ =`

- No `let _ =` in `dep_wait.rs`. In `apply.rs`, every graph mutation result is handled: the
  cycle error is inspected (`register_waits_or_refuse` `:1654-1666`) or propagated (`?`); the
  `park` failure path calls `clear_waits` and returns `Err` (`:1699-1705`, `:1755-1758`).
- The `let _ =` occurrences that do exist are unrelated to the edge maps: `handlers.rs:336`
  and `:423` discard a **poll-timeout** `Result` inside the read-vote wait loop (intended, and
  bounded by `READ_DEP_WAIT_POLL` `:140`); `coordinator.rs:1758, 2041, 3254` discard **channel
  sends** of responses. None of these updates an edge map.

---

## (4) What a reported abandon guarantees about durability

### 4a. The claim

The intended guarantee is stated twice:
- `accord/coordinator.rs:543-552` — "an abandon must leave **no committed row readable on any
  replica** ... the abandon is decided BEFORE the coordinator writes its own shard ... an abandon
  means no replica applied."
- `accord/abandon_durability.rs:1-13` — `TxnAbandoned` is the client contract for "NOT committed;
  safe to retry", and "A client that is told this **must not** be able to observe any row the
  transaction wrote."

The ordering supports the coordinator-local half: `remote_apply` is awaited at `:2772` **before**
the coordinator's own `local_apply`/`local_apply_wait` join at `:2807`, so on the abandon return
at `:2799` the `local_apply` future (`:2599-2705`) is dropped without being polled and the
coordinator writes no shard. `finalize_no_write` (`:1378-1397`) then finalizes the local SM with
an **empty** payload (`handle_apply(txn_id, Vec::new())`, `:1394`) and fans an empty-payload
`Apply` to remotes (`:1388`, `no_write_fanout` `:1414+`) — a no-write finalize, which writes no
row (empty-payload handling at `state_machine.rs:1094-1108`).

### 4b. The guarantee does NOT hold on every path that can return `TxnAbandoned`

`TxnAbandoned` has one emit site (`coordinator.rs:2799`). The invariant the branch relies on is
"`!apply_ok` implies no replica applied" — but a replica can hold a durable row while
**withholding its `ApplyOK`**. In `accord/handlers.rs:156-162`:

```
156| async fn apply_writeset_and_ack(&self, txn_id: TxnId, writes: Vec<Vec<u8>>) -> Option<Message> {
157|     let apply_status = self.apply_writeset_durably(txn_id, writes).await?;
158|     if !matches!(apply_status, SmResponse::NoWriteFinalized)
159|         && !await_txn_applied(&self.state, txn_id).await
160|     {
161|         return None;   // NO ApplyOK sent
162|     }
```

`apply_writeset_durably` (`:181-195`) persists through the durable batch/commit-log fsync in
phase 2 (`:190-192`) and only then marks `Applied` in phase 3 (`:192`). The `Applied` advance
itself can fail: `bookkeep_applied` (`state_machine.rs:1300-1321`) returns early when the
`Applied` marker fsync fails, and the comment at `:1304-1307` states the txn "stays Committed"
while the storage write has already happened. When that happens, `await_txn_applied` waits out
its `configured_txn_timeout()` bound (`handlers.rs:346-353`) and returns `false` → `:161`
returns `None` → the quorum predicate `is_apply_ok` (`coordinator.rs:2735-2740`) never counts
that replica → `apply_ok == false` → the coordinator reports `TxnAbandoned` at `:2799`.

Net: a replica can be holding the transaction's rows durably *and* the coordinator can truthfully
fail the apply quorum and report "NOT committed; safe to retry". On such a path the §4a guarantee
("no committed row readable on any replica") is **false**.

The `abandon_durability.rs` test does **not** exercise this path: its `ReplicaTransport` refuses
every `Apply` by echoing a *different* `txn_id` (`:167-190`, `ack_apply:false` at `:126-131`), so
no replica ever persists — the test asserts `read_row == None` for a case where nothing was
written, and after restart (`:239-264`). It is a correct negative test for the
refuse-everything case, but it does not cover "a replica persisted but withheld the ack".

---

## (5) Which of the three waits actually fired in the million-row case

**The code can distinguish WHICH STAGE fired, but cannot distinguish which *sub-cause* within
the apply stage.** Concretely:

- The client string `"...exceeded the {}s dependency-wait bound and was rolled back (NOT
  committed)..."` is produced **only** by `Display for AccordDriverError::TxnAbandoned`
  (`coordinator.rs:577-582`), whose only construction site is `coordinator.rs:2799`. So the
  fired wait was the **apply-stage bound** (`configured_txn_timeout()`), *not* the graph's
  `DEP_WAIT_TIMEOUT` (dead, §1c) and *not* the read/barrier wait (`DEFAULT_BARRIER_TIMEOUT`,
  5 s, which abstains rather than abandons, `handlers.rs:270-339`). The `abandoned:` prefix →
  40001 mapping (`server.rs:1208-1213`) is what the client saw.
- Within the apply stage, `quorum_broadcast_within`/`apply_fanout_bounded_within` return a bare
  `bool` (`coordinator.rs:3205` `Ok(quorum.all_reached())`, `:3269`). That collapses at least
  three distinct sub-causes into `apply_ok == false`: (i) a per-peer RPC that **timed out** at
  the bound (`send_with_timeout`, `:3177`/`:3237`), (ii) a peer that **replied but the ack did
  not count** (empty/mismatched `txn_id`, predicate `:2735-2740`; tests `:6044-6068`,
  `:6074-6096`), and (iii) a peer RPC that **errored** — a case that is *logged*
  (`:3188-3194`, `:3239-3246`) but not surfaced in the returned value. The coordinator's own
  ERROR log at `:2791-2797` prints `unmet = participant.shards.len()` and `timeout` but not
  which sub-cause. **So the code cannot, from the returned error alone, say whether the
  million-row apply quorum failed by timeout, by non-matching acks, or by RPC error** — only a
  log line (present at WARN for RPC error, `:3189`) disambiguates, and only if that log was
  captured.
- A likely discriminator: a million-row load is multi-key, so the fired wait is most plausibly
  `apply_fanout_bounded_within` (`:2751`) rather than the single-key
  `quorum_broadcast_within` (`:2745`). But **the code path taken depends on
  `self.write_set.len() == 1` at `:2741`**, which is not observable from this revision — see
  "Could not determine".

---

## Contradictions (recorded, NOT resolved)

- **C1 — message text vs. governing mechanism.** The client message says "dependency-wait bound"
  (`coordinator.rs:579`) but is emitted from the apply-quorum branch (`:2799`) and its value is
  `configured_txn_timeout()`, which bounds the **Apply fan-out RPC** (`:2745/2751/3237/3177`).
  The string is load-bearing for client classification (`:571-576`, `3419-3432`; `server.rs:1208`),
  so it cannot simply be renamed without a compatibility decision. The code acknowledges the
  prior "silent fork" where the string named a bound that never expired
  (`:7497-7506`, `:2899-2902`). Unresolved: whether the name is a defect to fix or a frozen
  client contract.

- **C2 — two unrelated 10-second constants.** `dep_wait::DEP_WAIT_TIMEOUT` (`dep_wait.rs:25`) and
  `state_machine::DEFAULT_TXN_TIMEOUT` (`state_machine.rs:1556-1557`, from
  `epoch_drain.rs:43`) are both 10 s, and `Lane::Data` is also 10 s (`ferrosa-net/src/pool.rs:707`).
  Only the second governs the message; the first has no production caller. Unresolved: whether the
  coincidence of defaults is intended or an accident that makes the bound hard to reason about.

- **C3 — `forget()` doc-comment vs. the actual guard.** The `forget` doc (`dep_wait.rs:320-335`)
  says an age bound "can forget a dependency whose `TxnState` is still live (the apply path keeps
  such a dependency in the waiter's set)", implying the apply path can name a live dep whose graph
  record is gone. In this revision the apply path **filters to live-`TxnState` deps**
  (`ordered_deps`, `state_machine.rs:1072`) and `prune(max_age)` is test-only (`apply.rs:1466`,
  `state_machine.rs:1417`), so the hazard is a property of `prune()`, not a production path. The
  comment's own causal framing ("the apply path keeps such a dependency in the waiter's set") is
  consistent with `prune` being *usable*, but the safety argument that makes `forget` correct
  relies on `ordered_deps` — a filter the `forget` comment does not cite. Unresolved: whether the
  doc overstates production reachability.

- **C4 — "no replica applied" vs. ack-withholding.** `coordinator.rs:545-552` and
  `abandon_durability.rs:6-13` assert an abandon means no replica holds a committed row; but a
  replica whose `Applied` marker fsync fails still holds the durable write while withholding its
  `ApplyOK` (`handlers.rs:157-161`; `state_machine.rs:1300-1308`), so the coordinator can abandon
  while that replica holds the row. Unresolved: whether this is a known accepted case or a gap.

---

## Could not determine

- **The exact sub-cause of the million-row apply-quorum failure** (RPC timeout vs. non-matching
  acks vs. RPC error). The decision collapses to a `bool` (`coordinator.rs:3205/3269`); only a
  WARN log line (`:3189`, `:3240`) distinguishes an RPC error, and nothing distinguishes
  "timed out" from "replied-but-uncounted" in the returned value. §5.
- **Whether the million-row write was single-key or multi-key on the wire** — decides
  `quorum_broadcast_within` (`:2745`) vs `apply_fanout_bounded_within` (`:2751`). The branch is
  chosen by `self.write_set.len() == 1` (`:2741`), not observable from this revision.
- **Whether `mark_applied`'s forward-edge cleanup at `dep_wait.rs:219` is ever reached with a
  non-empty forward set** (i.e. whether the §3a asymmetry ever bites). The normal flow appears to
  clear `waiting_on[txn_id]` when the last dep applies (`:204-213`) before `mark_applied(txn_id)`
  runs, but I did not prove no ordering reaches `:219` with edges present.
- **The live cluster's RF/quorum and whether the coordinator was itself a replica** in the failing
  run — this decides whether the missing ack was one remote or more (`coordinator.rs:3222-3225`
  counts self as an implicit ack; `:2555` `self_is_replica`), and whether the abandon branch was
  reachable at all for that RF.
- **Whether any *other* (non-this-revision) build or a dynamic dispatch path calls
  `DepWaitGraph::prune(max_age)`, `check_timeouts`, `abort_timeout`, or `break_cycle`.** In
  *this* revision there is no production caller for any of them; I can only speak to this tree.
- **The precise value of `FERROSA_ACCORD_TXN_TIMEOUT_SECS` in the failing environment** — the
  message renders `timeout.as_secs_f64()` (`coordinator.rs:581`); the reported failure text said
  10 s, which matches the default (`state_machine.rs:1556-1557`) but not necessarily the operator's
  setting.

---

## Evidence index (key citations)

- `accord/dep_wait.rs:25,112` (DEP_WAIT_TIMEOUT); `:14-17` (header's 10 s claim);
  `:86-101` (struct); `:135-169` (register_wait); `:175-190` (break_cycle);
  `:196-223` (mark_applied, asymmetry at `:219`); `:229-241` (clear_waits); `:257-280` (waive);
  `:309-318` (prune = TIME); `:320-345` (forget = IDENTITY + doc); `:360-369`/`:374-385`
  (check_timeouts/abort_timeout, no prod caller); `:450-474` (remove_txn);
  `:724-785` (prune tests); `:790-810` (forget test).
- `accord/apply.rs:1391-1418` (DepWaitApplier); `:1464-1469` (`prune_graph_for_test`, `#[cfg(test)]`);
  `:1510-1559` (reclaim_stale); `:1601-1608` (forget_graph_applied); `:1648-1670`
  (register_waits_or_refuse); `:1686-1718`/`:1736-1770` (try_apply_writeset[_borrowed]);
  `:1781-1789`/`:1814-1850` (cascade / apply_unblocked → mark_applied).
- `accord/coordinator.rs:520-561` (error enum); `:563-585` (Display, `:577-582` message);
  `:2527-2538` (`apply_phase`, bound `:2535`); `:2545-2550`; `:2599-2705` (local_apply);
  `:2719-2726` (local_apply_wait); `:2733-2754` (remote_apply); `:2772` (apply_ok);
  `:2783-2800` (abandon emit `:2799`); `:2802-2854` (post-quorum local, Network `:2849`);
  `:2896-2912` (quorum_broadcast_within); `:2955-3206` (apply_fanout_bounded_within);
  `:3212-3269` (quorum_broadcast_per_peer, send_with_timeout `:3237`);
  `:1378-1397` (finalize_no_write); `:5899-5975`, `:6044-6068`, `:6074-6096` (tests);
  `:7497-7506` (silent-fork test).
- `accord/state_machine.rs:1537,1548,1556-1560` (constants); `:1644-1657` (resolvers);
  `:1056-1074` (ordered_deps filter `:1072`); `:1089-1108`,`:1191-1217` (apply);
  `:1227-1238` (finish_apply); `:1255-1267` (release_waiters_ordered_before);
  `:1300-1321` (Applied fsync fail → stays Committed); `:1373-1423` (prune_applied +
  forget lockstep `:1419`); `:1836-1885` (no-live-state test).
- `accord/handlers.rs:140` (READ_DEP_WAIT_POLL); `:156-169` (apply_writeset_and_ack → `None`);
  `:253-339` (await_conflicting_deps_applied; barrier bound `:270`); `:346-353` (await_txn_applied).
- `ferrosa-postgres/src/server.rs:1202-1213` (abandoned → 40001);
  `ferrosa-cql/src/router.rs:3931-3936` (CQL mapping);
  `ferrosa-net/src/pool.rs:707` (Lane::Data = 10 s);
  `accord/abandon_durability.rs:1-13,126-131,167-190,196-265` (invariant + test).
