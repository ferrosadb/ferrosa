# Cross-DC relay: one copy per remote data center

Status: in progress. Owner: Ben. Decisions from the design grill (2026-10-16).

## Model (decided)

- A **DC is a local Raft group**: nodes inside it are locally connected and fully
  replicated. A DC→DC link crosses a WAN, so it has latency and little bandwidth:
  it must not be chatty, and it carries **one copy per remote DC per operation**,
  never one copy per remote node.
- Membership stays **per-DC**: a DC's ring holds its own nodes only, and replica
  placement stays DC-local. Cross-DC reachability is a separate **DC registry**
  (dc → its known candidate nodes) used *only* to relay.
- The receiving DC may be reached through **any live node** in it: the registry
  holds candidates and the relay rotates to the next one when one refuses.
- Delivery is **consistency-level dependent**: `LOCAL_*`/`QUORUM` commit locally
  and deliver asynchronously (counted, retried, repaired); `EACH_QUORUM`/`ALL`
  require the remote DC to acknowledge before the client is told it committed.
- Everything committed into a per-DC Raft group is relayed this way — DDL/schema,
  membership, Accord applies. The DML replica fan-out becomes DC-granular too: the
  receiving DC resolves its own replicas locally.

## Invariants (one test each; no code before they are stated)

| id | invariant | test |
|---|---|---|
| I1 | At most one copy per remote DC per operation: N DCs ⇒ ≤ N−1 cross-DC sends, regardless of node count | `registry_holds_every_peer_of_a_remote_dc_as_a_candidate`, `fan_out_sends_exactly_one_copy_per_remote_dc` |
| I2 | A DC that acknowledged is never sent another copy | `fan_out_retries_without_resending_to_a_delivered_dc` |
| I3 | A refused copy rotates to ANOTHER live node of that DC; a DC that exhausts its candidates AND attempts is reported + counted, never dropped silently | `fan_out_rotates_to_the_next_live_candidate_in_the_dc`, `fan_out_reports_a_dc_that_never_acknowledges` |
| I4 | One received copy reaches the receiving DC's own quorum through its own Raft group (inside-DC mesh) | `one_copy_per_remote_dc_reaches_that_dcs_own_quorum` |
| I5 | The registry is learned from announced DCs; a peer that announced none is never given one | `registry_skips_peers_with_no_announced_dc` |
| I6 | CL-dependent: strict CL fails the operation when a remote DC does not ack; QUORUM/LOCAL_* succeed locally and count the pending delivery | `strict_cl_fails_when_a_remote_dc_does_not_ack`, `local_cl_commits_and_records_a_pending_delivery` |
| I7 | Chatter bound: one relay op per remote DC per operation — no per-row or per-message cross-DC traffic | `fan_out_makes_one_attempt_per_dc_when_all_ack` |
| I8 | A retried copy that already applied is a no-op (Accord `txn_id` ledger; schema `schema_version`) | existing: `multi_dc_apply` ledger tests + `cross_dc_dispatch` dedupe note |
| I9 | A relayed DML op leaves the same rows in the remote DC as a local write (data preserved, no duplication) | to be written with the DML slice |

### Slice 4 invariants (DML fan-out), stated before any write-path change

The DDL slice relays through `DdlPath`; DML is harder because the fan-out lives on
the hot path (`coordinate_write_nts`), so these are written first, one test each:

| id | invariant | why it could break |
|---|---|---|
| I10 | A DML operation addresses **at most one node per remote DC** (N DCs ⇒ ≤ N−1 cross-DC sends), never one per replica node | the existing path enumerates replicas and would enumerate remote ones |
| I11 | The receiving DC resolves **its own** replica set for the token; the sender never names a remote replica | else placement logic leaks across the WAN and DC-local placement stops being DC-local |
| I12 | DML is **data-preserving**: the same rows land in the remote DC, no loss and no duplication, and a retried copy is a no-op | a retry after a lost ack must not double-apply (idempotency is by the Accord `txn_id` the engine already dedupes on) |
| I13 | The client's consistency level is unchanged by the relay: only `EACH_QUORUM`/`ALL`/`SERIAL` wait on a remote DC; `LOCAL_*`/`QUORUM` do not | a WAN hop must never become part of a local quorum's latency |
| I14 | The relayed payload is the existing encoding on the hot path — the Accord write-set / capnp frames the path already uses, never a new bincode or JSON per row | hot-path serialization is capnp by rule; a JSON row image already rides inside the Accord write-set |

I10–I12 are the reason this slice is not started yet: which node in the receiving
DC the copy goes to (its registry contact, rotated per I3) and how that node
places the rows for its own ring (I11) interact with the CL-76 finding — a DC that
cannot accept clients still applies relayed internode work, which is exactly the
shape a DML copy needs, but it must not be *chosen* by placement logic.

## CL-76 — a DC's group must converge (evidence first, then invariants)

The six-process acceptance test (`data_center_process::a_ddl_created_in_one_dc_reaches_the_other`)
fails today: dc1 (`us-west-2`) becomes ready, dc2 (`eu-west-1`) never does. Retained node logs
(`FERROSA_TEST_KEEP_NODE_LOGS`) give the mechanism, and it is not discovery:

| node | DC | is seed | `raft.initialize` members | membership changes |
|---|---|---|---|---|
| 3333 | us-west-2 | yes | `{1111, 3333}` (2) | `add_learner` + `change_membership(AddVoterIds)` for 2222 |
| 1111, 2222 | us-west-2 | no | — | — (waited for leader) |
| 4444 | eu-west-1 | **yes** | `{4444}` (1) | **none** |
| 5555 | eu-west-1 | **yes** | `{5555}` (1) | **none** |
| 6666 | eu-west-1 | **yes** | `{6666}` (1) | **none** |

Every dc2 node connected to the seed and to its own DC's peers (the log shows inbound peers
`5555`/`6666` and `peer_count=0` at seed time), so the peers were reachable and known — later.
Two defects compound:

1. **The seed test is vacuously true on an empty same-DC peer set.** The rule is "the node with
   the highest UUID among all members (self + peers)", so a node that knows no peer of its own DC
   computes `max({self}) == self` and initialises a **one-voter group**. dc2 produced three of
   them; dc1 produced one group that then converged.
2. **Nothing adds a same-DC peer that arrives after initialisation.** dc1 recovered only because
   a committed `JoinNode` runs the learner→voter promotion; on dc2 no promotion ever ran, so the
   groups stayed at one voter forever and `committed_voters >= expected` could never hold.

### Invariants for the fix (one test each)

| id | invariant | how it is violated today |
|---|---|---|
| I-A | within a DC exactly one node initialises its group, and only from a **complete** same-DC membership (self + known same-DC peers), never from an empty peer set | `max({self}) == self` makes every lone node the seed |
| I-B | a node in cluster mode whose local voter set is missing a **known same-DC** peer adds it (learner → voter), so the group converges to every node of its DC even when a peer connects after initialisation | no path fires; the voter set is fixed at initialisation |
| I-C | no membership change ever adds a peer of another DC (ADR-015 stands; the cross-DC surface is the registry) | holds today — must keep holding |
| I-D | readiness is unchanged: ready ⟺ raft leader ∧ committed_voters ≥ `expected_cluster_size` | holds; the fix must not lower or fake it |
| I-E | repeated connections for a peer already in the voter set issue no membership change, and the pending/promoted bookkeeping is pruned | must not regress into a promotion storm |
| I-F | membership changes are consensus metadata only: no token-ring/placement change, no data deletion or rewrite, no ordering change | must not regress |

`I-A` is proven red by a unit test on the seed decision with an empty same-DC peer set; `I-B` by
the six-process test plus a real-openraft test that initialises `{A}` and then connects a same-DC
`B` in cluster mode and asserts the voter set becomes `{A, B}`; `I-C`/`I-E` by the new path's own
tests; `I-D`/`I-F` by the existing suite staying green.

### Attempt 1 (reverted) — what it taught, and why the fix is not just `I-A`

`I-A` and `I-B` were implemented and measured on the six-process test. Retained logs from that run:

- Both DCs' seeds took the late path (`CL-76: this node is the seed and its DC's declared
  membership is complete`) — 3333 for `us-west-2`, 6666 for `eu-west-1` — so gating on a complete
  membership does reach the right node in both DCs.
- **No node called `raft.initialize` at all** (`RaftMsg::Initialize` appears zero times in all six
  logs), and the hook ran **15 times** on one node. So the late path is blocked downstream by the
  existing "only the seed initialises, and only from a recovered-membership-free state" guard, and
  re-entering `transition_to_cluster` per peer event is a churn regression.
- dc1 — the DC that *worked* before — stopped working: node 1111 hit
  `cluster formation timed out ... attempting to add self to existing cluster voter set (P0-21)`
  with `peer_count=1`.

The lesson, and the thing to design against for attempt 2: **the mesh is genuinely incomplete at
formation time** (a node sees one same-DC peer, not two), and in the working case the full voter set
is reached through the formation-timeout → `cluster_rejoin` (`JoinNode` → `add_learner` →
`change_membership`) path — the one that already fires for dc1. So `I-A` alone is wrong: it turns a
working (if late) formation into a wait. The fix has to make **`I-B` converge for a DC whose seed saw
an incomplete set**, reusing the existing rejoin promotion, and must fire **once** (no per-event
`transition_to_cluster`), not replace the seed rule with a completeness gate.

### Attempt 2 — the precise blocker, from the debug-level node logs

Re-running the same test with `FERROSA_TEST_RUST_LOG="info,ferrosa_cluster::controller=debug"` (the
join path's early returns log at DEBUG, which the harness pins to `info` by default) puts the actual
decision in the dc2 nodes' logs:

- `membership: peer join already pending, skipping duplicate trigger peer=5555…`
- **`membership: peer already present in token ring with current metadata, skipping join trigger
  peer=5555…`** — the same-DC peer IS in this node's **token ring** (placement membership) while it
  is absent from its **openraft voter set**, and the guard that exists to stop duplicate-join churn
  short-circuits exactly the reconciliation that is needed. dc2 ends with 0 membership changes while
  dc1 gets one (`2222` was not yet in `3333`'s ring, so its trigger proceeded).
- `WARN membership: JoinNode proposal failed peer=2222… e=has to forward request to: Some(6…)` — a
  proposal attempted on a non-leader whose forward target did not resolve, i.e. proposing across the
  per-DC groups is a second, independent failure.
- Each dc2 node also ran the seed path (`seed: submitted JoinNode + AssignTokens … leader=…`), so a
  dc2 node did initialise its own group — consistent with the three one-voter groups above.

So the convergence fix is a **membership-reconciliation** fix, not a seed-rule fix:

1. The trigger must key on the peer's absence from the **voter set**, not on the token ring — an
   entry in the ring is not a vote, and today that inequality is what stops the promotion.
2. A proposal must be able to reach a leader that can promote it (resolve the forward target across
   the per-DC groups, or promote on the local leader), or the `has to forward request to` failure
   needs a defined retry that does not depend on a node_map entry that may not exist yet.
3. `I-A` still has a role — `max({self}) == self` is vacuously true, and three one-voter groups in
   one DC cannot ever reach `expected` — but it must be applied **with** (1)+(2), never alone, and
   must not turn a late-but-working formation into a wait.

Evidence lives only in the retained node logs from that run
(`FERROSA_TEST_KEEP_NODE_LOGS=<dir>`, ~1 MB, in the session scratch dir and pruned after 24 h), which
is why both harness knobs (`KEEP_NODE_LOGS`, overridable `RUST_LOG`) are part of this change.


## Slices

1. **DC registry + live-candidate rotation** — done (`e5a3086b`).
   `dc_registry()` replaces the single-contact rule: every known node of a remote
   DC is a candidate (deterministic order), the relay tries them in turn, and a
   DC is delivered by whichever candidate acknowledges.
2. **CL-dependent delivery** — done (`aabd7235`): `EACH_QUORUM`/`ALL`/`SERIAL`
   wait for every remote DC; the others commit and record a pending delivery for
   repair (`CROSS_DC_PENDING_REPAIRS_TOTAL`).
3. **Wire the relay into the per-DC Raft commit path** — in progress. The runtime
   half is in: `ModeController::cross_dc_registry()` (built from `connected_peers`
   + announced DCs) and `ModeController::relay_ddl()`, which hands a committed DDL
   to every remote DC over the existing `Message::PairDdlForward`, so the
   receiving node's `ClusterDdlForwardHandler` puts the copy into *its* DC's
   group — no new protocol surface. Proven on real 3-voter groups in
   `tests/cross_dc_ddl_relay.rs` (one copy → every voter in the receiving DC has
   the keyspace).
   Remaining: the CQL router call site (after a successful DDL commit) and a
   real-process 2-DC CQL proof (create keyspace on `us-west-2`, read it on
   `eu-west-1`).
4. **DML DC-granularity**: the coordinator sends one copy per remote DC; the
   receiving DC fans out to its own replicas.
5. **3-DC real-process ring**: each DC relays to the other two; kill/partition
   tests per the HA bar.
