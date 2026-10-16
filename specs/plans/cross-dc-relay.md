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
