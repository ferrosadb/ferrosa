---
title: Accord read-vote quorum loss leaves a committed transaction unapplied
status: blocked-on-design
created: 2026-09-22
updated: 2026-09-22
severity: P0
area: ferrosa-cluster Accord coordinator and recovery
affected-versions: v2026.09.21.2058 and main at 730ecb53
---

# Accord read-vote quorum loss leaves a committed transaction unapplied

## Symptom

The local three-node Ferrosa Memory cluster became unable to claim one
consolidation lease after a restart. The first post-restart attempt reported:

```text
generic IF read-vote lacked F+1 (2) agreement on the row at t
(got 1 reads) — refusing a non-linearizable LWT
```

Later attempts against the same lease row repeatedly reported zero reads. All
three nodes then logged five-second `ReadVote` dependency-wait timeouts. The
failure continued while unrelated Accord transactions still committed and
applied, so this is a poisoned-key liveness failure rather than total cluster
unavailability.

On 2026-09-22 the MCP retry loop reproduced this every roughly 2 minutes 49
seconds, while node1 and node2 emitted matching dependency-wait timeouts. The
installed release was `v2026.09.21.2058`. Current main at `730ecb53` contains
the same control flow and no later recovery change.

## Root-cause hypothesis

`AccordCoordinatorDriver::run_transaction` commits locally and reaches F+1
`CommitOK` before it starts the conditional read-vote
(`ferrosa-cluster/src/accord/coordinator.rs`, phases 3 and 4).

If the generic read-vote does not obtain F+1 identical rows, the coordinator
returns `AccordDriverError::Network` immediately. If the existence vote does
not obtain an explicit F+1 decision, it returns `QuorumUnavailable`. Neither
branch applies the transaction, schedules recovery, or otherwise drives the
already-committed transaction to a terminal state.

That committed transaction remains in each replica's conflict index. Later
transactions touching the same key discover it as a dependency and wait for
`Applied`; because no Apply retry exists for this path, every later read-vote
times out and abstains. The first transient quorum miss therefore becomes a
persistent per-key outage.

This is consistent with the state-machine contract: a committed no-write
transaction must still reach `Applied` to GC its conflict-index entry and wake
dependency waiters. The current `finalize_no_write` path does that only after an
authoritative F+1 condition decision says the condition is false.

## Safety constraint

Do **not** call `finalize_no_write` merely because the read-vote lost quorum.
The condition may actually be true, in which case no-write finalization would
discard a committed write. Do **not** retry by creating a new transaction id;
that leaves the original committed dependency behind and can execute the LWT
twice.

Once phase 3 has committed, the implementation must durably retain enough
state to recover the same transaction id, obtain an authoritative read-vote
decision, and drive the corresponding write or no-write Apply to completion.
Returning an unavailable error to the client is safe only if that completion
work is durably scheduled and survives coordinator restart.

## Design decision required

Choose and document the recovery owner and client contract for a transaction
that commits before its conditional read-vote becomes decidable:

1. The request coordinator retries/recover-drives the same transaction until a
   decision is reached, with a durable background handoff if the client timeout
   expires; or
2. Any replica can discover and recover a committed-but-unapplied transaction,
   including after coordinator death.

The design must also define how the original IF predicate/gate and mutation are
made available during recovery. The current committed state records timestamp
and dependencies but the coordinator owns the generic predicate evaluator.

## TDD test list

- [ ] RED: a generic-IF transaction reaches F+1 CommitOK, gets fewer than F+1
      read-votes, and remains recoverable under the same transaction id.
- [ ] RED: after read-vote quorum is restored, recovery reaches the correct
      terminal outcome: real Apply when the predicate is true, no-write Apply
      when it is false.
- [ ] RED: a later same-key LWT does not dependency-wait forever behind the
      recovered transaction.
- [ ] RED: recovery survives coordinator loss/restart after CommitOK and before
      the read-vote decision.
- [ ] RED: repeated recovery is idempotent and cannot apply the mutation twice.
- [ ] RED: an unavailable client response never implies the LWT was aborted;
      retry behavior is documented and tested.
- [ ] Existing condition-false tests continue to prove that authoritative F+1
      decisions use the no-write finalization path.
- [ ] Existing quorum-loss tests continue to prove that missing votes are not
      promoted to implicit positive or negative votes.

## Acceptance criteria

- [ ] A transient read-vote quorum loss cannot leave a committed transaction
      permanently blocking later transactions on the same key.
- [ ] Recovery uses the original transaction id and preserves Accord safety and
      LWT exactly-once/idempotence properties.
- [ ] Both generic `IF` and existence-predicate branches have the same durable
      completion guarantee after phase 3.
- [ ] Metrics/logs distinguish a recoverable committed-incomplete transaction
      from a fresh quorum failure and expose recovery age/attempt count.
- [ ] A three-node integration test injects the commit/read-vote failure seam,
      restores quorum, and proves the key becomes live without restarting or
      resetting the cluster.
- [ ] The live consolidation lease row can be repaired through the supported
      recovery mechanism without deleting application data.

## Implementation notes

Blocked on the recovery-owner and durable predicate/mutation representation
decision above. No engine change was made while that safety decision is open.
