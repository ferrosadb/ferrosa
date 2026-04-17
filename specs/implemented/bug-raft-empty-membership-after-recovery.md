---
type: bug
priority: P1
reported-by: ferrosa-memory podman cluster restart
implemented-by: ""
verified-by: ""
created: 2026-04-16
updated: 2026-04-16
---

# Raft startup succeeds but membership is empty → no quorum, no elections

## Observed

After the `bug-raft-startup-fails-after-oom-purged-log` fix landed, all 3 nodes
in the ferrosa-memory podman cluster start up without the old
`Failed to get log entries` error. However, the persisted Raft state on every
node comes back with **empty membership**:

```
membership_state: MembershipState {
    committed: EffectiveMembership {
        log_id: None,
        membership: Membership { configs: [], nodes: {} },
        voter_ids: {}
    },
    effective: EffectiveMembership {
        log_id: None,
        membership: Membership { configs: [], nodes: {} },
        voter_ids: {}
    }
}
```

All three nodes start as `server_state: Learner`. No node attempts
`raft.initialize()` (node1: "non-seed node — skipping raft.initialize(), waiting
for leader AppendEntries"; node3: "raft initialize returned error (may be
already initialized) ... not allowed to initialize due to current raft state").
Since no voters exist, no election can happen. After 30s the cluster controller
logs:

```
WARN ferrosa_cluster::controller::cluster: raft leader election timed out after ~30s — reverting to Pair mode
```

In Pair mode the CQL listener is up (`CQL server listening on 0.0.0.0:9042`)
but closes client connections mid-handshake. External clients see:

```
NoHostAvailable: error('unpack requires a buffer of 2 bytes')
```

All SSTables still exist under `~/data/ferrosa-memory/node*/sstables/` (~13,934
entities of user data), but they are unreachable until quorum forms.

## Repro

1. Run ferrosa-memory podman cluster with 3 nodes until it has taken writes
   (so log entries and a snapshot exist).
2. Crash / restart the cluster (e.g. OOM kill, `podman compose down && up`).
3. On restart, nodes pass the log-purge recovery (the earlier bug fix) but
   come up with empty `membership_state`.

The `snapshot_meta.last_membership` is also `StoredMembership { log_id: None,
membership: Membership { configs: [], nodes: {} } }` — so the snapshot the
nodes recover from does not carry membership either. This suggests membership
was never durably written into a snapshot/log entry, OR the log replay step
discards it.

## Root Cause Hypotheses

1. **Snapshot omits membership**: When the state machine takes a snapshot,
   `last_membership` is being stored as the default (empty) value instead of
   the current committed membership.
2. **Log replay clears membership**: The state machine reconstruction during
   startup walks the log but does not track `Membership` ChangeConfig entries
   (or the state machine apply path skips them — the logs show many
   "Raft apply: system table write skipped for CreateTable" warnings, which
   hints that the apply path may silently drop or misroute entry kinds).
3. **Recovery path doesn't reconstruct membership**: The new
   `recover_from_purge_point()` path sets `last_applied` but may not reconstruct
   `last_membership` from the log / snapshot metadata.

Evidence for #2 or #3: `last_applied` and `committed` are both
`T11488-N2459565876494606882-1125`, so the log has entries and they were
applied — but the resulting membership is empty. A change-membership entry
either was never in the log, or was not applied to state.

## Expected

After a clean restart with a populated log/snapshot, the cluster should
reconstruct its last-committed membership and form quorum without operator
intervention. Losing membership on every restart defeats the purpose of having
persistent Raft state.

## Proposed Fix Direction

- Verify that `SnapshotBuilder` captures `last_membership` into the snapshot
  metadata (check `FerrosStateMachine::build_snapshot`).
- Verify that the state machine applies `Membership` / `ChangeMembership` log
  entries to persisted state — not just CQL/DDL entries.
- If neither of the above is the issue, extend `recover_from_purge_point()` to
  also walk the log (or re-read the snapshot metadata) to reconstruct
  `last_membership`.
- Fail-loud: if the state machine recovers `last_applied` but
  `last_membership` is still empty AND the log has entries, log an ERROR
  rather than silently starting as a Learner with no voters.

## Temporary Workaround

Wipe `~/data/ferrosa-memory/node*/raft/` on all nodes (keeping `sstables/` and
`commitlog/`) and restart. The seed node (node1 in standalone mode) will
re-bootstrap membership; node2 and node3 will rejoin via pair/cluster mode.
SSTable reads resume once the state machine is rebuilt from commitlog replay.

This is destructive to Raft log history but preserves the user data. It is
the operational equivalent of "every restart is a cold start for Raft" — not
acceptable as a long-term behavior.

## Acceptance Criteria

- [ ] After `podman compose down && up` on a cluster with existing data,
      nodes come up with non-empty `membership_state` reflecting the previously
      committed configuration.
- [ ] Quorum forms within 30s of startup (no "raft leader election timed out"
      warning).
- [ ] CQL client connections succeed (end-to-end: `SELECT COUNT(*) FROM
      agent_memory.entity_store` returns the expected count).
- [ ] Unit test: snapshot round-trip preserves `last_membership`.
- [ ] Unit test: state machine replay of a log that includes a ChangeMembership
      entry results in the expected `last_membership`.
- [ ] Regression test against the ferrosa-memory 3-node cluster: restart loop
      (up → wait healthy → down → up) 3 times without membership loss.

## Implementation Notes

Root cause confirmed: `recover_from_purge_point()` set `last_applied` but did not recover `last_membership`. After OOM kill, the in-memory membership reverted to empty (default).

Fix:
1. `SledLogStore::find_last_membership()` — scans the log backwards for the most recent `Membership` entry (`ferrosa-cluster/src/raft/log_store.rs`)
2. `FerrosStateMachine::recover_membership()` — sets `last_membership` from the log if it's empty (`ferrosa-cluster/src/raft/state_machine.rs`)
3. `controller/cluster.rs` — calls `find_last_membership()` + `recover_membership()` after the purge-point recovery, before `FerrosRaft::new()`

Tests: `recover_membership_restores_from_log`, `recover_membership_noop_when_already_set`

## Related

- `specs/implemented/bug-raft-startup-fails-after-oom-purged-log.md` — the
  immediate predecessor; that fix exposes this issue by letting startup
  progress far enough to reveal the empty-membership state.
