---
type: todo
priority: P1
status: draft
created: 2026-04-25
updated: 2026-04-25
---

# Bug: Raft node with stale log inflates term forever; never demotes to follower

## Why this is a Ferrosa bug

A Ferrosa cluster node that briefly loses connectivity to the rest of the
cluster enters an infinite election loop and can never re-join, even after
network recovery. Its term advances unboundedly while the live quorum stays
on a small term (8 vs ~18,000 in the case observed). The disconnected node:

- can't win an election — its log is behind, so peers reject it
- can't step down — its own term is now greater than every peer's, so the
  leader's `AppendEntries` and `InstallSnapshot` RPCs are rejected by the
  candidate's own `vote_handler` ("rejected by local vote: T18363 …")

The cluster is functionally degraded (2-of-3 quorum) and the lonely node
spins forever burning CPU on losing elections. After the controller's
shutdown timeout fires, the node hits a 10-second drain budget and gets
SIGKILLed (exit 137). It does not recover on next boot — the on-disk Raft
state remembers the inflated term.

This is the classic "disruptor partition" failure mode that Raft's PreVote
extension was designed to prevent. OpenRaft supports PreVote; either it is
not enabled in Ferrosa, or it is bypassed in the cluster bootstrap path.

## Observed on

- Ferrosa commit: `44a7e6b` (`fix(cluster): surface swallowed Raft bootstrap signals (p0-08)`)
- Image: `localhost/ferrosa-memory-node:latest`
- Cluster: local 3-node podman cluster from
  `ferrosa-memory/docker-compose.yml` (prod tier — `node1`/`node2`/`node3`,
  not the `*-test` variants)
- Containers: `ferrosa-memory_node1_1`, `_node2_1`, `_node3_1`
- node1 host_id: `11111111-1111-1111-1111-111111111111` (Raft id `1229782938247303441`)
- node2 host_id: `22222222-2222-2222-2222-222222222222` (Raft id `2459565876494606882`)
- node3 host_id: `33333333-3333-3333-3333-333333333333` (Raft id `3689348814741910323`)

## Symptom timeline

1. `node3` started 2026-04-24 13:52:39 PDT alongside `node1`/`node2`.
2. Cluster reached quorum and committed `T8` with `node1` as leader,
   replicating to log index 1591.
3. `node3` either never replicated past `T3` log 1087 or rolled back to
   that point. Its on-disk vote/log state was left at
   `T3-N3689348814741910323-1087`.
4. `node3` began incrementing its candidate term every ~17 s (the
   `election_timeout_max` after the P2 starvation fix). Each round:
   - `Engine::elect → new candidate {T<N>-N3689…:uncommitted}`
   - `node3` votes for itself; sends `RequestVote` to peers
   - peers reply with `{T8-N1229…:committed, last_log:Some("T8-N1229…-1591")}`
   - `Engine::handle_vote_resp` logs `seen a greater log id`
     (`Some(T8-N1229…-1591) > Some(T3-N3689…-1087)`)
   - `vote_handler` then logs `vote T8-N1229…:uncommitted is rejected by
     local vote: T<N>-N3689…:uncommitted` because `node3`'s term `<N>` is
     larger than the leader's term `8`.
5. Loop continues until `T18363` (~32 hours, ~18,300 elections, roughly
   one every 6 s — the actual cadence is tighter than the configured
   max because elections fire on every "no quorum" timer).
6. `2026-04-25 22:20:39 PDT`: pod restart triggered SIGTERM on `node3`.
   `ferrosa_cluster::controller`: `shutdown timed out — aborting remaining
   tasks remaining=1`. Exit 137 (`OOMKilled=false`, so it was the 10-second
   drain budget the runtime aborts on).
7. `node1` and `node2` continue spamming reconnect attempts on `Raft`,
   `Data`, `Bulk` lanes against `10.89.1.86:7000` (no route to host) at
   ~30 s per lane — log noise but writes still succeed via 2-of-3 quorum.

## Reproduction

This is reproduced by the existing 3-node podman compose from
`ferrosa-memory/docker-compose.yml`, exercised over a 24+ hour soak.
A faster repro exists by simulating it directly:

1. `podman compose up -d node1 node2 node3` and let the cluster reach
   quorum (verify: `podman logs node1 | grep "openraft.*leader"`).
2. While the cluster is stable, isolate `node3` from `node1`/`node2` for
   60 s — long enough for `node3` to start a few elections and increment
   its term past the cluster's. Two ways:
   - `podman network disconnect <pod-network> ferrosa-memory_node3_1`
     wait 60 s, `podman network connect …`
   - or `iptables -A INPUT -p tcp --dport 7000 -j DROP` inside `node3`.
3. Reconnect. `node3` will keep spamming elections at its now-higher term;
   `node1` will continue serving traffic; `node3` will never re-replicate.
4. Compare the two states:

   ```bash
   podman logs ferrosa-memory_node1_1 | grep -E "T[0-9]+-N1229" | tail -1
   podman logs ferrosa-memory_node3_1 | grep -E "elect, new candidate" | tail -1
   ```

   Expected: `node1` term stays at `T8`, `node3` term is monotonically
   increasing.

## Expected behavior

A node that briefly partitioned and is now reconnected must be able to
re-join the cluster as a follower. Specifically:

1. **PreVote**: a candidate that has been disconnected should not actually
   bump its persisted term until it gets a quorum of pre-vote responses
   from peers that *would* grant it. OpenRaft's `Config::enable_tick` /
   `enable_heartbeat` plus its PreVote support cover this — it has to be
   wired through Ferrosa's `RaftConfig` builder.
2. **Stale-candidate detection**: even without PreVote, when a candidate
   sees a higher *log id* in a peer's response (as `node3` does — the log
   says `seen a greater log id`), it should fall back to follower against
   that peer's leader, regardless of term comparison. Pure-Raft semantics
   say the leader's term wins; here `node3`'s incremented term is a local
   artifact of its disconnection, not consensus.
3. **Operator escape hatch**: an admin command (`ferrosa cluster reset-raft
   --node N`) that wipes a node's Raft term/vote state and rejoins it as
   an empty follower. Today the only recovery is to delete the node's
   Raft data dir manually.

## Files to look at

- `ferrosa-cluster/src/raft/` — Ferrosa's OpenRaft adapter; `RaftConfig`
  builder is where PreVote would be enabled.
- `ferrosa-cluster/src/controller/peer_events.rs` — the
  "peer suspected dead (not transitioning)" log (visible on `node3-test`
  briefly at 02:00:16Z) shows the controller refusing to act on a dead
  peer; this same code path may need to detect a self-stuck candidate.
- `ferrosa-cluster/src/config.rs` — election timeout knobs (already bumped
  to 3000/6000 ms in `bug-bulk-write-raft-starvation.md`).
- `ferrosa-net/src/reconnect.rs` — the reconnect lane noise that node1
  and node2 produce while node3 is unreachable. Not the bug itself, but
  worth a back-off review if reconnect attempts keep firing forever.

## Diagnostics already collected

- `node3` final state and last 80 log lines: see attached transcript on
  `feature/skills-and-richer-entities` ferrosa-memory branch session of
  2026-04-25.
- `node1` and `node2` are healthy at `T8`, log 1591, `table_count=45`,
  serving CQL.
- `node3` healthcheck stayed `healthy` until SIGKILL; the issue is purely
  in the Raft layer, not in the storage engine or HTTP transport.
- `node3-test` (parallel test cluster) showed the same "peer suspected
  dead: 3 missed heartbeats" pattern transiently at 02:00:16Z and
  recovered. `node3` (prod cluster) did not recover.

## Workaround

Per `ferrosa-memory/CLAUDE.md`: do not add a workaround in `ferrosa-memory`.

For local recovery only (not for production):

```bash
podman volume rm ferrosa-memory_node3_data    # wipes node3's Raft state
podman compose up -d node3                     # rejoins as fresh node
```

This loses any uncommitted writes that were on `node3` only — acceptable
because Raft's quorum guarantees they were never visible to clients.

## Related

- `bug-bulk-write-raft-starvation.md` — earlier election-storm bug; the
  starvation fixes there (election timeout 3000/6000 ms, dedicated
  Raft-lane OS thread, write backpressure) keep the *cluster* alive under
  load but do not address an isolated node whose term has run away.
- `cluster-formation-architecture.md` and
  `cluster-formation-state-machine.md` — cover the join progression
  (`standalone → pair → cluster`); a stale-candidate detector should slot
  in to the `cluster` mode's peer-event handler.
