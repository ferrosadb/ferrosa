# Bug: cluster formation stalls when pre-vote is enabled without a pre-vote transport

- **Status**: Fixed (default flipped; guarded by regression test)
- **Component**: `ferrosa-cluster` — Raft formation / election
- **Forge**: t_b0aac0d3 (root cause), t_32cb5ad3 (pre-vote transport — separate, still open)
- **Related**: ADR-012 (PreVote + CheckQuorum), the stalled openraft pre-vote fork epic

## Symptom

A fresh 3-node cluster intermittently never elects a leader. The seed
(highest UUID) transitions to cluster mode, calls `raft.initialize()`, reaches
**term 1 as a Candidate**, and then freezes there for the entire formation
window. The `/cluster/topology` endpoint reports `committed_cluster_size: 0` /
`openraft_voters: []` on the peers because no leader ever commits the
membership. Reproduced at roughly 3% under CPU starvation (shared vCPU); CI job
89743146291.

Crucially the term does **not** climb (this is not the T1→T19 election-storm
signature). It sticks at exactly 1: elections have stopped firing entirely.

## Timeline evidence

From the metrics-watch timeline recorder in
`ferrosa-cluster/tests/cluster_formation.rs`, seed with two peers whose rafts
never come up:

```
+19401ms [ff..01] term=0 state=Learner   vote=Vote{ term:0,  node:0  } last_log=None
+19429ms [ff..01] term=1 state=Candidate vote=Vote{ term:1,  node:.. } last_log=Some(0)
   ... (no further transitions for the rest of the window) ...
```

Term reaches 1 via the `initialize()`-driven first election, then never moves.

## Mechanism (file:line)

1. `ferrosa-cluster/src/config.rs` defaulted `raft_enable_pre_vote = true`
   (ADR-012 intent).
2. The pinned openraft fork's tick election path **hard-gates**
   `engine.elect()` behind `run_pre_vote_round()` (fork
   `openraft/src/core/raft_core.rs`, the pre-vote round in the tick handler),
   which sends `RaftNetwork::pre_vote()` to every voter.
3. `FerrosRaftNetwork` (`ferrosa-cluster/src/raft/network.rs`) implements
   `RaftNetwork` (impl at line 250) overriding only `append_entries` (252) and
   `vote` (355). It does **not** override `pre_vote`, so the default trait impl
   returns an "unimplemented" `NetworkError`.
4. openraft counts that error as a **NO** vote. In any multi-voter cluster a
   pre-vote quorum is therefore structurally impossible — every candidate
   always loses its pre-vote round, so `elect()` is never reached and no term
   advances.
5. The seed reaches term 1 only because `initialize()` seeds the initial
   candidate state directly. After that, the election *timer* fires on schedule
   but the pre-vote gate suppresses every election. A transient first-round
   vote loss (e.g. a peer whose raft is not yet constructed) is then
   **permanent** instead of self-healing on the next timeout.

## Why debug logging hides it

Verbose `RUST_LOG=openraft=debug` does synchronous, formatted writes on the
raft hot path. That shifts the timing enough that peers' rafts are usually up
before the seed's first vote round, so the initial vote loss doesn't happen and
the stall doesn't reproduce (≈20/20 passes with debug logging vs ~3% failures
without). The tests deliberately use a metrics-watch timeline recorder instead
of debug logging so the election story is captured without perturbing the race.

## The fix

Default `raft_enable_pre_vote` to `false`
(`ferrosa-cluster/src/config.rs:131`) until the pre-vote network transport
exists. With the gate off, the fork's tick path calls `elect()` directly, so a
candidate that loses a round simply re-campaigns on the next election timeout
(term climbs 2, 3, …) and a transient vote loss self-heals.

The `FERROSA_RAFT_ENABLE_PRE_VOTE` env override still works both ways, so a
build that has implemented the transport can opt back in without a code change.

Regression guard: `candidate_re_campaigns_while_peers_are_down` in
`ferrosa-cluster/tests/cluster_formation.rs`. It starts only the seed, drives it
to a 3-voter `initialize()` with two peers that never come up, and asserts the
seed's `current_term` advances past 1 (election-timer liveness, not
leadership). It fails deterministically with pre-vote enabled and passes with it
disabled.

## What remains

- **Pre-vote transport (t_32cb5ad3)**: implement `FerrosRaftNetwork::pre_vote`
  (and the fork's `PreCandidate` engine path) so ADR-012's pre-vote can be
  turned back on. Until then pre-vote only subtracts liveness; do not re-enable
  the default.
- **Election-storm mitigation is unchanged**: the T1→T19 divergence-storm
  failure mode is still handled by the `election_guard` watchdog +
  `ELECTION_STORM_TERM_JUMPS_TOTAL` counter
  (`ferrosa-cluster/src/raft/election_guard.rs`), not by pre-vote.
- **LazyRaft vote-handler backoff**: the 3×5s vote-handler backoff window during
  formation still exists, but with elections firing again it is now
  self-healing via re-election rather than a permanent stall.
