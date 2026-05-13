# ADR-012: PreVote, CheckQuorum, and Leadership Transfer

> Date: 2026-05-09
> Status: Proposed
> Supersedes: implies migration of `bug-raft-stale-candidate-runaway-term-no-prevote.md` from in-process to a fix landing in Sprint 3
> Companion to: ADR-013 (Membership), ADR-018 (Fork openraft)

## Context

openraft 0.9 deliberately omits PreVote (Ongaro §9.6), CheckQuorum (Ongaro §6.4), and Leadership Transfer (§3.10). Its README lists "get rid of pre-vote RPC" as a goal; the substitute is a leader-lease state machine combined with `Vote` ordering. This substitute has documented cases where it does not preserve liveness:

1. **PreVote substitute insufficient under leases-expired healing.** A 30 s partition heals after every follower's lease lapses; a rejoining node's inflated `(committed, term, voted_for)` triple poisons the cluster. CockroachDB hit this (cockroach#92088), fix: enable both PreVote and CheckQuorum (PR #104042). The decentralizedthoughts.github.io 2020-12-12 post proves: **without both, Raft does not guarantee liveness under network omission faults**.
2. **CheckQuorum absence makes the leader zombie.** A leader without majority connectivity cannot commit, but does not voluntarily step down. Clients keep submitting writes that hang. We saw this in 2026-05-09 19:51 production logs: 114 AppendEntries failures + 521 reconnect events over 5 minutes.
3. **No `transfer_to(node_id)`.** The only API exposed is `raft.trigger().elect()` (force *this* node to start an election) — racing with the current leader's heartbeats and other followers' election timers. There is no `TimeoutNow` RPC.

Ferrosa today partially compensates with two bolt-on subsystems:
- `election_guard.rs` — detects election storms (term-jump bursts or 30 s rolling windows of Candidate state without log progress) and disables elections via `runtime_config().elect(false)` for 60 s.
- `snapshot_pusher.rs` — proactively triggers `InstallSnapshot` on followers more than 10 entries behind.

Neither is a substitute for the protocol-level fixes. They prevent symptoms (storms, wedged followers) from becoming outages, but they do not prevent the underlying causes (term inflation, zombie leader, manual leadership transfer). The known-and-open `bug-raft-stale-candidate-runaway-term-no-prevote.md` documents an unrecoverable end state that the bolt-ons cannot fix.

## Decision

Add **PreVote**, **CheckQuorum**, and **Leadership Transfer** to ferrosa's openraft fork (`github.com/ferrosadb/openraft`, branch `correctness/prevote-checkquorum`). Enable all three by default in ferrosa builds. **Retire** the bolt-ons (`election_guard`, `snapshot_pusher`) in Sprint 4, gated on a 2-week clean Jepsen window against the Sprint 3 build (zero `ELECTION_STORM_TERM_JUMPS_TOTAL` increments under any nemesis combination, runaway-term repro produces zero term advances).

### PreVote

Implement per Ongaro §9.6:

- New RPC `PreVoteRequest` distinct from `RequestVote`. New trait method `RaftNetwork::pre_vote(req)`.
- Voter answers based on the same election-restriction predicate as `RequestVote` (the candidate's `last_log_id` is at least as up-to-date), **plus** answers `false` if it has heard from the leader within the last election timeout (lease-aware).
- Term advances only after a successful pre-vote majority. Pre-vote rejection does **not** mutate `Vote` state.
- Election state machine:
  ```
  Follower --(election timeout)--> PreCandidate --(pre-vote majority)--> Candidate --(vote majority)--> Leader
                                  ^                                  |
                                  |--(pre-vote rejected)--<---rejected-|
  ```
- Interaction with openraft's `single-term-leader` cargo feature: PreVote semantics differ. **Decision: do not enable `single-term-leader`**. Document in ADR-018.

### CheckQuorum

Implement per Ongaro §6.4 / etcd's behavior:

- Leader periodically (every `heartbeat_interval`) checks: have I received an `AppendEntries` ack from a majority within the last `election_timeout × ratio` window?
- If no: leader transitions to Follower voluntarily, clears its `Vote.voted_for`, surrenders the lease.
- `ratio` configurable via `FERROSA_RAFT_CHECK_QUORUM_RATIO`, **default `0.75`** — earlier step-down than etcd's `1.0`. Rationale: ferrosa runs unusually long election timeouts (3000–6000 ms vs upstream 150–300 ms) to absorb sled disk-IO contention; a 1× ratio means up to 6 s of zombie-leader latency on writes before voluntary step-down. 0.75 cuts that to ~2.25–4.5 s without unnecessary churn under transient blips, and stays well above the heartbeat interval (300 ms) so single-RTT hiccups don't trigger step-down.
- New metric `RAFT_LEADER_VOLUNTARY_STEPDOWN_TOTAL` with reason label (`quorum_lost`, `transfer_initiated`, etc.).

### Leadership Transfer

Implement per Ongaro §3.10:

- New RPC `TimeoutNow { vote, last_log_id }` — instructs target follower to immediately start an election skipping the election timer.
- Leader-side: drain pending writes (no new `client_write` accepted during transfer); ensure target's `replication.matched_index` equals leader's `last_log_index` (catch up via AppendEntries); send `TimeoutNow`.
- Follower-side: on `TimeoutNow`, immediately call the same path as election-timer-fired; PreVote phase is skipped (transfer is by trusted leader directive).
- API: `raft.trigger().transfer_to(node_id).await -> Result<(), TransferError>`.
- Timeout: if target doesn't win within `election_timeout × 2`, leader resumes leadership and returns `TransferError::Timeout`.
- Operator command: `ferrosa-ctl raft transfer-leader --to <host_id>`.

### Interaction with the bolt-ons — retire after Sprint 3 verification

`election_guard` and `snapshot_pusher` were added because PreVote and CheckQuorum did not exist. With Sprint 3 they are redundant for their primary purposes:

- `election_guard` (storm detection + 60 s elect-suppression) becomes a no-op once PreVote prevents term inflation. Any storm event after Sprint 3 indicates a PreVote bug, not a runtime situation to suppress.
- `snapshot_pusher` (proactive InstallSnapshot to followers more than 10 entries behind) is partially still useful for the wiped-rebootstrap path (S-04 in `raft-failure-mode-matrix.md`) where the wiped node's term is below the leader's. After Leadership Transfer + clean re-election, this case becomes routine.

**Plan**:

- **Sprint 3** lands PreVote + CheckQuorum + Leadership Transfer. Both bolt-ons stay in the codebase but get a temporary deprecation marker.
- **Sprint 4** retires `election_guard` entirely — provided two prerequisites hold:
  1. Jepsen smoke + standard tiers run for ≥ 2 weeks against the Sprint 3 build with zero `ELECTION_STORM_TERM_JUMPS_TOTAL` increments under any nemesis combination.
  2. The runaway-term repro (`bug-raft-stale-candidate-runaway-term-no-prevote.md`) produces zero term advances.
  If either fails, `election_guard` stays and we file a PreVote bug instead of removing the safety net.
- **Sprint 4** also retires `snapshot_pusher`'s 10-entries-behind detector. The wiped-rebootstrap path (S-04) is handled by openraft's normal snapshot-on-log-inconsistency response. The "voter not in replication map" case (the original P0-20 motivation) is gone once Sprint 1's `MembershipChanger` ensures every voter is registered atomically.

The deletion lands as a single PR with the two prerequisites verified in the PR description. Old metrics names are kept (zeroed) for one release for downstream dashboards.

## Rationale

The combination is correctness-equivalent to the etcd / CockroachDB / TiKV defaults. Each component handles a distinct failure mode:

| Component | Fixes |
|---|---|
| PreVote | Disruptive elections after partition heal; runaway-term recovery (`bug-raft-stale-candidate-runaway-term-no-prevote.md`). |
| CheckQuorum | Zombie leader during asymmetric or partial partitions; long re-election windows. |
| Leadership Transfer | Graceful drains for DC-aware operations; multi-DC failover. |

**Honest caveat on today's outage**: PreVote alone would not have prevented today's membership-forwarding bug (which was state-drift, not election-related). PreVote+CheckQuorum *would* have turned the secondary 30 s election storm we observed in the 19:51 logs into a sub-second blip. So this ADR is not the cure for the F1 (two-maps drift) class — that is ADR-013 — but it is the cure for the F2 (election-storm) and F5 (open in-process bug) classes that produced 7 of the 38 fixes in the bug genome.

## Consequences

### Positive

- Liveness is restored under network omission faults (the decentralizedthoughts proof).
- The open P1 bug `bug-raft-stale-candidate-runaway-term-no-prevote.md` becomes unreproducible (Sprint 3 acceptance criterion).
- Multi-DC failover gains `transfer_to` as a primitive (Sprint 6).
- We get a fork to maintain (see ADR-018).

### Negative

- Forking openraft adds maintenance burden. The author has rejected PreVote in principle; we cannot upstream it. CheckQuorum and Leadership Transfer should be upstreamable.
- PreVote adds one round-trip to election; in healthy clusters this is invisible (election timeouts >> RTT). In partition-heal scenarios it adds latency before the term advances, which is the desired behavior.
- Maintenance: each openraft 0.x release we either rebase our fork or stay on a pinned version. Sprint 8 evaluates whether to upstream-merge into a future openraft 1.0.

### Neutral

- Bolt-on subsystems remain. They are cheap (tiny CPU overhead) and serve as production observability checks on the protocol-level fixes.

## Implementation effort

Per Agent D's analysis:

| Component | Effort | Upstream-mergeable |
|---|---|---|
| PreVote | Medium (~600–1500 LOC + tests) | No (author has declined) |
| CheckQuorum | Small-medium (~200–500 LOC + tests) | Yes |
| Leadership Transfer | Medium (~400–800 LOC + tests) | Yes |

Total Sprint 3 budget: 1 sprint (~10 working days) by one engineer plus review.

## Resolved decisions (2026-05-09 review)

1. **CheckQuorum ratio = 0.75** (not 1.0). Matches ferrosa's long election timeout (3000–6000 ms) — 0.75× gives ~2.25–4.5 s zombie-leader window before step-down, vs 6 s at 1.0. Above heartbeat interval (300 ms) so transient blips don't trigger.
2. **`single-term-leader` cargo feature: do not enable.** Confirmed. PreVote semantics are simpler under multi-term-leader (openraft default) and ferrosa's existing code assumes it.
3. **Bolt-on retirement: Sprint 4, gated on 2-week clean Jepsen window.** See "Interaction with the bolt-ons" above.

## Open questions (post-Sprint-3 telemetry-driven)

1. **CheckQuorum ratio adjustment.** If 0.75 produces unnecessary step-downs in production over the first 30 days, raise to 1.0. If zombie-leader windows still cause client-visible latency, lower to 0.5. Track via `RAFT_LEADER_VOLUNTARY_STEPDOWN_TOTAL` per-reason histogram.
2. **PreVote's interaction with manual `raft.trigger().elect()`.** Manual force-elect (e.g., `ferrosa-ctl raft elect-leader`) should skip PreVote — the operator is asserting they know better. Currently `runtime_config().elect()` does skip; document in the fork.

## Acceptance criteria (Sprint 3)

- [ ] Fork at `github.com/ferrosadb/openraft` exists with branch `correctness/prevote-checkquorum`. Baseline: openraft 0.9.24 + the existing `fix/separate-replication-timeout` patches.
- [ ] PreVote implemented; `cargo test -p openraft` passes; new test `prevote_rejects_isolated_candidate_term_inflation`.
- [ ] CheckQuorum implemented; new test `leader_steps_down_when_quorum_lost`.
- [ ] Leadership Transfer implemented; new test `transfer_leader_smoke`.
- [ ] ferrosa-cluster Cargo.toml repointed to fork; `cargo test --workspace` green.
- [ ] Repro of `bug-raft-stale-candidate-runaway-term-no-prevote.md` (60 s partition + reconnect) produces zero term advances on the partitioned node.
- [ ] Jepsen `dc-flap` nemesis on T3 produces zero `ELECTION_STORM_TERM_JUMPS_TOTAL` increments over a 1 h run.
- [ ] CockroachDB-style integration test: asymmetric partition causes leader to step down within `1 × election_timeout`.
- [ ] PRs filed upstream for CheckQuorum and Leadership Transfer.

## References

- Ongaro, "Consensus: Bridging Theory and Practice" (PhD dissertation, 2014), §3.10, §6.4, §9.6.
- Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (USENIX ATC 2014).
- decentralizedthoughts.github.io, "Raft does not Guarantee Liveness in the face of Network Faults" (2020-12-12).
- CockroachDB issue #92088, PR #104042.
- etcd PR #9352 (Raft Pre-Vote enablement).
- openraft discussion #15 ("get rid of pre-vote RPC").
- `specs/archive/bugs-verified/bug-raft-stale-candidate-runaway-term-no-prevote.md`.
- `specs/raft-correctness-plan.md` Sprint 3.
- `specs/raft-invariants.md` I-04, I-31.
- `specs/raft-failure-mode-matrix.md` S-15, S-19, S-20, S-21, S-27.
