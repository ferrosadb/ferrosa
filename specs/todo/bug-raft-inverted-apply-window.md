---
title: Inverted Raft apply window — committed can move to a lower index on a higher term
status: todo
created: 2026-09-21
updated: 2026-09-21
severity: P0
area: openraft fork (ferrosadb/openraft) + ferrosa-cluster
---

# Inverted Raft apply window

## Symptom

node1 and node2 of the local ferrosa-memory cluster logged:

```
core::io::error::Error: reversed Raft log range: start=17515, end=17454   (node1)
core::io::error::Error: reversed Raft log range: start=17465, end=17455   (node2)
```

### Timeline — read this before chasing it as a live fault

**These errors stopped on 2026-09-18T18:24:43** and have not recurred. The nodes
were restarted 2026-09-19 06:55 and the logs run to 2026-09-21T17:34 with no
further occurrence.

A `tail -c 100000000 | grep -c` over node2 counted ~45,985 hits and *looked*
live, but that byte window reaches back past Sep 18. **Counting occurrences in a
byte-bounded tail of an unrotated 680 MB log says nothing about recency.** Always
pull the timestamp on the last match.

An offline `ferrosa-ctl raft log-inspect` of node2 on 2026-09-21 found the log
entirely healthy:

```
vote:          term 1868
committed:     term 1868, index 18759
last_purged:   term 1772, index 17628
entries:       1131 total (1131 decodable, 0 unreadable)
index range:   17629 .. 18759
verdict:       all entries decode with this build
```

committed index == last log index; `last_purged` sits exactly one below the first
entry; 18759 − 17629 + 1 = 1131, so there are no gaps. The bug is real and worth
fixing — it took the cluster down for a day — but it is a **latent** defect, not
the cluster's current state.

## What this is NOT the cause of

Three things were initially attributed to this bug and do not survive measurement:

1. **Disk I/O saturation.** Node log write rate measured over 5s is 765 B/s
   *total* across all three nodes. The 3.3 GB/day figure is historical
   accumulation. The observed 1,455 MB/s at 15,000 tps was `mds_stores`
   (Spotlight) indexing the 83 GB data directory; as it settled from 157% CPU to
   12%, disk I/O fell to ~50-80 MB/s.
2. **Compaction starvation.** The `entity_store` SSTable directory count was
   static over a 6s sample (36,585 → 36,585). The 36,568-directory backlog is
   real and worth draining, but it is not being driven by this.
3. **The current leadership churn.** `CheckQuorum: leader has lost quorum
   contact, stepping down` was still firing three times in three seconds at
   17:32 with I/O already down to ~50 MB/s. Note `vote: term 1868` above — the
   cluster has held 1,868+ elections. That points at the runaway-term failure
   mode (`specs/in-process/bug-raft-stale-candidate-runaway-term-no-prevote.md`,
   which `ferrosa-ctl raft reset` documents itself as the recovery for), and it
   is a **separate investigation**.

What the full-text degradation *does* currently report, after a node2 restart, is
`fts_match search failed: ... net: lane is reconnecting; retry later` — a
distributed full-text query that needs a peer whose lane is still coming up.

## Root cause

`LogId` ordering is lexicographic on `(leader_id, index)` with **`leader_id`
first** (`openraft/src/log_id/mod.rs:23-32`). Under ferrosa's build
`CommittedLeaderId` is `leader_id_adv::LeaderId { term, node_id }` with derived
`Ord`, so **term outranks index**.

`RaftState::update_committed` (`raft_state/mod.rs:274-285`) accepts a new
committed whenever `committed > self.committed()`. Because term outranks index,
a committed log id from a **newer term at a lower index** compares greater and
is accepted.

`Command::Commit` then calls (`raft_core.rs:1974-1981`):

```rust
self.apply_to_state_machine(seq, already_committed.next_index(), upto.index).await?;
```

so `since = prev_committed.index + 1` (high) and `end = upto.index + 1` (low) —
inverted (`raft_core.rs:742-763`).

The restart interaction supplies the high `prev_committed`:
`StorageHelper::get_initial_state` raises committed to last_applied and seeds
`RaftState { committed: last_applied }` (`helper.rs:91-93`, `:158`). A node
restarts holding a high-index, old-term `last_applied`; a higher-term leader
then reports committed at a lower index (the node's uncommitted tail is
truncated); `update_committed` accepts it; the window inverts.

Three guards fail to catch it:

- `defensive.rs:29-32` sees `want_first > want_last`, classifies the inverted
  range as **empty**, and returns `Ok`. A genuine short read would be rejected
  by the `LogIndexNotFound` check at `:37-47`. That asymmetry is the defect.
- `raft_core.rs:759`'s `since == end` early return does not catch `since > end`.
- `raft_core.rs:752`'s `debug_assert!(since <= end)` is compiled out of release
  builds, which is what ferrosa ships.

Leaving `raft_core.rs:769`:

```rust
let last_applied = entries[entries.len() - 1].get_log_id().clone();
```

`0usize - 1` → `18446744073709551615` → the panic in `node1.err.log`.

## What is already correct — do not redo it

- `SledLogStore::try_get_log_entries` rejects the inverted range at the storage
  boundary (`log_store.rs:956`), with regression test
  `try_get_log_entries_reversed_range_fails_loud`. **The error in the logs is
  this guard working.** Per the standing order on silent failures it must not
  be weakened to quiet the symptom.
- `cfef680c` stops a purge running past what is durably applied.
- `local_state.rs` classifies `last_applied` against `last_purged` at startup.

### Rejected approach — recorded so it is not retried

Extending `classify_local_raft_state` to flag on-disk `last_applied > committed`
at startup **is wrong and dangerous**. `helper.rs:91-93` shows openraft
deliberately raises `committed` to `last_applied` on restart, so that on-disk
state is normal and self-healing. A startup check on it would fire on healthy
nodes and trigger a destructive reset. Tried and reverted 2026-09-21.

## The shipped binary does not have the existing fix

Two commits in the local `/Users/bkearns/src/openraft` clone already convert the
panic into a loud `StorageIOError`:

```
d9c06d30 fix(core): the panic is an inverted range, not a short read
430c57cd fix(core): a short log read must not underflow into a panic
```

Both sit **past** the pinned rev. `ferrosa/Cargo.toml:44` pins
`af87fa60bab5256cac6c08d09f84379e40636294`, and the vendored copy staged into
the container image —
`~/.ferrosa/images/vendor-stage/ferrosa/openraft-0.9.25+ferrosadb.1/src/core/raft_core.rs`
(mtime Aug 30, pre-fix) — has no inverted-range guard either. Until the pin is
advanced *and* the vendor stage refreshed, the shipped binary keeps panicking.

Note these two commits make it **fail loud**; they do not repair the ordering
asymmetry that generates the inversion.

## Acceptance criteria

### Tier 1 — ship the fix
**Upstream PR: https://github.com/ferrosadb/openraft/pull/2** — carries all three
commits (`430c57cd`, `d9c06d30` fail-loud; `b2500fd2` the root-cause fix in
`update_committed`). 239 lib + 77 consensus integration tests pass.

- [ ] Merge ferrosadb/openraft#2
- [ ] Advance the `openraft` rev in `ferrosa/Cargo.toml:44` past that merge
- [ ] Refresh the image vendor stage so the built binary carries it
- [ ] Test: a release build (not just debug) converts an inverted window into a
      `StorageIOError`, never a panic — the `debug_assert` gap is the reason
      this must be asserted in release

### Tier 2 — root cause: stop the inversion being generated
DONE in ferrosadb/openraft#2 (`b2500fd2`). Locus chosen: `update_committed`
rejects a committed-index regression. 7 tests including a property test that no
accepted commit can produce an inverted apply window.

- [x] Test: `update_committed` with a higher-term, lower-index log id does not
      move committed backwards in index
- [x] Test: the surrounding behaviour is pinned so the guard does not over-reach
- [ ] Still open: fix `defensive.rs:29-32` so an inverted range is an error
      rather than being classified "empty". `d9c06d30` handles it at the
      `raft_core` call site; `defensive.rs` itself is still asymmetric.

### Tier 3 — recover the two live nodes
- [ ] A repair that does not require `reset()`, or a documented decision that
      reset is correct here. The raft dir is the metadata plane only — the 83 GB
      of SSTables are separate — so reset is cheaper than it sounds. Twelve
      `raft.reset-*` backups across the three nodes show it is what has been
      done repeatedly.
- [ ] `SledLogStore::inspect` renders a marker-consistency verdict. Today it
      reports raw markers and decode health only, so a node in this state
      inspects as healthy: every entry decodes, `first_undecodable` is `None`.

## Implementation notes

(to be filled in)
