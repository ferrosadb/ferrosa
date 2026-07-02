# Multi-node streaming test harness + the real coordinated-paging fix

**Status:** proposed. Prereq for un-reverting the cluster projected-paged scan on
`feat/stream-range-reads-no-cap` (PR #237). Tracks `t_3fc6be3c`.

## Why this exists

The coordinated projected paged-scan fix was attempted twice (`36140808`,
`3ec30240`), passed **every in-process unit test + adversarial verify**, and
**failed on the live 3-node cluster all three times** — hang → node OOM → SSTable
corruption. Root cause of the *testing* failure: the in-process loopback harness
uses **parked** replicas on `127.0.0.1`. Real replicas **actively stream large
scans over a real network**, which is where the defects live:

1. The `RangeReadStreamCancel` never actually fired live (0 sent / 0 received).
2. The coordinator's per-replica receive buffers grow **unbounded** — an
   abandoned or slow page lets replicas flood `Lane::Bulk` → heartbeat starvation
   + OOM.

Parked-replica unit tests structurally cannot exercise either. We need a harness
with **real nodes, real IPs, real streaming** — never the user's live cluster.

**The same defect has a cleaner reproduction (`t_ee98faa0`).** On the *stable
reverted* cluster (so this is NOT the #237 code — it's pre-existing), a single
`hybrid_search` FTS query — `… context_snippet = fts_match(<terms>)` over
`entity_store` — OOM-kills the coordinator: one query → node1 `OOMKilled`, restart,
`total_candidates: 0`, repeat. `list` (plain partition read) never triggers it;
only the `fts_match` content scan does. Root: the **replica-side scan serializes
partitions to wire (`partition_to_wire` → bincode buffer) unbounded** against the
2 GB cap. This is the same unbounded-replica-scan-memory bug as (2), just triggered
by one FTS query instead of a multi-page paged scan — so it is the **primary RED
case** (deterministic, single query) and the real fix (Part C.2) closes both.

## Part A — the fly.io multi-node harness

Leverage `deploy/fly-bench/` (`ferrosa-main.Dockerfile`, `ferrosa-entrypoint.sh`,
`flyctl` present).

- **Provision:** `flyctl` deploys **N≥3 ferrosa nodes as separate fly machines**
  (distinct private IPs — proves address-agnosticism, unlike the loopback test),
  form an RF=3 cluster over the fly private network.
- **Seed:** load a table well past the page size (≥50k partitions, ≥3 pages), plus
  a graph table mirroring `agent_memory.typed_edges` for the viz case.
- **Probe suite (Part B) runs against the fly cluster, asserts, tears down.**
- **Isolation:** gated behind a `live-infra-tests` feature + a `FERROSA_TEST_FLY`
  env (like the existing Firecracker/container gates) so default CI never triggers
  it. Runs as a **manual/nightly** job, or on-demand for #237 — NOT the user's
  fmem-dev cluster, NOT the merge-blocking CI lane.
- **Cost/teardown:** machines destroyed on completion (`flyctl machine destroy`);
  fail-loud if teardown fails so we never leak billing.

## Part B — TDD matrix (each a RED that reproduces the live defect, then GREEN)

Every case has a hard wall-clock timeout so a hang/deadlock **fails** rather than
hanging the run.

0. **FTS content scan OOM (`t_ee98faa0`) — the primary, deterministic RED.** A
   single `SELECT … WHERE context_snippet = fts_match(<terms>)` over a large
   `entity_store` (mirroring hybrid_search) must return candidates with coordinator
   peak RSS **bounded** — no OOM. On today's code it kills the coordinator in one
   query. Cheapest, most reliable reproduction of the replica-side-scan memory blowup.
1. **Multi-page projected scan, RF>1, >10k:** `SELECT <cols>` returns *all* rows,
   terminates, peak node RSS bounded (independent of result size), no OOM.
2. **Abandoned page (client disconnect mid-scan):** coordinator cancels the remote
   producers within a bound; live-producer count returns to 0; node memory reclaims
   (no leak). This is the exact live-failure — must go RED on the reverted fix.
3. **Slow/backpressured consumer:** coordinator per-replica receive buffers stay
   bounded; a slow client cannot make a replica accumulate unboundedly.
4. **Viz `SnapshotStreamEnd` gap-close** (`t_dc729b1d`): drive `ws://…/viz/ws`;
   zero new `sequence gap/reorder` closes, no `observed_seq=5`, no partial-snapshot,
   probe reaches `SnapshotStreamEnd`. Filter the out-of-scope `relation_time` decode
   error. Revert-to-confirm-RED on the fly cluster.
5. **Data integrity across pages+replicas:** union of pages == full keyset, no
   gap/dup/miscount; `COUNT` == scan cardinality across all coordinators.
6. **ORDER BY spill under cancel** (`t_5cf8dc78`): cancel a spilling ORDER BY;
   temp-sort dir removed, no orphan runs.

## Part C — the real solution (only merged once Part B is green on fly)

1. **Cancel that actually fires + is promptly honored.** Fix why the guard sent 0
   cancels live; ensure a replica's scan loop **checks cancellation between chunks**
   (not just at partition boundaries) so it stops mid-large-scan. Verify the
   round-trip on real machines.
2. **Bounded replica-side scan memory + backpressure.** The replica scan/serialize
   path (`partition_to_wire` → bincode buffer) must **not** accumulate the whole
   result — stream + bound the in-flight serialized bytes. Every per-replica receive
   channel is a *bounded* mpsc; the replica producer **blocks on a full channel**
   (backpressure) instead of buffering unboundedly. This is the single fix that
   closes **both** the FTS-scan OOM (`t_ee98faa0`, case 0) and the paged-scan
   OOM/abandon (`t_3fc6be3c`, cases 1-3) — the parked-replica unit test could never
   force it.
3. Then, and only then, un-revert the cluster projected-paged arm (replace the
   `write_path.rs` fail-loud with the paging implementation), gated on Part B green.

## Sequencing

1. **Done:** branch made safe (fail-loud interim, `2de9097a`).
2. Build Part A (fly harness + provisioning script + a `#[cfg(feature=…)]` test
   entry that provisions → probes → tears down).
3. Write Part B RED cases against it; confirm they reproduce the live hang/OOM.
4. Implement Part C; iterate until Part B is fully green on fly.
5. Un-revert the cluster arm; re-validate on fly; only then consider #237 merge.

## Progress — root confirmed, faithful RED + fly scaffold landed (2026-07-01)

**Root of the OOM is CONFIRMED and pinned by an in-process test that drives the
REAL wire serialization (not a parked producer):**

- The replica **producer** (`coordinator::stream_request_handler::handle_stream_request`
  → chunked `emit_chunk` → `partition_to_wire` → bincode) is already bounded: it
  streams `chunk_size`/row-cap frames and awaits each `send` (backpressure via the
  lane actor's bounded mpsc + `reserve().await`).
- The coordinator's **Stream** API (`coordinate_range_read_stream_all_with`) is
  already bounded (guarded by `tests/range_scan_streaming_memory_bound.rs`).
- The **unbounded** path is the coordinator's `Vec<Partition>`-returning consume:
  `coordinator::stream_consumer::consume_range_stream` accumulates EVERY partition
  from EVERY replica into `StreamConsumeOutcome.partitions`, and
  `coordinator::range_read_stream::coordinate_range_read_stream_limited_rows` then
  does `all_partitions.extend(outcome.partitions)`. Peak = O(result). This is the
  path `WritePath::range_read` / the CQL SELECT-ALLOW-FILTERING / `fts_match`
  content-scan callers use — the single-query FTS OOM (`t_ee98faa0`) and the
  multi-page projected scan OOM (`t_3fc6be3c`) share it.

**Landed on `feat/stream-range-reads-no-cap`:**

- `ferrosa-cluster/tests/replica_scan_serialization_memory_bound.rs` — faithful
  RED: pipes the real producer's real bincode `RangeReadStream*` frames through a
  real bounded mpsc into the real `consume_range_stream`, and measures peak
  additional heap. Measured (row=4 KiB): consume peak **26 MB @ N=750 → 76 MB @
  N=12000** (grows with N, blows a 32 MiB in-flight budget) while the producer
  stays **flat at ~23 MB** regardless of N. Pinned as a characterization test
  (asserts the bug is present) so CI stays green; the fix FLIPS the two
  assertions to `< budget` / `< small*3` and renames to `..._is_bounded`.
- `deploy/fly-stream-scan/` — Part A scaffold: `provision.sh` / `seed.sh` /
  `probe.sh` (Part B cases 0–6, each under a hard wall-clock timeout + per-node
  2 GiB RSS assertion) / `teardown.sh` (fail-loud) / `run-all.sh` (dry-run by
  default; `--i-will-pay` to bill). `README.md` documents manual invocation.
  **Nothing runs `flyctl deploy` automatically.**
- `ferrosa-cluster/tests/fly_stream_scan_live.rs` — gated live entry behind
  feature `live-infra-tests` + `FERROSA_TEST_FLY=1`; panics loudly with setup
  instructions when the feature is on but the env/`flyctl` is missing (no false
  pass). Shells out to `run-all.sh --i-will-pay`.

**STOP taken (scope guard):** the fix (Part C.2) — converting `consume_range_stream`
+ `coordinate_range_read_stream_limited_rows` from `Vec<Partition>`-returning
accumulators to bounded-channel partition-at-a-time streams, and rewiring every
`WritePath::range_read` consumer (the CQL SELECT/ALLOW-FILTERING/FTS-content-scan
surface, `write_path.rs:791`) to consume the stream — is a cross-crate refactor
beyond a focused step. The faithful RED that proves the unbounded growth is
committed; the fix is the next unit of work. The 2 GiB cap is NEVER raised.

## The memory cap is intentional — never raise it

The 2 GB node `mem_limit` is a **deliberate forcing function**: it makes any
unbounded allocation OOM *early and loudly* instead of hiding behind slack RAM
until it blows up at production scale. **Do not raise it** (not on fly, not on
fmem-dev). The FTS/paged-scan OOMs are the cap working as designed. The pass
condition for every case here is **"completes under the real cap"** — bounded
memory is the fix; more RAM is not. (Any doc/task that lists "raise the mem_limit"
as a mitigation is wrong and should be struck.)

## Non-goals

- Not on the user's live fmem-dev cluster.
- Not in the merge-blocking CI lane (fly provisioning is slow + costs money).
- Does not change the other #237 wins (aggregates, DISTINCT, COUNT, single-node
  ORDER BY spill, uncapping the working shapes) — those are cluster-proven.
