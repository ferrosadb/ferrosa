# Fly multi-node streaming-scan memory harness

Scaffold for the Part A/B harness from
[`specs/proposed/multi-node-streaming-test-harness.md`](../../specs/proposed/multi-node-streaming-test-harness.md).

Purpose: provision **N ≥ 3 real ferrosa nodes as separate fly machines** (distinct
private IPs — proves address-agnosticism, unlike the loopback unit tests), form an
RF=3 cluster over the fly private network, seed a large table, run the FTS / paged /
viz probes, and assert every node stays **under the intentional 2 GiB `mem_limit`**
while a full `fts_match` content scan and multi-page projected scans complete.

## HARD CONSTRAINT — never raise the 2 GiB `mem_limit`

The 2 GiB per-node cap is a deliberate forcing function (see the spec). The pass
condition is *"the scan completes under the real cap"* — bounded memory is the fix,
more RAM is not. These scripts set `--vm-memory 2048` and MUST NOT be edited to raise
it. A probe that OOMs is the cap doing its job.

## This is SCAFFOLD — it does not deploy

**Nothing here runs `flyctl deploy` / `flyctl machine run` automatically.** Autonomous
provisioning bills money, so the deploy step is behind an explicit human action. The
scripts print exactly what they *would* run and stop, unless you pass `--i-will-pay`
(see below). The in-process faithful memory-bound test
(`ferrosa-cluster/tests/replica_scan_serialization_memory_bound.rs`) is what validates
the fix in CI; this harness is the on-demand live confirmation for PR #237.

## Files

| File | Role |
| --- | --- |
| `provision.sh` | Create the fly app + N ferrosa machines (RF=3), 2 GiB each. Dry-run by default. |
| `seed.sh` | Load ≥50k partitions (≥3 pages) into `entity_store` + a `typed_edges` graph table for the viz case, over CQL on a provisioned node. |
| `probe.sh` | Run the Part B probe suite (FTS content scan, multi-page projected scan, abandoned-page cancel, slow consumer, viz `SnapshotStreamEnd`, integrity, ORDER-BY-spill cancel) and assert per-node RSS < 2 GiB throughout. |
| `teardown.sh` | `flyctl machine destroy` every machine + app. Fail-loud if any destroy fails so billing never leaks. |
| `run-all.sh` | Orchestrates provision → seed → probe → teardown. Dry-run by default. |
| `config.env` | Tunables (node count, region, app name, table sizes). |

## Manual invocation

```bash
cd deploy/fly-stream-scan

# 1. Review what would run (dry-run, no billing):
./run-all.sh

# 2. Provision + seed + probe + teardown for real (bills money — explicit opt-in):
./run-all.sh --i-will-pay

# Or step-by-step:
./provision.sh --i-will-pay
./seed.sh
./probe.sh            # exits non-zero if any node exceeds 2 GiB or a probe hangs
./teardown.sh         # always run this; teardown failure is fail-loud
```

## Gated live-infra test entry

`ferrosa-cluster/tests/fly_stream_scan_live.rs` holds the gated Rust entry that WOULD
drive this harness. It is behind:

- crate feature `live-infra-tests`, and
- env `FERROSA_TEST_FLY=1`.

Per the repo test policy, with the feature enabled but `FERROSA_TEST_FLY` unset (or
`flyctl` missing) it `panic!`s with setup instructions rather than silently passing.
It shells out to `run-all.sh --i-will-pay`; it never provisions on its own in CI.

```bash
FERROSA_TEST_FLY=1 cargo test -p ferrosa-cluster --features live-infra-tests \
  --test fly_stream_scan_live -- --nocapture
```
