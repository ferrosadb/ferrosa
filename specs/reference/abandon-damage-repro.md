# Abandon-leaves-damage: the canonical reproducer

> Last updated: 2026-10-12
> Script: `scripts/abandon-damage-repro.sh` (one command, prints a PASS/FAIL verdict)
> Grounding: `commit-path-ground-truth.md` (G1, the experiment result, U1), `commit-path-refactor-plan.md`

## What it is for

The ground-truth note's decisive next probe was:

```
BEGIN; COPY 3 rows; <force the COMMIT to fail>;   →   point-lookup a known key, assert ABSENT
```

at a size where the storage state is fully enumerable, instead of inferring the behaviour from a
million-row table. `scripts/abandon-damage-repro.sh` is that probe, runnable in one command against
the live 3-node cluster.

## Running it

```
scripts/abandon-damage-repro.sh            # ~3.5 min, prints a PASS/FAIL verdict
```

It needs the `fly` CLI, authenticated, and the bench cluster named at the top of the script. Every
runner call is short and all long work is launched detached and polled from a file, because
`fly machine exec` has a ~10 s deadline of its own.

## What it measures

The invariant is **`TxnAbandoned` must leave no readable row**. The client contract ("NOT committed;
safe to retry") is a lie if any row of the abandoned transaction is readable afterwards, because a
retry can then double-apply.

Three instrument rules are baked into the script, each of them a trap this rig has already produced:

- **Never a `COUNT`.** A partial apply preserves a count and drops the tail, so a count cannot
  distinguish "the coordinator wrote its share" from "the whole load landed". Every check is a point
  lookup of a known key.
- **Always the PER-NODE union.** The read path serves a node's local store; a node that is not a
  replica of a key answers empty (rc=0, no value) *without error*. Reporting a per-node read as
  "absent" would be wrong — the script reads every key on node1, node2 and node3 and reports which
  nodes return it, so a partial read is visible as such instead of looking like loss.
- **Never trust a client exit code alone.** This front end refuses a multi-statement `-c` and the
  runner's `psql` returns 0 even when a statement fails unless `ON_ERROR_STOP` is set; the script
  sets it *and* greps the client text.

## Guards (both fail loud, neither returns a verdict)

| Guard | Condition | Verdict |
| --- | --- | --- |
| Cold cluster | any control read matches `read timeout … data_present=false` (the named signature) | `INADMISSIBLE`, exit 3 |
| Cold cluster | the control write does not commit, or a control key is not served by all three nodes | `INADMISSIBLE`, exit 3 |
| Leftover client | `psql`/`pgbench` alive on the runner before or after the run | `INADMISSIBLE`, exit 3 |

## The mechanism used (named), and the ones refuted

**Used: a whole-transaction write-set whose Apply fan-out cannot reach quorum inside the operator's
10 s bound.** This is the same mechanism that produced the recorded `pgbench -i --scale 10` abandon
(a single-transaction bulk load; fact #1 of the brief) — it is reproduced here at **three keys**, so
the storage state is fully enumerable. The bound is a deliberate detector and is not raised.

**Refuted — SIGSTOP of the remote engines (mechanism (i)).** The premise is that "the commit phase
completes before the apply fan-out, so stopping the two remote engines isolates the apply stage".
It does not hold on this rig. Measured:

| freeze | client result | time | rows left behind |
| --- | --- | --- | --- |
| both remote engines (node2 + node3) | `58000: transaction commit failed: Storage(InvalidData("Accord quorum unavailable"))` | ~2.7 s | none — keys 101/102/103 absent on all three nodes |
| one remote engine (node2 only) | identical `Accord quorum unavailable` | 2.7 s | none — same keys absent on all three nodes |
| one remote engine (node3 only) | identical `Accord quorum unavailable` | ~2 s | (control insert, no row expected) |

So a frozen peer fails the **Commit** consensus, not the Apply fan-out: the transaction never reaches
the stage the abandon is produced from. The 2.7 s rejection is a different bound from the 10 s
Apply bound. Freezing is therefore not a way to force the case, **and it is not harmless**: SIGSTOP
wedged the stopped node's inter-node lanes (`lane is reconnecting; retry later`, `Raft lane timeout`),
leaving node1 unable to PreAccept to either peer. Recovering the cluster required recycling all
three machines. Mechanism (ii) — stopping the machines mid-copy — is a strictly stronger version of
the same freeze and cannot do better; it was not needed.

## Size sweep (why three keys, not three bytes)

Same 3-key transaction, one row per key, varying only the value size:

| write-set | result |
| --- | --- |
| 3 rows × 8 MB (24 MB) | COMMIT clean, no abandon |
| 3 rows × 40 MB (120 MB) | **COMMIT ABANDONED** — `40001: transaction was abandoned and NOT committed: … exceeded the 10s dependency-wait bound … safe to retry` |
| 300 000 rows × ~14 B (4.3 MB) | clean |
| 1 000 000 rows × ~14 B (14.8 MB) | clean |

The bound responds to the **write-set's byte volume**, not to the key count: a million small keys
(14.8 MB) commit fine while three large keys (120 MB) trip it. That is what makes an enumerable
3-key case possible at a size that reproduces.

**Smallest reproducing size measured: 3 rows × 40 MB = 120 MB.** 3 rows × 8 MB (24 MB) does not
reproduce; the threshold was not bisected further, so the smallest reproducing write-set lies in
`(24 MB, 120 MB]`. The reproducing figure is comparable to the recorded `pgbench -i --scale 10`
write-set (~110 MB), which is why the same bound trips there.

## Results

`scripts/abandon-damage-repro.sh` is committed with a 3-row × 40 MB forced-failure case, a per-node
point-lookup union of all three keys, the cold-cluster guard and the leftover-client guard.

**Two consecutive runs, byte-for-byte the same verdict** (2026-10-12, 3-node fly cluster
`maas-loadtest-control`, deployed image `pgwire-77cdf369`):

```
19:21:30 run: BEGIN; COPY 1 2 3 (40000000 B each); COMMIT
  write_set_bytes: 120000009
  txn_rc=3 elapsed=209s
  client: psql:/tmp/ar.txn.sql:7: ERROR:  transaction was abandoned and NOT committed: invalid data:
          abandoned: transaction exceeded the 10s dependency-wait bound and was rolled back
          (NOT committed); safe to retry
19:25:04   the COMMIT was ABANDONED (the forced failure landed)
  key 1 node1: rc=0|      key 1 node2: rc=0|      key 1 node3: rc=0|
  key 2 node1: rc=0|      key 2 node2: rc=0|2     key 2 node3: rc=0|
  key 3 node1: rc=0|      key 3 node2: rc=0|      key 3 node3: rc=0|3

VERDICT: FAIL  -- DAMAGE REPRODUCED
  the client was told the transaction was NOT committed and was safe to retry,
  yet these rows are readable now:
    key 2 node2: rc=0|2
    key 3 node3: rc=0|3
  per-node union is non-empty: a retry would double-apply.
```

Run 1 (`19:16:42`) produced the identical verdict, keys and per-node placement. So the defect is
**reproducible on demand at three keys** on the deployed build, and the two readable rows are the two
*remote* shards — the coordinator's own shard (key 1) is the one that does **not** survive, which is
the same shape the deployed build's commit path produces: the coordinator's local apply is joined
with the remote fan-out and is lost when the fan-out abandons.

**Deployed-build gap.** The image the reproducer was measured against (`pgwire-77cdf369`) is one
commit behind this branch on the paths under test; the undelivered commit is
`c085d5ad fix(accord): decide the apply ack in one journal entry point` (R0 of the refactor plan).
The reproducer is the R0 gate: run it before and after deploying that commit — the expected
transition is `FAIL` → `PASS`.

**Guard evidence.**

| Guard | How it was exercised | Observed |
| --- | --- | --- |
| Machine not started | `fly machine stop` node2, then run the script | `VERDICT: INADMISSIBLE -- not all control nodes are 'started' -- refuse to measure`, exit 3 |
| Control read not served by every node | same run's earlier attempt, immediately after a node restart (cluster not settled) | `guard_read node1: rc=124` (hung), node2/node3 `rc=0\|CTRL` → `INADMISSIBLE`, exit 3 |
| Leftover client | a hung `psql` from an earlier probe was still alive on the runner | `VERDICT: INADMISSIBLE -- a client is left over on the runner`, exit 3 |
| Cold-cluster signature `read timeout … data_present=false` | predicate verified against the recorded signature; **not** induced live | `grep -E 'read timeout.*data_present=false'` fires on the string in `commit-path-ground-truth.md` |

The last row is a limit, stated plainly: the named `data_present=false` signature is a *silent node*
condition that cannot be induced without damaging the cluster, so that one predicate is verified on
the recorded string rather than by crashing a live node. Every other guard fired live.


## Limits

- The per-node reads can only report *what a node returns*; a node that answers empty without error
  may either not own the key or have lost it. The union separates "short single-node scan" from
  "loss", and a node that answers with the dangling-read error is reported separately — it does not
  distinguish an absent fragment from an unreachable one (U1 in the ground truth).
- A point lookup on a *large* table is served as a scan on this front end and can hang; the reproducer
  avoids that by keeping the table at three rows.
