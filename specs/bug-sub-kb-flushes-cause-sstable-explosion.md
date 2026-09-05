---
title: Sub-kilobyte flushes produce an sstable per write, and reads pay for it forever
status: fix landed 2026-09-05, not yet verified against a live cluster
severity: P1 — sustained ~95% CPU on two of three nodes, indefinitely
reported: 2026-09-01
reported-by: observed on the ferrosa-memory dev cluster (~/data/ferrosa-memory)
component: ferrosa-storage (flush + compaction)
---

# Sub-kilobyte flushes produce an sstable per write

> **A fix landed on main the same day this report was filed here.** `1ad286c8`
> "perf(storage): bound SSTable flush amplification" (PR #382) gates age-based
> flushes by data volume and retained WAL pressure, drains compaction backlogs
> through bounded queues, and exposes high read fanout — which is this report's
> first and third investigation directions respectively. It touches
> `engine.rs`, `store.rs`, `compaction/executor.rs` and `metrics.rs`.
>
> **It has NOT been verified against the cluster this was observed on.** The
> 20,040 sstable directories and the two nodes at ~95% CPU were measured before
> the fix existed; nobody has re-measured after. Until someone does, treat the
> mechanism as addressed and the outcome as unconfirmed — those are different
> claims, and the second is the one that matters to an operator.
>
> Related signal, taken 2026-09-05 on the live dev cluster: a `stats` call
> costing 4.6 seconds, which is the read-side fanout this describes.

## The symptom

Two of three nodes of the ferrosa-memory cluster sit at **~95% CPU
indefinitely** while producing almost no log output — about 2 lines a minute.
CPU that produces nothing observable is what prompted the investigation.

`ps -M` puts 74.8% of it on a single thread in state `R` with **22 minutes of
user time against 45 seconds of system time**, so it is computation rather than
syscalls. `sample` names the thread `data-rt` and the frames underneath it are
all one thing:

```
ferrosa_sstable::reader::ChunkedCompressedData::read_exact_at
ferrosa_sstable::compression::Compression::decompress
ferrosa_sstable::data::DataReader::read_cell
ferrosa_sstable::data::DataReader::read_row
ferrosa_sstable::marshal::value_length_if_fixed
```

Reading and decompressing sstables, continuously.

## The cause

The flushes are tiny. Over the last 300 flushes on node1:

```
count=300  mean=4,257 bytes
<1KB: 197    1–64KB: 101    >64KB: 2
```

**Two thirds of flushes are under one kilobyte**, and each one becomes an
sstable. The result:

| table | live data | sstables | mean sstable |
|---|---|---|---|
| `agent_memory.entity_store` | 580 MB | **22,966** | ~25 KB |
| `agent_memory.context_segments` | 130 MB | 8,743 | ~15 KB |
| `agent_memory.consolidation_requests` | 12 MB | 2,844 | ~4 KB |

20,040 sstable directories across the store. A healthy LSM at these data sizes
would hold tens, not tens of thousands.

The count is **stable, not growing** — compaction is running and keeping pace
with new flushes. It is not collapsing the existing backlog, so the steady
state is permanent rather than transient.

## Why it burns CPU rather than merely wasting disk

Every read consults every sstable that could hold the key. A full scan of
`consolidation_requests` opens and decompresses ~2,844 files to read 12 MB.
The memory server polls that table roughly **six times a minute** — one scan
every ten seconds — so the same thousands of files are decompressed
continuously. The work is real; the answer is 12 MB either way.

That is the CPU: not a loop, not an election storm (checked — zero
`greater log id` or vote-change lines in the last 2,000 log lines on either hot
node), but per-read overhead multiplied by a file count three orders of
magnitude too high.

## What to look at

1. **What triggers a flush.** A mean of 4 KB suggests flushes are driven by
   something other than memtable size — a timer, or a per-write or per-commit
   path. A size threshold in the megabytes would fold three orders of magnitude
   out of the file count on its own.
2. **Whether compaction has a minimum-input rule.** Keeping pace with new
   flushes while never reducing a 22,966-file backlog suggests it compacts what
   arrives rather than what has accumulated. Something should treat "this table
   has twenty thousand sstables" as work to do.
3. **Read-side fan-out.** Even correctly sized, a read touching thousands of
   candidates wants a bound, and a warning when it is exceeded — this is
   exactly the shape that produced no signal for days.

## Also worth fixing on the caller

Polling a table every ten seconds with `ALLOW FILTERING` is a full scan by
construction and would be expensive even against a healthy store. That belongs
to `ferrosa-memory` and is filed separately; it is an amplifier here, not the
cause.

## Reproducing

The evidence above is from the running dev cluster and needs no special setup:

```bash
ls -1 ~/data/ferrosa-memory/node1/sstables/agent_memory.entity_store | wc -l
sed 's/\x1b\[[0-9;]*m//g' ~/.ferrosa/logs/node1.out.log \
  | grep -oE 'data_bytes=[0-9]+' | tail -300
```

A regression test wants to assert the invariant directly: after writing N rows
in M batches, the sstable count is bounded by something related to data volume
rather than to the number of batches.
