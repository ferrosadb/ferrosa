# Compaction exploration after PR #601

This suite measures scheduled-request latency, compaction work, and recovery
after writes stop. It is a diagnostic campaign, not a Cassandra comparison or
an automatic strategy controller. Use a dedicated, disposable keyspace and a
separate generator. Never point it at production.

Measured evidence: [pilot](RESULTS.md) and
[sustained workloads, fan-in, read-ahead, and manifest audit](NEXT-RESULTS.md).

## Run and verify

Unit tests use only Python's standard library:

```sh
python3 -m unittest \
  tests.ci.test_compaction_experiment_analysis \
  tests.ci.test_compaction_experiment_workload \
  tests.ci.test_compaction_experiment_runner \
  tests.ci.test_compaction_fanin_matrix \
  tests.ci.test_compaction_lineage
```

For offline lineage evidence, run
`python3 deploy/fly-compaction-explore/lineage.py /path/to/cluster.log`.
It prints JSON with per-node/table promotion counts, maximum fully observed
generation depth, unknown-ancestry counts, and line-numbered diagnostics (exit
status 1 when diagnostics exist). Only a start, its declared distinct inputs,
and a local `/compaction/` promotion qualify. Known ancestry follows node,
table, and generation across publication into `/sstables/` when the ID remains
the same. The engine can rename generations at publication; a nested
`/sstables/table/gen/gen-Data.db` input with no observed mapping is unknown
ancestry, not a fresh flush. Never infer the mapping from adjacent IDs or timing.
Unseen compaction ancestry and its descendants stay unknown. This does not prove S3 upload or
manifest completion. Capture order and the current Fly text event format are
required; exact replay is deduplicated in memory, and overlapping tasks on a
node/table quarantine that pair for the rest of the capture because these logs
lack task IDs. Truncated or malformed evidence is diagnosed, not repaired.
Exit zero alone is not a coverage gate: also require the intended tables and
observed completed work; an empty capture cannot establish compaction coverage.
Verify with `python3 -m unittest tests.ci.test_compaction_lineage`.

The live runner additionally requires `cassandra-driver==3.29.2`. Run it on a
generator in the same region as the database:

```sh
python run.py --hosts NODE1_IPV6,NODE2_IPV6,NODE3_IPV6 \
  --ca-file /private/cluster-cert.pem --output results/point-low \
  --keyspace cx_point_low --keys 4096 --value-bytes 65536 \
  --rows-per-partition 1 --tables 1 --rate 300 --seconds 120 \
  --read-fraction 0.5 --concurrency 32 --preload-rate 100 --drain-seconds 60
```

The keyspace must be new. The runner uses RF=3, QUORUM reads and writes,
explicit STCS, TLS certificate verification, and no driver retries. It preloads
every readable key, interleaves a deterministic count of reads and writes,
continues reads while writes drain, then verifies every key. Payloads are
deterministic SHAKE-256 bytes, avoiding unrealistic compression of padding.
Repeated writes to a key use the same payload: this checks data integrity and
missing rows, **not** last-write-wins ordering, TTL, tombstone safety, or repair.

Raw JSONL includes every operation, errors and mismatches, scheduled-response
latency, service time, dispatch lag, sequence number, and wall-clock timestamps
anchored to the phase's monotonic clock. Admission is bounded by concurrency;
an overloaded generator retains the original schedule and exposes delay instead
of dropping requests. Summaries fail on missing operations or bad results.
Per-operation and combined percentiles use nearest ranks of raw samples, never
an average of percentiles. Collectors retain each node's Prometheus metrics and
per-table storage statistics. A failed collector makes the campaign incomplete.

## Live matrix

Use fresh keyspaces and output directories for each cell. Keep node settings
fixed across workload cells; record every intentional setting change.

| Shape | Keys | Value bytes | Rows/partition | Tables | Purpose |
|---|---:|---:|---:|---:|---|
| Point | 4096 | 65536 | 1 | 1 | Large selected values, flush/merge/verify cost |
| Wide | 4096 | 16384 | 1024 | 1 | Repeated row-head scans and wide-partition reads |
| Many tables | 4096 | 16384 | 1 | 16 | Fairness and shared compaction admission |

These are pilot sizes, not sufficient stress by themselves. The 64 MiB wide
fixture produced only one compaction per node in the initial run. For a
stronger test, use 16,384 keys for a 256 MiB working set, and choose the
many-table size/flush threshold so each table's working set substantially
exceeds its flush threshold. For example, 16 tables with 16 MiB each and an
explicitly recorded 4 MiB flush threshold give a four-to-one ratio. Keep this
diagnostic threshold change separate from baseline comparisons. Gate any
compaction conclusion on observed completed work and debt recovery; successful
client operations alone do not prove compaction was exercised.

Start with a small smoke run, then a rate ladder and read-only drain. Record
actual achieved rates and dispatch lag; a configured rate is not achieved load.
Short diagnostic runs can identify failures but cannot establish sustainable
capacity. For comparisons, run at least five randomized repeats, extend duration
through multiple merge generations, and size the working set beyond memory.
Use `--preload-rate` to keep the all-write setup phase below saturation; by
default it uses `--rate`, which doubles write pressure relative to a 50/50 phase.

## Fresh-process allocation probe matrix

Build the existing Rust probe from the revision being tested, then use the
stdlib wrapper:

```sh
cargo build --release -p ferrosa-storage --example compaction_allocation_probe
python deploy/fly-compaction-explore/fanin.py \
  --binary target/release/examples/compaction_allocation_probe \
  --output /tmp/compaction-fanin-candidate --repetitions 5 --seed 601
```

The wrapper randomizes 4/16/32/64-way fan-in cases and starts a fresh process
for every sample. One family holds total input rows at 262,144; the other holds
rows per input at 4,096. Each sample must pass the existing Rust probe's own
correctness validation and match the requested shape. Raw output, errors,
binary hash, invocation order and allocation/time summaries are retained.
Run the same matrix on separately built baseline/candidate binaries; randomize
the outer binary order for comparative trials.

This uses the existing overlapping-key fixture, 64-byte values, one worker,
and the probe's counting `System` allocator. It does not measure production
RSS, physical I/O, static columns, or disjoint keys. In the fixed-total family,
output row count falls as fan-in rises; it is not a pure isolation of the
head-selection cost. Add a disjoint-key fixture before deciding whether a heap
would improve selection. The generic `analyze.py probe` interface expects
separately supplied byte measurements; `fanin.py` consumes the existing Rust
schema directly and does not invent unavailable physical byte counts.

## Follow-on experiments and decision rules

1. **Selection progress:** characterize 4 × 128/129/200 MiB and 31/32/33 files
   using real selectors, then reproduce through the engine. A cap must not
   strand eligible debt indefinitely, and every strategy must obey admission.
2. **Fan-in scaling:** extend #601's fresh-process allocation probe to
   4/16/32/64 inputs with overlapping and disjoint partition/row keys. Hold
   total input rows/bytes constant as well as doing a fixed-per-input sweep.
   Compare time per input row, allocations, production-allocator RSS, and
   physical I/O. This distinguishes linear head scans from simply more work.
3. **Verification and compression:** randomize verification on/off and LZ4/
   uncompressed runs. Keep correctness readback on in every cell. Compare
   phase timers and device bytes before proposing fused digest/verification
   work; turning checks off is not itself an optimization recommendation.
4. **Shared memory budget:** sweep 1/2/4 workers, large cells, static columns,
   and multiple hot tables on the same performance-2x hardware. Sample RSS,
   memory/CPU/I/O pressure, cancellation latency, and foreground p99.9.
5. **Disk/S3 recovery:** on disposable storage only, inject bounded low-space
   and upload-delay conditions. Identify input sets across cancellation and
   retries. Repeated work with little completed output is evidence for
   cooldowns/admission fixes; a rising queue alone is not proof of thrashing.
6. **TTL and late writes:** use actual TTLs and supplied timestamps, restart
   and repair, and assert no resurrection. Do not label a TWCS option effective
   until the server exposes or verifies which strategy actually ran.

Prefer fixing measured progress/admission defects and specific hot phases before
automatic strategy switching. A future controller should first run in shadow
mode and use hysteresis, resource headroom, and per-table age fairness. Do not
tune tombstone safety settings from latency signals.

## Fly reuse and evidence

The 2026-10-23 pilot reuses the three stopped `maas-loadtest-control` machines
in LAX (2 performance CPUs, 4 GiB RAM each) with isolated data, Raft, and S3
paths. Original machine configurations contain secrets: keep their backups
private and outside git. Preserve the original volume contents, restore the
original image/configuration, and verify all three machines are stopped when
finished. Destroy only the generator created for this campaign. Do not deploy
app-wide secrets or touch `maas-loadtest-cassandra` during another baseline run.
Reserve exclusive use of the machines or use a separate app: a baseline cleanup
stopped these shared machines during the pilot. Capture Fly stop/crash events
and exclude externally interrupted cases from performance conclusions.

Pin the source revision, CI artifact, image digest, machine resources, runtime
settings, driver version, and complete CLI arguments. Keep raw evidence outside
git; check in only a compact report and sanitized manifest. Compaction output/
input is a reduction ratio, not physical write amplification. Queue depth does
not include every unselected eligible SSTable; inspect per-table file counts
and sizes as well.

Before load, recreate any cleared backing buckets, verify S3 PUT/GET/readback,
and verify real server-uploaded objects from **each node** after a small CQL
smoke test. Readiness and `upload_bytes_total` alone do not prove successful
remote persistence: the current upload counters also include failed attempts.
