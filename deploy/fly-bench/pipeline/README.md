# Pipeline performance experiments

[Measurement status for PR #613](RESULTS.md): the candidate now routes eligible
graph property equality through CQL-declared secondary indexes, counts physical
vertices through metadata and compares integer keys exactly. Fresh timings for
that code are pending; older tables are explicitly labelled as a baseline.
Cypher index DDL remains unsupported.

The current-results page contains comparator tables, the exact engine revision
and workload limits. [Experiment details and earlier checkpoints](HISTORY.md)
retain the measurement journey. The instructions below describe the harness.

This instrument validates exact row values, counts and key sets before accepting
timings. It measures PostgreSQL inserts and point reads, and Bolt point updates
and reads against a preloaded `BenchRow` fixture. A result identifies the source
revision and image supplied by the controller; independently record the running
binary checksum and image digest with the collector.

Use Python 3.11+ with `psycopg[binary]` and `neo4j`. Record installed package
versions and use the same client environment for both arms. Arm labels A–E identify experiment candidates; source revision and immutable image identity are the authoritative implementation metadata. Contract tests need
only the standard library:

```sh
python3 -m unittest discover -s deploy/fly-bench/pipeline -p 'test_*.py'
python3 deploy/fly-bench/pipeline/workload.py --help
```

`validated_multimetric_bench.py` supplies the common insert/count/point-read/scan
workload for all four front ends. CQL pins native protocol v4 with compression
off. Bolt requires `--bolt-backend ferrosa|neo4j`: both fixtures declare indexes
on `pk`, `bucket` and `payload`; Neo4j must report all three RANGE indexes ONLINE
with the correct label/property definitions. Ferrosa declares indexes through
CQL and uses UUID `id` as its native key, so regular `pk` reads exercise 2i.
Use `--cql-port`, `--cql-user` and `--cql-password` when that setup needs them.

SPARQL requires `--sparql-backend ferrosa|oxigraph`. Each logical record is three
triples in the isolated graph `urn:ferrosa:benchmark`; payloads, keys and buckets
match the other fixtures. One persistent HTTP connection per worker excludes
per-query TCP setup. Count returns one scalar over the records, and reads/scans
validate values. Oxigraph should use a persistent `--location`, as described in
its [pinned CLI documentation](https://github.com/oxigraph/oxigraph/blob/v0.5.11/cli/README.md).
Neither these small workloads nor equal request shapes establish durability,
large-dataset, traversal or multi-node equivalence.

```sh
python3 deploy/fly-bench/pipeline/validated_multimetric_bench.py \
  --engine sparql --sparql-backend ferrosa --host "$HOST" --port 8080 \
  --stages 5000 --concurrency 8 --read-samples 100 --out /tmp/sparql.json
```

Provide connection credentials in a private, mode-0600 JSON file, outside this
repository and the evidence directory. It maps `ferrosa`, `postgres` or `neo4j`
to `host`, `user`, `password`, and `pg_port` or `bolt_port`. The workload accepts
its path through `--runtime-config` and omits that path and its contents from the
result. Use a dedicated benchmark database: the PostgreSQL timing leg creates
and clears its own table. Bolt requires a separately loaded and validated
`BenchRow` fixture with `pk`, `bucket=pk%64`, and the 512 deterministic 96-byte
payload variants from `make_payloads(512)`.

```sh
python3 deploy/fly-bench/pipeline/workload.py \
  --runtime-config /run/private/benchmark.json \
  --arm A --round 1 --protocol pg --revision "$SOURCE_SHA" \
  --image "$IMAGE_DIGEST" --rows 5000 --concurrency 8 --reads 400 \
  --out results/A1-pg.json
```

Run A1/B1/B2/A2 on the same machine, volume, server configuration, client
environment and fixture size. Each PostgreSQL timing leg uses a fresh scoped
table. Warm each read worker before opening the timed barrier. Keep profiling
separate from the timing comparison. Use the same workload revision for both
arms, including disjoint per-worker keys in write profiling.

On a Linux server image containing `perf`, Python, `sysstat`, `procps`, `curl`
and the Ferrosa binary at `/usr/local/bin/ferrosa`, collect resource evidence:

```sh
sh deploy/fly-bench/pipeline/collect_software_cpu.sh /tmp/capture 300 stats
# In a separate phase, use profile instead of stats for 49 Hz user CPU stacks.
```

The collector creates `ready`; touch `/tmp/capture/stop` after the workload and
wait for `done`. It records CPU, RSS, faults, scheduling/context switches,
disk/network counters, pressure, process I/O, metrics, binary checksum and
mount mapping. `profile` adds `perf.data` and stack text for flamegraphs.
Scheduling counters and pressure do not identify individual mutex wait times;
RSS is not an allocation count. Match disk devices to mounts before attributing
traffic. Archive, download and checksum captures before stopping machines.

The workload and collector operate on machines already started by a controller.
The controller must stop every machine it starts in a `finally`/EXIT cleanup,
including on a failed leg, and verify stopped state. Volume deletion is a
separate operation. These scripts do not provision or delete infrastructure.

Software sampling caused 11–28% overhead in the recorded rig; sampled timings
are diagnostic only. Hardware PMU tracing distorted this VM much more and is
excluded from the accepted comparisons. See [RESULTS.md](RESULTS.md) for the
measured scope and limitations.
