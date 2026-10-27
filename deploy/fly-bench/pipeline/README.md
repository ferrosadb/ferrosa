# Pipeline performance experiments

[Measurement status](RESULTS.md): PR #625 combines automatic Bolt scalar schema
and index setup, streaming CQL LIMIT pages, and protocol transaction/RDF fixes.
Current matched-VM comparisons are recorded in RESULTS.md and current-results.json;
historical tables retain their measured runtime identity. Explicit Cypher index DDL remains unsupported.

Old and new binaries must use independent writable data and object-store
fixtures. The new transaction WAL envelope and RDF v2 cutover are not safe
cross-version fixture interchange formats.

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
off. Bolt requires `--bolt-backend ferrosa|neo4j`. Neo4j must report RANGE indexes
on `pk`, `bucket` and `payload` ONLINE with their correct label/property definitions.
Ferrosa creates only the keyspace through CQL, then sends ordinary Bolt CREATE
and deletes its owned sentinel. Table and scalar-index setup must be automatic;
the fixture must not predeclare them. A generated UUID `id` remains the native
key, so regular `pk` reads exercise secondary indexes. Use a fresh benchmark
keyspace/database; existing rows are not erased. `--cql-port`, `--cql-user` and
`--cql-password` control keyspace bootstrap. Scans project and validate all three
fields, just like point reads.

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


The current comparison uses the portable probes below (install `psycopg[binary]`,
`neo4j` and `cassandra-driver` in the same client environment for both products).
`protocol_probe.py` config entries name targets such as `ferrosa_pg`/`postgres`,
`ferrosa_cql`/`cassandra`, or `ferrosa_sparql`/`oxigraph`. A fresh `--scope` owns
its benchmark table; SPARQL owns and clears only its benchmark graph. These
commands write fixture data and belong on a dedicated benchmark database.

```sh
python3 deploy/fly-bench/pipeline/protocol_probe.py \
  --config /run/private/protocols.json --target ferrosa_pg --protocol pg \
  --scope bench_repro --rows 5000 --reads 20000 --write-concurrency 1 \
  --out /tmp/ferrosa-pg.json
python3 deploy/fly-bench/pipeline/bolt_probe.py \
  --runtime-config /run/private/benchmark.json --target ferrosa \
  --action measure --reads 20000 --serial-reads 500 --out /tmp/ferrosa-bolt.json
```

For CQL use 20,000 rows/eight writers; for SPARQL use 2,000 records/eight writers.
Bolt first needs `--action seed` for each of `--label BenchRow` and
`--label BenchWrite`, and `--action access` records index/count access proof.
Run native/Ferrosa/Ferrosa/native twice, with no concurrent benchmark work.
`resources_low_overhead.sh` is the one-process 1 Hz OS collector used by this run;
CPU stack profiling is a separate phase. `--profile` in the protocol probe
selects a 30-second duration loop; CPU profiler attachment belongs to the controller.

The [count/scan ownership audit](SCAN_COUNTS.md) records the three follow-up
changes, their regression evidence, the observed scan improvement and the
remaining SQL/SPARQL count and predicate-access gaps.

`profile_operation_probe.py` runs the count/scan diagnostic loops used for the
current CPU captures. It shares the existing adapters and deterministic payloads;
`--profile` is required. Use the same existing fixture scope and configure CPU
profiler attachment separately after the ready gate:

```sh
python3 deploy/fly-bench/pipeline/profile_operation_probe.py \
  --config /path/to/benchmark-config.json --target ferrosa_pg --protocol pg \
  --scope existing_fixture --rows 5000 --profile --diagnostic-operation count \
  --out /tmp/count.json --ready-file /tmp/count.ready --go-file /tmp/count.go
# Start the process CPU recorder after /tmp/count.ready, then touch /tmp/count.go.
# Use --diagnostic-operation scan for a complete key/value-validated scan loop.
```

Each diagnostic loop has eight workers and a 30-second deadline. Full scan
validation consumes client CPU; these rates are excluded from primary tables.
