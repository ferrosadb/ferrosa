# Fly LAX Cost/Value/Speed Benchmark

This harness is for a repeatable comparison:

- Ferrosa: 3 data nodes, 2 GB RAM each, S3-compatible Tigris backend.
- NoSQLBench: 1 separate client node in LAX.
- Cassandra baseline: 3 data nodes with persistent Fly volumes, sized so it is not memory-starved.

Default benchmark workload is NoSQLBench NB5 `activities/baselines/cql_iot.yaml default`, using the built-in CQL/DataStax Java driver workload. Each repeat runs schema/truncate, rampup, then the measured main phase, and leaves result tarballs on the benchmark node for `fly ssh sftp get`.

Ferrosa is built from `origin/main` by default. The harness creates a clean `git archive` from `BENCH_GIT_REF` and records the exact source ref and SHA under `target/fly-bench/<run-id>/`. Only the Fly entrypoint wrapper is overlaid into the image so Machines can map Fly/Tigris environment variables and private IPv6 addresses at runtime.

## Current State

The `ferrosa` Fly org, `ferrosa-lax` Tigris bucket, three Ferrosa Machines, and one NoSQLBench Machine have been provisioned in `lax`. Cassandra is created only when the baseline is ready to run, and should be destroyed after metrics are collected.

## Intended Defaults

| Target | Fly app | Region | Machine | Storage |
| --- | --- | --- | --- | --- |
| Ferrosa | `ferrosa-lax` | `lax` | 3 x shared CPU, 2 GB RAM | Tigris + local cache |
| NoSQLBench | `ferrosa-bench-lax` | `lax` | 8 x performance CPU, 32 GB RAM | ephemeral results |
| Cassandra | `ferrosa-cassandra-lax` | `lax` | 3 x performance, 8 GB RAM | 50 GB volume per node |

The Cassandra system is intentionally larger so the baseline is not just a heap-starvation result. Ferrosa stays at 2 GB to test the low-end object-backed value hypothesis.

## Workload

The default run is:

```bash
WORKLOAD=activities/baselines/cql_iot.yaml
SCENARIO=default
WARMUP_CYCLES=5000000
MEASURE_CYCLES=50000000
REPEATS=5
THREADS=256
RF=3
READ_CL=LOCAL_QUORUM
WRITE_CL=LOCAL_QUORUM
NB_JAVA_MAX_HEAP=24g
```

Increase `MEASURE_CYCLES` when the loaded dataset does not exceed the Ferrosa local cache or Cassandra memory/OS page cache.

For Ferrosa ramp tests, use `run-ferrosa-ramp`. It switches to
`/usr/local/share/nosqlbench/cql_iot_append.yaml`, a local copy of the IoT
workload that does not truncate between stages, and runs increasing stages:
16/1k, 32/10k, 64/100k, 128/1M, and 256/5M threads/main-cycles.
