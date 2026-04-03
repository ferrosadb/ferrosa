# FMEA — Observability Subsystem

> Date: 2026-04-02
> Scope: Failure modes in the observability architecture — telemetry layer, profiling, sampling, billing, alerting
> Reference: specs/observability-architecture.md

## FMEA Table (sorted by RPN descending)

| ID | Component | Failure Mode | Effect | Sev | Occ | Det | RPN |
|----|-----------|-------------|--------|-----|-----|-----|-----|
| OF1 | FerrosaTelemetryLayer | Self-write feedback loop — telemetry CQL writes generate new spans, which are written, recursively | Exponential span amplification. Bounded channel fills in milliseconds. Node CPU saturated by telemetry writes. Data path blocked if channel uses blocking send. | 9 | 7 | 8 | **504** |
| OF2 | FerrosaTelemetryLayer | CQL endpoint down — telemetry writes fail | All spans dropped silently (design says best-effort). No observability data for the outage period — the time when telemetry is most needed. If retry logic exists, backlog grows unbounded. | 5 | 6 | 3 | **90** |
| OF3 | Flame profiling | OOM during profiling window — high request rate generates millions of span-close writes to BufWriter | BufWriter<Vec<u8>> grows unbounded. At 10k req/sec with 5 spans/req over 30 seconds = 1.5M folded stack lines. Memory exhaustion kills the node. | 10 | 4 | 7 | **280** |
| OF4 | Sampling config | 0% sample rate in prod — `FERROSA_TELEMETRY_SAMPLE_RATE=0.0` | Zero spans captured. All virtual tables empty. Slow query detection disabled. Alerts never fire. Operators blind to issues. No error or warning logged. | 7 | 3 | 9 | **189** |
| OF5 | Sampling config | 100% sample rate in prod — `FERROSA_TELEMETRY_SAMPLE_RATE=1.0` (dev default leaks to prod) | Every request generates full span tree. At 50k req/sec: 250k spans/sec written to `system_observability.spans`. Telemetry consumes >30% of node I/O. Compaction falls behind on observability keyspace. | 7 | 5 | 4 | **140** |
| OF6 | Query fingerprints | Ring buffer overflow — top 10k fingerprints exceeded by diverse query workload | Eviction of low-frequency fingerprints. Optimization advisor loses visibility into tail queries. If eviction is LRU, intermittent queries are never captured. If eviction is by count, new query patterns are invisible. | 4 | 5 | 6 | **120** |
| OF7 | Partition hotspot tracker | Sampling bias — top-k min-heap with 1% sampling misses actual hotspots | False positives: random partitions appear hot due to sampling variance. False negatives: actual hotspot sampled below threshold. Advisor recommends unnecessary partition key changes. | 5 | 6 | 7 | **210** |
| OF8 | Metrics rollup task | Rollup falls behind — compaction or high write rate delays background task | Raw metrics accumulate beyond 7-day TTL window. Storage grows unbounded for `system_observability.metrics`. TTL tombstones pile up. Eventually, observability keyspace compaction competes with user keyspace compaction. | 6 | 4 | 5 | **120** |
| OF9 | Alert evaluator | False alert — threshold rule fires on transient spike or metrics lag | Operator alert fatigue. False alerts desensitize team. Real alerts ignored. If alerts trigger automated remediation (future), false alerts cause unnecessary failovers. | 6 | 5 | 4 | **120** |
| OF10 | Billing counters | Counter reset on node restart — in-memory counters not persisted | Usage gap in billing data. Customer undercharged for the lost period. `ferrosa-dbaas` aggregator sees missing buckets but cannot reconstruct usage. Revenue leakage proportional to restart frequency. | 8 | 4 | 6 | **192** |
| OF11 | Trace context propagation | Old node ignores 32-byte header — mixed-version cluster | Cross-node traces broken at old node boundary. Spans from old nodes appear as orphaned root spans. Trace visualization shows disconnected fragments. No error — old node silently drops the extension. | 4 | 6 | 8 | **192** |
| OF12 | Alert evaluator | Alert evaluator itself crashes — unhandled error in threshold evaluation | All alerting silently stops. No meta-alert for "alerting is down." Operators unaware until they manually check. | 8 | 2 | 9 | **144** |

## Critical Findings (RPN >= 150)

### CRITICAL-1: Self-Write Feedback Loop (OF1, RPN=504)

The highest-risk item by far. `FerrosaTelemetryLayer` writes spans to ferrosa via CQL. Those CQL writes traverse the instrumented code path (`cql.request` -> `cql.parse` -> `cql.execute`), generating new spans, which are batched and written again. Without an explicit circuit breaker, this is a guaranteed infinite loop.

**Required fix:** The telemetry `CqlClient` must bypass the tracing subscriber entirely. Options:
1. Use a separate `tracing::Subscriber` with no `FerrosaTelemetryLayer` for the telemetry write task.
2. Tag telemetry-origin spans with a `is_telemetry=true` field and filter them in the layer's `on_close`.
3. Use `tracing::dispatcher::with_default()` to run telemetry writes under a no-op dispatcher.

Option 3 is simplest and most robust.

### CRITICAL-2: Flame Profiling OOM (OF3, RPN=280)

`FlameLayer` writes a folded stack line per span close into a `BufWriter<Vec<u8>>`. Under production load (10k+ req/sec), a 30-second profiling window generates millions of lines. `Vec<u8>` has no size limit.

**Required fix:** (1) Cap BufWriter at 64 MB — drop span lines beyond the cap. (2) Hard-limit `seconds` parameter to 60. (3) Monitor memory usage during profiling; abort if RSS increase exceeds 128 MB.

### CRITICAL-3: Partition Hotspot Tracker Bias (OF7, RPN=210)

At 1% sampling, a partition receiving 1% of traffic looks identical to uniform distribution. The min-heap tracker needs at least 10x the sampling rate relative to the hotspot threshold to achieve statistical significance.

**Required fix:** (1) Use count-min sketch instead of naive sampling for frequency estimation. (2) Require minimum sample count (e.g., 1000) before reporting a partition as hot. (3) Document confidence intervals in advisor recommendations.

### CRITICAL-4: Billing Counter Loss on Restart (OF10, RPN=192)

In-memory counters for `client_usage` reset to zero on process restart. The 1-minute bucket granularity means up to 60 seconds of billing data is lost per restart.

**Required fix:** (1) Flush billing counters to commit log every 10 seconds (not just on 1-minute bucket boundary). (2) On startup, replay commit log to recover partial-bucket counters. (3) `ferrosa-dbaas` aggregator flags missing buckets and interpolates from coordinator-side counters.

### CRITICAL-5: Trace Context Propagation in Mixed Clusters (OF11, RPN=192)

During rolling upgrades, old nodes silently drop the 32-byte trace context extension. This is by design (backward compatible), but the effect is that trace stitching fails silently — no error, no warning, just broken traces.

**Required fix:** (1) Log WARN on first trace context drop per peer. (2) Expose `ferrosa_trace_context_drops_total` counter per peer. (3) Web UI: show "trace incomplete — N nodes lack trace support" when rendering partial traces.

## Detection Improvements

| ID | Current Detection | Proposed Improvement |
|----|------------------|---------------------|
| OF1 | None — silent amplification | Span counter per flush cycle; alert if self-referential spans detected |
| OF2 | None — silent drop | `ferrosa_telemetry_drops_total` counter; alert if >0 for 60s |
| OF3 | OOM killer (too late) | Memory watermark check every 1s during profiling; abort at 64 MB |
| OF4 | None — silently empty tables | Startup warning if sample rate is 0.0; periodic check that spans table has recent rows |
| OF5 | Operator notices high I/O | Startup warning if sample rate >0.1 and node is not in dev mode |
| OF7 | None — bad recommendations | Confidence score on hotspot reports; suppress below threshold |
| OF10 | Missing billing buckets in dbaas | Flush-before-shutdown hook; commit log recovery on startup |
| OF11 | None — silently broken traces | Per-peer trace context support flag; UI warning on partial traces |
| OF12 | None — silent evaluator death | Watchdog: if alert evaluator hasn't run in 2x its interval, log CRITICAL |
