# Observability Architecture — Internals Instrumentation

> Created: 2026-04-03
> Status: Draft
> Scope: End-to-end distributed tracing, metrics, contention/bottleneck instrumentation
> References: CMU-PDL-14-102 (Sambasivan et al.), Dynatrace Developer's Guide to Observability

---

## Design Decisions

Following CMU-PDL-14-102's four design axes:

| Axis | Choice | Rationale |
|------|--------|-----------|
| **Causal relationships** | Trigger-preserving (shows critical path) | Diagnosis use case — "why is this query slow?" |
| **Metadata propagation** | Hybrid fixed-width (trace_id + span_id) | Constant-size, works with OTel, less brittle than static |
| **Sampling** | Head-based coherent, 100% in dev, 1% in prod | Coherent sampling required for trigger causality; configurable per-node |
| **Visualization** | Gantt charts (Jaeger) + flow graphs (Grafana Tempo) | Gantt for individual traces, flow graphs for steady-state diagnosis |

## Stack

```
Application Code
    ↓ #[instrument] spans + tracing::info_span!
tracing crate (subscriber)
    ↓ tracing-opentelemetry layer
opentelemetry SDK (BatchSpanProcessor)
    ↓ OTLP gRPC exporter
Jaeger / Grafana Tempo / OTEL Collector
```

**Crate additions:**
- `tracing-opentelemetry` — bridge tracing spans to OTel
- `opentelemetry` + `opentelemetry-otlp` — OTLP export
- `opentelemetry_sdk` — batch processor, sampling config
- `metrics` crate (optional) — for histogram/counter if we outgrow atomics

**Configuration (env vars):**
- `FERROSA_OTEL_ENDPOINT` — OTLP gRPC endpoint (default: disabled)
- `FERROSA_OTEL_SAMPLE_RATE` — sampling ratio 0.0-1.0 (default: 1.0)
- `FERROSA_OTEL_SERVICE_NAME` — service name (default: "ferrosa")

---

## Instrumentation Points

### Layer 1: CQL Request Lifecycle (business metrics, long queries)

| Span | Location | Attributes | Why |
|------|----------|------------|-----|
| `cql.request` | `server.rs` accept loop | `cql.opcode`, `cql.keyspace`, `client.address` | Top-level request span — every CQL frame gets one |
| `cql.parse` | `parser.rs` entry | `cql.statement_type`, `cql.table` | Parser latency, identifies complex queries |
| `cql.route` | `router.rs` route dispatch | `cql.consistency_level`, `cql.rf` | Routing decision latency |
| `cql.execute` | `router.rs` per-statement handler | `cql.rows_returned`, `cql.rows_scanned` | Execution time — the "business metric" |
| `cql.prepared` | `prepared.rs` cache lookup | `cql.cache_hit` | Prepared statement cache effectiveness |

**Slow query detection:** Log at WARN when `cql.execute` span exceeds configurable threshold (default 1s). Include full query text at DEBUG level.

### Layer 2: Cluster Coordination (contention, consensus)

| Span | Location | Attributes | Why |
|------|----------|------------|-----|
| `cluster.write` | `coordinator/write.rs` | `cl`, `rf`, `replicas`, `acks` | Write fan-out latency + quorum wait |
| `cluster.read` | `coordinator/read.rs` | `cl`, `digest_match`, `repair_needed` | Read coordination + digest comparison |
| `cluster.read_repair` | `coordinator/read.rs` | `stale_replicas`, `repair_success` | Read repair latency (now inline) |
| `accord.txn` | `accord/coordinator.rs` | `txn_id`, `path` (fast/slow), `shards` | Full Accord transaction lifecycle |
| `accord.preaccept` | `accord/coordinator.rs` | `ballot`, `deps_count` | PreAccept round latency |
| `accord.commit` | `accord/coordinator.rs` | `commit_type` | Commit decision latency |
| `raft.propose` | `ddl_path.rs` | `op_type`, `leader_id` | DDL through Raft latency |
| `raft.forward` | `ddl_path.rs` | `leader_id` | Non-leader DDL forwarding latency |

### Layer 3: Storage Engine (I/O bottlenecks, backpressure)

| Span | Location | Attributes | Why |
|------|----------|------------|-----|
| `storage.write` | `engine.rs` write | `table`, `key_size`, `row_count` | Per-write storage latency |
| `storage.read` | `engine.rs` read | `table`, `memtable_hit`, `sstable_count` | Read path breakdown |
| `storage.flush` | `flush.rs` | `table`, `bytes_flushed`, `duration_ms` | Memtable flush duration |
| `storage.compaction` | `compaction/` | `input_sstables`, `output_bytes`, `strategy` | Compaction duration + I/O |
| `storage.s3_upload` | `upload/` | `component`, `bytes`, `retry_count` | S3 upload latency + retries |
| `commitlog.write` | `commitlog/` | `segment_id`, `entry_bytes` | Commit log write latency |
| `commitlog.sync` | `commitlog/` | `sync_strategy`, `entries_batched` | fsync latency (critical for durability) |

### Layer 4: Network / Internode (queue depths, backpressure)

| Span | Location | Attributes | Why |
|------|----------|------------|-----|
| `net.rpc` | `rpc/client.rs` send | `peer`, `msg_type`, `lane`, `bytes` | RPC round-trip latency |
| `net.handshake` | `handshake.rs` | `peer`, `protocol_version` | Connection establishment time |
| `net.pool` | `pool.rs` | `peer`, `lane`, `active_rpcs` | Connection pool utilization |

### Layer 5: Metrics (counters + histograms via atomics)

Extend the existing Prometheus metrics with:

| Metric | Type | Labels | Why |
|--------|------|--------|-----|
| `ferrosa_cql_requests_total` | counter | `opcode`, `keyspace`, `status` | Request rate by type |
| `ferrosa_cql_request_duration_seconds` | histogram | `opcode`, `keyspace` | Latency distribution (p50/p95/p99) |
| `ferrosa_cql_slow_queries_total` | counter | `keyspace` | Slow query count |
| `ferrosa_cql_connections_active` | gauge | — | Current CQL connection count |
| `ferrosa_cluster_write_duration_seconds` | histogram | `cl`, `status` | Coordinator write latency |
| `ferrosa_cluster_read_duration_seconds` | histogram | `cl`, `status` | Coordinator read latency |
| `ferrosa_storage_flush_duration_seconds` | histogram | `table` | Flush latency distribution |
| `ferrosa_storage_compaction_duration_seconds` | histogram | `strategy` | Compaction latency |
| `ferrosa_storage_s3_upload_duration_seconds` | histogram | — | S3 upload latency |
| `ferrosa_storage_memtable_bytes` | gauge | `table` | Current memtable size |
| `ferrosa_storage_sstable_count` | gauge | `table` | SSTable count per table |
| `ferrosa_net_rpc_duration_seconds` | histogram | `msg_type`, `peer` | RPC latency per message type |
| `ferrosa_net_rpc_inflight` | gauge | `peer` | In-flight RPCs per peer |
| `ferrosa_net_bytes_sent_total` | counter | `peer`, `lane` | Network egress |
| `ferrosa_net_bytes_received_total` | counter | `peer`, `lane` | Network ingress |
| `ferrosa_commitlog_sync_duration_seconds` | histogram | `strategy` | fsync latency |
| `ferrosa_raft_proposal_duration_seconds` | histogram | `op_type` | Raft proposal latency |
| `ferrosa_mode_transitions_total` | counter | `from`, `to` | Mode transition count |

### Layer 6: Contention-Specific Instrumentation

| Point | Location | What to capture | Why |
|-------|----------|-----------------|-----|
| Memtable shard lock | `memtable/` | Contention count per shard, wait time | Identifies hot partitions |
| ArcSwap loads | `write_path.rs`, `ddl_path.rs` | Load count (sampled) | Mode transition frequency |
| transition_guard | `peer_events.rs` | Hold duration, contention count | Serialized mode transitions |
| Raft log store | `raft/log_store.rs` | Write latency, batch size | sled I/O bottleneck |
| S3 upload queue | `upload/` | Queue depth, drain rate | Backpressure indicator |
| Hint store | `hints/` | Pending hints per peer, replay rate | Divergence indicator |
| Commit log segments | `commitlog/` | Active segments, closed-pending-upload | Archive lag |

---

## Context Propagation

Per CMU-PDL-14-102 Section 4, use hybrid fixed-width metadata:

1. **Within a node:** `tracing::Span` context propagates via `tracing::Instrument` and task-local storage
2. **Across nodes (internode RPC):** Inject `trace_id` + `span_id` into internode message headers (new 32-byte optional header field after the existing 12-byte frame header)
3. **Across nodes (CQL):** Extract trace flag from CQL v5 custom payload (driver-initiated tracing)

The internode propagation requires a wire format change:
- Add optional `trace_context: Option<[u8; 32]>` to the codec frame
- When present: 16-byte trace_id + 8-byte span_id + 8-byte flags
- Backward compatible: old nodes ignore the extension

---

## Sprint Plan

### O1: Foundation (tracing + OTel export)

| # | Task | Size | Description |
|---|------|------|-------------|
| O1.1 | Add OTel dependencies to workspace | S | `tracing-opentelemetry`, `opentelemetry`, `opentelemetry-otlp`, `opentelemetry_sdk` |
| O1.2 | Configure OTel subscriber in main.rs | M | Conditional OTLP layer when `FERROSA_OTEL_ENDPOINT` set, with sampling config |
| O1.3 | Instrument CQL request lifecycle | M | `#[instrument]` on server accept, parse, route, execute |
| O1.4 | Slow query logging | S | Threshold-based WARN log with query text when execution > 1s |
| O1.5 | CQL request metrics (counter + histogram) | M | `ferrosa_cql_requests_total`, `ferrosa_cql_request_duration_seconds` |

### O2: Cluster + Storage Spans

| # | Task | Size | Description |
|---|------|------|-------------|
| O2.1 | Instrument coordinator write/read | M | Spans on `coordinate_write_with`, `coordinate_read_with`, `coordinate_write_nts`, `coordinate_read_nts` |
| O2.2 | Instrument Accord transaction lifecycle | M | Spans on PreAccept, Accept, Commit, Execute phases |
| O2.3 | Instrument storage engine | M | Spans on write, read, flush, compaction |
| O2.4 | Instrument commit log | S | Spans on write_entry, sync (fsync timing critical) |
| O2.5 | Storage + compaction metrics | M | Histograms for flush/compaction duration, gauges for memtable/SSTable sizes |

### O3: Network + Contention

| # | Task | Size | Description |
|---|------|------|-------------|
| O3.1 | Instrument internode RPC | M | Span on send/receive with peer, msg_type, bytes, latency |
| O3.2 | Trace context propagation across nodes | L | 32-byte trace header in internode frames, inject/extract |
| O3.3 | Contention metrics | M | Memtable shard contention, transition guard hold time, S3 upload queue depth |
| O3.4 | Network bandwidth metrics | S | `ferrosa_net_bytes_sent_total`, `ferrosa_net_bytes_received_total` by peer + lane |
| O3.5 | In-flight RPC gauge | S | `ferrosa_net_rpc_inflight` per peer |

### O4: Dashboard + Alerts

| # | Task | Size | Description |
|---|------|------|-------------|
| O4.1 | Grafana dashboard JSON | M | Pre-built dashboard with CQL latency, cluster write/read, storage I/O, network panels |
| O4.2 | Alert rules | S | Slow query rate > threshold, S3 upload queue > 100, hint store growing, compaction falling behind |
| O4.3 | Web console integration | M | Expose OTel trace links in `ferrosa-ctl` and web console |

---

## Design Principles

1. **Always-on, low overhead:** Spans are cheap when not exported. OTel export is optional (env var). Target <1% overhead with 1% sampling.
2. **Trigger-preserving causality:** Every span shows the critical path of the triggering request, not background work attributed to it.
3. **Contention visibility first:** Prioritize instrumenting lock contention, queue depths, and backpressure over exhaustive function tracing.
4. **Coherent sampling:** Head-based, decided at CQL request entry. All spans within a sampled request are captured.
5. **Backward-compatible wire format:** Trace context in internode frames is optional; old nodes ignore it.
