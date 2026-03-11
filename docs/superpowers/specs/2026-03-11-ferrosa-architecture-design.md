# Ferrosa Architecture Design

A Rust reimplementation of Apache Cassandra with S3-backed storage, designed for
cloud-native deployment with ephemeral compute and durable object storage.

## Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Deployment target | AWS-first, flag any lock-in for future portability | Start concrete, stay portable |
| Storage model | Write-behind async S3 | Minimal write latency impact, quorum mitigates data loss window |
| Rust strategy | Independent crates + Java as behavioral oracle | Clean Rust-native code, not a Java transliteration |
| SSTable compatibility | Layered: read Big+BTI, write BTI, future native format | Smooth migration path + room to innovate |
| Cluster protocol | CQL client compat, Ferrosa's own internode protocol | Apps work unchanged, clean internal design |
| Consensus | Raft for metadata, tunable consistency levels, transactions deferred | Proven Rust libraries + Cassandra CL semantics preserved |
| Java phase | Analysis only, not a deliverable | Understand the system deeply to avoid inheriting unnecessary complexity |
| Test infrastructure | Sprites (Firecracker VMs) + fly.io, <$50/month | Fast spin-up/kill for chaos testing, on-demand to control cost |

### AWS Lock-in Flags

These decisions are AWS-first but must remain portable to S3-compatible stores (MinIO, etc.):

- **S3 object metadata** (`x-amz-meta-*`): Standard across S3-compatible stores. No lock-in.
- **S3 client library**: Start with `aws-sdk-s3` (works with MinIO via endpoint override). Add trait abstraction for `object_store` crate (Apache Arrow) if broader backend support needed.
- **S3 conditional writes** (`If-None-Match`, Nov 2024): Not required for write-behind model. If used in future native format, flag as portability concern — MinIO supports them, other stores may not.

---

## Two Parallel Tracks

### Track 1: Java Analysis & Characterization (Informs Rust, Not a Deliverable)

| Step | Activity | Output | Feeds Into |
|------|----------|--------|------------|
| 1a | DSM analysis of Cassandra source | Module map, dependency cycles, dead code list | Crate boundaries |
| 1b | Behavioral characterization | What Cassandra does at each CL, during compaction, repair, failure/recovery — edge cases not just happy paths | Rust test suite + implementation |
| 1c | "What we wouldn't do" ADR | Unnecessary complexity: legacy shims, dead code paths, obsolete perf hacks, over-abstraction | Scope reduction for Rust |
| 1d | SSTable format deep dive | Byte-level spec of Big and BTI formats from source + real SSTables | ferrosa-sstable crate |
| 1e | CQL protocol spec | All CQL native protocol v5 messages, framing, error codes, type serialization | ferrosa-cql crate |

Tools for DSM: JDepend, Lattix, Structure101, or custom analysis via javap + dependency graphs.

### Track 2: Rust Implementation (Independent Crates)

Build order with testable milestone at each step:

| Order | Crate | Why This Order | Milestone |
|-------|-------|---------------|-----------|
| 1 | ferrosa-common | Shared types needed by all crates | Type definitions compile |
| 2 | ferrosa-sstable | No dependencies, immediately useful | Read real Cassandra SSTables, round-trip BTI write/read |
| 3 | ferrosa-storage | Builds on sstable, core engine | Single-node writes + reads with S3 backend |
| 4 | ferrosa-schema | Needed before CQL can execute | Parse CREATE TABLE, validate schemas |
| 5 | ferrosa-cql | Client-facing protocol | cqlsh connects and runs basic queries |
| 6 | ferrosa-net | Needed for multi-node | Two nodes exchange messages |
| 7 | ferrosa-cluster | Distributed coordination | 3-node cluster handles reads/writes at QUORUM |
| 8 | ferrosa (binary) | Compose everything | Full database, characterization tests pass |

Track 1 findings feed into Track 2 decisions. Track 2a (ferrosa-sstable) can start immediately — it only needs the SSTable format documentation, not Java analysis results.

---

## Storage Architecture: Write-Behind Async S3

### Write Path

```
Client Write (CQL INSERT/UPDATE)
    ↓
Commit Log (local ephemeral NVMe)
  + async ship to S3 (small, frequent — seconds)
    ↓
Memtable (RAM)
  ACK to client after commit log + memtable on enough replicas (tunable CL)
    ↓ flush threshold
SSTable → local ephemeral disk
    ↓ async upload (priority queue)
SSTable → S3 (durable)
  S3 object metadata tracks: table, generation, components, checksum
    ↓ compaction (local)
Merged SSTable → local → async S3
```

### Data Loss Mitigations

The write-behind model has a window between local write and S3 upload. Five layers of defense:

| Layer | Mechanism | Window Covered |
|-------|-----------|---------------|
| 1. Quorum writes | Write to RF replicas, ack after CL nodes confirm. With RF=3 CL=QUORUM, data on ≥2 nodes. | Node death before any S3 upload |
| 2. Commit log shipping | Async upload commit log segments to S3 every N seconds (configurable, default 5s). Small payloads = fast upload. New node can replay from S3. | Node death between write and SSTable flush |
| 3. SSTable upload priority | Freshly-flushed SSTables get upload priority over compaction output. | Node death between flush and S3 upload |
| 4. Replica upload coordination | Track which replicas have confirmed S3 upload per SSTable generation. At least one confirming marks data "fully durable." | Multi-node failure before any replica uploads |
| 5. Increased quorum (optional) | Users can set write CL=ALL or use higher RF during migration. Trades write latency for durability. | Catastrophic multi-node failure |

### Read Path

```
Client Read (CQL SELECT)
    ↓
Check Memtable (RAM) — newest data
    ↓ if not found
Check Local SSTable Cache (ephemeral NVMe)
  Bloom filter → partition index → row data
    ↓ cache miss
Fetch from S3 → cache locally → serve
```

Cache eviction: LRU by SSTable access time. Bloom filters and partition indices always cached (small, high value).

### Node Recovery

New/replacement nodes can serve reads within seconds:

1. Join cluster via Raft
2. Get token assignment
3. Download SSTable manifest from S3
4. Fetch Bloom filters + partition indices
5. Serve reads immediately (S3 fallback for cache misses)
6. Background: warm cache from S3 as traffic arrives

No hours-long streaming from other nodes required.

### S3 Object Layout

```
s3://ferrosa-data/{cluster_id}/
  ├── {keyspace}/{table}/
  │   ├── sstables/
  │   │   ├── {generation}-Data.db
  │   │   ├── {generation}-Partitions.db
  │   │   ├── {generation}-Rows.db
  │   │   ├── {generation}-Filter.db
  │   │   ├── {generation}-Statistics.db
  │   │   └── {generation}-CompressionInfo.db
  │   └── manifest.json
  ├── commitlog/
  │   ├── {node_id}/{segment_id}.log
  │   └── {node_id}/checkpoint.json
  └── metadata/
      ├── schema.json
      └── topology.json
```

### S3 Object Metadata (per SSTable component)

| Key | Value | Purpose |
|-----|-------|---------|
| `x-amz-meta-ferrosa-table` | `keyspace.table_name` | Quick identification |
| `x-amz-meta-ferrosa-generation` | `42` | SSTable generation |
| `x-amz-meta-ferrosa-format` | `bti-1.0` | Format version |
| `x-amz-meta-ferrosa-min-token` | `-9223372036854775808` | Partition range start (cache warming) |
| `x-amz-meta-ferrosa-max-token` | `3074457345618258602` | Partition range end |
| `x-amz-meta-ferrosa-level` | `0` | Compaction level |
| `x-amz-meta-ferrosa-checksum` | `sha256:abc123...` | Integrity verification |
| `x-amz-meta-ferrosa-uploaded-by` | `node-3` | Source node |
| `x-amz-meta-ferrosa-created-at` | `2026-03-11T...` | Lifecycle policies |

---

## Rust Crate Architecture

### Dependency Graph

```
                    ferrosa (binary)
                    ├── ferrosa-cluster
                    │   ├── ferrosa-net
                    │   │   └── ferrosa-common
                    │   ├── ferrosa-storage
                    │   │   ├── ferrosa-sstable
                    │   │   │   └── ferrosa-common
                    │   │   └── ferrosa-common
                    │   └── ferrosa-common
                    ├── ferrosa-cql
                    │   ├── ferrosa-schema
                    │   │   └── ferrosa-common
                    │   ├── ferrosa-storage
                    │   └── ferrosa-common
                    └── ferrosa-common
```

### ferrosa-sstable (first deliverable, independently useful)

Responsibilities:
- Read Cassandra Big format SSTables
- Read Cassandra BTI (trie-based) SSTables
- Write BTI format SSTables
- SSTable component abstraction (Data, Partitions, Rows, Filter, Statistics, etc.)
- Bloom filter read/write
- Compression/decompression (LZ4, Zstd, Snappy)
- Future: native Ferrosa format behind feature flag

Key traits:
```rust
trait SSTableReader {
    fn partitions(&self) -> PartitionIter;
    fn get(&self, key: &DecoratedKey) -> Option<Partition>;
    fn bloom_filter(&self) -> &BloomFilter;
    fn statistics(&self) -> &SSTableStats;
}

trait SSTableWriter {
    fn write_partition(&mut self, p: Partition);
    fn finish(self) -> SSTableDescriptor;
}
```

Standalone tools:
- `ferrosa-sstable-dump` — inspect SSTables
- `ferrosa-sstable-import` — migrate from Cassandra

### ferrosa-storage (S3 write-behind engine)

Responsibilities:
- Memtable (skip list or lock-free trie)
- Commit log (local + async S3 shipping)
- Flush: memtable → SSTable (via ferrosa-sstable)
- S3 upload manager (priority queue, retry, tracking)
- Local cache management (LRU, eviction, warming)
- Compaction strategies (Size-Tiered, Leveled, Time-Window)
- Read path: memtable → cache → S3 fallback

Key dependencies:
- `aws-sdk-s3` — S3 client (async). Works with MinIO via endpoint override.
- `tokio` — async runtime
- `crossbeam` — concurrent data structures

### ferrosa-cql (CQL protocol compatibility)

Responsibilities:
- CQL native protocol v5 (binary framing)
- CQL parser (SELECT, INSERT, UPDATE, DELETE, batch, prepared statements)
- Query planner + optimizer
- Result set serialization
- Authentication/authorization hooks

Compatibility target: all standard CQL drivers (DataStax Java/Python/Go, gocql, scylla-rust-driver) connect without modification.

### ferrosa-cluster (distributed coordination)

Responsibilities:
- Raft-based metadata consensus (schema, topology, token assignment) via `openraft`
- Node membership + failure detection
- Token ring management + virtual nodes
- Request routing (coordinator pattern)
- Tunable consistency level enforcement
- Read repair
- Hinted handoff
- Anti-entropy repair

Research items (deferred):
- Accord or similar for distributed transactions
- Tempo / Janus — alternative consensus protocols
- EPaxos — leaderless consensus
- Clock synchronization (HLC, TrueTime-like)
- QUIC transport for multi-datacenter (via `quinn` crate)

### ferrosa-net (internode protocol)

Responsibilities:
- Custom binary internode protocol
- Connection pooling + multiplexing
- Message framing + versioning
- TLS encryption (via `rustls`)
- Backpressure + flow control

Start with TCP + length-prefixed framing. QUIC is a research item.

---

## Testing Strategy

### Test Infrastructure: Sprites + fly.io (<$50/month)

All test clusters are on-demand: spin up for test runs, tear down after. Nightly CI triggers the cycle.

| Purpose | Nodes | Spec | Est. Cost |
|---------|-------|------|-----------|
| Correctness (Cassandra baseline) | 3 | shared-cpu-2x, 1GB | ~$3/mo on-demand |
| Correctness (Ferrosa) | 3 | shared-cpu-2x, 1GB | ~$3/mo on-demand |
| Performance baseline | 3 | shared-cpu-4x, 2GB | ~$5/mo on-demand |
| Chaos / node kill | 3-5 | shared-cpu-1x, 256MB | ~$1-2/run |

Infrastructure fully scripted:
```
ferrosa-test spin-up --nodes 3 --spec shared-cpu-2x --provider fly
ferrosa-test run --suite correctness --workload ycsb-a
ferrosa-test collect-metrics --output s3://ferrosa-metrics/
ferrosa-test tear-down
```

### Performance Regression Detection

Uses **Hunter** (DataStax, open source) for automated change point detection:
- Modified E-divisive means algorithm with Student's t-test
- Detects statistically significant performance changes in time-series data
- 5% relative change filter to remove noise
- Needs ~30+ data points for reliable detection

Performance philosophy:
- **Cassandra baseline is the floor to beat**, not the target
- Rust Ferrosa should *exceed* Cassandra throughput and latency
- Regressions are detected against **Ferrosa's own prior performance**
- If Ferrosa is ever slower than the Java baseline on the same workload, that's a bug
- No hardcoded regression threshold — Hunter detects statistically significant changes, team investigates

Metrics collected per run:
- Throughput (ops/sec)
- p50 latency (median)
- p99 latency (tail)
- p999 latency (extreme tail)
- S3 upload lag — time from flush to S3 confirmation (Ferrosa-specific)
- Cache hit ratio — local vs S3 reads (Ferrosa-specific)

### Suite 1: Data Integrity (Most Important)

No data loss under any condition.

| Test | Validates | Method |
|------|-----------|--------|
| Write + read back | Data written is readable at all CL levels | YCSB workload A |
| Node kill during write | Data survives single node death | Write at QUORUM, kill node mid-stream, verify |
| Node kill + recovery | Recovered node has complete data from S3 | Kill, replace, verify S3 recovery |
| Multi-node failure | Data survives RF-1 failures | Kill nodes sequentially, verify remaining |
| Compaction correctness | Compaction doesn't lose or corrupt data | Write dataset, compact, verify all rows |
| S3 upload verification | S3 SSTables match local originals | Checksum comparison |
| Cold start from S3 | Node with no local data serves correctly | Wipe local, restart, verify reads |

### Suite 2: Performance Baselines

YCSB workloads:

| Workload | Mix | Models |
|----------|-----|--------|
| A | 50% read, 50% update | Session store |
| B | 95% read, 5% update | Photo tagging |
| C | 100% read | User profile cache |
| D | 95% read, 5% insert | Latest status |
| F | 50% read, 50% read-modify-write | User database |

Run against both Cassandra (baseline) and Ferrosa on identical Sprite hardware. Feed results to Hunter.

### Suite 3: Chaos / Failure Injection

Sprites are ideal — fast spin-up/kill, Firecracker isolation.

| Scenario | Action | Verify |
|----------|--------|--------|
| Node crash | Kill Sprite VM (no graceful shutdown) | Cluster continues, data intact after recovery |
| Network partition | iptables isolation | Correct behavior per CL |
| Slow node | tc qdisc latency injection | Cluster doesn't degrade |
| Disk full | Fill ephemeral storage | Node stops writes, S3 data accessible |
| S3 unavailable | Block S3 endpoint | Local writes continue, uploads queue |
| Rolling restart | Restart nodes one at a time | Zero downtime, no data loss |
| Simultaneous multi-kill | Kill 2 of 3 nodes | Remaining rejects sub-quorum writes |

### Suite 4: CQL Compatibility

- Driver matrix: DataStax Java, Python, Go drivers; gocql; scylla-rust-driver
- Protocol: all CQL native protocol v5 message types, error responses, type serialization
- DDL: CREATE/ALTER/DROP for keyspaces, tables, indexes
- DML: INSERT, UPDATE, DELETE, SELECT at all CL levels, BATCH, prepared statements
- Types: all CQL data types including collections, UDTs, tuples, counters
- cqlsh: connects and operates normally

### Pre-1.0 Test Backlog

These tests are required before declaring production readiness. Deferred from initial development but tracked here as the bar for replacing Cassandra in production.

**Data Model Edge Cases:**
- Tombstone & TTL handling — expiration, gc_grace_seconds, tombstone accumulation degrading reads, overflow protection
- Large partition handling — partitions exceeding memory, wide rows with thousands of clustering keys
- Counter correctness — special merge semantics, concurrent increments across replicas
- Timestamp conflict resolution — last-write-wins with client timestamps, out-of-order writes, clock skew

**Distributed Correctness:**
- Hinted handoff — accumulation during node downtime, correct replay on return, storage limits
- Read repair verification — intentional divergence between replicas, verify repair corrects it
- Anti-entropy repair — full and incremental repair produce consistent state under load
- Range queries & pagination — token range scans, paging through large results, coordinator changes mid-page

**S3-Specific Failure Modes:**
- S3 throttling (HTTP 429) — backpressure, exponential retry, local writes continue
- S3 LIST eventual consistency — manifest-based tracking must not depend on LIST for correctness
- Partial upload failure — cleanup, retry, no orphaned S3 objects
- S3 cost profiling — track PUT/GET/LIST call volume to catch cost explosions

**Operational Scenarios:**
- Rolling upgrade — Ferrosa version N and N+1 coexist, no data loss, no downtime
- Schema evolution under load — ALTER TABLE while writes are active, schema disagreement resolution
- Compaction under heavy write load — L0 accumulation, read amplification growth
- Backup/restore from S3 — point-in-time recovery, snapshot consistency
- Long-running soak test (24-72hr) — memory leaks, file descriptor leaks, S3 upload queue growth
- Memory pressure — Sprites' small memory makes this natural; OOM handling, backpressure

**Migration:**
- SSTable import correctness — every row from Cassandra Big + BTI SSTables reads correctly in Ferrosa

### CI Pipeline

```
Push to branch → cargo test (unit) → cargo clippy + fmt
    ↓ merge to main
Nightly: spin up Sprites → run suites 1-4 → collect metrics to S3
    ↓
Hunter: analyze metrics → alert on significant regression → tear down Sprites
```

---

## Research Items

Deferred but tracked for future investigation:

| Area | Options | Notes |
|------|---------|-------|
| Distributed transactions | Accord, Tempo, Janus, EPaxos | Evaluate when core is stable |
| Clock synchronization | HLC, TrueTime-like | Needed for cross-DC consistency |
| Transport protocol | QUIC (quinn crate) | Better for multi-DC, built-in multiplexing |
| Native SSTable format | S3-optimized: larger blocks, content-addressed, embedded metadata | Behind feature flag, after BTI is solid |
| Object store abstraction | `object_store` crate (Apache Arrow) | For GCS/Azure/MinIO portability |
| S3 conditional writes | `If-None-Match` for consistency | Portability concern: not all S3-compat stores support this |

---

## References

- Fleming et al., "Hunter: Using Change Point Detection to Hunt for Performance Regressions," ICPE '23. [Paper](https://dl.acm.org/doi/10.1145/3578244.3583719) | [Code](https://github.com/datastax-labs/hunter)
- DeCandia et al., "Dynamo: Amazon's Highly Available Key-value Store," SOSP '07. [Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Apache Cassandra Source](https://github.com/apache/cassandra)
- [AWS S3 Object Metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)
- [Sprites Documentation](https://docs.sprites.dev/)
- [fly.io Documentation](https://fly.io/docs/)
