# Data Flow

> Last updated: 2026-03-11
> Status: Approved

## Overview

Ferrosa uses a write-behind async S3 storage model. Writes go to local ephemeral storage first, then are asynchronously uploaded to S3. Reads check memtable, local cache, then fall back to S3 on cache miss.

## Write Path

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant Coord as Coordinator Node
    participant R1 as Replica 1
    participant R2 as Replica 2
    participant S3 as S3

    C->>Coord: INSERT (CL=QUORUM)
    Coord->>R1: Forward mutation
    Coord->>R2: Forward mutation

    par On each replica
        R1->>R1: Write to commit log (local NVMe)
        R1->>R1: Write to memtable (RAM)
        R1-->>S3: Async: ship commit log segment
    and
        R2->>R2: Write to commit log (local NVMe)
        R2->>R2: Write to memtable (RAM)
        R2-->>S3: Async: ship commit log segment
    end

    R1->>Coord: ACK
    R2->>Coord: ACK
    Coord->>C: SUCCESS (QUORUM met)

    Note over R1,S3: Later: memtable flush
    R1->>R1: Flush memtable → SSTable (local)
    R1-->>S3: Async: upload SSTable (priority queue)
```

## Read Path

```mermaid
sequenceDiagram
    participant C as CQL Client
    participant Coord as Coordinator
    participant R as Replica
    participant Cache as Local Cache (NVMe)
    participant S3 as S3

    C->>Coord: SELECT (CL=QUORUM)
    Coord->>R: Forward read

    R->>R: Check memtable
    alt Found in memtable
        R->>Coord: Return data
    else Not in memtable
        R->>Cache: Check local SSTable cache
        alt Cache hit
            R->>R: Bloom filter → partition index → data
            R->>Coord: Return data
        else Cache miss
            R->>S3: Fetch SSTable component
            S3->>R: SSTable data
            R->>Cache: Cache locally
            R->>Coord: Return data
        end
    end

    Coord->>C: Result
```

## SSTable Lifecycle in S3

```mermaid
stateDiagram-v2
    [*] --> Memtable: Client write
    Memtable --> LocalSSTable: Flush threshold
    LocalSSTable --> S3SSTable: Async upload
    S3SSTable --> Active: Manifest updated

    state "Compaction" as Compact {
        Active --> Reading: Compaction selects inputs
        Reading --> Merging: Read input SSTables
        Merging --> NewSSTable: Write merged output
        NewSSTable --> Uploaded: Upload to S3
    }

    Uploaded --> Active: New manifest written
    Active --> GracePeriod: Superseded by compaction
    GracePeriod --> Deleted: Grace period expires (1hr)
```

### Manifest Update Protocol

1. Compaction completes: new SSTables uploaded, confirmed durable in S3
1. Updated `manifest.json` written (atomic S3 PUT — new generations in, old out)
1. Old SSTables marked for deletion with 1-hour grace period
1. Background GC deletes expired S3 objects
1. Periodic orphan sweep catches objects not in any manifest

## Commit Log Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Active: New segment opened
    Active --> Active: Writes appended
    Active --> Closed: Segment full (32MB)
    Active --> Shipped: Timer fires (5s) — partial upload
    Closed --> Shipped: Upload to S3
    Shipped --> Retained: SSTable flush not yet durable
    Retained --> Deletable: Corresponding SSTables confirmed in S3
    Deletable --> [*]: Cleanup
```

**Commit log entry format**: 4-byte length + 8-byte logical timestamp + CQL mutation payload + 4-byte CRC32.

**Recovery**: New node reads `checkpoint.json` from S3, determines replay starting point, downloads and replays segments newer than the latest durable SSTable.

## S3 Upload Backpressure

```mermaid
flowchart TD
    A[SSTable flushed to local disk] --> B{Upload queue depth}
    B -->|Normal| C[Queue for async upload]
    B -->|Queue growing| D{What type?}
    D -->|Fresh SSTable| C
    D -->|Compaction output| E[Deprioritize or drop]
    C --> F{Local disk usage}
    F -->|Below 80%| G[Continue accepting writes]
    F -->|Above 80%| H[Backpressure: WriteTimeout to clients]
    E --> F
```

## Node Recovery

```mermaid
sequenceDiagram
    participant New as New Node
    participant Raft as Raft Leader
    participant S3 as S3

    New->>Raft: Join cluster request
    Raft->>New: Token assignment + Raft snapshot
    New->>S3: Download SSTable manifest
    New->>S3: Fetch Bloom filters + partition indices
    Note over New: Can serve reads now (S3 fallback)
    New->>S3: Download commit log checkpoint
    New->>S3: Replay recent commit log segments

    loop Background cache warming
        New->>S3: Fetch hot SSTables based on access
    end
```

## Data Loss Mitigations

| Layer | Mechanism | Window Covered |
|-------|-----------|---------------|
| 1. Quorum writes | Data on >= 2 nodes before ACK (RF=3, CL=QUORUM) | Node death before S3 upload |
| 2. Commit log shipping | S3 upload every 5s (configurable) | Node death before SSTable flush |
| 3. SSTable upload priority | Fresh flushes before compaction output | Node death between flush and upload |
| 4. Replica coordination | Track S3 upload confirmation per replica | Multi-node failure before any upload |
| 5. Increased quorum (optional) | CL=ALL or higher RF | Catastrophic multi-node failure |

## S3 Object Layout

```
s3://ferrosa-data/{cluster_id}/
  {keyspace}/{table}/
    sstables/{generation}-{component}.db
    manifest.json
  commitlog/
    {node_id}/{segment_id}.log
    {node_id}/checkpoint.json
  metadata/
    schema.json
    topology.json
```

## Related Specs

- [Overview](overview.md) — system overview
- [Components](components.md) — crate architecture
- [Testing](testing.md) — data integrity and chaos tests
