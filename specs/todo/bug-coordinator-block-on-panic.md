# Bug: Read Coordinator Panics — block_on() Inside Tokio Runtime

**Severity:** Critical (cluster unusable — all CQL reads crash)
**Component:** ferrosa-cluster/src/coordinator/mod.rs:282
**Commit:** 774a6d4

## Issue

The refactored read coordinator uses `block_on()` inside the tokio async runtime, causing a panic:

```
thread 'tokio-rt-worker' panicked at ferrosa-cluster/src/coordinator/mod.rs:282:35:
Cannot start a runtime from within a runtime. This happens because a function (like `block_on`) 
attempted to block the current thread while the thread is being used to drive asynchronous tasks.
```

All CQL connections are dropped after this panic. The cluster accepts TCP connections but immediately closes them.

## Fix

Replace `block_on()` at line 282 with `.await`. The function is already in an async context — `block_on` is unnecessary and illegal inside tokio.

## Reproduction

```bash
# Build from commit 774a6d4
podman compose up -d
# Wait for cluster formation
# Any CQL query triggers the panic:
python3 -c "from cassandra.cluster import Cluster; Cluster(['127.0.0.1'], port=19042, protocol_version=4).connect('agent_memory')"
# → ConnectionShutdown
# Check logs: podman logs node1 | grep "panicked"
```

## Update: Fix 63d812d Still Broken

Replaced block_on with StorageEngine routing, but ALL CQL operations (reads AND writes) now timeout indefinitely. Even `SELECT count(*)` fails after 90+ seconds of cluster uptime.

```
MutationForward failed: timeout: Data lane timeout  hid=33333333
Raft apply: system table write failed: write timeout CL=ONE, received=0, required=1
```

The coordinator routes to replicas but the Data lane connections between nodes are not established. The cluster forms (Raft leader elected) but the write/read paths through the coordinator can't reach other nodes.

This is a more fundamental issue than the original local-only reads — the coordinator was added but the Data lanes aren't ready when it starts routing.
