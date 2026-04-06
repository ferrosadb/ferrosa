# Bug: Stack Overflow in Local Read/Write Routing

**Severity:** Critical (node crashes)
**Component:** ferrosa-cql/ferrosa-storage
**Commit:** 393d6f8

## Issue

Node1 crashes with stack overflow during cluster formation:

```
thread 'tokio-rt-worker' (10) has overflowed its stack
fatal runtime error: stack overflow, aborting
```

Exit code 133. The crash happens after Raft applies CreateTable operations and during bootstrap streaming.

Also: repeated errors `Raft apply: system table write failed for CreateTable: table not registered: system_schema.tables` — the system schema tables aren't registered before Raft tries to write DDL state to them.

## Likely Cause

The refactored local routing in commit 393d6f8 has infinite recursion — a read/write call invokes the storage engine which calls back into the routing layer which calls storage again, overflowing the stack.

## Reproduction

```bash
# Build from commit 393d6f8
podman compose up -d
# Node1 crashes within 30 seconds with exit code 133
podman logs node1 | grep "overflow"
```
