# Bug: Compaction Task Fails on Corrupt SSTable After Node Data Wipe

**Severity:** High
**Component:** ferrosa-storage (compaction)
**Branch:** feat/sparql-endpoint

## Symptoms

1. After wiping node data directories (`rm -rf ~/data/ferrosa-memory/node{1,2,3}/*`) and restarting, the cluster forms successfully and Raft elects a leader
2. After ~30 minutes of operation, compaction task fails:
   ```
   [compaction] task failed: read partitions: I/O error: read_exact_at: wanted 101 bytes, got 1
   ```
3. Data lane write forwarding begins timing out repeatedly:
   ```
   ERROR ferrosa_cluster::pair::handler: PairWriteForward handler failed: net: timeout: Data lane send timeout
   ```
4. Timeouts repeat every ~5 seconds indefinitely
5. Health checks still pass (TCP port check), masking the failure
6. CQL reads eventually timeout, hanging any client (MCP server, viz dashboard)

## Root Cause Analysis

The `rm -rf` wipe of node data directories may leave partial state:
- SSTable files may be partially written during shutdown (podman SIGKILL vs graceful stop)
- Commit log segments reference SSTables that no longer exist
- Compaction attempts to read a truncated SSTable file (got 1 byte instead of 101)
- The compaction failure appears to cascade into Data lane saturation — possibly holding a lock or blocking the tokio runtime

## Reproduction

```bash
# 1. Start a 3-node cluster, let it run and accept writes
podman compose up -d
./scripts/restore-memory.sh  # ~7000 writes

# 2. Hard stop (simulates ungraceful shutdown)  
podman compose down

# 3. Wipe data dirs (simulates operator cleanup)
rm -rf ~/data/ferrosa-memory/node{1,2,3}/*

# 4. Restart
podman compose up -d

# 5. Wait 20-40 minutes for compaction to trigger
# 6. Observe: compaction I/O error → Data lane timeouts → cluster unresponsive
```

## Expected Behavior

- Compaction should detect and skip corrupt/truncated SSTable files
- A corrupt SSTable in compaction should not cascade to Data lane timeouts
- Health checks should detect CQL unavailability, not just TCP port liveness

## Proposed Fix

1. **SSTable validation in compaction**: Before reading partitions, verify file header/length. Skip corrupt files with a warning.
2. **Compaction error isolation**: Compaction failures should not block the Data lane. Use `spawn_blocking` or a separate runtime.
3. **Health check improvement**: Health probe should execute a lightweight CQL query (`SELECT now() FROM system.local`), not just TCP check.

## Related

- `specs/verified/bug-bulk-write-raft-starvation.md` — similar Data lane saturation pattern but different trigger (bulk writes vs compaction failure)
- The P0 fix from pitr-raft-fixes (Raft on dedicated thread) should prevent election storms, but the Data lane itself is still blocked

## Log Evidence

```
[compaction] task failed: read partitions: I/O error: read_exact_at: wanted 101 bytes, got 1
ERROR ferrosa_cluster::pair::handler: PairWriteForward handler failed: net: timeout: Data lane send timeout
```

Timestamps: timeouts repeat every ~5s for the entire observation window (10+ minutes).

## Verification (2026-04-05, branch fix/p0-compaction-ddl-readiness, commit 5330968)

Fresh cluster startup — no compaction I/O errors, no Data lane timeouts.
- **Status: VERIFIED FIXED**
