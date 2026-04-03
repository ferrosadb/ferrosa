# BUG: Bootstrap streaming completes instantly without transferring data

## Symptom

Single-node ferrosa cluster with existing data (2039 entities, 31k edges in `agent_memory` keyspace). Raft state wiped, cluster reformed as 3-node via progressive join. Bootstrap streaming logs say "complete" but no data is transferred:

```
INFO  ferrosa_cluster::controller::cluster: starting bootstrap streaming to new token owners
INFO  ferrosa_cluster::controller::cluster: bootstrap streaming complete — all nodes Normal
```

These two log lines are 2ms apart. 2039 entities and 31k edges were not streamed.

After cluster formation, reads at LOCAL_QUORUM return 0 rows because:
- Data exists only on node1 (the original single-node cluster)
- Node2 and node3 have empty storage
- LOCAL_QUORUM requires 2/3 nodes to agree → 2 nodes return empty → result is empty

## Reproduction

1. Start a single-node ferrosa cluster:
   ```
   FERROSA_MODE=dev FERROSA_HOST_ID=11111111-1111-1111-1111-111111111111
   ```

2. Write data via CQL (e.g., create keyspace `agent_memory`, populate entities/edges tables)

3. Stop the cluster, wipe raft state:
   ```
   rm -rf ~/data/ferrosa-memory/node{1,2,3}/raft
   ```

4. Start 3-node cluster with progressive join (node1 standalone → node2 pairs → node3 triggers cluster):
   ```yaml
   # docker-compose.yml
   node1: FERROSA_MODE=dev  (no seed)
   node2: FERROSA_MODE=dev, FERROSA_SEED=node1:7000
   node3: FERROSA_MODE=dev, FERROSA_SEED=node1:7000
   ```

5. Wait for cluster formation (all 3 nodes reach Normal state)

6. Query at LOCAL_QUORUM:
   ```sql
   SELECT count(*) FROM agent_memory.entities;
   -- Returns 0
   ```

7. Query at ONE against node1 specifically:
   ```sql
   -- Same query returns 2039
   ```

## Expected behavior

Bootstrap streaming should:
1. Identify all SSTables/memtable data on node1
2. Calculate token ownership for the new 3-node ring
3. Stream data to node2 and node3 for their token ranges
4. Only log "complete" after data transfer finishes

## Additional issues observed

After cluster formation, Raft AppendEntries also time out periodically:

```
WARN  openraft::replication: error replication to target=... timeout after 300ms when AppendEntries
```

And the S3 SSTable sync fails continuously:

```
WARN  ferrosa: S3 SSTable sync failed e=invalid format: failed to save manifest: Operation not yet implemented.
```

(This is a separate RustFS compatibility issue but compounds the problem.)

## Environment

- ferrosa branch: `fix/standalone-progressive-join` (30768c0)
- Cluster: 3-node via podman compose
- Data: 2039 entities, 31k edges in `agent_memory`
- All written when node1 was standalone

## Files

- `ferrosa-cluster/src/controller/cluster.rs` — bootstrap streaming logic (around the "starting bootstrap streaming" log line)
