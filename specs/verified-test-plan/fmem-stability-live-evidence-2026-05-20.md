# Fmem stability live evidence and RED acceptance

Captured: 2026-05-20T20:11:59.873715+00:00 UTC

Scope: read-only/non-destructive evidence for ferrosa-memory local Docker cluster. No data deletion, restarts, rebuilds, or writes were performed.

## Finding summary

This run is RED for fmem stability. Docker health is green and direct bounded CQL partition reads succeed on all three contact ports at ONE and LOCAL_QUORUM, but the cluster still shows severe thread and memory pressure plus MCP timeouts:

- `s3-sync` threads are unbounded-looking: node1=452, node2=4707, node3=436 across two samples five seconds apart.
- Docker PIDs mirror the leak: node2 has ~4,763 PIDs and 1.3–1.7 GiB resident/cgroup memory vs a 2 GiB cap.
- memcg peak hit/exceeded the 2 GiB cap on node1 and node2, with `memory.events max` counts in the thousands; no current `oom_kill` and no recent Docker OOM/die/restart events were observed.
- MCP `tools/list` and `/healthz` are fast, but `get_stats`, `count_entities_by_type`, and `retrieve_entities` all returned HTTP 504 at ~30s in this run.
- Logs show recurring `request exceeded 30s`, `LOCAL_QUORUM received=1 required=2`, peer-suspected-dead, and OpenRaft AppendEntries timeout evidence.

## Container inventory

```text
NAMES                                 IMAGE                               STATUS                  PORTS
ferrosa-memory-node3-1                ferrosa-memory-node                 Up 39 hours (healthy)   7000/tcp, 0.0.0.0:17476->7474/tcp, [::]:17476->7474/tcp, 0.0.0.0:17689->7687/tcp, [::]:17689->7687/tcp, 0.0.0.0:19044->9042/tcp, [::]:19044->9042/tcp, 0.0.0.0:19092->9090/tcp, [::]:19092->9090/tcp
ferrosa-memory-node2-1                ferrosa-memory-node                 Up 39 hours (healthy)   7000/tcp, 0.0.0.0:17475->7474/tcp, [::]:17475->7474/tcp, 0.0.0.0:17688->7687/tcp, [::]:17688->7687/tcp, 0.0.0.0:19043->9042/tcp, [::]:19043->9042/tcp, 0.0.0.0:19091->9090/tcp, [::]:19091->9090/tcp
ferrosa-memory-node1-1                ferrosa-memory-node                 Up 39 hours (healthy)   7000/tcp, 0.0.0.0:17474->7474/tcp, [::]:17474->7474/tcp, 0.0.0.0:17687->7687/tcp, [::]:17687->7687/tcp, 0.0.0.0:18080->8080/tcp, [::]:18080->8080/tcp, 0.0.0.0:19042->9042/tcp, [::]:19042->9042/tcp, 0.0.0.0:19090->9090/tcp, [::]:19090->9090/tcp
ferrosa-memory-ferrosa-memory-mcp-1   ferrosa-memory-ferrosa-memory-mcp   Up 45 hours (healthy)   
ferrosa-memory-minio-1                minio/minio:latest                  Up 45 hours (healthy)   0.0.0.0:19000->9000/tcp, [::]:19000->9000/tcp, 0.0.0.0:19001->9001/tcp, [::]:19001->9001/tcp
```


## Docker state and current resource counters

```text
ferrosa-memory-node1-1 restart=1 running=true exit=0 oom=false pid=1200171 started=2026-05-19T05:00:30.196174608Z image=sha256:ae642abecc3ed80776c906088556213443ccc4f1f8826fe97afa7bb671dfb413
ferrosa-memory-node2-1 restart=0 running=true exit=0 oom=false pid=1196858 started=2026-05-19T04:58:52.858432139Z image=sha256:ae642abecc3ed80776c906088556213443ccc4f1f8826fe97afa7bb671dfb413
ferrosa-memory-node3-1 restart=0 running=true exit=0 oom=false pid=1197296 started=2026-05-19T04:59:03.660439453Z image=sha256:ae642abecc3ed80776c906088556213443ccc4f1f8826fe97afa7bb671dfb413
ferrosa-memory-ferrosa-memory-mcp-1 restart=0 running=true exit=0 oom=false pid=7460 started=2026-05-18T22:58:36.586782102Z image=sha256:a971393aaad17587cdf46f73cbe7ef8d814228ed7c0b19070926f058a16aab95
```

```text
NAME                                  MEM USAGE / LIMIT     MEM %     PIDS
ferrosa-memory-node1-1                190.1MiB / 2GiB       9.28%     504
ferrosa-memory-node2-1                1.324GiB / 2GiB       66.21%    4763
ferrosa-memory-node3-1                339.1MiB / 2GiB       16.56%    488
ferrosa-memory-ferrosa-memory-mcp-1   17.07MiB / 60.71GiB   0.03%     17
```


## Recent Docker OOM/die/restart/kill events

```text
<none>
```


## memcg pressure

```text
--- ferrosa-memory-node1-1 pid=1200171 cgroup=/system.slice/docker-c1f2480f831f65b74bb1497a5de74e3769f13c2185aa1c0f075b1446e1584f08.scope
memory.current=1198256128 
memory.peak=2147487744 
memory.max=2147483648 
memory.swap.current=908857344 
memory.events=low 0 high 0 max 3251 oom 0 oom_kill 0 oom_group_kill 0 
pids.current=505 
pids.max=74289 
--- ferrosa-memory-node2-1 pid=1196858 cgroup=/system.slice/docker-39c0701fba6b2ea78f6f207c7f88c8c18d614ac27a302a39e783dcd969863f00.scope
memory.current=1706266624 
memory.peak=2147491840 
memory.max=2147483648 
memory.swap.current=1645379584 
memory.events=low 0 high 0 max 2833 oom 0 oom_kill 0 oom_group_kill 0 
pids.current=4765 
pids.max=74289 
--- ferrosa-memory-node3-1 pid=1197296 cgroup=/system.slice/docker-2ec8fe41e6702667bc63f1cf05bcef01590269f020f11476017671ff6e58d453.scope
memory.current=365899776 
memory.peak=374808576 
memory.max=2147483648 
memory.swap.current=0 
memory.events=low 0 high 0 max 0 oom 0 oom_kill 0 oom_group_kill 0 
pids.current=492 
pids.max=74289 
--- ferrosa-memory-ferrosa-memory-mcp-1 pid=7460 cgroup=/system.slice/docker-cdb88fa8f87822a98fb06f51d72d3d0dc89bfab9fe0788a5066a2750b186fbbe.scope
memory.current=55439360 
memory.peak=62492672 
memory.max=max 
memory.swap.current=0 
memory.events=low 0 high 0 max 0 oom 0 oom_kill 0 oom_group_kill 0 
pids.current=17 
pids.max=74289
```


## Thread/PID histogram; s3-sync count sampled twice

### sample 1

```text
--- ferrosa-memory-node1-1
proc_count=6
task_count=504
s3_sync_count=452
    452 s3-sync
     18 tokio-rt-worker
      8 raft-rt
      8 data-rt
      2 raft-lane-57266
      1 udf-epoch-ticke
      1 tracing-appende
      1 sled-io-6
      1 sled-io-5
      1 sled-io-4
      1 sled-io-3
      1 sled-io-2
      1 sled-io-1
      1 sled-io-0
      1 raft-lane-85899
      1 log flusher
      1 index-builder-1
      1 index-builder-0
      1 ferrosa
      1 compaction-exec
--- ferrosa-memory-node2-1
proc_count=6
task_count=4763
s3_sync_count=4707
   4707 s3-sync
     20 tokio-rt-worker
      8 raft-rt
      8 data-rt
      2 raft-lane-85899
      2 raft-lane-28633
      1 udf-epoch-ticke
      1 tracing-appende
      1 sled-io-6
      1 sled-io-5
      1 sled-io-4
      1 sled-io-3
      1 sled-io-2
      1 sled-io-1
      1 sled-io-0
      1 s3-schema-sync
      1 log flusher
      1 index-builder-1
      1 index-builder-0
      1 ferrosa
--- ferrosa-memory-node3-1
proc_count=6
task_count=491
s3_sync_count=436
    436 s3-sync
     18 tokio-rt-worker
      8 raft-rt
      8 data-rt
      4 raft-lane-57266
      1 udf-epoch-ticke
      1 tracing-appende
      1 sled-io-7
      1 sled-io-6
      1 sled-io-5
      1 sled-io-4
      1 sled-io-3
      1 sled-io-2
      1 sled-io-1
      1 sled-io-0
      1 raft-lane-28633
      1 log flusher
      1 index-builder-1
      1 index-builder-0
      1 ferrosa
```

### sample 2

```text
--- ferrosa-memory-node1-1
proc_count=6
task_count=504
s3_sync_count=452
cat: /proc/1/task/242148/comm: No such file or directory
cat: /proc/1/task/242150/comm: No such file or directory
    452 s3-sync
     16 tokio-rt-worker
      8 raft-rt
      8 data-rt
      2 raft-lane-57266
      1 udf-epoch-ticke
      1 tracing-appende
      1 sled-io-6
      1 sled-io-5
      1 sled-io-4
      1 sled-io-3
      1 sled-io-2
      1 sled-io-1
      1 sled-io-0
      1 raft-lane-85899
      1 log flusher
      1 index-builder-1
      1 index-builder-0
      1 ferrosa
      1 compaction-exec
--- ferrosa-memory-node2-1
proc_count=6
task_count=4762
s3_sync_count=4707
   4707 s3-sync
     19 tokio-rt-worker
      8 raft-rt
      8 data-rt
      2 raft-lane-85899
      2 raft-lane-28633
      1 udf-epoch-ticke
      1 tracing-appende
      1 sled-io-6
      1 sled-io-5
      1 sled-io-4
      1 sled-io-3
      1 sled-io-2
      1 sled-io-1
      1 sled-io-0
      1 s3-schema-sync
      1 log flusher
      1 index-builder-1
      1 index-builder-0
      1 ferrosa
--- ferrosa-memory-node3-1
proc_count=6
task_count=491
s3_sync_count=436
cat: /proc/1/task/242394/comm: No such file or directory
cat: /proc/1/task/242396/comm: No such file or directory
    436 s3-sync
     16 tokio-rt-worker
      8 raft-rt
      8 data-rt
      4 raft-lane-57266
      1 udf-epoch-ticke
      1 tracing-appende
      1 sled-io-7
      1 sled-io-6
      1 sled-io-5
      1 sled-io-4
      1 sled-io-3
      1 sled-io-2
      1 sled-io-1
      1 sled-io-0
      1 raft-lane-28633
      1 log flusher
      1 index-builder-1
      1 index-builder-0
      1 ferrosa
```


## Suspicious log evidence (last 2000 lines per node/MCP)

### ferrosa-memory-node1-1

```text
[2m2026-05-20T20:02:18.652214Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:19.103943Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:19.555135Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:20.006032Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:20.456791Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:20.907358Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:21.358941Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:21.809540Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:22.260882Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:22.711431Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:23.162612Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:23.614189Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:24.065805Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:24.517119Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:24.967736Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:25.418617Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:25.869877Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:26.320745Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:26.771473Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:27.222986Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:27.673621Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:28.125216Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:28.576366Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:29.027952Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:29.478621Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:29.930009Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:30.381030Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:30.832179Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:31.283758Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:31.297686Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307351297685 [3mdata_size[0m[2m=[0m2937 [3mpartitions_size[0m[2m=[0m291 [3mdir[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log
[2m2026-05-20T20:02:31.298277Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307351297685 [3mdata_bytes[0m[2m=[0m2937 [3mpath[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307351297685-Data.db
[2m2026-05-20T20:02:31.734362Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:32.185819Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:32.636255Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:33.087728Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:33.539118Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:33.989833Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:34.440406Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:34.891990Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:35.342555Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:35.793873Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:36.244401Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:36.696044Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:37.146553Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:37.598139Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:38.048834Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:38.499635Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:38.951023Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:39.401576Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:39.853108Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:40.303229Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:40.754772Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:41.206140Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:41.656405Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:42.107351Z[0m [32m INFO[0m [2mopenraft::core::raft_core[0m[2m:[0m election timeout passed, check if it is a voter for election
[2m2026-05-20T20:02:42.540384Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:03:31.266150Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307411266147 [3mdata_size[0m[2m=[0m3090 [3mpartitions_size[0m[2m=[0m291 [3mdir[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log
[2m2026-05-20T20:03:31.266792Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307411266147 [3mdata_bytes[0m[2m=[0m3090 [3mpath[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307411266147-Data.db
[2m2026-05-20T20:03:31.269058Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: starting streaming task [3mtable_id[0m[2m=[0magent_memory.tool_usage_log [3minputs[0m[2m=[0m4
[2m2026-05-20T20:03:31.270057Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307081274886 [3mdata_file_size[0m[2m=[0m2559 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307081274886-Data.db"
[2m2026-05-20T20:03:31.706152Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307231265888 [3mdata_file_size[0m[2m=[0m2785 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307231265888-Data.db"
[2m2026-05-20T20:03:31.949062Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307351297685 [3mdata_file_size[0m[2m=[0m2937 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307351297685-Data.db"
[2m2026-05-20T20:03:31.985274Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307411266147 [3mdata_file_size[0m[2m=[0m3090 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307411266147-Data.db"
[2m2026-05-20T20:03:33.669601Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:03:34.203868Z[0m [32m INFO[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307414203864 [3mdata_size[0m[2m=[0m1618 [3mpartitions_size[0m[2m=[0m291 [3mdir[0m[2m=[0m/var/lib/ferrosa/compaction/agent_memory.tool_usage_log
[2m2026-05-20T20:03:34.296588Z[0m [32m INFO[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307414203864 [3mdata_bytes[0m[2m=[0m1618 [3mpath[0m[2m=[0m/var/lib/ferrosa/compaction/agent_memory.tool_usage_log/1779307414203864-Data.db
[2m2026-05-20T20:03:41.296462Z[0m [32m INFO[0m [2mferrosa_storage::engine[0m[2m:[0m compaction: swap complete [3mtable_id[0m[2m=[0magent_memory.tool_usage_log [3mpre_swap_count[0m[2m=[0m16 [3mpost_swap_count[0m[2m=[0m13 [3mremoved[0m[2m=[0m4
[2m2026-05-20T20:03:41.767957Z[0m [33m WARN[0m [2mferrosa_storage::engine[0m[2m:[0m compaction: upload queue full, deferring to sync_sstables_to_s3 — swap already complete, no data loss (pending-log persists) [3me[0m[2m=[0minvalid format: upload queue full [3msstable_id[0m[2m=[0m1779307414203864
[2m2026-05-20T20:04:33.670483Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:05:33.670493Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:06:33.670537Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:07:31.640870Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307651640865 [3mdata_size[0m[2m=[0m3237 [3mpartitions_size[0m[2m=[0m291 [3mdir[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log
[2m2026-05-20T20:07:31.641564Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307651640865 [3mdata_bytes[0m[2m=[0m3237 [3mpath[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307651640865-Data.db
[2m2026-05-20T20:07:33.669606Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:08:33.670162Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:09:31.580367Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307771580363 [3mdata_size[0m[2m=[0m3383 [3mpartitions_size[0m[2m=[0m291 [3mdir[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log
[2m2026-05-20T20:09:31.581455Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307771580363 [3mdata_bytes[0m[2m=[0m3383 [3mpath[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307771580363-Data.db
[2m2026-05-20T20:09:33.669874Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:10:33.669542Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:11:33.670069Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
```

### ferrosa-memory-node2-1

```text
[2m2026-05-20T20:02:42.543381Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281029
[2m2026-05-20T20:02:42.543383Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281031
[2m2026-05-20T20:02:42.543384Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281030
[2m2026-05-20T20:02:42.543391Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281032
[2m2026-05-20T20:02:42.543394Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281021
[2m2026-05-20T20:02:42.543395Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281022
[2m2026-05-20T20:02:42.543487Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281034
[2m2026-05-20T20:02:42.543495Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281035
[2m2026-05-20T20:02:42.543497Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281033
[2m2026-05-20T20:02:42.543499Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281036
[2m2026-05-20T20:02:42.543714Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281040
[2m2026-05-20T20:02:42.543719Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281038
[2m2026-05-20T20:02:42.543721Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281037
[2m2026-05-20T20:02:42.543731Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281044
[2m2026-05-20T20:02:42.543733Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281041
[2m2026-05-20T20:02:42.543741Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281042
[2m2026-05-20T20:02:42.543743Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281043
[2m2026-05-20T20:02:42.543744Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281039
[2m2026-05-20T20:02:42.543746Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281047
[2m2026-05-20T20:02:42.543747Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281046
[2m2026-05-20T20:02:42.543749Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281048
[2m2026-05-20T20:02:42.543750Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281045
[2m2026-05-20T20:02:42.543810Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281049
[2m2026-05-20T20:02:42.543988Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281050
[2m2026-05-20T20:02:42.543995Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281051
[2m2026-05-20T20:02:42.544082Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281052
[2m2026-05-20T20:02:42.544090Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281053
[2m2026-05-20T20:02:42.544195Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281057
[2m2026-05-20T20:02:42.544198Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281056
[2m2026-05-20T20:02:42.544209Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281054
[2m2026-05-20T20:02:42.544212Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m281055
[2m2026-05-20T20:03:01.485462Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m70
[2m2026-05-20T20:03:06.966447Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:03:45.475362Z[0m [33m WARN[0m [2mferrosa_net::peer[0m[2m:[0m peer suspected dead: 3 missed heartbeats [3mhost_id[0m[2m=[0m11111111-1111-1111-1111-111111111111
[2m2026-05-20T20:03:45.475374Z[0m [33m WARN[0m [2mferrosa_net::peer[0m[2m:[0m peer suspected dead: 3 missed heartbeats [3mhost_id[0m[2m=[0m33333333-3333-3333-3333-333333333333
[2m2026-05-20T20:03:45.475376Z[0m [33m WARN[0m [2mferrosa_cluster::controller::peer_events[0m[2m:[0m peer suspected dead (not transitioning) [3mpeer[0m[2m=[0m11111111-1111-1111-1111-111111111111
[2m2026-05-20T20:03:45.475380Z[0m [33m WARN[0m [2mferrosa_cluster::controller::peer_events[0m[2m:[0m peer suspected dead (not transitioning) [3mpeer[0m[2m=[0m33333333-3333-3333-3333-333333333333
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/08/agent_memory.context_segment_terms/1778185782540274/1778185782540274-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/08/agent_memory.context_segment_terms/1778185782540274/1778185782540274-Data.db</Key><ETag>&#34;0167aa25c39e9d4a9802f2e79792317b-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:04:06.966435Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:05:06.966774Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/61/agent_memory.context_segment_terms/1778229054128803/1778229054128803-Partitions.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/61/agent_memory.context_segment_terms/1778229054128803/1778229054128803-Partitions.db</Key><ETag>&#34;514928521cc69dadc1893f2406b4297b-2&#34;</ETag></CompleteMultipartUploadResult>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/61/agent_memory.context_segment_terms/1778229054128803/1778229054128803-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/61/agent_memory.context_segment_terms/1778229054128803/1778229054128803-Data.db</Key><ETag>&#34;b6882bda9c8cf77809a9d816b5cf39d0-4&#34;</ETag></CompleteMultipartUploadResult>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/08/agent_memory.context_segment_terms/1778185782540274/1778185782540274-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/08/agent_memory.context_segment_terms/1778185782540274/1778185782540274-Data.db</Key><ETag>&#34;0167aa25c39e9d4a9802f2e79792317b-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:06:06.966692Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:07:06.966548Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:07:30.933097Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307650933096 [3mdata_size[0m[2m=[0m3237 [3mpartitions_size[0m[2m=[0m291 [3mdir[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log
[2m2026-05-20T20:07:30.933491Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307650933096 [3mdata_bytes[0m[2m=[0m3237 [3mpath[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307650933096-Data.db
[2m2026-05-20T20:07:30.935304Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: starting streaming task [3mtable_id[0m[2m=[0magent_memory.tool_usage_log [3minputs[0m[2m=[0m4
[2m2026-05-20T20:07:30.935317Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307084858911 [3mdata_file_size[0m[2m=[0m2234 [3mpath[0m[2m=[0m"/var/lib/ferrosa/compaction/agent_memory.tool_usage_log/1779307084858911-Data.db"
[2m2026-05-20T20:07:30.935387Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307230941933 [3mdata_file_size[0m[2m=[0m2785 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307230941933-Data.db"
[2m2026-05-20T20:07:30.935456Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307350933474 [3mdata_file_size[0m[2m=[0m3011 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307350933474-Data.db"
[2m2026-05-20T20:07:30.935523Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307650933096 [3mdata_file_size[0m[2m=[0m3237 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307650933096-Data.db"
[2m2026-05-20T20:07:30.952702Z[0m [32m INFO[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307650952701 [3mdata_size[0m[2m=[0m2533 [3mpartitions_size[0m[2m=[0m489 [3mdir[0m[2m=[0m/var/lib/ferrosa/compaction/agent_memory.tool_usage_log
[2m2026-05-20T20:07:30.953044Z[0m [32m INFO[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307650952701 [3mdata_bytes[0m[2m=[0m2533 [3mpath[0m[2m=[0m/var/lib/ferrosa/compaction/agent_memory.tool_usage_log/1779307650952701-Data.db
[2m2026-05-20T20:07:40.930357Z[0m [32m INFO[0m [2mferrosa_storage::engine[0m[2m:[0m compaction: swap complete [3mtable_id[0m[2m=[0magent_memory.tool_usage_log [3mpre_swap_count[0m[2m=[0m16 [3mpost_swap_count[0m[2m=[0m13 [3mremoved[0m[2m=[0m4
[2m2026-05-20T20:07:41.275448Z[0m [33m WARN[0m [2mferrosa_storage::engine[0m[2m:[0m compaction: upload queue full, deferring to sync_sstables_to_s3 — swap already complete, no data loss (pending-log persists) [3me[0m[2m=[0minvalid format: upload queue full [3msstable_id[0m[2m=[0m1779307650952701
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:08:06.966953Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/08/agent_memory.context_segment_terms/1778185782540274/1778185782540274-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/08/agent_memory.context_segment_terms/1778185782540274/1778185782540274-Data.db</Key><ETag>&#34;0167aa25c39e9d4a9802f2e79792317b-2&#34;</ETag></CompleteMultipartUploadResult>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:09:06.966195Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/61/agent_memory.context_segment_terms/1778229054128803/1778229054128803-Partitions.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/61/agent_memory.context_segment_terms/1778229054128803/1778229054128803-Partitions.db</Key><ETag>&#34;514928521cc69dadc1893f2406b4297b-2&#34;</ETag></CompleteMultipartUploadResult>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/61/agent_memory.context_segment_terms/1778229054128803/1778229054128803-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/61/agent_memory.context_segment_terms/1778229054128803/1778229054128803-Data.db</Key><ETag>&#34;b6882bda9c8cf77809a9d816b5cf39d0-4&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:09:30.938414Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307770938409 [3mdata_size[0m[2m=[0m3383 [3mpartitions_size[0m[2m=[0m291 [3mdir[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log
[2m2026-05-20T20:09:30.938910Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307770938409 [3mdata_bytes[0m[2m=[0m3383 [3mpath[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307770938409-Data.db
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:10:06.966976Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/08/agent_memory.context_segment_terms/1778185782540274/1778185782540274-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/08/agent_memory.context_segment_terms/1778185782540274/1778185782540274-Data.db</Key><ETag>&#34;0167aa25c39e9d4a9802f2e79792317b-2&#34;</ETag></CompleteMultipartUploadResult>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:11:06.966585Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://minio:9000/ferrosa-memory/node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Location><Bucket>ferrosa-memory</Bucket><Key>node2/6f/agent_memory.context_segment_terms/1778595602166101/1778595602166101-Data.db</Key><ETag>&#34;699bca80eb95050f429560a1506c6656-2&#34;</ETag></CompleteMultipartUploadResult>
[2m2026-05-20T20:11:59.365659Z[0m [33m WARN[0m [1mcql.request[0m[1m{[0m[3mcql.opcode[0m[2m=[0mExecute [3mclient.address[0m[2m=[0m172.20.0.1:45630[1m}[0m[2m:[0m [2mferrosa_cql::router[0m[2m:[0m executing query with ALLOW FILTERING — full table scan with post-filter [3mkeyspace[0m[2m=[0m"agent_memory" [3mtable[0m[2m=[0mentity_store
[2m2026-05-20T20:12:06.966614Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:12:13.429271Z[0m [33m WARN[0m [2mferrosa_net::peer[0m[2m:[0m peer suspected dead: 3 missed heartbeats [3mhost_id[0m[2m=[0m11111111-1111-1111-1111-111111111111
[2m2026-05-20T20:12:13.429293Z[0m [33m WARN[0m [2mferrosa_cluster::controller::peer_events[0m[2m:[0m peer suspected dead (not transitioning) [3mpeer[0m[2m=[0m11111111-1111-1111-1111-111111111111
```

### ferrosa-memory-node3-1

```text
[2m2026-05-20T20:02:42.542732Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592520
[2m2026-05-20T20:02:42.542734Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592518
[2m2026-05-20T20:02:42.542735Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592522
[2m2026-05-20T20:02:42.542737Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592434
[2m2026-05-20T20:02:42.542739Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592513
[2m2026-05-20T20:02:42.542740Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592505
[2m2026-05-20T20:02:42.542742Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592524
[2m2026-05-20T20:02:42.542743Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592526
[2m2026-05-20T20:02:42.542752Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592527
[2m2026-05-20T20:02:42.542919Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592537
[2m2026-05-20T20:02:42.542935Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592532
[2m2026-05-20T20:02:42.542938Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592528
[2m2026-05-20T20:02:42.542940Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592530
[2m2026-05-20T20:02:42.542947Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592545
[2m2026-05-20T20:02:42.542948Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592543
[2m2026-05-20T20:02:42.542950Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592533
[2m2026-05-20T20:02:42.542961Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592529
[2m2026-05-20T20:02:42.542963Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592531
[2m2026-05-20T20:02:42.542965Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592544
[2m2026-05-20T20:02:42.542966Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592536
[2m2026-05-20T20:02:42.542967Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592541
[2m2026-05-20T20:02:42.542970Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592540
[2m2026-05-20T20:02:42.542972Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592535
[2m2026-05-20T20:02:42.542973Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592538
[2m2026-05-20T20:02:42.542974Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592539
[2m2026-05-20T20:02:42.542976Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592534
[2m2026-05-20T20:02:42.542994Z[0m [33m WARN[0m [2mferrosa_net::rpc::client[0m[2m:[0m orphan RPC response: caller dropped before reply [3mpeer[0m[2m=[0m172.20.0.2:7000 [3mstream_id[0m[2m=[0m592542
[2m2026-05-20T20:03:06.836311Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:04:06.836306Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:05:06.836607Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:06:06.836591Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:07:06.836458Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:07:34.813034Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307654813033 [3mdata_size[0m[2m=[0m3237 [3mpartitions_size[0m[2m=[0m291 [3mdir[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log
[2m2026-05-20T20:07:34.813405Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307654813033 [3mdata_bytes[0m[2m=[0m3237 [3mpath[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307654813033-Data.db
[2m2026-05-20T20:08:06.836904Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:09:06.837018Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:09:34.812202Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307774812200 [3mdata_size[0m[2m=[0m3383 [3mpartitions_size[0m[2m=[0m291 [3mdir[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log
[2m2026-05-20T20:09:34.812605Z[0m [32m INFO[0m [1mstorage.flush[0m[1m{[0m[3mtable[0m[2m=[0magent_memory.tool_usage_log[1m}[0m[2m:[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307774812200 [3mdata_bytes[0m[2m=[0m3383 [3mpath[0m[2m=[0m/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307774812200-Data.db
[2m2026-05-20T20:09:34.814621Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: starting streaming task [3mtable_id[0m[2m=[0magent_memory.tool_usage_log [3minputs[0m[2m=[0m4
[2m2026-05-20T20:09:34.814637Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307354813306 [3mdata_file_size[0m[2m=[0m3011 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307354813306-Data.db"
[2m2026-05-20T20:09:34.814722Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307654813033 [3mdata_file_size[0m[2m=[0m3237 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307654813033-Data.db"
[2m2026-05-20T20:09:34.814789Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1779307774812200 [3mdata_file_size[0m[2m=[0m3383 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1779307774812200-Data.db"
[2m2026-05-20T20:09:34.814853Z[0m [32m INFO[0m [2mferrosa_storage::compaction::executor[0m[2m:[0m compaction: opening input SSTable [3mgen[0m[2m=[0m1778344222793539 [3mdata_file_size[0m[2m=[0m4062 [3mpath[0m[2m=[0m"/var/lib/ferrosa/sstables/agent_memory.tool_usage_log/1778344222793539-Data.db"
[2m2026-05-20T20:09:35.132208Z[0m [32m INFO[0m [2mferrosa_storage::flush[0m[2m:[0m flush: writing SSTable [3mgen[0m[2m=[0m1779307775132209 [3mdata_size[0m[2m=[0m5833 [3mpartitions_size[0m[2m=[0m389 [3mdir[0m[2m=[0m/var/lib/ferrosa/compaction/agent_memory.tool_usage_log
[2m2026-05-20T20:09:35.132676Z[0m [32m INFO[0m [2mferrosa_storage::flush[0m[2m:[0m flush: Data.db verified on disk [3mgen[0m[2m=[0m1779307775132209 [3mdata_bytes[0m[2m=[0m5833 [3mpath[0m[2m=[0m/var/lib/ferrosa/compaction/agent_memory.tool_usage_log/1779307775132209-Data.db
[2m2026-05-20T20:09:44.809308Z[0m [32m INFO[0m [2mferrosa_storage::engine[0m[2m:[0m compaction: swap complete [3mtable_id[0m[2m=[0magent_memory.tool_usage_log [3mpre_swap_count[0m[2m=[0m17 [3mpost_swap_count[0m[2m=[0m14 [3mremoved[0m[2m=[0m4
[2m2026-05-20T20:09:44.921604Z[0m [33m WARN[0m [2mferrosa_storage::engine[0m[2m:[0m compaction: upload queue full, deferring to sync_sstables_to_s3 — swap already complete, no data loss (pending-log persists) [3me[0m[2m=[0minvalid format: upload queue full [3msstable_id[0m[2m=[0m1779307775132209
[2m2026-05-20T20:10:06.836817Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:11:06.836392Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:12:06.836623Z[0m [32m INFO[0m [2mferrosa_cluster::controller::cluster[0m[2m:[0m maintenance: periodic memory check [3mtable_count[0m[2m=[0m56 [3mclosed_buf_bytes[0m[2m=[0m0 [3maccord_txns[0m[2m=[0m0
[2m2026-05-20T20:12:11.277149Z[0m [33m WARN[0m [2mopenraft::replication[0m[2m:[0m error replication to target=1229782938247303441 [3merror[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:11.277169Z[0m [31mERROR[0m [2mopenraft::replication[0m[2m:[0m RPCError [3merr[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:11.277204Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m update progress error [3mrequest_id[0m[2m=[0mHeartBeat [3mresult[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:11.277209Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m heartbeat error: timeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441, no update to inflight data
[2m2026-05-20T20:12:11.878238Z[0m [33m WARN[0m [2mopenraft::replication[0m[2m:[0m error replication to target=1229782938247303441 [3merror[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:11.878254Z[0m [31mERROR[0m [2mopenraft::replication[0m[2m:[0m RPCError [3merr[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:11.878286Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m update progress error [3mrequest_id[0m[2m=[0mHeartBeat [3mresult[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:11.878291Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m heartbeat error: timeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441, no update to inflight data
[2m2026-05-20T20:12:12.479714Z[0m [33m WARN[0m [2mopenraft::replication[0m[2m:[0m error replication to target=1229782938247303441 [3merror[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:12.479733Z[0m [31mERROR[0m [2mopenraft::replication[0m[2m:[0m RPCError [3merr[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:12.479771Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m update progress error [3mrequest_id[0m[2m=[0mHeartBeat [3mresult[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:12.479776Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m heartbeat error: timeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441, no update to inflight data
[2m2026-05-20T20:12:12.808631Z[0m [33m WARN[0m [2mferrosa_net::peer[0m[2m:[0m peer suspected dead: 3 missed heartbeats [3mhost_id[0m[2m=[0m11111111-1111-1111-1111-111111111111
[2m2026-05-20T20:12:12.808650Z[0m [33m WARN[0m [2mferrosa_cluster::controller::peer_events[0m[2m:[0m peer suspected dead (not transitioning) [3mpeer[0m[2m=[0m11111111-1111-1111-1111-111111111111
[2m2026-05-20T20:12:13.079939Z[0m [33m WARN[0m [2mopenraft::replication[0m[2m:[0m error replication to target=1229782938247303441 [3merror[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:13.079954Z[0m [31mERROR[0m [2mopenraft::replication[0m[2m:[0m RPCError [3merr[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:13.079985Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m update progress error [3mrequest_id[0m[2m=[0mHeartBeat [3mresult[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:13.079989Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m heartbeat error: timeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441, no update to inflight data
[2m2026-05-20T20:12:13.681086Z[0m [33m WARN[0m [2mopenraft::replication[0m[2m:[0m error replication to target=1229782938247303441 [3merror[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:13.681103Z[0m [31mERROR[0m [2mopenraft::replication[0m[2m:[0m RPCError [3merr[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:13.681132Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m update progress error [3mrequest_id[0m[2m=[0mHeartBeat [3mresult[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:13.681137Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m heartbeat error: timeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441, no update to inflight data
[2m2026-05-20T20:12:14.282178Z[0m [33m WARN[0m [2mopenraft::replication[0m[2m:[0m error replication to target=1229782938247303441 [3merror[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:14.282197Z[0m [31mERROR[0m [2mopenraft::replication[0m[2m:[0m RPCError [3merr[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:14.282228Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m update progress error [3mrequest_id[0m[2m=[0mHeartBeat [3mresult[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:14.282232Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m heartbeat error: timeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441, no update to inflight data
[2m2026-05-20T20:12:14.883034Z[0m [33m WARN[0m [2mopenraft::replication[0m[2m:[0m error replication to target=1229782938247303441 [3merror[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:14.883048Z[0m [31mERROR[0m [2mopenraft::replication[0m[2m:[0m RPCError [3merr[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:14.883073Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m update progress error [3mrequest_id[0m[2m=[0mHeartBeat [3mresult[0m[2m=[0mtimeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
[2m2026-05-20T20:12:14.883077Z[0m [33m WARN[0m [2mopenraft::engine::handler::replication_handler[0m[2m:[0m heartbeat error: timeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441, no update to inflight data
```

### ferrosa-memory-ferrosa-memory-mcp-1

```text
[2m2026-05-20T19:19:51.802122Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:45220 error: request exceeded 30s
[2m2026-05-20T19:21:54.985062Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:39848 error: request exceeded 30s
[2m2026-05-20T19:22:06.408201Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:49098 error: request exceeded 30s
[2m2026-05-20T19:22:51.898881Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:34276 error: request exceeded 30s
[2m2026-05-20T19:24:55.055802Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:47530 error: request exceeded 30s
[2m2026-05-20T19:25:06.509607Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:57224 error: request exceeded 30s
[2m2026-05-20T19:25:52.001962Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:33992 error: request exceeded 30s
[2m2026-05-20T19:26:55.265599Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:34074 error: request exceeded 30s
[2m2026-05-20T19:27:55.159672Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:33932 error: request exceeded 30s
[2m2026-05-20T19:28:10.509531Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:43304 error: request exceeded 30s
[2m2026-05-20T19:28:52.106122Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:38684 error: request exceeded 30s
[2m2026-05-20T19:29:55.354127Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:59496 error: request exceeded 30s
[2m2026-05-20T19:30:55.260419Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:50550 error: request exceeded 30s
[2m2026-05-20T19:31:52.205819Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:36650 error: request exceeded 30s
[2m2026-05-20T19:32:55.433067Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:37084 error: request exceeded 30s
[2m2026-05-20T19:33:55.364926Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:37496 error: request exceeded 30s
[2m2026-05-20T19:34:34.369048Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:55634 error: request exceeded 30s
[2m2026-05-20T19:34:52.279355Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:47236 error: request exceeded 30s
[2m2026-05-20T19:35:55.517148Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:34156 error: request exceeded 30s
[2m2026-05-20T19:36:55.383982Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:42760 error: request exceeded 30s
[2m2026-05-20T19:37:52.383507Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:40400 error: request exceeded 30s
[2m2026-05-20T19:38:55.621639Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:49426 error: request exceeded 30s
[2m2026-05-20T19:39:55.434182Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:55542 error: request exceeded 30s
[2m2026-05-20T19:40:48.179082Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:54830 error: request exceeded 30s
[2m2026-05-20T19:40:48.179083Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:54838 error: request exceeded 30s
[2m2026-05-20T19:40:52.486650Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:48492 error: request exceeded 30s
[2m2026-05-20T19:41:55.677593Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:58118 error: request exceeded 30s
[2m2026-05-20T19:42:55.460674Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:52790 error: request exceeded 30s
[2m2026-05-20T19:43:52.590212Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:43004 error: request exceeded 30s
[2m2026-05-20T19:44:55.737342Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:46242 error: request exceeded 30s
[2m2026-05-20T19:45:55.723768Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:44192 error: request exceeded 30s
[2m2026-05-20T19:46:52.692648Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:35678 error: request exceeded 30s
[2m2026-05-20T19:47:55.838076Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:50672 error: request exceeded 30s
[2m2026-05-20T19:48:55.826790Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:50360 error: request exceeded 30s
[2m2026-05-20T19:49:00.113045Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:50366 error: request exceeded 30s
[2m2026-05-20T19:49:00.113045Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:50368 error: request exceeded 30s
[2m2026-05-20T19:49:23.329208Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:41374 error: request exceeded 30s
[2m2026-05-20T19:49:52.797079Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:60834 error: request exceeded 30s
[2m2026-05-20T19:49:54.726247Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:60838 error: request exceeded 30s
[2m2026-05-20T19:49:54.726314Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:60846 error: request exceeded 30s
[2m2026-05-20T19:51:11.870157Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:52178 error: request exceeded 30s
[2m2026-05-20T19:51:55.864503Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:56346 error: request exceeded 30s
[2m2026-05-20T19:52:52.869645Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:59296 error: request exceeded 30s
[2m2026-05-20T19:53:12.873595Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:41270 error: request exceeded 30s
[2m2026-05-20T19:54:53.586765Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:49686 error: request exceeded 30s
[2m2026-05-20T19:54:55.944898Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:49714 error: request exceeded 30s
[2m2026-05-20T19:55:15.084831Z[0m [33m WARN[0m [2mferrosa_memory_core::dispatch[0m[2m:[0m tool call FAILED [3mtool[0m[2m=[0m"count_entities_by_type" [3mcode[0m[2m=[0m-32603 [3mmsg[0m[2m=[0m"Database returned an error: Internal server error. This indicates a server-side bug, Error message: server error: storage error: invalid data: cluster: read timeout: CL=LOCAL_QUORUM, received=1, required=2, data_present=true" [3melapsed_ms[0m[2m=[0m21497
[2m2026-05-20T19:55:52.972903Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:34130 error: request exceeded 30s
[2m2026-05-20T19:56:12.976932Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:60678 error: request exceeded 30s
[2m2026-05-20T19:56:41.229279Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:39314 error: request exceeded 30s
[2m2026-05-20T19:57:02.301294Z[0m [33m WARN[0m [2mferrosa_memory_core::dispatch[0m[2m:[0m tool call FAILED [3mtool[0m[2m=[0m"count_entities_by_type" [3mcode[0m[2m=[0m-32603 [3mmsg[0m[2m=[0m"Database returned an error: Internal server error. This indicates a server-side bug, Error message: server error: storage error: invalid data: cluster: read timeout: CL=LOCAL_QUORUM, received=1, required=2, data_present=true" [3melapsed_ms[0m[2m=[0m21071
[2m2026-05-20T19:57:23.461040Z[0m [33m WARN[0m [2mferrosa_memory_core::dispatch[0m[2m:[0m tool call FAILED [3mtool[0m[2m=[0m"retrieve_entities" [3mcode[0m[2m=[0m-32603 [3mmsg[0m[2m=[0m"Database returned an error: Internal server error. This indicates a server-side bug, Error message: server error: storage error: invalid data: cluster: read timeout: CL=LOCAL_QUORUM, received=1, required=2, data_present=true" [3melapsed_ms[0m[2m=[0m21158
[2m2026-05-20T19:57:32.303719Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:60368 error: request exceeded 30s
[2m2026-05-20T19:57:56.048011Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:60586 error: request exceeded 30s
[2m2026-05-20T19:58:02.305671Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:52162 error: request exceeded 30s
[2m2026-05-20T19:58:53.071600Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:54366 error: request exceeded 30s
[2m2026-05-20T19:59:13.083289Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:45180 error: request exceeded 30s
[2m2026-05-20T19:59:55.522813Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:40228 error: request exceeded 30s
[2m2026-05-20T20:00:05.438418Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:58440 error: request exceeded 30s
[2m2026-05-20T20:00:56.123413Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:36114 error: request exceeded 30s
[2m2026-05-20T20:01:25.556743Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:45030 error: request exceeded 30s
[2m2026-05-20T20:01:46.874665Z[0m [33m WARN[0m [2mferrosa_memory_core::dispatch[0m[2m:[0m tool call FAILED [3mtool[0m[2m=[0m"count_entities_by_type" [3mcode[0m[2m=[0m-32603 [3mmsg[0m[2m=[0m"Database returned an error: Internal server error. This indicates a server-side bug, Error message: server error: storage error: invalid data: cluster: read timeout: CL=LOCAL_QUORUM, received=1, required=2, data_present=true" [3melapsed_ms[0m[2m=[0m21317
[2m2026-05-20T20:01:53.156794Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:44314 error: request exceeded 30s
[2m2026-05-20T20:02:12.659140Z[0m [33m WARN[0m [2mferrosa_memory_core::dispatch[0m[2m:[0m tool call FAILED [3mtool[0m[2m=[0m"retrieve_entities" [3mcode[0m[2m=[0m-32603 [3mmsg[0m[2m=[0m"Database returned an error: Internal server error. This indicates a server-side bug, Error message: server error: storage error: invalid data: cluster: read timeout: CL=LOCAL_QUORUM, received=1, required=2, data_present=true" [3melapsed_ms[0m[2m=[0m21459
[2m2026-05-20T20:02:13.186955Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:37416 error: request exceeded 30s
[2m2026-05-20T20:02:21.201634Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:37450 error: request exceeded 30s
[2m2026-05-20T20:03:56.225964Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:47426 error: request exceeded 30s
[2m2026-05-20T20:04:53.210355Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:36484 error: request exceeded 30s
[2m2026-05-20T20:05:00.233374Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:36498 error: request exceeded 30s
[2m2026-05-20T20:05:00.233374Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:36508 error: request exceeded 30s
[2m2026-05-20T20:05:13.277913Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:57948 error: request exceeded 30s
[2m2026-05-20T20:06:56.233757Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:34474 error: request exceeded 30s
[2m2026-05-20T20:07:25.004026Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:33508 error: request exceeded 30s
[2m2026-05-20T20:07:53.220767Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:38010 error: request exceeded 30s
[2m2026-05-20T20:08:13.331230Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:51224 error: request exceeded 30s
[2m2026-05-20T20:09:20.883266Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:34620 error: request exceeded 30s
[2m2026-05-20T20:09:56.320886Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:39880 error: request exceeded 30s
[2m2026-05-20T20:10:53.324099Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:51820 error: request exceeded 30s
[2m2026-05-20T20:11:13.438087Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:51304 error: request exceeded 30s
[2m2026-05-20T20:12:13.365657Z[0m [33m WARN[0m [2mferrosa_memory_core::http[0m[2m:[0m connection from 127.0.0.1:48270 error: request exceeded 30s
```


## MCP bounded HTTP/JSON-RPC smokes

```text
[
  {
    "target": "healthz/live",
    "ok": true,
    "http": 200,
    "seconds": 0.002,
    "body": "ok"
  },
  {
    "target": "healthz/ready",
    "ok": true,
    "http": 200,
    "seconds": 0.0,
    "body": "ready"
  },
  {
    "ok": true,
    "http": 200,
    "seconds": 0.001,
    "body_prefix": "{\"id\":1,\"jsonrpc\":\"2.0\",\"result\":{\"tools\":[{\"description\":\"Persist raw pre-compaction conversation context as deterministic semantic segments, with Nomic embeddings when configured and temporal prev/next links for later expansion.\",\"inputSchema\":{\"properties\":{\"conversation_id\":{\"maxLength\":512,\"type\":\"string\"},\"embed_missing\":{\"type\":\"boolean\"},\"messages\":{\"items\":{\"properties\":{\"content\":{\"maxLength\":131072,\"type\":\"string\"},\"created_at\":{\"description\":\"Optional RFC3339 timestamp\",\"type\":\"string\"},\"metadata\":{\"type\":\"object\"},\"role\":{\"type\":\"string\"},\"turn_index\":{\"type\":\"integer\"}},\"required\":[\"role\",\"content\",\"turn_index\"],\"type\":\"object\"},\"minItems\":1,\"type\":\"array\"},\"segmentation\":{\"type\":\"object\"},\"session_id\":{\"type\":\"string\"}},\"required\":[\"conversation_id\",\"messages\"],\"type\":\"object\"},\"name\":\"ingest_context_segments\"},{\"description\":\"Hybrid-search raw context segments with lexical BM25 fallback plus Nomic vector ANN, optionally returning bounded prev/next temporal expansion win",
    "target": "tools/list"
  },
  {
    "ok": false,
    "seconds": 30.002,
    "error": "<HTTPError 504: 'Gateway Timeout'>",
    "target": "get_stats"
  },
  {
    "ok": false,
    "seconds": 30.002,
    "error": "<HTTPError 504: 'Gateway Timeout'>",
    "target": "count_entities_by_type"
  },
  {
    "ok": false,
    "seconds": 30.001,
    "error": "<HTTPError 504: 'Gateway Timeout'>",
    "target": "retrieve_entities"
  }
]
```


## Direct bounded CQL smokes

```text
[
  {
    "port": 19042,
    "connect_seconds": 0.595,
    "system_local_one": {"ok": true, "seconds": 0.001, "rows": 1},
    "entity_partition_one": {"ok": true, "seconds": 0.002, "rows": 1},
    "entity_partition_lq": {"ok": true, "seconds": 0.016, "rows": 1}
  },
  {
    "port": 19043,
    "connect_seconds": 0.5,
    "system_local_one": {"ok": true, "seconds": 0.001, "rows": 1},
    "entity_partition_one": {"ok": true, "seconds": 0.002, "rows": 1},
    "entity_partition_lq": {"ok": true, "seconds": 0.005, "rows": 1}
  },
  {
    "port": 19044,
    "connect_seconds": 0.509,
    "system_local_one": {"ok": true, "seconds": 0.0, "rows": 1},
    "entity_partition_one": {"ok": true, "seconds": 0.002, "rows": 1},
    "entity_partition_lq": {"ok": true, "seconds": 0.003, "rows": 1}
  }
]
```

## RED acceptance criteria

The current stability bug should remain RED until all of these are proven in one fresh evidence run:

1. No unbounded s3-sync thread accumulation: each Ferrosa node has a small, stable s3-sync count (target <= 1 active s3-sync worker, or an explicitly bounded documented limit) across repeated samples under normal flush activity.

2. Bounded process/thread pressure: Docker PIDs and cgroup pids.current are stable and far below pids.max; no node shows thousands of threads or monotonic task-count growth.

3. Bounded memory pressure: memory.current is stable below the 2 GiB cap, memory.peak does not repeatedly re-hit memory.max after rollout, memory.events max/oom/oom_kill do not increase during the quiet window, and swap does not monotonically rise.

4. No fresh destructive/recovery churn: restart counters remain unchanged; Docker events show no fresh oom/die/restart/kill; logs are quiet for orphan RPC, peer-suspected-dead, Bulk lane send timeout, OpenRaft AppendEntries timeout storms, and upload queue full loops.

5. Bounded reads complete: direct CQL partition reads at ONE and LOCAL_QUORUM succeed on ports 19042/19043/19044 within 8s.

6. MCP recall is healthy: healthz live/ready, tools/list, get_stats, count_entities_by_type, and retrieve_entities complete within their budgets without HTTP 504/request-exceeded-30s or LOCAL_QUORUM received<required errors.

7. Verification is non-destructive: no data volumes are deleted or reset; restarts/rebuilds, if later needed for a fix, are explicit rollout actions and not part of this evidence-only RED acceptance.
