# Clustered graph adjacency quorum/read-after-write seam

Status: documented failing seam

## Seam

A graph edge mutation that derives adjacency rows must satisfy clustered write consistency for both:

1. the primary graph edge row, and
2. the derived `system_graph_<keyspace>.adjacency` row.

A successful HTTP graph mutation should be immediately read-after-write visible through a follow-up graph traversal at the configured consistency level.

## Known failure

The ferrosa-memory PR #13 cluster integration run exposed a real CL=QUORUM failure for the fmem-shaped graph write path:

```text
failed to write derived adjacency row for agent_memory.co_occurs_with:
cluster: write timeout: CL=QUORUM, received=1, required=2
```

The failing log captured during investigation was saved locally as:

```text
/tmp/fmem-cluster-full-25526015591-rerun2.log
```

This should not be hidden with broader HTTP/client timeouts. The graph mutation is tiny; if quorum cannot be reached for the derived adjacency row, the correct next step is to root-cause the cluster write/replica acknowledgement path.

## Existing adjacent coverage

- `ferrosa-graph/tests/graph_http_integration.rs::graph_engine_constructed_before_fmem_ddl_registers_adjacency_for_first_edge_write` covers dynamic adjacency storage registration for DDL created after `GraphEngine::new`.
- `ferrosa-graph/tests/graph_http_integration.rs::full_shape_typed_edge_merge_writes_real_agent_memory_adjacency` covers local derived adjacency materialization for fmem-shaped typed edges.
- `ferrosa-graph/tests/graph_http_integration.rs::cluster_anchor_full_primary_key_match_reads_remote_vertex_without_range_scan` covers clustered graph remote reads at CL=ONE.

These do **not** prove CL=QUORUM graph adjacency write/read-after-write.

## Required functional coverage before closing the underlying bug

Add a dedicated live-cluster or deterministic multi-node harness test that:

1. boots/targets a dedicated non-production Ferrosa cluster with RF >= 2,
2. creates the graph keyspace/tables and dynamic adjacency table path,
3. performs the fmem-shaped `MERGE` edge mutation at CL=QUORUM,
4. verifies the HTTP mutation returns success without timeout,
5. immediately follows with a graph traversal that requires the derived adjacency row,
6. verifies the traversal returns the expected edge endpoint, and
7. fails loudly with received/required acknowledgements if quorum is not satisfied.

Do not run this against the live dev CQL ports (`127.0.0.1:19042-19044`) unless the test is explicitly scoped to a disposable test cluster.
