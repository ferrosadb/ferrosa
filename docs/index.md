# Ferrosa Suite Documentation

Public developer-preview documentation for Ferrosa Database and Ferrosa Memory. These pages describe install and getting-started flows; engineering plans and verification notes are tracked separately from the public site.

Latest stable releases: Ferrosa Database `v0.17.0`; Ferrosa Memory `v0.23.0`.

## Products

- [Ferrosa Database](database/) — developer-preview database docs, examples, CQL/Cypher/SPARQL notes, and migration guidance for the latest stable `v0.17.0` release.
- [Ferrosa Memory](ferrosa-memory/) — developer-preview long-context linked memory for agents, including document/chunk retrieval, task-aware query decomposition, feedback-aware reranking, query-intent fusion, and Codex/Claude/Hermes hook onboarding for the latest stable `v0.23.0` release.
- [Ferrosa Memory Getting Started](ferrosa-memory/getting-started.html) — run the local stack, connect MCP clients, and try memory examples.

## Database Docs

- [Getting Started](database/getting-started.html) — installation and first cluster
- [Migration](database/migration.html) — migrating from Apache Cassandra
- [CQL Compatibility](database/cql-compatibility.html) — supported CQL features and driver compatibility
- [Vector Indexes](database/vector-indexes.html) — HNSW and quantized HVQ vector search. In the in-tree evaluation, HVQ reads **3.2× fewer bytes per query** and answers **~3.5× faster** at equal recall@10 vs the full HNSW sidecar.
- [Examples](database/examples/) — generated database examples
