---
type: feature
priority: P0
reported-by: gap-closure-audit
implemented-by: ""
verified-by: ""
created: 2026-04-17
updated: 2026-04-17
---

# S2-graph-cluster-read-routing

Route 8 graph/SPARQL read locations through WritePath instead of direct storage.read(). Files: ferrosa-graph adjacency/reconcile.rs, executor/expand.rs, varpath.rs; ferrosa-sparql executor.rs.
