---
type: feature
priority: P0
reported-by: gap-closure-audit
implemented-by: ""
verified-by: ""
created: 2026-04-17
updated: 2026-04-17
---

# S1-register-batchlog-handlers

Register BatchlogWrite/Delete/Replay RPC handlers in controller/cluster.rs. Handlers exist in coordinator/batch.rs but are never registered. Batch writes sent to remote nodes are silently dropped.
