---
type: feature
priority: P0
reported-by: gap-closure-audit
implemented-by: ""
verified-by: ""
created: 2026-04-17
updated: 2026-04-17
---

# S1-register-truncate-handler

Register TruncateForward/TruncateAck RPC handlers in controller/cluster.rs. Truncate operations sent to remote nodes are silently dropped, leaving partial state.
