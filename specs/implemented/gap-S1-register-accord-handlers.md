---
type: feature
priority: P0
reported-by: gap-closure-audit
implemented-by: ""
verified-by: ""
created: 2026-04-17
updated: 2026-04-17
---

# S1-register-accord-handlers

Register all 11 Accord message type handlers (PreAccept through RecoverOK) in controller/cluster.rs. Required before Accord can be wired into the CQL path.
