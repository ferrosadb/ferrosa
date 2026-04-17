---
type: bug
priority: P2
reported-by: ferrosa-memory production cluster observation
implemented-by: ""
verified-by: ""
created: 2026-04-17
updated: 2026-04-17
---

# Noisy logs: 75 eprintln! calls bypass logger + WARN/ERROR messages for expected behavior

## Observed

Node1 logs are "full of warnings and errors" that make it impossible to distinguish real failures from expected operational noise. Two categories:

### 1. `eprintln!` bypasses structured logger (75 calls in production code)

Output appears without timestamps, making it impossible to correlate with tracing spans. Examples:
- `[store] sidecar persist failed for gen N: E` (store.rs:507)
- `[compaction] task failed: E` (executor.rs:59)
- `saving schema snapshot to S3 at ...` (manifest.rs:351+)
- `[accord] sync_writer failed during commit` (state_machine.rs:327)
- `[telemetry] write_observability failed: E` (writer.rs:154)
- `[flush] late-writer replay put failed: E` (store.rs:565)

### 2. WARN/ERROR for expected operational events

Messages that fire during normal cluster formation/streaming but are logged at WARN:
- `bootstrap streaming failed for ... Bulk lane timeout` — expected during formation
- `raft not initialized yet, cannot admit peer` — expected during startup
- `no handler registered msg_type=...` — fixed, but was WARN instead of ERROR
- `lane permanently failed after max reconnection attempts` — now auto-recovers
- `peer disconnected` / `peer suspected dead` — expected during restart

## Fix Direction

1. Replace all `eprintln!` with `tracing::warn!` or `tracing::error!` as appropriate
2. Downgrade expected-during-formation messages to `tracing::info!` or `tracing::debug!`
3. Upgrade genuine failures from WARN to ERROR

## Acceptance Criteria

- [ ] Zero `eprintln!` calls in production code (excluding `main()` pre-logger-init)
- [ ] No WARN/ERROR log lines during normal 3-node cluster formation and steady state
- [ ] All genuine failures are ERROR level with actionable context
