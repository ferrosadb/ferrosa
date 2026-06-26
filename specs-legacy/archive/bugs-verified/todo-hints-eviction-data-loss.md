# TODO: Hint Eviction Silently Deletes Undelivered Mutations

**Severity:** Critical (permanent data loss when hints overflow)
**Component:** ferrosa-cluster

## Issue

`hints/mod.rs:268-305` — when the per-peer hint byte cap is exceeded, oldest hint segments are deleted without ever being delivered:

```rust
fn evict_oldest(&self, peer_id: Uuid, state: &mut PeerHintState, cap_bytes: u64) -> Result<()> {
    // Deletes oldest segment files...
    state.needs_repair = true;  // Sets flag but nothing checks it
    Ok(())
}
```

The `needs_repair` flag is set but never checked by the delivery logic. Deleted hints are permanently lost — the mutations they contained will never reach the failed replica.

## Impact

If a peer is down for an extended period and the hint store fills up:
1. Oldest hints are deleted
2. When the peer recovers, it misses those mutations
3. Anti-entropy repair is the only recovery path, but it may not run promptly
4. In the interim, the peer has divergent state

## Fix

1. When `needs_repair` is set, trigger immediate anti-entropy repair for the affected peer
2. Or: don't evict — apply backpressure to writes instead (Cassandra's approach)
3. Or: track evicted token ranges and request streaming from surviving replicas
