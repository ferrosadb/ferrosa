# TODO: MutationForwardHandler Returns ACK When Write Fails (Phantom ACK)

**Severity:** Critical (data loss — silent write failure on replicas)
**Component:** ferrosa-cluster

## Issue

`MutationForwardHandler::handle()` (coordinator/mod.rs:123-128) silently discards the storage write result:

```rust
for row in &mutation.rows {
    let _ = self.storage.write(&table_id, &mutation.key, row.clone(), mutation.timestamp);
}
Some(Message::MutationAck(Bytes::new()))
```

If `storage.write()` fails (table not registered, storage full, etc.), the handler still returns `MutationAck`. The coordinator counts this as a successful replica ACK.

## Impact

1. **Schema propagation lag**: Node A creates a table and writes immediately. Node B hasn't applied the Raft DDL yet — table not registered. MutationForward arrives, write fails silently, ACK returned. Data lost on node B.
2. **Storage errors**: Any storage-level failure (disk full, I/O error) is silently swallowed. Data appears to be replicated but isn't.
3. **Consistency violation**: With QUORUM writes, the coordinator may count a phantom ACK as meeting quorum. The data isn't actually durable on enough replicas.

## Fix

Return `MutationNack` or no response (causing timeout) when the write fails. The coordinator should treat this as a failure and store a hint for later replay.

```rust
for row in &mutation.rows {
    if let Err(e) = self.storage.write(&table_id, &mutation.key, row.clone(), mutation.timestamp) {
        tracing::warn!(%e, table = %table_id, "MutationForward write failed");
        return None; // No ACK — coordinator will count as failure and store hint
    }
}
Some(Message::MutationAck(Bytes::new()))
```

## Related

- The same pattern exists in `RepairWriteHandler` (coordinator/mod.rs:~160)
- Schema propagation timing: DDL should block writes until Raft confirms all voters have applied
