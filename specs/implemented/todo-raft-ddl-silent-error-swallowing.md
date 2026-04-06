# TODO: Raft State Machine Silently Swallows DDL Side-Effect Errors

**Severity:** Critical (silent inconsistency between Raft state and storage engine)
**Component:** ferrosa-cluster

## Issue

Every DDL operation in `raft/state_machine.rs` uses `let _ =` to discard errors from schema, engine, and system table writes. Over 20 instances across CreateKeyspace, CreateTable, DropTable, AlterTable, CreateIndex, etc.

```rust
RaftOp::CreateTable(table) => {
    self.state.tables.insert(...);  // State machine updated
    let _ = schema.create_table_internal(*table.clone());  // ERROR IGNORED!
    let _ = engine.register_table(table.to_storage_schema());  // ERROR IGNORED!
    let _ = writer.apply(SystemTableMutation::TableCreated(...));  // ERROR IGNORED!
}
```

## Impact

If `engine.register_table()` fails (disk full, I/O error):
1. RaftState thinks table exists
2. Storage engine has no registration
3. Client gets success
4. Writes to the table silently fail via MutationForwardHandler
5. Data is permanently lost

## Fix

At minimum: log errors at `error!` level. Ideally: return a response indicating partial failure so the client knows the DDL didn't fully apply. Consider marking the state machine as "inconsistent" to trigger a repair.
