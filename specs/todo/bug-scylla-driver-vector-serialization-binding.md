---
type: todo
priority: P1
status: draft
created: 2026-05-02
updated: 2026-05-02
---

# Bug: scylla-rust-driver fork fails to serialize `Vec<u8>` into CQL `VECTOR<float, N>` parameter

## Why this is a Ferrosa bug

Ferrosa advertises `VECTOR<float, N>` as a native CQL type (via `Custom("org.apache.cassandra.db.marshal.VectorType(...)")` on the wire). The scylla-rust-driver fork at `ferrosadb/scylla-rust-driver` (`rev=2c493e8cf9968d6c83d33a3dc36c3ef3da595f9a`, branch `fix/cassandra-vector-custom-type`) was patched to **deserialize** `VectorType` from result rows (read path). However, it still fails to **serialize** `Vec<u8>` (or `Vec<f32>`) when binding a prepared statement parameter against a `VECTOR<float, 768>` column.

The failure mode is a `SerializationError` at parameter-binding time — not a server rejection. This means the driver itself rejects the binding before the query ever reaches Ferrosa.

## Observed on

- Ferrosa commit: `fe11d50` (test cluster, 3-node podman)
- Client: `ferrosa-memory` via `scylla-rust-driver` fork
- Fork rev: `2c493e8cf9968d6c83d33a3dc36c3ef3da595f9a`
- Table: `agent_memory.memo_cache` — column `result_embedding vector<float, 768>`

## Repro

1. Start a Ferrosa test cluster with the `agent_memory` keyspace and `memo_cache` table (see `ddl/005_vector_columns.cql`).
2. Attempt to bind a `Vec<u8>` (raw big-endian f32 bytes) to the `result_embedding` column via a prepared statement:

```rust
let embedding: Vec<f32> = (0..16).map(|i| i as f32 / 16.0).collect();
let bytes = crate::vector::encode_vector(&embedding); // Vec<u8>, big-endian f32 bytes

// Prepared statement: INSERT INTO agent_memory.memo_cache (..., result_embedding, ...) VALUES (..., ?, ...)
// Binding includes `Some(bytes)` for the embedding parameter.
storage.memo_put(&ctx, &entry).await
```

3. The driver returns:

```
SerializationError: Failed to serialize query arguments (...):
failed to serialize column result_embedding:
SerializationError: Failed to type check Rust type alloc::vec::Vec<u8>
against CQL type Custom("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 768)"):
expected one of the CQL types: [Blob]
```

## Root cause

The scylla-rust-driver fork added `VectorType` support to the **deserialization** path (result set parsing) but not to the **serialization** path (prepared statement parameter binding). When the driver encounters a `VectorType` column spec in a prepared statement, it has no `SerializeCql` implementation that maps `Vec<u8>` (or `Vec<f32>`) to the `VectorType` CQL type.

## CQL VECTOR wire format

Per CEP-30 / Cassandra 5.0, `VECTOR<float, N>` wire format is:
- 4-byte length prefix: `N` as big-endian `i32`
- Followed by `N` consecutive big-endian IEEE 754 `f32` values (4 bytes each)

Total payload: `4 + N*4` bytes.

The current workaround in `ferrosa-memory` encodes `Vec<f32>` to raw bytes (just the f32 values, no length prefix) and attempts to bind as `Vec<u8>`. This would need the driver to:
1. Accept `Vec<u8>` for `VectorType` columns (treat as raw wire bytes), OR
2. Accept `Vec<f32>` and encode it automatically (add the length prefix + big-endian f32s).

## Proposed fix

Option A (preferred — minimal Ferrosa change):
- Extend the scylla-rust-driver fork's `SerializeCql` impl to accept `Vec<u8>` for `VectorType` columns, treating the bytes as already-encoded wire format (length prefix + f32 values). This mirrors how the deserialization path was patched.

Option B (cleaner API):
- Add a `Vector` wrapper type in the driver (or ferrosa-memory) that holds `Vec<f32>` and implements `SerializeCql` + `FromCqlVal` for `VectorType`. The wrapper handles encode/decode automatically.

## Acceptance criteria

- [ ] `cargo test -p ferrosa-memory-core --test scylla_driver_migration t05_memo_put_with_embedding_roundtrip` passes against a live Ferrosa test cluster.
- [ ] No regressions in existing vector read path (t09 `vector_encode_decode_is_driver_independent` must still pass).
- [ ] Driver fork rev is bumped in `ferrosa-memory/Cargo.toml` and `Cargo.lock`.

## Related

- Upstream PR (scylla-rust-driver): https://github.com/scylladb/scylla-rust-driver/pull/1624
- Ferrosa fork: `https://github.com/ferrosadb/scylla-rust-driver/tree/fix/cassandra-vector-custom-type`
- ferrosa-memory vector encode/decode: `crates/ferrosa-memory-core/src/vector.rs`
- ferrosa-memory scylla driver migration test: `crates/ferrosa-memory-core/tests/scylla_driver_migration.rs` (T-05)
