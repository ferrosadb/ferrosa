---
crate: ferrosa-row-bridge
doc: fmea
last_updated: 2026-09-17
---

# ferrosa-row-bridge — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). This crate is small but on the critical data path, so
severities are high.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| RB-1 | Encoder divergence — a front-end encodes a row differently than the shared codec | Rows written by one front-end read back wrong/invisible via the other (silent corruption) | 10 | 2 | 6 | 120 | **Structural**: both front-ends call this crate; no second encoder exists (D10). Reinforced by the Postgres differential oracle (PG write → CQL read agree). |
| RB-2 | Cells emitted out of storage-column-index order | SSTable reader misreads cells → parse drift → row data loss | 10 | 1 | 7 | 70 | `build_row` sorts cells by index before returning; documented invariant. |
| RB-3 | NULL written as a live empty cell instead of a tombstone | Reads return `""`/`0` instead of NULL | 7 | 2 | 4 | 56 | `build_row`/`build_delete_row` emit tombstones for `Null`; covered by the PG NULL differential case. |
| RB-4 | In-crate test coverage gap — most canonical codec/row tests live in `ferrosa-cql`, not here | A change to this crate can pass `cargo test -p ferrosa-row-bridge` while breaking the real encoding | 8 | 4 | 7 | 224 | **Reduced, still open.** Duration now has an in-crate Cassandra wire vector and malformed-input regression. Move/duplicate the remaining codec + row builder unit tests into this crate. See roadmap. |
| RB-5 | Composite partition-key encoding mismatch vs the engine's key format | Wrong partition routing / unreadable keys | 9 | 1 | 6 | 54 | `build_decorated_key` uses the documented `[2-byte len][bytes][0x00]` composite format; exercised by CQL + PG round-trips. |
| RB-6 | Lossy/unsupported CQL types decoded as NULL silently | Data appears as NULL rather than erroring | 5 | 3 | 5 | 75 | Documented known gap (collections, UDT, tuple, vector decode to NULL in some paths). Track which types are in scope per front-end. |
| RB-7 | Duration components used LEB128 instead of Cassandra's signed leading-ones vint format | Standard drivers decoded truncated duration values, marked the connection defunct, and failed all later requests | 8 | 8 | 2 | 128 → 8 | **Fixed:** encode/decode delegate to the canonical signed-vint implementation, reject trailing bytes, and assert Cassandra's exact wire vector in-crate. |
| RB-8 | `assemble_column_cells` turned a corrupt simple cell (frozen list/map/tuple/UDT or scalar) into `None` via `decode_value(..).ok()` (jsonb FM-78, JB-T8) | Corrupt data read as a missing value: silent data loss; worse once jsonb nests in collections | 9 | 3 | 7 | 189 → 27 | **Fixed (T-034):** decode failure is an `AssembleError` with column type and byte length, counted by `corrupt_element_count()`. The callers' propagation is RB-Tcf7ca2cc below. |
| RB-Tcf7ca2cc | `decode_output_row` and the `ferrosa-cql` metadata decomposition logged a corrupt cell (or an undecodable partition/clustering key component) and handed the column back as `None` | The client saw NULL for a corrupt value: silent data loss on every read path | 9 | 3 | 3 | 81 → 9 | **Fixed (t_cf7ca2cc):** the whole `partition_to_rows*` family plus `visit_`/`consume_partition_rows_with_clustering` return `Result<_, RowDecodeError>` (column, partition key, reason; caller adds the table). No fallback: the read fails. Tests: `corrupt_simple_cell_fails_the_read`, `corrupt_simple_cell_fails_the_streaming_visitor`, `corrupt_simple_cell_fails_the_consuming_visitor`, `uncorrupted_simple_cell_reads_unchanged`. |

## Top risks to act on

1. **RB-4 (RPN 224)** — the highest risk is *test placement*: this crate's own
   test suite does not exercise its core functions, so its green build is not a
   real safety signal. Move the bridge codec/row unit tests in-crate.
2. **RB-1 (RPN 120)** — encoder divergence is severe but well-mitigated
   structurally + by the differential oracle; keep the "no second encoder" rule.

## Detection assets

- Postgres differential oracle (`ferrosa-postgres/tests/differential_oracle.rs`)
  — PG-written rows must read identically over CQL.
- `ferrosa-cql` bridge unit tests (build_decorated_key/build_row/encode_clustering).
- In-crate duration signed-vint wire vector and trailing-byte rejection tests.
- `corrupt_simple_cell_fails_the_read` (RB-Tcf7ca2cc),
  `collection_corrupt_element_is_error_not_none` (RB-8) and the
  `corrupt_element_count()` counter.

## T-022 type-name registry

| ID | Failure mode | Effect | Detection | Mitigation |
|----|--------------|--------|-----------|------------|
| RB-T022-01 | `TypeParser::parse_type` keeps its own scalar switch | Drift from the CQL bridge and schema tables (FM-20) | `type_names_consumers_agree` in `ferrosa-cql` (exercises `parse_cql_type`) | Scalars resolve via `ferrosa_common::cql_type::names::scalar_from_name`; the parser keeps only the collection/tuple/vector/frozen/UDT grammar |

## T-150 jsonb type threading

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation |
|---|---|---|---|---|---|---|---|
| RB-T150-01 | Corrupt stored jsonb cell decoded as a value or as NULL | Silent data loss or a bad document | 9 | 2 | 2 | 36 | `decode_value` validates and returns `corrupt jsonb cell: ...`; the row path wraps it in `RowDecodeError`. Test: `corrupt_jsonb_cell_is_a_typed_error_not_null`. |
| RB-T150-02 | Forbidden jsonb nesting accepted by the type parser | A set/map-key of jsonb reaches storage | 8 | 2 | 2 | 32 | `reject_forbidden_jsonb` runs after both parse entry points. Test: `parse_rejects_set_map_key_and_vector_of_jsonb`. |

## T-151 typed jsonb faults

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation |
|---|---|---|---|---|---|---|---|
| RB-T151-01 | Byte-corrupted jsonb cell returned as a value or NULL | Silent data loss | 9 | 2 | 2 | 36 | One `JsonbValue::from_bytes` per cell; refusal is `JsonbFault::CorruptJsonb`, counted by `corrupt_jsonb_count()`. Tests: `rowbridge_jsonb_corrupt_cell_is_error`, `corrupt_jsonb_cells_fail_select_with_typed_fault` (ferrosa-cql). |
| RB-T151-02 | Cell written by a newer codec (unknown envelope byte) read as corrupt or NULL during a rolling upgrade | Misdiagnosis or silent loss (FM-07) | 8 | 3 | 2 | 48 | Distinct `JsonbFault::UnknownEnvelope`. Test: `mixed_version_read_of_future_envelope_is_error_not_null`. |
| RB-T151-03 | Fault flattened to text inside a collection, tuple or UDT | Nested corruption loses its type | 6 | 3 | 3 | 54 | `AssembleError.jsonb` and `HasJsonbFault` carry it to `RowDecodeError`. Tests: `nested_jsonb_corruption_keeps_the_typed_fault`, `corrupt_jsonb_cells_fail_the_read_with_the_typed_fault`. |
| RB-T151-04 | `From<RowBridgeError> for CqlError` maps a jsonb fault to a client error (0x2200) on a path that calls `decode_value` outside the row bridge | Client sees a request error for a server data fault | 5 | 3 | 5 | 75 | Open: no such path known; key-component decodes in the `ferrosa-cql` bridge keep no fault. |
| RB-T151-05 | `ferrosa-postgres` does not map `RowDecodeError` to a typed server error | PG SELECT of a corrupt cell may surface a generic error | 5 | 3 | 5 | 75 | Open: not verified in this packet. |
