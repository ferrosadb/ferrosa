---
crate: ferrosa-row-bridge
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-row-bridge — Roadmap

Sourced from in-code TODOs, the FMEA gaps ([fmea.md](fmea.md)), and the
dependency/usage review.

## Now (highest value)

- **Move the codec + row-builder unit tests in-crate** (FMEA RB-4). Today the
  canonical tests live in `ferrosa-cql`'s `bridge` module, so
  `cargo test -p ferrosa-row-bridge` does not exercise this crate's core. Bring
  `build_decorated_key` / `build_row` / `build_delete_row` / `encode_clustering`
  / `encode_value` / `decode_value` round-trip tests here.

## Next

- **Enumerate the supported-type matrix per front-end.** Make explicit which CQL
  types `encode_value`/`decode_value` round-trip vs. which decode to NULL
  (Duration, collections, UDT, tuple, vector). Surface unsupported types as
  fail-loud where a front-end requires them, rather than silent NULL.
- **De-duplicate `ferrosa-cql`'s remaining metadata decomposition variants** that
  still live in `ferrosa-cql` but reuse this crate's liveness helpers — fold them
  here so all row decomposition has one home.

## Later

- **Property-test the round-trip** (`decode(encode(v)) == v`) across the scalar
  type space as a regression net independent of the front-ends.
- **Collections / UDT / tuple / vector** encode+decode support, once a consuming
  front-end needs them over the shared path.

## Non-goals

- Protocol framing, query planning, or transport — those belong to the front-ends
  (`ferrosa-cql` / `ferrosa-postgres`), not here.
