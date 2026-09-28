# ferrosa-jsonb

The jsonb value model for Ferrosa. A leaf crate: it depends on no other ferrosa
crate and on no arrow-rs crate (decision D5a; `scripts/ci/guard-arrow-free.sh
ferrosa-common ferrosa-jsonb` enforces it). Runtime dependencies are `bytes`,
`num-bigint` and `thiserror`.

## Implemented (T-100)

- `JsonbError` and `LimitsError`: typed errors that name the measured value and
  the bound.
- `Limits`: tunable ingest limits (D14, D14b). Resolved by
  `Limits::from_config(&LimitsConfig, write_path_max)`. Precedence is TOML, then
  `FERROSA_JSONB_*` environment, then default. Startup fails on a zero value, on a
  value above its ceiling, and on `max_input_bytes` or `max_encoded_bytes` above the
  write-path maximum (the commit-log segment size, passed in by the binary).
- `HardCeilings`: compiled read ceilings (D14b): depth 1000, 131072 digits before
  and 16383 after the decimal point (D14a), 256 MiB encoded (D14d).
- `DuplicateKeyPolicy` (D6b): `LastWins` (default) or `Error`.

## Implemented (T-101)

- `Number`: exact decimal (`i64`, `i128` with scale, or `BigInt` with scale). Built
  by `Number::parse_lexeme`, `from_i64`, `from_u64`, `from_f64_shortest`. Scale is
  preserved (`1.10` stays scale 2). `Display` prints canonical text with scale.
  `Eq`, `Ord` and `Hash` compare by value (`1.0 == 1 == 1.00`).
- `NumberKind` via `Number::kind()`: the smallest Variant kind (int8..int64,
  decimal4/8/16, `ferrosa.bigdecimal`); u64 above `i64::MAX` is decimal16 scale 0 (T11).
- D14a digit caps run on the lexeme before any `BigInt` is built (JB-D2).
- New errors: `InvalidNumber { offset }`, `NonFiniteNumber`.

## Implemented (T-102)

- `JsonbBuilder`: begin/end/key/scalar events, iterative (no recursion, depth
  bounded by `Limits::max_depth`), size checked as values arrive. `finish()` returns
  `Encoded { bytes, duplicate_keys_dropped }`: the canonical cell, `0xF1` envelope
  then Variant v1 metadata then value (architecture 3.2, C1-C11 as amended by D2a).
- Canonical form: sorted unique dictionary, minimal offset and field-id widths, the
  smallest numeric kind at the stored scale (decimals keep their scale), no
  Float/Double. `ferrosa.bigdecimal` is primitive id 63 (header `0xFC`, zigzag-varint
  scale, unsigned-varint length, minimal two's-complement big-endian unscaled) and is
  used only when `decimal16` cannot hold the value.
- Duplicate keys: last wins, counted in `duplicate_keys_dropped`; under
  `DuplicateKeyPolicy::Error` they are `JsonbError::DuplicateKey` (D6b).
- New error: `JsonbError::BuilderMisuse { reason }` for out-of-order events.
- Tests: hand-derived byte tests (`tests/hand_bytes.rs`), a golden corpus of 237
  values (`tests/golden/canonical_v1/corpus.txt`, `tests/golden_v1.rs`) and the
  primitive-table conformance test that pins id 63 as unassigned upstream.

Not yet implemented: the validator and reader (T-103 on), text parse and print.

## Safety

`#![forbid(unsafe_code)]`; clippy denies `unwrap_used`, `expect_used`, `panic` and
`indexing_slicing` outside tests (`clippy.toml` relaxes them for tests only).

Docs: [specs/overview.md](specs/overview.md), [specs/fmea.md](specs/fmea.md),
[specs/roadmap.md](specs/roadmap.md).
