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

Not yet implemented: the value, codec, validator and builder (T-102 on).

## Safety

`#![forbid(unsafe_code)]`; clippy denies `unwrap_used`, `expect_used`, `panic` and
`indexing_slicing` outside tests (`clippy.toml` relaxes them for tests only).

Docs: [specs/overview.md](specs/overview.md), [specs/fmea.md](specs/fmea.md),
[specs/roadmap.md](specs/roadmap.md).
