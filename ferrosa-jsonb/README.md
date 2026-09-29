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

## Implemented (T-103)

- `parse_text(&[u8], &Limits) -> Result<Encoded, JsonbError>`: RFC 8259 text to
  canonical jsonb. Order: input size, depth pre-scan (skips strings), iterative parse
  (explicit frame stack; depth 1000 passes on a 256 KiB thread, 1001 is
  `DepthExceeded`), lexeme to `Number::parse_lexeme`.
- Faults are typed with a byte offset: `Syntax { offset, reason }` (lone surrogate,
  BOM, NaN/Infinity, raw control character, trailing text), `InvalidUtf8 { offset }`.
- Duplicate keys (D6b): last wins; `parse_text_observed(.., edge, &dyn
  DuplicateKeyObserver)` reports the count once per document for the metric and edge
  log line. Under `DuplicateKeyPolicy::Error`: `DuplicateKey { path }` (`$.a[0]`).
- `InflightBudget` / `InflightPermit`: node-wide in-flight byte budget for adapters
  (JB-D5); `InflightBudget::working_set(n)` is `WORKING_SET_MULTIPLE` (32) times `n`.
- Tests: `tests/parse.rs` (y/n corpus, depth, duplicates, 10 MiB proportionality,
  proptest determinism); fuzz target `fuzz/fuzz_targets/parse_text.rs`.

## Implemented (T-104)

- `JsonbRef::validate(&[u8])` is the only way to get a reader and takes no `Limits`
  (D14b): it checks `HardCeilings` only (256 MiB, depth 1000, D14a digit caps). It
  accepts a cell iff the cell is canonical: envelope (`UnknownEnvelope { byte }`
  for 0xF2, 0x01, 0x00 and every other byte), metadata header and widths, sorted
  unique UTF-8 dictionary with every key used (C3), monotone in-bounds contiguous
  offsets, ascending in-range field ids, minimal widths and `is_large`, defined
  primitives (C10 ids are `ExcludedPrimitive`), smallest numeric kind (C9),
  bigdecimal layout, short vs long strings (C7), and no trailing bytes.
  Faults are `InvalidEncoding { reason: EncodingFault }`.
- The walk is iterative with a frame stack capped at 1000 (a 256 KiB thread
  validates depth 1000) and allocates nothing from a claimed count: every table is
  checked against the bytes that hold it before it is read, and the only sized
  allocation is a dictionary bitset bounded by the metadata bytes already checked.
- Reader: `JsonbRef::root`, `ValueRef::{kind, as_bool, as_str, as_number,
  as_object, as_array}`, `ObjectRef::{len, get, iter}` (binary search over the
  sorted field ids), `ArrayRef::{len, get, iter}`. All access is `slice::get` or
  checked arithmetic and returns `Result`; strings borrow from the cell. A wrong
  kind is `WrongKind`, never a guess.
- Two T-102 defects found by the validator and fixed here: the metadata
  `offset_size_minus_one` was written at bit 5 instead of bits 6-7 (Variant spec;
  affects dictionaries over 255 bytes or keys), and keys of a duplicate that lost
  (D6b) stayed in the dictionary (breaks C3). Seven golden cases were re-blessed;
  nothing was released on the old bytes.
- Tests: `tests/validate.rs` (every golden value validates and rebuilds
  byte-for-byte through the reader; a mutation corpus; hostile hand-built cells;
  depth on 256 KiB; proptest). Fuzz target `fuzz/fuzz_targets/validate.rs`.

Not yet implemented: text print (T-105) and export.

## Safety

`#![forbid(unsafe_code)]`; clippy denies `unwrap_used`, `expect_used`, `panic` and
`indexing_slicing` outside tests (`clippy.toml` relaxes them for tests only).

Docs: [specs/overview.md](specs/overview.md), [specs/fmea.md](specs/fmea.md),
[specs/roadmap.md](specs/roadmap.md).
