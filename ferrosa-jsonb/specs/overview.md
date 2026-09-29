---
crate: ferrosa-jsonb
doc: overview
last_updated: 2026-09-28
---

# ferrosa-jsonb — overview

Design source: `ferrosa-suite/specs/jsonb/` (decisions D5a, D6b, D14, D14a, D14b,
D14d; component K1). This document describes what the crate does today.

## Position

Leaf crate. Dependencies: `bytes`, `num-bigint`, `thiserror`. Dependents: none yet;
the `ferrosa` binary will call `Limits::from_config` at startup.

## Limits (ingest) and ceilings (read)

Tunable `Limits` gate writes and query arguments only. Reads validate against
`HardCeilings`, so lowering a tunable never makes a stored cell unreadable (D14b,
FM-17).

| Limit | Default | Ceiling | Env override |
|-------|---------|---------|--------------|
| `max_input_bytes` | 10 MiB | 256 MiB | `FERROSA_JSONB_MAX_INPUT_BYTES` |
| `max_encoded_bytes` | 10 MiB | 256 MiB | `FERROSA_JSONB_MAX_ENCODED_BYTES` |
| `max_nesting_depth` | 1000 | 1000 | `FERROSA_JSONB_MAX_NESTING_DEPTH` |
| `max_key_list_length` | 1000 | 1,000,000 | `FERROSA_JSONB_MAX_KEY_LIST_LENGTH` |
| `max_index_terms_per_doc` | 10,000 | 10,000,000 | `FERROSA_JSONB_MAX_INDEX_TERMS_PER_DOC` |
| `max_path_len` | 4 KiB | 1 MiB | none |
| `path_step_budget` | 1,000,000 | 1,000,000,000 | none |

The ceilings for the last four rows are a T-100 choice; D14b fixes only depth,
digits and encoded size.

`HardCeilings`: depth 1000, 131072 digits before and 16383 after the decimal
point, 256 MiB encoded.

## Startup validation

`Limits::from_config` refuses: a zero tunable, a tunable above its ceiling, an
unparseable env value, and `max_input_bytes` / `max_encoded_bytes` above the
write-path maximum passed by the caller (commit-log segment size, D14d). Precedence
is TOML, then env, then default. `from_config_with_env` injects the environment so
tests never call `set_var`.

## Number (T-101)

`Number` is exact: no implicit `f64`. `parse_lexeme` follows architecture C9. The
stored scale is `max(0, F - e)` for `F` fraction digits and exponent `e`; the
unscaled integer is rescaled to match, `-0` is `0`, and no trailing zero is
stripped. Order of work: (1) grammar scan, (2) digit counts in `i64` arithmetic
(exponent saturates, so `1e2147483648` cannot overflow), (3) `HardCeilings::check_digits`,
(4) only then `BigInt::parse_bytes`. An integer part of 147456 digits or more is
refused without scanning further, so a 10 MiB single-number document fails in
microseconds.

Representation is canonical: `I64` only at scale 0, else `I128`, else `Big`.
`Eq`/`Hash`/`Ord` use a value form (unscaled, scale) with trailing zeros removed to
scale 0, for comparison only (D2a). `from_f64_shortest` uses Rust's shortest
round-trip formatting; integral floats get scale 1; NaN and infinities are
`NonFiniteNumber`. `to_f64_if_shortest_round_trips` returns `Some` only when the
`f64` re-renders to the same value.

## Canonical encoder and builder (T-102)

`JsonbBuilder` (builder.rs) turns events into an arena of nodes (scalars are
pre-encoded into one byte pool; containers hold child indices) with an explicit frame
stack. `encode_tree` (encode.rs) then writes the cell in three iterative steps:
(1) collect the sorted unique key dictionary, (2) compute every node's encoded size
children-first (a child's arena index is always above its parent's, so a reverse scan
needs no stack), (3) check the total against `Limits` and `HardCeilings`, allocate
once, and write depth-first with an explicit stack. Object entries are sorted by key
bytes when the object closes, so field ids ascend (C5).

Cell: `0xF1`, metadata (`header 0x11 | (offset_size-1)<<5`, dictionary size, offsets,
key bytes), value. Numbers: int8/16/32/64 at scale 0; decimal4/8/16 (scale byte then
little-endian unscaled) by magnitude and scale; otherwise primitive 63.

Size enforcement is incremental: every attached node adds its smallest possible
encoding to a running lower bound checked against `max_encoded_bytes` and the hard
ceiling, so an oversized document fails while being built. The bound counts a
duplicate key's value until the object closes, then returns it; the exact size is
checked again before the output is allocated.

Byte equality implies value equality, not the reverse (`1.0` and `1` differ in bytes).
Value equality arrives with the reader (T-105 on).

## Validator and reader (T-104)

`reader.rs` holds `Meta` (dictionary), `Head` (container header and tables) and the
public `JsonbRef`, `ValueRef`, `ObjectRef`, `ArrayRef` views; `validate.rs` holds the
`Walker`. `validate_cell` checks size, envelope, metadata, then walks the value with a
frame stack: each frame is one open container with a cursor and the previous field
id. A container is checked when opened (offset 0 is zero, offset n equals the child
region, `is_large` iff count > 255, minimal offset and id widths); each child is
checked when visited (ids strictly ascending and in range, offsets monotone and in
bounds, scalar at its canonical kind). A bitset over dictionary ids proves every key
is used (C3). Numbers decode through the same `decode_number` the reader uses and
must report the kind their header claims, so the canonical kind rule lives in one
place (`Number::kind`). Bigdecimal digit caps compare bit length to `10^(131072 +
scale)` and build the exact power only within one bit of the boundary.

## Streaming parser (T-103)

`parse_text` (parse.rs) checks input size, runs `prescan_depth` (a one-pass bracket
counter that ignores strings and escapes), then a `Parser` drives `JsonbBuilder` with
an explicit `Frame` stack: no recursion, so nesting costs heap not stack. A string is
scanned as runs between delimiters and each run is validated as UTF-8 (a multi-byte
character never holds an ASCII delimiter). `\u` pairs join; a lone surrogate is an
error at its backslash. Numbers are scanned strictly (no leading zeros, digits around
`.`, exponent digits) and handed to `Number::parse_lexeme` as a lexeme. The builder
resolves duplicates; the parser rewrites its `DuplicateKey` with the object's path.
Working set: input + O(depth) frames + arena, documented as at most 32x input.

## Text printer (T-105)

`print.rs` walks a `ValueRef` with an explicit `Frame` stack (array iterator, object
iterator, or a sorted entry list). `Printer::emit` is the only write path: it checks
`written + len <= budget` before writing, so an over-budget print fails with
`BudgetExceeded` having written nothing past the budget. Styles differ in three
places only: separators (`,`/`:` versus `, `/`: `), object key order (`PgText` sorts
one object's entries by byte length then bytes, as `lengthCompareJsonbStringValue`
does), and number text (`Normalized` prints `Number::value_normalized`). Strings use
one `escape_json` implementation for all styles. Numbers print through `Number`'s
`Display`, which is plain decimal with scale. The three text forms are frozen (they
feed the D13a hash); a change needs a new golden version.

## Incremental enforcement

`Limits::check_*` and `HardCeilings::check_*` are pure comparisons meant to be
called as bytes, levels and digits arrive, so a hostile document is refused before
it is buffered. Depth walkers must be iterative.
