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
Value equality arrives with the reader (T-103 on).

## Incremental enforcement

`Limits::check_*` and `HardCeilings::check_*` are pure comparisons meant to be
called as bytes, levels and digits arrive, so a hostile document is refused before
it is buffered. Depth walkers must be iterative.
