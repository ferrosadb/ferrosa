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

## Incremental enforcement

`Limits::check_*` and `HardCeilings::check_*` are pure comparisons meant to be
called as bytes, levels and digits arrive, so a hostile document is refused before
it is buffered. Depth walkers must be iterative.
