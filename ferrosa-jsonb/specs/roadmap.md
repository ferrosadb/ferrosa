---
crate: ferrosa-jsonb
doc: roadmap
last_updated: 2026-09-28
---

# ferrosa-jsonb — roadmap

## Now

- T-102 done: canonical encoder, builder, bigdecimal primitive 63. Still open from
  T-101: a clippy `disallowed-methods` entry restricting f64 parsing to `number.rs`.
- Integration tests are separate crates, so they carry their own
  `#![allow(clippy::expect_used, ...)]`; `clippy.toml` covers `cfg(test)` only.
- Wire `Limits::from_config` into `ferrosa/src/main.rs`, passing the commit-log
  segment size (`DEFAULT_SEGMENT_SIZE`, 32 MiB on main), and map the TOML section
  into `LimitsConfig`. Not done in T-100: the binary has no jsonb dependency yet.

## Next

- Validator and reader (T-104), print, export.
- T-103 done (streaming parser). Open: a counting-allocator peak-allocation test for
  `WORKING_SET_MULTIPLE` (the crate forbids `unsafe`, which a `GlobalAlloc` needs, so
  it must live in a separate dev crate); adapter wiring of `DuplicateKeyObserver`
  to the metric and edge log line.

## Later

- Confirm the ceilings chosen for `max_key_list_length`, `max_index_terms_per_doc`,
  `max_path_len` and `path_step_budget` (D14b fixes only depth, digits and size).
- D14d (256 MiB read ceiling) is open to stakeholder override.
