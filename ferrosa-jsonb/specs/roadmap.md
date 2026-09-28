---
crate: ferrosa-jsonb
doc: roadmap
last_updated: 2026-09-28
---

# ferrosa-jsonb — roadmap

## Now

- T-101 done: `Number`. Follow-ups for T-102: canonical Variant encoding of
  `NumberKind`, and a clippy `disallowed-methods` entry restricting f64 parsing to
  `number.rs` (not added in T-101).
- Wire `Limits::from_config` into `ferrosa/src/main.rs`, passing the commit-log
  segment size (`DEFAULT_SEGMENT_SIZE`, 32 MiB on main), and map the TOML section
  into `LimitsConfig`. Not done in T-100: the binary has no jsonb dependency yet.

## Next

- Value, canonical codec, validator, builder (K1), then text parse and export.

## Later

- Confirm the ceilings chosen for `max_key_list_length`, `max_index_terms_per_doc`,
  `max_path_len` and `path_step_budget` (D14b fixes only depth, digits and size).
- D14d (256 MiB read ceiling) is open to stakeholder override.
