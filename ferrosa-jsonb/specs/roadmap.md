---
crate: ferrosa-jsonb
doc: roadmap
last_updated: 2026-09-28
---

# ferrosa-jsonb — roadmap

## Now

- T-101: exact `Number` with scale and the D14a digit caps on the lexeme.
- Wire `Limits::from_config` into `ferrosa/src/main.rs`, passing the commit-log
  segment size (`DEFAULT_SEGMENT_SIZE`, 32 MiB on main), and map the TOML section
  into `LimitsConfig`. Not done in T-100: the binary has no jsonb dependency yet.

## Next

- Value, canonical codec, validator, builder (K1), then text parse and export.

## Later

- Confirm the ceilings chosen for `max_key_list_length`, `max_index_terms_per_doc`,
  `max_path_len` and `path_step_budget` (D14b fixes only depth, digits and size).
- D14d (256 MiB read ceiling) is open to stakeholder override.
