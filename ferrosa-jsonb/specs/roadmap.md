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

## T-106 open items

- T-106 done (order, Eq, Hash, Debug, serde). Open: `JsonbValue::view()` revalidates the
  cell on every compare, hash and print (a cached `Meta` would remove the second pass);
  no comparison work budget (the walk is O(size) plus one sort per object);
  `comparison_faults()` must be exported as a metric by the adapter; the PG order table is
  from documentation, the live postgres:16 diff is T-301.

## Next

- T-105 done (printers, output budget). Open: PgText is verified against documented
  PostgreSQL 16 behavior and the source rules, not yet a live postgres:16 differential
  (T-301); an `io::Write` sink adapter; `PgText` key sort allocates one Vec per object.

- T-104 done (validator, checked reader, fuzz target). Open: run the `validate`
  fuzz target for a soak (needs `cargo fuzz`); validate-time cost of the UTF-8 pass
  is paid again on `as_str` (measure before caching); print (T-105), export.
- T-103 done (streaming parser). Open: a counting-allocator peak-allocation test for
  `WORKING_SET_MULTIPLE` (the crate forbids `unsafe`, which a `GlobalAlloc` needs, so
  it must live in a separate dev crate); adapter wiring of `DuplicateKeyObserver`
  to the metric and edge log line.

## Later

- Confirm the ceilings chosen for `max_key_list_length`, `max_index_terms_per_doc`,
  `max_path_len` and `path_step_budget` (D14b fixes only depth, digits and size).
- D14d (256 MiB read ceiling) is open to stakeholder override.
