---
crate: ferrosa-common
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-common — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), in-code markers, and the
dependency/usage review. The crate has **no `TODO`/`FIXME`/`unimplemented!`
markers in source** — the items below come from real gaps found while reading
the code, not from leftover tags.

## Now (highest value)

- **Make the HLC clock read fail-safe (FMEA FC-1).** `wall_clock_ns()` calls
  `.expect("system clock before Unix epoch")` on the Accord hot path; a clock
  step before the epoch panics the thread inside `now()`/`merge()`. Either
  saturate the duration to `0` (with a one-shot warning) or thread a typed clock
  error out of `now()`. No node should crash because NTP moved the wall clock.
- **Instrument `TaskPool::current()` (FMEA FC-5).** The ambient-runtime fallback
  is undocumented at runtime: emit a one-shot `warn!` and a counter the first
  time a named pool spawns on `tokio::spawn` instead of its dedicated runtime, so
  the "wrong runtime" degradation is observable (consistent with the fail-loud
  rule and the Raft-starvation history).

## Next

- **Promote backpressure to a typed `Error` variant (FMEA FC-7).**
  `is_backpressure()` substring-matches `InvalidData` messages
  (`"...below write reserve"`, `"overloaded:"`). Add an explicit
  `Error::Overloaded`/`Backpressure` variant so load shedding can't break when a
  message string is reworded — mirroring the typed `CorruptSstable` design.
- **Property-test `CqlValue` total order (FMEA FC-3).** Add a proptest that the
  cross-type `Ord` (discriminant index + `total_cmp`) is a true total order
  across the full variant space — index and sort correctness depend on it.
- **Extend `test-generators` to `CqlValue`/`CqlType` (FMEA FC-8).** Today the
  shared proptest strategies cover only `CellValue` and keys; a `CqlValue`
  strategy would let every downstream crate property-test value round-trips from
  one place.

## Later

- **Broaden geometry support (FMEA FC-4).** `parse_wkb`/`marshal_wkb` handle only
  Point and single-ring Polygon, and reject antimeridian-crossing polygons
  ("deferred to P2-d"). Add MultiPolygon / interior rings / antimeridian handling
  when a consumer needs them — the current behavior is a documented fail-loud
  limitation, not a bug.
- **Audit `#[non_exhaustive]` downstream matches (FMEA FC-6).** When new
  `Error`/`DataType` variants are added, sweep downstream wildcard arms so a new
  error isn't silently swallowed by a catch-all.

## Non-goals

- Wire-format CQL encode/decode — lives in `ferrosa-cql` / `ferrosa-row-bridge`.
- Any dependency on another Ferrosa crate — this crate is the leaf by design;
  taking an upward dependency would re-introduce the cycle the type moves
  (`CqlType`/`CqlValue`, `TableSchema`) were made to break.
- Consensus orchestration, storage layout, transport — only the shared *types*
  for them live here.
