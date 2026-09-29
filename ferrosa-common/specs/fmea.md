---
crate: ferrosa-common
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-common — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). This is the leaf crate, so its failure modes propagate to
every crate above it — severities are correspondingly high.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| FC-1 | `HybridLogicalClock::wall_clock_ns()` panics via `.expect("system clock before Unix epoch")` | A clock anomaly (NTP step before epoch / VM clock glitch) panics the thread on **every** `now()`/`merge()` — i.e. on the Accord hot path. Violates "check all returns" (no `Result`) | 8 | 2 | 5 | 80 | **Open.** `now()`/`merge()` return owned values, not `Result`; the only failure path is a hard panic. Should saturate-to-0 or surface a typed clock error. See roadmap Now. |
| FC-2 | Murmur3 drift from Cassandra (e.g. tail sign-extension "fixed") | Every key re-tokenizes → silent, total misplacement vs. existing SSTables and ring | 10 | 1 | 4 | 40 | Characterization vectors from Cassandra source (`murmur3::tests`, `tools/generate_murmur3_vectors.java`); the sign-extension asymmetry is documented as intentional. |
| FC-3 | `CqlValue` cross-type `Ord` regresses (discriminant index or `total_cmp` changed) | Sorted structures keyed on `CqlValue` mis-order → wrong query results / index corruption | 9 | 2 | 6 | 108 | `total_cmp` for float/double/vector + fixed `discriminant_index`; covered by `cql_type::tests` ordering cases, but the **full** cross-type matrix is not exhaustively property-tested. |
| FC-4 | `parse_wkb` rejects valid-but-unsupported geometry as an error (only Point + single-ring Polygon; antimeridian deferred to P2-d) | Callers needing MultiPolygon / holes / antimeridian get `InvalidData`, not a result | 4 | 4 | 2 | 32 | **Designed fail-loud** (clear error messages, not silent NULL). Tracked as a feature gap, not a bug. |
| FC-5 | `TaskPool::current()` silently falls back to ambient `tokio::spawn` | Subsystem work lands on the default runtime instead of its tunable pool → contention (cf. the Raft-starvation class of bug) without a hard signal | 6 | 4 | 6 | 144 | **Documented but quiet.** Doc comment warns hot paths should prefer `runtime()`, but `current()` does not log/meter when it spawns on the ambient runtime — a degradation with no observable line. |
| FC-6 | New `#[non_exhaustive]` `Error` variant (or any `DataType` variant, now exhaustive) breaks downstream matches | Compile break (or worse, a wildcard arm silently swallows a new error) | 5 | 2 | 3 | 30 | `#[non_exhaustive]` is deliberate; downstream wildcard arms must be audited when variants are added. |
| FC-7 | Legacy `Error::is_backpressure()` still classifies older overloads by substring match | A reworded legacy error can lose overload classification | 6 | 3 | 7 | 126 → 36 | **Mitigated:** new storage hard-pressure errors use `Error::Overloaded { reason, table }`; consumers match the variant. `is_backpressure()` retains old strings for rolling upgrades and cross-crate errors, and CQL logs when this fallback is used. |
| FC-8 | `test-generators` proptest strategies cover only `CellValue`/keys, not `CqlValue`/`CqlType` | Downstream property tests can't exercise the full value space from the shared generators | 3 | 5 | 4 | 60 | Generators exist for cells + keys; a `CqlValue` strategy would let every consumer property-test round-trips. Roadmap Next. |

## Top risks to act on

1. **FC-5 (RPN 144)** — `TaskPool::current()` is a *silent* degradation: it
   spawns on the ambient runtime with no log/metric. Given Ferrosa's history of
   runtime-contention bugs (Raft heartbeat starvation), an un-instrumented
   fallback onto the wrong runtime is exactly the kind of "looks fine, runs hot"
   failure the fail-loud rule warns against. Add a one-shot `warn!` + a counter.
2. **FC-7 (RPN 36 residual)** — legacy string matching remains for older
   storage/cluster errors; new storage admission errors use the typed overload
   variant and CQL logs when this fallback matches.
3. **FC-3 (RPN 108)** — `CqlValue` total order underpins index/sort correctness;
   add a property test over the full cross-type matrix as a regression net.

## Detection assets

- `murmur3::tests` — Cassandra characterization vectors (bit-exact placement).
- `accord::tests` (28 tests) — HLC monotonicity, drift rejection, ballot roles,
  `TxnState` transitions.
- `cql_type::tests` / `schema::tests` / `geometry::tests` — type IDs, ordering,
  marshal-type validation, WKB round-trips and error paths.
- `key::tests` / `token::tests` / `cell::tests` — ring order, tie-breaking,
  cell-state predicates.

**Detection gaps:** no in-crate test forces the HLC clock-error path (FC-1), no
exhaustive `CqlValue` ordering property test (FC-3), and no observability on the
`TaskPool::current()` fallback (FC-5).

## T-022 type-name registry

| ID | Failure mode | Effect | Detection | Mitigation |
|----|--------------|--------|-----------|------------|
| COM-T022-01 | A scalar name is added to one front-end switch but not another (drift, FM-20) | Same column type parses in one path and errors or mis-marshals in another | `type_names_round_trip_every_scalar`, consumer-agreement tests in `ferrosa-cql` and `ferrosa-schema` | One registry in `cql_type::names`; every former switch delegates to it |
| COM-T022-02 | A new `CqlType` variant is omitted from the registry | Variant has no name or marshal class | Exhaustive `entry` match (compile error); `SCALAR_TYPES` length asserted in the round-trip test | No `_ =>` arm on `CqlType` in the registry |
| COM-T022-03 | Case-sensitivity mismatch between callers (`cql_to_marshal_type` is exact, parsers fold case) | `INT` marshals as pass-through text | `lookup_is_exact_and_ci_variant_folds_case` pins both behaviours | `scalar_from_name` exact, `scalar_from_name_ci` folded; callers keep prior behaviour (drift recorded, not fixed) |
