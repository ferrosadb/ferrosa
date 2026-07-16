# Geospatial Index — design proposal

> Docs triage note (2026-07-15): moved from `specs/todo/` to `specs/implemented/`.
> Implementation evidence: `ferrosa-index/src/geo/` implements geo encoding,
> coverings, k-NN/refinement, polygon predicates, and R-tree helpers;
> `ferrosa-cql/src/router.rs` implements `GEO_NEAREST`, `GEO_WITHIN_RADIUS`,
> `GEO_WITHIN_BBOX`, and `ST_WITHIN` query routing.
> Verification run: `cargo test -p ferrosa-cql --lib geo_` (11 passed).

**Status:** Implemented and locally verified.
**Author:** blueprint, 2026-06-10.

## 1. Where we are today

Ferrosa has **no geospatial support**: no geometry column type, no spatial index,
no spatial predicates. The secondary-index catalog is `IndexType { BTree, Hash,
Composite, Phonetic, Filtered, Vector, FullText }` (`ferrosa-index/src/lib.rs:99`).

The **Vector** index is the right template to copy. It is the only existing index
that is *not* a comparison index — it answers "nearest" rather than "equals/range",
which is exactly the shape a geo index needs. It already establishes every seam a
new "search-shaped" index must pass through:

- a parallel trait set (`ferrosa-index/src/vector/mod.rs`: `IndexBuilder`/`IndexReader`
  with a `nearest()` method, distinct from the generic secondary-index traits in
  `ferrosa-index/src/lib.rs:214`);
- a per-generation **sidecar** artifact (`{gen}-VEC-{name}.db`, `{gen}-QVEC-{name}.qvec`
  — `ferrosa-storage/src/flush.rs:1275`);
- a **method enum** with build/search dispatch (`VectorIndexMethod` in
  `ferrosa-storage/src/store.rs:377`, search dispatch ~`store.rs:3873`);
- a **CQL surface** (`ORDER BY col ANN OF [..] LIMIT k`, parsed/ranked in
  `ferrosa-cql/src/router.rs:248`);
- DDL option parsing (`resolve_vector_index_method`, `router.rs:6145`).

## 2. Scope & strategy (phased)

Geometry support is a spectrum. Do it in two phases so Phase 1 ships value fast by
**reusing the BTree pipeline**, and Phase 2 adds a native tree only if arbitrary
geometry is required.

| | Phase 1 — Point index (recommended first) | Phase 2 — Geometry index (follow-up) |
|---|---|---|
| Data | `geo_point` (lat/lon) only | points, lines, polygons (WKB) |
| Structure | **space-filling curve** (S2 cell id / geohash → `u64`) stored in the **existing BTree** sidecar | native **R-tree** (`rstar`) as a dedicated sidecar, mirroring Vector |
| Queries | bbox, radius (k-NN + within-radius) | `ST_Within`/`ST_Intersects`/`ST_Contains`, exact distance |
| New code | an *encoder* + a geo-aware *builder* (reuses BTree storage/reader) + a *query rewriter* | full parallel index (build/serialize/search), like `vector/` |
| Effort | **moderate** (~1–2 wk) | **large** (~3–5 wk) |
| Deps | `s2` (or `geohash`) + `geo`/`haversine` for refine | `rstar`, `geo`, `wkb`/`geozero` |

**Recommendation:** ship Phase 1 (points) first. Point-in-radius and bbox cover the
large majority of geo workloads (stores near me, geofences, "within N km"), and the
space-filling-curve approach piggybacks on the BTree build/query/sidecar machinery
already in production — the new index type is mostly an *encoder* and a *query
rewriter*, not a new on-disk structure.

### Why a space-filling curve for Phase 1

Encode each point to a sortable 1-D key (S2 cell id at a fixed level, or a geohash
prefix). Nearby points share key prefixes, so a **bbox/radius query becomes a small
set of contiguous key ranges** (S2 "covering") over the existing BTree, followed by
an exact **haversine** refinement to drop the cells' false positives. k-NN ("nearest
K") is an expanding-ring cell search + refine. This reuses `IndexReader::range`
(`ferrosa-index/src/lib.rs`) verbatim — only key derivation and query planning are new.

## 3. Column type

CQL has no native geo type. Phase 1 options, simplest first:

1. **`frozen<tuple<double,double>>`** (lat, lon) — zero new type machinery; the geo
   index's builder reads the two doubles. Pragmatic MVP.
2. A marshalled **`PointType`** (custom `org.apache.cassandra.db.marshal`-style type,
   16 bytes: two `f64`). Cleaner surface; needs a new type in `ferrosa-common`/marshal.
3. Phase 2: **`GeometryType`** storing WKB (well-known binary) for arbitrary geometry.

Recommend (1) for Phase 1 to avoid type-system surgery; revisit (2) with Phase 2.

## 4. CQL query surface (mirror `ANN OF`)

Phase 1 predicates (parse + plan in `ferrosa-cql`, dispatch like the vector path):

```sql
-- k nearest
SELECT ... FROM places ORDER BY location GEO_NEAREST OF (37.77, -122.42) LIMIT 20;
-- within radius (meters), optionally with k cap
SELECT ... FROM places WHERE GEO_WITHIN_RADIUS(location, (37.77,-122.42), 1500);
-- bounding box
SELECT ... FROM places WHERE GEO_WITHIN_BBOX(location, (37.70,-122.52), (37.83,-122.35));
```

These mirror `apply_ann_of_ordering` (`router.rs:248`): the planner detects the geo
function, asks the geo index for candidate `RowPosition`s (cell ranges), then refines
with exact distance/containment before returning.

## 5. Implementation map (every seam to touch)

Grounded in the Vector index's path. A new `IndexType::Geo` variant must be added at
each switch site below (these are *exhaustive* matches, so the compiler will flag
any miss once the variant exists — a useful forcing function):

1. **`ferrosa-index/src/lib.rs`** — add `Geo` to `IndexType` (l.99) + `Display` (l.111).
2. **`ferrosa-index/src/geo/`** *(new module, modeled on `vector/`)* — the encoder
   (`point → cell_id`), the geo-aware builder (extract point cell → `(IndexKey,
   RowPosition)`, reusing BTree sidecar in Phase 1), and the query planner helpers
   (covering ranges, expanding-ring k-NN, haversine/bbox refine). A `GeoCrs`/distance
   enum is the analog of `DistanceMetric` (`lib.rs:123`) — `Wgs84Spherical` (haversine)
   vs `Planar`.
3. **`ferrosa-cql/src/router.rs`** — `resolve_index_type()` add `Some("geo") =>`
   (l.6128); add geo option parsing analogous to `resolve_vector_index_method`
   (l.6145) for `crs`, `level`/`precision`; system_schema kind map (l.2118); the geo
   query functions in the planner (analogous to `apply_ann_of_ordering`, l.248).
4. **`ferrosa-schema/src/system/index_tables.rs`** — `index_type_kind()` add `Geo =>
   "geo"` (l.65). `IndexMetadata.options` (`metadata/index.rs:7`) already carries the
   `crs`/`level` knobs — no struct change.
5. **`ferrosa-index-builder/src/worker.rs`** — `parse_index_type()` add `"geo"` (l.305)
   so the remote builder can build geo indexes.
6. **`ferrosa-storage/src/memtable/eager_index.rs:63`** — **fix the existing hardcoded
   `IndexType::BTree`** (it stamps every eager-built index as BTree). This is a real
   bug that already mis-stamps non-BTree indexes; read the index type from schema.
   Geo (and correct query dispatch) depends on this being right.
7. **`ferrosa-storage/src/index/{scheduler.rs,remote_backend.rs}`** — Phase 1 geo
   builds through the existing `LocalBackend`/remote secondary path (it produces a
   sorted sidecar); the only addition is geo key-derivation in the builder. Add geo to
   the `remote_backend.rs:278` artifact dispatch only if Phase 2 uses a distinct file.
8. **`ferrosa-storage/src/store.rs` + `flush.rs`** — Phase 1: none beyond the builder
   (reuses `write_secondary_sidecar`, `flush.rs:1257`). Phase 2: add a geo method enum
   + `write_geo_sidecar` (`{gen}-GEO-{name}.rtree`) + search dispatch, exactly mirroring
   `VectorIndexMethod` / `write_vector_sidecar`.

## 6. Phased plan

**Phase 1 — Point geo index (S2/geohash over BTree)**
1. `geo` module in `ferrosa-index`: cell encoder, covering-range + expanding-ring
   planners, haversine/bbox refine. Pure, fully unit-testable (no storage). *(TDD core.)*
2. `IndexType::Geo` + the switch sites (§5.1, .3, .4, .5). Compiler-driven.
3. Fix `eager_index.rs:63` hardcoded BTree (read type from schema).
4. Geo-aware builder: read the `tuple<double,double>` cell → cell id → reuse the
   BTree sidecar writer.
5. CQL `GEO_NEAREST OF` / `GEO_WITHIN_RADIUS` / `GEO_WITHIN_BBOX` parse + plan +
   index dispatch + exact refine.
6. Tests: round-trip build/query; recall vs brute-force on a fixture point set;
   antimeridian / pole edge cases; refine correctness.

**Phase 2 — Geometry index (R-tree)**
7. `GeometryType` (WKB) column type + marshal.
8. Native R-tree geo index (parallel trait set + `-GEO-` sidecar + search dispatch),
   mirroring `vector/` and `VectorIndexMethod`.
9. `ST_Within`/`ST_Intersects`/`ST_Contains`, exact predicates, polygon/line support.

## 6a. Phase-2 status & this PR's slice

Phase 1 (point index over BTree, `GEO_NEAREST` / `GEO_WITHIN_RADIUS` /
`GEO_WITHIN_BBOX` / `ST_WITHIN(point, polygon)`) is **implemented and tested** on
main: `ferrosa-index/src/geo/` carries the encoder, cover/knn planners, haversine
refine, single-ring `Polygon` + ray-cast, and a bulk-loaded STR `Rtree`. The CQL
surface dispatches through `route_geo_select` with EXPLAIN `GeoIndex` and
`index_usage` accounting.

Phase 2 is decomposed into independent vertical slices so each can land green:

| Slice | What it adds | Status |
|---|---|---|
| **P2-a — R-tree in the live `ST_WITHIN` path** | use the existing `geo::rtree` to prune polygon-bbox candidates before the exact ray-cast | landed |
| **P2-b — stored `GEOMETRY` column (WKB)** | a marshalled geometry type in `ferrosa-common`, WKB parse/serialize, round-trip | **this PR — landed** |
| **P2-c — `ST_INTERSECTS` / `ST_CONTAINS`** | predicates between two *stored* geometries, backed by P2-b + the R-tree sidecar | **this PR — algorithmic core landed; CQL surface deferred** |
| **P2-d — rich geometry** | multi-ring polygons (holes), linestrings, antimeridian splitting, native `-GEO-` sidecar | deferred |

### What THIS PR (P2-b + P2-c core) delivers

- **P2-b — stored `GEOMETRY` column type, WKB-marshalled** in
  `ferrosa-common/src/geometry.rs`: a `Geometry` enum (`Point` + single-outer-ring
  `Polygon` over `(lat, lon)` degrees), with `marshal_wkb` / `parse_wkb` against
  the OGC WKB byte format. Serialization emits little-endian (NDR) and closes
  polygon rings explicitly; parsing accepts either byte order. Round-trip and
  edge-case tests pass (17): point/polygon round-trip, big-endian parse,
  explicit-vs-open ring equivalence, and **loud rejection** (no silent wrong
  answer) of unknown byte-order flags, unsupported geometry types (`LineString`,
  `MultiPolygon`, Z/M), multi-ring/hole polygons, degenerate rings, truncated
  buffers, trailing bytes, and antimeridian-crossing polygons.
- **P2-c — algorithmic `ST_CONTAINS` / `ST_INTERSECTS` core** in
  `ferrosa-index/src/geo/predicate.rs`: `st_contains` / `st_intersects` over two
  stored `ferrosa_common::Geometry` values, bridging the marshalled geometry to
  the existing `point_in_polygon` ray-cast. Exact for Point/Point, Polygon/Point,
  and Point/Polygon (boundary counts as inside; `ST_INTERSECTS` is symmetric).
  Polygon-vs-polygon returns `PredicateError::UnsupportedPair` (needs ring-edge
  crossing detection — deferred to P2-d) rather than a plausible-but-wrong answer.
  10 unit tests cover the SF-square / Ferry-Building / NYC fixtures, symmetry,
  boundary inclusion, and the loud polygon-vs-polygon rejection.

#### Already landed (P2-a, prior PR)

- A pure, tested `geo::points_in_polygon_rtree(candidates, polygon)` helper
  (`ferrosa-index/src/geo/geometry.rs`): bulk-loads the candidate points into the
  `Rtree` (degenerate point bboxes), queries it with the polygon's exact bounding
  box to **prune off-bbox candidates in O(log n)**, then runs `point_in_polygon`
  only on the survivors. Verified equal to brute-force ray-casting (including a
  concave-notch case and degenerate/empty inputs).
- Wires that helper into the live single-polygon `ST_WITHIN` query path
  (`ferrosa-cql/src/router.rs::route_geo_select`), replacing the prior brute-force
  `filter(point_in_polygon)` over every fetched candidate. The cell cover is a
  coarse over-approximation of the polygon bbox, so the R-tree prune removes the
  rows the cover pulled in beyond the true bbox before any ray-cast runs.
- End-to-end coverage unchanged and still green: the central-SF and concave-notch
  `ST_WITHIN` E2E tests both assert EXPLAIN `GeoIndex` + the `index_usage`
  increment, and now exercise the R-tree-pruned path.

### What THIS PR defers (see `remaining`)

- **CQL surface for `GEOMETRY` + `ST_CONTAINS` / `ST_INTERSECTS`** — the column
  type is not yet exposed through DDL/`CqlType`/`CqlValue`, and the predicates are
  not yet wired into `router.rs` query planning. This PR ships the marshal
  foundation (P2-b) and the exact predicate core (P2-c) so the remaining work is
  pure plumbing: a `CqlType::Geometry` (Custom-typed on the wire, like Vector), a
  `CqlValue::Geometry(Vec<u8>)` carrying WKB, DDL `geometry` type resolution, and
  `route_geo_select` dispatch that decodes two stored geometries and calls
  `geo::st_contains` / `geo::st_intersects`.
- **Polygon-vs-polygon `ST_*`** — needs ring-edge crossing detection; the core
  rejects it loudly today (`PredicateError::UnsupportedPair`).
- **Multi-ring polygons / holes, linestrings, antimeridian-crossing polygons**
  (P2-d). `parse_wkb` rejects all of these loudly (no silent wrong answer), as
  the prior `ST_WITHIN` antimeridian path does.

## 7. Risks & open questions

- **CRS / distance:** Phase 1 should default to WGS84 spherical (haversine); planar is
  a fast-path option. Document that radius is meters on the sphere.
- **Edge cases:** antimeridian (±180° lon) and polar regions break naive geohash boxes;
  S2 handles these better than geohash — prefer **S2** for Phase 1.
- **Recall vs cost:** cell level trades index selectivity against refine work; expose
  `level`/`precision` as an index option and pick a sane default (e.g. S2 level 12 ≈
  ~2 km cells) with query-time covering expansion.
- **Multi-SSTable merge:** like all secondary indexes, geo candidates are gathered
  per-generation then merged — reuse the existing per-sidecar fan-in.
- **Write path:** point encoding happens at index-build time (flush + compaction), not
  on the hot write path — same as vector. No CQL write-path change for Phase 1.
- **`eager_index.rs:63` bug** is a prerequisite, not optional — without it any non-BTree
  index (including geo, and arguably the eager-built indexes today) is mis-typed.

## 8. Bottom line

Adding **point** geospatial indexing is a *moderate* effort that reuses the BTree
pipeline: a new `ferrosa-index/src/geo` encoder/planner, the `IndexType::Geo` switch
sites, one real bug fix (`eager_index.rs:63`), and a CQL geo-predicate surface modeled
on the existing `ANN OF` vector path. Full **geometry** (polygons, `ST_*`) is a larger
follow-up that adds a native R-tree index in the exact shape the Vector index already
demonstrates.
