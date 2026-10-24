# Graph identity, attributes, and index access paths

**Status:** proposed — owner-directed, awaiting implementation
**Scope:** implicit graph tables (`ferrosa-graph`), storage key derivation, derived indexes
**Supersedes:** the scalar-key and content-addressed-key models described below under "Rejected"

## Context

Three separate attempts to fix relationship `MERGE` failed on the same class of defect, and the
symptoms were always identity, never execution:

- Issue #610: the write path derived a 32-byte content-addressed key while the edge table
  declared a 16-byte `uuid` clustering column, and the read path derived 32 independently — so
  write-side-only truncation made written edges untraversable.
- A correct-at-the-parser change (bind anonymous labeled nodes under a `:Label` key) regressed
  ~15 edge/adjacency tests, because a synthesised variable name silently became part of stored
  bytes.
- Two invariant failures (`concurrent_merge_node_and_relationship_remain_idempotent`,
  `exists_pattern_predicate_filters_rows_with_matching_paths`) reproduced across every attempt
  because each one unified a different pair of derivations and left the edge/adjacency pair
  deriving separately.

Root cause: **variable name, column identity, and key material were the same thing**, and key
derivation was duplicated in the planner, the write path, and the read path.

## Decision

### D1 — Identity is the declared key tuple, any arity

Vertices and edges are the same kind of entity: a row in a table whose identity is its declared
key columns, in declared order.

```
vertex  (label,   id)
edge    (src, rel, dst)              — the implicit default
edge    (src, rel, dst, since)       — when the model needs more identity
```

Identity is a tuple of the declared key columns, not a hash and not a surrogate. When every key
column is supplied there is nothing to generate, so write and read cannot disagree.

### D2 — One derivation, no name parameter

`derive_key(label, key_columns, values) -> Key` is the only derivation, used by the write
shape, the read path, the planner's shape builder, and the raw-key rebind. Its parameter list
cannot accept a variable name, so a name is **unrepresentable** at the derivation boundary —
this is enforced by the type, not by discipline.

### D3 — Display names are separate from identity

Planner node/edge identity carries `display_name: Option<Arc<str>>` for error messages, EXPLAIN,
and column labels. It never reaches key bytes. Anonymous nodes get a generated scope id that is
likewise never persisted.

### D4 — Attributes are a typed property map, not identity

Edges and nodes carry an attribute map as payload:

```
(a)-[:MET {prob: 0.5}]->(b)
(a)-[:MET {lat: 23.000, lon: 36.0900, at: ...}]->(b)
```

Attributes describe the entity. Adding an attribute must not change what the entity *is*, or
every enrichment becomes a migration.

### D5 — An attribute may join identity only under canonical encoding

If a model genuinely requires an attribute in the key, encode deterministically:

- **sorted keys** — a map is unordered, so `{prob, at}` and `{at, prob}` must produce identical
  bytes; sort by key name at the boundary;
- **fixed-point numbers** — `0.5` is `5000` basis points, never an `f64`: `0.1 + 0.2 != 0.3`,
  `-0.0` differs from `0.0` bitwise, and `NaN != NaN` makes a MERGE fail to match itself;
- **typed tags** so a string `"5"` and a number `5` cannot collide.

Default remains D4: attributes are payload.

### D6 — Adjacency is a projection of the edge key

```
adjacency key = (node, direction, edge_key)
```

Direction lives here, not in the edge identity — `(src, rel, dst)` is already directed. An edge
is written to two adjacency rows (outgoing from src, incoming to dst), and adjacency is never
derived independently.

### D7 — Access paths are derived index tables, not key orderings

Key order is chosen for locality and canonicalisation, never for query speed. Non-identity
lookups are served by derived structures:

- **property predicates** (`prob > 0.7`) → secondary index on the attribute;
- **proximity** → a geo table keyed `(cell, entity)`, where `cell` is an H3/S2/geohash cell at a
  chosen resolution. A radius query computes covering cells, **range-scans each cell prefix**,
  then filters by exact haversine. Multi-resolution comes free by varying cell size.
- **time** → the same shape with `(bucket, ts)`.

Coordinates are stored as fixed-point microdegrees (`23.000` → `23_000_000`): exact, sortable,
hashable, no NaN.

## Consequences

**Positive.** Identity is small and stable; enrichment is additive; write and read cannot
diverge because there is one derivation; proximity and time become prefix scans on structures
the engine already favours, which is a first-class capability rather than a plugin.

**Costs and hazards.**

- Every index is a **derived structure maintained through the observer path**. That is why
  class F (observer backpressure) is load-bearing and lands first: an index built on a queue
  that drops adds a silent-loss path per index.
- Each index multiplies derived-write volume. Start with the attribute map plus **one** index,
  measure, then add.
- Variable-length, high-cardinality attributes in identity invite write amplification and poor
  compaction. Keep identity narrow.
- Implicit edge DDL changes: the endpoint columns become the key, and attributes become a typed
  map. Existing tables created under the surrogate model need a migration story.

## Rejected

- **Content-addressed single key** — a hash over the whole row made identity depend on
  everything, including fields that should be mutable; produced the 32-into-16-byte collision
  (#610) and could not be reconstructed by a reader without duplicating the hash input.
- **Scalar `prob` as a key component** — forces canonicalisation of a float, and conflates
  "same relationship measured twice" with "two distinct probabilistic claims".
- **`(lat, lon)` in the key** — two floats give no scannable ordering; proximity is an index
  problem.
- **Variable name as a key input** — the original defect; unrepresentable under D2.

## Sequencing and acceptance

Lands **after class F backpressure (C1)**, together with the graph label-index fix — the two
share the derived-structure machinery.

Acceptance:

1. `i4_a_relationship_merge_with_bound_endpoints_is_idempotent_and_traversable`
2. `concurrent_merge_node_and_relationship_remain_idempotent`
3. `exists_pattern_predicate_filters_rows_with_matching_paths`
4. `ferrosa-graph` lib 457/0 and `bolt_write_gaps` 15/0
5. A cell-prefix radius query returning the same set as a linear scan over the fixture

## Open questions

1. Attribute map encoding: inline in the row vs a sidecar; and whether it is schemaless or
   typed per label.
2. Cell algorithm and default resolution (H3 vs S2 vs geohash) — affects precision and the
   number of covering cells per query.
3. Whether endpoints are the partition key (one row per src, clustering by dst) or the full key
   — this decides whether `src`-prefix scans are cheap.
4. Migration for existing implicitly-created edge tables that key on a surrogate uuid.
