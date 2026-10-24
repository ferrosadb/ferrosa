# Best practices: one keyspace per domain, one table per access path

**Scope:** graph entities that carry attributes, and the indexes over those attributes.
**Companions:** `graph-identity-and-attributes.md` (what identity and attributes are), this
document (where they physically live).

The short version: **the base tuple table stays narrow and immutable; every attribute access
path gets its own table, specialised for that path; a keyspace groups tables that share a
lifecycle.** Getting this wrong is the single largest source of write amplification in a graph
workload, because amplification is multiplicative in indexes and then multiplied again by
compaction.

## 1. The cost model that drives every rule

A logical write costs:

```
base write
  + one derived write per index
  + compaction rewrite of everything touched, per level
```

So one edge with four indexes is five writes, and each of those five will be rewritten by
compaction several times. Two consequences:

- **Indexes are the dominant cost, not the base row.** Add an index only when a query needs it,
  and prefer making the query a prefix scan over adding a structure.
- **Wide rows amplify compaction.** If all attributes live in one row, every attribute update
  rewrites the whole row and every index entry derived from it. Narrow base rows mean an
  attribute change rewrites only that attribute's rows.

## 2. Layout: base, attributes, indexes

### 2.1 Base tuple table — narrow, immutable, identity only

```
<label>_v   key (id)                 value: nothing beyond identity-bearing columns
<label>_e   key (src, rel, dst)      value: nothing beyond the endpoints
```

Only identity lives here. No optional attributes, no blobs, no JSON. This table's job is to make
"does this entity exist" and "give me its key" cheap, and to be written once in the entity's
lifetime.

### 2.2 Attribute table — separate, so enrichment never touches identity

Two shapes, and the choice is about hot/cold, not taste:

```
<label>_a   key (id, attr_name)      value: typed attr value      ← per-attribute rows
<label>_ad  key (id)                 value: typed map (sidecar)   ← whole-map row
```

- **Per-attribute rows** (`_a`) win when attributes mutate independently, when different
  attributes need different indexes, or when the set is open-ended. Each write touches one row.
  Cost: reading the whole entity needs a prefix scan rather than a point read.
- **Sidecar map** (`_ad`) wins when the map is read as a unit and mutated as a unit (a measured
  observation row, a device state snapshot). Cost: any attribute change rewrites the whole map.

**Rule:** if any attribute is indexed, use per-attribute rows. Otherwise the index maintenance
either rewrites the whole map or cannot tell which attribute changed.

### 2.3 Index tables — one per access path, specialised

Every index entry is the pair `(index_key, identity)` with an empty or tiny value:

```
<label>_idx_<attr>   key (attr_value, id)          property equality/range
<label>_geo_<res>    key (cell, id)                value: exact coordinates
<label>_ts           key (bucket, ts, id)          time window
```

Three properties this buys, all of which a multi-column index inside the base table cannot:

- **The index can be dropped or rebuilt without touching the base table.**
- **Each index gets its own clustering order** — geo clusters by cell at the chosen resolution,
  time by bucket, property by value — so each becomes a prefix scan.
- **Writes touch only the indexes that changed.**

**Covering vs non-covering is a deliberate choice.** Keep the value empty and pay a second
lookup for the row; store a small projection and save the fetch but multiply the bytes each
index write costs. Default to non-covering; make an index covering only when a measured query
cannot afford the second hop.

### 2.4 Adjacency — also an index, and not special

```
adjacency   key (node, direction, edge_key)
```

Same shape as every other access path, derived from the edge key. It is listed separately only
because traversal is the hottest path, so it deserves its own compaction tuning.

## 3. Keyspaces: group by lifecycle, not by entity

A keyspace is the unit of replication, retention, and DDL lifecycle; a table is the unit of
ordering and compaction. So:

- **One keyspace per domain** — `graph`, `graph_geo`, or a tenant namespace if replication or
  retention genuinely differs.
- **One table per access path** within it.
- **Do not create a keyspace per index.** It looks tidy and it multiplies operational surface:
  every new index becomes a new replication decision, a new backup unit, a new permission.
  Create a separate keyspace only when the index needs a *different* replication factor,
  retention, or storage class than its base — e.g. a geo index that can be rebuilt and therefore
  runs at lower RF.

### Naming convention

```
<label>_v          vertices
<label>_e          edges
<label>_a          attributes, key (id, attr_name)
<label>_ad         attributes, sidecar map
<label>_idx_<attr> property index
<label>_geo_<res>  geo index at a named resolution
<label>_ts         time index
adjacency          adjacency, shared per keyspace
```

The suffix carries the access path, so a table's role is readable from its name and tooling can
enumerate "all indexes for label X" mechanically.

## 4. Consistency: index and base are not one write

Splitting rows across tables means the base write and each index write are separate mutations.
Decide explicitly which model you are in:

| model | meaning | when |
|---|---|---|
| **atomic** | base + indexes applied in one batch; all or nothing | small fan-out, transactional workloads |
| **derived, repaired** | base commits, indexes follow; a missing entry is repaired by reconcile | large fan-out, high write rate |

If you choose *derived*, then two things are mandatory and are not optional polish:

1. **The derived write must never be silently dropped.** This is class F: a dropped index update
   leaves durable data with no path to it. Backpressure — an inline fallback on a full queue —
   not a drop counter nobody reads. This is why class F lands before any new index.
2. **A repair path must exist and be triggered automatically**, so a divergence is bounded rather
   than permanent.

Attribute updates also need **read-modify-write for the old index entry**: changing `prob` from
0.5 to 0.6 must delete `(0.5, id)` and insert `(0.6, id)`. Forgetting the delete leaves a phantom
row that a range scan will return — a correctness bug that looks like a duplicate entity.

## 5. Rules of thumb

**Inline vs separate attribute table**

- Indexed, mutates independently, or open-ended → separate `_a` rows.
- Read and written as one blob, nothing indexed → `_ad` sidecar.
- Identity-bearing → it belongs in the base key, and only under the canonicalisation rules
  (sorted keys, fixed-point numbers, typed tags).

**When to add an index**

1. The query must be answerable by a **prefix scan or point lookup** on the index key. If the
   planner would have to scan the whole index, the index is not the answer.
2. The attribute must be **selective**. An index on a value with few distinct values is a scan
   wearing a hat — this is the same trap as the unselective-index heuristic (share 10, minimum
   matches 50) already in the planner.
3. You can name the query it serves. "Might be useful" indexes are pure amplification.

**Attribute key ordering** — most-selective component first only if it stays scannable:
`(cell, id)` for geo, `(bucket, ts, id)` for time, `(value, id)` for equality/range. Never put a
low-cardinality component before a high-cardinality one you need to bound.

**Numbers in keys** → fixed point (`0.5` → `5000`, `23.000°` → `23_000_000`) so equality is
exact and ordering is total.

## 6. Anti-patterns

| anti-pattern | why it hurts |
|---|---|
| One wide row holding identity + all attributes | every attribute update rewrites the row and all its derived index entries |
| A JSON/blob attribute column queried by scanning | no prefix scan possible; you have built a scan and called it a column |
| Keyspace per index | multiplies replication/retention/backup surface for no query benefit |
| Index on every attribute | amplification is multiplicative; measure before adding the third |
| Index entry carrying the whole row (default covering) | multiplies bytes per write; make covering opt-in |
| No delete on attribute update | phantom index rows return entities that no longer match |
| Deriving index entries through a dropping queue | silent unreachability — the class F defect |
| Mixing hot and cold attributes in one table | compaction rewrites cold data with hot data |

## 7. Observability — required, not optional

Per index table, publish:

- writes/sec and bytes written (the amplification numerator),
- row count and on-disk size (the denominator's growth),
- **backpressure events** and **queue depth vs capacity** for the observer path maintaining it.

Without these you cannot answer "what did this index cost me", and the default answer becomes
"add another index and hope".

## 8. Review checklist

- [ ] Base table holds identity only; no optional attributes.
- [ ] Any indexed attribute lives in per-attribute rows, not a sidecar map.
- [ ] Every index table's key begins with the component its queries scan by.
- [ ] Index values are empty unless covering was a measured decision.
- [ ] Attribute update deletes the previous index entry (read-modify-write).
- [ ] Index maintenance is backpressure-protected, not drop-and-forget.
- [ ] A repair/reconcile path exists and is triggered automatically.
- [ ] Numbers in keys are fixed-point; map keys are sorted before encoding.
- [ ] Amplification measured as bytes written per logical write, before and after adding the index.
- [ ] Keyspaces are per domain/lifecycle, not per index.
