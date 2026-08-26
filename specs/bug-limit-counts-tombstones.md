# LIMIT counts tombstones, so a query returns fewer rows than exist — often zero

**Status**: reported, not fixed
**Found**: 2026-08-25, on the native 3-node cluster (ports 19042/3/4)
**Severity**: silent wrong answer. No error, no warning; the query reports success
and an empty result for a partition that holds live rows.

## What happens

`LIMIT n` is applied before deleted rows are filtered out. When the first `n`
entries in clustering order are tombstones, they consume the whole limit and the
query returns nothing — while the same query without `LIMIT` returns every live
row.

## Minimal reproduction

Against `agent_memory.knowledge_by_state`, whose key is
`PRIMARY KEY ((tenant_id, state, priority_band), page_key)`:

```python
# 20 rows in one partition; delete the 14 with the LOWEST page_keys,
# leaving 6 live rows behind them in clustering order.
for k in keys:      INSERT ... (tenant_id, 'proposed', 'high', k, ...)
for k in keys[:14]: DELETE ... WHERE tenant_id=? AND state=? AND priority_band=? AND page_key=?
```

| query | rows returned | expected |
|---|---|---|
| no `LIMIT` | 6 | 6 |
| `LIMIT 3` | **0** | 3 |
| `ORDER BY page_key ASC LIMIT 3` | 3 | 3 |
| `ORDER BY page_key DESC LIMIT 3` | 3 | 3 |

Deterministic across repeated runs — five consecutive executions all returned 0.

The same shape appears in real data. In one tenant of `knowledge_by_state`:

| partition | live rows | `LIMIT 5` |
|---|---|---|
| `state='proposed'` (212 rows had been moved out) | 14 | **0** |
| `state='rejected'` | 28 | 5 |
| `state='approved'` | 184 | 5 |

Only the partition with a large tombstone run ahead of its live rows is affected.

## Why this shape is common here

Both knowledge queues are partitioned BY the mutable column (`state`), so a
decision is a move: delete from the old partition, insert into the new one. A
review queue therefore accumulates one tombstone per decided item, ahead of the
items still waiting. That is the normal steady state of the table, not an abuse
of it.

Any table partitioned by a mutable column has this shape — it is the standard
Cassandra modelling answer for "query by current state".

## Why an `ORDER BY` hides it

`ORDER BY` appears to take a different scan path that merges and filters before
applying the limit. `CqlKnowledgeStore::page` always emits
`ORDER BY page_key DESC LIMIT n`, which is why the mobile Knowledge and Claims
tabs read correctly today. That is luck, not protection: the ordering was added
because this engine accepts `CLUSTERING ORDER BY DESC` in DDL and then ignores
it, and nothing records that it is also what keeps the limit honest.

## Expected behaviour

`LIMIT n` bounds LIVE rows returned to the client. Deleted rows must be filtered
before the limit is applied, so a partition holding `k` live rows returns
`min(k, n)` of them regardless of how many tombstones precede them.

Returning zero from a partition that holds rows is the worst available outcome:
it is indistinguishable from an empty queue, and the caller has no signal that
anything was dropped. If the engine cannot honour the limit without scanning
past a tombstone budget, it should fail loudly rather than answer short.

## Where to look

The limit is presumably applied at the storage/scan layer while tombstone
resolution happens later in the merge. The unordered path and the `ORDER BY`
path evidently differ in where that happens; making the unordered path match the
ordered one is likely the smallest correct fix.

## Do not work around this

`ferrosa-memory` is a test program for this engine. The right response is a fix
here, not an `ORDER BY` added defensively to every call site in the consumer —
that would hide the next instance of it.
