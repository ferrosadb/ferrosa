# CLUSTERING ORDER BY ... DESC is accepted and then ignored

**Severity:** medium — silent wrong answer, no error at any point.
**Found:** 2026-08-24, native 3-node cluster (127.0.0.1:19042-44), via
`ferrosa-memory` migration 055.

## What happens

A table declared newest-first serves oldest-first, and nothing reports a
problem. The DDL is accepted, the table is created, reads succeed, and the row
order is the opposite of the declaration.

```sql
CREATE TABLE agent_memory.entity_source_by_root (
    tenant_id   uuid,
    source_root text,
    recorded_at timestamp,
    entity_id   uuid,
    PRIMARY KEY ((tenant_id, source_root), recorded_at, entity_id)
) WITH CLUSTERING ORDER BY (recorded_at DESC, entity_id ASC);
```

Insert six rows one second apart, then select the partition with no ORDER BY:

```
2026-01-01 00:00:00  ...0001     <- oldest first
2026-01-01 00:00:01  ...0002
2026-01-01 00:00:02  ...0003
2026-01-01 00:00:03  ...0004
2026-01-01 00:00:04  ...0005
2026-01-01 00:00:05  ...0006
```

Expected under Cassandra semantics: descending, newest first. The `entity_id
ASC` tie-break IS applied (two rows sharing a timestamp come back in entity_id
order), so the clause is parsed — only the direction is dropped.

## Why it matters

The default read order of a partition is the contract a paged reader is built
on. A caller that declares DESC and pages the partition gets the oldest rows
first while believing it has the newest, and there is no error, no warning, and
no metric to notice. The failure is invisible until someone compares the list
against the data by hand.

It is also the one case where a workaround is not obviously available: a reader
cannot cheaply "start from the end" of a partition.

## Workaround in use

Query-time `ORDER BY recorded_at DESC` IS honoured, so ferrosa-memory 055
orders explicitly at read time and does not rely on the DDL clause. That works
and is standard CQL, but it means every reader must remember to write it.

## Related gap found at the same time — RESOLVED

> **Fixed after this report was written.** `ae5f3497` ("fix(cql): support
> compound clustering tuple slices", PR #363, merged 2026-08-25) added support
> for the tuple form below. This section is kept because it records why
> migration 055 uses a synthetic `page_key`, which is now a workaround for a
> bug that no longer exists and can be removed. The ordering issue above is
> still open.

Multi-column slice restrictions were not supported:

```sql
WHERE tenant_id=? AND source_root=? AND (recorded_at, entity_id) < (?, ?)
-- SyntaxException: expected identifier, got LParen
```

This is the standard way to express a keyset cursor over a compound clustering
key. Single-column `<`, `>`, and ranges all work. 055 works around it with a
synthetic single-column `page_key`, but the tuple form is what a caller
familiar with Cassandra will reach for first, and a syntax error at least failed
loud — unlike the ordering issue above.

## Suggested fix

Either honour `CLUSTERING ORDER BY ... DESC`, or **reject the DDL** that
contains it. Rejecting is much better than accepting-and-ignoring: it is a
build-time failure instead of a wrong list in production.
