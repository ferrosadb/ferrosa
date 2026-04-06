---
type: bug
priority: P2
created: 2026-04-06
updated: 2026-04-06
---

# SELECT now() FROM system.local returns empty/null — crashes Python driver

## Description

`SELECT now() FROM system.local` returns a result that causes the Python cassandra-driver to crash with:

```
ValueError: Invalid shape in axis 0: 0.
```

This suggests the `now()` built-in function either returns NULL or the result set has 0 columns, which triggers a Cython array allocation error in the driver's row parser.

`SELECT key FROM system.local` works fine (returns `Row(key='local')`).

## Expected Behavior

`SELECT now() FROM system.local` should return a single row with a timeuuid value, matching Cassandra's behavior.

## Impact

- Breaks health checks in cassandra-driver applications that use `SELECT now()` as a connectivity test
- Standard pattern used by many CQL clients and monitoring tools
