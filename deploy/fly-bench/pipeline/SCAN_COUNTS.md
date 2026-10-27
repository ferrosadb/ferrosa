# COUNT and scan ownership

Current PG/CQL measurements use production code `2286f389a`; tests-only changes
follow it. [Before/current/native tables](RESULTS.md) and [raw observations](current-results.json)
show the current result: PG COUNT improves 8.3×, CQL scan mean
improves 12.7%, and PG bounded-scan mean remains essentially flat.
Removing these copies does not explain the entire scan gap. One native PG leg
flagged CPU steal; five PG CPU captures were interrupted. See RESULTS.md for
the explicit quality/profiling limits.

## What changed

| Boundary | Earlier behavior | Current behavior |
| --- | --- | --- |
| Stored scalar → decoded text/blob | New String/Vec payload allocation and copy | SharedText retains immutable CellBytes backing; blobs retain CellBytes |
| CQL projection | Clone selected row/cell vectors | Consume rows; identity reuses vectors, reordered columns move values, repeated outputs share payloads |
| CQL page selection | Clone bounded rows again | Reuse first-page vector or move selected rows |
| CQL → SQL | Clone owned scalar payloads | Move/share the same immutable backing |
| PG partition decoding | Borrowed decode/materialization | Lazy OwnedPartitionRows emits one decoded row at a time |
| Scalar wire encoding | Borrow scalar bytes into response buffers | Same: no intermediate scalar payload vector; final frame buffering copies bytes |

The scalar decoder validates UTF-8 without copying payload bytes. Adopting stored
text still allocates one ownership handle; it is not allocation-free. Tests keep
other owners alive and assert original payload pointers through decoded CQL,
SQL values and clones. Serialization, equality/hash/order and capacity accounting
retain their contracts. Static/complex/duplicate-ordinal rows keep the established
fallback. Corruption fails the statement with context, including after a valid
prefix and after flush; it never completes successfully.

## Isolated allocation checkpoint

Before the shared scalar type migration, a source-matched fixture isolated only
CQL projection/page selection over 10,000 rows. Moving the vectors reduced
requested bytes for a full 96-byte-payload page from **4,800,000 to 0** and allocation
events from **40,002 to 0**. At 4,096-byte payloads the earlier clone path requested
84,800,000 bytes; movement still requested 0. Partial-page selection retained one
120,000-byte output-vector allocation. This records the **earlier row-movement
checkpoint**, not final-type end-to-end allocation volume or Fly throughput.
Final-type pointer guards establish sharing; the full process still allocates.

## What COUNT reads

Eligible PG unfiltered `COUNT(*)` over a single-PK table without clustering/static
columns uses key/liveness metadata and MVCC reconciliation, bypassing SQL payload
rows and the row channel. TTL/deletion exceptions reread pinned overlapping
physical sources before LWW. Source errors fail closed and expired snapshots
return `40001`. Unsupported providers/shapes use the normal clustered stream.

Current COUNT p50 is 5.496ms versus native
0.672ms. It still walks metadata; eliminating row
materialization does not make it constant-time. CQL's count result is its own
protocol path and must not be attributed to the PG-only optimization.

## Remaining measured costs and limits

- The primary scan uses one fully consumed bounded query. Read-concurrency 8
  belongs to point reads; separate delivery controls have their own mode/limits.
  PG first-row streaming and CQL first-page availability are not the same unit.
- The PG socket sink already batches into an8 KiB buffer and both PG/CQL use
  TCP_NODELAY. The provider still sends individual rows through a bounded channel
  to the blocking SQL iterator; the result stream has another bounded handoff.
- SSTable parsing still performs small ReadAt calls through the chunk cache,
  with cache lookup/locking and copies into parsing buffers. Immutable decoded
  values do not remove these physical-read costs. CPU graphs expose these paths;
  CPU sample fractions are not elapsed request or lock-wait fractions.
- Long eight-reader delivery loops can hit client CPU/runqueue limits. Their
  instrumented/delivery rates cannot replace the unprofiled primary table.
- Profiles can represent a different flush/cache state from primary queries.
  Warm data does not imply matched internal memtable/SST tiers. Cold/S3 and
  whole-process allocation volume are unmeasured.

The final comparison retains every observation, including slower legs. Earlier
partial diagnostic runs had unbounded context-switch text/competing decoding or
an SSH timeout; those do not replace the current accepted cohort. The revised
collector samples context switches, caps files, decodes after timing, verifies
backups before cleanup and guards disk space.
