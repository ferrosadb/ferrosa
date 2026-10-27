# Count and bounded-scan ownership audit

The branch now includes PostgreSQL single-row COUNT pushdown (`148870d00`).
Eligible unfiltered `COUNT(*)` counts keys/liveness and reconciles MVCC without
constructing SQL payload rows or using the row channel. TTL/deletion exceptions
reread pinned physical sources to preserve LWW visibility. Other query/table
shapes use the ordinary scan. **No matched Fly measurement of this new path has
run yet.** The timings and remaining-cost discussion below describe measured
runtime `a2709e100`; they must not be presented as results for the new code.

The published scan rate is a fully consumed, bounded query: the primary probe
runs one serial scan per observation after warming, rather than measuring
maximum sustained scan throughput. Same-protocol pairs request the same fields,
payload width and limit through the same driver. Across protocols, table sizes,
fetching and query work differ (SPARQL uses a three-pattern join), so the four
scan rows/s figures are not interchangeable storage-bandwidth measurements.

The first measured candidate (`c24cad554`) still had avoidable work in SQL and
SPARQL. The follow-up at `a2709e100` removes three costs. Throughput tables must
retain their measured source identity until the new Fly run is accepted.

| Path | Confirmed cost | Change and proof |
| --- | --- | --- |
| SQL COUNT without GROUP BY | Full input traversed sort/spill machinery before accumulating | Consume and release one row at a time; exact COUNT/SUM succeed with unusable scratch storage. Peak accumulator test: 296 bytes at both 100,000 and 1,000,000 rows. |
| SQL bounded scan projection | Selected owned text/blob values cloned into each output row | Move each value into its final occurrence; clone only additional repeated output columns. Pointer-preservation regressions fail on the old implementation. Identity projections reuse the input row vector. |
| SPARQL multi-pattern scan | Every candidate pair cloned a solution before compatibility checks; shared-subject joins compared every pair | Borrow terms before extending a solution; index borrowed typed subject keys to input row positions. At 128 records, requested bytes fall from 18,498,367 to 1,916,787 (89.6% less). Bag multiplicity and repeated-variable constraints remain tested. |

[Allocation measurements and verification scope](scan-count-ownership.json).
The allocation experiment uses the same three-pattern query, 96-byte payloads,
a warmed in-process store and thread-local accounting. It checks every key,
bucket and payload. Four times as many rows requested 12.24 times as many bytes
before, versus 3.78 times after. Local development-profile timings are not used
as comparative product throughput.

## What counts still read

- PostgreSQL still decodes physical rows, resolves MVCC before-images and own
  writes, and feeds the SQL operator. The scalar accumulator no longer sorts
  those rows, but storage projection/count pushdown remains absent here.
- SPARQL single-pattern COUNT streams decoded triples/bindings into counters;
  it does not return the matching rows to the client. Joins/property paths may
  retain bounded intermediate solutions. DISTINCT retains identity sets.
- Eligible Bolt COUNT already counts live-row metadata without materializing
  vertex JSON. It still walks storage metadata; the earlier flushed-count CPU
  capture shows chunk-cache lookup and lock contention. This is a separate
  cost from copying vertex payloads, and a CPU sample does not measure a mutex
  wait duration.
- General SPARQL joins still have a compatibility fallback when subjects are
  unbound, and non-subject joins may still compare every pair. Intermediate
  solution vectors remain; fully pipelined joins are not implemented.

These changes do not claim an end-to-end zero-copy protocol or constant-time
COUNT. SQL values move through the audited projection boundary; decoding,
MVCC reconciliation, RDF term construction and wire serialization still have
costs that require their own measurements.

## Matched follow-up observations

The follow-up at `a2709e100` measured PostgreSQL COUNT at 43.967 ms versus native
0.865 ms, and scans at 92,742 versus 420,447 rows/s (four observations per
product). Removing projection copies and the scalar sort did not close this
end-to-end gap. A separate eight-worker, flushed-data count CPU capture has
1,382 samples: scan producer appears in 41.17%, SSTable code in 25.18%, MVCC in
12.08% and the row channel in 13.60%. These inclusive categories overlap; they
are not elapsed fractions, and this profile is not the serial primary count.
The provider still constructs and delivers rows; eligible metadata/key-only
count pushdown under MVCC is not implemented.

SPARQL scan throughput was 29,937 rows/s versus Oxigraph 74,329 in the same
follow-up. The earlier candidate measured 1,816 rows/s: this is an observed
cross-run improvement, while the allocation fixture supplies the isolated
copy-removal proof. A remaining confirmed source cost is that every
`PredicateScan` still calls `stream_scan`; the three-pattern benchmark scans
the triples table per pattern. Predicate-aware access is unimplemented. This
is extra work, not proof that it alone explains the remaining 2.5x ratio.
