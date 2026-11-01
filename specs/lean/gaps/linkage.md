# Where the corpus and the extraction overlap

Ledger items: **317** (257 met, 60 failing). Classified reds: 15 broken, 38 with no code path, 7 not defects.

## The extracted surface

| module | crate | sources | status | translated symbols |
|---|---|---|---|---|
| Sstable | ferrosa-sstable | src/compression.rs | ok | 64 |
| Storage | ferrosa-storage | src/count.rs | stale | 5 |

Files under extraction: `ferrosa-sstable/src/compression.rs`
Files named by a broken row: 19 — overlap: **1**

## The broken rows, by distance from the extraction

| class | count |
|---|---|
| crate-measured-file-not-extracted | 5 |
| no-code-anchor | 5 |
| crate-charon | 2 |
| crate-modules-partial | 1 |
| crate-aeneas-crash | 1 |
| extracted | 1 |

| area | what | anchor(s) | how | named symbol extracted? | class |
|---|---|---|---|---|---|
| Recovery | replay is idempotent | `ferrosa-storage/src/commitlog/mutation.rs` | explicit | **no** | crate-charon |
| Tombstone | a local deletion time is representable for every TTL | `ferrosa-common/src/cell.rs`, `ferrosa-row-bridge/src/row.rs` | explicit | **no** | crate-measured-file-not-extracted |
| SQL | three-valued logic is preserved | — | none | **no** | no-code-anchor |
| SQL | aggregates follow the empty-set rules | — | none | **no** | no-code-anchor |
| SQL | the commit decision is durable before the acknowledgement | `ferrosa-postgres/src/query.rs`, `ferrosa-postgres/src/server.rs` | explicit | **no** | crate-measured-file-not-extracted |
| SPARQL | `p+` returns each pair once | — | none | **no** | no-code-anchor |
| SPARQL | RDF-star terms are distinct from IRIs in the store | — | none | **no** | no-code-anchor |
| Index | index and table cannot diverge | `ferrosa-storage/src/engine.rs`, `ferrosa-storage/src/store.rs` | explicit | **no** | crate-charon |
| Consistency | the coordinator enforces the per-datacenter requirement | `ferrosa-cluster/src/coordinator/write.rs` | explicit | **no** | crate-modules-partial |
| Streaming | a page bound in rows is a bound in memory | `ferrosa-cql/src/paging.rs` | ident | **no** | crate-measured-file-not-extracted |
| Streaming | the scan never materialises a page of rows before encoding | `ferrosa-cql/src/result.rs` | ident | **no** | crate-measured-file-not-extracted |
| Streaming | the reader streams partitions to the caller | `ferrosa-sstable/src/reader.rs`, `ferrosa-sstable/src/types.rs` | ident | **no** | crate-aeneas-crash |
| Streaming | a decoded chunk is written into a caller-supplied buffer | `ferrosa-cluster/src/accord/compression.rs`, `ferrosa-sstable/src/compression.rs` | explicit | **no** | extracted |
| Streaming | a slow client cannot grow the coordinator's memory | `ferrosa-cql/src/connection.rs`, `ferrosa-cql/src/result.rs` | explicit | **no** | crate-measured-file-not-extracted |
| Streaming | acks are coalesced so a high-latency pipe is not ack-starv | — | none | **no** | no-code-anchor |

## What this says

- `no-code-anchor`: the row's text names nothing that exists in the Rust, so there is no
  extraction it could be tied to — the claim is about behaviour, not about a site.
- `crate-aeneas-crash` / `crate-oom` / `crate-unmeasured`: the file is real and the blocker is
  the translator, not the work of stating a refinement.
- `crate-measured-file-not-extracted`: the crate translates; the file only needs a scope in
  `specs/lean4-rust/modules.txt`.
- `extracted`: the code is in the extraction today, so a refinement is possible now. These are
  the only rows where the corpus can stop being a statement about a model.

