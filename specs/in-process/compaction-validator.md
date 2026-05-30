# Compaction Validator

Status: In progress (design approved 2026-05-30). Branch stacks on the HVQ
branch (`docs/hvq-vector-indexes`); lands after it.

Adapts the differential method of the Cassandra `compaction-validator` tool
(cscotta/cassandra) to Ferrosa: generate deterministic data, run it through
compaction, and validate the output three independent ways. The Java tool is
Cassandra-pipeline-specific (cursor vs iterator, `CQLSSTableWriter`, `Index.db`)
so the methodology is ported, not the code.

## Goals

- Catch compaction correctness bugs (data loss, wrong conflict resolution,
  tombstone/TTL mishandling, ordering/format violations).
- Be runnable both as bounded CI property tests and as a continuous soak.
- Surface gaps in existing coverage and close them; TDD-fix any real bug found.

## Validation modes (same corpus)

1. **Oracle** — independent brute-force reference merge encoding the rules in
   `ferrosa-storage/src/merge.rs`: partition/row deletion = `max(marked_for_delete_at)`;
   cell-level last-write-wins by timestamp; deletion suppression (rows with
   `pk_liveness.ts < partition.mfd_at` removed; cells with `ts < row.mfd_at`
   removed; static cells vs partition deletion). Compaction output is compared
   cell-for-cell to the oracle.
2. **A/B differential** — compact identical inputs via `SizeTieredStrategy` and
   `UnifiedCompactionStrategy`; assert identical logical merged output (the two
   strategies differ only in which SSTables they group, not in merge result).
3. **Format auditor invariants** — on merged output: clustering-key ordering,
   no duplicate column per row, timestamp/TTL/local-deletion-time
   non-negativity, and cell-flag mutual exclusivity (a cell is not both a
   tombstone and expiring).

## Corpus (seeded, deterministic)

Overlapping SSTables of shared partitions; cell LWW conflicts; partition/row/
cell tombstones; expiring (TTL) cells; wide rows; tombstone-then-resurrected-by-
newer-write. The loadgen generator currently covers neither TTL nor wide rows —
closing that is part of this work.

## Errata → Ferrosa property tests

The six Cassandra cursor-compaction defects, as universal property checks:
tombstone-resurrected-by-expiring data loss; timestamp tie-break direction;
IS_DELETED + IS_EXPIRING exclusivity; reverse-scan consistency; index
final-block boundary read; wide-row highest-position cell preservation. Any
that reproduce against Ferrosa are fixed red-first.

## Placement

Reusable core `ferrosa_storage::compaction::validator` (oracle + corpus +
auditor + driver), compiled only under
`#[cfg(any(test, feature = "compaction-validator"))]` so it never enters the
production lib. Consumed by:

- CI: `ferrosa-storage/tests/compaction_validator.rs` — bounded seeded property
  tests (proptest is already a dependency).
- Soak: a `compaction-soak` mode in `ferrosa-loadgen` (continuous generate ->
  flush -> compact -> validate, with seed pinning), reusing the same oracle and
  auditor against its `ground_truth`.

## Sequencing

Oracle first (it is the reference everything else leans on), then the corpus
generator, then A/B driver, then the auditor, then the errata property tests,
then the loadgen soak mode. TDD throughout; any compaction bug surfaced gets a
red test then a fix.

## Verification

- `cargo test -p ferrosa-storage` (validator tests green).
- `cargo test -p ferrosa-storage --features compaction-validator`.
- Soak mode runs a bounded loop clean in CI; longer runs via the loadgen CLI.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` clean.
