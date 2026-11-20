---
title: Red ledger accounting — PR1
status: complete
date: 2026-11-19
baseline: ferrosa v0.36.9 (9c2a076a4)
companions:
  - specs/reds-audit.md  (the audit behind these numbers)
  - specs/reds-burndown.md (the plan; its flip estimate is superseded)
---

# Red ledger accounting

PR1's job is to make the corpus **countable and honest about what a row means**. It is not the work
of fixing the rows — that is the follow-on PR, item by item.

Three things were wrong before this PR, and all three are now machine-checked:

1. **A `.met` row could not say whether it was about the code or about a model.** `SpecItem` had
   `name` and `status` and nothing else, so `Ferrosa/Storage/Recovery.lean` could prove that
   corruption is undetectable while the real record carried a CRC, and nothing in the pipeline could
   notice.
2. **Only the red side of the ledger had a Rust export.** The side a reader is meant to trust was
   unexamined — no table, no count, no test.
3. **A red could be silently refuted, or silently duplicated.** Nothing tied a row to the code it
   described, so rows survived for a year after their fix landed.

## The corpus today

| area | rows | red | met | code-tagged |
|---|---|---|---|---|
| `Ferrosa.Raft.Obligations` | 34 | 16 | 18 | 6 |
| `Ferrosa.Streaming.Audit` | 36 | 14 | 22 | 0 |
| `Ferrosa.Storage.Format` | 28 | 9 | 19 | 0 |
| `Ferrosa.SystemTables.Projection` | 12 | 7 | 5 | 0 |
| `Ferrosa.Ownership.Obligations` | 24 | 6 | 18 | 0 |
| `Ferrosa.Storage.Streaming` | 23 | 6 | 17 | 0 |
| `Ferrosa.Cql.AggregateObligations` | 10 | 6 | 4 | 0 |
| `Ferrosa.Storage.Mvcc` | 33 | 5 | 28 | 0 |
| `Ferrosa.Codec.Audit` | 20 | 5 | 15 | 0 |
| `Ferrosa.Cdc.Subscribe` | 45 | 4 | 41 | 0 |
| `Ferrosa.Storage.Tombstone` | 35 | 4 | 31 | 0 |
| `Ferrosa.Testing.Histories` | 20 | 3 | 17 | 0 |
| `Ferrosa.Unsafe.Invariants` | 14 | 3 | 11 | 0 |
| `Ferrosa.Testing.JepsenModels` | 12 | 3 | 9 | 0 |
| `Ferrosa.Storage.Recovery` | 7 | 3 | 4 | 0 |
| `Ferrosa.Sql.Semantics` | 6 | 3 | 3 | 0 |
| `Ferrosa.Bolt.Session` | 6 | 3 | 3 | 0 |
| `Ferrosa.Sparql.Semantics` | 6 | 3 | 3 | 0 |
| `Ferrosa.Sql.Consistency` | 47 | 2 | 45 | 3 |
| `Ferrosa.Testing.Jepsen` | 10 | 2 | 8 | 0 |
| `Ferrosa.Sql.Txn` | 6 | 2 | 4 | 0 |
| `Ferrosa.Commons.Hll` | 6 | 2 | 4 | 0 |
| `Ferrosa.Sql.Index` | 5 | 1 | 4 | 0 |
| `Ferrosa.Flight.Subscribe` | 5 | 1 | 4 | 0 |
| `Ferrosa.Testing.Guarantees` | 10 | 0 | 10 | 0 |
| `Ferrosa.Cql.Subscribe` | 5 | 0 | 5 | 0 |
| **total** | **465** | **113** | **352** | **9** |

`code-tagged` counts rows carrying the `code: ` marker — the only rows a reader may act on as
assurance about the shipping implementation. Before this PR the marker existed on 12 rows out of 465;
9 now, because 6 were restated as red and 3 verified `.met` rows were tagged.

## What the audit found

Full detail in [`reds-audit.md`](reds-audit.md). The counts:

| | Count | Meaning |
|---|---|---|
| **Red side** (163 rows at audit time) | | |
| REFUTED | 20 | code is right; row should not be red |
| CONFIRMED | 51 | real |
| PARTIAL | 33 | a narrowed residual survives; text overstates it |
| UNVERIFIABLE | 2 | needs measurement, not a read |
| **Met side** (358 rows at audit time) | | |
| code-verified | 160 | code implements it |
| design | 138 | model theorem; says nothing about the implementation |
| **code-untrue** | **42 named** | the code contradicts a row that reads as proof |
| code-no-site | 2 named | no implementation, prose presents it as a feature |

### A correction to the audit's own totals

The audit's summary tables said 52 `code-untrue`. That figure is wrong: one group's header folded
three rows its own prose classed `design`, and another's header said 15 where its prose enumerated 12.
The **named, per-row lists reconcile exactly to 42** (13 + 15 + 2 + 5 + 7), and those names are what
the follow-on PR will work from. Six are already restated, so **36 remain**, plus 2 `no-site` and 5
rows the prose hedged as design-but-suspect.

## What PR1 changes

### 1. A claim is now tagged, and the tag is derived

`Ferrosa/Ledger.lean` gains:

```lean
inductive Claim where
  | design   -- proved of a model; carries no claim about the shipping implementation
  | code     -- a claim about the shipping implementation
```

`claimKind` derives it from the `code: ` name prefix the corpus already used — so tagging a row is a
**rename, not a new field to keep in step**, and the tag cannot disagree with itself. Two theorems
make it enforced rather than decorative:

- `claimKind_is_code_iff_marked` — a row is about the code exactly when marked.
- `a_met_design_row_says_nothing_about_the_code` — a design row cannot be read as assurance. An
  untagged row is `design`, because a claim that has not said it is about the code must not be read
  as one.

`render` prints `[CODE]` / `[DESIGN]`, and `Export.lean` carries `claimKind` into the JSON.

### 2. The trusted side is now machine-readable

`scripts/red_ledger.py` generates `red-ledger/src/met.rs` (352 `Met` rows: claim, proof theorem,
kind) alongside `reds.rs`. Three new tests:

- `every_met_names_its_proof` — a `.met` row with no theorem is the same waiver a red with no witness is.
- `red_and_met_tables_are_disjoint` — the two tables partition the corpus.
- `met_count_is_pinned` — a `.met` row cannot vanish silently, same rule as the reds.

### 3. The mark is load-bearing, so it must be reachable

`both_claim_kinds_are_represented` fails the build when no row is code-tagged. Without it the marker
could stop matching — a rename, a missing prefix on a flip — and every row would silently become
`design` with nothing failing. It went red as soon as it was written, which is exactly the condition
it exists to catch.

### 4. A staleness ratchet

`scripts/red_sites.py` records, per row, the commit it was last verified at and the files that
verification read. When any of those files changes, the row must be re-verified. A symbol check cannot
catch semantic rot — `publish_snapshot` still exists, but it no longer lets a stale builder overwrite a
newer durable snapshot — so change-tracking is the honest primitive.

**Limitation, measured not assumed:** only **41 of 107** rows cite a `.rs` file at all. The other 66
reference a concept ("replay is idempotent", "the merge is order-independent") and cannot be ratcheted
until they name a site. That is itself a ledger defect and is filed for the follow-on PR.

### 5. Six rows restated, with the counts moving as designed

`Ferrosa.Storage.Format` — the six rows the storage audit named with exact evidence:

| was `.met` | now red, because |
|---|---|
| a wrong or corrupt trie can never return a wrong value | `partition_index.rs:116-122` verifies one hash byte and `reader.rs:993-1017` never compares the requested key; `merge.rs:60` adopts `first.key` |
| the index is a speed-up, not an authority | the same defect: index damage changes the answer |
| a corrupt trie block is detected | checksums cover `Data.db` only (`checksum.rs:1-6`) |
| one damaged byte costs one record | `store.rs:4556-4580` quarantines the generation; `reader.rs:1347-1361` abandons the walk |
| an absent footer is recoverable | `partition_index.rs:60-72` refuses it outright |
| a local deletion time cannot wrap | the cell ldt is `i32` and saturates to `i32::MAX` = never expires |

Each is classified in `Broken.broken` (built, shipping, wrong), so `every_red_is_classified_exactly_once`
holds. Pins moved in the same commit: `format_has_nine_red_items`, `ledgerTotal_pinned` 107 → 113,
`broken_has_sixty_two_items`.

The corpus refused the first attempt: I classified those reds in **both** `unimplemented` and
`Broken`. A red is one or the other — `rows.length = ledgerTotal` caught it. That constraint is now
known rather than learned again.

## Verification

```
cd specs/lean4 && lake build                 # 99 jobs, green
lake exe export build/specs.json             # 92 modules, 465 claims, 113 red
python3 scripts/red_ledger.py --check        # 113 reds, Rust side up to date
cargo fmt --check                            # clean
cargo clippy -p red-ledger --all-targets     # zero warnings
cargo test -p red-ledger                     # 7 passed
```

## What PR1 does not do

Deliberately out, and queued for the follow-on PR:

| Work | Rows |
|---|---|
| restate `code-untrue` with evidence | 36 remaining |
| restate `code-no-site` | 2 |
| flip REFUTED rows to `.met` with witnesses | 16 |
| replace the 6 stale models, then flip | 6 |
| narrow rather than flip | 1 |
| resolve witness cited by both `.met` and `.failing` | 5 pairs |
| reclassify `Broken.lean` for the refuted rows | up to 20 |

And separately tracked: the **9 shipping defects the audit found in no ledger row at all**, plus the
data-integrity defects hiding behind `code-untrue` rows (a corrupt index returning another key's rows;
a replayed DDL entry latching a node; the default consistency level being `ONE`). Those are code bugs,
not bookkeeping, and belong with the correctness packets — not in a docs PR.
