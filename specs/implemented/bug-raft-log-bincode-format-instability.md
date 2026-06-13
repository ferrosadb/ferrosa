---
type: implemented
priority: P1
status: fixed
created: 2026-06-12
fixed: 2026-06-12
affected-versions: all (no raft log format versioning exists)
branch: fix/raft-log-filterpredicate-bincode-compat
---

# Bug: persisted Raft log entries have no format versioning — build drift bricks the metadata plane silently

## Observed (fmem 3-node cluster, 2026-06-12)

All three nodes fail Raft initialization at boot with an identical fatal:

```
ERROR ferrosa_cluster::controller::cluster: raft initialization failed (Fatal)
  fatal=when Read Logs: alloc::boxed::Box<bincode::error::ErrorKind>:
  invalid value: integer `1635017060`, expected variant index 0 <= i < 26
```

- Snapshot at `last_applied=5999` decodes fine on every build tried
  (`recovered raft topology from persisted state machine snapshot
  member_count=3 token_count=768`).
- Log entries `[6000, 6121]` are unreadable by **both** the current build
  and the previous deployed build (`ferrosa-memory-node:rowpage-prev`) —
  they were written by an older build whose `RaftCommand`/`RaftOp` bincode
  layout differed (variant added/reordered or embedded struct field change).
  `1635017060` is ASCII bytes being misread as an enum tag — framing slip,
  not random corruption: byte-identical error at the same offset on three
  independently-written node logs.

## Why this is severe

1. **The failure is silent in operation.** Raft init failure is non-fatal
   to the process: the node recovers topology optimistically and keeps
   serving CQL. The fmem cluster ran **10+ hours with a dead metadata
   plane** (no DDL replication, no membership changes possible) while
   TCP-based healthchecks reported `healthy`. Only the new `/readyz`
   leader-gated probe surfaced it (`{"ready":false,"waiting_for":"raft_leader"}`).
2. **No build can repair it.** Old and new builds both fail to decode;
   there is no `--skip-bad-entries` or log-truncate tooling.
3. **Every release upgrade is exposed.** Any commit that touches the
   `RaftOp` enum (or any type embedded in `RaftCommand`) changes the
   on-disk log format with no version tag, no compat test, and no error
   message that names the real cause.

## Root cause (confirmed)

The immediate trigger was `FilterPredicate` changing from a flat struct
(`{column_position, op, value}`) to a versioned conjunction struct
(`{version, clauses: Vec<FilterClause>}`). Bincode serialized the old flat
struct as a 3-field tuple; the new struct serialized as a completely
different layout (`version: u8` first, then the clause vec), producing
an unreadable tag when the old log entries were replayed by the new build.

`1635017060` decimal = `0x6177696F` = ASCII `avio` — the start of
`"active"` in old `value` bytes being misread as a `u32` enum variant tag.
This is a framing slip, not bit corruption.

The `FilterPredicate::Deserialize` impl only handled JSON (human-readable)
via the `Wire` struct pattern. Bincode (non-human-readable) fell through to
the derived deserializer, which tried to decode the new struct layout from
old bytes and failed.

## Fix implemented (decode compat — bincode wire stability)

**Branch**: `fix/raft-log-filterpredicate-bincode-compat`

### Wire scheme

Manual `Serialize`/`Deserialize` impls on `FilterPredicate` maintain a
stable bincode wire layout across old and new builds:

| Case | Bincode layout |
|------|---------------|
| Single-clause (new or legacy) | `(column_position: usize, op: FilterOp, value: Vec<u8>)` — byte-identical to the old flat struct |
| Multi-clause conjunction | `(usize::MAX [sentinel], FilterOp::Eq [sentinel], bincode_bytes: Vec<u8>)` — `value` carries `bincode::serialize`d `Vec<FilterClause>` |
| JSON (human-readable) | `{"version": N, "clauses": [...]}` — unchanged |

The sentinel `usize::MAX` is chosen because `0..usize::MAX-1` covers all
valid column positions. No legacy log entry could have produced `usize::MAX`
as `column_position` (it would require a table with 2^64-1 columns).

### Files changed

- `ferrosa-index/Cargo.toml` — added `bincode = "1"` to `[dependencies]`
  (required because the custom Serialize impl calls `bincode::serialize` for
  multi-clause conjunction paths)
- `ferrosa-index/src/lib.rs` — removed `Serialize` from `FilterPredicate`
  derive; added manual `impl serde::Serialize for FilterPredicate` and
  replaced the JSON-only `impl<'de> Deserialize<'de> for FilterPredicate`
  with a dual-format (JSON + bincode) impl
- `ferrosa-schema/Cargo.toml` — added `bincode = "1"` to `[dev-dependencies]`
- `ferrosa-schema/src/metadata/index.rs` — added bincode round-trip and
  legacy-decode tests for `IndexMetadata`
- `ferrosa-cluster/src/raft/mod.rs` — added `raft_op_variant_tag_stability`
  test that pins bincode discriminant tags for key `RaftOp` variants

### Tests added

**ferrosa-index (bincode_compat_tests)**:
- `bincode_roundtrip_single_clause_predicate` — round-trips single-clause via bincode
- `bincode_roundtrip_conjunction_predicate` — round-trips multi-clause conjunction via bincode
- `bincode_decodes_legacy_flat_predicate` — decodes a byte-for-byte legacy flat struct
- `bincode_index_type_variant_tag_stability` — pins `IndexType` variant discriminants 0–7

**ferrosa-schema (metadata::index::tests)**:
- `bincode_roundtrip_index_metadata_with_predicate` — round-trips `IndexMetadata` with predicate via bincode
- `bincode_decodes_legacy_flat_predicate_via_index_metadata` — decodes legacy `IndexMetadata` written by old build

**ferrosa-cluster (raft::tests)**:
- `raft_op_variant_tag_stability` — pins bincode discriminant tags for `CreateIndex` (13), `DropIndex` (14), `JoinNode` (22), `UpdateNodeInfo` (23), `LeaveNode` (24), `AssignTokens` (25), `ApproveNode` (27) and more

## Status: Fixed (recovery on the live fmem cluster still pending sign-off)

### Fixed (branch `fix/raft-log-filterpredicate-bincode-compat`)

- Bincode decode compatibility: new builds can decode `FilterPredicate`
  log entries written by old builds (and vice versa for single-clause case)
- Stability tests pin `IndexType` and `RaftOp` variant tags so future
  reordering is caught at CI time before it reaches a persistent log

### Fixed (branch `fix/raft-log-format-versioning`, 2026-06-12)

All four items from the original fix shape are now implemented:

1. **Versioned log envelope** — landed earlier as W1.19c (`FRE1` magic +
   `ENTRY_FORMAT_VERSION` byte in `SledLogStore::serialize_entry`, with
   legacy bare-bincode and pre-UpdateNodeInfo decode fallthroughs);
   unknown future versions fail loud
2. **CI golden-file decode gate** —
   `ferrosa-cluster/tests/fixtures/raft_log_entries_golden_v1.bin`
   freezes `serialize_entry` bytes across drift-prone variants (NodeInfo,
   IndexMetadata, FilterPredicate single + conjunction, membership);
   `golden_raft_log_entries_decode` fails any change that can no longer
   decode them. Regenerate only for an intentional, version-bumped format
   change: `FERROSA_REGEN_RAFT_GOLDEN=1 cargo test -p ferrosa-cluster
   golden_raft_log_entries_decode`
3. **`ferrosa-ctl raft log-inspect/log-truncate`** — offline operator
   tooling. `log-inspect` reports vote/committed/purge markers, the
   readable/unreadable split, and the first bad entry (index, error, hex
   preview) plus the exact recovery command; `log-truncate --from <idx>`
   drops the unreadable tail and clamps the committed marker to the
   surviving log or the snapshot-covered purge point. `--dry-run`
   previews; the destructive path refuses without `--yes`
4. **Startup error message improvement** — decode errors now name the
   failing entry index, the blast radius (metadata plane down, CQL still
   serving, /readyz not-ready), and the `ferrosa-ctl` recovery procedure
   (`SledLogStore::deserialize_entry_at` / `UnreadableLogEntry`)

Smoke-verified against a copy of the live fmem node1 raft dir: inspect
located 79 unreadable entries in `[1523..3081]` directly above the purge
point (1522); truncate removed them and clamped committed `3081 → 1522`;
re-inspect came back clean.

## Recovery procedure for the fmem cluster (pending operator approval)

State drifts as the cluster churns (originally entries 6000–6121; by
2026-06-12 evening node1 showed 79 unreadable entries in `[1523..3081]`
after a purge to 1522, vote term up to 964 from election churn). The
procedure, per node:

1. Stop the node.
2. `ferrosa-ctl raft log-inspect --data-dir <dir>` — note the first bad
   index.
3. `ferrosa-ctl raft log-truncate --data-dir <dir> --from <first-bad>
   --yes`
4. Restart after all damaged nodes are truncated.

Cost: committed metadata ops at/above the cut are discarded — acceptable
only because membership (3 nodes) and token ring (768) are stable and
CQL schema is persisted separately in `schema.json`. Requires explicit
sign-off.

## Related

- `specs/todo/bug-slow-raft-cold-start-after-graceful-shutdown.md`
- `/readyz` probe (`ferrosa/src/web/readiness.rs`) — detection path
