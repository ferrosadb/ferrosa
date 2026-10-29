---
crate: ferrosa-ctl
doc: fmea
last_updated: 2026-10-28
---

# ferrosa-ctl — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). `ferrosa-ctl` is an operator tool, so the worst outcomes are
**destructive recovery commands run against the wrong target** and **commands
that silently look successful when the node side does nothing**.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| CTL-1 | `raft log-truncate` run with the wrong `--from` (or on a node that didn't need it) | Permanent loss of committed metadata ops `>= from` on that node | 10 | 3 | 5 | 150 | `--from` is mandatory (clap-required); mutation needs `--yes`; `--dry-run` previews; `log-inspect` prints the exact `--from` to use. **Residual gap:** no cross-node consistency check — the operator must repeat correctly on every damaged node. |
| CTL-2 | Unwired HTTP endpoint (`raft transfer-leader`, `cluster add-learner/promote/demote`, `snapshot`, `restore`) treated by the operator as a working operation | Operator believes a membership/PITR change happened when it didn't | 7 | 5 | 3 | 105 | Commands surface upstream `404`/`501` with a spec pointer (fail-loud). **Residual gap:** `snapshot`/`restore` POST to endpoints the node does not yet serve, so they only "work" once the node side lands; status is not obvious from `--help`. |
| CTL-3 | `sstable s3-clean --apply` deletes object-store generations on a misread `<keyspace>.<table>` dir name | Durable S3 copies of (mis)identified gens deleted; cold restart can't recover them | 9 | 2 | 5 | 90 | Dry-run default; only CORRUPT gens (per the engine smoke test) are targeted; reuses engine `FERROSA_S3_*` config. **Residual gap:** deletion is irreversible and depends on the dir being named exactly `<keyspace>.<table>`. |
| CTL-4 | `auth set-password` quoting/escaping bug lets a crafted role/password break out of the `ALTER ROLE` statement | CQL injection in an admin path | 9 | 1 | 4 | 36 | `escape_double_quotes`/`escape_single_quotes` applied; dedicated unit tests for `'` in password and `"` in role. Low occurrence; server also validates. |
| CTL-5 | `--ssl` requested for `auth set-password` | Operator expects an encrypted admin connection; cleartext password would otherwise traverse the network | 8 | 2 | 2 | 32 | `CqlClient` has no TLS yet, so `--ssl` **errors out** rather than silently connecting in cleartext. **Open gap:** no TLS for any CQL command — admin password crosses the wire in cleartext on non-localhost. |
| CTL-6 | `sstable reingest --apply` re-inserts salvaged rows but some rows are unrecoverable / silently dropped | Partial recovery presented without a clear "what was lost" tally | 6 | 4 | 5 → 3 | 120 → 72 | Resilient salvage recovers header + leading rows; `--limit` enables a controlled first run; dry-run default. Since 2026-10-28 nothing is lost silently: skipped rows are counted by reason (`RowSkip`: key shape, key length, unknown column, no live cell), dropped fixed-width cells are counted, server rejections print their reason (first 20, then counted), and a generation that cannot be opened or salvaged is printed and fails the command after the rest are processed. Rows stream from a blocking salvage task through a 256-row channel, so memory is bounded whatever the generation size. Tests: `reconstruct_row_names_cells_from_the_column_list_of_their_own_row_kind`, `salvage_streams_reconstructed_rows_and_reports_its_result`. **Residual gap:** salvage only reaches partitions before the first undecodable one (t_d34e5ffe), and rows beyond the first decode failure in a partition are not recovered. |
| CTL-11 | `sstable reingest --apply` named a clustered row's cells from `static_columns` when the table had static columns | A regular value written into the static column (a write path for wrong data) | 9 | 3 | 8 | 216 → 18 | `reconstruct_row` indexes a clustered row into `regular_columns` and a static row into `static_columns` (the decoder's convention), and now also reingests static rows. A cell index outside its list rejects the row. Test: `reconstruct_row_names_cells_from_the_column_list_of_their_own_row_kind` (red on the old indexing). |
| CTL-12 | Admin-API bodies, metadata reads and stats that fail are turned into empty values | An empty ring, an empty success body, a missing component, a nil host id, or an unverified generation that reads as healthy | 6 | 4 | 7 | 168 → 24 | Admin-API bodies stream with no size cap (`commands/body.rs`): success bodies are copied to stdout chunk by chunk, JSON bodies are parsed as they download (bounded channel into a blocking parser, unknown fields skipped), the ring prints one entry at a time and refuses a non-object entry, and an error body is quoted from its JSON `error` field or streamed to stderr under a marker. A truncated body is an error, never a short or empty result. `open_reader` fails on any metadata read error except a missing Filter.db (warned) or CompressionInfo.db (uncompressed). `inspect`/`salvage` report why a generation did not open. `component_len` warns on a non-absence stat failure. `schema check` refuses an unparsable host id, shows an absent one as unknown, and names its reference node as the first endpoint, not "the leader" (t_515e487b). `s3-clean` streams the listing into `delete_stream`. Tests: the `copy_body_*`, `parse_json_body_*`, `error_from_body_*`, `ring_*`, `snapshot_*`, `open_reader_refuses_an_unreadable_filter_component`, `inspect_and_salvage_report_why_a_generation_did_not_open`, and `s3_clean_*` tests. |
| CTL-10 (t_78c7f689) | `sstable salvage`/`reingest` decoded rows past a misframed row in a drifted SSTable and re-inserted them through the live write path | Garbage rows (keys made of neighbouring cell bytes) written back with their original timestamps, indistinguishable from real data. A candidate secondary write path for the ~400 bad `co_occurs_with` rows; compaction re-encoding is the primary one | 9 | 3 | 8 | 216 → 36 | The decoder now refuses a row whose body disagrees with `row_size` (ferrosa-sstable ST-21), so salvage stops at the misframed row and keeps only the genuine leading rows; `sstable find` lets an operator locate rows by the bytes a bad key carries and see each cell's column, type and timestamps. `find` streams rows and emits each match as found, so the diagnostic cannot itself become an OOM on a tenant-wide match; an unreadable metadata component fails the open rather than becoming an empty buffer. Test: `find_names_cells_from_the_column_list_of_their_own_row_kind`, `find_stops_and_returns_the_error_when_the_sink_fails`, `open_reader_refuses_an_unreadable_filter_component` plus the `find_*` tests. **Residual:** `reconstruct_row` validates fixed-width key lengths only; a misread whose widths sum to `row_size` still passes (see ST-21). |
| CTL-7 | Web-API command output is the raw server body; a partial/odd JSON shape is printed verbatim | Operator misreads ring/repair/restore result | 4 | 4 | 4 | 64 | `ring` does best-effort column extraction; `repair` pretty-prints JSON when parseable else dumps raw. Non-2xx always becomes an error. Cosmetic, not data-affecting. |
| CTL-8 | `cluster bootstrap-dc` looks like it created a per-DC Raft group | Operator assumes a DC group exists | 5 | 3 | 3 | 45 | Output explicitly says "Sprint 6 scaffolding — the live HTTP wire-up lands in Sprint 7"; only derives + prints the `RaftGroupId`. |
| CTL-9 | `sstable mark-evicted --apply` run against a node that is still running, or one that overwrites a real evictor marker | Markers appear under a live engine, or a real eviction's trigger and byte figures are replaced by a recovery tool's | 8 | 3 | 3 | 72 | Probes the sled flock under `<data-dir>/raft/**/db` and refuses when held (also for a dry run); refuses `--apply` when no lock exists to probe unless `--assume-stopped`; skips generations that already carry a marker; markers record `source` and `trigger = recovered` so they are never mistaken for an eviction decision. Tests: `a_live_data_dir_is_refused_even_for_a_dry_run`, `apply_never_overwrites_a_real_evictor_marker`, `apply_without_a_lock_to_probe_needs_assume_stopped`. Residual: a node on a data dir with no raft directory cannot be detected; the probe sees only the raft lock, not other processes using the SSTables. |

## Top risks to act on

1. **CTL-1 (RPN 150)** — `raft log-truncate` is the most dangerous command:
   irreversible committed-data loss, and correctness depends on the operator
   applying the right `--from` on every damaged node. Mitigations are good but
   there is no automated multi-node guard.
2. **CTL-6 (RPN 72)** — `sstable reingest` now accounts for every skipped
   row and failed generation, but salvage still cannot reach partitions after
   the first undecodable one (t_d34e5ffe), so yield on a badly drifted file is
   a floor.
3. **CTL-2 / CTL-5** — several lifecycle commands target endpoints the node
   doesn't serve yet, and no CQL command uses TLS (admin password in cleartext
   off-localhost). Both are fail-loud today but are real functional gaps.

## Detection assets

- `tests/integration.rs` — boots a real CQL server + `system_observability.*`
  virtual tables and exercises the queries the observability commands issue.
- `src/main.rs` parsing tests (46) pin every subcommand's flags, defaults, and
  the `--from`-required / dry-run-default invariants.
- `src/auth.rs` tests cover password validation and `ALTER ROLE` escaping.
- `src/commands/sstable.rs` tests run against genuine BTI fixtures via
  `ferrosa-storage`'s `test-support` feature.

Operator compaction stop validates paired table scope before sending, sends names
as JSON values, authenticates through the admin API, and checks the 202 response
and echoed scope. A fixed 30-second request bound prevents an unreachable node
from holding the CLI indefinitely; request acknowledgement bodies are capped at
4 KiB. It reports cancellation requests, never completed termination. Parser and
HTTP fixture tests cover scope, authentication, server errors and invalid replies.
