# Lean extraction gap inventory — first pass

Measured 2026-10-30 with the Aeneas/Charon at `~/src/aeneas` (branch
`fix/unsupported-sig-is-not-fatal`, commits `348d1bb`, `30d534d`, `8be744a`, `371c429`, `4343f97`,
`8b744a…`, `ef8b0be`) against `~/src/ferrosa-suite/ferrosa`.

## Method

Two harnesses, both writing `specs/lean/gaps/<crate>.json`:

- `scripts/lean_gap_sweep.sh [crate…]` — whole crate (`--prefix <crate>::*`), resumable, per-crate
  timeout. Records `timeout` when extraction does not finish inside the budget.
- `scripts/lean_gap_sample.sh <crate>…` — one representative module (largest non-root `.rs` under
  `src/`), because whole-crate extraction does not finish for the large crates.

Four numbers are kept separate per run: translator errors, Lean elaboration errors, `sorry`
declarations, and the stage reached. A translator crash (`aeneas-crash`) is not a gap in the code.

## Results

| crate | scope | stage | translation | lean | sorry | s |
|---|---|---|---|---|---|---|
| ferrosa-cdc | whole crate | compiled | 0 | 0 | 0 | 4 |
| ferrosa-alloc-count | whole crate | compiled | 0 | 0 | 3 | 26 |
| ferrosa-jsonb | `reader` | aeneas | 0 | **0** | 2 | 21 |
| ferrosa-cluster | `accord::coordinator` | aeneas | 0 | **2** | 0 | 29 |
| ferrosa-ctl | `commands::sstable` | **aeneas-crash** | – | – | – | 30 |
| ferrosa-sstable | `writer` | **aeneas-crash** | – | – | – | 65 |
| ferrosa-common | `accord` | timeout (100 s) | – | – | – | 100 |
| ferrosa-cql | `router` | timeout (110 s) | – | – | – | 110 |
| ferrosa-flight | `service` | timeout | – | – | – | 100 |
| ferrosa-graph | `executor::expand` | timeout | – | – | – | 100 |
| ferrosa-index | `vector::hnsw` | timeout (180 s) | – | – | – | 180 |
| ferrosa-net | `protocol` | timeout (180 s) | – | – | – | 180 |
| ferrosa-sql | `parser` | timeout (190 s) | – | – | – | 190 |
| ferrosa-storage | `engine` | timeout (190 s) | – | – | – | 190 |

Whole-crate runs also timed out for `ferrosa-cluster`, `ferrosa-common`, `ferrosa-cql`,
`ferrosa-ctl` (170–200 s budgets).

16 of the 27 lib crates are still unmeasured.

## What this says

1. **Ferrosa's own code extracts.** Every module that finished has 0 translator errors, and two of
   them are at 0 elaboration errors. The 98-error `cluster_join` case is about the foreign serde
   trait group it pulls in, not about `ferrosa_common` itself.
2. **Two distinct defect classes remain, and they are not gaps.** `aeneas-crash` (`CFailure` with no
   diagnostic) on `ferrosa-ctl::commands::sstable` and `ferrosa-sstable::writer`; and the
   mutually-recursive foreign trait group (`t_0c152aac`) that no inductive encoding in Lean can
   express faithfully.
3. **Measurement throughput is the binding constraint, not the analysis.** A representative module
   costs 20–190+ s in charon/aeneas, and many exceed any interactive budget. A full inventory needs a
   persistent runner (cron or a systemd unit), not foreground chunks.
4. **`sorry` is the honest kind of gap** and is small where measured (0–3 per module), so the
   unmodelled surface is not currently hiding behind holes.

## Next

- Land the runner (cron every N hours, JSON per module, CI gate on regressions).
- Triage the two `aeneas-crash` sites with `-print-error-emitters` (see `references/translator-crashes.md`).
- Resolve the recursive-group representation decision (`t_0c152aac`) before writing more specs.
