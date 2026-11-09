# TODO: ferrosa-plpgsql is unmeasured in the Lean gap inventory

**Severity:** Low (a coverage hole in the extraction ratchet)
**Component:** Lean gap inventory (`scripts/lean_gap_inventory.py`, `scripts/lean_gap_ratchet.py`)
**Files:** `specs/lean/gaps/dag.md`, `specs/lean/gaps/baseline.json`

## Issue

The PL/pgSQL crate added on `feat/postgres-oban` shows as `ferrosa-plpgsql — unmeasured` in
`specs/lean/gaps/dag.md`. It has no ceiling in `baseline.json`, so the "may shrink, never grow"
ratchet does not gate it, and Aeneas extraction gaps can grow in it unseen.

## What closes it

Measure the crate on the pinned toolchain (aeneas 80b690a9, charon 7b462d91, Lean 4.35.0)
with `python3 scripts/lean_gap_inventory.py --only ferrosa-plpgsql`, then record its baseline
with `scripts/lean_gap_ratchet.py update`. The update is a new ceiling, not growth.

Recorded 2026-11-09 while consolidating PR 656, because the forge task board was unreachable.
