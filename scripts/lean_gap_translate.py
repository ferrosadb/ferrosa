#!/usr/bin/env python3
"""Gate step 1: every crate in scope produces Lean.

Two milestones were being reported as one. (1) a crate *translates*: charon and aeneas run and
write a `.lean` file; (2) that Lean *elaborates*: `lake env lean` accepts it with no
`unknownIdentifier`, `synthInstanceFailed` or `invalidField`. A crate that dies inside aeneas and a
crate whose Lean is merely wrong both looked like "red", so the coverage question -- how much of
the workspace even reaches Lean -- was hidden behind the expensive question, and only the handful
of crates that elaborate cheaply were gated at all.

This asserts milestone 1 and nothing else. It is a coverage assertion, not a ratchet: there is no
count to shrink, no baseline to update, and no comparison between this run and the last. For every
crate the workspace contains, either the translate-only record says aeneas wrote a non-empty
`.lean`, or the crate is named in the scope file with a reason it cannot be measured on a runner.
A crate that is absent from both is a hole, and a hole is a failure -- silently omitting a crate is
the one thing this file exists to prevent.

It never reads a class count, and the class ceiling is a different question with a different
script: `lean_gap_ratchet.py` compares error classes against `baseline.json` and is unaffected by
`elaboration: "skipped"` because a skipped elaboration has no `lean_errors` to compare.

Usage
  lean_gap_translate.py check                  # the gate: fail unless every crate translates
  lean_gap_translate.py check --only ferrosa-ctl ferrosa-sstable
  lean_gap_translate.py scope                  # the crates this gate asserts, and the exclusions
  lean_gap_translate.py list-crates            # in-scope crates, space separated (for a sweep)

Environment
  REPO         repository root (default: this script's parent's parent)

Exit status is 0 when every in-scope crate translated, 1 otherwise.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(os.environ.get("REPO") or Path(__file__).resolve().parent.parent)
# The translate-only sweep keeps its own records. Sharing `specs/lean/gaps/` would let a
# translate-only run overwrite the elaboration records the class ratchet reads, and a record
# with no `lean_errors` under a state row that says `aeneas` is exactly the mismatch that turns
# one gate's artifact into another gate's false pass.
GAPS = REPO / "specs/lean/gaps/translate"

# The one stage a translate-only run writes when aeneas produced a file. Deliberately not
# `aeneas` and not `compiled`: neither this script nor the ratchet may confuse a translation
# with an elaboration.
TRANSLATED = "aeneas-ok"


# ---------------------------------------------------------------- inputs


def workspace_crates() -> list[str]:
    """Every library crate in the workspace -- the scope of step 1.

    Read from cargo rather than from a list kept by hand: the point of a coverage gate is that a
    crate added tomorrow is in it, and a hand-written list is exactly how five of twenty-seven
    crates ended up unmentioned.
    """
    meta = json.loads(subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=REPO, capture_output=True, text=True, check=True).stdout)
    return sorted(p["name"] for p in meta["packages"]
                  if any(t["kind"] == ["lib"] for t in p["targets"]))


def load_scope(path: Path) -> dict[str, str]:
    """Crates deliberately outside the runner sweep, with the reason, from the scope file.

    Same shape as `ratchet-scope.txt` on purpose -- `<crate> <reason>`, `#` starts a comment --
    because it is the same kind of statement: a gap in a control, written down where the next
    person will find it. A crate here is *known unmeasured*, not absent, and the gate prints each
    one with its reason every run.
    """
    out: dict[str, str] = {}
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        return out
    for lineno, raw in enumerate(lines, 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) < 2:
            # Fail loud: an entry with no reason is a crate dropped out of the gate with nobody
            # saying why, which is the failure mode the file exists to rule out.
            raise SystemExit(f"{path}:{lineno}: expected '<crate> <reason>', got {raw.strip()!r}")
        if parts[0] in out:
            raise SystemExit(f"{path}:{lineno}: {parts[0]} is already listed on another line")
        out[parts[0]] = parts[1].strip()
    return out


def load_records(inventory: Path) -> dict[str, dict]:
    """The translate-only rows in `state.json`, with each crate's detail record beside them.

    `state.json` is authoritative for *whether* a crate was measured and how that run ended;
    `<crate>.json` carries the exception line and the trace. They are merged only when they agree
    on the stage, because a detail file from an older run would report the previous crash's
    exception as this run's answer.
    """
    state = json.loads((inventory / "state.json").read_text())
    rows = state.get("crates") or {}
    out: dict[str, dict] = {}
    for name, row in rows.items():
        meas = dict(row.get("measurement") or {})
        detail: dict = {}
        path = inventory / f"{name}.json"
        if path.is_file():
            try:
                detail = json.loads(path.read_text())
            except ValueError:
                detail = {}
        if detail.get("stage") == meas.get("stage") and detail.get("elaboration") == "skipped":
            merged = dict(detail)
            for key in ("stage", "translated", "seconds", "scope"):
                if key in meas:
                    merged[key] = meas[key]
            meas = merged
        out[name] = {"measurement": meas, "measured_at": row.get("measured_at"),
                     "fingerprint": row.get("fingerprint")}
    return out


# ---------------------------------------------------------------- the assertion


def crate_failure(crate: str, row: dict | None) -> str | None:
    """Why this crate did not translate, or None when it did.

    Every branch that returns a message names the crate and the stage, because "the gate is red"
    is not actionable and "ferrosa-index crashed in aeneas" is. The `.lean` check is on the file
    the record names as well as on the size the run recorded: a record that claims translation
    while naming an empty or truncated file is worse than no record at all.
    """
    if not row:
        return (f"FAIL unmeasured: {crate} has no translate-only record at all; a crate nobody "
                f"measured is not a crate that failed, it is a crate nobody can make a claim about")
    if not row.get("fingerprint"):
        return f"FAIL unmeasured: {crate} has a row but no measurement (never completed a run)"

    meas = row.get("measurement") or {}
    stage = str(meas.get("stage") or "unknown")
    error = str(meas.get("error") or "").strip()
    suffix = f": {error[:300]}" if error else ""

    if stage == TRANSLATED:
        if meas.get("translated") is not True:
            return (f"FAIL verdict: {crate} is at {TRANSLATED} but does not say `translated: true`; "
                    f"the record and its stage disagree")
        size = meas.get("lean_bytes")
        lean = str(meas.get("lean") or "")
        if not lean:
            return (f"FAIL no-file: {crate} claims translation but names no .lean file")
        path = Path(lean)
        if not path.is_file():
            return (f"FAIL no-file: {crate} names {lean}, which does not exist; translation is "
                    f"only a claim about a file that is there -- re-run the sweep on this host "
                    f"(`scripts/lean_gap_inventory.py --translate-only --force {crate}`)")
        actual = path.stat().st_size
        if actual == 0:
            return f"FAIL empty: {crate} produced an empty .lean file ({lean})"
        if isinstance(size, int) and size > 0 and actual != size:
            return (f"FAIL truncated: {crate} recorded {size} B of Lean but {lean} holds {actual} B; "
                    f"the file on disk is not the file that was measured")
        if not isinstance(size, int) or size <= 0:
            return (f"FAIL no-file: {crate} translated but recorded no file size; the record does "
                    f"not prove a non-empty .lean was written")
        return None

    if stage == "aeneas-crash":
        return (f"FAIL crash: {crate} -- aeneas died before writing Lean "
                f"(aeneas-crash{suffix})")
    if stage == "no-lean":
        return f"FAIL no-lean: {crate} -- {error or 'aeneas produced no non-empty .lean file'}"
    if stage == "charon":
        return f"FAIL charon: {crate} -- charon produced no .llbc{suffix}"
    if stage in ("oom", "timeout", "locked"):
        return (f"FAIL unmeasured: {crate} -- the run ended at {stage}, so nothing about "
                f"translation was measured{suffix}")
    if stage in ("aeneas", "compiled"):
        # A check-mode record: elaboration was attempted, so this is not the measurement this
        # gate asserts on. Reading its `lean_errors` here would be the exact conflation of the
        # two milestones the gate exists to undo.
        return (f"FAIL wrong-mode: {crate} is recorded at {stage}, which is an elaboration "
                f"record; point --inventory at the translate-only records "
                f"(`lean_gap_inventory.py --translate-only`)")
    return f"FAIL unknown: {crate} is at stage {stage!r}, which this gate does not recognise"


def check(records: dict[str, dict], workspace: list[str],
          excluded: dict[str, str],
          all_crates: list[str] | None = None) -> tuple[list[tuple[str, str]], list[str], list[str]]:
    """(failures, known-unmeasured, notes) for the workspace in scope.

    A failure is `(crate, message)`, not a formatted string: the headline counts failing crates
    and the log prints them, and parsing one's own output to get the other is how a count and a
    list drift apart.

    `workspace` is what is being asserted this run (`--only` narrows it); `all_crates` is the whole
    workspace. The two checks that are about the *set* rather than about a crate -- a stale excuse,
    a record for something that is not a library -- run against the whole workspace, because a
    narrowed run must not report every crate it skipped as stale.

    Kept as a pure function of its inputs so the gate's rules are testable without a sweep: the
    expensive part is the measurement, and a rule about what a record means should not need one.
    """
    all_crates = all_crates if all_crates is not None else workspace
    failures: list[tuple[str, str]] = []
    known: list[str] = []
    notes: list[str] = []

    for crate in sorted(workspace):
        if crate in excluded:
            known.append(f"known unmeasured: {crate} -- {excluded[crate]}")
            continue
        why = crate_failure(crate, records.get(crate))
        if why:
            failures.append((crate, why))
        else:
            m = records[crate]["measurement"]
            notes.append(f"translated: {crate} ({m.get('lean_bytes')} B, "
                         f"{records[crate].get('measured_at') or 'undated'})")

    for crate in sorted(set(excluded) - set(all_crates)):
        # A stale exclusion is a crate that left the workspace while its excuse stayed behind,
        # and the next crate to take that name would be excused without being measured.
        failures.append((crate, f"FAIL stale: {crate} is in the scope file but not in the "
                                f"workspace; delete the line or fix the name"))

    for crate in sorted(set(records) - set(all_crates)):
        notes.append(f"measured but not a workspace lib crate: {crate}")

    return failures, known, notes


# ---------------------------------------------------------------- command line


def headline(workspace: list[str], excluded: dict[str, str],
             failures: list[tuple[str, str]]) -> str:
    scope = len(workspace)
    known = len([c for c in workspace if c in excluded])
    failing = {crate for crate, _ in failures}
    if failures:
        return (f"lean-gap-translate: FAIL -- {len(failing)} of {scope} crates in scope produce no "
                f"Lean: {', '.join(sorted(failing))}")
    return (f"lean-gap-translate: PASS -- {scope - known}/{scope} crates in scope translate"
            + (f", {known} known unmeasured on a runner" if known else ""))


def cmd_check(args) -> int:
    inventory = Path(args.inventory)
    if not (inventory / "state.json").is_file():
        raise SystemExit(
            f"lean-gap-translate: no translate-only records at {inventory}/state.json; run "
            f"`scripts/lean_gap_inventory.py --translate-only` first")
    workspace = workspace_crates()
    scoped = workspace
    if args.only:
        scoped = [c for c in workspace if c in set(args.only)]
        missing = sorted(set(args.only) - set(workspace))
        if missing:
            raise SystemExit(f"lean-gap-translate: --only names crates that are not library "
                             f"crates in this workspace: {', '.join(missing)}")
    excluded = load_scope(Path(args.scope))
    records = load_records(inventory)

    print("lean-gap-translate: this gate asserts that every crate in scope produces Lean; it says "
          "nothing about\n  whether that Lean elaborates. Class growth is `lean_gap_ratchet.py`.")

    failures, known, notes = check(records, scoped, excluded, all_crates=workspace)
    for line in known:
        print(line)
    for line in notes:
        print(line)
    for _, message in failures:
        print(message)

    print(headline(scoped, excluded, failures))
    if failures:
        crashes = len([m for _, m in failures if m.startswith("FAIL crash")])
        print(f"::error title=Lean translation gate::{len(failures)} crate(s) fail step 1"
              + (f", {crashes} of them inside aeneas" if crashes else ""))
        print("lean-gap-translate: translation is a coverage assertion, not a ceiling; a crate "
              "that produces no Lean has no gap to ratchet")
        return 1
    return 0


def cmd_scope(args) -> int:
    workspace = workspace_crates()
    excluded = load_scope(Path(args.scope))
    print(f"{len(workspace)} library crates in scope")
    for crate in workspace:
        print(f"  {crate:22} {'excluded: ' + excluded[crate] if crate in excluded else 'in scope'}")
    for crate in sorted(set(excluded) - set(workspace)):
        print(f"  {crate:22} STALE: in the scope file, not in the workspace")
    return 0


def cmd_list(args) -> int:
    excluded = load_scope(Path(args.scope))
    print(" ".join(c for c in workspace_crates() if c not in excluded))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Gate step 1: every crate in scope produces Lean (translation, not elaboration)")
    ap.add_argument("--inventory", default=str(GAPS),
                    help="directory holding the translate-only state.json and records")
    ap.add_argument("--scope", default=None,
                    help="crates excluded from the runner sweep, with reasons "
                         "(default: <inventory>/translate-scope.txt)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("check", help="fail unless every crate in scope produced Lean")
    a.add_argument("--only", nargs="*", default=None,
                   help="assert only these crates, still against the workspace")
    a.set_defaults(func=cmd_check)

    b = sub.add_parser("scope", help="print what is in scope and what is excluded, and why")
    b.set_defaults(func=cmd_scope)

    c = sub.add_parser("list-crates", help="print the in-scope crates, space separated")
    c.set_defaults(func=cmd_list)

    args = ap.parse_args()
    args.scope = args.scope or str(Path(args.inventory) / "translate-scope.txt")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
