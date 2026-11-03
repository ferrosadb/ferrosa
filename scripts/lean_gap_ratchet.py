#!/usr/bin/env python3
"""Ratchet the Lean gap inventory: reds may shrink, never grow.

`lean_gap_inventory.py` measures. Nothing measured it *against* anything, so a charon or
aeneas change, or an ordinary Rust change in a crate that used to elaborate, could add
Lean errors and the only record of it was a JSON file nobody diffed. This compares the
current records against a committed baseline and fails when any error class grew.

Three rules, and the reasons they are rules rather than heuristics:

* Growth fails closed. A class present now but absent from the baseline counts as growth
  from zero, and a crate that used to conclude but now stops at `oom`, `aeneas-crash` or
  `timeout` is a coverage regression -- also growth, because the reds it used to report
  are now unreported rather than fixed.
* Shrinking never fails. Closing reds is the entire point; requiring a baseline rewrite
  in the same commit would make closing one red block an unrelated PR.
* Numbers from two toolchains are not comparable, so a build whose identity differs from
  the baseline's pin is refused rather than compared. Aeneas output changes with the
  translator; a comparison across builds would report "growth" that is really a bump.
  The check is on the *tree* as well as the commit, so a merge commit whose tree is the
  pinned tree is accepted (a rebase that changed nothing did not change the toolchain).

Usage
  lean_gap_ratchet.py check                 # gate: fail on growth, note shrinkage
  lean_gap_ratchet.py update                # rewrite the baseline from the records
  lean_gap_ratchet.py update --accept-growth  # rewrite it even though a class grew
  lean_gap_ratchet.py list-crates           # the crates the baseline gates, space separated
  lean_gap_ratchet.py show                  # what the baseline holds

Environment
  AENEAS_SRC   checkout the aeneas binary was built from (default ~/src/aeneas)
  CHARON_SRC   checkout the charon binary was built from (default $AENEAS_SRC/charon)
  LEAN         lean binary to ask for its version (default `lean` on PATH)
  REPO         repository root (default: this script's parent's parent)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from lean_gap_inventory import tool_identity

REPO = Path(os.environ.get("REPO") or Path(__file__).resolve().parent.parent)
GAPS = REPO / "specs/lean/gaps"
BASELINE = GAPS / "baseline.json"
STATE = GAPS / "state.json"
SCOPE = GAPS / "ratchet-scope.txt"

SCHEMA = 1

# The aeneas backends' Lean library is what an elaboration error is measured against, so
# a different Lean changes every count. The version is read from the toolchain file the
# Aeneas backend pins, not guessed from a `lean --version` string.
AENEAS_REPO = "https://github.com/ferrosadb/aeneas"
CHARON_REPO = "https://github.com/AeneasVerif/charon"

RE_EXC = re.compile(r"Fatal error: exception (\w+)")

# A record with one of these stages measured the whole unit and the counts are real.
CONCLUDED = ("aeneas", "compiled", "aeneas-crash")


# ---------------------------------------------------------------- inventory -> classes


def crash_class(record: dict) -> str:
    """The exception a translator crash died of, as a class.

    `aeneas-crash` is a red like any other: the crate has no Lean at all. Grouping by the
    exception keeps `Not_found` from being laundered into a different crash -- both are a
    crash, but a new exception in a crate that used to crash differently is worth seeing.
    """
    m = RE_EXC.search(str(record.get("error") or ""))
    return f"crash.{m.group(1)}" if m else "crash.unknown"


def classes_of(record: dict) -> tuple[dict[str, int], dict[str, list[str]]]:
    """Class counts and per-class instance locations for one record.

    Instances are `<generated file>:<line>:<col>`, which is what names a *new* red. They
    are not paths into this repository: the Lean file is a temporary extraction of the
    crate, so the location identifies the generated declaration, not the Rust source.
    """
    counts: dict[str, int] = {}
    where: dict[str, list[str]] = {}

    for err in record.get("errors") or []:
        kind = err.get("kind") or "lean.unknown"
        counts[kind] = counts.get(kind, 0) + 1
        loc = err.get("location") or "?"
        where.setdefault(kind, []).append(loc)

    transl = record.get("translation_errors") or 0
    if transl:
        counts["translation.error"] = counts.get("translation.error", 0) + transl
        where.setdefault("translation.error", []).extend(
            str(m)[:160] for m in (record.get("translation_messages") or [])[:transl])

    sorry = record.get("sorry_declarations") or 0
    if sorry:
        counts["sorry.declaration"] = sorry

    stage = record.get("stage")
    if stage == "aeneas-crash":
        cls = crash_class(record)
        counts[cls] = counts.get(cls, 0) + 1
        where.setdefault(cls, []).append(str(record.get("crate") or "?"))

    return counts, where


def crate_entry(record: dict, module_records: dict[str, dict] | None = None) -> dict:
    """One crate's baseline entry: its classes, their instances, and whether it concluded.

    A module-covered crate (`ferrosa-schema`, `ferrosa-view`) has no crate-level count --
    each module run translates the code reachable from that module -- so its entries are
    per module and are compared per module. Summing them would double-count shared code
    and invent a number the measurement never made.
    """
    scope = str(record.get("scope") or "crate")
    stage = str(record.get("stage") or "unknown")
    entry: dict = {"scope": scope, "stage": stage, "classes": {}, "instances": {}}

    if stage.startswith("modules"):
        mods = record.get("modules") or {}
        entry["modules"] = {}
        entry["concluded"] = bool(mods)
        for name, mrow in sorted(mods.items()):
            mstage = str(mrow.get("stage") or "unknown")
            mrecord = {**(module_records or {}).get(name, {}), **mrow}
            counts, where = classes_of(mrecord)
            concluded = mstage in CONCLUDED and not (
                mstage in ("aeneas", "compiled") and mrow.get("lean_errors") is None)
            entry["modules"][name] = {"stage": mstage, "concluded": concluded,
                                      "classes": counts,
                                      "instances": {k: sorted(set(v)) for k, v in where.items()}}
            entry["concluded"] = entry["concluded"] and concluded
            for cls, n in counts.items():
                entry["classes"][cls] = entry["classes"].get(cls, 0) + n
                entry["instances"].setdefault(cls, []).extend(
                    f"{name}:{loc}" for loc in where.get(cls, []))
        return entry

    counts, where = classes_of(record)
    entry["classes"] = counts
    entry["instances"] = {k: sorted(set(v)) for k, v in where.items()}
    # `aeneas` means aeneas produced a file and Lean was asked about it. A run that was
    # killed mid-translation reports the same stage with no counts at all -- a stage
    # without a number is not a measurement, and treating it as one lets a crate with
    # reds be re-recorded as having none.
    entry["concluded"] = stage in CONCLUDED and not (
        stage in ("aeneas", "compiled") and record.get("lean_errors") is None)
    return entry


def merge_record(row: dict, path: Path) -> dict:
    """The counts `state.json` recorded for this run, with the error list beside them.

    `state.json` is authoritative for *whether* and *what* was measured last; the record
    file beside it carries the per-error detail that names a new red. They are merged only
    when they agree on the count: if the detail file is from an older run, its locations
    would point at reds that are no longer there, so it is dropped rather than mixed in.
    """
    meas = dict(row or {})
    try:
        detail = json.loads(path.read_text()) if path.is_file() else {}
    except ValueError:
        detail = {}
    same_run = bool(detail) and detail.get("lean_errors") == meas.get("lean_errors")
    rec = dict(detail) if same_run else {}
    for key in ("stage", "translation_errors", "lean_errors", "seconds"):
        if key in meas:
            rec[key] = meas[key]
    # `state.json` names the hole count `sorry`; the record names it `sorry_declarations`.
    if meas.get("sorry") is not None:
        rec["sorry_declarations"] = meas["sorry"]
    if not same_run:
        rec.pop("errors", None)
        rec.pop("translation_messages", None)
    return rec


def read_records(inventory: Path) -> dict[str, dict]:
    """Every crate record in the inventory directory, per crate and per module.

    `state.json` says which crates were measured and in what unit; the `<crate>.json` and
    `<crate>.<module>.json` files hold the detail. A crate whose detail is missing still
    contributes its count -- as one unclassified class -- so a total cannot grow unseen.
    """
    state = json.loads((inventory / "state.json").read_text())
    crates = state.get("crates") or {}
    out: dict[str, dict] = {}
    for name, row in crates.items():
        meas = dict(row.get("measurement") or {})
        if not meas:
            out[name] = {"stage": "unmeasured", "scope": "none"}
            continue
        if str(meas.get("stage", "")).startswith("modules"):
            module_records: dict[str, dict] = {}
            for module, mrow in (meas.get("modules") or {}).items():
                module_records[module] = merge_record(mrow, inventory / f"{name}.{module}.json")
            merged = dict(meas)
            merged["modules"] = {m: module_records[m] for m in (meas.get("modules") or {})}
            entry = crate_entry(merged, module_records)
        else:
            merged = merge_record(meas, inventory / f"{name}.json")
            entry = crate_entry(merged)
            # A count with no error list must still be ratcheted: an unclassified total that
            # grew is exactly the silent growth this gate exists to stop.
            if (merged.get("lean_errors") or 0) > sum(entry["classes"].values()):
                missing = merged["lean_errors"] - sum(
                    n for k, n in entry["classes"].items() if k.startswith("lean."))
                if missing > 0:
                    entry["classes"]["lean.unclassified"] = missing
        entry["measured_at"] = row.get("measured_at")
        entry["fingerprint"] = row.get("fingerprint")
        out[name] = entry
    return out


# ---------------------------------------------------------------- toolchain identity


def git_ok(*args: str) -> str | None:
    try:
        r = subprocess.run(["git", "-C", *args], capture_output=True, text=True)
    except OSError:
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def pin_aeneas(repo: Path | None = None) -> dict:
    """What the aeneas build is, identified by source rather than by binary.

    A binary cannot say which sources it came from, and `--version` is not a fact aeneas
    prints. The checkout it was built from can, and that is the identity a measurement is
    a function of.
    """
    repo = repo or Path(os.environ.get("AENEAS_SRC") or Path.home() / "src/aeneas")
    commit = git_ok(str(repo), "rev-parse", "HEAD")
    if commit is None:
        raise SystemExit(f"lean-gap-ratchet: {repo} is not a git checkout; set AENEAS_SRC "
                         f"to the aeneas source the binary was built from")
    tree = git_ok(str(repo), "rev-parse", "HEAD^{tree}")
    return {"repo": AENEAS_REPO, "commit": commit, "tree": tree}


def pin_charon(aeneas_repo: Path | None = None) -> dict:
    repo = Path(os.environ.get("CHARON_SRC")
                or (aeneas_repo or Path(os.environ.get("AENEAS_SRC") or Path.home() / "src/aeneas"))
                / "charon")
    commit = git_ok(str(repo), "rev-parse", "HEAD") if repo.is_dir() else None
    if commit is None:
        raise SystemExit(f"lean-gap-ratchet: {repo} is not a git checkout; set CHARON_SRC to "
                         f"the charon source the binary was built from")
    return {"repo": CHARON_REPO, "commit": commit,
            "tree": git_ok(str(repo), "rev-parse", "HEAD^{tree}")}


def pin_lean() -> dict:
    """The Lean the Aeneas backend elaborates against, from the backend's own toolchain file."""
    candidates = (Path(os.environ.get("LEAN_PKG") or Path.home() / ".local/aeneas/backends/lean"),
                  Path(os.environ.get("AENEAS_SRC") or Path.home() / "src/aeneas") / "backends/lean")
    backend = next((path for path in candidates if (path / "lean-toolchain").is_file()),
                   candidates[0])
    manifest = backend / "lean-toolchain"
    toolchain = manifest.read_text().strip() if manifest.is_file() else None
    # The same project selects Lean for elaboration, inventory and ratchet checks.
    # Never accept a failed elan invocation's stderr as a compiler identity.
    text = tool_identity([os.environ.get("LEAN") or "lean", "--version"],
                         "lean", budget=120, cwd=backend)
    match = re.search(r"version (\d+\.\d+\.\d+)", text)
    version = match.group(1) if match else text
    commit = re.search(r"commit ([0-9a-f]{7,40})", text)
    version_commit = commit.group(1) if commit else None
    return {"toolchain": toolchain, "version": version, "commit": version_commit}


def resolve_toolchain() -> dict:
    aeneas = pin_aeneas()
    aeneas_repo = Path(os.environ.get("AENEAS_SRC") or Path.home() / "src/aeneas")
    return {"aeneas": aeneas, "charon": pin_charon(aeneas_repo), "lean": pin_lean()}


def same_source(pin: dict, built: dict) -> bool:
    """Whether two identities name the same toolchain.

    Commit equality is the easy case. Tree equality covers a merge commit that changed
    nothing, which is exactly what a freshly merged aeneas main is: refusing it would
    fail the gate on the merge that landed the pinned work.
    """
    if not pin or not built:
        return False
    a, b = pin.get("commit"), built.get("commit")
    if a and b and (a.startswith(b) or b.startswith(a)):
        return True
    return bool(pin.get("tree") and built.get("tree") and pin["tree"] == built["tree"])


def same_build(repo: Path | None, pin_sha: str | None, other_sha: str | None) -> bool:
    """Whether two commit ids are the same source, consulting `repo` for trees.

    `state.json` records a commit, and so does the baseline. A difference is only drift if
    the trees differ; when the commit is not in the repo at all, this says "not the same",
    because an unverifiable identity must not pass as a verified one.
    """
    if not pin_sha or not other_sha:
        return False
    if pin_sha.startswith(other_sha) or other_sha.startswith(pin_sha):
        return True
    if not repo or not repo.is_dir() or len(pin_sha) < 40 or len(other_sha) < 40:
        return False
    a = git_ok(str(repo), "rev-parse", f"{pin_sha}^{{tree}}")
    b = git_ok(str(repo), "rev-parse", f"{other_sha}^{{tree}}")
    return bool(a and b and a == b)


def short(sha: str | None) -> str:
    return (sha or "?")[:10]


def check_drift(baseline: dict, state: dict, built: dict, verify_build: bool) -> list[str]:
    """Every way the three toolchains can disagree, as messages. Empty means comparable.

    Three records exist and all three have to agree: the baseline says what toolchain the
    numbers in it are from, `state.json` says what the last measurement run used, and the
    checkouts say what was actually built. Any disagreement makes a comparison meaningless
    -- not merely suspect -- so this returns failures, not warnings.
    """
    problems: list[str] = []
    pin = as_pin(baseline.get("toolchain") or {})
    recorded = as_pin(state.get("toolchain") or {})

    pin_a = pin["aeneas"].get("commit")
    rec_a = recorded["aeneas"].get("commit")
    if pin_a and rec_a and not same_build(
            Path(os.environ.get("AENEAS_SRC") or Path.home() / "src/aeneas"), pin_a, rec_a):
        problems.append(
            f"state.json records aeneas {short(rec_a)} but the baseline pins {short(pin_a)}: "
            f"the numbers on disk were measured with a different translator than the one the "
            f"baseline was taken with")

    pin_c = pin["charon"].get("commit")
    rec_c = recorded["charon"].get("commit")
    aeneas_repo = Path(os.environ.get("AENEAS_SRC") or Path.home() / "src/aeneas")
    if pin_c and rec_c and not same_build(
            Path(os.environ.get("CHARON_SRC") or aeneas_repo / "charon"), pin_c, rec_c):
        problems.append(
            f"state.json records charon {short(rec_c)} but the baseline pins {short(pin_c)}: "
            f"a charon change alters what reaches aeneas, so the counts are not comparable")

    pin_l = pin["lean"].get("version")
    rec_l = recorded["lean"].get("version")
    if pin_l and rec_l and pin_l != rec_l:
        problems.append(
            f"state.json records Lean {rec_l} but the baseline pins Lean {pin_l}: "
            f"elaboration errors are a property of this compiler")

    if not verify_build:
        return problems

    built_a = built.get("aeneas") or {}
    if pin_a and not same_source(pin["aeneas"], built_a):
        problems.append(
            f"the aeneas that was built is {short(built_a.get('commit'))} "
            f"(tree {short(built_a.get('tree'))}) but the baseline pins "
            f"{short(pin_a)} (tree {short(pin['aeneas'].get('tree'))}): "
            f"the toolchain being tested is not the toolchain the numbers describe")

    built_c = built.get("charon") or {}
    if pin_c and not same_source(pin["charon"], built_c):
        problems.append(
            f"the charon that was built is {short(built_c.get('commit'))} but the baseline "
            f"pins {short(pin_c)}")

    built_l = built.get("lean") or {}
    if pin_l and built_l.get("version") and pin_l != built_l["version"]:
        problems.append(
            f"the Lean on PATH is {built_l['version']} but the baseline pins "
            f"{pin_l}")
    if (pin["lean"].get("commit") and built_l.get("commit")
            and not (pin["lean"]["commit"].startswith(built_l["commit"])
                     or built_l["commit"].startswith(pin["lean"]["commit"]))):
        problems.append(
            f"the Lean on PATH is build {short(built_l['commit'])} but the baseline pins "
            f"build {short(pin['lean']['commit'])} (a moved tag is still a different compiler)")
    return problems


# ---------------------------------------------------------------- comparison


def compare(baseline: dict, fresh: dict[str, dict], only: set[str] | None = None,
            skip: set[str] | None = None) -> tuple[list[str], list[str], list[str]]:
    """(growth, shrink, notes) between a baseline and a fresh measurement.

    Every class is compared per crate, not per suite: a red that moved from one crate to
    another is both a growth and a shrink, and reporting the pair is the only way to see
    that it moved rather than appeared.
    """
    growth: list[str] = []
    shrink: list[str] = []
    notes: list[str] = []
    base_crates = baseline.get("crates") or {}

    for crate in sorted(base_crates):
        if only and crate not in only:
            continue
        if skip and crate in skip:
            # A crate that left the gate is not compared: there is no ceiling to hold it to.
            continue
        base = base_crates[crate]
        now = fresh.get(crate)
        if now is None:
            growth.append(
                f"FAIL unmeasured: {crate} is in the baseline but this run produced no record "
                f"for it; an unmeasured crate cannot shrink and must not pass")
            continue
        if base.get("concluded") and not now.get("concluded"):
            growth.append(
                f"FAIL coverage: {crate} concluded at {base.get('stage')} in the baseline but "
                f"stops at {now.get('stage')} now ({now.get('scope')}); the reds it reported "
                f"are unreported, not fixed")
            continue
        if not base.get("concluded") and now.get("concluded"):
            notes.append(f"IMPROVED coverage: {crate} now concludes at {now.get('stage')} "
                         f"(was {base.get('stage')})")
        if not (base.get("concluded") and now.get("concluded")):
            continue
        # A crash produces no Lean at all, so its "classes" are not comparable with a crate
        # that elaborated: diffing them would report the crash as one new red and every
        # vanished elaboration error as a close. Compare the outcome first.
        if outcome(base) != outcome(now):
            if outcome(base) == "counted":
                growth.append(
                    f"FAIL coverage: {crate} elaborated in the baseline ({base.get('stage')}, "
                    f"{sum((base.get('classes') or {}).values())} reds) but now stops at "
                    f"{now.get('stage')}; its reds are unreported, not fixed")
            else:
                notes.append(f"IMPROVED outcome: {crate} no longer crashes at "
                             f"{base.get('stage')} (now {now.get('stage')})")
            continue

        if str(base.get("scope", "")).startswith("modules"):
            # Compare module by module: a module that vanished is a coverage loss even if
            # the surviving modules carry the same totals.
            for module, mbase in sorted((base.get("modules") or {}).items()):
                mnow = (now.get("modules") or {}).get(module)
                if mnow is None:
                    growth.append(f"FAIL unmeasured: {crate} module {module} is in the baseline "
                                  f"but this run did not measure it")
                    continue
                if mbase.get("concluded") and not mnow.get("concluded"):
                    growth.append(f"FAIL coverage: {crate} module {module} stops at "
                                  f"{mnow.get('stage')} (was {mbase.get('stage')})")
                    continue
                growth += class_diffs(f"{crate}::{module}", mbase, mnow)
                shrink += closed(f"{crate}::{module}", mbase, mnow)
            for module in sorted(set((now.get("modules") or {})) - set((base.get("modules") or {}))):
                notes.append(f"new module measured: {crate}::{module} (not in the baseline)")
            continue

        growth += class_diffs(crate, base, now)
        shrink += closed(crate, base, now)

    for crate in sorted(set(fresh) - set(base_crates)):
        if skip and crate in skip:
            continue
        if not only or crate in only:
            notes.append(f"not in the baseline: {crate} ({fresh[crate].get('stage')}); "
                         f"`update` records it, `check` ignores it")
    return growth, shrink, notes


def closed(where: str, base: dict, now: dict) -> list[str]:
    out: list[str] = []
    now_classes = now.get("classes") or {}
    for cls, old in sorted((base.get("classes") or {}).items()):
        new = now_classes.get(cls, 0)
        if new < old:
            out.append(f"closed: {where} {cls} {old} -> {new} (-{old - new})")
    return out


def outcome(entry: dict) -> str:
    """What kind of measurement an entry is: an elaboration, or a translator crash.

    `aeneas-crash` is a concluded measurement (it is deterministic) but it is a different
    kind of answer from a Lean error count, so the two are never diffed against each other.
    """
    return "crash" if str(entry.get("stage")) == "aeneas-crash" else "counted"


def class_diffs(where: str, base: dict, now: dict) -> list[str]:
    out: list[str] = []
    base_classes = base.get("classes") or {}
    now_classes = now.get("classes") or {}
    for cls in sorted(set(base_classes) | set(now_classes)):
        old = base_classes.get(cls, 0)
        new = now_classes.get(cls, 0)
        if new == old:
            continue
        if new < old:
            continue                    # shrinkage is always acceptable; reported by caller
        base_instances = set((base.get("instances") or {}).get(cls, []))
        new_instances = [i for i in (now.get("instances") or {}).get(cls, [])
                         if i not in base_instances]
        msg = (f"FAIL growth: {where} {cls} {old} -> {new} (+{new - old})")
        if new_instances:
            msg += "\n" + "\n".join(f"        new at {i}" for i in new_instances[:20])
            if len(new_instances) > 20:
                msg += f"\n        ... and {len(new_instances) - 20} more"
        elif old == 0:
            msg += "\n        (this class did not exist in the baseline)"
        out.append(msg)
    return out


def load_scope(path: Path) -> dict[str, str]:
    """Crates deliberately outside the CI ceiling, with the reason, from ratchet-scope.txt.

    A crate that is measured but not gated is a hole in the control, and a hole nobody
    wrote down is indistinguishable from one nobody noticed. This file is that writing:
    `update` refuses to gate a crate listed here, and `check` fails if one is gated anyway.
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
            raise SystemExit(f"{path}:{lineno}: expected '<crate> <reason>', got {raw.strip()!r}")
        out[parts[0]] = parts[1].strip()
    return out


# ---------------------------------------------------------------- baseline


def build_baseline(records: dict[str, dict], toolchain: dict, crates: list[str]) -> dict:
    keep = ("scope", "stage", "concluded", "classes", "instances", "modules")
    return {
        "schema": SCHEMA,
        "note": ("Lean gap baseline. `classes` are counted per crate; a class may shrink "
                 "freely, and any growth fails scripts/lean_gap_ratchet.py check. Rewrite "
                 "with `lean_gap_ratchet.py update` after a measurement, never by hand."),
        "toolchain": toolchain,
        "crates": {c: {k: v for k, v in records[c].items() if k in keep}
                   for c in crates if c in records},
    }


def totals(baseline: dict) -> dict[str, int]:
    sums: dict[str, int] = {}
    for entry in (baseline.get("crates") or {}).values():
        for cls, n in (entry.get("classes") or {}).items():
            sums[cls] = sums.get(cls, 0) + n
    return sums


def headline(baseline: dict) -> str:
    """`N reds` and `M holes`, kept apart.

    A `sorry` is a hole, not a red: the declaration exists and admits it is unproven.
    Folding the two into one number moves the figure the plan tracks (1057 reds) by
    thousands of holes and makes neither number mean anything.
    """
    classes = totals(baseline)
    reds = sum(n for k, n in classes.items() if not k.startswith("sorry."))
    holes = sum(n for k, n in classes.items() if k.startswith("sorry."))
    return f"{reds} reds across {len(baseline.get('crates') or {})} crates, {holes} sorry holes"


def load(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        raise SystemExit(f"lean-gap-ratchet: no baseline at {path}; create one with "
                         f"`lean_gap_ratchet.py update`")
    except ValueError as exc:
        raise SystemExit(f"lean-gap-ratchet: {path} is not valid JSON: {exc}")


def as_pin(tc: dict) -> dict:
    """Normalise either toolchain shape (`state.json`'s strings or the baseline's objects).

    `state.json` predates this script and stores `{"charon": "<sha>", "aeneas": "<sha>",
    "lean": "<lean --version line>"}`. Reading both shapes in one place keeps every
    comparison free of "which format is this" branches.
    """
    aeneas = tc.get("aeneas")
    charon = tc.get("charon")
    lean = tc.get("lean")
    if isinstance(lean, str):
        m = re.search(r"version (\d+\.\d+\.\d+)", lean)
        commit = re.search(r"commit ([0-9a-f]{7,40})", lean)
        lean = {"version": m.group(1) if m else None,
                "commit": commit.group(1) if commit else None}
    return {
        "aeneas": aeneas if isinstance(aeneas, dict) else {"commit": aeneas},
        "charon": charon if isinstance(charon, dict) else {"commit": charon},
        "lean": lean if isinstance(lean, dict) else {},
    }


def describe_toolchain(tc: dict) -> str:
    tc = as_pin(tc)
    a = (tc.get("aeneas") or {}).get("commit")
    c = (tc.get("charon") or {}).get("commit")
    lean = tc.get("lean") or {}
    return (f"aeneas {short(a)} · charon {short(c)} · "
            f"Lean {lean.get('version') or lean.get('toolchain') or '?'}")


# ---------------------------------------------------------------- commands


def cmd_check(args) -> int:
    baseline = load(Path(args.baseline))
    state_path = Path(args.state)
    if not state_path.is_file():
        raise SystemExit(f"lean-gap-ratchet: no measurement state at {state_path}; run "
                         f"scripts/lean_gap_inventory.py first")
    state = json.loads(state_path.read_text())

    built = resolve_toolchain() if not args.no_verify_build else {}
    drift = check_drift(baseline, state, built, verify_build=not args.no_verify_build)
    if drift:
        print("lean-gap-ratchet: refusing to compare -- the toolchains disagree")
        for problem in drift:
            print(f"FAIL toolchain drift: {problem}")
        print("::error title=Lean gap ratchet::toolchain drift; the inventory and the "
              "baseline were taken with different builds")
        return 1

    scope = load_scope(Path(args.scope))
    gated_out = sorted(set(baseline.get("crates") or {}) & set(scope))
    if gated_out:
        for crate in gated_out:
            print(f"FAIL scope: {crate} is in the baseline but ratchet-scope.txt declares it "
                  f"outside the gate ({scope[crate]}); one of the two is wrong")
        return 1

    fresh = read_records(Path(args.inventory))
    only = set(args.only) if args.only else None
    growth, shrink, notes = compare(baseline, fresh, only, skip=set(scope))
    for note in notes:
        print(note)

    n = len(baseline.get("crates") or {})
    print(f"lean-gap-ratchet: {describe_toolchain(baseline.get('toolchain') or {})} · "
          f"{n} gated crate{'' if n == 1 else 's'}")
    print(f"lean-gap-ratchet: baseline holds {headline(baseline)}")
    for line in shrink:
        print(line)

    if growth:
        for line in growth:
            print(line)
        classes = sum(1 for line in growth if line.startswith("FAIL growth:"))
        print(f"::error title=Lean gap ratchet::{classes} class(es) grew; a red may be "
              f"closed but never added")
        print("lean-gap-ratchet: FAIL -- the baseline is a ceiling, not a report")
        return 1

    print(f"lean-gap-ratchet: no class grew across "
          f"{len(baseline.get('crates') or {})} crate"
          f"{'' if len(baseline.get('crates') or {}) == 1 else 's'}"
          + (f"; {len(shrink)} class(es) shrank, record them with `update`" if shrink else ""))
    return 0


def cmd_update(args) -> int:
    records = read_records(Path(args.inventory))
    toolchain = resolve_toolchain() if not args.no_verify_build else json.loads(
        Path(args.state).read_text()).get("toolchain")
    if not args.no_verify_build:
        state = json.loads(Path(args.state).read_text())
        drift = check_drift({"toolchain": toolchain}, state, toolchain, verify_build=True)
        if drift:
            for problem in drift:
                print(f"FAIL toolchain drift: {problem}")
            print("lean-gap-ratchet: refusing to record a baseline from records this build "
                  "did not produce")
            return 1

    path = Path(args.baseline)
    old = json.loads(path.read_text()) if path.is_file() else None
    only = set(args.only) if args.only else None
    scope = load_scope(Path(args.scope))
    in_scope = [c for c in records if (not only or c in only) and c in scope]
    if in_scope:
        for crate in sorted(in_scope):
            print(f"not gated (ratchet-scope.txt): {crate} -- {scope[crate]}")
    crates = sorted(c for c in records if (not only or c in only)
                    and records[c].get("concluded") and c not in scope)
    skipped = sorted(c for c in records if (not only or c in only)
                     and not records[c].get("concluded"))

    if old is not None:
        growth, shrink, _ = compare(old, records, only, skip=set(scope))
        # A crate leaving the measurable set is not "growth to accept": it is a crate with
        # no ceiling any more, and recording it would silently drop it from the ratchet.
        # Only count growth can be accepted, and only on purpose.
        coverage = [g for g in growth if g.startswith(("FAIL coverage", "FAIL unmeasured"))]
        if coverage:
            for line in coverage:
                print(line)
            print("lean-gap-ratchet: refusing to record a baseline that drops a crate out of "
                  "the gate; restore the measurement (this is what the ratchet exists to catch)")
            return 1
        if growth and not args.accept_growth:
            for line in growth:
                print(line)
            print("lean-gap-ratchet: refusing to ratchet the baseline up; fix the reds or "
                  "pass --accept-growth to record the decision")
            return 1
        for line in growth:
            print(f"accepted: {line}")
        for line in shrink:
            print(f"recorded: {line}")

    new = build_baseline(records, toolchain, crates)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(new, indent=2, sort_keys=True) + "\n")
    print(f"wrote {path}: {headline(new)} ({describe_toolchain(toolchain)})")
    if skipped:
        print(f"not recorded (did not conclude, so they have no ceiling to hold): "
              f"{', '.join(skipped)}")
    return 0


def cmd_list(args) -> int:
    baseline = load(Path(args.baseline))
    print(" ".join(sorted(baseline.get("crates") or {})))
    return 0


def cmd_show(args) -> int:
    baseline = load(Path(args.baseline))
    print(f"toolchain: {describe_toolchain(baseline.get('toolchain') or {})}")
    classes = totals(baseline)
    print(f"total:     {headline(baseline)}")
    for crate, entry in sorted((baseline.get("crates") or {}).items()):
        n = sum((entry.get("classes") or {}).values())
        top = sorted((entry.get("classes") or {}).items(), key=lambda kv: -kv[1])[:3]
        detail = ", ".join(f"{k} {v}" for k, v in top) or "-"
        print(f"  {crate:22} {entry.get('stage'):12} {n:4}  {detail}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    # Everything lives in one directory: the records, `state.json`, the baseline and the
    # scope file. Defaulting `--inventory` alone therefore has to take the rest with it,
    # or a check can compare a scratch inventory against the repository's state.
    ap.add_argument("--inventory", default=str(GAPS), help="directory holding <crate>.json records")
    ap.add_argument("--baseline", default=None,
                    help="baseline to check against (default: <inventory>/baseline.json)")
    ap.add_argument("--state", default=None,
                    help="measurement state (default: <inventory>/state.json)")
    ap.add_argument("--scope", default=None,
                    help="crates outside the gate (default: <inventory>/ratchet-scope.txt)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("check", help="fail when any class grew; shrinking passes")
    a.add_argument("--only", nargs="*", default=None)
    a.add_argument("--no-verify-build", action="store_true",
                   help="skip the check that the built toolchain is the pinned one")
    a.set_defaults(func=cmd_check)

    b = sub.add_parser("update", help="rewrite the baseline from the current records")
    b.add_argument("--only", nargs="*", default=None)
    b.add_argument("--accept-growth", action="store_true")
    b.add_argument("--no-verify-build", action="store_true")
    b.set_defaults(func=cmd_update)

    c = sub.add_parser("list-crates", help="print the gated crates, space separated")
    c.set_defaults(func=cmd_list)

    d = sub.add_parser("show", help="summarise the baseline")
    d.set_defaults(func=cmd_show)

    args = ap.parse_args()
    inventory = Path(args.inventory)
    args.baseline = args.baseline or str(inventory / "baseline.json")
    args.state = args.state or str(inventory / "state.json")
    args.scope = args.scope or str(inventory / "ratchet-scope.txt")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
