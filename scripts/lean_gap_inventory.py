#!/usr/bin/env python3
"""Inventory the Lean gap for every lib crate, in dependency order, incrementally.

Why this exists rather than a shell loop: the suite is 27 crates with a real dependency DAG, and
extraction of a crate depends on its workspace dependencies' *source* (charon inlines reachable
code), so a change in a dependency invalidates the dependents. Measuring in DAG order and keying
each row on a fingerprint of (crate sources + transitive workspace dependency sources + toolchain)
means a re-run only touches what actually changed.

Correctness: identity uses the same project/toolchain context as elaboration.
Last revised: 2026-11-03
Last changed: Probe Lean in the built backend project without assuming a global elan default.

Per crate it measures the *whole* crate. A module sample is opt-in (`--sample`), because a sample is
not coverage: `ferrosa-jsonb`'s `reader` module elaborated with no Lean errors while the whole crate
dies inside aeneas in the region pass, so a green sample can hide a red crate. There is no time
budget by default either -- the bound that protects the host is memory, and lean_gap_report.py
enforces it per extraction. A crate a memory limit stops is covered module by module instead: every
module the crate root declares gets its own run and its own record, and the crate settles only when
all of them concluded. A kill is not a conclusion -- it replaces the reds that crate would have
reported with nothing -- so the row carries the reason it stopped (`crate_level`, `stopped`) and
the coverage gate still fails.

Usage:
  lean_gap_inventory.py                 # measure the whole of every crate whose sources changed
  lean_gap_inventory.py --sample        # opt in to a module sample when a crate cannot be whole
  lean_gap_inventory.py --force ferrosa-cdc
  lean_gap_inventory.py --only ferrosa-cdc ferrosa-common   # subset, still DAG-ordered
  lean_gap_inventory.py --dry-run       # print the plan, measure nothing
  lean_gap_inventory.py --dag-only      # regenerate dag.json/dag.md only
  lean_gap_inventory.py --rekey         # keep cached rows when a toolchain field became resolvable

Artifacts (all committed):
  specs/lean/gaps/state.json      per-crate fingerprint + last measurement
  specs/lean/gaps/dag.json        crate DAG in topological order
  specs/lean/gaps/dag.md          mermaid graph + checklist
"""
from __future__ import annotations

from typing import Any

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

try:                                      # 3.11+
    import tomllib
except ModuleNotFoundError:               # pragma: no cover - CI and the dev boxes are 3.11+
    tomllib = None                        # type: ignore[assignment]

REPO = Path(os.environ.get("REPO", Path.home() / "src/ferrosa-suite/ferrosa"))
GAPS = REPO / "specs/lean/gaps"
STATE = GAPS / "state.json"
DAG_JSON = GAPS / "dag.json"
DAG_MD = GAPS / "dag.md"
CHARON = os.environ.get("CHARON", str(Path.home() / "src/aeneas/charon/bin/charon"))
AENEAS = os.environ.get("AENEAS", str(Path.home() / "src/aeneas/src/_build/default/main.exe"))
LEAN_PKG = os.environ.get("LEAN_PKG", str(Path.home() / ".local/aeneas/backends/lean"))
# The sources the two binaries were built from. CI builds a pinned commit in a clone, so
# the identity has to come from wherever that clone is, not from `$HOME/src/aeneas` on the
# developer's machine -- a state.json that names the wrong build is the drift the ratchet
# exists to catch.
AENEAS_SRC = Path(os.environ.get("AENEAS_SRC") or Path.home() / "src/aeneas")
CHARON_SRC = Path(os.environ.get("CHARON_SRC") or AENEAS_SRC / "charon")
# No time budget by default. A cap turns "this crate needs eleven minutes" into "this crate is
# unmeasured", and the sweep exists for coverage, not for a fast report. The bound that actually
# protects the host is memory: lean_gap_report.py puts every extraction in a cgroup with a ceiling
# and records `oom` when it bites. Set WHOLE_BUDGET to bound a crate anyway; whatever was in force
# is written into state.json so a row can be read back later.
WHOLE_BUDGET = int(os.environ.get("WHOLE_BUDGET", "0"))
# Used by `--sample`, and by the per-module coverage of a crate that cannot be extracted whole.
# Unbounded by default for the same reason as WHOLE_BUDGET.
MODULE_BUDGET = int(os.environ.get("MODULE_BUDGET", "0"))


# ---------------------------------------------------------------- workspace facts


def metadata() -> dict:
    out = subprocess.run(["cargo", "metadata", "--no-deps", "--format-version", "1"],
                         cwd=REPO, capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def lib_crates(meta: dict) -> dict[str, dict]:
    crates: dict[str, dict] = {}
    # (typed so the state dict below is not inferred as a single value type)
    for p in meta["packages"]:
        if not any(t["kind"] == ["lib"] for t in p["targets"]):
            continue
        crates[p["name"]] = {
            "dir": Path(p["manifest_path"]).parent,
            # A crate can list itself as a dev-dependency; a self-edge is not an ordering fact.
            "deps": sorted({d["name"] for d in p["dependencies"]
                            if d.get("path") is not None and d["name"] != p["name"]}),
        }
    return crates


def dag(crates: dict[str, dict]) -> list[str]:
    """Topological order over workspace-internal lib dependencies (dependencies first)."""
    seen: dict[str, int] = {}
    order: list[str] = []

    def visit(name: str, stack: tuple[str, ...]) -> None:
        if name in stack:                      # cycle: record and stop, do not recurse forever
            raise SystemExit(f"dependency cycle: {' -> '.join(stack + (name,))}")
        if seen.get(name) == 2:
            return
        seen[name] = 1
        for dep in crates.get(name, {}).get("deps", []):
            if dep in crates:
                visit(dep, stack + (name,))
        seen[name] = 2
        order.append(name)

    for name in sorted(crates):
        visit(name, ())
    return order


def tool_identity(cmd: list[str], what: str, budget: int = 300,
                  *, cwd: str | Path | None = None) -> str:
    """One line naming `what`, or a loud failure.

    A toolchain that cannot be named is a measurement that cannot be reproduced. It used to be
    written as `unknown`, which then differed from the recorded identity on the next run and
    dropped the whole cache -- so a host that was merely busy read as a new toolchain.
    """
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=budget,
                           cwd=REPO if cwd is None else cwd)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(f"lean_gap_inventory: cannot identify {what}: {exc} "
                         f"({cmd[0]}); point the environment at the toolchain and re-run")
    lines = (r.stdout or r.stderr).strip().splitlines()
    if r.returncode != 0 or not lines:
        raise SystemExit(f"lean_gap_inventory: {what} reported no identity "
                         f"(exit {r.returncode}): {' '.join(cmd)}")
    return lines[0]


def toolchain() -> dict:
    """Identity of the toolchain a measurement is valid under.

    `lean` is asked through `lake env lean` first, because that is what elaborates a measurement,
    then through the elan shim. Both probes run in the built backend project:
    its lean-toolchain selects the compiler even when elan has no global default.
    A timeout in the first form says the host was loaded, not that Lean moved.

    charon and aeneas are identified by the *sources* their binaries were built from, read from
    the checkouts named by CHARON_SRC and AENEAS_SRC. A version string cannot do this job for
    either tool, and a developer's `$HOME/src/aeneas` is not the build CI ran.
    """
    charon_id = git_identity(CHARON_SRC, "charon")
    if charon_id is None:
        pin = AENEAS_SRC / "charon-pin"
        if pin.is_file():
            # The pin file carries a comment line above the commit it names.
            charon_id = next((ln.strip() for ln in reversed(pin.read_text().splitlines())
                              if ln.strip() and not ln.lstrip().startswith("#")), "")
            if not charon_id:
                raise SystemExit(f"lean_gap_inventory: {pin} names no charon commit")
        else:
            charon_id = tool_identity([CHARON, "--version"], "charon")
    aeneas_id = git_identity(AENEAS_SRC, "aeneas")
    if aeneas_id is None:
        raise SystemExit(f"lean_gap_inventory: {AENEAS_SRC} is not a git checkout; set AENEAS_SRC "
                         f"to the source the aeneas binary was built from")
    lean = None
    for cmd in (["lake", "env", "lean", "--version"], ["lean", "--version"]):
        try:
            lean = tool_identity(cmd, "lean", cwd=LEAN_PKG)
            break
        except SystemExit:
            continue
    if lean is None:
        raise SystemExit("lean_gap_inventory: cannot identify lean through lake or through elan")
    return {
        "charon": charon_id,
        "aeneas": aeneas_id,
        "lean": lean,
    }


def git_identity(repo: Path, what: str) -> str | None:
    """The full commit of a checkout, or None when it is not one."""
    try:
        r = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                           capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired):
        return None
    sha = (r.stdout or "").strip()
    if r.returncode != 0 or not sha:
        return None
    return sha


def crate_fingerprint(name: str, crates: dict[str, dict], tc: dict) -> str:
    """Hash crate sources + transitive workspace dependency sources + toolchain identity."""
    h = hashlib.sha256()
    h.update(json.dumps(tc, sort_keys=True).encode())
    seen: set[str] = set()

    def add(n: str) -> None:
        if n in seen or n not in crates:
            return
        seen.add(n)
        src = crates[n]["dir"] / "src"
        files = sorted(src.rglob("*.rs")) if src.is_dir() else []
        for f in files:
            h.update(str(f.relative_to(REPO)).encode())
            h.update(f.read_bytes())
        for f in (crates[n]["dir"] / "Cargo.toml",):
            if f.is_file():
                h.update(f.read_bytes())
        for dep in crates[n]["deps"]:
            add(dep)

    add(name)
    return h.hexdigest()[:16]


def toolchain_only_unresolved(recorded: dict, current: dict) -> bool:
    """True when two identities differ only in a field one of them failed to resolve.

    `unknown` is a resolution failure, not a different compiler, so a row measured under it is
    still valid. A field that differs between two *resolved* values is a real toolchain change and
    invalidates the row -- hence the test is per field, not on the identity as a whole.
    """
    return (sorted(recorded) == sorted(current) and recorded != current
            and all(recorded[k] == current[k] or "unknown" in (recorded[k], current[k])
                    for k in recorded))


def should_adopt(prev: dict, fingerprint_at_recorded_tc: str, fingerprint: str,
                 recorded_tc: dict, current_tc: dict) -> bool:
    """Whether a cached measurement survives a change in the recorded toolchain identity.

    Only the identity may have moved. The row's fingerprint must still match when this crate's
    sources are hashed against the toolchain the row recorded; if it does not, the sources moved as
    well and the measurement is stale for a reason this rule does not excuse.
    """
    return (bool(prev.get("measurement"))
            and prev.get("fingerprint") == fingerprint_at_recorded_tc
            and prev.get("fingerprint") != fingerprint
            and toolchain_only_unresolved(recorded_tc, current_tc))


# A `mod NAME;` declaration in a crate root, with whatever attributes sit directly above it.
# Only the `;` form is matched: an inline `mod x { ... }` has no file of its own and no prefix
# of its own to start a run from.
_MOD_DECL = re.compile(
    r"(?P<attrs>(?:^[ \t]*#\[[^\n]*\][ \t]*\r?\n)*)"
    r"^[ \t]*(?:pub(?:[ \t]*\([^)\n]*\))?[ \t]+)?mod[ \t]+(?P<name>[A-Za-z_][A-Za-z0-9_]*)[ \t]*;",
    re.MULTILINE)
_CFG_ATTR = re.compile(r"^[ \t]*#\[[ \t]*cfg[ \t]*\((.*)\)[ \t]*\][ \t]*$", re.MULTILINE)


def default_features(crate_dir: Path) -> set[str]:
    """The features a plain `--lib` build compiles with. Empty when the manifest is unreadable."""
    if tomllib is None:
        return set()
    try:
        manifest = tomllib.loads((crate_dir / "Cargo.toml").read_text())
    except (OSError, ValueError):
        return set()
    default = (manifest.get("features") or {}).get("default") or []
    return {str(name) for name in default if not str(name).startswith("dep:")}


def _split_top_level(expr: str) -> list[str]:
    parts, depth, current = [], 0, []
    for char in expr:
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        depth += 1 if char == "(" else -1 if char == ")" else 0
        current.append(char)
    if "".join(current).strip():
        parts.append("".join(current))
    return [part.strip() for part in parts]


def cfg_value(expr: str, features: set[str]) -> bool | None:
    """Whether a `cfg(...)` body holds for a plain `--lib` build; None when it cannot be told.

    Only the two facts this build actually has are evaluated: `test` is false (charon compiles
    `--lib`, never `--test`) and a `feature = "x"` is true only when `x` is on by default.
    Anything else -- `target_os`, a cfg alias -- is unknown, and unknown means *keep the
    module*: a module dropped on a guess is coverage lost with nothing to show for it.
    """
    expr = expr.strip()
    call = re.match(r"^(any|all|not)\s*\((.*)\)$", expr, re.S)
    if call:
        op, values = call.group(1), [cfg_value(p, features) for p in
                                     _split_top_level(call.group(2))]
        if op == "not":
            value = values[0] if values else None
            return None if value is None else not value
        if op == "any":
            return True if True in values else (None if None in values else False)
        return False if False in values else (None if None in values else True)
    feature = re.match(r'^feature\s*=\s*"(.*)"$', expr)
    if feature:
        return feature.group(1) in features
    if re.match(r"^[A-Za-z_][\w-]*$", expr):
        return False if expr == "test" else None
    return None


def declared_modules(crate_dir: Path) -> list[str]:
    """Top-level modules the crate root declares *and this build compiles*.

    A directory listing is not a module list. `ferrosa-sstable`'s `src/` also holds `bin/`
    (binary targets), files no `mod` line mentions, and modules behind `#[cfg(test)]` or a
    non-default feature. charon only translates the library, so a prefix naming one of those
    matches no item, produces no `.llbc`, and can never conclude -- which made the crate
    permanently `modules-partial` however well every real module measured.
    """
    try:
        text = (crate_dir / "src/lib.rs").read_text()
    except OSError:
        return []
    features = default_features(crate_dir)
    found = []
    for match in _MOD_DECL.finditer(text):
        gates = _CFG_ATTR.findall(match.group("attrs"))
        if any(cfg_value(gate, features) is False for gate in gates):
            continue
        found.append(match.group("name"))
    return sorted(found)


def scan_modules(crate_dir: Path) -> list[str]:
    """Every file stem and Rust-bearing directory under `src/`, by name.

    The fallback for a crate root this script cannot read: a `lib.rs` that `include!`s its
    modules or declares none. Narrowing the module set on a parse failure would drop coverage
    silently, so the older, wider answer is the safe one here.
    """
    src = crate_dir / "src"
    if not src.is_dir():
        return []
    found = []
    for path in sorted(src.iterdir()):
        if path.is_file() and path.suffix == ".rs" and path.stem != "lib":
            found.append(path.stem)
        elif path.is_dir() and any(path.glob("*.rs")):
            found.append(path.name)
    return found


def modules(crate_dir: Path) -> list[str]:
    """The module set a crate that cannot be extracted whole is covered by, one run each.

    Read from the crate root, because a module the library does not contain has no prefix charon
    can start from. A root that declares nothing falls back to the directory, so an unreadable
    file never shrinks the set.
    """
    return declared_modules(crate_dir) or scan_modules(crate_dir)


def sample_module(crate_dir: Path) -> str | None:
    src = crate_dir / "src"
    if not src.is_dir():
        return None
    best, size = None, -1
    for f in src.rglob("*.rs"):
        if f.name in ("lib.rs", "main.rs"):
            continue
        parts = [p for p in f.relative_to(src).with_suffix("").parts if p != "mod"]
        if parts and f.stat().st_size > size:
            best, size = "::".join(parts), f.stat().st_size
    return best


# ---------------------------------------------------------------- measurement

# A stage that means the crate was actually read end to end. `aeneas-crash` counts: it is a
# deterministic property of this crate under this toolchain, and re-running it produces the same
# exception until the translator is fixed -- a bad answer, but an answer. A kill is not on this
# list on purpose: `oom` and `killed` say the reds were never read, so treating either as an
# answer would let a smaller host report a smaller gap.
CONCLUDED_STAGES = ("aeneas", "compiled", "aeneas-crash")


def settles(row: dict, fingerprint: str) -> bool:
    """Whether a cached row answers the coverage question for this crate.

    Only a row that measured *the crate* and finished the job does. A module sample is a different
    unit, and `timeout`/`oom`/`locked`/`unknown` record the absence of a measurement; treating any
    of those as done is how a sweep reports coverage it never had.
    """
    meas = row.get("measurement") or {}
    if row.get("fingerprint") != fingerprint:
        return False
    if meas.get("stage") == "modules":
        # Covered module by module because the crate does not fit in memory. Coverage is every
        # module concluding, and `modules-partial` is not that.
        mods = meas.get("modules") or {}
        return bool(mods) and all(m.get("stage") in CONCLUDED_STAGES for m in mods.values())
    return meas.get("scope") == "crate" and meas.get("stage") in CONCLUDED_STAGES


def run_report(crate: str, prefix: str | None, budget: int, work_log: Path,
               out: Path | None = None) -> dict:
    cmd = [sys.executable, str(REPO / "scripts/lean_gap_report.py"), crate]
    if prefix:
        cmd += ["--prefix", prefix]
    if out is not None:
        # A per-module run needs its own record or every module overwrites the crate's file and
        # only the last one's error list survives.
        cmd += ["--out", str(out)]
    started = time.time()
    # The report script serializes extractions machine-wide. Waiting for that lock is not
    # measuring, so the child is told to give up just inside our own budget and say so: a queued
    # crate reported as `timeout` would read as "charon is too slow on this crate" when nothing
    # was extracted at all. A budget of 0 means no time limit at all -- the lock wait is then
    # unbounded too, because a capped wait would be the time limit coming back in through the door.
    lock_budget = 0 if budget <= 0 else (max(budget - 10, 1) if budget > 10 else budget)
    try:
        done = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True,
                              timeout=budget or None,
                              env={**os.environ, "LEAN_GAP_LOCK_TIMEOUT": str(lock_budget)})
        # One sentinel phrase, shared with lean_gap_sweep.sh and lean_gap_sample.sh. It must be
        # the give-up message, not the waiting one: a run that waited and then succeeded is a
        # measurement, and one that waited and then failed for another reason is that failure.
        gave_up = "gave up waiting for the extraction lock"
        queued = done.returncode != 0 and gave_up in (done.stderr or "")
        stage = "locked" if queued else None
    except subprocess.TimeoutExpired:
        stage = "timeout"
    secs = round(time.time() - started)
    data = {}
    try:
        data = json.loads((out or GAPS / f"{crate}.json").read_text())
    except Exception:
        pass
    # A queued or timed-out run produced nothing; carrying the previous run's counts beside a
    # `locked`/`timeout` stage would read as a measurement that was never made.
    measured = stage is None
    row = {
        "stage": stage or data.get("stage", "unknown"),
        "translation_errors": data.get("translation_errors") if measured else None,
        "lean_errors": data.get("lean_errors") if measured else None,
        "sorry": data.get("sorry_declarations") if measured else None,
        "seconds": secs,
    }
    work_log.write_text(json.dumps(row))
    return row


def coverage_row(rows: dict[str, dict], crate_level: str) -> dict:
    """The crate's row when it was covered module by module instead of in one run.

    No crate-level count is claimed: each module run translates the code reachable from that
    module, so summing `lean_errors` across modules would count shared code more than once. The
    per-module rows are the authoritative numbers; this row only says how much of the crate
    concluded.

    A partial row names *where* it stopped and *why*. Without that, `modules (1 measured)` -- the
    string the 2026-11-04 job printed -- is a count with no cause, and a reader has to guess
    whether the cap, the budget or the module itself was at fault.
    """
    stages = [r.get("stage") for r in rows.values()]
    concluded = [s for s in stages if s in CONCLUDED_STAGES]
    complete = bool(stages) and len(concluded) == len(stages)
    row = {
        "scope": f"modules ({len(concluded)}/{len(stages)})",
        "stage": "modules" if complete else "modules-partial",
        "translation_errors": None,
        "lean_errors": None,
        "sorry": None,
        "seconds": sum(r.get("seconds") or 0 for r in rows.values()),
        "crate_level": crate_level,
        "modules": rows,
    }
    if stages and not complete:
        module, mrow = next((name, r) for name, r in rows.items()
                            if r.get("stage") not in CONCLUDED_STAGES)
        row["scope"] = f"{row['scope']}; stopped at {module}: {mrow.get('stage')}"
        row["stopped"] = {"module": module, "stage": mrow.get("stage"),
                          "error": mrow.get("error")}
    return row


class RunStamp:
    """Which on-disk rows this run wrote, so its own writes are never read as a rival's.

    `stale_writer` exists because two runs covering the same crate are a genuine conflict and the
    newer result must win. A module sweep writes a row after every module, and those rows are
    *this* run's: comparing them to the run's own start time made every write after the first
    look like a competitor's. On 2026-11-04 that cost `ferrosa-sstable` thirteen measured modules
    -- it kept the first row, discarded the rest -- and the ratchet correctly called the result a
    coverage regression.
    """

    def __init__(self) -> None:
        # Second resolution, like `measured_at` itself: two writes inside one second are one
        # stamp, and both are still ours.
        self.own: set[str] = set()
        self.started: datetime | None = None

    def begin(self) -> datetime:
        """Stamp a run start and return it."""
        self.started = datetime.now(timezone.utc)
        return self.started

    def stamp(self) -> str:
        """Record a timestamp this run is about to write."""
        at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.own.add(at)
        return at

    def is_foreign(self, measured_at: str | None) -> bool:
        """Whether a row on disk was written by another run, after this one started."""
        if not measured_at or measured_at in self.own or self.started is None:
            return False
        try:
            return datetime.fromisoformat(measured_at) > self.started
        except ValueError:
            return False


def publish(crates_on_disk: dict, state: dict, stamps: dict, owned: set[str]) -> dict:
    """Merge this run's rows into what is on disk instead of replacing the file.

    `persist` merges one crate at a time so a concurrent run's crates survive; a whole-file write
    at the end would undo that, and would also re-instate the row of a crate this run discarded
    in favour of a newer measurement. Each crate keeps its own stamp: a rival row can be newer
    than the crate it competes for and older than the run's last crate, so one run-wide stamp
    would not see it.
    """
    rows = crates_on_disk if isinstance(crates_on_disk, dict) else {}
    for name in owned:
        row = (state["crates"] or {}).get(name)
        if row is None:
            continue
        stamp = stamps.get(name)
        if stamp is not None and stamp.is_foreign((rows.get(name) or {}).get("measured_at")):
            continue
        rows[name] = row
    return rows


def measure_modules(crate: str, crates: dict[str, dict], whole: dict,
                    on_row=None, prior: dict | None = None,
                    on_crate_level=None) -> dict:
    """Cover a crate that cannot be extracted whole, one run per module.

    `ferrosa-net` dies inside aeneas above 25 GiB, so no crate-level row is obtainable on this
    host. The substitute is exhaustive rather than representative: every module gets its own run
    and its own stage, and the crate settles only when all of them concluded. A largest-file sample
    will not do -- a clean sample can hide a crashing crate (`ferrosa-jsonb`).

    `on_row` is called after each module with `(module, row)` so a sweep that is interrupted keeps
    what it has measured; this loop runs for tens of minutes per crate. `on_crate_level` is called
    once with the crate-level stage, so the resumable row can say why the crate needed modules
    instead of claiming a constant.
    """
    ident = crate.replace("-", "_")
    mods = modules(crates[crate]["dir"])
    if on_crate_level is not None:
        on_crate_level(whole.get("stage"))
    if not mods:
        return {"scope": "none", "stage": "no_module", "translation_errors": None,
                "lean_errors": None, "sorry": None, "seconds": whole.get("seconds", 0),
                "crate_level": whole["stage"]}
    rows: dict[str, dict] = dict(prior or {})
    for i, mod in enumerate(mods, 1):
        if rows.get(mod, {}).get("stage") in CONCLUDED_STAGES:
            print(f"    module {i}/{len(mods)} {mod}: already {rows[mod]['stage']}, keeping it",
                  flush=True)
            continue
        print(f"    module {i}/{len(mods)} {mod} ...", flush=True)
        row = run_report(crate, f"{ident}::{mod}::*", MODULE_BUDGET,
                         Path(tempfile.gettempdir()) / f"mod-{crate}-{mod}.json",
                         out=GAPS / f"{crate}.{mod}.json")
        row["scope"] = f"module {mod}"
        rows[mod] = row
        print(f"      -> {row['stage']} transl={row.get('translation_errors')} "
              f"lean={row.get('lean_errors')} sorry={row.get('sorry')} {row.get('seconds')}s",
              flush=True)
        if on_row is not None:
            on_row(mod, row)
    return coverage_row(rows, whole["stage"])


def measure(crate: str, crates: dict[str, dict], sample: bool = False,
            on_row=None, prior: dict | None = None, on_crate_level=None) -> dict:
    """Measure the whole crate. A module sample is opt-in, because a sample is not coverage.

    The two units answer different questions: ferrosa-jsonb's `reader` module elaborated with no
    Lean errors while the whole crate dies inside aeneas in the region pass, so a module row cannot
    stand in for a crate row even when it is green.
    """
    ident = crate.replace("-", "_")
    whole = run_report(crate, None, WHOLE_BUDGET, Path(tempfile.gettempdir()) / f"whole-{crate}.json")
    if whole["stage"] == "locked":
        # Nothing was extracted, so a module sample would be queued too -- and reporting one
        # would claim a measurement this run never made.
        whole["scope"] = "crate"
        return whole
    if whole["stage"] in ("oom", "killed"):
        # A crate that cannot be extracted whole still has to be covered whole: fall back to every
        # module, not to one. `killed` is the same fact recorded honestly: the child died of
        # memory, but at the host's limit rather than at a ceiling this script applied.
        return measure_modules(crate, crates, whole, on_row=on_row, prior=prior,
                               on_crate_level=on_crate_level)
    if not sample or whole["stage"] != "timeout":
        whole["scope"] = "crate"
        return whole
    mod = sample_module(crates[crate]["dir"])
    if mod is None:
        return {"scope": "none", "stage": "no_module", "translation_errors": None,
                "lean_errors": None, "sorry": None, "seconds": whole["seconds"]}
    row = run_report(crate, f"{ident}::{mod}::*", MODULE_BUDGET,
                     Path(tempfile.gettempdir()) / f"mod-{crate}.json")
    row["scope"] = f"module {mod}"
    row["crate_level"] = whole["stage"]
    return row


# ---------------------------------------------------------------- rendering


def render(crates: dict[str, dict], order: list[str], state: dict) -> None:
    tc = state.get("toolchain", {})
    rows, lines = [], ["```mermaid", "graph TD"]
    for i, name in enumerate(order, 1):
        for dep in crates[name]["deps"]:
            if dep in crates:
                lines.append(f"  {dep.replace('-', '_')} --> {name.replace('-', '_')}")
    lines.append("```")

    done = 0
    for name in order:
        rec = state["crates"].get(name, {})
        m = rec.get("measurement") or {}
        stage = m.get("stage", "unmeasured")
        ok = stage in ("compiled", "aeneas") and (m.get("lean_errors") == 0)
        done += 1 if rec.get("fingerprint") else 0
        mark = "x" if ok else " "
        scope = m.get("scope", "-")
        rows.append(
            f"| {name} | {mark} | {scope} | {stage} | {m.get('translation_errors', '-')} "
            f"| {m.get('lean_errors', '-')} | {m.get('sorry', '-')} | {m.get('seconds', '-')} "
            f"| {rec.get('measured_at', '-')} |")
        lines.append("")  # placeholder, mermaid already emitted

    checklist = [f"- [{'x' if (state['crates'].get(n, {}).get('measurement') or {}).get('stage') in ('compiled','aeneas') and ((state['crates'].get(n, {}).get('measurement') or {}).get('lean_errors') == 0) else ' '}] "
                 f"{n} — {(state['crates'].get(n, {}).get('measurement') or {}).get('stage', 'unmeasured')}"
                 for n in order]

    md = [f"# Lean extraction inventory — {done}/{len(order)} crates measured", "",
          f"Toolchain: {tc.get('charon','?')} · aeneas {tc.get('aeneas','?')} · {tc.get('lean','?')}",
          f"Budgets: whole crate {state.get('budgets', {}).get('whole_seconds') or 'unbounded'}s, "
          f"module {state.get('budgets', {}).get('module_seconds')}s, "
          f"sample fallback {'on' if state.get('budgets', {}).get('sample') else 'off'}",
          f"Updated: {state.get('updated_at','-')}", "",
          "Generated by `scripts/lean_gap_inventory.py`. A crate is re-measured when its fingerprint",
          "changes -- its own sources, its transitive workspace dependencies' sources, or the",
          "toolchain -- and whenever its last row did not measure the crate end to end. `scope` says",
          "which unit the numbers describe, so a module row is never read as a crate row.", "",
          "## Checklist", ""] + checklist + ["", "## Results", "",
          "| crate | ok | scope | stage | transl | lean | sorry | s | measured |",
          "|---|---|---|---|---|---|---|---|---|"] + rows + ["", "## Dependency order", ""] + lines
    DAG_MD.write_text("\n".join(md) + "\n")
    DAG_JSON.write_text(json.dumps(
        {"order": order, "edges": {n: [d for d in crates[n]["deps"] if d in crates] for n in order},
         "toolchain": tc}, indent=2) + "\n")


# ---------------------------------------------------------------- main


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", nargs="*", default=[])
    ap.add_argument("--only", nargs="*", default=[])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--dag-only", action="store_true")
    ap.add_argument("--rekey", action="store_true",
                    help="keep cached measurements when the recorded toolchain identity only "
                         "became resolvable (an `unknown` field is a resolution failure)")
    ap.add_argument("--sample", action="store_true",
                    help="fall back to one representative module when the whole crate runs out of "
                         "time or memory; off by default, because a sample is not coverage")
    args = ap.parse_args()

    GAPS.mkdir(parents=True, exist_ok=True)
    crates = lib_crates(metadata())
    order = dag(crates)
    tc = toolchain()
    state: Any = {"toolchain": tc,
                  "budgets": {"whole_seconds": WHOLE_BUDGET, "module_seconds": MODULE_BUDGET,
                              "sample": args.sample},
                  "crates": {}}
    recorded_tc: dict = {}
    if STATE.is_file():
        try:
            old = json.loads(STATE.read_text())
            # Every row carries the toolchain in its own fingerprint, so a changed identity makes
            # exactly the affected rows stale. Nothing here drops the cache wholesale: that read a
            # busy host as a new toolchain and re-extracted the suite for a version string.
            recorded_tc = old.get("toolchain") or {}
            state["crates"] = old.get("crates", {}) or {}
            if recorded_tc != tc:
                print(f"toolchain identity: {recorded_tc} -> {tc}")
        except Exception:
            pass

    want = set(args.only) if args.only else set(order)
    todo: list[tuple[str, str]] = []
    adopt: list[tuple[str, str]] = []
    for name in order:
        if name not in want:
            continue
        fp = crate_fingerprint(name, crates, tc)
        prev = state["crates"].get(name, {})
        meas = prev.get("measurement") or {}
        if name in args.force:
            todo.append((name, fp))
        elif settles(prev, fp):
            print(f"skip {name} ({meas.get('stage')}, {prev.get('measured_at', '-')})")
        elif (args.rekey
              and should_adopt(prev, crate_fingerprint(name, crates, recorded_tc), fp,
                               recorded_tc, tc)):
            adopt.append((name, fp))
        else:
            todo.append((name, fp))

    dry = args.dry_run or args.dag_only
    if adopt:
        print(f"{'would adopt' if dry else 'adopted'} {len(adopt)} cached measurement(s) across "
              f"the identity change: {', '.join(n for n, _ in adopt)}")
        if not dry:
            for name, fp in adopt:
                state["crates"][name]["fingerprint"] = fp
            # Persist the adoption before the first extraction: recognising ten cached rows and
            # then losing them to an interrupted run would mean extracting them again on resume.
            state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            STATE.write_text(json.dumps(state, indent=2) + "\n")

    print(f"plan: {len(todo)} to measure, {len(want) - len(todo)} unchanged, {len(order)} crates total")
    if dry:
        for name, _ in todo:
            print(f"  would measure {name} (deps: {', '.join(d for d in crates[name]['deps'] if d in crates) or '-'})")
        # Deliberately no STATE.write_text here: a dry run that stamped the new identity over the
        # recorded one would erase the `unknown` the next run needs to recognise the rows it may
        # keep, turning a preview into the re-extraction it was meant to avoid.
        render(crates, order, state)
        return 0

    owned: set[str] = set()
    stamps: dict[str, RunStamp] = {}
    for i, (name, fp) in enumerate(todo, 1):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ({i}/{len(todo)}) {name} ...", flush=True)
        owned.add(name)
        stamp = RunStamp()
        stamp.begin()
        stamps[name] = stamp

        def stale_writer(_name: str = name, _stamp: RunStamp = stamp) -> bool:
            """True when the file already holds a measurement newer than this run's start.

            Two runs that cover the same crate both hold state.json open and the last writer wins,
            so a slow run that began earlier can replace a result produced later — including one
            produced after a toolchain change. That happened: a `--only ferrosa-cql` run started
            before an aeneas fix, and finishing 20 minutes later it overwrote the post-fix
            measurement (`aeneas`, 0 Lean errors) with its own pre-fix crash. Comparing timestamps
            is enough here because `measured_at` is the completion time of the run that wrote it.

            What it must *not* catch is this run's own progress writes, which is what `_stamp`
            is for: a module sweep writes a row per module, and treating the second one as a
            rival's discarded the whole measurement.
            """
            try:
                on_disk = (json.loads(STATE.read_text()).get("crates") or {}).get(_name) or {}
            except (OSError, ValueError):
                return False
            return _stamp.is_foreign(on_disk.get("measured_at"))

        def persist(_name: str = name, _stamp: RunStamp = stamp) -> None:
            """Write this crate's entry into the file as it is ON DISK, not as this run remembers it.

            `state` is loaded once at startup, so writing it straight back replaces every crate with
            the copy this run began with — and a concurrent run's finished crates vanish. That is
            not hypothetical: a `--only ferrosa-cql` re-measure overlapped a 27-crate sweep, and
            each run's next write would have erased the other's rows. Merging the one crate this
            run owns into the current file makes distinct crates safe; `stale_writer` makes the
            SAME crate safe, since two runs measuring it is a genuine conflict, not a merge.
            """
            try:
                on_disk = json.loads(STATE.read_text())
                if not isinstance(on_disk, dict):
                    raise ValueError("not an object")
            except (OSError, ValueError):
                on_disk = {"crates": dict(state["crates"])}
            crates_on_disk = on_disk.get("crates")
            if not isinstance(crates_on_disk, dict):
                crates_on_disk = {}
                on_disk["crates"] = crates_on_disk
            crates_on_disk[_name] = state["crates"][_name]
            # Stamped, so the next `stale_writer` call knows this row is ours and not a rival's.
            _stamp.own.add(state["crates"][_name]["measured_at"])
            on_disk["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            STATE.write_text(json.dumps(on_disk, indent=2) + "\n")
            state["crates"] = on_disk["crates"]
            state["updated_at"] = on_disk["updated_at"]

        # A module sweep runs for tens of minutes; keep every finished module on disk so an
        # interrupted run resumes instead of starting the crate over.
        prior = ((state["crates"].get(name) or {}).get("measurement") or {}).get("modules")
        partial: dict[str, dict] = dict(prior or {})
        crate_level = {"stage": "unknown"}

        def note_crate_level(stage: str | None, _level: dict = crate_level) -> None:
            _level["stage"] = stage or "unknown"

        def progress(mod: str, mrow: dict, _name: str = name, _fp: str = fp,
                     _partial: dict = partial, _level: dict = crate_level,
                     _stamp: RunStamp = stamp) -> None:
            if stale_writer(_name):
                return
            _partial[mod] = mrow
            row_p = coverage_row(_partial, _level["stage"])
            state["crates"][_name] = {
                "fingerprint": _fp,
                "measured_at": _stamp.stamp(),
                "measurement": row_p,
            }
            persist(_name)

        row = measure(name, crates, args.sample, on_row=progress, prior=prior,
                      on_crate_level=note_crate_level)
        if stale_writer():
            print(f"    -> discarded: state.json already holds a newer measurement of {name}",
                  flush=True)
            continue
        state["crates"][name] = {
            "fingerprint": fp,
            "measured_at": stamp.stamp(),
            "measurement": row,
        }
        print(f"    -> {row.get('scope')} {row['stage']} transl={row.get('translation_errors')} "
              f"lean={row.get('lean_errors')} sorry={row.get('sorry')} {row.get('seconds')}s", flush=True)
        state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        persist(name)
        render(crates, order, state)

    state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # Merge, never replace: `persist` kept every crate it wrote, and a crate this run discarded
    # in favour of a newer measurement must stay discarded.
    try:
        on_disk = json.loads(STATE.read_text())
        if not isinstance(on_disk, dict):
            raise ValueError("not an object")
    except (OSError, ValueError):
        on_disk = {}
    on_disk["crates"] = publish(on_disk.get("crates") or {}, state, stamps, owned)
    for key in ("toolchain", "budgets"):
        if state.get(key) is not None:
            on_disk[key] = state[key]
    on_disk["updated_at"] = state["updated_at"]
    STATE.write_text(json.dumps(on_disk, indent=2) + "\n")
    state["crates"] = on_disk["crates"]
    render(crates, order, state)
    print(f"wrote {STATE}, {DAG_JSON}, {DAG_MD}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
