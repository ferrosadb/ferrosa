#!/usr/bin/env python3
"""Inventory the Lean gap for every lib crate, in dependency order, incrementally.

Why this exists rather than a shell loop: the suite is 27 crates with a real dependency DAG, and
extraction of a crate depends on its workspace dependencies' *source* (charon inlines reachable
code), so a change in a dependency invalidates the dependents. Measuring in DAG order and keying
each row on a fingerprint of (crate sources + transitive workspace dependency sources + toolchain)
means a re-run only touches what actually changed.

Per crate it tries the whole crate first, then falls back to one representative module, because
whole-crate extraction does not finish for the large crates. Both numbers are recorded, so a
`timeout` at crate level with a module number beside it is read for what it is.

Usage:
  lean_gap_inventory.py                 # measure everything whose fingerprint changed
  lean_gap_inventory.py --force ferrosa-cdc
  lean_gap_inventory.py --only ferrosa-cdc ferrosa-common   # subset, still DAG-ordered
  lean_gap_inventory.py --dry-run       # print the plan, measure nothing
  lean_gap_inventory.py --dag-only      # regenerate dag.json/dag.md/CHECKLIST.md

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
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(os.environ.get("REPO", Path.home() / "src/ferrosa-suite/ferrosa"))
GAPS = REPO / "specs/lean/gaps"
STATE = GAPS / "state.json"
DAG_JSON = GAPS / "dag.json"
DAG_MD = GAPS / "dag.md"
CHARON = os.environ.get("CHARON", str(Path.home() / "src/aeneas/charon/bin/charon"))
AENEAS = os.environ.get("AENEAS", str(Path.home() / "src/aeneas/src/_build/default/main.exe"))
LEAN_PKG = os.environ.get("LEAN_PKG", str(Path.home() / ".local/aeneas/backends/lean"))
WHOLE_BUDGET = int(os.environ.get("WHOLE_BUDGET", "240"))
MODULE_BUDGET = int(os.environ.get("MODULE_BUDGET", "150"))


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


def toolchain() -> dict:
    def first_line(cmd: list[str]) -> str:
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            return (r.stdout or r.stderr).strip().splitlines()[0]
        except Exception:
            return "unknown"

    pin = Path.home() / "src/aeneas/charon-pin"
    if pin.is_file():
        # The pin file carries a comment line above the commit it names.
        charon_id = next((ln.strip() for ln in reversed(pin.read_text().splitlines())
                          if ln.strip() and not ln.lstrip().startswith("#")), "unknown")
    else:
        charon_id = first_line([CHARON, "--version"])
    return {
        "charon": charon_id,
        "aeneas": first_line(["git", "-C", str(Path.home() / "src/aeneas"), "rev-parse", "--short", "HEAD"]),
        "lean": first_line(["lake", "env", "lean", "--version"]),
    }


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


def sample_module(crate_dir: Path) -> str | None:
    src = crate_dir / "src"
    best, size = None, -1
    if not src.is_dir():
        return None
    for f in src.rglob("*.rs"):
        if f.name in ("lib.rs", "main.rs"):
            continue
        parts = [p for p in f.relative_to(src).with_suffix("").parts if p != "mod"]
        if parts and f.stat().st_size > size:
            best, size = "::".join(parts), f.stat().st_size
    return best


# ---------------------------------------------------------------- measurement


def run_report(crate: str, prefix: str | None, budget: int, work_log: Path) -> dict:
    cmd = [sys.executable, str(REPO / "scripts/lean_gap_report.py"), crate]
    if prefix:
        cmd += ["--prefix", prefix]
    started = time.time()
    try:
        subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=budget)
        stage = None
    except subprocess.TimeoutExpired:
        stage = "timeout"
    secs = round(time.time() - started)
    data = {}
    try:
        data = json.loads((GAPS / f"{crate}.json").read_text())
    except Exception:
        pass
    row = {
        "stage": stage or data.get("stage", "unknown"),
        "translation_errors": data.get("translation_errors"),
        "lean_errors": data.get("lean_errors"),
        "sorry": data.get("sorry_declarations"),
        "seconds": secs,
    }
    work_log.write_text(json.dumps(row))
    return row


def measure(crate: str, crates: dict[str, dict]) -> dict:
    ident = crate.replace("-", "_")
    whole = run_report(crate, None, WHOLE_BUDGET, Path(tempfile.gettempdir()) / f"whole-{crate}.json")
    if whole["stage"] != "timeout":
        whole["scope"] = "crate"
        return whole
    mod = sample_module(crates[crate]["dir"])
    if mod is None:
        return {"scope": "none", "stage": "no_module", "translation_errors": None,
                "lean_errors": None, "sorry": None, "seconds": whole["seconds"]}
    row = run_report(crate, f"{ident}::{mod}::*", MODULE_BUDGET,
                     Path(tempfile.gettempdir()) / f"mod-{crate}.json")
    row["scope"] = f"module {mod}"
    row["crate_level"] = "timeout"
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
          f"Updated: {state.get('updated_at','-')}", "",
          "Generated by `scripts/lean_gap_inventory.py`. A crate is re-measured only when its",
          "fingerprint changes: its own sources, its transitive workspace dependencies' sources, or",
          "the toolchain. Whole-crate runs get a budget; crates that exceed it fall back to their",
          "largest module, which is why `scope` says which unit the numbers describe.", "",
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
    args = ap.parse_args()

    GAPS.mkdir(parents=True, exist_ok=True)
    crates = lib_crates(metadata())
    order = dag(crates)
    tc = toolchain()
    state: Any = {"toolchain": tc, "crates": {}}
    if STATE.is_file():
        try:
            old = json.loads(STATE.read_text())
            if old.get("toolchain") == tc:
                state["crates"] = old.get("crates", {}) or {}
            else:
                print(f"toolchain changed ({old.get('toolchain')} -> {tc}); re-measuring everything")
        except Exception:
            pass

    want = set(args.only) if args.only else set(order)
    todo = []
    for name in order:
        if name not in want:
            continue
        fp = crate_fingerprint(name, crates, tc)
        prev = state["crates"].get(name, {})
        if name in args.force or prev.get("fingerprint") != fp:
            todo.append((name, fp))
        else:
            print(f"skip {name} (unchanged, {prev.get('measured_at','-')})")

    print(f"plan: {len(todo)} to measure, {len(want) - len(todo)} unchanged, {len(order)} crates total")
    if args.dry_run or args.dag_only:
        for name, _ in todo:
            print(f"  would measure {name} (deps: {', '.join(d for d in crates[name]['deps'] if d in crates) or '-'})")
        state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        STATE.write_text(json.dumps(state, indent=2) + "\n")
        render(crates, order, state)
        return 0

    for i, (name, fp) in enumerate(todo, 1):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ({i}/{len(todo)}) {name} ...", flush=True)
        row = measure(name, crates)
        state["crates"][name] = {
            "fingerprint": fp,
            "measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "measurement": row,
        }
        print(f"    -> {row.get('scope')} {row['stage']} transl={row.get('translation_errors')} "
              f"lean={row.get('lean_errors')} sorry={row.get('sorry')} {row.get('seconds')}s", flush=True)
        state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        STATE.write_text(json.dumps(state, indent=2) + "\n")
        render(crates, order, state)

    state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    STATE.write_text(json.dumps(state, indent=2) + "\n")
    render(crates, order, state)
    print(f"wrote {STATE}, {DAG_JSON}, {DAG_MD}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
