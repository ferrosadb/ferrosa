#!/usr/bin/env python3
"""Extract one Ferrosa crate to Lean and report what stands between it and elaboration.

Closing gaps by hand does not scale past a couple of crates: the interesting numbers --
translation errors, elaboration errors, and axioms -- are spread across three tools that
each print them differently, and the counts drift whenever charon or aeneas is bumped.
This turns that into one record per crate, written to specs/lean/gaps/<crate>.json, plus a
markdown summary that a scheduler can diff to see gaps appearing or closing.

Usage:  scripts/lean_gap_report.py ferrosa-sstable [--prefix ferrosa_sstable::]
        (run from the repo root; all paths overridable by environment variables)

Environment:
  CHARON      charon binary            (default ~/src/aeneas/charon/bin/charon)
  AENEAS      aeneas binary            (default ~/src/aeneas/src/_build/default/main.exe)
  LEAN_PKG    Aeneas Lean library      (default ~/.local/aeneas/backends/lean)
  CARGO_TARGET_DIR                     (default ~/.local/share/cargo-target)

Exit status is 0 when the crate extracts and elaborates, 1 otherwise: this is meant to run
in CI where the exit code is the signal and the JSON is the diagnosis.
"""
from __future__ import annotations

import argparse
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

REPO = Path(__file__).resolve().parent.parent
CHARON = os.environ.get("CHARON", str(Path.home() / "src/aeneas/charon/bin/charon"))
AENEAS = os.environ.get("AENEAS", str(Path.home() / "src/aeneas/src/_build/default/main.exe"))
LEAN_PKG = os.environ.get("LEAN_PKG", str(Path.home() / ".local/aeneas/backends/lean"))
GAPS = REPO / "specs/lean/gaps"

# "Could not find: type name: X" / "unsupported: ..." -- aeneas prefixes each with [Error]
RE_TRANSLATION = re.compile(r"^\[Error\].*?:\s*(.*)$", re.MULTILINE)
RE_ERRORS_TOTAL = re.compile(r"Generated the partial file \(because of (\d+) errors, including (\d+) unique")
RE_LEAN_ERROR = re.compile(r"^(.*?):(\d+):(\d+): error\(([^)]+)\): (.*)$")


# Charon drives its own nightly miri sysroot, so it must not share a target directory with
# ordinary cargo builds: stale artifacts from another toolchain make rustc fail with
# "found possibly newer version of crate `core`".
CHARON_TARGET = Path(os.environ.get("CHARON_TARGET_DIR",
                                    Path.home() / ".cache/ferrosa-lean-extract/target"))


def run(cmd: list[str], cwd: Path | None = None, timeout: int = 3600,
        env: dict | None = None) -> subprocess.CompletedProcess:
    full_env = {**os.environ, **(env or {})}
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                          timeout=timeout, env=full_env)


_CRATE_DIRS: dict[str, Path] | None = None


def crate_dir_for(crate: str) -> Path | None:
    """Resolve a package to its directory through cargo metadata.

    Guessing `REPO/<crate>` misses members that live in a nested directory, which reports as a
    harness fault ("no crate directory") rather than as anything about the crate.
    """
    global _CRATE_DIRS
    if _CRATE_DIRS is None:
        meta = json.loads(run(["cargo", "metadata", "--no-deps", "--format-version", "1"],
                              cwd=REPO).stdout)
        _CRATE_DIRS = {p["name"]: Path(p["manifest_path"]).parent for p in meta["packages"]}
    found = _CRATE_DIRS.get(crate)
    if found is not None and found.is_dir():
        return found
    for candidate in (REPO / "crates" / crate, REPO / crate):
        if candidate.is_dir():
            return candidate
    return None


def extract(crate: str, prefix: str, work: Path) -> dict:
    """charon -> .llbc -> aeneas -> .lean, inside a scratch copy of the crate's own tree."""
    crate_dir = crate_dir_for(crate)
    if crate_dir is None:
        return {"stage": "locate", "error": f"no crate directory for {crate}"}

    CHARON_TARGET.mkdir(parents=True, exist_ok=True)
    started = time.time()
    charon = run([CHARON, "cargo", "--preset=aeneas", "--start-from", prefix, "--", "--lib"],
                 cwd=crate_dir, env={"CARGO_TARGET_DIR": str(CHARON_TARGET)})

    # charon writes the .llbc at the workspace root, which is not the crate directory when the
    # crate is a workspace member -- look in both, and only accept files this run produced.
    llbc_path = None
    newest = started - 1
    for base in (crate_dir, REPO):
        for cand in base.glob("*.llbc"):
            m = cand.stat().st_mtime
            if m > newest:
                llbc_path, newest = cand, m
    if llbc_path is None:
        return {"stage": "charon", "error": "no .llbc produced",
                "log": ((charon.stdout or "") + (charon.stderr or ""))[-4000:]}

    t_aeneas = time.time() - 1
    aeneas = run([AENEAS, "-backend", "lean", "-dest", str(work), str(llbc_path)], cwd=crate_dir)
    raw = (aeneas.stdout or "") + (aeneas.stderr or "")
    out = "\n".join(l for l in raw.splitlines() if not l.lstrip().startswith(("Applied", "[", "|")))
    out = "\n".join(l for l in out.splitlines() if "prepasses" not in l and "\r" not in l)

    # A crash is not a translation error: aeneas dies with an uncaught exception and writes
    # nothing, usually a tool bug rather than an unsupported construct. Keep the frames.
    if "Fatal error: exception" in raw:
        lines = raw.splitlines()
        idx = next(i for i, l in enumerate(lines) if "Fatal error: exception" in l)
        return {"stage": "aeneas-crash",
                "error": lines[idx].strip(),
                "trace": [l.strip() for l in lines[idx:idx + 8]][:8],
                "llbc": str(llbc_path),
                "log": "\n".join(lines[-40:])}

    # aeneas writes next to the llbc or into -dest depending on the version; accept either,
    # but only a file this run produced.
    lean_files = [f for base in (work, crate_dir, REPO)
                  for f in base.glob("*.lean") if f.stat().st_mtime > t_aeneas]
    if not lean_files:
        return {"stage": "aeneas", "error": "no .lean produced",
                "log": "\n".join(raw.splitlines()[-40:])}
    lean_files.sort(key=lambda f: f.stat().st_mtime)

    totals = RE_ERRORS_TOTAL.search(out)
    translation = [m.strip() for m in RE_TRANSLATION.findall(out)]
    return {
        "stage": "aeneas",
        "llbc": str(llbc_path),
        "lean": str(lean_files[0]),
        "translation_errors": len(translation),
        "translation_unique_reported": int(totals.group(2)) if totals else None,
        "translation_messages": translation[:200],
        "aeneas_exit": aeneas.returncode,
    }


def elaborate(lean_file: Path) -> dict:
    """Compile the generated module against the Aeneas Lean library."""
    res = run(["lake", "env", "lean", str(lean_file.resolve())], cwd=Path(LEAN_PKG),
              timeout=1800)
    out = (res.stdout or "") + (res.stderr or "")
    errors = []
    for line in out.splitlines():
        m = RE_LEAN_ERROR.match(line)
        if m:
            errors.append({"location": f"{Path(m.group(1)).name}:{m.group(2)}:{m.group(3)}",
                           "kind": m.group(4), "message": m.group(5)[:300]})
    warns = out.count("declaration uses `sorry`")
    # [lake] exits non-zero on warnings in some configurations; the errors themselves are the
    # verdict, so do not let the exit status override a clean parse.
    return {"lean_errors": len(errors), "errors": errors[:200], "sorry_declarations": warns,
            "compiled": not errors}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("crate")
    ap.add_argument("--prefix", default=None, help="--start-from pattern; defaults to crate::*")
    args = ap.parse_args()

    crate = args.crate
    prefix = args.prefix or crate.replace("-", "_") + "::*"
    work = Path(tempfile.mkdtemp(prefix=f"leangap-{crate}-"))
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        rec = extract(crate, prefix, work)
        if rec.get("lean"):
            rec.update(elaborate(Path(rec["lean"])))
        elif "error" not in rec:
            rec["error"] = "no Lean file to elaborate"
    finally:
        shutil.rmtree(work, ignore_errors=True)

    rec.update({"crate": crate, "prefix": prefix, "generated_at": started,
                "toolchain": {"charon": CHARON, "aeneas": AENEAS, "lean_pkg": LEAN_PKG}})
    GAPS.mkdir(parents=True, exist_ok=True)
    (GAPS / f"{crate}.json").write_text(json.dumps(rec, indent=2) + "\n")

    ok = rec.get("compiled", False)
    print(f"{crate}: translation_errors={rec.get('translation_errors', '?')} "
          f"lean_errors={rec.get('lean_errors', '?')} sorry={rec.get('sorry_declarations', '?')} "
          f"-> {'OK' if ok else 'GAPS'}  ({GAPS / (crate + '.json')})")
    if rec.get("error"):
        print("  " + str(rec["error"])[:400])
    for e in rec.get("errors", [])[:5]:
        print(f"  {e['location']}: {e['kind']}: {e['message'][:120]}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
