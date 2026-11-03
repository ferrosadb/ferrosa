#!/usr/bin/env python3
"""Extract one Ferrosa crate to Lean and report what stands between it and elaboration.

Closing gaps by hand does not scale past a couple of crates: the interesting numbers --
translation errors, elaboration errors, and axioms -- are spread across three tools that
each print them differently, and the counts drift whenever charon or aeneas is bumped.
This turns that into one record per crate, written to specs/lean/gaps/<crate>.json, plus a
markdown summary that a scheduler can diff to see gaps appearing or closing.

Usage:  scripts/lean_gap_report.py ferrosa-sstable [--prefix ferrosa_sstable::]
        scripts/lean_gap_report.py ferrosa-sstable --translate-only
        (run from the repo root; all paths overridable by environment variables)

`--translate-only` measures the first milestone only: charon and aeneas run, and the record
says whether aeneas produced a non-empty `.lean` file. It does *not* elaborate, and it says so
with `elaboration: "skipped"`. The two milestones are separate questions -- "every crate
produces Lean" is a coverage claim that costs seconds per small crate, while "that Lean
elaborates" needs the Aeneas library and minutes per crate -- and a record that merged them
would let a translation crash read as "zero Lean errors".

Environment:
  CHARON      charon binary            (default ~/src/aeneas/charon/bin/charon)
  AENEAS      aeneas binary            (default ~/src/aeneas/src/_build/default/main.exe)
  LEAN_PKG    Aeneas Lean library      (default ~/.local/aeneas/backends/lean)
  CARGO_TARGET_DIR                     (default ~/.local/share/cargo-target)
  LEAN_GAP_LOCK                        extraction lock file
                                       (default ~/.cache/ferrosa-lean-extract/lean-gap-report.lock)
  LEAN_GAP_LOCK_TIMEOUT                seconds to wait for that lock; 0 waits forever (default)
  LEAN_GAP_MEM_MAX                     memory ceiling for one extraction (default: 60% of RAM);
                                       0/none/off runs each child unbounded
  LEAN_GAP_MEM_SWAP_MAX                swap ceiling beside it (default 1G)
  LEAN_GAP_KEEP_LEAN                   where `--translate-only` copies the `.lean` it produced
                                       (default ~/.cache/ferrosa-lean-extract/translate);
                                       empty keeps the file in the scratch tree and records
                                       only its size and digest

Two separate ceilings, because they stop two different ways of taking the host down:

* The lock stops concurrent extractions. Charon and Aeneas' Lean backend are the two things that
  exhaust this machine, and three concurrent `aeneas -backend lean` runs took a 30 GB host to a
  global OOM that killed an unrelated CI rustc.
* The memory ceiling stops one extraction from doing it alone. Every child this script starts --
  charon, aeneas, the elaborator -- runs inside its own user cgroup scope with `MemoryMax`, so a
  runaway is killed inside that scope and recorded as `oom`. This process stays outside the scope
  and therefore survives to write the record; a launcher that bounds itself (a systemd unit with
  its own limits) is then a belt on top of braces rather than the only thing standing between the
  scan and the session.

Exit status is 0 when the crate extracts and elaborates, 1 otherwise: this is meant to run
in CI where the exit code is the signal and the JSON is the diagnosis.
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
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

REPO = Path(__file__).resolve().parent.parent
CHARON = os.environ.get("CHARON", str(Path.home() / "src/aeneas/charon/bin/charon"))
AENEAS = os.environ.get("AENEAS", str(Path.home() / "src/aeneas/src/_build/default/main.exe"))
LEAN_PKG = os.environ.get("LEAN_PKG", str(Path.home() / ".local/aeneas/backends/lean"))
GAPS = REPO / "specs/lean/gaps"
# Generated items each crate carries, excluded from extraction (see exclusions()).
EXCLUDES = Path(os.environ.get("LEAN_GAP_EXCLUDES", str(REPO / "specs/lean/excludes.txt")))

# "Could not find: type name: X" / "unsupported: ..." -- aeneas prefixes each with [Error]
RE_TRANSLATION = re.compile(r"^\[Error\].*?:\s*(.*)$", re.MULTILINE)
RE_ERRORS_TOTAL = re.compile(r"Generated the partial file \(because of (\d+) errors, including (\d+) unique")
RE_LEAN_ERROR = re.compile(r"^(.*?):(\d+):(\d+): error\(([^)]+)\): (.*)$")


# Charon drives its own nightly miri sysroot, so it must not share a target directory with
# ordinary cargo builds: stale artifacts from another toolchain make rustc fail with
# "found possibly newer version of crate `core`".
CHARON_TARGET = Path(os.environ.get("CHARON_TARGET_DIR",
                                    Path.home() / ".cache/ferrosa-lean-extract/target"))

_SCOPE_OK: bool | None = None
_WARNED_UNBOUNDED = False


def mem_cap() -> str | None:
    """The memory ceiling for one child process, as a systemd size string, or None.

    A single whole-crate charon run has reached 24.4 GB resident on this suite. Sixty percent of
    RAM leaves the desktop, the local cluster and the MCP server a working set instead of trading
    them for an extraction that was going to time out anyway. The cap is per child rather than on
    this process, so an over-cap charon dies alone and the parent records why.
    """
    raw = os.environ.get("LEAN_GAP_MEM_MAX")
    if raw is not None:
        return None if raw.strip().lower() in ("", "0", "none", "off") else raw.strip()
    try:
        total_kb = next(int(line.split()[1]) for line in
                        Path("/proc/meminfo").read_text().splitlines()
                        if line.startswith("MemTotal:"))
    except (OSError, StopIteration, ValueError):
        return None
    return f"{max(total_kb * 6 // 10 // (1024 * 1024), 1)}G"


def mem_swap_cap() -> str:
    return os.environ.get("LEAN_GAP_MEM_SWAP_MAX", "1G")


def scope_available() -> bool:
    """Whether a child can be put in a memory-limited user scope on this host.

    Probed rather than assumed, once per process: `systemd-run --user` needs a user manager and a
    delegated cgroup tree, which CI containers do not have. Guessing either way loses a measurement
    -- too optimistic loses the run, too pessimistic loses the ceiling.
    """
    global _SCOPE_OK
    if _SCOPE_OK is None:
        cap = mem_cap()
        try:
            _SCOPE_OK = subprocess.run(
                ["systemd-run", "--user", "--scope", "--quiet",
                 "-p", f"MemoryMax={cap or '1G'}", "--", "true"],
                capture_output=True, timeout=30).returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            _SCOPE_OK = False
    return _SCOPE_OK


def killed_by_cap(returncode: int, cap: str | None) -> bool:
    """Whether a child was killed for exceeding the ceiling rather than failing on its own.

    An over-cap scope SIGKILLs its whole tree, so the direct child here is the killed
    `systemd-run` and no error text survives. 137 is the only signal left, and it only means this
    when a ceiling was actually in force.
    """
    return bool(cap) and returncode in (137, -9)


def bounded(cmd: list[str]) -> list[str]:
    """Wrap a child in a memory-limited scope when one can be created."""
    cap = mem_cap()
    if not cap:
        return cmd
    if not scope_available():
        global _WARNED_UNBOUNDED
        if not _WARNED_UNBOUNDED:
            print(f"lean_gap_report: no user cgroup scope available, running children unbounded "
                  f"(the {cap} ceiling is not in force; set LEAN_GAP_MEM_MAX=0 to silence)",
                  file=sys.stderr, flush=True)
            _WARNED_UNBOUNDED = True
        return cmd
    return ["systemd-run", "--user", "--scope", "--quiet",
            "-p", f"MemoryMax={cap}",
            "-p", f"MemorySwapMax={mem_swap_cap()}",
            "--", *cmd]


def lock_path() -> Path:
    return Path(os.environ.get("LEAN_GAP_LOCK",
                               Path.home() / ".cache/ferrosa-lean-extract/lean-gap-report.lock"))


@contextlib.contextmanager
def single_extraction():
    """Hold the machine-wide extraction lock for one crate.

    Charon and Aeneas' Lean backend are the two things that exhaust this host: charon reached
    21.5 GB and 24.4 GB of resident memory on ferrosa-common, and on 2026-10-30 three concurrent
    `aeneas main.exe -backend lean` runs took a 30 GB machine to a global OOM that killed an
    unrelated CI rustc. So one crate extracts at a time.

    The lock is machine-wide rather than per checkout or per worktree, because the contended
    resource is host memory, not a directory. It is flock, so the kernel drops it when the
    holder dies -- a killed charon leaves no stale lock to clear by hand.
    """
    path = lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    timeout = float(os.environ.get("LEAN_GAP_LOCK_TIMEOUT") or 0)
    with open(path, "a+", encoding="utf-8") as fh:
        started = time.monotonic()
        announced = False
        while True:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                waited = time.monotonic() - started
                # A timeout here loses a measurement, so it must fail loudly rather than
                # record the crate as unmeasured-but-fine.
                if timeout and waited >= timeout:
                    raise SystemExit(
                        # "gave up waiting for the extraction lock" is the sentinel the batch
                        # drivers (lean_gap_sweep.sh, lean_gap_sample.sh, lean_gap_inventory.py)
                        # match to report `locked` instead of a phantom measurement.
                        f"lean_gap_report: gave up waiting for the extraction lock after "
                        f"{waited:.0f}s at {path} (held by pid {read_holder(fh) or '?'}); "
                        f"LEAN_GAP_LOCK_TIMEOUT={timeout:g}")
                if not announced:
                    print(f"lean_gap_report: waiting for the extraction lock at {path} "
                          f"(pid {read_holder(fh) or '?'})", file=sys.stderr, flush=True)
                    announced = True
                time.sleep(0.5)
        try:
            fh.seek(0)
            fh.truncate()
            fh.write(f"{os.getpid()}\n")
            fh.flush()
            if announced:
                print(f"lean_gap_report: acquired the extraction lock after "
                      f"{time.monotonic() - started:.0f}s", file=sys.stderr, flush=True)
            yield
        finally:
            fh.seek(0)
            fh.truncate()
            fh.flush()
            fcntl.flock(fh, fcntl.LOCK_UN)


def read_holder(fh) -> str:
    """The pid the current holder wrote, for the waiting message; empty if unreadable."""
    try:
        fh.seek(0)
        return fh.read().strip()
    except OSError:
        return ""


def run(cmd: list[str], cwd: Path | None = None, timeout: int = 3600,
        env: dict | None = None) -> subprocess.CompletedProcess:
    full_env = {**os.environ, **(env or {})}
    return subprocess.run(bounded(cmd), cwd=cwd, capture_output=True, text=True,
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


def exclusions(crate: str, path: Path | None = None) -> list[str]:
    """Generated items this crate carries, as charon name patterns.

    aeneas on `ferrosa_net.llbc` emitted 13,295 internal errors in five minutes, 11,147 of them
    from `envelope_capnp` -- 31k lines that a build script writes into OUT_DIR and the crate
    `include!`s. Schema output is not product code, and translating it is what pushed the run past
    22 GiB. The list lives beside the records rather than on one invocation's command line,
    because it is a fact about the crate's build that the next sweep must also know.
    """
    path = path or EXCLUDES
    try:
        text = path.read_text()
    except FileNotFoundError:
        return []
    found = []
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != 2:
            # Fail loud: a typo'd exclusion silently translates the generated code again, which is
            # exactly the 22 GiB run this file exists to prevent.
            raise SystemExit(f"{path}:{lineno}: expected '<crate> <charon name pattern>', "
                             f"got {raw.strip()!r}")
        owner, pattern = parts
        if owner == crate:
            found.append(pattern)
    return found


def charon_cmd(crate: str, prefix: str, exclude: list[str]) -> list[str]:
    cmd = [CHARON, "cargo", "--preset=aeneas", "--start-from", prefix]
    for pattern in exclude:
        cmd += ["--exclude", pattern]
    return cmd + ["--", "--lib"]


def extract(crate: str, prefix: str, work: Path) -> dict:
    """charon -> .llbc -> aeneas -> .lean, inside a scratch copy of the crate's own tree."""
    crate_dir = crate_dir_for(crate)
    if crate_dir is None:
        return {"stage": "locate", "error": f"no crate directory for {crate}"}

    CHARON_TARGET.mkdir(parents=True, exist_ok=True)
    started = time.time()
    charon = run(charon_cmd(crate, prefix, exclusions(crate)),
                 cwd=crate_dir, env={"CARGO_TARGET_DIR": str(CHARON_TARGET)})
    if killed_by_cap(charon.returncode, mem_cap()):
        # The scope killed charon for exceeding the ceiling. That is not a translator error and not
        # a timeout: it is this crate being too large for one whole-crate run, which is exactly the
        # case the module sample exists for.
        return {"stage": "oom", "memory_max": mem_cap(),
                "error": f"charon killed at the {mem_cap()} memory ceiling"}

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
    if killed_by_cap(aeneas.returncode, mem_cap()):
        return {"stage": "oom", "memory_max": mem_cap(), "llbc": str(llbc_path),
                "error": f"aeneas killed at the {mem_cap()} memory ceiling"}
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


def keep_lean_dir() -> Path | None:
    """Where a translate-only run parks the `.lean` it produced, or None to keep none.

    The extraction itself lives in a `mkdtemp` that is removed on the way out, so the path in a
    check-mode record has stopped existing by the time anyone reads it. A translate-only run
    copies the file to a stable path first, which lets the gate check the artifact the record
    claims -- the point of milestone 1 is the file, not a count beside it.
    """
    raw = os.environ.get("LEAN_GAP_KEEP_LEAN",
                         str(Path.home() / ".cache/ferrosa-lean-extract/translate"))
    return Path(raw) if raw.strip() else None


def translation_record(rec: dict, crate: str, keep: Path | None = None) -> dict:
    """The record a translate-only run produces: did aeneas write Lean, or not?

    Elaboration is the second milestone, so it is not attempted here, and the record says so
    with `elaboration: "skipped"`. It deliberately carries no `lean_errors`, no `compiled` and
    no `sorry_declarations`: a zero in any of those would be a claim this run never made, and
    every reader of the record shape -- `lean_gap_ratchet.py` included -- treats a zero as a
    measurement. `translated` is the only verdict, and it is true only when a non-empty file
    exists.

    A crash stays `aeneas-crash` with its exception line, because a crate that dies inside the
    translator and a crate whose Lean merely fails to elaborate are different failures and the
    whole point of this mode is to stop reporting them as one.
    """
    rec = dict(rec)
    rec["elaboration"] = "skipped"
    for key in ("lean_errors", "errors", "compiled", "sorry_declarations"):
        rec.pop(key, None)

    stage = rec.get("stage")
    if stage == "aeneas":
        src = Path(rec["lean"]) if rec.get("lean") else None
        size = src.stat().st_size if src is not None and src.is_file() else 0
        if src is None or size <= 0:
            # aeneas exited without a crash and wrote nothing usable. That is not a crash and
            # not a translation either, so it gets its own stage rather than borrowing one.
            rec.update({"stage": "no-lean", "translated": False,
                        "error": "aeneas produced no non-empty .lean file"})
            return rec
        data = src.read_bytes()
        rec.update({"stage": "aeneas-ok", "translated": True,
                    "lean_bytes": size, "lean_sha256": hashlib.sha256(data).hexdigest()})
        if keep is not None:
            keep.mkdir(parents=True, exist_ok=True)
            dest = keep / f"{crate}.lean"
            dest.write_bytes(data)
            rec["lean"] = str(dest)
        return rec

    if stage == "aeneas-crash":
        rec["translated"] = False
        return rec

    # `charon`, `oom`, `locate`: nothing reached aeneas, so there is no file to talk about.
    rec["translated"] = False
    rec.setdefault("error", f"no Lean produced (stage {stage})")
    return rec


def elaborate(lean_file: Path) -> dict:
    """Compile the generated module against the Aeneas Lean library."""
    res = run(["lake", "env", "lean", str(lean_file.resolve())], cwd=Path(LEAN_PKG),
              timeout=1800)
    if killed_by_cap(res.returncode, mem_cap()):
        return {"stage": "oom", "memory_max": mem_cap(), "lean_errors": None,
                "sorry_declarations": None, "compiled": False,
                "error": f"elaboration killed at the {mem_cap()} memory ceiling"}
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
    ap.add_argument("--out", default=None,
                    help="record path (default specs/lean/gaps/<crate>.json); a per-module run "
                         "passes its own path so sibling modules do not overwrite each other")
    ap.add_argument("--translate-only", action="store_true",
                    help="run charon and aeneas and stop: record whether a non-empty .lean was "
                         "produced, and record `elaboration: skipped` instead of elaborating")
    args = ap.parse_args()

    crate = args.crate
    prefix = args.prefix or crate.replace("-", "_") + "::*"
    work = Path(tempfile.mkdtemp(prefix=f"leangap-{crate}-"))
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        # Serializing here is the whole point: extract() runs charon and then aeneas, and both
        # scale with crate size, not with how many sessions decided to measure one at once.
        with single_extraction():
            rec = extract(crate, prefix, work)
            if args.translate_only:
                rec = translation_record(rec, crate, keep_lean_dir())
            elif rec.get("lean"):
                rec.update(elaborate(Path(rec["lean"])))
            elif "error" not in rec:
                rec["error"] = "no Lean file to elaborate"
    finally:
        shutil.rmtree(work, ignore_errors=True)

    rec.update({"crate": crate, "prefix": prefix, "generated_at": started,
                "memory_max": mem_cap(),
                "toolchain": {"charon": CHARON, "aeneas": AENEAS, "lean_pkg": LEAN_PKG}})
    GAPS.mkdir(parents=True, exist_ok=True)
    # A translate-only record never lands in the elaboration directory by default: those records
    # are what `lean_gap_ratchet.py` diffs, and a translate-only row there -- no `lean_errors`,
    # `elaboration: skipped` -- would either read as a coverage regression or, worse, replace the
    # error list a growth report names.
    default_dir = (GAPS / "translate") if args.translate_only else GAPS
    out = Path(args.out) if args.out else default_dir / f"{crate}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rec, indent=2) + "\n")

    if args.translate_only:
        # `translated` is the verdict and `lean_errors` is absent, so this line cannot be
        # mistaken for the elaboration summary below it.
        ok = rec.get("translated") is True
        print(f"{crate}: stage={rec.get('stage')} translated={'yes' if ok else 'no'} "
              f"elaboration=skipped"
              + (f" lean={rec.get('lean_bytes')}B" if ok else "")
              + f" -> {'TRANSLATED' if ok else 'NO LEAN'}  ({out})")
        if rec.get("error"):
            print("  " + str(rec["error"])[:400])
        return 0 if ok else 1

    ok = rec.get("compiled", False)
    print(f"{crate}: translation_errors={rec.get('translation_errors', '?')} "
          f"lean_errors={rec.get('lean_errors', '?')} sorry={rec.get('sorry_declarations', '?')} "
          f"-> {'OK' if ok else 'GAPS'}  ({out})")
    if rec.get("error"):
        print("  " + str(rec["error"])[:400])
    for e in rec.get("errors", [])[:5]:
        print(f"  {e['location']}: {e['kind']}: {e['message'][:120]}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
