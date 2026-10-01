#!/usr/bin/env python3
"""guard-no-python-data-writes.py: fail if a script can write into a data dir.

Policy: Python scripts must not recover or mutate production data. They are
reference and test material. The supported path for touching a node's data
directory is the product (`ferrosa-ctl`), which is tested, dry-run by default
and refuses a live node. See scripts/README.md.

Why: on 2026-09-29 `scripts/recover-evicted-sstables.py` wrote eviction
markers straight into live node data directories. Both it and the evictor
wrote an empty `<gen>.evicted` file, so afterwards nobody could tell a real
eviction from the script's own marking.

What it flags: a tracked `scripts/**/*.py` that (a) declares a data-directory
style CLI argument (`--data-dir`, `--data-root`, `--sstables-dir`, ...) AND
(b) contains a mutating filesystem call: `open()` for write/append/create,
`os.open` with a write flag, `os.remove/rename/...`, `shutil.rmtree/move/...`,
`Path.write_text/unlink/mkdir/touch/...`, or `subprocess` running rm/mv/cp/dd.
Each such call is reported.

Deliberate exceptions carry a marker comment, as in guard-no-memory-public.sh:

  * on a mutating line:  `# data-dir-write-ok: <reason>` waives that line;
  * on the data-dir argument line: waives every mutation in that file (for a
    harness that owns a scratch directory it creates itself).

The check is a heuristic. It does not follow paths through variables, so it
can miss a write whose target is computed far from the call; it keys on the
presence of a data-dir argument to stay quiet about scripts that never take
one. A reviewer still has to read a new script that takes a data dir.

    scripts/guard-no-python-data-writes.py          # this repo
    scripts/guard-no-python-data-writes.py <dir>    # another checkout

Exit 0 clean, 1 on a violation, 2 if it cannot scan.
"""

import re
import subprocess
import sys
from pathlib import Path

MARKER = "data-dir-write-ok"

# add_argument("--data-dir"), ("--data-root"), ("--sstables-dir"), ...
DATA_DIR_ARG = re.compile(
    r"""add_argument\(\s*["']--(?:data|sstables?|storage|node)[-_](?:dir|root|path|directory)["']"""
)

_OPEN_WRITE_MODE = r"""["'][^"']*[wax+][^"']*["']"""
MUTATING = [
    # open(path, "w") / open(path, mode="ab") / Path.open("w")
    re.compile(r"\bopen\([^)]*(?:,\s*|mode\s*=\s*)" + _OPEN_WRITE_MODE),
    re.compile(r"\bos\.open\([^)]*\bO_(?:WRONLY|RDWR|CREAT|APPEND|TRUNC)\b"),
    re.compile(
        r"\bos\.(?:remove|unlink|rename|replace|rmdir|removedirs|makedirs|mkdir"
        r"|truncate|symlink|link|chmod|chown)\("
    ),
    re.compile(r"\bshutil\.(?:rmtree|move|copy|copy2|copyfile|copytree)\("),
    re.compile(r"\.(?:write_text|write_bytes|unlink|rmdir|mkdir|touch|truncate)\("),
    re.compile(
        r"""\bsubprocess\.\w+\(\s*\[?\s*["'](?:rm|mv|cp|dd|truncate|touch|tee|rsync)["']"""
    ),
]


def tracked_scripts(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "--", "scripts/*.py"],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip() or "git ls-files failed")
    return [p for p in out.stdout.splitlines() if p]


def violations_in(text: str) -> list[tuple[int, str]]:
    """(line number, line) for each unwaived mutating call in a data-dir script."""
    lines = text.splitlines()
    arg_lines = [l for l in lines if DATA_DIR_ARG.search(l)]
    if not arg_lines or any(MARKER in l for l in arg_lines):
        return []
    hits = []
    for n, line in enumerate(lines, 1):
        code = line.strip()
        if code.startswith("#") or MARKER in line:
            continue
        if any(p.search(line) for p in MUTATING):
            hits.append((n, code))
    return hits


def main(argv: list[str]) -> int:
    root = Path(argv[1] if len(argv) > 1 else ".")
    try:
        files = tracked_scripts(root)
    except (RuntimeError, OSError) as e:
        print(f"guard-no-python-data-writes: cannot scan {root}: {e}", file=sys.stderr)
        return 2
    bad = []
    for rel in files:
        try:
            text = (root / rel).read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            print(f"guard-no-python-data-writes: cannot read {rel}: {e}", file=sys.stderr)
            return 2
        bad += [(rel, n, line) for n, line in violations_in(text)]
    if not bad:
        print(f"guard-no-python-data-writes: clean ({len(files)} script(s) checked)")
        return 0
    print(
        "guard-no-python-data-writes: a script takes a data directory and mutates files. "
        "Python must not recover or mutate production data; put the operation in "
        "ferrosa-ctl (see scripts/README.md).",
        file=sys.stderr,
    )
    print(f"Offending lines (a deliberate one carries '{MARKER}: <reason>'):", file=sys.stderr)
    for rel, n, line in bad:
        print(f"  {rel}:{n}: {line}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
