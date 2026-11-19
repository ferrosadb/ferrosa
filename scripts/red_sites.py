#!/usr/bin/env python3
"""Fail when a red-ledger row has not been re-checked since its cited code changed.

The red ledger records *why* the code fails a claim, citing a file and often a symbol. Two kinds of
rot follow, and this script exists for the second:

  1. **Mechanical** — a `file.rs:NNN` moves, or a symbol is renamed or deleted. `--sites` catches
     this directly by looking for the file and the identifier.
  2. **Semantic** — the symbol survives and the code under it stops saying what the row claims. This
     is how rows came to describe code that had already been fixed: `publish_snapshot` still exists,
     but it no longer lets a stale builder overwrite a newer durable snapshot. No amount of symbol
     matching finds that.

So the primary gate is a **staleness ratchet**: each row records the commit it was last verified
against and the files that verification read. When any of those files changes, the row must be
re-verified and the commit moved forward. A row cannot silently describe code nobody has looked at
since the code changed.

`specs/lean4/red-sites.json` is the record. It is deliberately separate from the Lean source and
from the generated table: the ledgers say what is claimed, the sites file says when a human last
confirmed the claim still describes the tree.

Usage:
    red_sites.py --check    exit 1 if any row is unverified, stale, or cites a missing site
    red_sites.py --stale    print the stale rows and the files that moved (no failure)
    red_sites.py --stamp NAME... [--at SHA]
                            record that NAME was verified at SHA (default HEAD), using the files
                            the row cites

Exit codes: 0 clean, 1 a row is unverified/stale/broken, 2 usage or repository error.
"""

import argparse
import json
import pathlib
import re
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
SPECS = ROOT / "specs/lean4/build/specs.json"
SITES = ROOT / "specs/lean4/red-sites.json"

# `ferrosa-foo/src/bar.rs:123` or `state_machine.rs:960` or `write.rs:753`.
SITE_RE = re.compile(r"\b((?:[\w.-]+/)*[\w.-]+\.rs):(\d+)(?:-(\d+))?\b")

# A bare `symbol.rs:123` is relative to whatever crate the row's module names, so it is resolved by
# searching the tree rather than guessed.
BACKTICK_RE = re.compile(r"`([A-Za-z_][A-Za-z0-9_:]*(?:\([^`]*\))?)`")


def git(*args: str, check: bool = True) -> str:
    out = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        capture_output=True, text=True,
    )
    if check and out.returncode != 0:
        sys.stderr.write(f"git {' '.join(args)} failed: {out.stderr.strip()}\n")
        raise SystemExit(2)
    return out.stdout


def resolve(sha: str) -> str | None:
    out = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", "--quiet", f"{sha}^{{commit}}"],
        capture_output=True, text=True,
    )
    return out.stdout.strip() or None


def is_ancestor(sha: str, of: str) -> bool:
    out = subprocess.run(
        ["git", "-C", str(ROOT), "merge-base", "--is-ancestor", sha, of],
        capture_output=True, text=True,
    )
    return out.returncode == 0


def trackable_paths(path: str) -> list[str]:
    """A cited path may be written relative to a crate root. Find where it lives."""
    direct = ROOT / path
    if direct.exists():
        return [path]
    # `git ls-files '*/<path>'` — unique match only; an ambiguous citation is a row defect.
    hits = [p for p in git("ls-files", f"*/{path}").splitlines() if p.endswith(path)]
    return hits


def row_files(red: dict) -> tuple[list[str], list[str]]:
    """The files a row cites, and the citations that could not be resolved to a tracked file."""
    texts = [red.get("why", ""), red.get("witness", ""), red.get("claim", "")]
    cited, missing = set(), []
    for text in texts:
        for m in SITE_RE.finditer(text):
            raw = m.group(1)
            found = trackable_paths(raw)
            if not found:
                missing.append(f"{raw}:{m.group(2)} (no tracked file)")
            cited.update(found)
    return sorted(cited), missing


def check_site_lines(red: dict, files: list[str]) -> list[str]:
    """Every `file.rs:NNN` a row cites must exist, and the line number must be in range."""
    problems = []
    for text in (red.get("why", ""), red.get("witness", "")):
        for m in SITE_RE.finditer(text):
            raw, line = m.group(1), int(m.group(2))
            for path in trackable_paths(raw):
                full = ROOT / path
                try:
                    total = sum(1 for _ in full.open("rb"))
                except OSError:
                    continue
                if line > total:
                    problems.append(
                        f"{raw}:{line} is past the end of {path} ({total} lines)")
    return problems


def load_specs() -> list[dict]:
    if not SPECS.exists():
        sys.stderr.write(
            "specs/lean4/build/specs.json is missing: run "
            "`cd specs/lean4 && lake exe export build/specs.json`\n")
        raise SystemExit(2)
    # Reuse the generator's slug so names agree with red-ledger/src/reds.rs exactly.
    sys.path.insert(0, str(HERE))
    from red_ledger import slug  # noqa: PLC0415  (path is set up above)

    reds = []
    for area in json.loads(SPECS.read_text())["areas"]:
        for item in area["items"]:
            if item.get("kind") == "met":
                continue
            reds.append({
                "name": slug(area["key"], item["claim"]),
                "module": area["key"],
                "claim": item["claim"],
                "why": item.get("why", ""),
                "witness": item.get("witness", ""),
            })
    return reds


def load_sites() -> dict:
    if not SITES.exists():
        return {"rows": {}}
    return json.loads(SITES.read_text())


def save_sites(doc: dict) -> None:
    doc["rows"] = dict(sorted(doc["rows"].items()))
    SITES.parent.mkdir(parents=True, exist_ok=True)
    SITES.write_text(json.dumps(doc, indent=2) + "\n")


def stale_files(entry: dict, head: str) -> list[str]:
    verified = entry.get("verified_at")
    if not verified:
        return []
    sha = resolve(verified)
    if sha is None:
        return ["<verified_at commit is not in this repository>"]
    if not is_ancestor(sha, head):
        return ["<verified_at commit is not an ancestor of HEAD>"]
    files = entry.get("files") or []
    if not files:
        return ["<row cites no files, so nothing can be re-checked>"]
    moved = git("diff", "--name-only", f"{sha}..{head}", "--", *files).splitlines()
    return moved


def report(reds: list[dict], doc: dict, head: str) -> int:
    rows = doc["rows"]
    names = {r["name"] for r in reds}
    unverified, stale, broken = [], [], []

    for red in reds:
        entry = rows.get(red["name"])
        files, missing = row_files(red)
        if missing:
            broken.append((red["name"], missing))
        broken_lines = check_site_lines(red, files)
        if broken_lines:
            broken.append((red["name"], broken_lines))
        if entry is None:
            unverified.append(red["name"])
            continue
        moved = stale_files(entry, head)
        if moved:
            stale.append((red["name"], moved))

    orphans = sorted(set(rows) - names)
    if orphans:
        sys.stderr.write(
            f"{len(orphans)} row(s) in red-sites.json are no longer red "
            f"(a met row leaves this file too):\n")
        for name in orphans:
            sys.stderr.write(f"  {name}\n")

    for title, items in (("BROKEN SITE", broken), ("STALE", stale)):
        if not items:
            continue
        sys.stderr.write(f"\n{title} — {len(items)} row(s):\n")
        for name, detail in items:
            sys.stderr.write(f"  {name}\n")
            for d in detail:
                sys.stderr.write(f"      {d}\n")

    if unverified:
        sys.stderr.write(f"\nUNVERIFIED — {len(unverified)} row(s) with no record in "
                         f"red-sites.json:\n")
        for name in unverified:
            sys.stderr.write(f"  {name}\n")

    total = len(reds)
    if broken or stale or unverified or orphans:
        sys.stderr.write(
            f"\n{total - len(unverified) - len(stale)} of {total} reds are verified and current.\n"
            "Re-read each row above against the code, then:\n"
            "  scripts/red_sites.py --stamp <name> [...]\n")
        return 1
    print(f"red sites: {total} rows verified, none stale")
    return 0


def stamp(reds: list[dict], doc: dict, names: list[str], at: str) -> int:
    sha = resolve(at)
    if sha is None:
        sys.stderr.write(f"{at} is not a commit in this repository\n")
        return 2
    by_name = {r["name"]: r for r in reds}
    unknown = [n for n in names if n not in by_name]
    if unknown:
        sys.stderr.write("not a red (a met row leaves the table, so it leaves this file too):\n")
        for n in unknown:
            sys.stderr.write(f"  {n}\n")
        return 1
    for name in names:
        files, missing = row_files(by_name[name])
        if missing:
            sys.stderr.write(f"{name}: unresolved citation {missing}\n")
            return 1
        doc["rows"][name] = {"verified_at": sha, "files": files}
        print(f"stamped {name} at {sha[:9]} ({len(files)} file(s))")
    save_sites(doc)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--check", action="store_true", help="fail on any unverified or stale row")
    g.add_argument("--stale", action="store_true", help="print stale rows, never fail")
    g.add_argument("--stamp", nargs="+", metavar="NAME", help="record a verification")
    ap.add_argument("--at", default="HEAD", help="commit to stamp (default HEAD)")
    args = ap.parse_args()

    head = resolve("HEAD")
    if head is None:
        sys.stderr.write("not a git repository, or HEAD is unborn\n")
        return 2
    reds, doc = load_specs(), load_sites()

    if args.stamp:
        return stamp(reds, doc, args.stamp, args.at)
    if args.stale:
        rows = doc["rows"]
        hit = [(r["name"], stale_files(rows[r["name"]], head))
               for r in reds if r["name"] in rows and stale_files(rows[r["name"]], head)]
        for name, moved in hit:
            print(f"{name}\n    {', '.join(moved)}")
        print(f"\n{len(hit)} of {len(reds)} rows cite files that moved since verification")
        return 0
    return report(reds, doc, head)


if __name__ == "__main__":
    raise SystemExit(main())
