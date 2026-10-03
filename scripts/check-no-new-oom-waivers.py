#!/usr/bin/env python3
"""Fail if the p0-oom-audit waiver file gained or extended a waiver without Ben's approval.

Rule (2026-10-03): no new waivers. A `p0-oom-audit` finding is fixed in code;
an allowlist entry leaves a landmine for future runs. This check compares
`specs/p0-oom-guard/oom-audit-allow.toml` on HEAD with the merge base against
the target branch, and fails when:

  - an [[allow]] entry exists on HEAD that the base did not have
    (keyed by path + rule), or
  - an existing entry's `expires` date moved later,

unless a commit in the range that touches the file carries the trailer
`Waiver-Approved-By: Ben Kearns`. Removing entries, or shortening an expiry, is
always fine.

Usage: check-no-new-oom-waivers.py [BASE_REF]   (default: origin/main)
"""
import re
import subprocess
import sys

WAIVER_FILE = "specs/p0-oom-guard/oom-audit-allow.toml"
TRAILER = re.compile(r"^Waiver-Approved-By:\s*Ben Kearns\s*$", re.MULTILINE)


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def entries(text: str) -> dict:
    """Map (path, rule) -> expires for every [[allow]] block."""
    out = {}
    for block in text.split("[[allow]]")[1:]:
        fields = dict(re.findall(r'^\s*(\w+)\s*=\s*"([^"]*)"', block, re.MULTILINE))
        key = (fields.get("path", ""), fields.get("rule", ""))
        out[key] = fields.get("expires", "")
    return out


def main() -> int:
    base_ref = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
    base = git("merge-base", "HEAD", base_ref).strip()
    try:
        before = entries(git("show", f"{base}:{WAIVER_FILE}"))
    except subprocess.CalledProcessError:
        before = {}
    with open(WAIVER_FILE, encoding="utf-8") as f:
        after = entries(f.read())

    added = sorted(k for k in after if k not in before)
    extended = sorted(k for k in after if k in before and after[k] > before[k])
    if not added and not extended:
        print(f"oom-waiver check: no new or extended waivers since {base[:10]}")
        return 0

    log = git("log", "--format=%B%x00", f"{base}..HEAD", "--", WAIVER_FILE)
    if TRAILER.search(log):
        print("oom-waiver check: new/extended waivers carry Ben's approval trailer")
        return 0

    print("oom-waiver check: FAIL. Waivers were added or extended without approval.")
    for path, rule in added:
        print(f"  added:    {path} [{rule}]")
    for path, rule in extended:
        print(f"  extended: {path} [{rule}] {before[(path, rule)]} -> {after[(path, rule)]}")
    print("Fix the finding in code instead. Only Ben can approve a waiver, recorded as a")
    print("'Waiver-Approved-By: Ben Kearns' trailer on the commit that adds it.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
