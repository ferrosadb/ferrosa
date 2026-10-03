#!/usr/bin/env python3
"""Fail CI when a change adds a waiver without Ben's approval.

Rule (Ben, 2026-10-03): no waivers. A waiver leaves a landmine for future runs;
the finding gets fixed instead. A waiver is either of:

  - an `#[ignore]` on a test (`#[ignore]` or `#[ignore = "..."]`);
  - a date-based exemption with a date after today: `expires = "YYYY-MM-DD"`,
    "until YYYY-MM-DD", "deadline", "review by", "sunset". This covers
    p0-oom-audit allowlist entries and their renewals. A past date is history,
    not a waiver.

Every line ADDED between the merge base with BASE_REF and the working tree is
checked, uncommitted changes included. Removing a waiver is always fine. A
waiver passes only if a commit in the range carries the trailer
`Waiver-Approved-By: Ben Kearns`.

Usage: check-no-new-waivers.py [BASE_REF]   (default: origin/main)
"""
import datetime
import re
import subprocess
import sys

# An #[ignore] attribute in attribute position (line start, optionally after other
# attributes like #[test]), or the conditional form #[cfg_attr(..., ignore)].
# Prose that merely mentions "#[ignore]" mid-sentence is not a waiver.
IGNORE = re.compile(
    r"^\s*(#\[[^\]]*\]\s*)*#\[\s*ignore\b|#\[\s*cfg_attr\s*\(.*\bignore\b"
)
DATED = re.compile(
    r"\b(expires|until|deadline|review[-_ ]by|sunset)\b\W{0,4}(\d{4}-\d{2}-\d{2})",
    re.IGNORECASE,
)
TRAILER = re.compile(r"^Waiver-Approved-By:\s*Ben Kearns\s*$", re.MULTILINE)
# This check and its tests necessarily spell out the patterns they look for.
SELF = {"scripts/check-no-new-waivers.py", "tests/ci/test_check_no_new_waivers.py"}


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def added_waivers(base: str):
    """Yield (path, line) for every added line that is a waiver."""
    path = None
    for line in git("diff", "--no-color", "--unified=0", base, "--").splitlines():
        if line.startswith("+++ "):
            path = line[6:] if line.startswith("+++ b/") else None
        elif line.startswith("+") and path and path not in SELF:
            text = line[1:]
            if IGNORE.search(text) or is_future_exemption(text):
                yield path, text.strip()


def is_future_exemption(text: str) -> bool:
    """A date-based waiver exempts something until a date still to come. A
    date today or earlier ("these were asserts until 2026-10-03", written that
    day) is history, not a waiver."""
    today = datetime.date.today().isoformat()
    return any(m.group(2) > today for m in DATED.finditer(text))


def main() -> int:
    base_ref = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
    base = git("merge-base", "HEAD", base_ref).strip()
    found = list(added_waivers(base))
    if not found:
        print(f"waiver check: no #[ignore] or date-based waivers added since {base[:10]}")
        return 0
    if TRAILER.search(git("log", "--format=%B", f"{base}..HEAD")):
        print(f"waiver check: {len(found)} waiver line(s) added, approved by Ben's trailer")
        return 0
    print("waiver check: FAIL. Waivers added without approval:")
    for path, text in found:
        print(f"  {path}: {text}")
    print("Fix the finding instead. Only Ben can approve a waiver, recorded as a")
    print("'Waiver-Approved-By: Ben Kearns' trailer on a commit in this change.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
