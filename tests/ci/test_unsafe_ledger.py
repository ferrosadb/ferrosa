"""Contract test: docs/unsafe-ledger.md must describe the unsafe we actually have.

`unsafe` is the part of the codebase no proof covers — the Lean corpus models a specified machine,
Aeneas refuses to translate `unsafe`, and Miri only sees executed paths. The defence is that the set
is small, enumerated, and reviewed by hand, which only works if the enumeration cannot silently drift.

This test is the ratchet for the *audit queue*: it fails when a file's production count differs from
its ledger row, when a file with `unsafe` has no row at all, or when a count goes down without the
ledger being updated — stale documentation in the other direction is still stale.

`docs/unsafe-policy.md` and `scripts/ci-gates/guard-unsafe.py` are the admission rule: may a site
exist at all, and under what kind. This file deliberately carries no second scanner — one scanner,
one truth. It imports the gate's, so the ledger and the allowlist cannot disagree about what a
production site is.
"""

from __future__ import annotations

import importlib.util
import re
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LEDGER = REPO / "docs" / "unsafe-ledger.md"
GUARD = REPO / "scripts" / "ci-gates" / "guard-unsafe.py"


def _load_guard():
    """Import the hyphenated guard script.

    `sys.modules` must be populated before `exec_module`: the guard declares a dataclass, and
    dataclasses resolve string annotations through `sys.modules`.
    """
    spec = importlib.util.spec_from_file_location("guard_unsafe_ledger", GUARD)
    assert spec is not None and spec.loader is not None, f"cannot load {GUARD}"
    module = importlib.util.module_from_spec(spec)
    sys.modules["guard_unsafe_ledger"] = module
    spec.loader.exec_module(module)
    return module


guard = _load_guard()

# The scanner is the expensive part (59 sites over ~1200 files); setUp runs per test method.
_CACHE: dict[str, dict[str, int]] = {}


def production_counts() -> dict[str, int]:
    """Sites the gate counts as production, per file."""
    if "production" not in _CACHE:
        counts: dict[str, int] = {}
        for site in guard.collect(REPO):
            counts[site.file] = counts.get(site.file, 0) + 1
        _CACHE["production"] = counts
    return _CACHE["production"]


def all_counts() -> dict[str, int]:
    """Every `unsafe` construct in `*/src/**`, `#[cfg(test)]` bodies included."""
    if "all" in _CACHE:
        return _CACHE["all"]
    counts: dict[str, int] = {}
    for path in sorted(REPO.glob("*/src/**/*.rs")):
        rel = path.relative_to(REPO).as_posix()
        if "target/" in rel:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        hits = sum(1 for line in text.splitlines() if guard.UNSAFE.search(guard.strip_literals(line)))
        if hits:
            counts[rel] = hits
    _CACHE["all"] = counts
    return counts


def test_only_counts() -> dict[str, int]:
    production = production_counts()
    counts: dict[str, int] = {}
    for rel, total in all_counts().items():
        remaining = total - production.get(rel, 0)
        if remaining:
            counts[rel] = remaining
    return counts


def parse_ledger(text: str) -> dict[str, int]:
    """Per-file counts from the inventory table."""
    counts: dict[str, int] = {}
    for line in text.splitlines():
        if not line.startswith("| `"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 2 or not cells[0].startswith("`"):
            continue
        path = cells[0].strip("`")
        if not path.endswith(".rs"):
            continue
        if not re.fullmatch(r"\d+", cells[1]):
            raise AssertionError(f"ledger row for {path} has a non-numeric site count: {cells[1]!r}")
        counts[path] = int(cells[1])
    return counts


class UnsafeLedgerContract(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(LEDGER.exists(), f"missing {LEDGER}")
        self.text = LEDGER.read_text(encoding="utf-8")
        self.production = production_counts()
        self.test_only = test_only_counts()
        self.ledger = parse_ledger(self.text)

    def test_every_production_site_is_in_the_ledger(self) -> None:
        missing = sorted(set(self.production) - set(self.ledger))
        self.assertEqual(
            missing,
            [],
            "unsafe sites in crates with no ledger row — add a row to docs/unsafe-ledger.md:\n  "
            + "\n  ".join(f"{p}: {self.production[p]}" for p in missing),
        )

    def test_counts_match_the_ledger(self) -> None:
        drift = {
            path: (self.ledger[path], self.production.get(path, 0))
            for path in sorted(self.ledger)
            if self.ledger[path] != self.production.get(path, 0)
        }
        self.assertEqual(
            drift,
            {},
            "unsafe counts drifted from docs/unsafe-ledger.md (ledger, actual):\n  "
            + "\n  ".join(f"{p}: ledger={l} actual={a}" for p, (l, a) in drift.items()),
        )

    def test_total_matches_the_declared_number(self) -> None:
        match = re.search(r"\| Production code[^|]*\|\s*\*\*(\d+)\*\*\s*\|", self.text)
        if match is None:
            self.fail("the inventory table no longer declares a production total")
        declared = int(match.group(1))
        actual = sum(self.production.values())
        self.assertEqual(declared, actual, f"ledger declares {declared} production sites, found {actual}")

    def test_no_unsafe_in_a_sealed_crate(self) -> None:
        section = self.text.split("## Sealed crates", 1)
        self.assertEqual(len(section), 2, "docs/unsafe-ledger.md lost its '## Sealed crates' section")
        body = section[1].split("Not sealed", 1)[0]
        sealed = set(
            re.findall(r"`((?:ferrosa-[a-z0-9-]+|red-ledger|xtask|tools/alloc-count))`", body)
        )
        self.assertGreater(len(sealed), 10, f"sealed-crate list looks empty or unparsed: {sorted(sealed)}")
        offenders = sorted(path for path in self.production if path.split("/src/")[0] in sealed)
        self.assertEqual(offenders, [], f"sealed crates must not contain unsafe: {offenders}")
        # The ledger's list and the crates that actually carry the attribute must agree, or the list
        # is describing a seal somebody removed.
        self.assertEqual(
            sorted(sealed),
            sorted(guard.sealed_crates(REPO)),
            "the ledger's sealed list and the #![forbid(unsafe_code)] attributes disagree",
        )

    def test_safety_comment_coverage_is_reported(self) -> None:
        sites = guard.collect(REPO)
        total = len(sites)
        covered = sum(1 for site in sites if site.safety)
        mechanism = sum(1 for site in sites if site.safety and guard.safety_is_mechanism(site.safety))
        # Recorded rather than enforced here: the gate enforces it for permitted sites, and this
        # number is how the remaining debt stays visible. Print it so it does not go stale unseen.
        print(
            f"\nunsafe SAFETY-comment coverage: {covered}/{total} production sites carry a comment, "
            f"{mechanism} name a mechanism; {sum(self.test_only.values())} test-only sites in src/"
        )


if __name__ == "__main__":
    unittest.main()
