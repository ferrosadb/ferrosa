"""Contract test: docs/unsafe-ledger.md must describe the unsafe we actually have.

`unsafe` is the part of the codebase no proof covers — the Lean corpus models a specified machine,
Aeneas refuses to translate `unsafe`, and Miri only sees executed paths. The defence is that the set
is small, enumerated, and reviewed by hand, which only works if the enumeration cannot silently drift.

This test is the ratchet: it rescans `*/src/**/*.rs`, ignores `#[cfg(test)]` modules (counted
separately in the ledger), and fails when a file's production count differs from its ledger row, or
when a file with `unsafe` has no row at all. Decreasing a count is a failure too — it means the ledger
is stale in the other direction, and stale documentation is what this exists to prevent.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LEDGER = REPO / "docs" / "unsafe-ledger.md"

# `unsafe {`, `unsafe fn`, `unsafe impl`, `unsafe trait`, `unsafe extern`. Deliberately narrower than
# a bare `unsafe` search, which also matches SAFETY comments, doc prose and `forbid(unsafe_code)`.
UNSAFE = re.compile(r"unsafe\s*(\{|fn|impl|trait|extern)")
SAFETY = re.compile(r"//\s*SAFETY|///.*SAFETY")
CFG_TEST = re.compile(r"#\[cfg\(test\)\]")
MOD_OPEN = re.compile(r"\bmod\s+\w+\s*\{")


def _strip_line_comment(line: str) -> str:
    idx = line.find("//")
    return line[:idx] if idx >= 0 else line


def cfg_test_ranges(lines: list[str]) -> list[tuple[int, int]]:
    """Line ranges covered by `#[cfg(test)] mod ... { }` blocks, 1-indexed and inclusive."""
    ranges: list[tuple[int, int]] = []
    depth = 0
    pending_cfg_test = False
    for number, raw in enumerate(lines, start=1):
        code = _strip_line_comment(raw)
        if CFG_TEST.search(code):
            pending_cfg_test = True
        elif pending_cfg_test and MOD_OPEN.search(code):
            pending_cfg_test = False
            start_depth = depth
            opener = number
            scan_depth = depth
            scan = number
            while scan <= len(lines):
                text = _strip_line_comment(lines[scan - 1])
                scan_depth += text.count("{") - text.count("}")
                if scan_depth <= start_depth and scan > opener:
                    break
                scan += 1
            ranges.append((opener, scan))
        depth += code.count("{") - code.count("}")
    return ranges


CFG_TEST_MOD = re.compile(r"\bmod\s+(\w+)\s*;")


def test_only_module_files(root: Path) -> set[str]:
    """Files reachable only through a `#[cfg(test)] mod name;` declaration.

    The block scan above sees a module's body only when the body is in the same file. A test module
    declared as `#[cfg(test)] mod allocation_test_support;` lives in another file, so its `unsafe`
    sites were counted as production even though no non-test build compiles them. Resolve those
    declarations, and follow them transitively.
    """
    candidates = sorted(root.glob("*/src/**/*.rs"))
    resolved: set[str] = set()
    pending = list(candidates)
    seen: set[Path] = set()
    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        armed = False
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            code = _strip_line_comment(raw)
            if CFG_TEST.search(code):
                armed = True
                continue
            declaration = CFG_TEST_MOD.search(code)
            if armed and declaration:
                armed = False
                name = declaration.group(1)
                for candidate in (path.parent / f"{name}.rs", path.parent / name / "mod.rs"):
                    if candidate.is_file():
                        resolved.add(candidate.relative_to(root).as_posix())
                        pending.append(candidate)
            elif code.strip() and not code.strip().startswith("#["):
                armed = False
    return resolved


def scan(root: Path) -> tuple[dict[str, int], dict[str, int]]:
    """(production, test-only) unsafe counts per crate-relative file path."""
    whole_file_test_modules = test_only_module_files(root)
    production: dict[str, int] = {}
    test_only: dict[str, int] = {}
    for path in sorted(root.glob("*/src/**/*.rs")):
        rel = path.relative_to(root).as_posix()
        if rel.startswith(("target/", ".")) or "/examples/" in rel or rel.startswith("examples/"):
            continue
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        ranges = cfg_test_ranges(lines)
        for number, line in enumerate(lines, start=1):
            code = _strip_line_comment(line)
            if not UNSAFE.search(code):
                continue
            if rel in whole_file_test_modules or any(a <= number <= b for a, b in ranges):
                bucket = test_only
            else:
                bucket = production
            bucket[rel] = bucket.get(rel, 0) + 1
    return production, test_only


def parse_ledger(text: str) -> dict[str, int]:
    """Per-file counts from the inventory table, plus the declared total."""
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
        self.production, self.test_only = scan(REPO)
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
        sealed = set(re.findall(r"`(ferrosa-[a-z0-9-]+)`", body))
        self.assertGreater(len(sealed), 10, f"sealed-crate list looks empty or unparsed: {sorted(sealed)}")
        offenders = sorted(
            path for path in self.production if path.split("/src/")[0] in sealed
        )
        self.assertEqual(offenders, [], f"sealed crates must not contain unsafe: {offenders}")

    def test_safety_comment_coverage_is_reported(self) -> None:
        total = covered = 0
        for rel in self.production:
            path = REPO / rel
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            ranges = cfg_test_ranges(lines)
            for number, line in enumerate(lines, start=1):
                if not UNSAFE.search(_strip_line_comment(line)):
                    continue
                if any(a <= number <= b for a, b in ranges):
                    continue
                total += 1
                window = lines[max(0, number - 5) : number]
                if any(SAFETY.search(entry) for entry in window):
                    covered += 1
        # Recorded rather than enforced: the ledger's audit queue is how this number moves, and
        # failing here would block every unrelated change. Print it so the debt stays visible.
        print(
            f"\nunsafe SAFETY-comment coverage: {covered}/{total} production sites "
            f"({100 * covered // max(total, 1)}%), test-only sites: {sum(self.test_only.values())}"
        )


if __name__ == "__main__":
    unittest.main()
