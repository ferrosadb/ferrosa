#!/usr/bin/env python3
"""Fail CI on new production unbounded/materializing read patterns.

Ferrosa has historically hit OOMs when code accidentally materializes whole
ranges/tables into RAM instead of using streaming/paged paths. This guard catches
the highest-risk call shape: `read_range(None, None, ...)` in production Rust.

Tests may exercise the fail-closed cap and intentional exceptions may be marked
with `allowlist unbounded-read` on the same or immediately preceding line.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALLOW = "allowlist unbounded-read"
PATTERNS = [
    re.compile(r"\.read_range\s*\(\s*None\s*,\s*None\s*,"),
    re.compile(r"\bread_range\s*\(\s*None\s*,\s*None\s*,"),
]
SKIP_DIRS = {".git", "target", ".worktrees"}


def is_test_file(path: Path) -> bool:
    rel = path.relative_to(ROOT)
    return "tests" in rel.parts or path.name.endswith("_test.rs")


def in_cfg_test_module(lines: list[str], idx: int) -> bool:
    """Best-effort detector for crate-local `#[cfg(test)] mod tests { ... }`."""
    depth = 0
    test_depths: list[int] = []
    pending_cfg_test = False
    for line_no, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("#[cfg(test)]"):
            pending_cfg_test = True
        if pending_cfg_test and re.search(r"\bmod\s+tests\b", stripped):
            # Count the opening brace for this module below, then mark that depth.
            pass

        opens = line.count("{")
        closes = line.count("}")
        if pending_cfg_test and re.search(r"\bmod\s+tests\b", stripped) and opens:
            test_depths.append(depth + opens)
            pending_cfg_test = False
        depth += opens - closes
        test_depths = [d for d in test_depths if depth >= d]
        if line_no == idx:
            return bool(test_depths)
    return False


def allowlisted(lines: list[str], idx: int) -> bool:
    window = lines[max(0, idx - 3) : idx]
    return any(ALLOW in line for line in window)


def main() -> int:
    violations: list[str] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for filename in filenames:
            if not filename.endswith(".rs"):
                continue
            path = Path(dirpath) / filename
            if is_test_file(path):
                continue
            lines = path.read_text(errors="ignore").splitlines()
            for idx, line in enumerate(lines, start=1):
                stripped = line.strip()
                if stripped.startswith("//") or stripped.startswith("///") or stripped.startswith("//!" ):
                    continue
                if not any(p.search(line) for p in PATTERNS):
                    continue
                if in_cfg_test_module(lines, idx):
                    continue
                if allowlisted(lines, idx):
                    continue
                rel = path.relative_to(ROOT)
                violations.append(f"{rel}:{idx}: {line.strip()}")

    if violations:
        print("Unbounded/materializing read guard failed.", file=sys.stderr)
        print(
            "Use a streaming/paged path instead of read_range(None, None, ...). "
            "If this is intentionally bounded and reviewed, add "
            "'allowlist unbounded-read' on the same or preceding line.",
            file=sys.stderr,
        )
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        return 1
    print("unbounded-read guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
