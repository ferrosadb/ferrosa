#!/usr/bin/env python3
"""Every `--all-features` workspace lane must exclude the live-infra tests.

Why this exists
---------------
A test behind `live-infra-tests` is *absent* from the default `cargo test`
target and *present* in every `--all-features` lane. Without its prerequisites
(`FERROSA_TEST_CLUSTER_NODES`, `FERROSA_TEST_CONTAINERS`,
`FERROSA_TEST_FIRECRACKER`) such a test does not skip quietly — the repo test
policy makes it `panic!` with setup instructions. So a lane that enables
`--all-features` on a runner with no cluster/containers fails on a test that
was never runnable there.

That is not a per-lane decision an author can remember. It happened: commit
532529ac added `--skip replicated_jsonb_index_builds_on_every_node` to ci.yml
(when the test landed) and forgot `nightly-fuzz.yml`, so the nightly fuzz
session went red on 2026-09-30 and stayed red for 18 days, filing a tracking
issue every night. The skip lists are hand-maintained copies of each other.

This guard turns "keep the copies in sync" into a checked invariant: for every
live-only test, at least one lane mechanism must cover it — a `--skip` pattern
that is a substring of the test name, an `--exclude <crate>`, or (nextest) a
`-E 'not binary(<target>)'` filter.

Exit codes: 0 = every lane covers every live-only test; 1 = at least one lane
does not (the offending lane, test and file are printed); 2 = usage error.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Lanes that build the whole workspace with --all-features. Each entry is a
# file whose `cargo test` / `cargo nextest run` command runs the live-infra
# tests. Keep this list explicit: a new lane is a new obligation.
LANE_FILES = [
    ".github/workflows/ci.yml",
    ".github/workflows/nightly-fuzz.yml",
    "scripts/pre-push-mirror-ci.sh",
]

FEATURE_GATE = '#![cfg(feature = "live-infra-tests")]'
PER_TEST_GATE = '#[cfg(feature = "live-infra-tests")]'

# Attribute blocks that can precede a test fn, e.g.
# `#[tokio::test(flavor = "multi_thread")]`.
TEST_ATTR = re.compile(r"#\[(?:tokio::)?test\b[^\]]*\]")
FN_DECL = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")
CFG_GATE_LINE = re.compile(r"#\[cfg\(feature = \"live-infra-tests\"\)\]")


def live_infra_tests(repo: Path) -> dict[str, list[str]]:
    """Map test-target path -> live-only test names in it.

    A target is live-only when the whole file is gated or when individual
    `#[test]` fns are gated. Helper fns (no `#[test]`) are not tests and are
    never demanded of a lane.
    """
    found: dict[str, list[str]] = {}
    for crate_dir in sorted(p for p in repo.iterdir() if p.is_dir()):
        if not (crate_dir / "Cargo.toml").is_file():
            continue
        for target in sorted((crate_dir / "tests").glob("*.rs")):
            text = target.read_text(errors="replace")
            rel = str(target.relative_to(repo))
            if FEATURE_GATE in text:
                names = _gated_test_names(text, whole_file=True)
            elif PER_TEST_GATE in text:
                names = _gated_test_names(text, whole_file=False)
            else:
                continue
            if names:
                found[rel] = names
    return found


def _gated_test_names(text: str, *, whole_file: bool) -> list[str]:
    """Test fns that are behind a `live-infra-tests` gate.

    Scans the attribute block immediately above each `fn` so a
    `#[cfg(feature = "live-infra-tests")]` on a helper fn is never mistaken for
    a gated test.
    """
    names: list[str] = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if not TEST_ATTR.search(line):
            continue
        # Walk the contiguous attribute block: the test attr is the last one.
        attrs: list[str] = [line]
        scan = index - 1
        while scan >= 0 and lines[scan].strip().startswith("#["):
            attrs.append(lines[scan])
            scan -= 1
        gated = whole_file or any(CFG_GATE_LINE.search(attr) for attr in attrs)
        if not gated:
            continue
        for body in lines[index + 1 : index + 4]:
            match = FN_DECL.search(body)
            if match:
                names.append(match.group(1))
                break
    return names


def lane_commands(text: str) -> list[str]:
    """The `--all-features --workspace` test commands in one lane file.

    Joins POSIX line continuations first, so a YAML block scalar and a shell
    script both yield single-line commands.
    """
    joined = text.replace("\\\n", " ")
    commands = []
    for line in joined.splitlines():
        if not re.search(r"cargo\s+(?:test|nextest\s+run)\b", line):
            continue
        if "--all-features" in line and "--workspace" in line:
            commands.append(line.strip())
    return commands


def array_skips(text: str) -> list[str]:
    """`--skip` patterns inside a `SKIPS=( ... )` array in the same file."""
    match = re.search(r"SKIPS=\((.*?)\n\s*\)", text, re.S)
    if not match:
        return []
    return re.findall(r"--skip\s+(\S+)", match.group(1))


def lane_coverage(repo: Path, lane: str) -> tuple[list[str], list[str], list[str], list[str]]:
    """(commands, excluded crates, excluded binaries, skip patterns) for a lane."""
    text = (repo / lane).read_text(errors="replace")
    excluded: list[str] = []
    binaries: list[str] = []
    skips: list[str] = []
    commands = lane_commands(text)
    for command in commands:
        excluded += re.findall(r"--exclude\s+(\S+)", command)
        binaries += re.findall(r"not\s+binary\(([^)]+)\)", command)
        skips += re.findall(r"--skip\s+(\S+)", command)
        if "${SKIPS[@]}" in command:
            skips += array_skips(text)
    return commands, excluded, binaries, skips


def covers(target: str, test: str, excluded: list[str], binaries: list[str], skips: list[str]) -> bool:
    crate = target.split("/", 1)[0]
    if crate in excluded:
        return True
    # `-E 'not binary(x)'` excludes the whole test binary; the binary name is
    # the target's file stem.
    if Path(target).stem in binaries:
        return True
    # libtest `--skip` is a substring filter, so a shorter pattern can cover
    # several tests (e.g. `--skip differential_oracle`).
    return any(pattern in test for pattern in skips)


def check(repo: Path) -> list[str]:
    problems: list[str] = []
    live = live_infra_tests(repo)
    if not live:
        return [f"no live-infra test targets found under {repo}: the scan is broken, not the tree"]
    for lane in LANE_FILES:
        if not (repo / lane).is_file():
            problems.append(f"{lane}: lane file is missing")
            continue
        commands, excluded, binaries, skips = lane_coverage(repo, lane)
        if not commands:
            problems.append(
                f"{lane}: no `cargo test|nextest run --all-features --workspace` command found; "
                "the lane was renamed or the parser is stale"
            )
            continue
        for target, names in live.items():
            for test in names:
                if not covers(target, test, excluded, binaries, skips):
                    problems.append(
                        f"{lane}: live-infra test `{test}` ({target}) is not covered — add "
                        f"`--skip {test}` (a lane with no cluster/containers runs it and it "
                        "panics by policy)"
                    )
    return problems


def main(argv: list[str]) -> int:
    if len(argv) > 2:
        print("guard-all-features-lane-live-infra.py [repo-root]", file=sys.stderr)
        return 2
    repo = Path(argv[1]).resolve() if len(argv) == 2 else Path(__file__).resolve().parents[2]
    problems = check(repo)
    if problems:
        for problem in problems:
            print(f"error: {problem}", file=sys.stderr)
        return 1
    lanes = ", ".join(LANE_FILES)
    print(f"ok: every live-infra test is excluded from each --all-features lane ({lanes})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
