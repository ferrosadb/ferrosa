#!/usr/bin/env python3
"""CI gate: `unsafe` is permitted only where the platform forces it, never for speed.

The rule is docs/unsafe-policy.md. This script is the enforcement: every production `unsafe` site
must appear in scripts/ci-gates/unsafe-allow.toml, either as a permitted `[[allow]]` entry (a kind
that names a platform mandate) or as a `[[debt]]` entry (a recorded finding with a fix). A site that
is in neither fails, and so does an entry whose site is gone — rot in both directions.

    python3 scripts/ci-gates/guard-unsafe.py            # this checkout
    python3 scripts/ci-gates/guard-unsafe.py --root DIR # another

Exit 0 clean, 1 on any violation, 2 if it cannot scan.

Failure classes (each has a RED test in tests/ci/test_guard_unsafe.py):

    unlisted              production `unsafe` with no entry at all
    stale                 an entry whose site no longer exists
    duplicate             more entries for a (file, item) than there are sites
    missing-field         an entry without file/item/kind/reason/owner/since
    perf / unknown        a `perf` or `unknown` kind in [[allow]] — those are findings, not kinds
    not-a-finding         a `perf`/`unknown` entry in [[debt]] whose `fix` is missing
    no-safety-comment     an [[allow]] site with no `// SAFETY:` comment above it
    weak-safety-comment   the comment names no mechanism (see MECHANISM below)
    sealed                production `unsafe` in a crate carrying #![forbid(unsafe_code)]
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - python < 3.11
    print("guard-unsafe: needs python 3.11+ for tomllib", file=sys.stderr)
    raise SystemExit(2)

ALLOWLIST = Path("scripts/ci-gates/unsafe-allow.toml")

# Permitted kinds. `perf` and `unknown` are deliberately absent: they are findings, not categories,
# so an entry that uses one in [[allow]] fails rather than passing as an unrecognised string.
PERMITTED = {"ffi", "syscall", "ioctl", "mmap", "allocator", "layout", "sync-primitive"}
FINDINGS = {"perf", "unknown"}
ALLOW_FIELDS = ("file", "item", "kind", "reason", "owner", "since")
DEBT_FIELDS = ("file", "item", "kind", "reason", "owner", "since", "fix")

# `unsafe {`, `unsafe fn`, `unsafe impl`, `unsafe trait`, `unsafe extern`. Narrower than a bare
# `unsafe` search, which also matches SAFETY comments, doc prose and `forbid(unsafe_code)`.
UNSAFE = re.compile(r"\bunsafe\s*(\{|fn|impl|trait|extern)")

CFG_TEST = re.compile(r"#\[cfg\(test\)\]")
ATTRIBUTE = re.compile(r"^\s*#!?\[")
ITEM_START = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:default\s+)?(?:const\s+)?(?:async\s+)?"
    r"(?:unsafe\s+)?(?:extern\s+\"[^\"]*\"\s+)?(fn|impl|trait|mod)\b"
)
MOD_DECL = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;")
SAFETY_LINE = re.compile(r"//\s*SAFETY\b|///\s*#\s*Safety", re.IGNORECASE)

# A SAFETY comment is a claim, not evidence. This heuristic cannot judge soundness — its job is to
# reject the two shapes that carry no information: a comment too short to say anything, and a comment
# that only restates the operation ("getrlimit is a standard POSIX call"). A comment passes when it
# is at least MIN_SAFETY_CHARS long and contains one of these terms. Each term is a word a mechanism
# uses — a cause, a guard, an ordering, a lifetime, an ownership — and none of them appear in a
# restatement of what the call does.
MECHANISM = (
    "because", "since ", "only if", "only when", "only used", "guarded", "initializ", "outliv",
    "immutable", "renamed", "checked", "exclusiv", "declared before", "never ", "for the duration",
    "own", "scoped", "cannot", "lifetime", "synchroniz", "atomic", "valid", "before it is read",
    "drop", "stable", "not moved", "non-overlapping", "pinned", "side effect", "dereference",
    "ensures", "completes before", "after all", "coordinat", "writable handle", "survives",
    "private to", "closed", "forward", "unmodified", "unchanged", "as long as", "holds",
)
MIN_SAFETY_CHARS = 40
SAFETY_LOOKBACK = 10


@dataclass(frozen=True)
class Site:
    file: str
    line: int
    kind: str  # "{" | "fn" | "impl" | ...
    item: str  # normalized enclosing item name, stable across line drift
    code: str
    safety: str | None


def strip_literals(line: str) -> str:
    """Remove string/char/raw-string literals, then line comments.

    Brace counting and the `unsafe` search both have to see code only: a `"{"` in a format string
    would otherwise shift the depth of every line after it in the file, and `"unsafe {"` in a doc
    example would otherwise count as a site.
    """
    out = []
    index = 0
    length = len(line)
    while index < length:
        char = line[index]
        if char == "r" and index + 1 < length and line[index + 1] in '#"':
            # Raw string: r"..." / r#"..."# / r##"..."##
            hashes = 0
            probe = index + 1
            while probe < length and line[probe] == "#":
                hashes += 1
                probe += 1
            if probe < length and line[probe] == '"':
                closer = '"' + "#" * hashes
                end = line.find(closer, probe + 1)
                index = length if end < 0 else end + len(closer)
                out.append('""')
                continue
        if char == '"':
            probe = index + 1
            while probe < length:
                if line[probe] == "\\":
                    probe += 2
                    continue
                if line[probe] == '"':
                    break
                probe += 1
            index = probe + 1
            out.append('""')
            continue
        if char == "'":
            match = re.match(r"'(?:\\.|[^'\\])'", line[index:])
            if match:
                index += match.end()
                out.append("' '")
                continue
        out.append(char)
        index += 1
    code = "".join(out)
    comment = re.search(r"(?<!:)//", code)
    return code[: comment.start()] if comment else code


def item_name(header: str) -> str:
    """The name of an item, without its signature or generics.

    `fn write_entry(&self, offset: u64) -> CommitLogPosition` -> `write_entry`
    `unsafe impl<'a, R: ReadAt> Send for RangeMerger<'a, R>` -> `unsafe impl Send for RangeMerger`

    The signature and the generic parameters are deliberately dropped: `item` must survive a
    parameter being added or a bound changing, which is the whole reason it is not a line number.
    """
    text = re.sub(r"\s+", " ", header.strip()).split("{", 1)[0].strip().rstrip(";").strip()
    while True:  # innermost-first, so `impl<A<B>>` converges
        reduced = re.sub(r"<[^<>]*>", "", text)
        if reduced == text:
            break
        text = reduced
    text = re.sub(r"\s+", " ", text).strip()
    match = re.search(r"\b(?:fn|trait|mod)\s+([A-Za-z_][A-Za-z0-9_]*)", text)
    if match:
        return match.group(1)
    return text


def safety_above(lines: list[str], number: int) -> str | None:
    """The nearest `// SAFETY:` or `/// # Safety` block at or above 1-indexed `number`.

    Scans back a bounded window rather than only the line above: a comment that covers several
    statements, or a pair of `unsafe impl Send`/`Sync` lines, has the comment above the first.
    """
    for index in range(number - 1, max(-1, number - 1 - SAFETY_LOOKBACK), -1):
        if not SAFETY_LINE.search(lines[index]):
            continue
        # Collect the whole contiguous comment block: a SAFETY line is often the first line of a
        # multi-line reason, and a Send/Sync pair shares one comment above the first.
        start = index
        while start > 0 and lines[start - 1].strip().startswith("//"):
            start -= 1
        end = index
        while end + 1 < len(lines) and lines[end + 1].strip().startswith("//"):
            end += 1
        return " ".join(line.strip() for line in lines[start : end + 1])
    return None


def safety_is_mechanism(text: str) -> bool:
    body = re.sub(r"^[/#!\s]*(?:SAFETY|#\s*Safety)\s*:?\s*", "", text.strip(), flags=re.IGNORECASE)
    if len(body) < MIN_SAFETY_CHARS:
        return False
    lowered = body.lower()
    return any(term in lowered for term in MECHANISM)


def test_only_files(root: Path, sources: list[Path]) -> set[str]:
    """Files reachable only through a `#[cfg(test)] mod name;` declaration, transitively.

    Without this a test module declared in another file is counted as production, which was a real
    false positive before: `#[cfg(test)] mod allocation_test_support;` lives in its own file.
    """
    resolved: set[str] = set()
    pending = list(sources)
    seen: set[Path] = set()
    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        armed = False
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            code = strip_literals(raw)
            if CFG_TEST.search(code):
                armed = True
                continue
            declaration = MOD_DECL.match(code)
            if armed and declaration:
                armed = False
                name = declaration.group(1)
                for candidate in (path.parent / f"{name}.rs", path.parent / name / "mod.rs"):
                    if candidate.is_file():
                        resolved.add(candidate.relative_to(root).as_posix())
                        pending.append(candidate)
            elif code.strip() and not ATTRIBUTE.match(code):
                armed = False
    return resolved


def collect(root: Path) -> list[Site]:
    """Every production `unsafe` site, with its enclosing item and its SAFETY claim."""
    sources = [p for p in sorted(root.glob("*/src/**/*.rs")) if "target/" not in p.as_posix()]
    whole_file_test = test_only_files(root, sources)
    sites: list[Site] = []

    for path in sources:
        rel = path.relative_to(root).as_posix()
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        file_is_test = rel in whole_file_test
        # Open items: (depth_at_open, name, is_cfg_test).
        stack: list[tuple[int, str, bool]] = []
        pending: tuple[int, str, bool] | None = None
        depth = 0
        pending_cfg_test = False

        for number, raw in enumerate(lines, start=1):
            code = strip_literals(raw)
            stripped = code.strip()

            if ATTRIBUTE.match(code):
                if CFG_TEST.search(code):
                    pending_cfg_test = True
                continue
            if not stripped:
                continue

            # An item that has closed cannot enclose this line.
            while stack and depth <= stack[-1][0]:
                stack.pop()

            if ITEM_START.match(code):
                pending = (depth, item_name(code), pending_cfg_test)

            match = UNSAFE.search(code)
            if match:
                current_test = (
                    file_is_test
                    or pending_cfg_test
                    or any(frame[2] for frame in stack)
                    or (pending[2] if pending and pending[0] == depth else False)
                )
                if not current_test:
                    # A site on an item's own header belongs to that item, not to its parent.
                    if pending and pending[0] == depth:
                        enclosing = pending[1]
                    elif stack:
                        enclosing = stack[-1][1]
                    else:
                        enclosing = "<file scope>"
                    sites.append(
                        Site(
                            file=rel,
                            line=number,
                            kind=match.group(1),
                            item=enclosing,
                            code=stripped[:200],
                            safety=safety_above(lines, number),
                        )
                    )

            depth_after = depth + code.count("{") - code.count("}")
            if pending is not None and depth_after > pending[0]:
                stack.append(pending)
                pending = None
            elif pending is not None and depth_after == pending[0] and stripped.endswith((";", "}")):
                pending = None
            depth = depth_after
            if not ATTRIBUTE.match(code):
                pending_cfg_test = False

    return sites


def sealed_crates(root: Path) -> set[str]:
    sealed = set()
    roots = (
        list(root.glob("*/src/lib.rs"))
        + list(root.glob("*/src/main.rs"))
        + list(root.glob("tools/*/src/lib.rs"))
    )
    for root_file in roots:
        if re.search(r"#!\[forbid\(unsafe_code\)\]", root_file.read_text(encoding="utf-8", errors="ignore")):
            parts = root_file.relative_to(root).parts
            sealed.add("/".join(parts[:-2]))
    return sealed


def load_allowlist(root: Path) -> tuple[list[dict], list[dict]]:
    path = root / ALLOWLIST
    if not path.is_file():
        raise SystemExit(f"guard-unsafe: missing {ALLOWLIST}")
    parsed = tomllib.loads(path.read_text(encoding="utf-8"))
    return parsed.get("allow", []), parsed.get("debt", [])


def check(root: Path) -> tuple[list[str], list[Site], int]:
    sites = collect(root)
    allow, debt = load_allowlist(root)
    problems: list[str] = []

    sites_by_group: dict[tuple[str, str], list[Site]] = defaultdict(list)
    for site in sites:
        sites_by_group[(site.file, site.item)].append(site)

    allow_groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    debt_groups: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for index, entry in enumerate(allow, start=1):
        missing = [field for field in ALLOW_FIELDS if not str(entry.get(field, "")).strip()]
        if missing:
            problems.append(
                f"missing-field  [[allow]] #{index} ({entry.get('file', '?')}::"
                f"{entry.get('item', '?')}) has no {', '.join(missing)}"
            )
            continue
        kind = entry["kind"]
        if kind in FINDINGS:
            problems.append(
                f"{kind:<13}  {entry['file']}::{entry['item']} is in [[allow]] as `{kind}`, which is "
                f"a finding, not a permitted kind (docs/unsafe-policy.md §2). Move it to [[debt]]."
            )
        elif kind not in PERMITTED:
            problems.append(
                f"missing-field  [[allow]] #{index} ({entry['file']}::{entry['item']}) has kind "
                f"{kind!r}; permitted: {', '.join(sorted(PERMITTED))}"
            )
        allow_groups[(entry["file"], entry["item"])].append(entry)

    for index, entry in enumerate(debt, start=1):
        missing = [field for field in DEBT_FIELDS if not str(entry.get(field, "")).strip()]
        if missing:
            problems.append(
                f"missing-field  [[debt]] #{index} ({entry.get('file', '?')}::"
                f"{entry.get('item', '?')}) has no {', '.join(missing)}"
            )
            continue
        kind = entry["kind"]
        if kind not in FINDINGS:
            problems.append(
                f"not-a-finding  {entry['file']}::{entry['item']} is in [[debt]] with kind {kind!r}; "
                f"[[debt]] is only for `perf` and `unknown` — a permitted site belongs in [[allow]]"
            )
        debt_groups[(entry["file"], entry["item"])].append(entry)

    for group, group_sites in sorted(sites_by_group.items()):
        file, item = group
        allow_here = allow_groups.get(group, [])
        debt_here = debt_groups.get(group, [])
        total = len(allow_here) + len(debt_here)
        if total < len(group_sites):
            problems.append(
                f"unlisted      {file}::{item} has {len(group_sites)} production `unsafe` site(s) "
                f"but {total} entry(ies). Add a [[allow]] entry (kind, reason, owner, since) — or, "
                f"if it is a perf/unknown finding, a [[debt]] entry with its fix."
            )
        elif total > len(group_sites):
            problems.append(
                f"duplicate     {file}::{item} has {total} entry(ies) for {len(group_sites)} site(s) "
                f"— delete the extra entry in the commit that removed the site"
            )

    for group, allow_here in sorted(allow_groups.items()):
        file, item = group
        group_sites = sites_by_group.get(group, [])
        if not group_sites:
            problems.append(
                f"stale         {file}::{item} is in [[allow]] but the site is gone — delete the "
                f"entry in the same commit that removed it"
            )
            continue
        for site in group_sites:
            if site.safety is None:
                problems.append(
                    f"no-safety-comment  {site.file}:{site.line} ({site.item}) has no `// SAFETY:` "
                    f"comment — write the mechanism, not the operation"
                )
            elif not safety_is_mechanism(site.safety):
                problems.append(
                    f"weak-safety-comment  {site.file}:{site.line} ({site.item}) SAFETY comment "
                    f"names no mechanism: {site.safety[:90]!r}. Say why the invariant holds "
                    f"(docs/unsafe-policy.md §3); a restatement of the call is not a reason"
                )

    for group in sorted(debt_groups):
        if group not in sites_by_group:
            problems.append(
                f"stale         {group[0]}::{group[1]} is in [[debt]] but the site is gone — delete "
                f"the entry in the same commit that removed it"
            )

    sealed = sealed_crates(root)
    for site in sites:
        crate = site.file.split("/src/")[0] if "/src/" in site.file else site.file.split("/")[0]
        if crate in sealed:
            problems.append(
                f"sealed        {site.file}:{site.line} is production `unsafe` in {crate}, which "
                f"carries #![forbid(unsafe_code)]"
            )

    return problems, sites, len(debt)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="repository root (default: .)")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    if not (root / "Cargo.toml").is_file():
        print(f"guard-unsafe: {root} is not a cargo workspace root", file=sys.stderr)
        return 2
    try:
        problems, sites, debt_count = check(root)
    except SystemExit as exc:
        print(exc, file=sys.stderr)
        return 2

    if problems:
        print(
            f"guard-unsafe: {len(problems)} violation(s). The rule is docs/unsafe-policy.md:\n"
            f"  no unsafe except where the platform forces it, and never for performance.\n",
            file=sys.stderr,
        )
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        print(
            "\nA permitted site needs a [[allow]] entry in scripts/ci-gates/unsafe-allow.toml:\n"
            "\n  [[allow]]\n"
            '  file   = "ferrosa-sstable/src/direct.rs"\n'
            '  item   = "fadvise_dontneed"\n'
            '  kind   = "syscall"\n'
            '  reason = "posix_fadvise takes a raw fd; offset/len 0 mean the whole file, and the "\n'
            '           "call is advisory so it cannot fault"\n'
            '  owner  = "storage"\n'
            '  since  = "2026-11-04"\n'
            "\nkind must be one of: " + ", ".join(sorted(PERMITTED)) + "\n"
            "perf and unknown are findings: they go in [[debt]] with a `fix`, and that is a\n"
            "reviewed change (docs/unsafe-policy.md §4).\n",
            file=sys.stderr,
        )
        return 1
    print(
        f"guard-unsafe: clean — {len(sites)} production sites allowlisted "
        f"({len(sites) - debt_count} permitted, {debt_count} recorded findings)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
