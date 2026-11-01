#!/usr/bin/env python3
"""ADR-013 gate: membership is changed in one place.

specs/decisions/013-membership-change-protocol.md puts every change to the four
membership stores (the state machine's members, openraft's membership, the
network factory's node map, the peer manager) behind the membership module, so
the four cannot drift apart. Outside it, ferrosa-cluster production code must
not:

* call the raw primitives `add_learner`, `change_membership` or
  `register_node`, on any receiver; or
* build a `RaftOp::JoinNode`, `RaftOp::LeaveNode` or `RaftOp::UpdateNodeInfo`,
  which are the membership ops the ADR names.

The receiver's name is not the contract. The gate this replaces matched the
literal `raft.client_write`, so `raft_arc.change_membership(...)` passed, while
every DDL proposal failed it; it then carried a line-number allowlist that had
drifted to excusing nothing, and CI never ran it.

Allowed: the membership module itself, the forwarding helper it uses
(`raft_forward.rs`), the network factory's own implementation of
`register_node` (`raft/network.rs`), and test code (`tests.rs`, `*_tests.rs`,
`#[cfg(test)]` items). There is no allowlist: a site is migrated, not excused.

Usage: no-raw-membership-mutation.py [REPO_ROOT]
Exit:  0 clean, 1 violations found, 2 cannot scan (not a git checkout).
"""

import re
import subprocess
import sys
from pathlib import Path

SCOPE = "ferrosa-cluster/src"
ALLOWED = (
    "ferrosa-cluster/src/membership/",
    "ferrosa-cluster/src/raft_forward.rs",
    # The forwarding helper's wire codec: it rebuilds a received op, it does
    # not originate one.
    "ferrosa-cluster/src/raft_forward_codec.rs",
    "ferrosa-cluster/src/raft/network.rs",
)
PRIMITIVE = re.compile(r"\.\s*(add_learner|change_membership|register_node)\s*\(")
MEMBERSHIP_OP = re.compile(r"\bRaftOp\s*::\s*(JoinNode|LeaveNode|UpdateNodeInfo)\b")


def is_test_file(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    return name == "tests.rs" or name.endswith("_tests.rs")


def blank(text: str, start: int, end: int, out: list) -> None:
    """Replace text[start:end] with spaces, keeping newlines so lines still count."""
    for i in range(start, end):
        if text[i] != "\n":
            out[i] = " "


def strip_comments_and_strings(text: str) -> str:
    out = list(text)
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end == -1 else end
            blank(text, i, end, out)
            i = end
        elif text.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(text, i, j, out)
            i = j
        elif (m := re.match(r'b?r(#*)"', text[i:])) and (i == 0 or not text[i - 1].isalnum()):
            close = '"' + m.group(1)
            end = text.find(close, i + m.end())
            end = n if end == -1 else end + len(close)
            blank(text, i, end, out)
            i = end
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            blank(text, i, j + 1, out)
            i = j + 1
        elif m := re.match(r"'(\\.|[^\\'\n])'", text[i:]):
            blank(text, i, i + m.end(), out)
            i += m.end()
        else:
            i += 1
    return "".join(out)


def strip_cfg_test_items(text: str) -> str:
    """Blank every `#[cfg(test)]` item: through its `;`, or its balanced braces."""
    out = list(text)
    for m in re.finditer(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]", text):
        j = m.end()
        while j < len(text) and text[j] not in "{;":
            j += 1
        if j >= len(text):
            break
        if text[j] == ";":
            blank(text, m.start(), j + 1, out)
            continue
        depth, k = 0, j
        while k < len(text):
            if text[k] == "{":
                depth += 1
            elif text[k] == "}":
                depth -= 1
                if depth == 0:
                    break
            k += 1
        blank(text, m.start(), k + 1, out)
    return "".join(out)


def end_of_group(code: str, i: int) -> int:
    """Index just past the `(...)` or `{...}` group starting at `i`, else `i`."""
    if i >= len(code) or code[i] not in "({":
        return i
    depth = 0
    for k in range(i, len(code)):
        if code[k] in "({[":
            depth += 1
        elif code[k] in ")}]":
            depth -= 1
            if depth == 0:
                return k + 1
    return len(code)


def in_matches_macro(code: str, i: int) -> bool:
    """Is position `i` inside the pattern argument of a `matches!(...)`?"""
    depth = 0
    for k in range(i - 1, -1, -1):
        if code[k] in ")]}":
            depth += 1
        elif code[k] in "([{":
            if depth == 0:
                return code[k] == "(" and re.search(r"matches!\s*$", code[:k]) is not None
            depth -= 1
    return False


def is_pattern(code: str, m: re.Match) -> bool:
    """A variant in pattern position is being recognised, not proposed.

    Pattern position: followed (past its fields) by `=>`, an alternative `|`, or
    the `=` of an `if let` / `let`; or inside `matches!(..)`.
    """
    start = m.end()
    while start < len(code) and code[start].isspace():
        start += 1
    rest = code[end_of_group(code, start):].lstrip()
    if rest.startswith("=>") or (rest.startswith("|") and not rest.startswith("||")):
        return True
    if rest.startswith("=") and not rest.startswith("=="):
        return True
    return in_matches_macro(code, m.start())


def violations(root: Path, rel: str) -> list:
    text = (root / rel).read_text(encoding="utf-8", errors="replace")
    code = strip_cfg_test_items(strip_comments_and_strings(text))
    found = []
    for m in PRIMITIVE.finditer(code):
        line = code.count("\n", 0, m.start()) + 1
        found.append((rel, line, f"raw primitive `{m.group(1)}`"))
    for m in MEMBERSHIP_OP.finditer(code):
        if is_pattern(code, m):
            continue
        line = code.count("\n", 0, m.start()) + 1
        found.append((rel, line, f"membership op `{m.group(1)}`"))
    return found


def tracked_sources(root: Path):
    listed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "--", SCOPE],
        capture_output=True,
        text=True,
    )
    if listed.returncode != 0:
        return None
    return [p for p in listed.stdout.splitlines() if p.endswith(".rs")]


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    files = tracked_sources(root)
    if files is None:
        print(f"cannot scan {root}: not a git checkout", file=sys.stderr)
        return 2
    found = []
    for rel in sorted(files):
        if rel.startswith(ALLOWED) or is_test_file(rel):
            continue
        found.extend(violations(root, rel))
    if not found:
        print("OK: membership changes stay in the membership module (ADR-013).")
        return 0
    print(f"VIOLATION: {len(found)} membership mutation(s) outside the membership module:")
    for rel, line, what in sorted(found):
        print(f"  {rel}:{line}: {what}")
    print("Route each through MembershipChanger (ferrosa-cluster/src/membership/).")
    print("See specs/decisions/013-membership-change-protocol.md. There is no allowlist.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
