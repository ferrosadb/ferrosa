#!/usr/bin/env python3
"""Fail when the extraction scope and the extraction disagree.

`scope.toml` says what is extracted and, for every item left out, why. This script checks that
claim against the two artifacts that cannot be talked into agreeing with it: the cargo workspace
(which crates exist) and the generated Lean (which items were left out).

The mechanism that makes this possible is that an excluded item is not silent. `extract.sh`
passes an item entry to charon as `--opaque`, aeneas emits it as a declaration whose body is
`sorry`, and the declaration carries the item's charon name pattern in an `@[rust_fun "..."]`
attribute. So the emitted module is a readable, greppable, committed list of everything that was
assumed, and `claims()` below decides whether a scope entry explains each one.

Usage:
  scope-check.py check [--root DIR] [--module M] [--quiet]
  scope-check.py items <crate> [--root DIR]     one charon pattern per line, for --opaque

Findings, each with the crate or module it is about:

  SILENT         an item is opaque in the Lean and no scope entry explains it
  STALE          a scope entry matches nothing in the Lean (the file rotted)
  UNSCOPED       a workspace crate has no scope decision
  UNSCOPED-CRATE a module was extracted from a crate that is not in scope
  STALE-CRATE    a named crate is not a workspace member / lockfile package
  NOT-VISIBLE    an item entry asks for an exclusion the emitted Lean cannot show
  BLANKET        more than the one blanket exclusion, or a crate name that is really one
  NO-REASON / NO-OWNER / NO-DATE / NO-SCHEMA   an entry nobody is accountable for
"""

from __future__ import annotations

import argparse
import datetime
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import NoReturn

REPO = Path(__file__).resolve().parents[2]
DEFAULT_SPEC = REPO / "specs" / "lean4-rust"

# The kinds of crate entry, and what each means. `own` is a ferrosa crate; `critical` is a
# dependency we chose to reason about. Everything else is covered by `[blanket]`.
CRATE_KINDS = ("own", "critical")
ITEM_KINDS = ("opaque",)

# Lean's top-level declaration keywords, as they appear in aeneas output. A decl continues until
# the next one of these at column 0, so the scan does not need to understand Lean's grammar.
_DECL = re.compile(
    r"^(?:@\[[^\]]*\]\s*)?"
    r"(?:noncomputable\s+|private\s+|protected\s+|partial\s+|unsafe\s+|@\[[^\]]*\]\s*)*"
    r"(def|opaque|abbrev|theorem|lemma|instance|inductive|structure|class|axiom)\s+"
    r"([^\s(:{]+)")
_RUST_FUN = re.compile(r'@\[rust_fun "([^"]+)"\]')
_SORRY = re.compile(r":=\s*sorry\b")


def fail(message: str) -> NoReturn:
    """A malformed scope file is not a finding about the code: it is a broken input."""
    print(f"FAIL: {message}", file=sys.stderr)
    raise SystemExit(2)


def load_scope(path: Path) -> dict:
    try:
        raw = tomllib.loads(path.read_text())
    except FileNotFoundError:
        fail(f"no scope file at {path}")
    except tomllib.TOMLDecodeError as exc:
        fail(f"{path}: {exc}")
    return {
        "schema": raw.get("schema"),
        "crates": raw.get("crate", []),
        "items": raw.get("item", []),
        "blanket": raw.get("blanket"),
    }


def workspace_members(root: Path) -> dict[str, Path]:
    """Every cargo workspace member: package name -> directory. No cargo needed."""
    manifest = root / "Cargo.toml"
    try:
        ws = tomllib.loads(manifest.read_text()).get("workspace", {})
    except FileNotFoundError:
        fail(f"no workspace manifest at {manifest}")
    members: dict[str, Path] = {}
    for entry in ws.get("members", []):
        for directory in sorted(root.glob(entry)) if "*" in entry else [root / entry]:
            pkg = directory / "Cargo.toml"
            if not pkg.is_file():
                continue
            name = tomllib.loads(pkg.read_text())["package"]["name"]
            members[name] = directory
    return members


def lock_packages(root: Path) -> set[str]:
    """Package names in Cargo.lock: a critical dependency that is not there is a rotted entry."""
    try:
        lock = tomllib.loads((root / "Cargo.lock").read_text())
    except (FileNotFoundError, tomllib.TOMLDecodeError):
        return set()
    return {p.get("name") for p in lock.get("package", []) if p.get("name")}


def modules(spec: Path) -> list[dict]:
    rows = []
    try:
        text = (spec / "modules.txt").read_text()
    except FileNotFoundError:
        return rows
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        rows.append({"module": parts[0], "crate": parts[1]})
    return rows


def lean_decls(text: str) -> list[dict]:
    """Top-level declarations in a generated module: name, its charon pattern, whether sorry.

    Aeneas writes the charon name pattern above each declaration as `@[rust_fun "..."]`. That
    attribute is the whole reason this check can be cheap: the item's identity in the scope file
    and its identity in the Lean are the same string.
    """
    decls: list[dict] = []
    current = None
    pattern = None
    for line in text.splitlines():
        attr = _RUST_FUN.search(line)
        if attr:
            pattern = attr.group(1)
        start = _DECL.match(line)
        if start:
            if current:
                current["sorry"] = bool(_SORRY.search("\n".join(current["body"])))
                decls.append(current)
            current = {"name": start.group(2), "kind": start.group(1),
                       "pattern": pattern, "body": []}
            pattern = None
            if _SORRY.search(line):
                current["body"].append(line)
            continue
        if current is not None:
            current["body"].append(line)
    if current:
        current["sorry"] = bool(_SORRY.search("\n".join(current["body"])))
        decls.append(current)
    for decl in decls:
        decl.pop("body", None)
    return decls


def elements(pattern: str) -> list[str]:
    """Split a charon name pattern on top-level `::`, keeping `{..}` and `<..>` together."""
    out, buf, depth = [], [], 0
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch in "{<(":
            depth += 1
        elif ch in "}>)" and depth:
            depth -= 1
        if depth == 0 and pattern.startswith("::", i):
            out.append("".join(buf))
            buf = []
            i += 2
            continue
        buf.append(ch)
        i += 1
    if buf:
        out.append("".join(buf))
    return [e.strip() for e in out if e.strip()]


def strip_generics(text: str) -> str:
    out, depth = [], 0
    for ch in text:
        if ch == "<":
            depth += 1
        elif ch == ">" and depth:
            depth -= 1
        elif depth == 0:
            out.append(ch)
    return "".join(out).strip()


def _generic_args(text: str) -> list[str]:
    """The arguments of the last `<...>` group in `text` (empty when it has none)."""
    end = text.rfind(">")
    if end < 0:
        return []
    depth, start = 0, None
    for i in range(end, -1, -1):
        if text[i] == ">":
            depth += 1
        elif text[i] == "<":
            depth -= 1
            if depth == 0:
                start = i
                break
    if start is None:
        return []
    args, buf, depth = [], [], 0
    for ch in text[start + 1:end]:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        if ch == "," and depth == 0:
            args.append("".join(buf).strip())
            buf = []
            continue
        buf.append(ch)
    args.append("".join(buf).strip())
    return [a for a in args if a]


def _impl_parts(elem: str) -> tuple[str, str | None, list[str]]:
    """`{impl Trait<Self> for Type}` -> (trait path, impl'd type, pattern generics).

    charon keeps the implemented type in the pattern's generics rather than after `for`, so both
    shapes have to be read: the scope file may write either.
    """
    inner = elem[len("{impl"):-1].strip() if elem.endswith("}") else elem[len("{impl"):]
    if " for " in inner:
        trait_part, type_part = inner.split(" for ", 1)
        return strip_generics(trait_part), type_part.strip(), _generic_args(trait_part)
    return strip_generics(inner), None, _generic_args(inner)


def _impl_claims(scope_elem: str, artifact_elem: str) -> bool:
    s_trait, s_type, s_args = _impl_parts(scope_elem)
    a_trait, a_type, a_args = _impl_parts(artifact_elem)
    if s_trait != a_trait:
        return False
    wanted = [s_type] if s_type else s_args
    if not wanted:
        return True
    # A trait impl written as a pattern keeps the implemented type in the trait's generics
    # (`Deserialize<Type>`); rustc's own name puts it after `for` (`Deserialize for Type`). Both
    # spellings have to compare, because the scope file copies whichever the tool printed.
    have = [a_type] if a_type else a_args
    if not have:
        return False
    return all(w in ("_", "*") or strip_generics(w).split("::")[-1] in
               {strip_generics(h).split("::")[-1] for h in have} for w in wanted)


def claims(scope_pattern: str, artifact_pattern: str) -> bool:
    """Does `scope_pattern` explain `artifact_pattern`? charon's matching rule, in Python.

    Two rules matter and both are load-bearing for a real entry:

    - a pattern matches a prefix of the item's name (name_matcher/mod.rs), so
      `ferrosa_view::metadata` covers everything in that module;
    - a pattern that starts with an impl block matches the rightmost impl block in the name, which
      is how a derive-generated method can be named without repeating the anonymous const's
      undisplayable path (`_#9`);
    - `_` is a glob for one element, which is the only way to step over that anonymous const.
    """
    scope = elements(scope_pattern)
    artifact = elements(artifact_pattern)
    if not scope:
        return False
    if scope[0].startswith("{impl"):
        idx = max((i for i, e in enumerate(artifact) if e.startswith("{impl")), default=None)
        if idx is None:
            return False
        artifact = artifact[idx:]
    if len(scope) > len(artifact):
        return False
    for i, se in enumerate(scope):
        ae = artifact[i]
        if se in ("_", "*"):
            continue
        if se.startswith("{impl") or ae.startswith("{impl"):
            if not (se.startswith("{impl") and ae.startswith("{impl")):
                return False
            if not _impl_claims(se, ae):
                return False
        elif strip_generics(se) != strip_generics(ae):
            return False
    return True


def crate_root(name: str) -> str:
    return name.replace("-", "_")


def own(pattern: str | None, crate: str) -> bool:
    """Is this assumption about the crate's own code, or about a foreign item?

    A foreign declaration (`core::option::Option::map`) is the translator's business and is
    identical in every crate; a declaration under the crate's own root, or a trait impl whose
    type is the crate's, is an assumption about ferrosa and has to be explained. Guessing wrong
    in the direction of "own" costs one scope entry; guessing wrong the other way is silence.
    """
    if not pattern:
        return False
    root = crate_root(crate)
    elems = elements(pattern)
    if elems and strip_generics(elems[0]) == root:
        return True
    for elem in elems:
        if elem.startswith("{impl") and any(
                strip_generics(part).split("::")[-1] == root
                for part in re.split(r"[\s,<>]+", elem)):
            return True
    return False


def entry_problems(entry: dict, what: str) -> list[tuple[str, str]]:
    found = []
    if not str(entry.get("reason", "")).strip():
        found.append(("NO-REASON", f"{what} carries no reason"))
    if not str(entry.get("owner", "")).strip():
        found.append(("NO-OWNER", f"{what} names no owner"))
    date = str(entry.get("date", "")).strip()
    try:
        datetime.date.fromisoformat(date)
    except ValueError:
        found.append(("NO-DATE", f"{what} has no ISO date (got {date!r})"))
    return found


def read_ledger(path: Path) -> list[dict]:
    """`scope.lock`: one row per assumption an extraction actually applied.

    Columns are tab-separated because a charon name pattern can contain spaces (`&'_0 mut @T`).
    `evidence` is `in-lean` when the emitted module carries the assumption as `def … := sorry`,
    and `not-emitted` when the translator drops the declaration -- which it does for an opaque
    trait-impl method that nothing reachable calls, so no Lean declaration can exist. The row is
    then the only record, which is exactly why it is generated from the tools' output rather than
    typed by hand, and why `check` refuses a row the scope file does not explain.
    """
    rows = []
    try:
        text = path.read_text()
    except FileNotFoundError:
        return rows
    for lineno, line in enumerate(text.splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 5:
            fail(f"{path}:{lineno}: expected 5 tab-separated columns "
                 f"(module, crate, pattern, evidence, date), got {len(parts)}: {line!r}")
        module, crate, pattern, evidence, date = (p.strip() for p in parts)
        if evidence not in ("in-lean", "not-emitted"):
            fail(f"{path}:{lineno}: evidence must be `in-lean` or `not-emitted`, got {evidence!r}")
        rows.append({"module": module, "crate": crate, "pattern": pattern,
                     "evidence": evidence, "date": date})
    return rows


def write_ledger(path: Path, rows: list[dict]) -> None:
    order = {"not-emitted": 0, "in-lean": 1}
    rows = sorted(rows, key=lambda r: (r["module"], order.get(r["evidence"], 2), r["pattern"]))
    body = "# module\tcrate\tpattern\tevidence\tdate\n"
    body += "".join(f"{r['module']}\t{r['crate']}\t{r['pattern']}\t{r['evidence']}\t{r['date']}\n"
                    for r in rows)
    path.write_text(body)


def llbc_items(llbc: Path, charon: str) -> list[str]:
    """Every item name a charon llbc carries, as charon's own pattern printing writes it.

    `charon pretty-print` emits a `// Full name: …` line above each declaration. Reading that is
    how `record` can tell a scope entry that matched something from one that matched nothing --
    charon itself says nothing for an `--opaque` pattern that matches no item (measured: exit 0,
    no warning), so a rotted entry would otherwise exclude nothing and stay silent.
    """
    try:
        out = subprocess.run([charon, "pretty-print", "--format", "json", str(llbc)],
                             capture_output=True, text=True, timeout=600)
    except (OSError, subprocess.TimeoutExpired) as exc:
        fail(f"cannot read {llbc}: {exc}")
    if out.returncode != 0:
        fail(f"charon could not pretty-print {llbc}: {out.stderr.strip()[:400]}")
    return [m.group(1).strip()
            for m in re.finditer(r"^// Full name: (.+)$", out.stdout, re.MULTILINE)]


def record(root: Path, module: str, llbc: Path, charon: str, spec: Path | None = None) -> int:
    """Write this module's ledger rows from the tools' own output.

    Two questions per scope entry, both answered by an artifact and not by the person running it:
    did charon still have the item (else the entry has rotted to nothing), and did aeneas leave a
    declaration for it in the Lean (else the assumption has no Lean surface and the row says so).
    """
    spec = spec or (root / "specs" / "lean4-rust")
    scope = load_scope(spec / "scope.toml")
    rows = [r for r in modules(spec) if r["module"] == module]
    if not rows:
        fail(f"{module} is not in modules.txt")
    crate = rows[0]["crate"]
    patterns = [str(i["pattern"]).strip() for i in scope["items"] if i.get("crate") == crate]
    lean = spec / "FerrosaRust" / f"{module}.lean"
    if not lean.is_file():
        fail(f"no {lean} to record against")
    assumptions = [d for d in lean_decls(lean.read_text()) if d["sorry"] and d["pattern"]]

    names = llbc_items(llbc, charon)
    problems = 0
    new_rows = []
    for pattern in patterns:
        if not any(claims(pattern, name) for name in names):
            # The entry matched nothing in the llbc, so charon made nothing opaque for it. Rot.
            print(f"STALE          {crate}: {pattern} matched no item in {llbc.name}: the entry "
                  "excludes an item that no longer exists")
            problems += 1
            continue
        visible = any(claims(pattern, d["pattern"]) for d in assumptions)
        new_rows.append({"module": module, "crate": crate, "pattern": pattern,
                         "evidence": "in-lean" if visible else "not-emitted",
                         "date": datetime.date.today().isoformat()})
    if problems:
        return 1

    lock = spec / "scope.lock"
    kept = [r for r in read_ledger(lock) if r["module"] != module]
    write_ledger(lock, kept + new_rows)
    visible = sum(1 for r in new_rows if r["evidence"] == "in-lean")
    print(f"recorded {len(new_rows)} assumption(s) for {module}: {visible} visible in the Lean, "
          f"{len(new_rows) - visible} only in scope.lock")
    return 0


def check(root: Path, spec: Path | None = None, only_module: str | None = None) -> tuple[list, dict]:
    spec = spec or (root / "specs" / "lean4-rust")
    scope = load_scope(spec / "scope.toml")
    found: list[tuple[str, str]] = []

    crates = [c for c in scope["crates"]]
    items = [i for i in scope["items"]]
    blanket = scope["blanket"]

    # --- the scope file itself -------------------------------------------------------------
    if scope["schema"] != 1:
        found.append(("NO-SCHEMA", f"scope.toml declares schema {scope['schema']!r}, expected 1"))

    for crate in crates:
        name = str(crate.get("name", "")).strip()
        kind = crate.get("kind")
        if not name:
            found.append(("NO-SCHEMA", "a [[crate]] entry has no name"))
            continue
        if "*" in name or "?" in name:
            found.append(("BLANKET", f"crate {name!r} is a wildcard: the only blanket "
                                     "exclusion allowed is [blanket]"))
        if kind not in CRATE_KINDS:
            found.append(("NO-SCHEMA", f"crate {name} has kind {kind!r}, expected one of "
                                       f"{', '.join(CRATE_KINDS)}"))
        found += entry_problems(crate, f"crate {name}")

    if not isinstance(blanket, dict):
        found.append(("BLANKET", "no [blanket] entry: nothing says what happens to a dependency "
                                 "that is not named as critical"))
    else:
        found += entry_problems(blanket, "the [blanket] exclusion")

    included = {c["name"] for c in crates if c.get("kind") in CRATE_KINDS}
    for item in items:
        crate = str(item.get("crate", "")).strip()
        pattern = str(item.get("pattern", "")).strip()
        if not crate or not pattern:
            found.append(("NO-SCHEMA", "an [[item]] entry needs both `crate` and `pattern`"))
            continue
        if item.get("kind") not in ITEM_KINDS:
            found.append(("NOT-VISIBLE", f"{crate}: item {pattern} asks for kind "
                                         f"{item.get('kind')!r}; only `opaque` leaves a declaration "
                                         "in the Lean, and an exclusion nobody can see is the "
                                         "failure this file exists to prevent"))
        if crate not in included:
            found.append(("NO-SCHEMA", f"item {pattern} is scoped in {crate}, which is not an "
                                       "included crate"))
        found += entry_problems(item, f"{crate}: item {pattern}")

    # --- the workspace: every ferrosa crate has a decision ---------------------------------
    members = workspace_members(root)
    named = {c["name"]: c for c in crates if c.get("name")}
    for name in sorted(members):
        if name not in named:
            found.append(("UNSCOPED", f"{name} is a workspace crate with no [[crate]] entry: "
                                      "neither included nor explicitly excluded"))
        elif named[name].get("kind") != "own":
            found.append(("NO-SCHEMA", f"{name} is a workspace crate but is scoped as "
                                       f"{named[name].get('kind')!r}, not 'own'"))
    locked = lock_packages(root)
    for crate in crates:
        name, kind = crate.get("name"), crate.get("kind")
        if kind == "own" and name not in members:
            found.append(("STALE-CRATE", f"{name} is listed as an own crate but is not a "
                                         "workspace member"))
        if kind == "critical":
            if name in members:
                found.append(("NO-SCHEMA", f"{name} is a workspace member, so it is `own`, "
                                           "not `critical`"))
            elif locked and name not in locked:
                found.append(("STALE-CRATE", f"{name} is listed as a critical dependency but no "
                                             "package of that name is in Cargo.lock"))

    # --- the evidence: what the emitted Lean actually assumes ------------------------------
    by_module = {}
    for row in modules(spec):
        if only_module and row["module"] != only_module:
            continue
        if row["crate"] not in included:
            found.append(("UNSCOPED-CRATE", f"{row['module']}: modules.txt extracts from "
                                            f"{row['crate']}, which scope.toml does not include"))
        lean = spec / "FerrosaRust" / f"{row['module']}.lean"
        if not lean.is_file():
            continue
        decls = lean_decls(lean.read_text())
        assum = [d for d in decls if d["sorry"] and d["pattern"]]
        by_module[row["module"]] = {"crate": row["crate"], "assumptions": assum}

    item_by_crate = {}
    for item in items:
        item_by_crate.setdefault(str(item.get("crate", "")), []).append(str(item.get("pattern", "")))

    # The ledger is what turns "the translator dropped the declaration" from silence into a
    # record. It is written by `record` from charon's llbc and the emitted Lean, never by hand.
    ledger = [r for r in read_ledger(spec / "scope.lock")
              if not only_module or r["module"] == only_module]

    explained = 0
    for module, info in sorted(by_module.items()):
        for decl in info["assumptions"]:
            if not own(decl["pattern"], info["crate"]):
                continue
            matching = [p for p in item_by_crate.get(info["crate"], [])
                        if claims(p, decl["pattern"])]
            if not matching:
                found.append(("SILENT", f"{module}: {decl['pattern']} is opaque in the Lean "
                                        f"(def {decl['name']} := sorry) and no scope entry "
                                        "explains it"))
                continue

    for crate, patterns in sorted(item_by_crate.items()):
        for pattern in patterns:
            ledger_rows = [r for r in ledger
                           if r["crate"] == crate and claims(pattern, r["pattern"])]
            in_lean = any(claims(pattern, d["pattern"])
                          for info in by_module.values() if info["crate"] == crate
                          for d in info["assumptions"])
            if in_lean:
                explained += 1
                if not ledger_rows:
                    # Visible in the Lean but not in the ledger the extraction wrote, so the
                    # ledger is out of date with the module it claims to describe.
                    found.append(("LEDGER", f"{crate}: {pattern} is visible in the Lean but has "
                                            f"no scope.lock row; re-record it"))
                elif all(r["evidence"] != "in-lean" for r in ledger_rows):
                    found.append(("LEDGER", f"{crate}: {pattern} is visible in the Lean but "
                                            "scope.lock says it was not emitted"))
                continue
            if not ledger_rows:
                found.append(("STALE", f"{crate}: {pattern} matches nothing: no generated module "
                                       "carries it and scope.lock has no row for it"))
                continue
            # No Lean declaration, so the row is the record -- and it may only be a row for a
            # module whose extraction actually ran, or it is a claim with no artifact behind it.
            explained += 1
            for row in ledger_rows:
                if row["evidence"] == "in-lean":
                    found.append(("LEDGER", f"{row['module']}: scope.lock says {crate}: "
                                            f"{pattern} was emitted into the Lean, and no "
                                            "declaration carrying it is there"))
                elif row["module"] not in by_module:
                    found.append(("LEDGER", f"{row['module']}: scope.lock records an assumption "
                                            f"for a module with no generated file "
                                            f"(FerrosaRust/{row['module']}.lean)"))

    # One row per entry: a ledger that explains the same assumption twice is two records of one
    # fact, and the count no longer means anything.
    for crate, patterns in sorted(item_by_crate.items()):
        for pattern in patterns:
            rows = [r for r in ledger if r["crate"] == crate and claims(pattern, r["pattern"])]
            if len(rows) > 1:
                found.append(("LEDGER", f"{crate}: {pattern} has {len(rows)} scope.lock rows "
                                        f"({', '.join(r['module'] for r in rows)}); one per entry"))
    for row in ledger:
        if not any(claims(p, row["pattern"]) for p in item_by_crate.get(row["crate"], [])):
            found.append(("SILENT", f"{row['module']}: scope.lock records {row['pattern']} as "
                                    "excluded and no scope entry explains it"))

    summary = {
        "own": sum(1 for c in crates if c.get("kind") == "own"),
        "critical": sum(1 for c in crates if c.get("kind") == "critical"),
        "items": len(items),
        "assumptions": explained,
        "blanket": isinstance(blanket, dict),
    }
    return found, summary


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p_check = sub.add_parser("check", help="compare the scope with the workspace and the Lean")
    p_check.add_argument("--root", type=Path, default=REPO)
    p_check.add_argument("--module", default=None,
                         help="only this module's rows (used by extract.sh after a regeneration)")
    p_check.add_argument("--quiet", action="store_true")
    p_items = sub.add_parser("items", help="print one crate's charon patterns, for --opaque")
    p_items.add_argument("crate")
    p_items.add_argument("--root", type=Path, default=REPO)
    p_included = sub.add_parser("included", help="is this crate in the extraction scope?")
    p_included.add_argument("crate")
    p_included.add_argument("--root", type=Path, default=REPO)
    p_record = sub.add_parser("record", help="write a module's scope.lock rows from the artifacts")
    p_record.add_argument("module")
    p_record.add_argument("--llbc", type=Path, required=True)
    p_record.add_argument("--charon", default="charon")
    p_record.add_argument("--root", type=Path, default=REPO)
    args = parser.parse_args(argv)

    if args.command == "record":
        return record(args.root, args.module, args.llbc, args.charon)

    if args.command == "included":
        scope = load_scope(args.root / "specs" / "lean4-rust" / "scope.toml")
        ok = any(c.get("name") == args.crate and c.get("kind") in CRATE_KINDS
                 for c in scope["crates"])
        if not ok:
            print(f"{args.crate}: no [[crate]] entry in scope.toml", file=sys.stderr)
        return 0 if ok else 1

    if args.command == "items":
        scope = load_scope(args.root / "specs" / "lean4-rust" / "scope.toml")
        for item in scope["items"]:
            if item.get("crate") == args.crate:
                print(item["pattern"])
        return 0

    found, summary = check(args.root, only_module=args.module)
    for code, detail in found:
        print(f"{code:<14} {detail}")
    if found:
        print(f"\n{len(found)} problem(s) in the extraction scope (specs/lean4-rust/scope.toml)")
        return 1
    if not args.quiet:
        critical = summary["critical"]
        items = summary["items"]
        print(f"OK {summary['own']} ferrosa crate(s) and {critical} critical "
              f"{'dependency' if critical == 1 else 'dependencies'} in scope, "
              f"{summary['assumptions']} assumption(s) explained by "
              f"{items} item {'entry' if items == 1 else 'entries'}, "
              f"one blanket exclusion ({'present' if summary['blanket'] else 'MISSING'})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
