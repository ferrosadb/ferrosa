#!/usr/bin/env python3
"""Join the corpus claims to what has actually been extracted from the Rust.

The corpus in `specs/lean4` states its claims about hand-written models, so a green theorem there
says nothing about the code. `specs/lean4-rust` holds Lean that Charon and Aeneas produced from the
source, so a claim whose subject appears there *could* be discharged by a refinement rather than by
a model.

A broken row names its site in prose (`"commitlog/mutation.rs: a zero mutation_id …"`,
`"read_partitions_limited returns Vec<Partition> …"`), so the anchor is found rather than given: the
identifiers and file names in the text are looked up in the Rust tree, which yields the file that
implements the claim, and that file is placed relative to the extraction.

This report answers three questions and writes the answer to `specs/lean/gaps/linkage.md`:

  1. Which claims name a place in the Rust at all, and where does that place sit relative to the
     extraction -- extracted, in a crate that was measured, in a crate that crashed, or nowhere?
  2. What does the extraction contain, and how much of it is claimed by a ledger row?
  3. Which of the claims that *could* be tied are still only model theorems?

Nothing here proves anything. It is a map of where the two corpora overlap and where they do not,
so the work of tying them can be scheduled against a count instead of an impression.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAN4 = ROOT / "specs/lean4"
EXTRACT = ROOT / "specs/lean4-rust"
GAPS = ROOT / "specs/lean/gaps"

STATUS = re.compile(r'⟨\s*"((?:[^"\\]|\\.)*)"\s*,\s*\.(met|failing)\b')
LEDGER_BLOCK = re.compile(r"def ledger\s*:\s*List SpecItem\s*:=\s*\[(.*?)\n\]", re.S)
QUOTED = re.compile(r'"((?:[^"\\]|\\.)*)"')
SOURCE_SPAN = re.compile(r"Source: '([^']+)', lines (\d+):\d+-(\d+):\d+")
DECL = re.compile(r"^(?:@\[[^\]]*\]\s*)?(?:noncomputable\s+)?(?:def|structure|axiom)\s+([\w.]+)", re.M)
RUST_DEF = re.compile(r"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?"
                      r"(?:fn|const|static|struct|enum|union|trait|type|mod)\s+([A-Za-z_]\w*)")
IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]{3,}")


def rows_of(path: Path, block_name: str) -> list[list[str]]:
    """The quoted strings of each `⟨...⟩` row of a list, grouped by row."""
    text = path.read_text()
    m = re.search(rf"def {block_name}\s*:\s*List \w+\s*:=\s*\[(.*?)\n\]", text, re.S)
    if not m:
        return []
    # Rows are ⟨a, b, c⟩ and strings only appear inside a row, so splitting on the constructor is
    # enough. Splitting on the quoted strings and guessing where a row ends put all fifteen broken
    # rows into one.
    return [QUOTED.findall(chunk.split("⟩")[0]) for chunk in m.group(1).split("⟨")[1:]]


def ledger_items() -> list[dict]:
    out = []
    for path in sorted(LEAN4.rglob("*.lean")):
        m = LEDGER_BLOCK.search(path.read_text())
        if not m:
            continue
        module = "Ferrosa." + str(path.relative_to(LEAN4)).replace("/", ".").removesuffix(".lean")
        body = m.group(1)
        for hit in STATUS.finditer(body):
            name, status = hit.group(1), hit.group(2)
            proof = None
            if status == "met":
                q = QUOTED.search(body[hit.end():hit.end() + 400].split("⟩")[0])
                proof = q.group(1) if q else None
            out.append({"ledger": module, "item": name, "status": status, "proof": proof})
    return out


def rust_index() -> dict[str, set[str]]:
    """identifier -> the files that define it, for every crate in the workspace."""
    index: dict[str, set[str]] = {}
    for path in ROOT.glob("*/src/**/*.rs"):
        crate = path.relative_to(ROOT).parts[0]
        rel = "/".join(path.relative_to(ROOT / crate).parts)
        for ident in RUST_DEF.findall(path.read_text(errors="replace")):
            index.setdefault(ident, set()).add(f"{crate}/{rel}")
    return index


def extracted_symbols() -> list[dict]:
    out = []
    for path in sorted((EXTRACT / "FerrosaRust").glob("*.lean")):
        text = path.read_text()
        decls = [(m.start(), m.group(1)) for m in DECL.finditer(text)]
        for m in SOURCE_SPAN.finditer(text):
            before = [d for start, d in decls if start < m.start()]
            out.append({"module": path.stem, "symbol": before[-1] if before else "?",
                        "file": m.group(1).lstrip("/"),
                        "start": int(m.group(2)), "end": int(m.group(3))})
    return out


def extraction_state() -> dict:
    state = json.loads((GAPS / "state.json").read_text())
    return {n: (row.get("measurement") or {}) for n, row in state.get("crates", {}).items()}


def sync_rows() -> list[dict]:
    out = []
    lock = EXTRACT / "sync.lock"
    if not lock.is_file():
        return out
    for line in lock.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        f = line.split()
        out.append({"module": f[0], "crate": f[1], "sources": f[3],
                    "status": " ".join(f[9:]) or "ok"})
    return out


GENERIC = {"mod", "lib", "main", "new", "from", "into", "run", "test", "value", "error",
           "write", "read", "open", "close", "send", "recv", "start", "stop", "load", "store"}


def resolve_rs(name: str) -> list[str]:
    """`commitlog/mutation.rs` -> crate-relative paths such as `ferrosa-storage/src/commitlog/mutation.rs`.

    The crate name has to survive: `classify` reads it off the first path segment, and a path that
    started at `src/` made every such row look like a crate called `src`, i.e. unmeasured.
    """
    out = []
    for crate_dir in ROOT.glob("*/src"):
        crate = crate_dir.parent.name
        for hit in crate_dir.rglob(Path(name).name):
            rel = hit.relative_to(crate_dir)
            if str(rel).endswith(name) or name == hit.name:
                out.append(f"{crate}/{crate_dir.name}/{rel}")
    return sorted(set(out))


def anchors_of(site: str, index: dict[str, set[str]]) -> tuple[list[str], list[str]]:
    """(explicit files, identifier files). Explicit wins; identifiers must be distinctive.

    A prose site is not a citation, so the rule is: an identifier counts only if it is long and is
    defined in at most three files. Without that, `mod`, `router` and `convert` matched half the
    workspace and every row looked anchored to a file that had nothing to do with it.
    """
    explicit = []
    for m in re.finditer(r"([\w./-]+\.rs)", site):
        explicit += resolve_rs(m.group(1))
    idents = []
    for tok in set(IDENT.findall(site)):
        if tok.lower() in GENERIC or len(tok) < 8:
            continue
        if not (("_" in tok) or tok[0].isupper() or tok.isupper()):
            continue
        files = index.get(tok, set())
        if 0 < len(files) <= 3:
            idents.append(tok)
    ident_files = sorted({f for t in idents for f in index.get(t, set())})
    return sorted(set(explicit)), ident_files


def main() -> int:
    items = ledger_items()
    broken = [r for r in rows_of(LEAN4 / "Ferrosa/Broken.lean", "broken") if len(r) >= 5]
    unimplemented = [r for r in rows_of(LEAN4 / "Ferrosa/Unimplemented.lean", "unimplemented")
                     if len(r) >= 4]
    nondefects = [r for r in rows_of(LEAN4 / "Ferrosa/Unimplemented.lean", "nonDefects")
                  if len(r) >= 5]
    symbols = extracted_symbols()
    state = extraction_state()
    synced = sync_rows()
    index = rust_index()

    extracted_sources = {f"{r['crate']}/{r['sources']}": r for r in synced
                         if r["status"].startswith("ok")}
    extracted_lean_names = {s["symbol"].split(".")[-1] for s in symbols}

    def classify(paths: list[str]) -> str:
        """Distance from the extraction of the best anchor: extracted beats everything."""
        if not paths:
            return "no-code-anchor"
        classes = []
        for path in paths:
            crate = path.split("/")[0]
            if any(path.startswith(src) for src in extracted_sources):
                classes.append("extracted")
                continue
            stage = (state.get(crate) or {}).get("stage")
            classes.append("crate-unmeasured" if stage is None
                           else f"crate-{stage}" if stage != "aeneas"
                           else "crate-measured-file-not-extracted")
        for best in ("extracted", "crate-measured-file-not-extracted"):
            if best in classes:
                return best
        return classes[0]

    gaps = []
    for row in broken:
        area, what, site, ledger, item = row[:5]
        explicit, ident_files = anchors_of(site, index)
        files = explicit or ident_files
        tokens = set(IDENT.findall(site))
        # "the file is extracted" and "the function the claim is about is extracted" are different
        # facts, and only the second makes a refinement possible. The compression row names
        # `decompress`; the extracted module has the types and helpers of that file and not that
        # function, which is exactly the gap a file-level join would hide.
        named = sorted({s["symbol"] for s in symbols if s["symbol"].split(".")[-1] in tokens})
        in_file = sorted({s["symbol"] for s in symbols
                          if any(s["file"].endswith(f.split("/", 1)[-1]) for f in files)})
        gaps.append({
            "area": area, "what": what, "ledger": ledger, "item": item, "site": site,
            "anchors": files,
            "confidence": "explicit" if explicit else ("ident" if ident_files else "none"),
            "symbol_named_in_site": named,
            "symbols_from_same_file": in_file,
            "class": classify(files),
        })

    classes: dict[str, int] = {}
    for g in gaps:
        classes[g["class"]] = classes.get(g["class"], 0) + 1

    claimed = {a for g in gaps for a in g["anchors"]}
    overlap = sorted(claimed & set(extracted_sources))

    lines = [
        "# Where the corpus and the extraction overlap",
        "",
        f"Ledger items: **{len(items)}** ({sum(1 for i in items if i['status'] == 'met')} met, "
        f"{sum(1 for i in items if i['status'] == 'failing')} failing). "
        f"Classified reds: {len(broken)} broken, {len(unimplemented)} with no code path, "
        f"{len(nondefects)} not defects.",
        "",
        "## The extracted surface",
        "",
        "| module | crate | sources | status | translated symbols |",
        "|---|---|---|---|---|",
    ]
    for r in synced:
        n = sum(1 for s in symbols if s["module"] == r["module"])
        lines.append(f"| {r['module']} | {r['crate']} | {r['sources']} | "
                     f"{r['status'].split(':')[0]} | {n} |")
    lines += [
        "",
        f"Files under extraction: {', '.join(f'`{s}`' for s in sorted(extracted_sources)) or 'none'}",
        f"Files named by a broken row: {len(claimed)} — overlap: **{len(overlap)}**",
        "",
        "## The broken rows, by distance from the extraction",
        "",
        "| class | count |",
        "|---|---|",
    ]
    for cls, n in sorted(classes.items(), key=lambda kv: -kv[1]):
        lines.append(f"| {cls} | {n} |")

    lines += ["", "| area | what | anchor(s) | how | named symbol extracted? | class |",
              "|---|---|---|---|---|---|"]
    for g in gaps:
        anchors = ", ".join(f"`{a}`" for a in g["anchors"][:2]) or "—"
        named = ", ".join(f"`{n}`" for n in g["symbol_named_in_site"][:2]) or "**no**"
        lines.append(f"| {g['area']} | {g['what'][:58]} | {anchors} | {g['confidence']} | "
                     f"{named} | {g['class']} |")

    lines += [
        "",
        "## What this says",
        "",
        "- `no-code-anchor`: the row's text names nothing that exists in the Rust, so there is no",
        "  extraction it could be tied to — the claim is about behaviour, not about a site.",
        "- `crate-aeneas-crash` / `crate-oom` / `crate-unmeasured`: the file is real and the blocker is",
        "  the translator, not the work of stating a refinement.",
        "- `crate-measured-file-not-extracted`: the crate translates; the file only needs a scope in",
        "  `specs/lean4-rust/modules.txt`.",
        "- `extracted`: the code is in the extraction today, so a refinement is possible now. These are",
        "  the only rows where the corpus can stop being a statement about a model.",
        "",
    ]

    GAPS.mkdir(parents=True, exist_ok=True)
    (GAPS / "linkage.md").write_text("\n".join(lines) + "\n")
    (GAPS / "linkage.json").write_text(json.dumps({
        "ledger_items": len(items), "broken": gaps, "classes": classes,
        "extracted_files": sorted(extracted_sources), "overlap": overlap,
        "symbols": len(symbols), "sync": synced,
    }, indent=2))

    print(f"ledger items {len(items)} | broken {len(broken)} | translated symbols {len(symbols)}")
    for cls, n in sorted(classes.items(), key=lambda kv: -kv[1]):
        print(f"  {cls}: {n}")
    print(f"overlap (broken anchor ∩ extracted file): {overlap or 'none'}")
    print("wrote specs/lean/gaps/linkage.md, linkage.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
