"""Generate the theorem register pages from the corpus's own export.

The register lists every theorem in the specification, part by part in the book's order, with a link
to the theorem on its module page. It is generated, never edited: `make register` rewrites it from
`../build/specs.json` (written by `lake exe docs` in `specs/lean4`), and `make check` fails when the
committed register differs from a fresh one, so a theorem added to the corpus cannot be missing here.

A theorem is marked red when a ledger cites it as the witness of an unmet claim, or when its
docstring opens with "Red". One whose docstring opens with "Counterexample" is marked as one and is
not counted red. Everything else is a guarantee.

Usage: python3 gen_register.py <specs.json> <output dir>
"""

import json
import pathlib
import re
import sys

ROMAN_TO_INDEX = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7}


def anchor(full: str) -> str:
    """The id Verso's literate renderer gives a declaration: every character outside
    `[A-Za-z0-9_]`, the namespace dots included, becomes three underscores."""
    return re.sub(r"[^A-Za-z0-9_]", "___", full)


def page(module: str) -> str:
    return module.replace(".", "/") + "/"


def first_sentence(doc: str) -> str:
    text = " ".join(doc.split())
    text = text.replace("**", "")
    match = re.search(r"(.+?[.!?])(\s|$)", text)
    sentence = match.group(1) if match else text
    if len(sentence) > 220:
        sentence = sentence[:217].rstrip() + "…"
    return sentence


def red_witnesses(areas: list) -> set:
    """Suffixes of every theorem a ledger cites as the witness of an unmet claim."""
    witnesses = set()
    for area in areas:
        for item in area["items"]:
            if item.get("kind") != "met" and item.get("witness"):
                witnesses.add(item["witness"].split(" ")[0])
    return witnesses


def is_red(full: str, doc: str, witnesses: set) -> bool:
    if doc.lstrip().lstrip("*").startswith("Red"):
        return True
    return any(full == w or full.endswith("." + w) for w in witnesses)


def is_counterexample(doc: str) -> bool:
    """A theorem showing what goes wrong in a design the code does not use, or a history the
    checker must reject. It is evidence for a guarantee, not a defect, so it is never counted red."""
    return doc.lstrip().lstrip("*").startswith("Counterexample")


def theorems_of(module: dict) -> list:
    return [e for e in module["events"] if e.get("k") == "decl" and e.get("kind") == "theorem"]


def render_part(part: dict, modules: dict, witnesses: set) -> tuple:
    index = ROMAN_TO_INDEX[part["numeral"]]
    lines = [f"/-!\n# Register {part['numeral']}: {part['title']}\n"]
    lines.append(part["intro"] + "\n")
    lines.append(
        "Tick a theorem once you have reviewed its statement and docstring on its module page. "
        "Ticks are kept in this browser only.\n"
    )
    total = 0
    reds = 0
    for key in part["modules"]:
        theorems = theorems_of(modules[key])
        if not theorems:
            continue
        # The heading is plain text: Verso copies a heading's markup into the page's table of
        # contents inside its own link, and a link nested in a link has no accessible name.
        lines.append(f"\n## `{key}`\n")
        lines.append(f"[The module page]({page(key)})\n")
        for t in theorems:
            red = is_red(t["full"], t.get("doc", ""), witnesses)
            total += 1
            reds += red
            link = f"[`{t['short']}`]({page(key)}#{anchor(t['full'])})"
            kind = ("**Red.** " if red
                    else "**Counterexample.** " if is_counterexample(t.get("doc", ""))
                    else "")
            lines.append(f"- {link}: {kind}{first_sentence(t.get('doc', ''))}")
    lines.append("-/\n")
    return index, "\n".join(lines), total, reds


def render_index(summaries: list) -> str:
    total = sum(s[2] for s in summaries)
    reds = sum(s[3] for s in summaries)
    rows = "\n".join(
        f"- [{s[4]}. {s[5]}](FerrosaProduct/Register/Part{s[0]}/): {s[2]} theorems, {s[3]} red"
        for s in summaries
    )
    imports = "\n".join(f"import FerrosaProduct.Register.Part{s[0]}" for s in summaries)
    return f"""{imports}

/-!
# The theorem register

Every theorem in the specification, {total} in all, in the book's order. {reds} are red: they prove that
a property the product needs does not hold yet, and each is tracked by a ledger in
`Ferrosa.Unimplemented` until the code is fixed. A theorem marked *counterexample* shows what goes
wrong in a design the code does not use, or a history the checker must reject; it is evidence for a
guarantee, not a defect. The rest are guarantees.

Each part lists its modules, and each theorem links to where it is stated and proved on its module
page. The register is generated from the compiled specification, so it cannot omit a theorem the
corpus contains.

{rows}
-/
"""


def main() -> None:
    spec = json.loads(pathlib.Path(sys.argv[1]).read_text())
    out = pathlib.Path(sys.argv[2])
    modules = {m["key"]: m for m in spec["modules"]}
    witnesses = red_witnesses(spec["areas"])
    (out / "Register").mkdir(parents=True, exist_ok=True)
    summaries = []
    for part in spec["parts"]:
        index, text, total, reds = render_part(part, modules, witnesses)
        (out / "Register" / f"Part{index}.lean").write_text(text)
        summaries.append((index, text, total, reds, part["numeral"], part["title"]))
    (out / "Register.lean").write_text(render_index(summaries))


if __name__ == "__main__":
    main()
