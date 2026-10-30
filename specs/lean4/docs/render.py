#!/usr/bin/env python3
"""Render the Ferrosa Lean 4 corpus as a PDF.

The document is assembled from three sources, in this order of authority:

  1. `build/specs.json`  — written by `lake exe docs`. Modules, sections, docstrings, and the
     obligation ledgers, read from `Unimplemented.areas`.
  2. `build/statements.lean` — generated here and elaborated by Lean. Every formal statement in the
     PDF comes out of this file, so a statement cannot differ from what the compiler holds.
  3. the LaTeX in this file — typography, and nothing else.

Prose is not invented here. It comes from the `/-!` sections and `/--` docstrings the modules carry,
which is why a paragraph added next to a proof appears in the PDF without touching this script.

    python3 docs/render.py [--out build/ferrosa-specs.pdf] [--part II]

Requires: `lake exe docs` to have run, and a XeLaTeX engine on PATH (tectonic or latexmk).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

LEAN4 = Path(__file__).resolve().parent.parent
BUILD = LEAN4 / "build"

# --------------------------------------------------------------------------------------------------
# text
# --------------------------------------------------------------------------------------------------

LATEX_SPECIALS = {
    "\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#",
    "_": r"\_", "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}",
}


def esc(s: str) -> str:
    return "".join(LATEX_SPECIALS.get(c, c) for c in s)


def breakable(code: str) -> str:
    """Give TeX somewhere to break inside an identifier.

    Lean names are long and unbreakable — `trie_cannot_produce_a_wrong_value` is 38 characters of
    solid box — and 20 overfull lines in the first build were all this. A zero-width break after
    the separators is invisible when it is not used and saves the margin when it is.
    """
    out = []
    for ch in code:
        out.append(ch)
        if ch in "_/.":
            out.append(r"\allowbreak{}")
    return "".join(out)


def inline(s: str) -> str:
    """Escape a line of prose, with `backticked` spans set in mono."""
    out, i = [], 0
    for m in re.finditer(r"`([^`]+)`", s):
        out.append(esc(s[i:m.start()]))
        out.append(r"\texttt{" + breakable(esc(m.group(1))) + "}")
        i = m.end()
    out.append(esc(s[i:]))
    return "".join(out)


def dedent(text: str) -> str:
    """Source blocks are indented to sit under their `/-`; strip the common prefix."""
    lines = text.expandtabs(2).split("\n")
    body = [l for l in lines if l.strip()]
    if not body:
        return ""
    pad = min(len(l) - len(l.lstrip()) for l in body)
    return "\n".join(l[pad:].rstrip() for l in lines).strip("\n")


def prose(text: str) -> str:
    """A block of docstring as LaTeX paragraphs."""
    text = dedent(text)
    if not text:
        return ""
    paras = re.split(r"\n\s*\n", text)
    return "\n\n".join(inline(" ".join(p.split())) for p in paras if p.strip())


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:60]


# --------------------------------------------------------------------------------------------------
# statements, from the compiler
# --------------------------------------------------------------------------------------------------

def lean_run(extra: list[str]) -> str:
    env = dict(os.environ)
    env["PATH"] = f"{Path.home()}/.elan/bin:" + env.get("PATH", "")
    r = subprocess.run(["lake", "env", "lean", *extra], cwd=LEAN4, capture_output=True, text=True, env=env)
    if r.returncode != 0:
        sys.stderr.write(r.stdout[-4000:] + r.stderr[-4000:])
        raise SystemExit(f"lean failed on {extra}")
    return r.stdout


# Three modules define `main`; importing two of them in one file makes the environment collide.
# They hold no ledger claims, and their theorems reach the document through their own modules.
MAIN_MODULES = {"Ferrosa.Harness", "Ferrosa.Ledgers", "Ferrosa.Accord.RunCheck"}


def checker_imports() -> list[str]:
    return [f"import {m}" for m in MODULES if m not in MAIN_MODULES]


def elaborate(stem: str, commands: list[str]) -> str:
    """Write a file of `commands`, elaborate it, and return stdout.

    `open` on a namespace that does not exist is a hard error, and the JSON necessarily carries
    namespaces from modules the checker cannot import (three modules define `main`). Rather than
    guess which are safe, run it and drop whatever Lean says is unknown, then run again.

    Why `#check` and not a metaprogram? Because `#check` prints what a reader sees in their editor:
    notation, not `And`/`Not`, and names shortened by whatever is open. A metaprogram in an exe has
    no ambient namespace, so it prints `Ferrosa.Topology.Pair.Readable` in full — which is the
    noise this document exists to remove.
    """
    for _ in range(4):
        body = checker_imports() + ["", ""]
        body += [f"open {ns}" for ns in OPEN_NS]
        body.append("")
        for i, cmd in enumerate(commands):
            body.append(f'#eval IO.println "\\u00a7{i}"')
            body.append(cmd)
        body.append('#eval IO.println "\\u00a7end"')
        f = BUILD / f"{stem}.lean"
        f.write_text("\n".join(body) + "\n")
        r = subprocess.run(["lake", "env", "lean", str(f)], cwd=LEAN4,
                           capture_output=True, text=True,
                           env={**os.environ, "PATH": f"{Path.home()}/.elan/bin:" + os.environ.get("PATH", "")})
        bad = set(re.findall(r"unknown namespace `([^`]+)`", r.stdout + r.stderr))
        if bad:
            OPEN_NS[:] = [ns for ns in OPEN_NS if ns not in bad]
            continue
        if r.returncode != 0:
            sys.stderr.write((r.stdout + r.stderr)[-4000:])
            raise SystemExit(f"lean failed on {f.name}")
        return r.stdout
    raise SystemExit("lean kept rejecting `open` namespaces")


def parse_sections(text: str) -> list[str]:
    parts, cur = [], []
    for line in text.split("\n"):
        if line.startswith("\u00a7"):
            parts.append("\n".join(cur).strip("\n"))
            cur = []
        else:
            cur.append(line)
    return parts[1:]  # drop the empty head before the first marker


def collect_statements(names: list[str]) -> dict[str, str]:
    out = parse_sections(elaborate("statements", [f"#check @{n}" for n in names]))
    return {n: out[i] for i, n in enumerate(names) if i < len(out)}


def collect_prints(names: list[str]) -> dict[str, str]:
    out = parse_sections(elaborate("prints", [f"#print {n}" for n in names]))
    return {n: out[i] for i, n in enumerate(names) if i < len(out)}


# --------------------------------------------------------------------------------------------------
# LaTeX
# --------------------------------------------------------------------------------------------------

PREAMBLE = r"""
\documentclass[10pt,twoside,openright]{book}
\usepackage{fontspec}
% Latin Modern ships inside the TeX bundle, so the text faces need no system font. The mono face
% does: Lean's statements are full of ⟨⟩ ∀ ∃ ∧ ⊆ ⌜⌝, and DejaVu Sans Mono is the smallest font that
% covers them. It is in `fonts-dejavu-core`, which the CI job installs explicitly.
% `Ligatures=TeX` turns a straight " into a curly one, and docstrings quote Lean strings —
% \texttt{"s is reachable"}. Common ligatures only, so the quotes stay as written.
\defaultfontfeatures{Ligatures={Common}}
\setmainfont{Liberation Serif}
\setsansfont{Liberation Sans}[Scale=0.96]
\setmonofont{DejaVu Sans Mono}[Scale=0.80]
\usepackage[a4paper,margin=25mm,headsep=5mm,footskip=10mm]{geometry}
\usepackage{xcolor}
% One glyph in the corpus (`⊆`, in a docstring) is missing from Liberation Serif; give the maths
% symbols a face that has them rather than dropping the character on the floor.
\newfontfamily\symfont{DejaVu Sans}[Scale=0.97]
\usepackage{newunicodechar}
\newunicodechar{⊆}{{\symfont ⊆}}
\newunicodechar{⊂}{{\symfont ⊂}}
\newunicodechar{⊨}{{\symfont ⊨}}
\newunicodechar{⊤}{{\symfont ⊤}}
\newunicodechar{⊥}{{\symfont ⊥}}
\usepackage{fvextra}
\usepackage{booktabs}
\usepackage{longtable}
\usepackage{array}
\usepackage{enumitem}
\usepackage{titlesec}
\usepackage{microtype}
\usepackage[hidelinks]{hyperref}

\definecolor{ink}{HTML}{1A1A1A}
\definecolor{quiet}{HTML}{6B6B6B}
\definecolor{rule}{HTML}{C9C9C9}
\definecolor{passed}{HTML}{1B5E20}
\definecolor{red}{HTML}{8C1D18}
\definecolor{accent}{HTML}{1F3A5F}

\color{ink}
% Long identifiers that still will not fit: let TeX stretch the line a little rather than run into
% the margin. Three ems is enough for the worst of them and invisible elsewhere.
\setlength{\emergencystretch}{3em}
\setlength{\parindent}{0pt}
\setlength{\parskip}{5pt}
\linespread{1.04}

\titleformat{\part}[display]
  {\normalfont\sffamily\bfseries\color{accent}\centering}
  {\Large Part \thepart}{6pt}{\Huge}
\titleformat{\chapter}[hang]
  {\normalfont\sffamily\bfseries\color{accent}\raggedright}
  {\large\thechapter}{10pt}{\Large\raggedright}
\titlespacing*{\chapter}{0pt}{-10pt}{14pt}

\DefineVerbatimEnvironment{Statement}{Verbatim}
  {frame=single,rulecolor=\color{rule},framerule=0.35pt,framesep=5pt,
   fontsize=\footnotesize,xleftmargin=1pt,breaklines=true,
   breaksymbolleft={},breaksymbolright={},breakindent=10pt}

\newcommand{\statuspass}{\textsf{\footnotesize\bfseries\color{passed}PASS}}
\newcommand{\statusred}{\textsf{\footnotesize\bfseries\color{red}RED}}
\newcommand{\src}[1]{\hfill{\scriptsize\color{quiet}\texttt{#1}}}

\newcommand{\claim}[3]{%
  \par\addvspace{9pt}%
  \noindent #1\quad{\bfseries #2}\par
  \nopagebreak\vspace{2pt}\noindent{\footnotesize\color{quiet}#3}\par\nopagebreak\vspace{3pt}}
\newcommand{\redwhy}[1]{%
  \par\vspace{2pt}\noindent{\footnotesize\color{red}why:}~{\footnotesize #1}\par}
% No \definition macro: verbatim material cannot be passed through a macro argument, so the
% definition blocks are written out in Python instead.
\newcommand{\tightlist}{\setlength{\itemsep}{0pt}\setlength{\parskip}{0pt}}
"""


def titlepage(d: dict) -> str:
    t = d["totals"]
    return rf"""
\begin{{titlepage}}
\centering
\vspace*{{38mm}}
{{\sffamily\bfseries\fontsize{{34}}{{40}}\selectfont\color{{accent}}The Ferrosa\\[2pt] Formal Specification\par}}
\vspace{{10mm}}
{{\large\itshape A machine-checked account of the storage path, the query surface,{chr(10)}replication, and what is still owed\par}}
\vspace{{26mm}}
{{\small\sffamily\color{{quiet}} {t['modules']} modules \quad·\quad {t['items']} claims \quad·\quad {t['red']} red \quad·\quad {t['theorems']} declarations\par}}
\vfill
{{\footnotesize\sffamily\color{{quiet}}Lean 4, core only, no Mathlib. Statements printed by the elaborator.\par
Generated from \texttt{{specs/lean4}} — do not edit this PDF.\par}}
\end{{titlepage}}
"""


PRIMER = r"""
\frontmatter
\chapter*{How to read a statement}
\addcontentsline{toc}{chapter}{How to read a statement}
\markboth{How to read a statement}{How to read a statement}

Every formal claim in this book appears twice: once as a sentence, which is how the corpus records
what it owes, and once as a Lean statement, which is what is actually proved. The two are written by
different people at different times, and the point of printing both is that you can check they agree.

A statement is read like a mathematical one, with four symbols worth knowing:

\begin{Statement}
theorem degraded_pair_is_read_only :
  Readable { r0 := Role.primary, r1 := Role.secondary, up0 := false, up1 := true } 1 ∧
  ¬Writable { r0 := Role.primary, r1 := Role.secondary, up0 := false, up1 := true } 1
\end{Statement}

\texttt{\{ r0 := \ldots\ \}} is a record: the fields are \emph{named}, so the state above is a pair
whose node 0 is the primary and is down, and whose node 1 is the secondary and is up.
\texttt{∧} is ``and'', \texttt{¬} is ``not'', \texttt{→} is ``implies'', \texttt{∀} is ``for all'',
and \texttt{:=} introduces a value. Anything after \texttt{:= by} is a proof and is not printed here:
the document shows what is claimed, not how it was discharged.

A claim marked \statuspass\ names the theorem that discharges it. A claim marked \statusred\ names the
theorem that \emph{exhibits the gap} — the counterexample, or the fact that pins the missing piece —
together with a sentence saying what is missing. Reds are counted, and the count is pinned by a
theorem, so this document cannot quietly disagree with the corpus about how much is unfinished.

Where a statement leans on a definition that is not obvious, the definition is printed underneath it,
once per chapter. That is the audit: you should not have to leave the page to see what a word means.
"""


def part_opener(p: dict) -> str:
    r"""One page per part: the numeral, the title, and the paragraph that says what the part is for.

    Not `\part`: that prints a page of its own and the intro would land on the next one, which is
    two nearly-blank pages in a row.
    """
    mods = esc(", ".join(m.replace("Ferrosa.", "") for m in p["modules"]))
    return rf"""
\cleardoublepage
\thispagestyle{{empty}}
\phantomsection\addcontentsline{{toc}}{{part}}{{Part {p['numeral']}\quad {esc(p['title'])}}}
\vspace*{{42mm}}
{{\sffamily\bfseries\color{{accent}}\fontsize{{22}}{{26}}\selectfont Part {p['numeral']}\par}}
\vspace{{4mm}}
{{\sffamily\bfseries\color{{accent}}\fontsize{{34}}{{38}}\selectfont {esc(p['title'])}\par}}
\vspace{{9mm}}
\noindent{{\rule{{\textwidth}}{{0.6pt}}}}\par
\vspace{{7mm}}
{{\setlength{{\parskip}}{{7pt}} {prose(p['intro'])}
}}
\vspace{{8mm}}
\noindent{{\footnotesize\color{{quiet}}\sffamily {len(p['modules'])} modules: {mods}.}}
"""


def chapter_head(mod: dict) -> str:
    key = mod["key"]
    short = key.replace("Ferrosa.", "")
    title = short.replace(".", " · ")
    out = [rf"\chapter{{{esc(title)}}}", rf"\label{{ch:{slug(key)}}}"]
    header = dedent(mod["header"])
    if header:
        # The header essay opens with a title and an ASCII ruler. The title becomes the chapter
        # title, so drop the first line; every line that is nothing but rule characters goes too,
        # wherever it sits — a ruler left in is a single unbreakable 320pt line under the chapter
        # heading.
        lines = [l for l in header.split("\n") if not re.fullmatch(r"\s*[=\-—~_*·]{4,}\s*", l)]
        if lines and lines[0].strip():
            lines = lines[1:]
        out.append(rf"\noindent{{\sffamily\color{{quiet}}\footnotesize\texttt{{{esc(key)}}}}}\par\vspace{{4pt}}")
        body = prose("\n".join(lines))
        if body:
            out.append(rf"\begin{{quote}}\small {body}\end{{quote}}")
    return "\n".join(out)


def claims_table(claims: list[dict]) -> str:
    if not claims:
        return ""
    rows = []
    for c in claims:
        mark = r"\statuspass" if c["kind"] == "met" else r"\statusred"
        rows.append(rf"{mark} & {inline(c['claim'])} & \pageref{{cl:{slug(c['claim'])}}} \\")
    return (
        r"\par\addvspace{8pt}\noindent{\sffamily\bfseries\footnotesize\color{accent}Claims in this chapter}"
        "\n\\par\\vspace{2pt}\n"
        r"{\footnotesize\begin{longtable}{@{}p{12mm}p{0.72\textwidth}@{\hfill}r@{}}" + "\n"
        + "\n".join(rows) + "\n"
        + r"\end{longtable}}"
    )


def claim_block(c: dict, thm: str, stmt: str, defs: list[tuple[str, str]], src: str) -> str:
    mark = r"\statuspass" if c["kind"] == "met" else r"\statusred"
    if c["kind"] == "met":
        prov = f"proved by \\texttt{{{breakable(esc(c['proof']))}}}"
    else:
        prov = f"witness: \\texttt{{{breakable(esc(c['witness']))}}}"
    out = [rf"\phantomsection\label{{cl:{slug(c['claim'])}}}",
           rf"\claim{{{mark}}}{{{inline(c['claim'])}}}{{{prov}}}"]
    if c["kind"] == "failing":
        out.append(rf"\redwhy{{{inline(c['why'])}}}")
    if stmt:
        out.append("\\begin{Statement}\n" + stmt + "\n\\end{Statement}")
    out.extend(def_block(name, body) for name, body in defs)
    if src:
        out.append(rf"\src{{{esc(src)}}}")
    return "\n".join(out)


def def_block(name: str, body: str) -> str:
    """A definition, expanded. Written out rather than macro'd: `Statement` is verbatim, and
    verbatim cannot travel through a macro argument."""
    head = (
        r"\par\addvspace{7pt}\noindent"
        "{\\footnotesize\\sffamily\\bfseries\\color{accent}" + esc(name) + r"}\par"
    )
    return head + "\n" + r"\nopagebreak\begin{Statement}" + "\n" + body + "\n" + r"\end{Statement}"


HEADINGS = ("\\section{", "\\subsection{", "\\subsubsection{", "\\chapter{", "\\part{",
             "\\appendix", "\\end{document}")


def drop_empty_headings(blocks: list[str]) -> list[str]:
    """Remove a heading that has nothing under it.

    Half the corpus ends with a `## The ledger` section whose only content is the `def ledger`
    itself — which the document does not print, because the claims it holds are printed at the
    theorems that discharge them, under a claims table at the top of the chapter. Without this the
    book would carry a bare heading and a blank half-page per module.
    """
    keep = []
    for i, b in enumerate(blocks):
        if b.startswith(("\\section{", "\\subsection{", "\\subsubsection{")):
            nxt = next((x for x in blocks[i + 1:] if x.strip()), "")
            if not nxt or nxt.startswith(HEADINGS):
                continue
        keep.append(b)
    return keep


def definition_block(name: str, body: str, src: str) -> str:
    # the source reference goes on its own line: `\end{Statement}` must end its line, with nothing
    # after it, or FancyVerb calls it extraneous input
    return def_block(name, body) + ("\n" + rf"\src{{{esc(src)}}}" if src else "")


# --------------------------------------------------------------------------------------------------
# assembly
# --------------------------------------------------------------------------------------------------

def strip_prefix(text: str) -> str:
    return re.sub(r"\bFerrosa\.", "", text)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(BUILD / "ferrosa-specs.pdf"))
    ap.add_argument("--tex", default=str(BUILD / "ferrosa-specs.tex"))
    ap.add_argument("--part", default=None, help="render a single part numeral, for review")
    ap.add_argument("--no-pdf", action="store_true")
    args = ap.parse_args()

    global MODULES
    data = json.loads((BUILD / "specs.json").read_text())
    mods = {m["key"]: m for m in data["modules"]}
    OPEN_NS.extend(sorted({ns for m in data["modules"] for ns in m["namespaces"]} | {"Ferrosa"}))
    MODULES = [m["key"] for m in data["modules"]]

    # ledger items, by the theorem that discharges or exhibits them
    by_proof: dict[str, list[dict]] = {}
    by_module: dict[str, list[dict]] = {}
    for area in data["areas"]:
        for item in area["items"]:
            by_module.setdefault(area["key"], []).append(item)

    decl_index: dict[str, tuple[str, dict]] = {}
    for m in data["modules"]:
        for e in m["events"]:
            if e["k"] == "decl":
                decl_index[e["full"]] = (m["key"], e)
                decl_index.setdefault(e["short"], (m["key"], e))

    def resolve(s: str) -> str | None:
        s = s.split(" (")[0].strip()
        segs = s.split(".")
        for k in range(len(segs)):
            tail = segs[k:]
            hits = [f for f in decl_index
                    if f.split(".")[-len(tail):] == tail and f in decl_index
                    and decl_index[f][1]["kind"] in ("theorem", "lemma")]
            hits = sorted({decl_index[f][1]["full"] for f in hits})
            if len(hits) == 1:
                return hits[0]
        return None

    # A discharged claim names its theorem in `proof`; a red names, in `witness`, the theorem that
    # *exhibits the gap* — the counterexample or the fact that pins the missing piece. Both anchor to
    # a declaration, so both are printed at that declaration. A red whose gap is not expressible in
    # Lean says `none (implementation item)` instead, and lands at the end of its own chapter.
    claims_by_theorem: dict[str, list[dict]] = {}
    orphans: dict[str, list[dict]] = {}
    for area in data["areas"]:
        for item in area["items"]:
            full = resolve(item["proof"] if item["kind"] == "met" else item["witness"])
            if full:
                claims_by_theorem.setdefault(full, []).append(item)
            else:
                orphans.setdefault(area["key"], []).append(item)

    # statements for every claimed theorem
    claimed = [n for n in claims_by_theorem if n in decl_index or n in {e["full"] for m in data["modules"] for e in m["events"] if e["k"] == "decl"}]
    stmts = collect_statements(sorted(claimed))

    # the definitions those statements mention, one level deep
    vocab = {}
    for full, (mk, e) in decl_index.items():
        if "." in full and e["kind"] in ("def", "abbrev", "structure", "inductive", "class"):
            vocab.setdefault(e["short"], full)
    wanted: dict[str, list[str]] = {}
    for thm, text in stmts.items():
        toks, seen = [], []
        for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_'.]*", text):
            for cand in (tok, tok.split(".")[-1]):
                full = vocab.get(cand)
                if full and full != thm and full not in seen and decl_index[full][0] == decl_index.get(thm, (None,))[0]:
                    seen.append(full)
                    toks.append(full)
        wanted[thm] = toks[:6]
    all_defs = sorted({d for v in wanted.values() for d in v})
    prints = collect_prints(all_defs) if all_defs else {}

    def fmt_print(name: str) -> str:
        body = strip_prefix(prints.get(name, ""))
        short = name.split(".")[-1]
        # `#print` on a structure says "number of parameters: 0", then repeats the structure name in
        # front of every field. Both are noise when the reader already knows which structure this is.
        lines = [l for l in body.split("\n") if not l.strip().startswith("number of parameters")]
        sp = strip_prefix(name)
        lines = [l.replace(sp + ".", "") if not l.strip().startswith("structure") else l for l in lines]
        while lines and not lines[-1].strip():
            lines.pop()
        if len(lines) > 14:
            lines = lines[:14] + ["  …"]
        return "\n".join(lines)

    parts = data["parts"] if not args.part else [p for p in data["parts"] if p["numeral"] == args.part]

    out = [PREAMBLE, r"\begin{document}", titlepage(data), PRIMER, r"\mainmatter", r"\tableofcontents"]

    total_claims = 0
    for p in parts:
        out.append(part_opener(p))
        for key in p["modules"]:
            mod = mods.get(key)
            if not mod:
                continue
            rendered_claims: list[dict] = []
            rendered: set[str] = set()
            chapter_body: list[str] = []
            for e in mod["events"]:
                if e["k"] == "sect":
                    lvl = e["level"]
                    cmd = {1: "section", 2: "section", 3: "subsection"}.get(lvl, "subsubsection")
                    text = dedent(e["text"])
                    title, _, rest = text.partition("\n")
                    chapter_body.append(rf"\{cmd}{{{inline(title.strip())}}}")
                    if rest.strip():
                        chapter_body.append(prose(rest))
                    continue
                full = e["full"]
                src = f"{mod['path']}:{e['line']}"
                if full in claims_by_theorem:
                    for c in claims_by_theorem[full]:
                        defs = [(d.split(".")[-1], fmt_print(d)) for d in wanted.get(full, []) if d in prints]
                        chapter_body.append(claim_block(c, full, strip_prefix(stmts.get(full, "")), defs, src))
                        rendered_claims.append(c)
                    rendered.add(full)
                elif full in {d for v in wanted.values() for d in v} and full not in rendered:
                    chapter_body.append(definition_block(e["short"], fmt_print(full), src))
                    rendered.add(full)
            for c in orphans.get(key, []):
                chapter_body.append(claim_block(c, "", "", [], ""))
                rendered_claims.append(c)
            out.append(chapter_head(mod))
            total_claims += len(rendered_claims)
            if rendered_claims:
                out.append(claims_table(rendered_claims))
            out.extend(drop_empty_headings(chapter_body))

    # appendix: every theorem in the corpus, by module, whether claimed or not
    out.append(r"\appendix")
    out.append(r"\chapter{Every theorem in the corpus}")
    out.append(
        prose("The book above prints the claims. This is the register: every theorem the corpus "
              "contains, in module order, with the claims that cite it. A theorem with no claim "
              "beside it is supporting machinery — a step lemma, an intersection fact, a case check.")
    )
    cited = {full: [c["claim"] for c in cs] for full, cs in claims_by_theorem.items()}
    for m in data["modules"]:
        if m["key"] not in {k for p in data["parts"] for k in p["modules"]}:
            continue
        thms = [e for e in m["events"] if e["k"] == "decl" and e["kind"] == "theorem"]
        if not thms:
            continue
        # no TOC entry: sixty-one modules in the contents would bury the seven parts
        out.append(rf"\section*{{{esc(m['key'].replace('Ferrosa.', ''))}}}")
        out.append(r"{\footnotesize\begin{longtable}{@{}p{0.52\textwidth}p{0.44\textwidth}@{}}")
        for e in thms:
            claims = cited.get(e["full"], [])
            mark = r"\statuspass" if claims else ""
            out.append(
                rf"{mark}~\texttt{{{breakable(esc(e['short']))}}} & "
                + (inline(claims[0]) if claims else r"{\color{quiet}—}") + r" \\")
        out.append(r"\end{longtable}}")

    out.append(r"\end{document}")

    tex = "\n\n".join(x for x in out if x)
    Path(args.tex).write_text(tex)
    print(f"rendered {len(tex.splitlines())} lines, {total_claims} claims placed → {args.tex}")

    if args.no_pdf:
        return
    engine = shutil.which("tectonic")
    if engine:
        subprocess.run([engine, "--keep-logs", "-o", str(BUILD), str(args.tex)], check=True)
    elif shutil.which("latexmk"):
        subprocess.run(["latexmk", "-xelatex", "-interaction=nonstopmode", str(args.tex)], check=True)
    else:
        raise SystemExit("no XeLaTeX engine: install tectonic or texlive + latexmk")
    print(f"pdf → {args.out}")


if __name__ == "__main__":
    OPEN_NS: list[str] = []
    MODULES: list[str] = []
    main()
