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


# Module name -> chapter key, so prose that names a module can point at it. The corpus's headers say
# "the two layers already specified", which is positional and stops being true the moment the parts
# are reordered — a hyperlink to the chapter does not.
CHAPTERS: dict[str, str] = {}


def chapter_for(token: str) -> str | None:
    """The chapter key a backticked name refers to, if it is a module this book contains."""
    t = token.strip().replace(chr(92), "/")
    for suffix in (".lean", ".md"):
        if t.endswith(suffix):
            t = t[: -len(suffix)]
    dotted = t.replace("/", ".")
    for cand in (t, dotted, "Ferrosa." + dotted, "Ferrosa." + t):
        if cand in CHAPTERS:
            return CHAPTERS[cand]
    return None


def inline(s: str) -> str:
    """Escape a line of prose: `backticked` spans in mono, **bold** and *emphatic* as themselves.

    The corpus writes its docstrings in markdown, and the emphasis is doing work — `**I-27 fails in
    the module as written.**` is the sentence the reader needs to see first.
    """
    out, i = [], 0
    for m in re.finditer(r"\*\*([^*]+)\*\*|`([^`]+)`|\*([^*]+)\*", s):
        out.append(esc(s[i:m.start()]))
        if m.group(1) is not None:
            out.append(r"\textbf{" + inline(m.group(1)) + "}")
        elif m.group(2) is not None:
            target = chapter_for(m.group(2))
            body = r"\texttt{" + breakable(esc(m.group(2))) + "}"
            out.append(rf"\hyperref[ch:{slug(target)}]{{{body}}}" if target else body)
        else:
            out.append(r"\emph{" + inline(m.group(3)) + "}")
        i = m.end()
    out.append(esc(s[i:]))
    # Straight quotes are paired quotes in the document; babel otherwise maps every one of them to a
    # closing quote, so a pair reads the wrong way round.
    parts, open_q = "".join(out).split(chr(34)), True
    fixed = parts[0]
    for p in parts[1:]:
        fixed += ("``" if open_q else "''") + p
        open_q = not open_q
    return fixed


def dedent(text: str) -> str:
    """Source blocks are indented to sit under their `/-`; strip the common prefix."""
    lines = text.expandtabs(2).split("\n")
    body = [l for l in lines if l.strip()]
    if not body:
        return ""
    pad = min(len(l) - len(l.lstrip()) for l in body)
    return "\n".join(l[pad:].rstrip() for l in lines).strip("\n")


def prose(text: str) -> str:
    """A block of docstring as LaTeX: paragraphs, and `*` runs as a list.

    The corpus's headers use `*` bullets to state the two or three things a module is responsible
    for. Rendered as one paragraph they read as a run-on sentence — which is how a spec stops
    looking like a spec.
    """
    text = dedent(text)
    if not text:
        return ""
    out: list[str] = []
    for para in re.split(r"\n\s*\n", text):
        lines = [l.rstrip() for l in para.split("\n") if l.strip()]
        if not lines:
            continue
        intro, items = [], []
        for l in lines:
            m = re.match(r"\s*[*-]\s+(.*)", l)
            if m:
                items.append([m.group(1)])
            elif items:
                items[-1].append(l.strip())
            else:
                intro.append(l.strip())
        if items:
            if intro:
                out.append(inline(" ".join(intro)))
            out.append(r"\begin{itemize}\setlength{\itemsep}{2pt}" +
                       "".join(rf"\item {inline(chr(32).join(i))}" for i in items) + r"\end{itemize}")
        else:
            out.append(inline(" ".join(para.split())))
    return "\n\n".join(out)


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
% openany, not openright: openright starts every chapter on a right-hand page and leaves the facing
% verso blank, which is right for a bound book and wrong for a document read on a screen — it cost
% 56 blank pages in 309. Chapters now start on the next page, whatever its side.
\documentclass[10pt,twoside,openany]{book}
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
\usepackage{newunicodechar}
\newfontfamily\symfont{DejaVu Sans}[Scale=0.97]
\newunicodechar{⟨}{{\symfont\symbol{"27E8}}}
\newunicodechar{⟩}{{\symfont\symbol{"27E9}}}
\newunicodechar{∃}{{\symfont\symbol{"2203}}}
\newunicodechar{∀}{{\symfont\symbol{"2200}}}
\newunicodechar{∈}{{\symfont\symbol{"2208}}}
\newunicodechar{∧}{{\symfont\symbol{"2227}}}
\newunicodechar{¬}{{\symfont\symbol{"00AC}}}
\newunicodechar{≤}{{\symfont\symbol{"2264}}}
\newunicodechar{≠}{{\symfont\symbol{"2260}}}
\newunicodechar{→}{{\symfont\symbol{"2192}}}
\newunicodechar{↔}{{\symfont\symbol{"2194}}}
\newunicodechar{⇒}{{\symfont\symbol{"21D2}}}
\newunicodechar{·}{{\symfont\symbol{"00B7}}}
\newunicodechar{×}{{\symfont\symbol{"00D7}}}
\newunicodechar{⊤}{{\symfont\symbol{"22A4}}}
\newunicodechar{⊥}{{\symfont\symbol{"22A5}}}
\newunicodechar{⊂}{{\symfont\symbol{"2282}}}
\newunicodechar{⊆}{{\symfont\symbol{"2286}}}
\newunicodechar{≥}{{\symfont\symbol{"2265}}}
\newunicodechar{α}{{\symfont\symbol{"03B1}}}
\newunicodechar{β}{{\symfont\symbol{"03B2}}}
\newunicodechar{γ}{{\symfont\symbol{"03B3}}}
\newunicodechar{δ}{{\symfont\symbol{"03B4}}}
\newunicodechar{σ}{{\symfont\symbol{"03C3}}}
\newunicodechar{μ}{{\symfont\symbol{"03BC}}}
\newunicodechar{τ}{{\symfont\symbol{"03C4}}}
\newunicodechar{λ}{{\symfont\symbol{"03BB}}}
\newunicodechar{⌜}{{\symfont\symbol{"231C}}}
\newunicodechar{⌝}{{\symfont\symbol{"231D}}}
\newunicodechar{⊢}{{\symfont\symbol{"22A2}}}
\newunicodechar{⊨}{{\symfont\symbol{"22A8}}}
\newunicodechar{⊆}{{\symfont\symbol{"2286}}}
\newunicodechar{⊂}{{\symfont\symbol{"2282}}}
\newunicodechar{⊨}{{\symfont\symbol{"22A8}}}
\newunicodechar{⊤}{{\symfont\symbol{"22A4}}}
\newunicodechar{⊥}{{\symfont\symbol{"22A5}}}
\usepackage{fvextra}
\usepackage[fleqn]{amsmath}
\setlength{\mathindent}{0pt}
\setlength{\abovedisplayskip}{3pt plus 1pt minus 1pt}
\setlength{\belowdisplayskip}{3pt plus 1pt minus 1pt}
\setlength{\abovedisplayshortskip}{3pt}
\setlength{\belowdisplayshortskip}{3pt}
\usepackage{amssymb}
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

% The statement itself is mathematics; the Lean it came from is printed underneath in small mono so
% the translation can be checked against the compiler's own words.
\DefineVerbatimEnvironment{LeanForm}{Verbatim}
  {fontsize=\scriptsize,frame=none,formatcom=\color{quiet},xleftmargin=1pt,
   breaklines=true,breaksymbolleft={},breaksymbolright={},breakindent=8pt}
\newcommand{\stmtrule}{\par\vspace{1pt}\noindent\rule{\linewidth}{0.3pt}\par\vspace{1pt}}

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
\clearpage
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
        # The opening title is a *block*, not a line: it runs to the ruler. Dropping one line left
        # the tail of the title as the chapter's first sentence, which is how "transaction shape."
        # came to stand alone at the top of a page.
        raw = header.split("\n")
        cut = next((i for i, l in enumerate(raw) if re.fullmatch(r"\s*[=\-—~_*·]{4,}\s*", l)), None)
        if cut is not None:
            raw = raw[cut + 1:]
        else:
            raw = raw[1:]
        out.append(rf"\noindent\hfill{{\sffamily\color{{quiet}}\footnotesize\texttt{{{esc(mod['path'])}}}}}\par\vspace{{2pt}}")
        body = prose("\n".join(raw))
        if body:
            out.append(body)
    return "\n".join(out)


def guarantees_chapter(parts: list[dict], claims_by_module: dict[str, list[dict]]) -> str:
    """The book's first page of content, and the only one written for someone who has not read the
    code: what the database promises, part by part, each promise with the theorem that settles it.

    Every sentence here is the corpus's own — the ledger claims, unaltered. The chapter adds no
    prose about what Ferrosa is; it rearranges what the corpus already says into the order a user
    asks the questions in.
    """
    out = [
        r"\chapter*{What this database guarantees}",
        r"\addcontentsline{toc}{chapter}{What this database guarantees}",
        prose("Each line below is a sentence this corpus asserts about the database, and the theorem "
              "that settles it. **Guaranteed** means the statement is proved, with no `sorry`, no "
              "extra axiom, and nothing imported beyond Lean's core. **Not guaranteed** means the "
              "corpus states the property and shows why the code as written does not have it — the "
              "witness names the theorem that exhibits the failure. A red line is not a bug in the "
              "proof: it is the proof working."),
        prose("The parts that follow take the same material a module at a time, and explain how each "
              "guarantee is reached. This chapter exists because that order is the author's, not "
              "yours."),
    ]
    for p in parts:
        rows = []
        for key in p["modules"]:
            for c in claims_by_module.get(key, []):
                mark = r"\statuspass" if c["kind"] == "met" else r"\statusred"
                ground = c["proof"] if c["kind"] == "met" else c["witness"]
                rows.append(
                    rf"{mark} & {inline(c['claim'])} & \texttt{{{breakable(esc(ground))}}} "
                    rf"& \pageref{{cl:{slug(c['claim'])}}} \\")
        if not rows:
            continue
        out.append(rf"\section*{{{esc(p['title'])}}}")
        out.append(
            r"{\footnotesize\begin{longtable}{@{}p{12mm}p{0.52\textwidth}p{0.30\textwidth}@{\hfill}r@{}}"
            "\n" + "\n".join(rows) + "\n" + r"\end{longtable}}"
        )
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


def math_display(tex: str) -> str:
    """The statement, set as mathematics between two rules."""
    return (
        "\\par\\addvspace{3pt}\\noindent\\stmtrule\\nopagebreak\n"
        "\\[" + tex + "\\]\\nopagebreak\n"
        "\\noindent\\stmtrule\\par"
    )


def _words(s: str) -> list[str]:
    """The words of a formula, with LaTeX wrappers and namespace prefixes removed."""
    s = re.sub(r"\\begin\{[^}]*\}(\[[^\]]*\])?", " ", s)
    s = re.sub(r"\\end\{[^}]*\}", " ", s)
    s = re.sub(r"\\[a-zA-Z]+", " ", s)
    # `ConsistencyLevel.one` and `one` are the same object; the namespace is not part of the reading
    s = re.sub(r"[A-Za-z0-9_']\.(?:[A-Za-z0-9_']+\.)*[A-Za-z0-9_']+",
               lambda m: m.group(0).rsplit(".", 1)[-1], s)
    s = re.sub(r"[^A-Za-z0-9_\s]", " ", s)
    return sorted(w.split(".")[-1] for w in s.split() if w)


def same_statement(tex: str, lean: str) -> bool:
    """Whether the mathematics and the Lean say the same thing word for word.

    Printing both then is printing the same formula twice, which reads as a mistake — the reader
    looks for the difference and there is none.
    """
    a, b = _words(tex), _words(lean)
    return bool(a) and a == b


def lean_form(lean: str, tex: str = "") -> str:
    """The same statement as Lean printed it, for checking the translation above."""
    if not lean.strip() or same_statement(tex, lean):
        return ""
    return (
        "\\par\\vspace{-1pt}\\noindent{\\scriptsize\\sffamily\\color{quiet}lean}\\par\\nopagebreak\n"
        "\\begin{LeanForm}\n" + lean + "\n\\end{LeanForm}"
    )


def claim_block(c: dict, thm: str, tex: str, lean: str, doc: str,
                defs: list[tuple[str, str]], src: str) -> str:
    mark = r"\statuspass" if c["kind"] == "met" else r"\statusred"
    if c["kind"] == "met":
        prov = f"proved by \\texttt{{{breakable(esc(c['proof']))}}}"
    else:
        prov = f"witness: \\texttt{{{breakable(esc(c['witness']))}}}"
    out = [rf"\phantomsection\label{{cl:{slug(c['claim'])}}}",
           rf"\claim{{{mark}}}{{{inline(c['claim'])}}}{{{prov}}}"]
    if c["kind"] == "failing":
        out.append(rf"\redwhy{{{inline(c['why'])}}}")
    if doc.strip():
        out.append(rf"{{\small {prose(doc)}}}")
    # The definition comes first. A reader who meets `mergeCell` in the middle of a formula and has
    # to turn the page to learn what it means has lost the argument the page was making.
    out.extend(def_block(name, body) for name, body in defs)
    if tex:
        out.append(math_display(tex))
    if lean:
        out.append(lean_form(lean, tex))
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


def statement_block(name: str, tex: str, lean: str, doc: str, desc: str,
                    defs: list[tuple[str, str]], src: str) -> str:
    """A theorem no ledger claims. The author's docstring is the sentence above it; where there is
    none, the statement stands on its name and its mathematics rather than on a sentence this
    renderer would have had to invent."""
    head = (
        r"\par\addvspace{7pt}\noindent"
        "{\\footnotesize\\sffamily\\color{quiet}" + esc(name) + r"}\par\nopagebreak"
    )
    # The author's sentence when there is one; otherwise a reading of the formula itself, in the
    # same place, so no theorem stands on its name alone.
    if doc.strip():
        body = "\n" + rf"{{\small {prose(doc)}}}"
    elif desc.strip():
        body = "\n" + rf"{{\small\itshape {desc}}}"
    else:
        body = ""
    body += "".join("\n" + def_block(n, b) for n, b in defs)
    body += "\n" + math_display(tex) + "\n" + lean_form(lean, tex)
    return head + body + ("\n" + rf"\src{{{esc(src)}}}" if src else "")


def module_shape(mod: dict) -> str:
    """One clause saying what an apparatus module contains, for the part's apparatus list."""
    kinds: dict[str, int] = {}
    for e in mod["events"]:
        if e["k"] == "decl":
            kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
    if not kinds:
        return "source file with no declarations"
    parts = [f"{n} {k if n > 1 else k}" for k, n in sorted(kinds.items(), key=lambda kv: -kv[1])]
    return ", ".join(parts)


def apparatus_block(items: list[tuple[str, str]], p: dict) -> str:
    rows = "\n".join(
        rf"\texttt{{{breakable(esc(k.replace('Ferrosa.', '')))}}} & {inline(shape)} \\"
        for k, shape in items)
    return (
        r"\section*{Apparatus}" + "\n"
        + prose("Modules in this part that carry no claims and no theorems: they hold the checker, "
                "the runners, and the syntax the rest is written in. Listed here so the book does "
                "not spend a page saying nothing.")
        + "\n{\\footnotesize\\begin{longtable}{@{}p{0.42\\textwidth}p{0.52\\textwidth}@{}}\\toprule\n"
        + rows + "\n\\bottomrule\\end{longtable}}"
    )


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

    # Statements for every theorem the book will print. A chapter used to show only what a ledger
    # cited, which left forty of the sixty-one modules with nothing but a header — the machinery was
    # invisible and the book looked padded. Every theorem now appears in its own module, and the
    # ledger claim is what turns one of them from a statement into a claim.
    book_modules = {k for p in data["parts"] for k in p["modules"]}
    declared = {e["full"] for m in data["modules"] for e in m["events"] if e["k"] == "decl"}

    def module_theorems(key: str) -> list[dict]:
        return [e for e in mods[key]["events"] if e["k"] == "decl" and e["kind"] == "theorem"]

    unclaimed = {k: [e for e in module_theorems(k) if e["full"] not in claims_by_theorem]
                 for k in book_modules if k in mods}
    stmts = collect_statements(sorted({n for n in claims_by_theorem if n in declared}
                                      | {e["full"] for v in unclaimed.values() for e in v}))

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
        wanted[thm] = toks
    referenced = {d for v in wanted.values() for d in v}
    # every definition the book will print: those a statement names, and those the author documented
    documented = {e["full"] for m in data["modules"] for e in m["events"]
                  if e["k"] == "decl" and e["kind"] in ("def", "abbrev", "structure", "inductive", "class")
                  and e["doc"].strip()}
    prints = collect_prints(sorted(referenced | documented)) or {}

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

    claims_by_module = {a["key"]: a["items"] for a in data["areas"]}
    for m in data["modules"]:
        CHAPTERS[m["key"]] = m["key"]
        CHAPTERS[m["key"].replace("Ferrosa.", "")] = m["key"]
        CHAPTERS[m["path"]] = m["key"]
    out = [PREAMBLE, r"\begin{document}", titlepage(data), PRIMER, r"\mainmatter", r"\tableofcontents",
           guarantees_chapter(data["parts"], claims_by_module)]

    total_claims = 0
    for p in parts:
        out.append(part_opener(p))
        apparatus: list[tuple[str, str]] = []
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
                        entry = decl_index.get(full, (None, {}))[1] or {}
                        chapter_body.append(claim_block(c, full, entry.get("tex", ""),
                                                        strip_prefix(stmts.get(full, "")),
                                                        entry.get("doc", ""), defs, src))
                        rendered_claims.append(c)
                    rendered.add(full)
                elif full in rendered:
                    pass
                elif e["kind"] in ("theorem", "lemma"):
                    # In place, not in a trailing register: the paragraph above asserts something and
                    # this is the statement that makes the assertion checkable.
                    sdefs = [(d.split(".")[-1], fmt_print(d)) for d in wanted.get(full, [])
                             if d in prints and d not in rendered]
                    rendered.update(d for d in wanted.get(full, []) if d in prints)
                    chapter_body.append(statement_block(e["short"], e.get("tex", ""),
                                                        strip_prefix(stmts.get(full, "")),
                                                        e.get("doc", ""), e.get("desc", ""),
                                                        sdefs, src))
                    rendered.add(full)
                elif full in referenced or (e["doc"].strip() and e["kind"] != "instance"):
                    # A definition earns its place if a statement leans on it, or if the author wrote
                    # a docstring for it — the corpus's own signal that it is worth reading.
                    chapter_body.append(definition_block(e["short"], fmt_print(full), src))
                    rendered.add(full)
            for c in orphans.get(key, []):
                chapter_body.append(claim_block(c, "", "", "", "", [], ""))
                rendered_claims.append(c)

            # A module with neither a claim nor a theorem is apparatus — a `#check` file, a runner,
            # a syntax skeleton. It gets a line in the part's apparatus list instead of a chapter
            # with one paragraph in it.
            rest = unclaimed.get(key, [])
            if not rendered_claims and not rest:
                apparatus.append((key, module_shape(mod)))
                continue

            out.append(chapter_head(mod))
            total_claims += len(rendered_claims)
            if rendered_claims:
                out.append(claims_table(rendered_claims))
            out.extend(drop_empty_headings(chapter_body))

        if apparatus:
            out.append(apparatus_block(apparatus, p))
            apparatus = []

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
