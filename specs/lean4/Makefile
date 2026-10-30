# The book.
#
# `specs/lean4/docs/` renders this corpus as a PDF. Nothing here writes prose: the chapters are the
# modules, the sections are the `/-!` blocks, the claim sentences are the ledgers, and the
# statements come out of Lean itself. Editing the PDF is not possible and editing the generator is
# usually the wrong fix — write next to the proof instead.
#
#   make book        data + PDF, everything
#   make data        build/specs.json — what the corpus says it proves
#   make pdf         the PDF, from existing data
#   make part P=II   one part, for review
#   make check       the drift checks on their own, no PDF
#
# Engine: tectonic if it is on PATH (a single binary, no sudo, no texlive), else latexmk with
# xelatex. Both consume the same .tex, so CI can use whichever it has.

LAKE ?= lake
PYTHON ?= python3
TECTONIC ?= tectonic
P ?=

.PHONY: book data pdf check clean

book: data pdf

data:
	$(LAKE) exe docs build/specs.json

check:
	$(LAKE) exe docs build/specs.json

pdf:
	$(PYTHON) docs/render.py $(if $(P),--part $(P),)

clean:
	rm -rf build
