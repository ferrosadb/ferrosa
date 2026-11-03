# The corpus, and the data the specification is rendered from.
#
# Nothing here writes prose: the chapters are the modules, the sections are the `/-!` blocks, the
# claim sentences are the ledgers, and the statements come out of Lean itself. Editing the rendered
# document is not possible and editing the generator is usually the wrong fix — write next to the
# proof instead. `specs/lean4/site/` renders the corpus as the specification; this Makefile only
# produces the data that renderer reads.
#
#   make data        build/specs.json — what the corpus says it proves
#   make check       the drift checks on their own
#
# The PDF is printed from the site, not from here: `make -C site pdf`.

LAKE ?= lake
PYTHON ?= python3

# A Lean `panic!` prints a line and returns a default value, so an exporter that panics still exits
# 0 with that default in its output. Aborting on panic makes it a failed build instead.
export LEAN_ABORT_ON_PANIC := 1

.PHONY: data check clean

data:
	$(LAKE) exe export build/specs.json

check:
	$(LAKE) exe export build/specs.json

clean:
	rm -rf build
