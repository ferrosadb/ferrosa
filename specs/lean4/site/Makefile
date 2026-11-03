# The specification as a web site.
#
#   make site                build the site; prints where it is
#   make serve               build, then serve it on this machine's LAN address
#   make serve PORT=8100     the same, on another port
#   make a11y                build, then audit every page against WCAG 2.2 A/AA (needs Node)
#
# `lake query :literateHtml` writes Verso's pages into $(VERSO). Verso rewrites only the pages whose
# sources changed, so the published site is a fresh copy of that directory in $(OUT), which
# `postprocess.py` then makes accessible and gives each module its opening essay. Rewriting
# Verso's directory in place would mix processed and unprocessed pages after a partial rebuild.

LAKE ?= lake
PYTHON ?= python3
NPM ?= npm
NODE ?= node
PORT ?= 8088
BIND ?= 0.0.0.0
VERSO := .lake/build/literate-html
OUT := .lake/build/site

SPECS_JSON := ../build/specs.json

.PHONY: site serve register check a11y clean

# Verso's pages read the stylesheet, scripts and wordmarks from the site root; they are copied on
# every build because Verso's trace does not cover them.
site:
	$(LAKE) query :literateHtml
	$(MAKE) -C .. data
	rm -rf $(OUT)
	cp -R $(VERSO) $(OUT)
	cp assets/logo.svg assets/logo-light.svg ferrosa.css review.js a11y.js $(OUT)/
	$(PYTHON) -I postprocess.py $(OUT) $(SPECS_JSON)
	@echo "site: $(OUT)/index.html"

# The theorem register is generated from the corpus's export; never edit it by hand.
register:
	$(MAKE) -C .. data
	$(PYTHON) -I gen_register.py $(SPECS_JSON) FerrosaProduct

# The PDF is the site, printed. `print.py` assembles the built pages in the book's order into one
# document and Chromium prints that, so the paper cannot drift from the web: there is one rendering
# of the corpus and both outputs come from it.
#
#   make pdf                 build/specs.pdf
#   make pdf CHROME=chromium another browser
PDF := .lake/build/specs.pdf
CHROME ?= $(shell command -v google-chrome || command -v chromium || command -v chromium-browser)

pdf: site
	@test -n "$(CHROME)" || { echo "make pdf: no Chrome/Chromium on PATH; pass CHROME=..." >&2; exit 2; }
	$(PYTHON) -I print.py $(OUT) .lake/build/specs-print.html
	$(CHROME) --headless --disable-gpu --no-sandbox --no-pdf-header-footer \
	  --print-to-pdf=$(abspath $(PDF)) file://$(abspath .lake/build/specs-print.html)
	@echo "pdf: $(PDF)"

# Fails when the committed register is not what the current corpus generates.
check: register
	git diff --exit-code -- FerrosaProduct/Register.lean FerrosaProduct/Register/

# Exits non-zero on any violation, and on any failure to audit. CHROMIUM may name a local Chromium
# to use instead of the one Playwright downloads.
a11y: site
	cd a11y && $(NPM) ci --no-fund --no-audit
	cd a11y && $(NODE) audit.mjs ../$(OUT)

serve: site
	$(PYTHON) -m http.server $(PORT) --bind $(BIND) --directory $(OUT)

clean:
	rm -rf $(OUT) $(VERSO)
