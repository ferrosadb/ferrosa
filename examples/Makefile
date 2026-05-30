ADOC := $(wildcard */*.adoc)
HTML := $(patsubst %.adoc,%.html,$(ADOC))
THEME := theme/ferrosa.css
OUTDIR := ../docs/database/examples

.PHONY: html clean test

html: $(ADOC) $(THEME) index.adoc
	@mkdir -p $(OUTDIR)
	@for f in $(ADOC); do \
		name=$$(basename $$f .adoc); \
		dir=$$(dirname $$f); \
		echo "Converting $$f -> $(OUTDIR)/$$name.html"; \
		asciidoctor -a reproducible -a 'source-highlighter!' -a stylesheet=$(THEME) -a linkcss -a stylesdir=. \
			-a docinfo=shared -a docinfodir=$$PWD \
			-D $(OUTDIR) -o $$name.html $$f; \
	done
	@echo "Converting index.adoc -> $(OUTDIR)/index.html"
	@asciidoctor -a reproducible -a 'source-highlighter!' -a stylesheet=$(THEME) -a linkcss -a stylesdir=. \
		-a docinfo=shared -a docinfodir=$$PWD \
		-D $(OUTDIR) -o index.html index.adoc
	@mkdir -p $(OUTDIR)/theme
	@cp $(THEME) $(OUTDIR)/theme/ferrosa.css
	@cp $(THEME) $(OUTDIR)/ferrosa.css

clean:
	rm -f $(OUTDIR)/*.html $(OUTDIR)/ferrosa.css $(OUTDIR)/theme/ferrosa.css

test:
	./test-examples.sh
