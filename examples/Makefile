ADOC := $(wildcard */*.adoc)
HTML := $(patsubst %.adoc,%.html,$(ADOC))
THEME := theme/ferrosa.css
OUTDIR := ../docs/examples

.PHONY: html clean test

html: $(ADOC) $(THEME)
	@mkdir -p $(OUTDIR)
	@for f in $(ADOC); do \
		name=$$(basename $$f .adoc); \
		dir=$$(dirname $$f); \
		echo "Converting $$f -> $(OUTDIR)/$$name.html"; \
		asciidoctor -a stylesheet=$(THEME) -a linkcss -a stylesdir=. \
			-D $(OUTDIR) -o $$name.html $$f; \
	done
	@cp $(THEME) $(OUTDIR)/ferrosa.css

clean:
	rm -f $(OUTDIR)/*.html $(OUTDIR)/ferrosa.css

test:
	./test-examples.sh
