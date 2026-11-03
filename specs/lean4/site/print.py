#!/usr/bin/env python3
"""Assemble the Verso site's pages into one print document.

    python3 print.py <site dir> <out.html>

The PDF is printed from the *site's own pages*, so the paper and the web
cannot disagree about what the specification says: there is one rendering of
the corpus, and the PDF is that rendering with a print stylesheet applied.

Page order is read from the site's `module-tree` navigation rather than from
`literate.toml`, because the tree is what a reader sees and what Verso
actually built. A page that exists but is not in the tree is not in the book.

Only the `main` region of each page is kept: the sidebar, module tree, page
toc, search, review widgets and every script are dropped, so what is printed
is the chapter and nothing that only makes sense with a mouse.

Anchors are neutralised (`<a href>` becomes `<a>`). A printed cross-reference
that points at a file name is worse than no link; the prose already names the
module it refers to. In-document `#anchor` links are kept.
"""

import html
import re
import sys
from pathlib import Path

MAIN_RE = re.compile(r'<main class="main-area"[^>]*>(.*?)</main>', re.S)
TREE_RE = re.compile(r'<nav class="module-tree".*?</nav>', re.S)
LINK_RE = re.compile(r'<a\b[^>]*href="([^"]+)"[^>]*>', re.I)
SCRIPT_RE = re.compile(r"<script\b.*?</script>", re.S | re.I)
# Controls that only work on the web page.
DROP_RE = re.compile(
    r'<(button|input|select|iframe)\b.*?</\1>|<(button|input|select|iframe)\b[^>]*/?>',
    re.S | re.I,
)

PRINT_CSS = """
@page { size: A4; margin: 18mm 16mm 20mm 16mm; }
html { font-size: 10.5pt; }
body { margin: 0; color: #111; background: #fff; }
.chapter { break-before: page; }
.chapter:first-of-type { break-before: auto; }
h1, h2, h3, h4 { break-after: avoid; }
pre, blockquote, table, figure { break-inside: avoid; }
img, svg { max-width: 100%; }
a { color: #111; text-decoration: none; }
.title-bar .breadcrumbs { font-size: 8.5pt; color: #555; }
.title-bar { margin-bottom: 1.2em; border-bottom: 0.6pt solid #bbb; padding-bottom: 0.4em; }
code, pre { font-size: 8.5pt; }
"""


def page_order(site: Path) -> list[str]:
    """The book's order, taken from the module tree Verso rendered."""
    index = site / "index.html"
    if not index.exists():
        raise SystemExit(f"print.py: no index.html in {site}")
    tree = TREE_RE.search(index.read_text(errors="replace"))
    if not tree:
        raise SystemExit("print.py: no module-tree navigation in index.html")
    seen, order = set(), []
    for href in LINK_RE.findall(tree.group(0)):
        if href.startswith(("http://", "https://", "#")):
            continue
        # `product/` and `FerrosaProduct/Cql/` are relative to the site root.
        path = href.rstrip("/")
        if path.endswith(".html"):
            path = path[: -len(".html")]
        if path in seen or not path:
            continue
        seen.add(path)
        order.append(path)
    return order


def chapter(site: Path, rel: str) -> str | None:
    page = site / rel / "index.html"
    if not page.exists():
        print(f"print.py: skipping missing page {page}", file=sys.stderr)
        return None
    text = page.read_text(errors="replace")
    m = MAIN_RE.search(text)
    if not m:
        print(f"print.py: no main region in {page}", file=sys.stderr)
        return None
    body = SCRIPT_RE.sub("", m.group(1))
    body = DROP_RE.sub("", body)
    body = LINK_RE.sub("<a>", body)
    return f'<section class="chapter" id="{html.escape(rel)}">\n{body}\n</section>'


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: print.py <site dir> <out.html>", file=sys.stderr)
        return 2
    site, out = Path(argv[1]), Path(argv[2])
    order = page_order(site)
    parts = [f"<title>{html.escape(site.name)} — specification</title>"]
    built = 0
    for rel in order:
        part = chapter(site, rel)
        if part is not None:
            parts.append(part)
            built += 1
    if not built:
        raise SystemExit("print.py: assembled no chapters")
    doc = (
        "<!doctype html>\n<html><head><meta charset=\"utf-8\">\n"
        f"<style>{PRINT_CSS}</style>\n"
        # The site's own stylesheet styles the pages being printed; the print
        # rules above only override what paper needs.
        f"<link rel=\"stylesheet\" href=\"{html.escape(str((site / 'ferrosa.css').resolve().as_uri()))}\">\n"
        f"<link rel=\"stylesheet\" href=\"{html.escape(str((site / 'literate.css').resolve().as_uri()))}\">\n"
        "</head><body>\n" + "\n".join(parts) + "\n</body></html>\n"
    )
    out.write_text(doc)
    print(f"print.py: {built} chapters -> {out} ({len(doc) / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
