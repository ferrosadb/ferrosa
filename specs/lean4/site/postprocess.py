"""Make Verso's literate pages accessible and put each module's opening essay on its page.

    python3 -I postprocess.py <site-dir> <specs.json>

Verso v4.35.0 renders these pages; this pass rewrites what it emits, in place, after every build:

- `<html>` gets `lang="en"` (WCAG 3.1.1).
- A module doc's list items arrive as `<p>` directly inside `<ul>`/`<ol>`; they become `<li>`, so a
  screen reader announces a list of N items rather than a list of nothing (1.3.1).
- Every page gets exactly one `<h1>` (axe page-has-heading-one). A corpus module's is the title of its
  header essay; a product page promotes the `#` heading Verso demotes to `<h2>`; any other page takes
  its breadcrumb's last crumb.
- A navigation entry with children is `<summary><a>…</a></summary>`: a link inside a disclosure
  toggle, so one control does two things and assistive technology can reach only one of them
  (4.1.2, axe nested-interactive). The link moves out beside the disclosure, and the summary keeps
  only the toggle, labelled with what it opens.
- A corpus module's header is a plain `/-` comment before `import`, because Lean admits no module
  doc there and the PDF exporter reads it as the module's essay. Verso shows it only inside the
  collapsed Imports box. The essay is rendered from the corpus export, the same text the PDF prints,
  and placed at the top of the page.

Every rewrite counts what it changed. When Verso's markup stops matching what a rewrite expects —
a page with no `<html>`, a nav entry whose summary is not one link, an essay using Markdown the
renderer does not know — the pass stops with the page named, rather than publishing a page it
silently failed to fix.
"""

from __future__ import annotations

import html
import json
import re
import sys
from html.parser import HTMLParser
from pathlib import Path


class Unexpected(Exception):
    """Verso emitted markup this pass was not written for."""


# ---------------------------------------------------------------------------------------------------
# The header essay: a setext title, then paragraphs with `code`, **bold** and *italic*. Nothing else
# occurs in the corpus today, and anything else is refused rather than guessed at.

UNSUPPORTED = re.compile(r"^\s*([-*+] |\d+[.)] |\||```|#|>|    \S)")


def render_inline(text: str) -> str:
    # Code spans become placeholders first, so emphasis can span them (`**a `b` c**`) and an
    # asterisk inside code is never read as emphasis. NUL cannot occur in a Lean source comment.
    spans: list[str] = []

    def hold(m: re.Match) -> str:
        spans.append("<code>" + html.escape(m.group(1)) + "</code>")
        return f"\0{len(spans) - 1}\0"

    text = html.escape(re.sub(r"`([^`]*)`", hold, text), quote=False)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"(?<![*\w])\*(?!\s)(.+?)(?<!\s)\*(?![*\w])", r"<em>\1</em>", text)
    return re.sub(r"\0(\d+)\0", lambda m: spans[int(m.group(1))], text)


def render_essay(key: str, text: str) -> tuple[str, str]:
    """The essay's title, and its body as HTML."""
    lines = [ln.strip() for ln in text.strip("\n").split("\n")]
    # A setext title may wrap: it is every line above the first underline, which must come before
    # the first blank line.
    under = next((i for i, ln in enumerate(lines) if re.fullmatch(r"=+|-+", ln) or not ln), None)
    if under is None or under == 0 or not lines[under]:
        raise Unexpected(f"{key}: header essay does not open with a setext title")
    title = " ".join(lines[:under])
    # The book's chapter titles all open with the corpus's name; on a page of its own site that
    # prefix says nothing, so the heading drops it.
    title = re.sub(r"^Ferrosa formal specification\s*[—-]\s*", "", title)
    title = title[:1].upper() + title[1:]
    paragraphs, current = [], []
    for ln in lines[under + 1:]:
        if UNSUPPORTED.match(ln) and not ln.startswith("**"):
            raise Unexpected(f"{key}: header essay uses Markdown the renderer does not handle: {ln!r}")
        if ln:
            current.append(ln)
        elif current:
            paragraphs.append(" ".join(current))
            current = []
    if current:
        paragraphs.append(" ".join(current))
    body = "\n".join(f"<p>{render_inline(p)}</p>" for p in paragraphs)
    return title, body


def essays(specs: Path) -> dict[str, tuple[str, str]]:
    """Page path (relative to the site root) to (title, body) for every module with an essay."""
    data = json.loads(specs.read_text())
    found = {}
    for module in data["modules"]:
        if module["header"].strip():
            page = module["key"].replace(".", "/") + "/index.html"
            found[page] = render_essay(module["key"], module["header"])
    return found


# ---------------------------------------------------------------------------------------------------
# The rewriting parser. It re-emits the document token by token, so anything it does not rewrite
# comes out byte-for-byte as Verso wrote it.

VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}


def start_tag(tag: str, attrs: list[tuple[str, str | None]], closed: bool = False) -> str:
    parts = [tag] + [k if v is None else f'{k}="{html.escape(v)}"' for k, v in attrs]
    return "<" + " ".join(parts) + ("/>" if closed else ">")


class Rewriter(HTMLParser):
    def __init__(self, page: str, essay: tuple[str, str] | None) -> None:
        super().__init__(convert_charrefs=False)
        self.page = page
        self.essay = essay
        self.out: list[str] = []
        self.stack: list[str] = []          # open elements, as emitted
        self.list_p: list[bool] = []        # per open <p>: was it rewritten to <li>
        self.in_nav = 0                     # depth inside nav.module-tree
        self.summary: list[str] | None = None   # buffered contents of a nav summary
        self.summary_attrs: list = []
        self.details_open: list[str] = []   # buffered <details> start tags awaiting their summary
        self.nav_wrapped: list[bool] = []   # per open nav <details>: did its link move into a wrapper
        self.promoted = False               # a product page's first <h2> became its <h1>
        self.h1 = 0
        self.counts = {"lang": 0, "li": 0, "nav": 0, "essay": 0, "h1": 0}
        self.crumb: list[str] = []
        self.in_crumb_current = False

    # -- helpers -----------------------------------------------------------------------------------

    def emit(self, text: str) -> None:
        if self.summary is not None:
            self.summary.append(text)
        else:
            self.out.append(text)

    # -- tags --------------------------------------------------------------------------------------

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        cls = (a.get("class") or "").split()
        if tag == "html":
            if "lang" not in a:
                attrs = attrs + [("lang", "en")]
                self.counts["lang"] += 1
        if tag == "nav" and "module-tree" in cls:
            self.in_nav = 1
        elif self.in_nav and tag not in VOID:
            self.in_nav += 1
        if tag == "span" and "current" in cls and self.stack[-1:] == ["li"] and "ol" in self.stack:
            self.in_crumb_current = True
        if self.in_nav and tag == "details":
            # Hold the <details> until its summary is read: the link must be written before it.
            self.details_open.append(start_tag(tag, attrs))
            self.stack.append(tag)
            return
        if self.in_nav and tag == "summary":
            self.summary = []
            self.summary_attrs = attrs
            self.stack.append(tag)
            return
        if tag == "p" and self.stack and self.stack[-1] in ("ul", "ol"):
            self.list_p.append(True)
            self.counts["li"] += 1
            self.stack.append("li")
            self.emit(start_tag("li", attrs))
            return
        if tag == "p":
            self.list_p.append(False)
        if tag == "h1":
            self.h1 += 1
        if tag == "h2" and not self.promoted and self.essay is None and self.page.startswith("FerrosaProduct/") \
                and self.h1 == 0 and self.after_imports_or_first():
            self.promoted = True
            self.counts["h1"] += 1
            self.h1 += 1
            self.stack.append("h1")
            self.emit(start_tag("h1", attrs))
            return
        if tag not in VOID:
            self.stack.append(tag)
        self.emit(start_tag(tag, attrs))

    def after_imports_or_first(self) -> bool:
        return "section" in self.stack

    def handle_startendtag(self, tag, attrs):
        self.emit(start_tag(tag, attrs, closed=True))

    def handle_endtag(self, tag):
        if self.in_nav and tag == "summary":
            self.close_summary()
            self.in_nav -= 1
            return
        if self.in_nav and tag == "details":
            self.stack.pop()
            self.emit("</details></div>" if self.nav_wrapped.pop() else "</details>")
            self.in_nav -= 1
            return
        if tag == "p":
            was_li = self.list_p.pop() if self.list_p else False
            if was_li:
                self.pop("li")
                self.emit("</li>")
                return
        if tag in ("h2",) and self.stack and self.stack[-1] == "h1":
            self.pop("h1")
            self.emit("</h1>")
            return
        if self.in_crumb_current and tag == "span":
            self.in_crumb_current = False
        if tag == "nav" and self.in_nav:
            self.in_nav = 0
        elif self.in_nav:
            self.in_nav -= 1
        if tag not in VOID:
            self.pop(tag)
        self.emit(f"</{tag}>")

    def close_summary(self):
        inner = "".join(self.summary or [])
        self.summary = None
        self.stack.pop()
        details = self.details_open.pop()
        if "<a" not in inner and "<" not in inner.strip():
            # A namespace with no page of its own: the toggle is plain text, already one control.
            self.nav_wrapped.append(False)
            self.emit(details + start_tag("summary", self.summary_attrs) + inner + "</summary>")
            return
        m = re.fullmatch(r"\s*(<a\b[^>]*>(.*?)</a>)\s*", inner, re.S)
        if not m:
            raise Unexpected(f"{self.page}: a navigation summary is not exactly one link: {inner[:120]!r}")
        self.nav_wrapped.append(True)
        link, label = m.group(1), re.sub(r"<[^>]+>", "", m.group(2)).strip()
        attrs = [(k, v) for k, v in self.summary_attrs]
        # The current page is marked on its link, not on the wrapper: Verso colours every link under a
        # `.current` element as if it were the highlighted one, which would hide the children.
        if "current" in (dict(attrs).get("class") or "").split():
            link = link.replace("<a ", '<a aria-current="page" ', 1)
        attrs = [(k, v) for k, v in attrs if k != "class"] + [("aria-label", f"{label}: show its pages")]
        self.emit(f'<div class="ferrosa-nav-node">{link}{details}{start_tag("summary", attrs)}</summary>')
        self.counts["nav"] += 1

    def pop(self, tag):
        if not self.stack or self.stack[-1] != tag:
            raise Unexpected(f"{self.page}: </{tag}> closes {self.stack[-1:] or 'nothing'}")
        self.stack.pop()

    # -- everything else passes through ------------------------------------------------------------

    def handle_data(self, data):
        if self.in_crumb_current:
            self.crumb.append(data)
        self.emit(data)

    def handle_entityref(self, name):
        self.emit(f"&{name};")

    def handle_charref(self, name):
        self.emit(f"&#{name};")

    def handle_comment(self, data):
        self.emit(f"<!--{data}-->")

    def handle_decl(self, decl):
        self.emit(f"<!{decl}>")

    def handle_pi(self, data):
        self.emit(f"<?{data}>")

    def content_start(self, text: str) -> int:
        """Where a page's own content begins: inside the content section, after its Imports box."""
        at = text.find('<section class="code-content"')
        if at < 0:
            raise Unexpected(f"{self.page}: no content section")
        at = text.index(">", at) + 1
        rest = text[at:]
        if rest.lstrip().startswith('<details class="imports-list"'):
            # The Imports box holds code, never another <details>, so its first close is its own.
            at += rest.index("</details>") + len("</details>")
        return at

    def result(self) -> str:
        text = "".join(self.out)
        if self.counts["lang"] != 1 and 'lang="' not in text[:300]:
            raise Unexpected(f"{self.page}: no <html> element to give a language")
        if self.essay:
            title, body = self.essay
            at = self.content_start(text)
            intro = f'<div class="ferrosa-module-intro"><h1>{render_inline(title)}</h1>\n{body}</div>'
            text = text[:at] + intro + text[at:]
            self.counts["essay"] += 1
            self.counts["h1"] += 1
            self.h1 += 1
        if self.h1 == 0:
            title = html.escape("".join(self.crumb).strip())
            if not title:
                raise Unexpected(f"{self.page}: no heading, essay or breadcrumb to title the page with")
            at = self.content_start(text)
            text = text[:at] + f'<h1 class="ferrosa-page-title">{title}</h1>' + text[at:]
            self.counts["h1"] += 1
            self.h1 += 1
        if self.h1 != 1:
            raise Unexpected(f"{self.page}: {self.h1} level-one headings, want exactly one")
        supplied = self.essay is not None or self.promoted or 'class="ferrosa-page-title"' in text
        return self.close_heading_gap(text) if supplied else text

    def close_heading_gap(self, text: str) -> str:
        """Verso renders a module doc's `##` as `<h3>`, one level below what was written, so a page
        whose `<h1>` this pass supplied jumps from `<h1>` to `<h3>` (WCAG 1.3.1, axe heading-order).
        Raise every content heading by the gap, so the highest one under the title is an `<h2>`."""
        at = self.content_start(text)
        body = text[at:]
        levels = [int(n) for n in re.findall(r"<h([2-6])[\s>]", body)]
        if not levels or min(levels) <= 2:
            return text
        gap = min(levels) - 2
        body = re.sub(r"<(/?)h([2-6])([\s>])", lambda m: f"<{m.group(1)}h{int(m.group(2)) - gap}{m.group(3)}", body)
        self.counts["raised"] = self.counts.get("raised", 0) + 1
        return text[:at] + body


def rewrite(root: Path, specs: Path) -> dict[str, int]:
    by_page = essays(specs)
    totals = {"pages": 0, "lang": 0, "li": 0, "nav": 0, "essay": 0, "h1": 0, "raised": 0}
    seen_essays = set()
    for path in sorted(root.rglob("index.html")):
        page = path.relative_to(root).as_posix()
        source = path.read_text()
        if 'class="ferrosa-nav-node' in source:
            raise Unexpected(f"{page}: already post-processed; rebuild the site first")
        essay = by_page.get(page)
        rw = Rewriter(page, essay)
        rw.feed(source)
        rw.close()
        path.write_text(rw.result())
        if essay:
            seen_essays.add(page)
        totals["pages"] += 1
        for k, v in rw.counts.items():
            totals[k] += v
    missing = set(by_page) - seen_essays
    if missing:
        raise Unexpected(f"essays with no page: {sorted(missing)}")
    return totals


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__.split("\n\n")[0], file=sys.stderr)
        return 2
    try:
        totals = rewrite(Path(argv[1]), Path(argv[2]))
    except Unexpected as err:
        print(f"postprocess: {err}", file=sys.stderr)
        return 1
    print("postprocess: " + ", ".join(f"{k}={v}" for k, v in totals.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
