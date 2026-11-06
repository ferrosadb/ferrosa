"""The specification's PDF is the site, printed.

Two things are pinned here, because both failed in review once:

1. `lean-specs.yml` must print the site rather than re-render the corpus. A
   second renderer is the defect this change removed, so the way it comes back
   is one step at a time: a leftover `render.py`, a tectonic download to keep a
   "fallback" alive, an artifact path pointing at the old build directory.
2. `site/print.py` must take the chapter order from the module tree Verso
   rendered, not from a list of its own. If it ever reads an order it keeps
   separately, the paper can disagree with the web and nothing notices.

The assembler is exercised on a synthetic site so the test cannot pass because
of a stale `.lake` directory.
"""

import importlib.util
import re
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WORKFLOW = REPO / ".github" / "workflows" / "lean-specs.yml"
PRINT_PY = REPO / "specs" / "lean4" / "site" / "print.py"


def load_print_module():
    spec = importlib.util.spec_from_file_location("ferrosa_print", PRINT_PY)
    assert spec is not None and spec.loader is not None, f"cannot load {PRINT_PY}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def page(body: str, *, scripts: bool = True) -> str:
    script = "<script>console.log('review widget')</script>" if scripts else ""
    return (
        "<html><body>"
        '<aside class="sidebar">nav</aside>'
        f'<main class="main-area" id="main-content">{body}</main>'
        f"{script}"
        "</body></html>"
    )


def load_postprocess_module():
    renderer = REPO / "specs" / "lean4" / "site" / "postprocess.py"
    spec = importlib.util.spec_from_file_location("ferrosa_postprocess", renderer)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class OpeningEssayUsesTheExistingRenderer(unittest.TestCase):
    def test_projection_essay_renders_without_losing_its_policy_names(self):
        module = load_postprocess_module()
        source = (REPO / "specs" / "lean4" / "Ferrosa" / "SystemTables" / "Projection.lean").read_text()
        header = source.split("/-", 1)[1].split("-/", 1)[0]
        title, body = module.render_essay("Ferrosa.SystemTables.Projection", header)
        self.assertIn("projection layer", title)
        for policy in ("timestampOnly", "timestampSeq", "seqFirst"):
            self.assertIn(f"<code>{policy}</code>", body)
        self.assertIn("merge_rows_into", body)
        self.assertIn("reconcile_ref", body)


    def test_quarantine_essay_renders_headings_and_wrapped_list(self):
        module = load_postprocess_module()
        source = (REPO / "specs" / "lean4" / "Ferrosa" / "Raft" / "Quarantine.lean").read_text()
        header = source.split("/-", 1)[1].split("-/", 1)[0]
        title, body = module.render_essay("Ferrosa.Raft.Quarantine", header)
        self.assertIn("Quarantine and online repair", title)
        self.assertIn("<h2>What the code has today, and what it does not</h2>", body)
        self.assertIn("<h2>What is modelled, and at what abstraction</h2>", body)
        self.assertEqual(body.count("<ul>"), 1)
        self.assertEqual(body.count("<li>"), 10)
        self.assertIn("<code>synced</code> — what the <code>Schema</code>", body)
        self.assertIn("once preserved, awaiting an explicit decision.</li>", body)


    def test_supported_blocks_escape_html_and_refuse_other_markdown(self):
        module = load_postprocess_module()
        _, body = module.render_essay("safe", "Title\n=====\n\n## <script> `x`\n\n* **bold** & <tag>\n  wrapped\n* second\n\nparagraph")
        self.assertEqual(body, "<h2>&lt;script&gt; <code>x</code></h2>\n<ul>\n<li><strong>bold</strong> &amp; &lt;tag&gt; wrapped</li>\n<li>second</li>\n</ul>\n<p>paragraph</p>")
        for unsupported in ("# top heading", "1. item\n  2. nested", "> quote", "```python", "| table |", "* item\n  * nested"):
            with self.subTest(unsupported=unsupported):
                with self.assertRaises(module.Unexpected):
                    module.render_essay("strict", "Title\n=====\n\n" + unsupported)

    def test_fenced_code_preserves_bytes_and_escapes_html(self):
        module = load_postprocess_module()
        _, body = module.render_essay("code", "Title\n=====\n\n```text\n<script>& `code` **literal**\n  indented\n```\n\nparagraph")
        self.assertEqual(body, '<pre><code class="language-text">&lt;script&gt;&amp; `code` **literal**\n  indented\n</code></pre>\n<p>paragraph</p>')
        for bad in ("```text\nunclosed", '```text onclick="bad"\nx\n```'):
            with self.subTest(bad=bad), self.assertRaises(module.Unexpected):
                module.render_essay("strict", "Title\n=====\n\n" + bad)

    def test_ordered_lists_keep_numbering_wrapped_text_and_block_boundaries(self):
        module = load_postprocess_module()
        _, body = module.render_essay("list", "Title\n=====\n\n3. **first**\n   wrapped\n4. `second`\n7. <last>\n\n* bullet\n\nparagraph")
        self.assertEqual(body, '<ol start="3">\n<li><strong>first</strong> wrapped</li>\n<li><code>second</code></li>\n<li value="7">&lt;last&gt;</li>\n</ol>\n<ul>\n<li>bullet</li>\n</ul>\n<p>paragraph</p>')

    def test_current_chapter_headers_use_supported_blocks(self):
        module = load_postprocess_module()
        checked = 0
        for path in (REPO / "specs" / "lean4" / "Ferrosa").rglob("*.lean"):
            source = path.read_text()
            if "/-" not in source:
                continue
            header = source.split("/-", 1)[1].split("-/", 1)[0]
            if not re.search(r"\n\s*[=-]{3,}\s*\n", header):
                continue
            with self.subTest(path=str(path.relative_to(REPO))):
                module.render_essay(str(path), header)
            checked += 1
        self.assertGreater(checked, 0)


class WorkflowPrintsTheSite(unittest.TestCase):
    def setUp(self):
        self.workflow = WORKFLOW.read_text()

    def test_no_latex_renderer_survives(self):
        for marker in ("render.py", "tectonic", "texlive", "latexmk", "ferrosa-specs.pdf"):
            self.assertNotIn(marker, self.workflow, f"{marker} still in lean-specs.yml")

    def test_the_pdf_is_printed_from_the_site(self):
        self.assertIn("make pdf", self.workflow)
        self.assertIn("working-directory: specs/lean4/site", self.workflow)

    def test_the_page_count_check_installs_its_tool(self):
        # `pdfinfo` is poppler-utils and is not on the runner image. Asking for it without
        # installing it fails the job at the assertion, which reads as a broken PDF.
        if "pdfinfo" in self.workflow:
            self.assertIn("poppler-utils", self.workflow)

    def test_the_artifact_is_the_printed_site(self):
        self.assertIn("specs/lean4/site/.lake/build/specs.pdf", self.workflow)

    def test_the_export_target_is_renamed(self):
        # `lake exe docs` went away with the book's LaTeX half; the data target is `export`.
        self.assertNotIn("lake exe docs", self.workflow)
        self.assertIn("lake exe export", self.workflow)


class AssemblesThePagesVersoOrdered(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.site = Path(self.tmp.name) / "site"
        self.site.mkdir()
        self.mod = load_print_module()

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, rel: str, text: str) -> None:
        target = self.site / rel / "index.html"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def tree(self, *hrefs: str) -> None:
        links = "".join(f'<a href="{h}">{h}</a>' for h in hrefs)
        (self.site / "index.html").write_text(
            f'<nav class="module-tree">{links}</nav>'
        )

    def test_order_comes_from_the_module_tree(self):
        # The tree says second-then-first; the assembler must not "fix" it into
        # alphabetical order, because the tree is what a reader sees.
        self.tree("Ferrosa/Second/", "Ferrosa/First/")
        self.write("Ferrosa/First", page("<h1>first</h1>", scripts=False))
        self.write("Ferrosa/Second", page("<h1>second</h1>", scripts=False))
        out = self.site / "out.html"
        self.mod.main(["print.py", str(self.site), str(out)])
        doc = out.read_text()
        # Match the heading, not the bare word: the print stylesheet carries `:first-of-type`,
        # so a bare `index("first")` finds CSS in <head> instead of the chapter.
        self.assertLess(doc.index("<h1>second</h1>"), doc.index("<h1>first</h1>"))

    def test_a_page_outside_the_tree_is_not_in_the_book(self):
        self.tree("Ferrosa/InBook/")
        self.write("Ferrosa/InBook", page("<h1>in</h1>", scripts=False))
        self.write("Ferrosa/Orphan", page("<h1>orphan</h1>", scripts=False))
        out = self.site / "out.html"
        self.mod.main(["print.py", str(self.site), str(out)])
        self.assertNotIn("orphan", out.read_text())

    def test_navigation_and_scripts_are_not_printed(self):
        self.tree("Ferrosa/One/")
        self.write(
            "Ferrosa/One",
            page('<h1>chapter</h1><a href="../Other/">see elsewhere</a>'),
        )
        out = self.site / "out.html"
        self.mod.main(["print.py", str(self.site), str(out)])
        doc = out.read_text()
        self.assertIn("chapter", doc)
        self.assertIn("see elsewhere", doc)      # the words stay
        self.assertNotIn("sidebar", doc)
        self.assertNotIn("<script", doc)
        self.assertNotIn('href="../Other/"', doc)  # the dead link does not

    def test_a_missing_page_is_skipped_not_fatal(self):
        self.tree("Ferrosa/Present/", "Ferrosa/Absent/")
        self.write("Ferrosa/Present", page("<h1>present</h1>", scripts=False))
        out = self.site / "out.html"
        self.assertEqual(self.mod.main(["print.py", str(self.site), str(out)]), 0)
        self.assertIn("present", out.read_text())


if __name__ == "__main__":
    unittest.main()
