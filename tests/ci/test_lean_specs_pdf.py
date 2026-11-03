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
