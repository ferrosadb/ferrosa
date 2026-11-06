"""Contract tests for scripts/ci-gates/guard-unsafe.py.

The invariant is docs/unsafe-policy.md: no `unsafe` except where the platform forces it, never for
performance. A gate that only ever says "clean" is not a gate, so every failure class gets a test
that builds a workspace it MUST reject, and every exclusion (`#[cfg(test)]`) gets a test that shows
what must NOT be counted.

Run with:  python3 -m unittest -q tests/ci/test_guard_unsafe.py
"""

from __future__ import annotations

import importlib.util
import shutil
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GUARD = REPO / "scripts" / "ci-gates" / "guard-unsafe.py"

# A SAFETY comment that names a mechanism, and one that only restates the call.
MECHANISM = (
    "// SAFETY: the `File` owns the descriptor for the duration of this call, so the fd cannot be\n"
    "// closed underneath it and no pointer argument is dereferenced.\n"
)
RESTATEMENT = "// SAFETY: getrlimit is a standard POSIX call.\n"


def load_guard():
    """Import the hyphenated guard script. `sys.modules` must be populated before exec: the module
    declares a dataclass, and dataclasses resolve string annotations through `sys.modules`."""
    spec = importlib.util.spec_from_file_location("guard_unsafe", GUARD)
    module = importlib.util.module_from_spec(spec)
    sys.modules["guard_unsafe"] = module
    spec.loader.exec_module(module)
    return module


guard = load_guard()


def allow_entry(file, item, kind, reason="the File owns the fd for the call", owner="t",
                since="2026-11-04", table="allow", extra=""):
    return (
        f"\n[[{table}]]\n"
        f'file   = "{file}"\n'
        f'item   = "{item}"\n'
        f'kind   = "{kind}"\n'
        f'reason = "{reason}"\n'
        f'owner  = "{owner}"\n'
        f'since  = "{since}"\n'
        f"{extra}"
    )


class GuardCase(unittest.TestCase):
    def repo(self, files: dict[str, str], *entries: str) -> Path:
        root = Path(tempfile.mkdtemp(prefix="guard-unsafe-"))
        self.addCleanup(shutil.rmtree, root, True)
        (root / "Cargo.toml").write_text("[workspace]\nmembers = []\n", encoding="utf-8")
        for rel, text in files.items():
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(textwrap.dedent(text), encoding="utf-8")
        allow = [e for e in entries if "[[allow]]" in e]
        debt = [e for e in entries if "[[debt]]" in e]
        target = root / "scripts" / "ci-gates"
        target.mkdir(parents=True, exist_ok=True)
        (target / "unsafe-allow.toml").write_text("\n".join(allow + debt), encoding="utf-8")
        return root

    def problems(self, root: Path) -> str:
        found, _sites, _debt = guard.check(root)
        return "\n".join(found)

    def assertClean(self, root: Path) -> None:
        found, sites, _debt = guard.check(root)
        self.assertEqual(found, [], f"expected clean, got:\n{found}")
        self.assertEqual(len(sites), 1)


class Listing(GuardCase):
    def test_a_listed_site_with_a_mechanism_passes(self):
        root = self.repo(
            {
                "crate/src/io.rs": f"""
                    pub fn thing(fd: i32) -> i32 {{
                        {MECHANISM}    unsafe {{ libc::fcntl(fd, 0) }}
                    }}
                """
            },
            allow_entry("crate/src/io.rs", "thing", "syscall"),
        )
        self.assertClean(root)

    def test_unlisted_site_fails(self):
        root = self.repo(
            {"crate/src/io.rs": f"pub fn thing(fd: i32) -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n"}
        )
        self.assertIn("unlisted", self.problems(root))

    def test_stale_entry_fails(self):
        root = self.repo(
            {"crate/src/io.rs": "pub fn thing() -> i32 { 0 }\n"},
            allow_entry("crate/src/io.rs", "thing", "syscall"),
        )
        self.assertIn("stale", self.problems(root))

    def test_duplicate_entries_fail(self):
        root = self.repo(
            {"crate/src/io.rs": f"pub fn thing() -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n"},
            allow_entry("crate/src/io.rs", "thing", "syscall"),
            allow_entry("crate/src/io.rs", "thing", "syscall"),
        )
        self.assertIn("duplicate", self.problems(root))


class Kinds(GuardCase):
    def test_perf_kind_is_rejected_from_allow(self):
        root = self.repo(
            {"crate/src/io.rs": f"pub fn thing() -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n"},
            allow_entry("crate/src/io.rs", "thing", "perf"),
        )
        message = self.problems(root)
        self.assertIn("perf", message)
        self.assertIn("finding, not a permitted kind", message)

    def test_unknown_kind_is_rejected_from_allow(self):
        root = self.repo(
            {"crate/src/io.rs": f"pub fn thing() -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n"},
            allow_entry("crate/src/io.rs", "thing", "unknown"),
        )
        self.assertIn("finding, not a permitted kind", self.problems(root))

    def test_an_invented_kind_is_rejected(self):
        root = self.repo(
            {"crate/src/io.rs": f"pub fn thing() -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n"},
            allow_entry("crate/src/io.rs", "thing", "speed"),
        )
        self.assertIn("has kind 'speed'", self.problems(root))

    def test_perf_is_allowed_as_recorded_debt(self):
        root = self.repo(
            {"crate/src/io.rs": f"pub fn thing() -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n"},
            allow_entry(
                "crate/src/io.rs", "thing", "perf", table="debt",
                extra='fix    = "an iterator instead of a raw pointer walk"\n',
            ),
        )
        found, _sites, debt = guard.check(root)
        self.assertEqual(found, [])
        self.assertEqual(debt, 1)

    def test_debt_without_a_fix_fails(self):
        root = self.repo(
            {"crate/src/io.rs": f"pub fn thing() -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n"},
            allow_entry("crate/src/io.rs", "thing", "perf", table="debt"),
        )
        self.assertIn("missing-field", self.problems(root))
        self.assertIn("fix", self.problems(root))

    def test_debt_with_a_permitted_kind_fails(self):
        root = self.repo(
            {"crate/src/io.rs": f"pub fn thing() -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n"},
            allow_entry(
                "crate/src/io.rs", "thing", "syscall", table="debt",
                extra='fix    = "nothing to fix"\n',
            ),
        )
        self.assertIn("not-a-finding", self.problems(root))


class Fields(GuardCase):
    def test_every_required_field_is_enforced(self):
        for field in ("file", "item", "kind", "reason", "owner", "since"):
            with self.subTest(field=field):
                entry = allow_entry("crate/src/io.rs", "thing", "syscall")
                kept = [line for line in entry.splitlines() if not line.strip().startswith(field)]
                self.assertTrue(len(kept) < len(entry.splitlines()), f"no {field} line to drop")
                root = self.repo(
                    {"crate/src/io.rs": f"pub fn thing() -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n"},
                )
                (root / "scripts/ci-gates/unsafe-allow.toml").write_text("\n".join(kept))
                found = self.problems(root)
                # Dropping a field must be reported, never silently accepted.
                self.assertIn("missing-field", found)
                self.assertIn(field, found)


class SafetyComments(GuardCase):
    def test_missing_safety_comment_fails(self):
        root = self.repo(
            {"crate/src/io.rs": "pub fn thing() -> i32 {\n    unsafe { 1 }\n}\n"},
            allow_entry("crate/src/io.rs", "thing", "syscall"),
        )
        self.assertIn("no-safety-comment", self.problems(root))

    def test_restating_the_call_is_not_a_mechanism(self):
        root = self.repo(
            {"crate/src/io.rs": f"pub fn thing() -> i32 {{\n    {RESTATEMENT}    unsafe {{ 1 }}\n}}\n"},
            allow_entry("crate/src/io.rs", "thing", "syscall"),
        )
        self.assertIn("weak-safety-comment", self.problems(root))

    def test_a_two_line_mechanism_is_one_comment(self):
        self.assertTrue(guard.safety_is_mechanism(MECHANISM.strip()))
        self.assertFalse(guard.safety_is_mechanism(RESTATEMENT.strip()))

    def test_debt_sites_are_not_required_to_carry_a_comment(self):
        # A debt entry is a finding *because* its justification is absent or invalid; requiring the
        # comment it lacks would make the two classes contradict each other.
        root = self.repo(
            {"crate/src/io.rs": "pub fn thing() -> i32 {\n    unsafe { 1 }\n}\n"},
            allow_entry(
                "crate/src/io.rs", "thing", "perf", table="debt",
                extra='fix    = "an iterator instead of a raw pointer walk"\n',
            ),
        )
        found, _sites, _debt = guard.check(root)
        self.assertEqual(found, [])


class Sealing(GuardCase):
    def test_unsafe_in_a_sealed_crate_fails(self):
        root = self.repo(
            {
                "crate/src/lib.rs": "#![forbid(unsafe_code)]\npub mod io;\n",
                "crate/src/io.rs": f"pub fn thing() -> i32 {{\n{MECHANISM}unsafe {{ 1 }}\n}}\n",
            },
            allow_entry("crate/src/io.rs", "thing", "syscall"),
        )
        self.assertIn("sealed", self.problems(root))


class CfgTest(GuardCase):
    """`#[cfg(test)]` bodies are not production sites. Each shape has bitten before."""

    def test_inline_test_module_is_not_counted(self):
        root = self.repo(
            {
                "crate/src/io.rs": f"""
                    pub fn thing() -> i32 {{ 0 }}
                    #[cfg(test)]
                    mod tests {{
                        #[test]
                        fn t() {{ unsafe {{ std::env::remove_var("X") }} }}
                    }}
                """
            }
        )
        self.assertEqual(self.problems(root), "")
        self.assertEqual(guard.collect(root), [])

    def test_test_module_in_another_file_is_not_counted(self):
        root = self.repo(
            {
                "crate/src/lib.rs": "pub mod io;\n#[cfg(test)]\nmod support;\n",
                "crate/src/io.rs": "pub fn thing() -> i32 { 0 }\n",
                "crate/src/support.rs": "pub fn helper() { unsafe { 1 } }\n",
            }
        )
        self.assertEqual(self.problems(root), "")

    def test_test_only_impl_is_not_counted(self):
        # `#[cfg(test)] impl Schema` — the shape the older scanner in
        # tests/ci/test_unsafe_ledger.py missed, because it only matched `#[cfg(test)] mod`.
        root = self.repo(
            {
                "crate/src/registry.rs": """
                    pub struct Schema;
                    #[cfg(test)]
                    impl Schema {
                        pub fn new_for_test() -> Self {
                            unsafe { std::env::remove_var("FERROSA_SUPERUSER_PASSWORD") }
                            Schema
                        }
                    }
                """
            }
        )
        self.assertEqual(guard.collect(root), [])

    def test_cfg_test_file_itself_is_not_counted(self):
        root = self.repo({"crate/src/lib.rs": "#[cfg(test)]\nmod only_tests;\n",
                          "crate/src/only_tests.rs": "fn t() { unsafe { 1 } }\n"})
        self.assertEqual(guard.collect(root), [])


class ItemResolution(GuardCase):
    def test_a_multi_line_signature_still_resolves_the_item(self):
        # The allowlist keys on the enclosing item, not a line number, so an entry must survive a
        # parameter being added. That only works if a signature spanning lines resolves to its name.
        root = self.repo(
            {
                "crate/src/io.rs": f"""
                    pub fn write_buffers(
                        &self,
                        bufs: &[&[u8]],
                        offset: u64,
                    ) -> usize {{
                        {MECHANISM}        unsafe {{ libc::pwritev(0, std::ptr::null(), 0, 0) }}
                    }}
                """
            },
            allow_entry("crate/src/io.rs", "write_buffers", "syscall"),
        )
        self.assertClean(root)

    def test_generics_are_stripped_from_an_impl_name(self):
        self.assertEqual(
            guard.item_name("unsafe impl<'a, R: ReadAt + Send + Sync> Send for RangeMerger<'a, R> {}"),
            "unsafe impl Send for RangeMerger",
        )

    def test_unsafe_inside_a_string_literal_is_not_a_site(self):
        # A doc example or a message string containing `unsafe {` is not a site, and a brace inside
        # a literal must not shift the depth of every line after it.
        root = self.repo(
            {
                "crate/src/io.rs": f"""
                    pub const WARNING: &str = "never write unsafe {{ in a doc example }}";
                    pub fn noisy() -> &'static str {{ "}}" }}
                    pub fn thing() -> i32 {{
                        {MECHANISM}    unsafe {{ 1 }}
                    }}
                """
            },
            allow_entry("crate/src/io.rs", "thing", "syscall"),
        )
        self.assertClean(root)


class RealTree(GuardCase):
    def test_this_repository_is_clean(self):
        found, sites, debt = guard.check(REPO)
        self.assertEqual(found, [], "the unsafe gate is red on this checkout:\n" + "\n".join(found))
        self.assertGreater(len(sites), 0, "the scanner found no sites at all — it is not scanning")
        self.assertLessEqual(debt, len(sites))

    def test_every_sealed_crate_really_has_no_sites(self):
        sealed = guard.sealed_crates(REPO)
        self.assertGreater(len(sealed), 10, f"sealed-crate detection looks broken: {sorted(sealed)}")
        for site in guard.collect(REPO):
            crate = site.file.split("/src/")[0]
            self.assertNotIn(crate, sealed, f"{site.file}:{site.line} is unsafe in sealed {crate}")


if __name__ == "__main__":
    unittest.main()
