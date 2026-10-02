"""The musl profiling link must not shadow Rust's own libunwind.

ferrosa's `profiling` feature turns on jemalloc's heap profiler via libunwind
(`profiling = ["tikv-jemallocator/profiling_libunwind"]`). jemalloc-sys, for that
feature, emits `cargo:rustc-link-lib=unwind` and needs `unw_backtrace`,
`unw_flush_cache` and `unw_set_caching_policy` -- none of which Rust's musl
sysroot provides.

Rust's musl sysroot DOES provide the C++ ABI unwinding symbols the Rust link
needs, in `self-contained/libunwind.a`: `_Unwind_Resume`, `__register_frame`,
`__deregister_frame`. Nothing else defines them: the link runs with
`-nostartfiles -nodefaultlibs`, so libgcc is not pulled in.

Because `-lunwind` binds to the first match on the search path, adding
`-L <build>/libunwind` in front of the sysroot makes `-lunwind` bind to the
hand-built archive instead. That archive has no `_Unwind_Resume`, so the link
fails with ~47 undefined references:

    error: linking with `musl-gcc` failed: exit status: 1
    undefined reference to `_Unwind_Resume'

Measured on a minimal crate that uses tikv-jemallocator + profiling_libunwind:
    -L <custom>/lib                              -> exit 101, 47 undefined
    -C link-arg=<custom>/lib/libunwind.a         -> exit 0

So the two libunwinds must BOTH be linked, by explicit path, so neither
displaces the other via the search path. These tests pin that.
"""

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
BUILD_LIBUNWIND = ROOT / ".github" / "scripts" / "build-musl-libunwind.sh"


def profiling_build_step() -> str:
    """The `Build optimized Ferrosa with DWARF symbols` step body."""
    workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    marker = "- name: Build optimized Ferrosa with DWARF symbols"
    assert marker in workflow, "the profiling build step is gone"
    body = workflow.split(marker, 1)[1]
    # up to the next step
    return body.split("\n      - name:", 1)[0]


def profiling_env_block() -> str:
    """Env for the profiling build step (CPPFLAGS/LDFLAGS/RUSTFLAGS)."""
    return profiling_build_step()


class ProfilingLibunwindLinkTest(unittest.TestCase):
    def test_the_custom_libunwind_directory_is_not_on_the_library_search_path(self):
        """`-L <custom>` shadows the sysroot's libunwind for `-lunwind`.

        The custom archive must be linked by explicit path instead, so the
        sysroot archive keeps resolving -lunwind and its _Unwind_Resume.
        """
        step = profiling_build_step()
        # Comments explain the rule and legitimately quote the forbidden form,
        # so strip them: only real commands can break the link.
        commands = [
            line
            for line in step.splitlines()
            if not line.lstrip().startswith("#")
        ]
        offenders = [
            line.strip()
            for line in commands
            # LDFLAGS="-L$unwind_prefix/lib" and RUSTFLAGS="-Lnative=..." both
            # put the custom dir on the search path, where it displaces -lunwind.
            if re.search(r"-L\s*\S*unwind_prefix|Lnative=\S*unwind_prefix", line)
        ]
        self.assertEqual(
            [], offenders,
            "the custom libunwind dir is on the search path, which shadows the "
            "sysroot libunwind and leaves _Unwind_Resume undefined",
        )

    def test_both_libunwinds_are_reachable_from_the_link_flags(self):
        """Neither archive may displace the other, so both are passed to the linker.

        The custom archive provides jemalloc's unw_backtrace; the sysroot archive
        provides the C++ ABI symbols. `-lunwind` can only reach one of them.

        This asserts against the value actually handed to the compiler, not the
        whole step: every path in the step also appears in the `echo`/probe
        lines, so a substring match on the step passes even when the link flags
        have lost an archive.
        """
        step = profiling_build_step()
        commands = "\n".join(
            line for line in step.splitlines()
            if not line.lstrip().startswith("#")
        )
        link_flags = "\n".join(
            line for line in commands.splitlines()
            if "RUSTFLAGS" in line or "link-arg" in line
        )
        # The custom archive is named by literal path.
        self.assertIn(
            "link-arg=$unwind_prefix/lib/libunwind.a", link_flags,
            "the hand-built libunwind is not in the link flags; it is what "
            "provides unw_backtrace for jemalloc's profiler",
        )
        # The sysroot archive is passed via the variable the resolution loop
        # fills in, so assert on that variable and on what it resolves to.
        self.assertIn(
            "link-arg=$sysroot_libunwind", link_flags,
            "the sysroot libunwind is not in the link flags; nothing else "
            "defines _Unwind_Resume under -nodefaultlibs",
        )
        self.assertIn(
            "self-contained/libunwind.a", commands,
            "the sysroot archive must be resolved to self-contained/libunwind.a",
        )

    def test_a_missing_sysroot_libunwind_fails_loud(self):
        """If the sysroot archive is absent, say so rather than link a broken binary."""
        step = profiling_build_step()
        self.assertIn(
            "no sysroot libunwind",
            step,
            "a missing sysroot libunwind must fail loudly: the alternative is a "
            "link error nobody can attribute",
        )

    def test_the_build_script_records_why_libunwind_is_needed_for_musl(self):
        """The next person must not delete this as redundant.

        Rust's sysroot libunwind looks sufficient (it defines the unwind
        symbols) but lacks unw_backtrace, which jemalloc's profiler calls.
        """
        script = BUILD_LIBUNWIND.read_text(encoding="utf-8")
        self.assertIn("profiling_libunwind", script)
        self.assertIn("unw_backtrace", script)


if __name__ == "__main__":
    unittest.main()
