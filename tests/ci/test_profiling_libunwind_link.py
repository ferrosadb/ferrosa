"""The musl profiling link must give jemalloc a working libunwind backtracer.

ferrosa's `profiling` feature turns on jemalloc's heap profiler via libunwind
(`profiling = ["tikv-jemallocator/profiling_libunwind"]`). Three independent
things must hold, and each was wrong at some point:

1. jemalloc's configure must FIND libunwind. `jemalloc/configure.ac` gates it on
   `AC_CHECK_HEADERS([libunwind.h])` (satisfied by CPPFLAGS) *and*
   `AC_CHECK_LIB([unwind],[unw_backtrace])` (needs a library search path, i.e.
   LDFLAGS). With headers but no `-L`, the check fails, `enable_prof_libunwind`
   silently reverts to 0, and jemalloc falls back to the JEMALLOC_PROF_GCC
   frame-pointer walker -- which segfaults on the first sampled allocation in an
   optimized (frameless) Rust build:

       prof_backtrace_impl (prof_sys.c, BT_FRAME(4)) -> SIGSEGV

   So `LDFLAGS="-L<custom>/lib"` is REQUIRED. It is consumed only by
   jemalloc-sys's configure; it is deliberately not a Rust link search path (see
   2.).

2. BOTH libunwinds are needed, linked by explicit path. The sysroot archive
   supplies the C++ ABI symbols the Rust link needs -- `_Unwind_Resume`,
   `__register_frame`, `__deregister_frame` -- which nothing else defines: the
   link runs with `-nodefaultlibs`, so libgcc is never pulled in. The hand-built
   archive supplies the libunwind C API jemalloc's profiler calls --
   `unw_backtrace`, `unw_flush_cache`, `unw_set_caching_policy` -- which the
   sysroot archive does not have. Because `-lunwind` binds to the first match on
   the search path, putting the custom dir on that path made `-lunwind` bind to
   the hand-built archive and the link died with ~47 undefined
   `_Unwind_Resume`. Hence: never `-L`/`-Lnative` the custom dir for the Rust
   link; pass both archives by explicit path.

3. Both archives AND the musl libc must sit in ONE `ld --start-group`. Pulling
   the hand-built archive's objects in makes ld require musl libc (`strcat`,
   `sigprocmask`), which under `-nodefaultlibs` is only in scope inside the
   group. Measured on x86_64 musl:

       --start-group(custom, sysroot)          -> exit 101, sigprocmask undefined
       --start-group(custom, sysroot, libc.a)  -> exit 0, unw_backtrace linked,
                                                  --version ok, .heap written

These tests pin all three.
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
    def test_jemalloc_configure_can_find_libunwind(self):
        """Without a library path jemalloc silently picks the broken PROF_GCC walker.

        `AC_CHECK_LIB([unwind],[unw_backtrace])` needs `-L`; CPPFLAGS alone only
        satisfies the header check, so `enable_prof_libunwind` reverts to 0 and
        the first sampled allocation segfaults. LDFLAGS must therefore expose the
        custom archive's directory.
        """
        step = profiling_build_step()
        commands = "\n".join(
            line for line in step.splitlines()
            if not line.lstrip().startswith("#")
        )
        self.assertIn(
            'LDFLAGS="-L$unwind_prefix/lib"', commands,
            "jemalloc's configure needs -L<custom>/lib to select libunwind; "
            "without it the profiler falls back to the frame-pointer walker, "
            "which segfaults on the first sampled allocation",
        )

    def test_the_custom_libunwind_directory_is_not_a_rust_link_search_path(self):
        """`-L`/`-Lnative` for the custom archive shadows the sysroot's libunwind.

        The custom archive must be linked by explicit path instead, so the
        sysroot archive keeps resolving the C++ ABI symbols (_Unwind_Resume).
        The jemalloc-configure `LDFLAGS=-L` is consumed by a C build script and
        is NOT a Rust link search path, so it does not count as an offender.
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
            # `-Lnative=` on RUSTFLAGS, or a link-arg `-L` carrying the custom
            # dir, would put it on the Rust link search path where it displaces
            # `-lunwind`. (The `LDFLAGS="-L..."` line is for jemalloc configure.)
            if re.search(r"Lnative=\S*unwind_prefix", line)
            or re.search(r"link-arg=-L\S*unwind_prefix", line)
        ]
        self.assertEqual(
            [], offenders,
            "the custom libunwind dir is on the Rust link search path, which "
            "shadows the sysroot libunwind and leaves _Unwind_Resume undefined",
        )

    def test_all_three_archives_are_in_one_start_group(self):
        """Custom libunwind + sysroot libunwind + musl libc, by explicit path.

        Neither libunwind may displace the other, and the hand-built archive's
        objects need musl libc (`strcat`, `sigprocmask`) which under
        `-nodefaultlibs` is only in scope inside an ld --start-group.

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
        self.assertIn("-Wl,--start-group", link_flags)
        self.assertIn("-Wl,--end-group", link_flags)
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
            "link-arg=$sysroot_libc", link_flags,
            "musl libc.a is not in the link flags; the hand-built libunwind's "
            "objects (strcat, sigprocmask) go unresolved without it",
        )
        self.assertIn(
            "self-contained/libunwind.a", commands,
            "the sysroot archive must be resolved to self-contained/libunwind.a",
        )
        self.assertIn(
            "self-contained/libc.a", commands,
            "the sysroot libc must be resolved to self-contained/libc.a",
        )

    def test_a_missing_sysroot_archive_fails_loud(self):
        """If a sysroot archive is absent, say so rather than link a broken binary."""
        step = profiling_build_step()
        self.assertIn(
            "no sysroot libunwind",
            step,
            "a missing sysroot libunwind must fail loudly: the alternative is a "
            "link error nobody can attribute",
        )
        self.assertIn(
            "no sysroot libc.a",
            step,
            "a missing sysroot libc.a must fail loudly: without it the "
            "hand-built libunwind's musl-libc references go unresolved",
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
