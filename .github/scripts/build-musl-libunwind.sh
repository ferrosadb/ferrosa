#!/usr/bin/env bash
# Build a musl-targeted libunwind for ferrosa's profiling image.
#
# WHY THIS EXISTS. ferrosa's `profiling` feature enables jemalloc's heap
# profiler with libunwind (`profiling = ["tikv-jemallocator/profiling_libunwind"]`).
# For that feature jemalloc-sys runs configure with `--enable-prof-libunwind` and
# emits `cargo:rustc-link-lib=unwind`, and jemalloc then calls the libunwind C
# API: `unw_backtrace`, `unw_flush_cache`, `unw_set_caching_policy`.
#
# Rust's musl sysroot ships its own self-contained/libunwind.a, but that archive
# does NOT define those three. It defines the C++ ABI symbols instead
# (_Unwind_Resume, __register_frame, __deregister_frame), which the Rust link
# needs because it runs with `-nodefaultlibs` and so never pulls in libgcc.
#
# So BOTH archives are required, and neither may go on the library search path:
# `-lunwind` binds to the first match, so a `-L` for this one shadows the
# sysroot's, and the link then fails with ~47 `undefined reference to
# _Unwind_Resume`. The release workflow links both by explicit path for this
# reason -- see tests/ci/test_profiling_libunwind_link.py.
#
# Do not delete this script as redundant: the sysroot archive looks sufficient
# (it defines the unwind symbols) but is missing unw_backtrace.
set -euo pipefail

target=${1:?usage: build-musl-libunwind.sh TARGET PREFIX}
prefix=${2:?usage: build-musl-libunwind.sh TARGET PREFIX}
version=1.8.1
sha256=ddf0e32dd5fafe5283198d37e4bf9decf7ba1770b6e7e006c33e6df79e6a6157

case "$target" in
  x86_64-unknown-linux-musl)
    host=x86_64-linux-musl
    cflags=
    ;;
  aarch64-unknown-linux-musl)
    host=aarch64-linux-musl
    # -O2 is REQUIRED here, not an optimization flourish.
    #
    # Setting CFLAGS at all REPLACES autoconf's default (-g -O2) rather than
    # appending to it. At -O0 libunwind 1.8.1's aarch64 aarch64_local_resume
    # inline asm -- which names 18 general and vector registers as fixed
    # operands (Gos-linux.c:41) -- cannot satisfy the register allocator:
    #
    #   src/aarch64/Gos-linux.c:41:7: error: 'asm' operand has impossible constraints
    #
    # x86_64 is unaffected only because its cflags are empty, so autoconf's
    # -O2 survives. Verified on aarch64 Linux / GCC 13.3 / musl-gcc: with
    # CFLAGS=-mno-outline-atomics the build fails at Gos-linux.c:41; adding
    # -O2 makes it succeed. Do not drop -O2 from this line.
    cflags="-O2 -mno-outline-atomics"
    ;;
  *)
    echo "unsupported libunwind target: $target" >&2
    exit 2
    ;;
esac

build_dir=$(mktemp -d)
trap 'rm -rf "$build_dir"' EXIT
archive="$build_dir/libunwind-$version.tar.gz"
curl --fail --location --retry 3 \
  "https://github.com/libunwind/libunwind/releases/download/v$version/libunwind-$version.tar.gz" \
  --output "$archive"
printf '%s  %s\n' "$sha256" "$archive" | sha256sum --check --status
tar -xzf "$archive" -C "$build_dir"

mkdir "$build_dir/build"
(
  cd "$build_dir/build"
  CC=musl-gcc CFLAGS="$cflags" \
    "$build_dir/libunwind-$version/configure" \
      --build="$(gcc -dumpmachine)" \
      --host="$host" \
      --prefix="$prefix" \
      --enable-static \
      --disable-shared \
      --disable-tests \
      --disable-documentation \
      --disable-minidebuginfo \
      --disable-zlibdebuginfo
  make -j"$(nproc)"
  make install
)
test -s "$prefix/lib/libunwind.a"