#!/usr/bin/env bash
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
    cflags=-mno-outline-atomics
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
