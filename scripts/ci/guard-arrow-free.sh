#!/bin/bash
# Fails when any listed crate's dependency graph reaches an arrow* or parquet*
# crate (hazards M8 / D5a): the value model crates must stay arrow-free.
#
# Usage: guard-arrow-free.sh <crate> [<crate>...]
# Env:   GUARD_MANIFEST_PATH  optional Cargo.toml to scan (used by the tests)
# Exit:  0 clean, 1 arrow/parquet reached, 2 bad usage or input missing.
set -u
if [ "$#" -lt 1 ]; then
  echo "usage: guard-arrow-free.sh <crate> [<crate>...]" >&2
  exit 2
fi
rc=0
for crate in "$@"; do
  args=(tree -p "$crate" --all-features --prefix none)
  [ -n "${GUARD_MANIFEST_PATH:-}" ] && args+=(--manifest-path "$GUARD_MANIFEST_PATH")
  if ! out=$(cargo "${args[@]}" 2>&1); then
    echo "guard-arrow-free: cargo tree -p $crate failed:" >&2
    echo "$out" | tail -5 >&2
    exit 2
  fi
  # The crate itself must be the first line; otherwise the input is not what we think.
  if ! echo "$out" | head -1 | grep -q "^$crate v"; then
    echo "guard-arrow-free: cargo tree -p $crate did not start with the crate; no input" >&2
    exit 2
  fi
  bad=$(echo "$out" | grep -E '^(arrow|parquet)[A-Za-z0-9_-]* v' | sort -u)
  if [ -n "$bad" ]; then
    echo "guard-arrow-free: $crate reaches arrow/parquet:" >&2
    echo "$bad" >&2
    echo "Find the path: cargo tree -p $crate --all-features -i arrow-array" >&2
    rc=1
  else
    echo "guard-arrow-free: $crate ok"
  fi
done
exit $rc
