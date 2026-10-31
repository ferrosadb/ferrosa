#!/usr/bin/env bash
# Fail when a committed extraction no longer matches the Rust it was produced from.
#
# Seconds, no charon, no aeneas: it compares the source hash recorded by `extract.sh` against the
# sources as they are now. That is the whole of "stays in sync" -- a generated file whose sources
# moved is a file that is making claims about code that no longer exists, and nothing in `lake
# build` can see it.
#
# Usage: check-sync.sh            exit 0 in sync, 1 on drift
set -uo pipefail
cd "$(dirname "$0")" || exit 2

source_hash() {
  ( cd "$1" && find $2 -type f -name '*.rs' -print0 2>/dev/null \
      | sort -z | xargs -0 sha256sum 2>/dev/null | sha256sum | cut -d' ' -f1 )
}

root=$(git -C "$PWD" rev-parse --show-toplevel) || exit 2
drift=0
checked=0

while read -r module crate hash sources scope tools; do
  case "$module" in ''|'#'*) continue ;; esac
  checked=$((checked + 1))
  if [ ! -f "FerrosaRust/$module.lean" ]; then
    echo "MISSING FerrosaRust/$module.lean (recorded in sync.lock)"
    drift=1
    continue
  fi
  now=$(source_hash "$root/$crate" "$sources")
  if [ "$now" != "$hash" ]; then
    echo "DRIFT   $module: $crate/$sources changed since the extraction was recorded"
    echo "        run: specs/lean4-rust/extract.sh $module"
    drift=1
  fi
done < sync.lock

if [ $checked -eq 0 ]; then
  echo "FAIL: sync.lock records no modules"
  exit 1
fi

if [ $drift -eq 0 ]; then
  echo "in sync: $checked extracted modules match their sources"
fi
exit $drift
