#!/usr/bin/env bash
# Fail when a committed extraction no longer matches the Rust it was produced from.
#
# Seconds, no charon, no aeneas: it compares the source hash recorded by `extract.sh` against the
# sources as they are now. That is the whole of "stays in sync" -- a generated file whose sources
# moved is a file that is making claims about code that no longer exists, and nothing in `lake
# build` can see it.
#
# Two further states are reported rather than passed silently. A row whose status is
# `stale:<reason>` is an extraction that cannot be regenerated under the current toolchain, and the
# reason names what blocks it. A row extracted at a different aeneas than the one installed is a
# translator that moved under a committed file; the recorded content may no longer be reproducible.
#
# Usage: check-sync.sh            exit 0 in sync, 1 on drift
set -uo pipefail
cd "$(dirname "$0")" || exit 2

source_hash() {
  ( cd "$1" && find $2 -type f -name '*.rs' -print0 2>/dev/null \
      | sort -z | xargs -0 sha256sum 2>/dev/null | sha256sum | cut -d' ' -f1 )
}

root=$(git -C "$PWD" rev-parse --show-toplevel) || exit 2
current_aeneas=$(git -C "$HOME/src/aeneas" rev-parse --short HEAD 2>/dev/null || echo unknown)
drift=0
stale=0
checked=0

while read -r line; do
  case "$line" in ''|'#'*) continue ;; esac
  # module crate hash sources scope aeneas <id> charon <id> status...
  read -r module crate hash sources scope _aeneas record_aeneas _charon _cid status <<<"$line"
  [ -n "${status:-}" ] || status=ok
  checked=$((checked + 1))

  case "$status" in
    stale:*)
      stale=$((stale + 1))
      echo "STALE   $module: ${status#stale:}"
      continue ;;
  esac

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
  if [ "$record_aeneas" != "$current_aeneas" ]; then
    echo "TOOL    $module: extracted at aeneas $record_aeneas, current is $current_aeneas"
    echo "        the translator moved under a committed extraction; regenerate to compare"
    drift=1
  fi
done < sync.lock

if [ $checked -eq 0 ]; then
  echo "FAIL: sync.lock records no modules"
  exit 1
fi

if [ $drift -eq 0 ] && [ $stale -eq 0 ]; then
  echo "in sync: $checked extracted modules match their sources"
elif [ $drift -eq 0 ]; then
  echo "sources in sync: $checked modules, $stale cannot be regenerated (see STALE above)"
fi
exit $drift
