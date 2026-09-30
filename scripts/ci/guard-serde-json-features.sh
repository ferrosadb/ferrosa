#!/bin/bash
# Fails when any crate in the workspace enables a forbidden serde_json feature.
#   arbitrary_precision  changes Number's representation workspace-wide (hazard M8)
#   preserve_order       changes Map to an insertion-ordered map (hazard M8)
#   unbounded_depth      removes the recursion limit (hazard M12)
# Cargo unifies features, so one dependent enabling one of these flips it for
# every crate, including the jsonb value model.
#
# Usage: guard-serde-json-features.sh
# Env:   GUARD_MANIFEST_PATH  optional Cargo.toml to scan (used by the tests)
# Exit:  0 clean, 1 forbidden feature found, 2 input missing or unreadable.
set -u
FORBIDDEN='arbitrary_precision|preserve_order|unbounded_depth'
args=(tree -e features -i serde_json --workspace --all-features --prefix none)
[ -n "${GUARD_MANIFEST_PATH:-}" ] && args+=(--manifest-path "$GUARD_MANIFEST_PATH")

if ! out=$(cargo "${args[@]}" 2>&1); then
  echo "guard-serde-json-features: cargo tree failed:" >&2
  echo "$out" | tail -5 >&2
  exit 2
fi
# Fail loud on an empty or wrong-shaped result: a guard that reads nothing passes nothing.
if ! echo "$out" | grep -q '^serde_json v'; then
  echo "guard-serde-json-features: serde_json is not in the dependency graph; the guard has no input" >&2
  exit 2
fi
if ! echo "$out" | grep -q '^serde_json feature "std"'; then
  echo "guard-serde-json-features: no 'serde_json feature' lines in cargo tree output; format changed?" >&2
  exit 2
fi
bad=$(echo "$out" | grep -E "^serde_json feature \"($FORBIDDEN)\"" | sort -u)
if [ -n "$bad" ]; then
  echo "guard-serde-json-features: forbidden serde_json feature enabled:" >&2
  echo "$bad" >&2
  echo "Find the enabler: cargo tree -e features -i serde_json --workspace --all-features" >&2
  exit 1
fi
echo "guard-serde-json-features: ok (no $FORBIDDEN)"
