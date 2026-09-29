#!/bin/bash
# Fails when a crate excluded from the root workspace has no CI job that runs it.
#
# `cargo ... --workspace` never reaches a path listed under `exclude` in the root
# Cargo.toml, so such a crate (e.g. ferrosa-jsonb-conformance) only runs if a job
# names its manifest explicitly. This guard requires every excluded path that
# contains a Cargo.toml to be referenced as `--manifest-path <path>/Cargo.toml`
# in a workflow under .github/workflows/.
#
# Env:  GUARD_ROOT  repo root to scan (default: the repo containing this script)
# Exit: 0 every excluded crate has CI, 1 at least one has none,
#       2 the exclude list could not be read or parsed (never passes silently).
set -u
root="${GUARD_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
manifest="$root/Cargo.toml"
if [ ! -f "$manifest" ]; then
  echo "guard-excluded-crates: $manifest not found" >&2
  exit 2
fi
if ! excluded=$(python3 - "$manifest" <<'PY'
import sys
try:
    import tomllib
except ImportError:
    sys.exit("python3 >= 3.11 (tomllib) required")
with open(sys.argv[1], "rb") as f:
    data = tomllib.load(f)
ws = data.get("workspace")
if not isinstance(ws, dict):
    sys.exit("no [workspace] table")
if "exclude" not in ws:
    sys.exit("no workspace.exclude key")
ex = ws["exclude"]
if not isinstance(ex, list) or not all(isinstance(e, str) for e in ex):
    sys.exit("workspace.exclude is not a list of strings")
print("\n".join(ex))
PY
); then
  echo "guard-excluded-crates: could not parse workspace.exclude in $manifest" >&2
  exit 2
fi
rc=0
checked=0
while IFS= read -r path; do
  [ -z "$path" ] && continue
  [ -f "$root/$path/Cargo.toml" ] || continue
  checked=$((checked + 1))
  if cat "$root"/.github/workflows/*.yml 2>/dev/null \
      | grep -Fq -- "--manifest-path $path/Cargo.toml"; then
    echo "guard-excluded-crates: $path ok"
  else
    echo "guard-excluded-crates: $path is excluded from the root workspace but no" >&2
    echo "  workflow runs it; add a job using --manifest-path $path/Cargo.toml" >&2
    rc=1
  fi
done <<< "$excluded"
echo "guard-excluded-crates: checked $checked excluded crate(s)"
exit $rc
