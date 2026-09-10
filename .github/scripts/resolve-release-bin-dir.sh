#!/usr/bin/env bash
# Print the directory holding the freshly-built release binaries.
#
# `cargo build --release` does not have one answer for where it writes. The
# plain case is <target-dir>/release, but a `build.target` in any cargo config
# (or a --target flag) moves it to <target-dir>/<triple>/release, and
# CARGO_TARGET_DIR or `build.target-dir` move <target-dir> itself. All of those
# are ordinary configuration on a developer machine.
#
# The install-smoke build hardcoded `target/release` and so worked on the
# hosted runners and on the self-hosted macOS box, and failed on the
# self-hosted Linux builder with:
#
#     Finished `release` profile [optimized + debuginfo] target(s) in 1m 19s
#     ERROR: missing ferrosa binary at target/release/ferrosa
#
# — a successful build reported as a missing binary, because the caller was
# guessing the layout of whichever machine it happened to run on.
#
# Usage:
#   resolve-release-bin-dir.sh <target-triple> [binary...]
# Prints the directory on stdout; exits non-zero, saying what it found, if the
# binaries are not where cargo should have put them.

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "ERROR: usage: $0 <target-triple> [binary...]" >&2
  exit 2
fi

TARGET="$1"
shift
if [[ $# -gt 0 ]]; then
  BINS=("$@")
else
  BINS=(ferrosa ferrosa-ctl)
fi

# Ask cargo where its target directory is. `target_directory` accounts for
# CARGO_TARGET_DIR and for `build.target-dir` in any config file that applies,
# which guessing from the environment alone does not. Fall back to the
# environment, then to the default, if cargo or python is unavailable.
TDIR=""
if command -v python3 >/dev/null 2>&1; then
  TDIR="$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
          | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])' \
          2>/dev/null || true)"
fi
if [[ -z "$TDIR" ]]; then
  TDIR="${CARGO_TARGET_DIR:-target}"
fi

for cand in "$TDIR/release" "$TDIR/$TARGET/release"; do
  found_all=1
  for b in "${BINS[@]}"; do
    [[ -x "$cand/$b" ]] || found_all=0
  done
  if [[ "$found_all" == "1" ]]; then
    echo "$cand"
    exit 0
  fi
done

# Fail loud, and say what IS there. "missing binary" with no listing is what
# made the original failure look like a build failure rather than a path one.
{
  echo "ERROR: built binaries (${BINS[*]}) are not where cargo should have put them."
  echo "  cargo target directory: $TDIR"
  echo "  looked in:              $TDIR/release"
  echo "                          $TDIR/$TARGET/release"
  echo "  CARGO_TARGET_DIR:       ${CARGO_TARGET_DIR:-<unset>}"
  echo "  executables actually present under $TDIR:"
  find "$TDIR" -maxdepth 3 -type f -perm -u+x -name 'ferrosa*' 2>/dev/null | head -20 \
    | sed 's/^/    /' || true
  echo "  If this build succeeded, a cargo config is redirecting the output;"
  echo "  check 'build.target' and 'build.target-dir' in ~/.cargo/config.toml"
  echo "  and in .cargo/config.toml on this runner."
} >&2
exit 1
