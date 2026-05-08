#!/usr/bin/env bash
# Stage and tar a release for one target triple.
#
# Produces:  dist/ferrosa-${GITHUB_REF_NAME}-<target>.tar.gz
#
# Top-level layout inside the tarball (no wrapper directory):
#   ferrosa
#   ferrosa-ctl
#   LICENSE
#   NOTICE
#   README.md
#   config/ferrosa.example.toml      (if present in repo at tag time)
#   launchd/com.ferrosadb.ferrosa.plist (if present)
#   systemd/ferrosa.service          (if present)
#
# The non-binary files are checked into the repo. Files that don't yet
# exist are skipped silently — the contract is "bundle whatever is in the
# repo at tag time", per specs/release.md.
#
# Fails loud if either binary is missing (per safety.md fail-loud rule).
#
# Usage:
#   stage-release-tarball.sh <target-triple> <release-bin-dir>
# Example:
#   stage-release-tarball.sh aarch64-apple-darwin target/aarch64-apple-darwin/release

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "ERROR: usage: $0 <target-triple> <release-bin-dir>" >&2
  exit 2
fi

TARGET="$1"
BIN_DIR="$2"

if [[ -z "${GITHUB_REF_NAME:-}" ]]; then
  echo "ERROR: GITHUB_REF_NAME must be set (run from a tag push)" >&2
  exit 2
fi

REF_NAME="$GITHUB_REF_NAME"
TARBALL="dist/ferrosa-${REF_NAME}-${TARGET}.tar.gz"

if [[ ! -x "${BIN_DIR}/ferrosa" ]]; then
  echo "ERROR: missing ferrosa binary at ${BIN_DIR}/ferrosa" >&2
  exit 1
fi
if [[ ! -x "${BIN_DIR}/ferrosa-ctl" ]]; then
  echo "ERROR: missing ferrosa-ctl binary at ${BIN_DIR}/ferrosa-ctl" >&2
  exit 1
fi

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

# Binaries — required.
cp "${BIN_DIR}/ferrosa" "${STAGE}/ferrosa"
cp "${BIN_DIR}/ferrosa-ctl" "${STAGE}/ferrosa-ctl"
chmod 755 "${STAGE}/ferrosa" "${STAGE}/ferrosa-ctl"

# Top-level docs — bundle if present at tag time.
for f in LICENSE NOTICE README.md; do
  if [[ -f "$f" ]]; then
    cp "$f" "${STAGE}/$f"
  else
    echo "WARN: $f not found in repo, skipping" >&2
  fi
done

# Platform integration directories — bundle if present at tag time.
# (Created by Task #7. Tarball must succeed even before that lands.)
for entry in \
    "config/ferrosa.example.toml" \
    "launchd/com.ferrosadb.ferrosa.plist" \
    "systemd/ferrosa.service"; do
  if [[ -f "$entry" ]]; then
    mkdir -p "${STAGE}/$(dirname "$entry")"
    cp "$entry" "${STAGE}/$entry"
  else
    echo "WARN: $entry not found in repo, skipping" >&2
  fi
done

mkdir -p dist
# Sort for deterministic tarball ordering.
( cd "$STAGE" && find . -mindepth 1 -print0 | LC_ALL=C sort -z \
    | tar --null --no-recursion -czf - --files-from=- ) > "$TARBALL"

echo "Wrote ${TARBALL}"
ls -lh "$TARBALL"
tar tzf "$TARBALL"
