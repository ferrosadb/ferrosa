#!/usr/bin/env bash
# Point this package at an Aeneas Lean backend.
#
# The backend is not vendored: it is the same release the extraction was produced with, so it comes
# from the host that has one and is gitignored here. Anything that builds this package -- a laptop,
# a CI job, a container -- runs this first.
#
# Usage: bootstrap.sh [path-to-aeneas-lean-backend]
#        defaults to $AENEAS_BACKEND, then ~/.local/aeneas/backends/lean
set -uo pipefail
cd "$(dirname "$0")" || exit 2

backend=${1:-${AENEAS_BACKEND:-$HOME/.local/aeneas/backends/lean}}
[ -f "$backend/lakefile.toml" ] || [ -f "$backend/lakefile.lean" ] || {
  echo "FAIL: no Lean backend at '$backend'"
  echo "      pass one: bootstrap.sh <path>   or set AENEAS_BACKEND"
  exit 2
}

rm -rf aeneas-backend
ln -s "$backend" aeneas-backend
echo "aeneas-backend -> $backend"
