#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: release-version.sh <ref-type> <ref-name> <run-number>" >&2
  exit 2
fi

ref_type="$1"
ref_name="$2"
run_number="$3"

case "$ref_type" in
  tag)
    if [[ "$ref_name" != v[0-9]* ]]; then
      echo "release tags must start with v followed by a digit: $ref_name" >&2
      exit 1
    fi
    version="${ref_name#v}"
    ;;
  branch)
    if [[ ! "$run_number" =~ ^[1-9][0-9]*$ ]]; then
      echo "workflow run number must be a positive integer: $run_number" >&2
      exit 1
    fi
    # Debian requires a numeric first character. Give a manually dispatched
    # branch build a unique prerelease version instead of using its branch name.
    version="0.0.0-main.${run_number}"
    ;;
  *)
    echo "unsupported Git ref type: $ref_type" >&2
    exit 1
    ;;
esac

if [[ ! "$version" =~ ^[0-9][A-Za-z0-9.+:~-]*$ ]]; then
  echo "release version is not valid for Debian packaging: $version" >&2
  exit 1
fi

printf '%s\n' "$version"
