#!/usr/bin/env bash
set -euo pipefail

# The release version is main's Cargo version ("main leads").
#
# Every PR bumps `[workspace.package] version` with scripts/semver-bump, so a
# release never computes its own version: it reads the one at HEAD (any
# pre-release suffix such as -nightly is dropped) and checks it against the
# latest stable `vX.Y.Z` tag. CalVer nightly tags (vYYYY.MM.DD.HHMM) and
# semver-v* bump tags are never the stable base.
#
# Usage: release-version-from-main.sh nightly|promote [force]
#   nightly  should_release=false when nothing landed since the latest stable
#            tag (unless force=true); otherwise main must be ABOVE that tag.
#   promote  HEAD is the nightly's commit; main's version there must be above
#            the latest stable tag and vX.Y.Z must not exist yet.
# Writes should_release, base_tag, commit_count, next_version, next_tag to
# $GITHUB_OUTPUT (stdout when unset). Refusals exit 1 with the reason.

MODE="${1:-}"
FORCE="${2:-false}"
case "$MODE" in
  nightly|promote) ;;
  *)
    echo "error: mode must be 'nightly' or 'promote', got '${MODE}'" >&2
    exit 2
    ;;
esac

raw="$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["workspace"]["package"]["version"])')"
if [[ ! "$raw" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)([-+].*)?$ ]]; then
  echo "error: Cargo.toml [workspace.package] version '${raw}' is not X.Y.Z[-suffix]" >&2
  exit 1
fi
version="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
tag="v${version}"

latest_tag="$({ git tag --list 'v[0-9]*.[0-9]*.[0-9]*' || true; } \
  | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
  | sort -V \
  | tail -n 1 \
  || true)"

if [[ -z "$latest_tag" ]]; then
  base_tag="v0.0.0"
  commit_count="$(git rev-list --count HEAD)"
else
  base_tag="$latest_tag"
  commit_count="$(git rev-list --count "${latest_tag}..HEAD")"
fi

emit() {
  echo "$1" >> "${GITHUB_OUTPUT:-/dev/stdout}"
}

if [[ "$MODE" == "nightly" && "$commit_count" == "0" && "$FORCE" != "true" ]]; then
  emit "should_release=false"
  emit "base_tag=${base_tag}"
  emit "commit_count=0"
  exit 0
fi

if [[ "$MODE" == "promote" ]] && git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  echo "error: ${tag} already exists: main's version ${version} has already been released stable." >&2
  echo "       Promote a nightly cut after a later bump on main." >&2
  exit 1
fi

highest="$(printf '%s\n%s\n' "${base_tag#v}" "$version" | sort -V | tail -n 1)"
if [[ "$version" != "${base_tag#v}" && "$highest" != "$version" ]]; then
  echo "error: main's version ${version} is BELOW the latest stable tag ${base_tag}." >&2
  echo "       Main leads: fix Cargo.toml on main (scripts/semver-bump --apply) instead of releasing backwards." >&2
  exit 1
fi
if [[ "$version" == "${base_tag#v}" ]]; then
  echo "error: main's version ${version} equals the latest stable tag ${base_tag} but ${commit_count} commit(s) landed since:" >&2
  echo "       a change reached main not bumped (scripts/semver-bump --check should have refused it)." >&2
  exit 1
fi

emit "should_release=true"
emit "base_tag=${base_tag}"
emit "commit_count=${commit_count}"
emit "next_version=${version}"
emit "next_tag=${tag}"
