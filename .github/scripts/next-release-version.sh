#!/usr/bin/env bash
set -euo pipefail

# Determine the next stable SemVer tag from Conventional Commit history.
#
# Bump policy:
# - major: any Conventional Commit with ! (e.g. feat!:), or BREAKING CHANGE in body
# - minor: any feat/feat(scope) commit
# - patch: any other commit since the latest stable vX.Y.Z tag
#
# Pre-release/build metadata tags are intentionally ignored as bases so a
# v1.0.0-beta.N tag does not supersede the latest stable release line.

BUMP_OVERRIDE="${1:-auto}"
FORCE_RELEASE="${2:-false}"

case "$BUMP_OVERRIDE" in
  auto|major|minor|patch) ;;
  *)
    echo "error: bump override must be one of: auto, major, minor, patch" >&2
    exit 2
    ;;
esac

latest_tag="$({ git tag --list 'v[0-9]*.[0-9]*.[0-9]*' || true; } \
  | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
  | sort -V \
  | tail -n 1 \
  || true)"

if [[ -z "$latest_tag" ]]; then
  latest_tag="v0.0.0"
  range="HEAD"
else
  range="${latest_tag}..HEAD"
fi

if [[ "$latest_tag" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
else
  echo "error: latest stable tag has unexpected shape: ${latest_tag}" >&2
  exit 2
fi

if [[ "$range" == "HEAD" ]]; then
  commit_count="$(git rev-list --count HEAD)"
else
  commit_count="$(git rev-list --count "$range")"
fi

if [[ "$commit_count" == "0" && "$FORCE_RELEASE" != "true" ]]; then
  {
    echo "should_release=false"
    echo "base_tag=${latest_tag}"
    echo "commit_count=0"
  } >> "${GITHUB_OUTPUT:-/dev/stdout}"
  exit 0
fi

if [[ "$BUMP_OVERRIDE" == "auto" ]]; then
  subjects_and_bodies="$(git log "$range" --format='%s%n%b%n==END-COMMIT==')"
  if grep -Eq '(^[A-Za-z]+([[:space:]]*\([^)]*\))?!:|^BREAKING CHANGE:|^BREAKING-CHANGE:)' <<<"$subjects_and_bodies"; then
    bump="major"
  elif grep -Eq '^feat([[:space:]]*\([^)]*\))?:' <<<"$subjects_and_bodies"; then
    bump="minor"
  else
    bump="patch"
  fi
else
  bump="$BUMP_OVERRIDE"
fi

case "$bump" in
  major)
    major=$((major + 1)); minor=0; patch=0 ;;
  minor)
    minor=$((minor + 1)); patch=0 ;;
  patch)
    patch=$((patch + 1)) ;;
esac

next_version="${major}.${minor}.${patch}"
next_tag="v${next_version}"

if git rev-parse -q --verify "refs/tags/${next_tag}" >/dev/null; then
  echo "error: computed tag ${next_tag} already exists" >&2
  exit 1
fi

{
  echo "should_release=true"
  echo "base_tag=${latest_tag}"
  echo "commit_count=${commit_count}"
  echo "bump=${bump}"
  echo "next_version=${next_version}"
  echo "next_tag=${next_tag}"
} >> "${GITHUB_OUTPUT:-/dev/stdout}"
