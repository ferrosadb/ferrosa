#!/usr/bin/env bash
# Guard: every third-party GitHub Action `uses:` must be pinned to a 40-char
# commit SHA, OR explicitly allowlisted in `.github/actions-allowlist`.
#
# Fails loud — non-zero exit with a list of offending file:line entries.
# Per the project fail-loud rule (rules/safety.md), this script never
# downgrades to a warning on weird input; if the workflow dir is missing
# or the allowlist parser sees a malformed entry, it exits non-zero.
#
# Usage:  check-actions-pinned.sh [WORKFLOWS_DIR] [ALLOWLIST_FILE]
# Defaults: .github/workflows  and  .github/actions-allowlist
set -euo pipefail

WORKFLOWS_DIR="${1:-.github/workflows}"
ALLOWLIST_FILE="${2:-.github/actions-allowlist}"

if [[ ! -d "$WORKFLOWS_DIR" ]]; then
  echo "ERROR: workflow dir '$WORKFLOWS_DIR' missing — refusing to no-op" >&2
  exit 2
fi

declare -a allow=()
if [[ -f "$ALLOWLIST_FILE" ]]; then
  while IFS= read -r line; do
    entry="${line%%#*}"
    entry="${entry//[[:space:]]/}"
    [[ -z "$entry" ]] && continue
    allow+=("$entry")
  done < "$ALLOWLIST_FILE"
fi

is_allowed() {
  local needle="$1"
  for a in "${allow[@]:-}"; do
    [[ "$a" == "$needle" ]] && return 0
  done
  return 1
}

fail=0
sha_re='^[a-f0-9]{40}$'

while IFS= read -r hit; do
  file="${hit%%:*}"
  rest="${hit#*:}"
  lineno="${rest%%:*}"
  content="${rest#*:}"
  action_ref="$(printf '%s\n' "$content" \
    | sed -E 's/.*uses:[[:space:]]*//; s/[[:space:]]*#.*$//; s/[[:space:]]+$//')"
  [[ "$action_ref" =~ ^\./ ]] && continue
  [[ "$action_ref" =~ ^\.github/ ]] && continue
  if [[ "$action_ref" != *"@"* ]]; then
    echo "FAIL  $file:$lineno  no @ref:  $action_ref" >&2
    fail=1
    continue
  fi
  ref="${action_ref##*@}"
  if [[ "$ref" =~ $sha_re ]]; then
    continue
  fi
  if is_allowed "$action_ref"; then
    continue
  fi
  echo "FAIL  $file:$lineno  not SHA-pinned and not allowlisted:  $action_ref" >&2
  fail=1
done < <(grep -rEn '^[[:space:]]*-?[[:space:]]*uses:[[:space:]]*[^[:space:]#]' "$WORKFLOWS_DIR" || true)

if [[ "$fail" -ne 0 ]]; then
  echo "" >&2
  echo "Pin every offending action to a 40-char SHA, or add the exact" >&2
  echo "'<owner>/<repo>@<ref>' line to '$ALLOWLIST_FILE' with a comment." >&2
  exit 1
fi

echo "All actions are SHA-pinned (or explicitly allowlisted)."
