#!/usr/bin/env bash
#
# Per-crate docs reminder (pre-commit).
#
# The per-crate-docs rule (CLAUDE.md / AGENTS.md): changing a crate's behavior,
# public API, dependency set, or known-issue/roadmap status is NOT done until
# that crate's README.md + specs/ are updated to match. This hook looks at what
# is staged and INDICATES which workspace crates have staged code changes
# (`<crate>/src/**.rs`) without any staged doc change (`<crate>/README.md` or
# `<crate>/specs/*.md`), so the docs don't silently drift from the code.
#
# Default: WARN (non-blocking) — it is a reminder, since some changes (test-only,
# trivial internal refactors) genuinely need no doc update.
# Strict:  set FERROSA_CRATE_DOCS_STRICT=1 to make it BLOCK the commit.
#
# Bash 3.2 compatible (macOS system bash). No associative arrays.
#
# Testable: set CRATE_DOCS_STAGED_OVERRIDE to a newline-separated file list to
# bypass `git diff` (used by the unit test below the marker).

set -euo pipefail

staged="${CRATE_DOCS_STAGED_OVERRIDE-$(git diff --cached --name-only --diff-filter=ACMR)}"
[ -n "$staged" ] || exit 0

# Crates with a staged Rust source change, deduplicated.
code_crates="$(printf '%s\n' "$staged" | sed -n 's#^\([^/][^/]*\)/src/.*\.rs$#\1#p' | sort -u)"
[ -n "$code_crates" ] || exit 0

missing=""
while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  # Only consider real workspace crates (a Cargo.toml at the crate root).
  [ -f "$crate/Cargo.toml" ] || continue
  # Did any doc for this crate get staged in the same commit?
  if ! printf '%s\n' "$staged" | grep -qE "^${crate}/(README\.md|specs/.*\.md)$"; then
    missing="${missing}${crate}
"
  fi
done <<EOF
$code_crates
EOF

[ -n "$missing" ] || exit 0

printf '\n\033[1;33m⚠  per-crate docs reminder\033[0m — staged code changes with no staged doc update:\n\n'
printf '%s' "$missing" | while IFS= read -r crate; do
  [ -n "$crate" ] || continue
  printf '   • \033[1m%s\033[0m — update %s/README.md and/or %s/specs/{overview,fmea,roadmap}.md\n' \
    "$crate" "$crate" "$crate"
done
printf '\nPer-crate-docs rule (CLAUDE.md): a crate change is not done until its README +\n'
printf 'specs reflect the new status, public API, dependency set, and roadmap/FMEA.\n'
printf 'Test-only or trivial internal changes can ignore this — it is a reminder.\n'

if [ "${FERROSA_CRATE_DOCS_STRICT:-0}" = "1" ]; then
  printf '\n\033[1;31mFERROSA_CRATE_DOCS_STRICT=1 → blocking.\033[0m Update the docs or `git commit --no-verify`.\n'
  exit 1
fi
exit 0
