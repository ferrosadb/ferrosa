#!/bin/bash
# guard-no-memory-public.sh — fail if this repo uses memory-public.
#
#   STOP: ferrosadb/ferrosa-memory (memory-public) is NOT TO BE USED.   # memory-public-ok: the rule itself
#
# It is an old public mirror of ferrosadb/ferrosa-memory-private. The two
# share tag names but not code — on 2026-09-11 the mirror was at schema v64
# and private at v66 — so anything built, cloned, pinned or downloaded from
# the mirror silently downgrades schemas and breaks installs. Every build,
# checkout, pin, submodule, release download and doc link must name
# ferrosadb/ferrosa-memory-private.
#
# Scans every tracked file for a reference to the public repo. A line that
# must keep one (this rule's own text, a mapping that rejects the mirror)
# carries the marker `memory-public-ok: <reason>` on the same line.
#
#   scripts/guard-no-memory-public.sh            # this repo
#   scripts/guard-no-memory-public.sh <dir>      # another checkout
#
# Exit 0 clean, 1 on any reference, 2 if it cannot scan.
set -u
ROOT="${1:-.}"
git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || { echo "guard-no-memory-public: $ROOT is not a git checkout" >&2; exit 2; }

# The public repo, not followed by -private or any other name character.
PATTERN='ferrosadb/ferrosa-memory(\.git)?($|[^-A-Za-z0-9_])'   # memory-public-ok: the pattern

hits=$(git -C "$ROOT" grep -nIE "$PATTERN" -- . 2>&1)
rc=$?
if [ "$rc" -gt 1 ]; then
  echo "guard-no-memory-public: git grep failed: $hits" >&2
  exit 2
fi
violations=$(printf '%s\n' "$hits" | grep -v 'memory-public-ok' | grep -v '^$')

if [ -z "$violations" ]; then
  echo "guard-no-memory-public: clean — no reference to memory-public"
  exit 0
fi
echo "guard-no-memory-public: memory-public is NOT TO BE USED. Use ferrosadb/ferrosa-memory-private." >&2
echo "Offending lines (mark a deliberate one with 'memory-public-ok: <reason>'):" >&2
printf '%s\n' "$violations" | sed 's/^/  /' >&2
exit 1
