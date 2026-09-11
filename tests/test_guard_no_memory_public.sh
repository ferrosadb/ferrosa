#!/bin/bash
# Tests for scripts/guard-no-memory-public.sh. Each case builds a throwaway
# git repo, so the guard is exercised against real `git grep`, not a mock.
set -u
GUARD="$(cd "$(dirname "$0")/.." && pwd)/scripts/guard-no-memory-public.sh"
fail=0
repo_with() {
  local d; d=$(mktemp -d)
  git -C "$d" init -q
  printf '%s\n' "$1" > "$d/file.txt"
  git -C "$d" add file.txt
  echo "$d"
}
expect() { # name want-exit content
  local d out rc
  d=$(repo_with "$3")
  out=$("$GUARD" "$d" 2>&1); rc=$?
  if [ "$rc" = "$2" ]; then echo "ok   $1"; else echo "FAIL $1: exit $rc, want $2"; echo "$out" | sed 's/^/     /'; fail=1; fi
  rm -rf "$d"
}
M=ferrosadb/ferrosa-memory   # memory-public-ok: test fixture
expect "a checkout of the public repo is refused"        1 "          repository: $M"
expect "a clone URL of the public repo is refused"       1 "git clone https://github.com/$M.git x"
expect "a raw URL into the public repo is refused"       1 "curl https://raw.githubusercontent.com/$M/main/x"
expect "a submodule on the public repo is refused"       1 "	url = https://github.com/$M.git"
expect "the private repo is allowed"                     0 "          repository: $M-private"
expect "a marked line is allowed"                        0 "see $M # memory-public-ok: the rule"
expect "an unrelated repo name is allowed"               0 "ferrosadb/ferrosa-memory_tools"
d=$(mktemp -d); "$GUARD" "$d" >/dev/null 2>&1; rc=$?; rm -rf "$d"
[ "$rc" = 2 ] && echo "ok   a non-checkout cannot be scanned (exit 2)" || { echo "FAIL non-checkout: exit $rc, want 2"; fail=1; }
exit $fail
