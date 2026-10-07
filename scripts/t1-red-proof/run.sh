#!/usr/bin/env bash
#
# T1 RED-side reproduction (t_80b2a90d).
#
# The GREEN (fix-side) tests are in the tree:
#   ferrosa/src/runtime.rs::tests::subsystem_worker_threads_stay_within_the_host_budget
#   ferrosa-storage/src/self_heal/mod.rs::controller_tests::
#       offloaded_tick_does_not_starve_a_coresident_liveness_task
#
# A green on the fixed revision alone proves nothing. This script reproduces the
# RED side deterministically: it applies graft.diff -- the *identical* test
# bodies, inline-tick / pre-fix shapes -- onto the pre-fix base revision in a
# throwaway git worktree, and runs them. Both must FAIL there.
#
# graft.diff is against 220aff3c (the merge base this branch was cut from).
#
# Usage:  scripts/t1-red-proof/run.sh [BASE_REV] [WORKTREE_DIR]
# Exit 0 == RED reproduced (both pre-fix tests failed, as required).
#
# Measured on an 18-vCPU macOS dev host (see README.md in this directory):
#   isolation  worst wake gap:  inline 695 ms (RED, bound 300) vs offloaded 38 ms
#   census     OS threads:      pre-fix 28 (RED, bound budget4+8=12) vs fixed 6

set -uo pipefail

BASE_REV="${1:-220aff3c}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(git -C "$HERE" rev-parse --show-toplevel)"
WT="${2:-$REPO_ROOT/.wt-t1-red}"

echo "### T1 RED proof: base=$BASE_REV  worktree=$WT"

if [ ! -d "$WT" ]; then
  git -C "$REPO_ROOT" worktree add --detach "$WT" "$BASE_REV" >/dev/null
fi

git -C "$WT" checkout -q -- .
git -C "$WT" apply "$HERE/graft.diff"
echo "### graft applied; running pre-fix tests (both must FAIL)"

echo "### RED 1: isolation (inline tick starves a co-resident liveness task)"
( cd "$WT" && cargo test -p ferrosa-storage --lib \
    inline_tick_starves_a_coresident_liveness_task_prefix -- --nocapture 2>&1 | tail -12 )
r1=$?

echo "### RED 2: thread census (pre-fix 26-run default oversubscribes a budget of 4)"
( cd "$WT" && cargo test -p ferrosa --bins \
    subsystem_worker_threads_stay_within_the_host_budget -- --nocapture 2>&1 | tail -12 )
r2=$?

echo "### RED proof exit: isolation=$r1 census=$r2 (non-zero == RED reproduced)"
if [ "$r1" -ne 0 ] && [ "$r2" -ne 0 ]; then
  echo "### RED REPRODUCED"
  exit 0
fi
echo "### NOT RED on at least one invariant — the GREEN claim is unsupported"
exit 1
