#!/usr/bin/env bash
#
# CI gate (W1.8): no `let _ =` in the Raft state machine, handlers, or
# network. Discarding errors there hides apply-path failures, the dominant
# silent-defect class identified in the bug genome (Sprint 1, ADR-013).
#
# A `let _ = expr` legitimately discards a Result-typed expression, but in
# the Raft state machine those Results carry critical apply errors. Any
# real reason to discard must be replaced with a typed propagation,
# explicit `RaftResponse::Error(_)`, or an explicit
# `#[allow(let_underscore_drop, reason = "...")]` annotation.
#
# Exits non-zero if any survivor is found.

set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
TARGETS=(
    "ferrosa-cluster/src/raft/state_machine.rs"
    "ferrosa-cluster/src/raft/handlers.rs"
    "ferrosa-cluster/src/raft/network.rs"
)

violations=0
for f in "${TARGETS[@]}"; do
    full="$ROOT/$f"
    if [[ ! -f "$full" ]]; then
        echo "WARN: target missing: $f" >&2
        continue
    fi
    # Match `let _ = ` (with trailing space). Allow `let _ignored = ` etc.
    if grep -nE '^\s*let _ = ' "$full"; then
        echo "VIOLATION: $f contains \`let _ = \` — replace with typed propagation."
        violations=$((violations + 1))
    fi
done

if [[ $violations -gt 0 ]]; then
    echo
    echo "FAIL: $violations file(s) violate the no-let-underscore-in-raft gate (W1.8)."
    echo "See specs/decisions/013-membership-change-protocol.md § 'Apply path returns errors'."
    exit 1
fi

echo "OK: no \`let _ = \` in Raft state machine / handlers / network."
exit 0
