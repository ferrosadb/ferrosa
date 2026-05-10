#!/usr/bin/env bash
#
# CI gate (W1.9): no raw raft.client_write / raft.add_learner /
# raft.change_membership / network_factory.register_node calls outside
# `ferrosa-cluster/src/membership/` and the controlled forwarding
# helper at `ferrosa-cluster/src/raft_forward.rs`.
#
# These are the four primitive mutations to the four membership stores
# (RaftStateMachine.state.members, openraft Membership, network_factory
# node_map, PeerManager). Allowing them outside the membership module
# breaks the atomicity contract in ADR-013. Sprint 1 W1.9 audits and
# migrates each existing call site; this gate prevents regression.
#
# Exits non-zero if any survivor is found.

set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
SEARCH_ROOT="$ROOT/ferrosa-cluster/src"

# Patterns: any of the four primitives.
PATTERNS=(
    'raft\.client_write'
    'raft\.add_learner'
    'raft\.change_membership'
    'network_factory\.register_node'
)

# Allowed locations: the membership module itself, the forwarding helper
# (which is invoked exclusively by the membership module to forward
# proposals to the leader), and the raft proto/state-machine files where
# the openraft trait impls live.
ALLOWED_PATHS=(
    "ferrosa-cluster/src/membership/"
    "ferrosa-cluster/src/raft_forward.rs"
)

violations=0
for pat in "${PATTERNS[@]}"; do
    # Find all hits in cluster crate.
    if hits=$(grep -rnE "$pat" "$SEARCH_ROOT" 2>/dev/null); then
        # Filter out allowed paths.
        filtered=""
        while IFS= read -r line; do
            keep=1
            for ap in "${ALLOWED_PATHS[@]}"; do
                if [[ "$line" == *"$ap"* ]]; then
                    keep=0
                    break
                fi
            done
            if [[ $keep -eq 1 ]]; then
                filtered+="${line}"$'\n'
            fi
        done <<< "$hits"

        if [[ -n "$filtered" ]]; then
            echo "VIOLATION: pattern \`$pat\` outside membership module:"
            echo "$filtered"
            count=$(printf '%s' "$filtered" | grep -c '.' || true)
            violations=$((violations + count))
        fi
    fi
done

if [[ $violations -gt 0 ]]; then
    echo
    echo "FAIL: $violations call site(s) violate the no-raw-client-write gate (W1.9)."
    echo "See specs/decisions/013-membership-change-protocol.md § 'Module: ferrosa-cluster/src/membership/'."
    exit 1
fi

echo "OK: no raw client_write / add_learner / change_membership / register_node outside membership module."
exit 0
