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
# breaks the atomicity contract in ADR-013. Sprint 1 W1.9 introduces the
# MembershipChanger API, lists the 11 surviving call sites in a
# tracked allowlist (no-raw-client-write.allowlist), and gates further
# regressions. Subsequent sprints migrate the allowlisted sites
# one-by-one, removing each entry as it goes.
#
# Exits non-zero if any non-allowlisted violation is found.

set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
SEARCH_ROOT="$ROOT/ferrosa-cluster/src"
ALLOWLIST_FILE="$ROOT/scripts/ci-gates/no-raw-client-write.allowlist"

# Patterns: any of the four primitives.
PATTERNS=(
    'raft\.client_write'
    'raft\.add_learner'
    'raft\.change_membership'
    'network_factory\.register_node'
)

# Allowed locations: the membership module itself, and the forwarding
# helper (which is invoked exclusively by the membership module to
# forward proposals to the leader).
ALLOWED_PATHS=(
    "ferrosa-cluster/src/membership/"
    "ferrosa-cluster/src/raft_forward.rs"
)

# Build the allowlist of `<relpath>:<line>` entries.
declare -A ALLOWLISTED
if [[ -f "$ALLOWLIST_FILE" ]]; then
    while IFS= read -r raw; do
        # Strip leading whitespace, comments, and blank lines.
        line="${raw%%#*}"
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        if [[ -n "$line" ]]; then
            ALLOWLISTED["$line"]=1
        fi
    done < "$ALLOWLIST_FILE"
fi

violations=0
violation_lines=""

for pat in "${PATTERNS[@]}"; do
    if hits=$(grep -rnE "$pat" "$SEARCH_ROOT" 2>/dev/null); then
        while IFS= read -r line; do
            # Skip empty lines from the loop's edge.
            [[ -z "$line" ]] && continue
            # Skip allowed paths.
            keep=1
            for ap in "${ALLOWED_PATHS[@]}"; do
                if [[ "$line" == *"$ap"* ]]; then
                    keep=0
                    break
                fi
            done
            [[ $keep -eq 0 ]] && continue
            # Reduce the grep hit to <relpath>:<line>.
            relpath_line=$(echo "$line" | awk -F':' '{ print $1":"$2 }')
            relpath_line="${relpath_line#$ROOT/}"
            if [[ -n "${ALLOWLISTED[$relpath_line]+x}" ]]; then
                continue
            fi
            violations=$((violations + 1))
            violation_lines+="$line"$'\n'
        done <<< "$hits"
    fi
done

if [[ $violations -gt 0 ]]; then
    echo "VIOLATION: $violations call site(s) bypass MembershipChanger:"
    echo "$violation_lines"
    echo
    echo "FAIL: see specs/decisions/013-membership-change-protocol.md."
    echo "If this is an intentional new bypass, add an entry to:"
    echo "  $ALLOWLIST_FILE"
    echo "with a comment citing the migration plan."
    exit 1
fi

echo "OK: no raw client_write / add_learner / change_membership / register_node \
outside membership module (or allowlisted)."
exit 0
