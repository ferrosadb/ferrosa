#!/usr/bin/env bash
# Control test for the Elle checker IN THIS ENVIRONMENT.
#
# A `valid? true` verdict is only evidence if the checker can still say no. On
# a machine whose JDK, lein, jepsen/Elle jars or :jvm-opts differ from CI's, a
# checker that silently degraded (e.g. failed to load Elle and reported
# nothing) would make every run look green. This runs the checker over two
# minimal, hand-written histories with known answers and asserts BOTH:
#
#   known-invalid-realtime.edn  -> exit != 0, valid? false, anomaly :G0-realtime
#   known-valid.edn             -> exit == 0, valid? true
#
# Run this BEFORE trusting any verdict produced by certify-cql.sh. It is a test
# of the CHECKER, not of the database.
#
# Usage: ferrosa-jepsen/elle-checker/test/checker-control.sh
# Env:   JAVA_HOME (default /opt/homebrew/opt/openjdk@17)
set -uo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$(cd "$DIR/.." && pwd)"
JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"

fail() { printf '\n[checker-control][FAIL] %s\n' "$*" >&2; exit 1; }
log() { printf '\n[checker-control] %s\n' "$*" >&2; }

[ -x "$JAVA_HOME/bin/java" ] || fail "no JDK at JAVA_HOME=$JAVA_HOME"
[ -f "$PROJECT/project.clj" ] || fail "not the elle-checker project: $PROJECT"

run() { # run <history> -> stdout captured by caller, exit code propagated
  ( cd "$PROJECT" && JAVA_HOME="$JAVA_HOME" JAVA_TOOL_OPTIONS=-Djava.awt.headless=true \
      lein run "$1" ) 2>&1
}

log "NEGATIVE control: a history with a real strict-serializability violation MUST be red"
NEG_OUT="$(run "$DIR/histories/known-invalid-realtime.edn")"
NEG_RC=$?
printf '%s\n' "$NEG_OUT" | sed 's/^/    | /' >&2
[ "$NEG_RC" -ne 0 ] || fail "known-invalid history returned exit 0 — the checker cannot say no"
printf '%s' "$NEG_OUT" | grep -q '^valid? false$' || fail "known-invalid history did not report valid? false"
printf '%s' "$NEG_OUT" | grep -q ':G0-realtime' || fail "known-invalid history did not report the expected :G0-realtime anomaly"

log "POSITIVE control: a legal history MUST be green"
POS_OUT="$(run "$DIR/histories/known-valid.edn")"
POS_RC=$?
printf '%s\n' "$POS_OUT" | sed 's/^/    | /' >&2
[ "$POS_RC" -eq 0 ] || fail "known-valid history returned exit $POS_RC — the checker cannot say yes"
printf '%s' "$POS_OUT" | grep -q '^valid? true$' || fail "known-valid history did not report valid? true"

log "PASS: the checker is red-capable and green-capable in this environment (negative exit=$NEG_RC, positive exit=$POS_RC)"
