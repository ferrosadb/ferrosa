#!/usr/bin/env bash
# pre-push-integration.sh — run the cluster integration tests if explicitly enabled.
#
# Gated by FERROSA_PRE_PUSH_INTEGRATION=1 because spinning up the 3-node Docker
# cluster + running --ignored tests takes 5-10 minutes.  Without the env var this
# script is a no-op (exit 0).
set -euo pipefail

if [ "${FERROSA_PRE_PUSH_INTEGRATION:-}" != "1" ]; then
  echo "[skipped: set FERROSA_PRE_PUSH_INTEGRATION=1 to run cluster integration]"
  exit 0
fi
exec ./scripts/test-with-cluster.sh --ci
