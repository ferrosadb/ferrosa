#!/usr/bin/env bash
# Regression test: the CI cluster bootstrap's stdout is consumed as GITHUB_ENV.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${SCRIPT_DIR}/../scripts/test-cluster-up-ci.sh"

grep -Eq '^        "\$\{REPO_ROOT\}" >&2$' "$SCRIPT" || {
    echo "docker build output must stay off stdout" >&2
    exit 1
}
pull_pattern="^        if docker pull \"\\\$image\" >&2; then$"
grep -Eq "$pull_pattern" "$SCRIPT" || {
    echo "docker pull output must stay off stdout" >&2
    exit 1
}
grep -Eq '^    up -d >&2$' "$SCRIPT" || {
    echo "docker compose output must stay off stdout" >&2
    exit 1
}
