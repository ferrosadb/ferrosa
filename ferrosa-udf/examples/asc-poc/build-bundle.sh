#!/usr/bin/env bash
# Build the asc + binaryen JS bundle the PoC loads into rquickjs.
# Output: $1 (default /tmp/asc-host/asc-bundle.mjs)
set -euo pipefail
OUT="${1:-/tmp/asc-host/asc-bundle.mjs}"
DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"
cp "$DIR/host-driver.mjs" "$DIR/package.json" "$DIR/package-lock.json" "$WORK/"
cd "$WORK"
# `npm ci` installs EXACTLY the committed lockfile (assemblyscript, binaryen,
# long, esbuild + the platform binary) and verifies each package's SHA-512
# integrity, failing on any mismatch — reproducible and tamper-resistant. Update
# versions only by editing package.json and regenerating package-lock.json.
npm ci --no-audit --no-fund
EXT="--external:fs --external:path --external:url --external:module --external:os --external:crypto --external:tty --external:util --external:process --external:worker_threads --external:perf_hooks --external:fs/promises --external:node:fs --external:node:path --external:node:url --external:node:module --external:node:os --external:node:process --external:node:crypto"
mkdir -p "$(dirname "$OUT")"
npx esbuild host-driver.mjs --bundle --format=esm --platform=neutral $EXT --outfile="$OUT"
echo "wrote $OUT ($(du -h "$OUT" | cut -f1))"
