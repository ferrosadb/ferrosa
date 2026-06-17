#!/usr/bin/env bash
# Nightly read-vs-compaction stress on a CPU-starved Fly machine.
#
# Provisions a throwaway **shared-cpu** Fly machine (its ~6.5% sustained baseline
# starves the compaction executor thread — the exact condition that widens the
# read-vs-compaction window, see bug t_940cc015), clones this repo, builds with
# `--features race-stress`, runs the phase-separated availability invariant
# (engine.rs `read_compaction_race_stress`), and ALWAYS destroys the app.
#
# A non-shared (performance) machine would NOT reproduce the starvation; use
# shared-cpu on purpose. Fan out by running this script N times with different
# regions / seeds for breadth.
#
# Required env:
#   FLY_API_TOKEN  fly auth token (CI secret)
#   GH_TOKEN       token that can clone ferrosadb/ferrosa (PAT or App token)
# Optional env (with defaults):
#   BRANCH=main  REGION=iad  VM=shared-cpu-1x  VM_MEMORY=2048
#   RACE_KEYS=2000 RACE_READERS=8 RACE_SECS=600 RACE_FLUSH_EVERY=50
set -euo pipefail

: "${FLY_API_TOKEN:?set FLY_API_TOKEN}"
: "${GH_TOKEN:?set GH_TOKEN}"
BRANCH="${BRANCH:-main}"
REGION="${REGION:-iad}"
VM="${VM:-shared-cpu-1x}"
VM_MEMORY="${VM_MEMORY:-2048}"
RACE_KEYS="${RACE_KEYS:-2000}"
RACE_READERS="${RACE_READERS:-8}"
RACE_SECS="${RACE_SECS:-600}"
RACE_FLUSH_EVERY="${RACE_FLUSH_EVERY:-50}"

APP="ferrosa-racestress-$(date +%s)-$$"
cleanup() { fly apps destroy "$APP" --yes >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo ":: creating throwaway app $APP"
fly apps create "$APP" --org "${FLY_ORG:-personal}" >/dev/null

# Remote build+run. `bash -c` (NOT -lc: a login shell drops /usr/local/cargo/bin
# from PATH -> cargo not found -> exit 127). The completion marker is ASSEMBLED
# at runtime (M below) so the literal `RS_RESULT` never appears in the script
# SOURCE — fly echoes the whole script to the log on boot, and a literal marker
# in the source would be matched by the host-side grep, falsely "completing" the
# run before the build even starts.
REMOTE='set -uo pipefail
M="RS_RE""SULT"   # emitted = RS_RESULT, but the source token is not that literal
export DEBIAN_FRONTEND=noninteractive
export PATH=/usr/local/cargo/bin:$PATH CARGO_HOME=/usr/local/cargo
apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq git capnproto cmake clang pkg-config libssl-dev build-essential >/dev/null 2>&1
cd /tmp
git clone --depth 1 --branch '"$BRANCH"' https://x-access-token:${GH_TOKEN}@github.com/ferrosadb/ferrosa f 2>&1 | tail -2
cd f || { echo "$M rc=66 (clone failed)"; exit 66; }
SECONDS=0
RACE_KEYS='"$RACE_KEYS"' RACE_READERS='"$RACE_READERS"' RACE_SECS='"$RACE_SECS"' RACE_FLUSH_EVERY='"$RACE_FLUSH_EVERY"' \
  cargo test -p ferrosa-storage --features race-stress --release --lib \
  committed_keys_stay_readable_under_compaction_storm -- --nocapture 2>&1 | tail -40
echo "$M rc=${PIPESTATUS[0]} elapsed=${SECONDS}s"'

echo ":: launching $VM in $REGION (starved baseline is the fuzzer)"
fly machine run rust:1-bookworm \
  --app "$APP" --vm-size "$VM" --vm-memory "$VM_MEMORY" --region "$REGION" --restart no \
  --env GH_TOKEN="$GH_TOKEN" \
  -- bash -c "$REMOTE" >/dev/null

echo ":: waiting for completion (RS_RESULT marker)…"
LOG="$(mktemp)"
deadline=$(( $(date +%s) + RACE_SECS + 3600 ))   # storm + generous build/boot headroom (starved build is slow)
rc=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  fly logs -a "$APP" --no-tail >"$LOG" 2>/dev/null || true
  if grep -qaE 'RS_RESULT rc=[0-9]+' "$LOG"; then
    rc="$(grep -aoE 'RS_RESULT rc=[0-9]+ ?(elapsed=[0-9]+s)?' "$LOG" | tail -1)"
    break
  fi
  sleep 20
done

echo "================ stress output ================"
sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -aE 'test result:|RS_RESULT rc=[0-9]+|silent data loss|panicked|error\[' | tail -25 || true
echo "==============================================="
if printf '%s' "$rc" | grep -qE 'rc=0\b'; then
  echo "PASS: $rc"; exit 0
else
  echo "FAIL/INCOMPLETE: ${rc:-no RS_RESULT marker within deadline}"; exit 1
fi
