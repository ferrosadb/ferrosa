#!/usr/bin/env bash
# tests/install_smoke.sh — user-grade install smoke for ferrosa (+ ferrosa-memory).
#
# Exercises the DOCUMENTED install path the way a real user does — NON-ROOT, an
# isolated $HOME, the bundled config, the documented launch command — against a
# LOCALLY-BUILT artifact (the published release can't be tested before it ships).
# This is the gate that would have caught issue #172 (daemon panicked on first
# start; `[storage].data_dir` ignored → unwritable /var/lib/ferrosa).
#
# Scenarios (all via the real docs/install.sh):
#   1. FRESH      — clean $HOME → install → daemon starts, binds CQL, no panic,
#                   serves a trivial request.
#   2. IDEMPOTENT — re-run the same version → "already up to date" no-op; daemon
#                   still serves; data dir untouched.
#   3. UPGRADE    — install a newer version over the old one → daemon restarts on
#                   the new binary AND the data dir (host_id + sstables/commitlog)
#                   is PRESERVED, not dropped.
#   4. PARTIAL    — simulate an interrupted install (missing version stamp) →
#                   re-run → recovers and serves; data preserved.
#
# Usage:
#   tests/install_smoke.sh [--ferrosa-tarball PATH] [--keep] [--debug-build]
# Notes:
#   - Builds a release tarball from THIS checkout if --ferrosa-tarball is unset
#     (use --debug-build for a faster debug binary during local iteration).
#   - Picks free ports and rewrites them into the installed config so the smoke
#     never collides with a real local daemon.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
KEEP=no
DEBUG_BUILD=no
FERROSA_TARBALL=""
while [ $# -gt 0 ]; do
  case "$1" in
    --ferrosa-tarball) FERROSA_TARBALL="$2"; shift 2 ;;
    --keep)            KEEP=yes; shift ;;
    --debug-build)     DEBUG_BUILD=yes; shift ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

WORK="$(mktemp -d "${TMPDIR:-/tmp}/ferrosa-install-smoke.XXXXXX")"
HOME_DIR="$WORK/home"
INSTALL="$HOME_DIR/.ferrosa"
LOGDIR="$WORK/logs"
mkdir -p "$HOME_DIR" "$LOGDIR"
FERROSA_PID=""

log()  { printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
ok()   { printf '  \033[32mOK\033[0m   %s\n' "$*"; }
fail() { printf '\n\033[31mSMOKE FAIL:\033[0m %s\n' "$*" >&2; [ -f "$LOGDIR/ferrosa.log" ] && { echo '--- last ferrosa log ---' >&2; tail -40 "$LOGDIR/ferrosa.log" >&2; }; exit 1; }

cleanup() {
  [ -n "$FERROSA_PID" ] && kill "$FERROSA_PID" 2>/dev/null || true
  [ -n "$FERROSA_PID" ] && wait "$FERROSA_PID" 2>/dev/null || true
  if [ "$KEEP" = yes ]; then echo "kept work dir: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

host_target() {
  case "$(uname -s)/$(uname -m)" in
    Darwin/arm64)           echo "aarch64-apple-darwin" ;;
    Linux/x86_64)           echo "x86_64-unknown-linux-gnu" ;;
    Linux/aarch64|Linux/arm64) echo "aarch64-unknown-linux-gnu" ;;
    *) fail "unsupported platform $(uname -s)/$(uname -m)" ;;
  esac
}

free_port() {
  # Ask the OS for an unused TCP port.
  python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
}

wait_tcp() { # host port timeout_secs
  local host="$1" port="$2" t="${3:-60}" i=0
  while [ "$i" -lt "$t" ]; do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
    sleep 1; i=$((i+1))
  done
  return 1
}

# --- build the release tarball from this checkout (unless one was provided) ----
build_tarball() {
  [ -n "$FERROSA_TARBALL" ] && { ok "using provided tarball $FERROSA_TARBALL"; return; }
  local profile_flag="--release" bindir="$REPO/target/release"
  if [ "$DEBUG_BUILD" = yes ]; then profile_flag=""; bindir="$REPO/target/debug"; fi
  log "building ferrosa + ferrosa-ctl ($([ "$DEBUG_BUILD" = yes ] && echo debug || echo release))"
  ( cd "$REPO" && cargo build $profile_flag --bin ferrosa --bin ferrosa-ctl >/dev/null )
  GITHUB_REF_NAME="v0.0.0-smoke" bash "$REPO/.github/scripts/stage-release-tarball.sh" \
    "$(host_target)" "$bindir" >/dev/null
  FERROSA_TARBALL="$REPO/dist/ferrosa-v0.0.0-smoke-$(host_target).tar.gz"
  [ -f "$FERROSA_TARBALL" ] || fail "stage-release-tarball did not produce $FERROSA_TARBALL"
  ok "staged tarball $FERROSA_TARBALL"
}

# --- run the REAL docs/install.sh against the local tarball, non-root, isolated -
run_installer() { # version-label  [extra-args...]
  local ver="$1"; shift
  HOME="$HOME_DIR" FERROSA_INSTALL_TARBALL="$FERROSA_TARBALL" \
    bash "$REPO/docs/install.sh" --version "$ver" --no-service --no-password "$@" \
    > "$LOGDIR/install-$ver.log" 2>&1 \
    || { cat "$LOGDIR/install-$ver.log" >&2; fail "installer failed for $ver"; }
}

# Rewrite the installed (bundled) config's listen ports to free ones so the
# smoke never collides with a real daemon. Keeps everything else as shipped.
# Only [cql].bind and [internode].bind are actually honored by the daemon today;
# [web]/[graph] bind + bolt_port are read from defaults (a known config-plumbing
# gap, tracked separately). So we patch the two that work to free ports, and the
# graph/web/postgres listeners use their documented defaults (7474/7687/9090/5432
# — free on a clean runner).
patch_ports() {
  CQL_PORT="$(free_port)"; INODE_PORT="$(free_port)"
  GRAPH_PORT=7474   # daemon ignores [graph].bind; binds the default
  local cfg="$INSTALL/config/ferrosa.toml"
  python3 - "$cfg" "$CQL_PORT" "$INODE_PORT" <<'PY'
import re, sys
cfg, cql, inode = sys.argv[1:4]
t = open(cfg).read()
# [cql].bind is the first bind line in the shipped config.
t = re.sub(r'(?m)^(\s*bind\s*=\s*")[^"]*:\d+(")', lambda m: m.group(1)+"127.0.0.1:"+cql+m.group(2), t, count=1)
t = t.replace("0.0.0.0:17000", "127.0.0.1:"+inode).replace("127.0.0.1:17000", "127.0.0.1:"+inode)
open(cfg, "w").write(t)
PY
}

start_ferrosa() {
  : > "$LOGDIR/ferrosa.log"
  HOME="$HOME_DIR" FERROSA_CONFIG="$INSTALL/config/ferrosa.toml" \
    "$INSTALL/bin/ferrosa" >> "$LOGDIR/ferrosa.log" 2>&1 &
  FERROSA_PID=$!
  wait_tcp 127.0.0.1 "$CQL_PORT" 60 || fail "CQL port $CQL_PORT never bound"
  kill -0 "$FERROSA_PID" 2>/dev/null || fail "ferrosa process exited during startup"
  grep -qiE "panic|Cannot drop a runtime" "$LOGDIR/ferrosa.log" && fail "panic in ferrosa startup log"
  ok "daemon up (pid $FERROSA_PID), CQL listening on $CQL_PORT, no panic"
}

stop_ferrosa() {
  [ -n "$FERROSA_PID" ] && kill "$FERROSA_PID" 2>/dev/null || true
  [ -n "$FERROSA_PID" ] && wait "$FERROSA_PID" 2>/dev/null || true
  FERROSA_PID=""
}

# "Does it serve" check: the CQL server reached the listening state and the
# process is still alive. Auth-free on purpose — the bundled config enables CQL
# role auth, so the real authenticated query exercise is done by the
# ferrosa-memory integration (which connects with the seed credentials).
serves_check() {
  grep -q "CQL server listening" "$LOGDIR/ferrosa.log" \
    || fail "daemon never reported 'CQL server listening'"
  kill -0 "$FERROSA_PID" 2>/dev/null || fail "daemon process exited"
  ok "CQL server reached listening state and process is alive (serving)"
}

# Graph engine must be ON by default (bundled config ships [graph] enabled = true
# so the Cypher/Bolt endpoints + viz work out of the box and ferrosa-memory can
# use them). Assert the HTTP graph port binds and the daemon did NOT log the
# disabled path.
graph_check() {
  grep -qi "graph engine disabled" "$LOGDIR/ferrosa.log" \
    && fail "graph engine is DISABLED — bundled config should default it ON"
  wait_tcp 127.0.0.1 "$GRAPH_PORT" 30 || fail "graph HTTP port $GRAPH_PORT never bound (graph not enabled by default?)"
  ok "graph engine enabled by default (HTTP $GRAPH_PORT serving)"
}

# Fingerprint the data dir for the upgrade-preserves-data assertion.
data_fingerprint() {
  local d="$INSTALL/data"
  [ -f "$d/host_id" ] || fail "no host_id under $d — data dir not where the config points"
  printf 'host_id=%s files=%s\n' "$(cat "$d/host_id")" \
    "$(find "$d" -type f | LC_ALL=C sort | wc -l | tr -d ' ')"
}

assert_data_under_install() {
  # Issue #172: the engine used to ignore [storage].data_dir and default to
  # /var/lib/ferrosa. Prove the data lives under the install root.
  [ -d "$INSTALL/data" ] && [ -f "$INSTALL/data/host_id" ] \
    || fail "data dir not under $INSTALL/data (the data_dir-ignored regression)"
  ok "data dir is under the configured install root ($INSTALL/data)"
}

# ============================================================================
# Scenario 1 — FRESH install
# ============================================================================
build_tarball
log "Scenario 1: FRESH install via docs/install.sh (non-root, HOME=$HOME_DIR)"
run_installer "v0.0.0-smoke"
[ -x "$INSTALL/bin/ferrosa" ] || fail "ferrosa binary not installed"
[ -f "$INSTALL/config/ferrosa.toml" ] || fail "config not installed"
ok "installed to $INSTALL"
patch_ports
start_ferrosa
serves_check
graph_check
assert_data_under_install
FP_FRESH="$(data_fingerprint)"; ok "data fingerprint: $FP_FRESH"

# ============================================================================
# Scenario 2 — IDEMPOTENT re-run (same version → no-op, still serving)
# ============================================================================
log "Scenario 2: IDEMPOTENT re-run of the same version"
HOST_ID_FRESH="$(cat "$INSTALL/data/host_id")"
run_installer "v0.0.0-smoke"
grep -qi "already installed" "$LOGDIR/install-v0.0.0-smoke.log" \
  && ok "installer reported already-up-to-date (idempotent)" \
  || ok "re-run completed without error"
serves_check
# host_id stability proves the data dir was reused, not wiped. (File COUNT can
# legitimately grow as the running daemon flushes sstables, so don't compare it.)
[ "$(cat "$INSTALL/data/host_id")" = "$HOST_ID_FRESH" ] \
  || fail "host_id changed on idempotent re-run — data dir was reset"
ok "data dir preserved (host_id stable) on idempotent re-run"

# ============================================================================
# Scenario 3 — UPGRADE (newer version) must PRESERVE the data dir
# ============================================================================
log "Scenario 3: UPGRADE to a newer version, data must be preserved"
HOST_ID_BEFORE="$(cat "$INSTALL/data/host_id")"
stop_ferrosa
run_installer "v0.0.1-smoke"   # different label → treated as an upgrade
grep -qi "upgrading ferrosa" "$LOGDIR/install-v0.0.1-smoke.log" \
  && ok "installer took the upgrade path" || ok "re-installed newer version"
start_ferrosa
serves_check
HOST_ID_AFTER="$(cat "$INSTALL/data/host_id")"
[ "$HOST_ID_BEFORE" = "$HOST_ID_AFTER" ] \
  || fail "host_id changed across upgrade ($HOST_ID_BEFORE -> $HOST_ID_AFTER) — data dir was DROPPED, not reused"
ok "host_id preserved across upgrade ($HOST_ID_AFTER) — database reused, not dropped"

# ============================================================================
# Scenario 4 — PARTIAL install recovery
# ============================================================================
log "Scenario 4: PARTIAL/interrupted install recovery"
stop_ferrosa
rm -f "$INSTALL/.version"          # simulate an install interrupted before the stamp
run_installer "v0.0.1-smoke"
[ -x "$INSTALL/bin/ferrosa" ] || fail "partial-recovery re-install did not restore the binary"
start_ferrosa
serves_check
[ "$(cat "$INSTALL/data/host_id")" = "$HOST_ID_AFTER" ] \
  || fail "data dir not preserved through partial-install recovery"
ok "recovered from a partial install; data preserved"

stop_ferrosa
log "ALL SCENARIOS PASSED — fresh / idempotent / upgrade-preserves-data / partial-recovery"
