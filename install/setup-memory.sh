#!/usr/bin/env bash
# ferrosa-memory fast setup — installs prebuilt binaries via the LATEST file,
# downloads ONBOARDING.md, optionally clones source repos, optionally pulls
# the Nomic embedding model, and hands off to a selected LLM harness.
#
# Reads https://ferrosadb.com/LATEST (a plain-text version tag like "v0.16.0")
# and uses it for both ferrosa and ferrosa-memory release artifacts (the two
# projects ship synchronized tags). No source compile.
#
# Usage:
#   curl -fsSL https://ferrosadb.com/setup-memory.sh | bash
#   curl -fsSL https://ferrosadb.com/setup-memory.sh | bash -s -- --version v0.16.0 --no-clone
#
# Env overrides (mostly for testing):
#   FERROSA_LATEST_URL    — version pointer (default https://ferrosadb.com/LATEST)
#   FERROSA_RELEASE_HOST  — ferrosa releases root
#   MEMORY_RELEASE_HOST   — ferrosa-memory releases root
#   ONBOARDING_URL        — ONBOARDING.md source (default github raw on main)
#   FERROSA_SUITE_DIR     — where to put cloned repos (default $HOME/src/ferrosa-suite)
#   FERROSA_INSTALL_ROOT  — binary install prefix (default $HOME/.ferrosa)
#   NOMIC_MODEL           — embedding model name (default nomic-embed-text-v2-moe)
#   FERROSA_MEMORY_START  — start the MCP server when done: yes|no (default yes on macOS)
set -euo pipefail

FERROSA_REPO="ferrosadb/ferrosa"
MEMORY_REPO="ferrosadb/ferrosa-memory"
LATEST_URL="${FERROSA_LATEST_URL:-https://ferrosadb.com/LATEST}"
FERROSA_RELEASE_HOST="${FERROSA_RELEASE_HOST:-https://github.com/${FERROSA_REPO}/releases}"
MEMORY_RELEASE_HOST="${MEMORY_RELEASE_HOST:-https://github.com/${MEMORY_REPO}/releases}"
ONBOARDING_URL="${ONBOARDING_URL:-https://raw.githubusercontent.com/${MEMORY_REPO}/main/ONBOARDING.md}"
FERROSA_SUITE_DIR="${FERROSA_SUITE_DIR:-$HOME/src/ferrosa-suite}"
INSTALL_ROOT="${FERROSA_INSTALL_ROOT:-${HOME}/.ferrosa}"
BIN_DIR="${INSTALL_ROOT}/bin"
CONFIG_DIR="${INSTALL_ROOT}/config"
DATA_DIR="${INSTALL_ROOT}/data"
LOG_DIR="${INSTALL_ROOT}/logs"
INSTALL_OUTCOME_PATH="${INSTALL_ROOT}/install-outcome.json"
NOMIC_MODEL="${NOMIC_MODEL:-nomic-embed-text-v2-moe}"
DB_HOST="${FERROSA_SETUP_CQL_HOST:-127.0.0.1}"
DB_PORT="${FERROSA_SETUP_CQL_PORT:-9042}"

VERSION=""
WANT_CLONE=""    # ask|yes|no
WANT_NOMIC=""    # ask|yes|no
WANT_HERMES=""   # ask|yes|no
WANT_HOOKS=""    # ask|yes|no — install harness hooks (default yes)
WANT_START="${FERROSA_MEMORY_START:-}"   # yes|no — start the MCP server (default yes on macOS)
HARNESS="${FERROSA_HARNESS:-auto}"   # auto|all|codex|claude|hermes|pi|generic
MCP_URL="${FERROSA_MEMORY_MCP_URL:-http://127.0.0.1:18765/mcp}"
DB_OUTCOME="not_attempted"
MCP_OUTCOME="not_attempted"

# Path to the LaunchAgent plist template shipped in the ferrosa-memory tarball.
# Populated in Stage 1 (before the extract dir is cleaned) and consumed by the
# server-start stage. Empty means "write the plist inline".
LAUNCHD_TEMPLATE=""
DB_LAUNCHD_TEMPLATE=""
DB_SYSTEMD_TEMPLATE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --version)    VERSION="$2"; shift 2 ;;
    --clone)      WANT_CLONE="yes"; shift ;;
    --no-clone)   WANT_CLONE="no"; shift ;;
    --nomic)      WANT_NOMIC="yes"; shift ;;
    --no-nomic)   WANT_NOMIC="no"; shift ;;
    --hermes)     WANT_HERMES="yes"; shift ;;
    --no-hermes)  WANT_HERMES="no"; shift ;;
    --hooks)      WANT_HOOKS="yes"; shift ;;
    --no-hooks)   WANT_HOOKS="no"; shift ;;
    --start)      WANT_START="yes"; shift ;;
    --no-start)   WANT_START="no"; shift ;;
    --harness)    HARNESS="$2"; shift 2 ;;
    --mcp-url)    MCP_URL="$2"; shift 2 ;;
    -h|--help)
      cat <<EOF
ferrosa-memory fast setup
  --version <tag>          install a specific tag (default: read $LATEST_URL)
  --clone / --no-clone     clone or update source repos under \$FERROSA_SUITE_DIR
  --nomic / --no-nomic     pull the Nomic embedding model via ollama
  --hooks / --no-hooks     install LLM-harness hooks (session-start/recall/turn)
  --start / --no-start     start the MCP server now via a macOS LaunchAgent (default: start on macOS)
  --harness <name>         which harness hooks to install: auto|all|codex|claude|hermes|pi|generic
  --mcp-url <url>          MCP endpoint baked into the hooks (default $MCP_URL)
  --hermes / --no-hermes   exec hermes "onboard me ..." when done
EOF
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

say() { printf ':: %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

write_install_outcome() {
  local tmp="${INSTALL_OUTCOME_PATH}.tmp.$$"
  (
    umask 077
    printf '{\n'
    printf '  "schema_version": 1,\n'
    printf '  "installer": "setup-memory",\n'
    printf '  "recorded_at": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '  "database": { "status": "%s" },\n' "$DB_OUTCOME"
    printf '  "mcp": { "status": "%s" }\n' "$MCP_OUTCOME"
    printf '}\n'
  ) > "$tmp"
  mv "$tmp" "$INSTALL_OUTCOME_PATH"
}

detect_target() {
  local os arch
  os=$(uname -s); arch=$(uname -m)
  case "$os/$arch" in
    Darwin/arm64)              echo "aarch64-apple-darwin" ;;
    Darwin/x86_64)
      die "Intel macOS is not supported in v0.x. Build from source: https://github.com/${MEMORY_REPO}#building" ;;
    Linux/x86_64)              echo "x86_64-unknown-linux-musl" ;;
    Linux/aarch64|Linux/arm64) echo "aarch64-unknown-linux-musl" ;;
    *) die "unsupported platform: $os/$arch" ;;
  esac
}
TARGET=$(detect_target)

if [ -z "$VERSION" ]; then
  say "resolving latest version from $LATEST_URL"
  VERSION=$(curl -fsSL "$LATEST_URL" | tr -d '[:space:]')
fi
[ -n "$VERSION" ] || die "no version resolved from $LATEST_URL"
case "$VERSION" in
  v*) : ;;
  *)  VERSION="v${VERSION}" ;;
esac

prompt_yes() {
  local q="$1" a
  if [ ! -t 0 ] && [ ! -r /dev/tty ]; then
    return 1
  fi
  read -r -p "$q [y/N] " a < /dev/tty
  case "${a:-N}" in y|Y|yes|Yes|YES) return 0 ;; *) return 1 ;; esac
}

# ── Stage 1: install binaries ───────────────────────────────────────────────
install_tarball() {
  local label="$1" host="$2" tarball="$3"
  local url="${host}/download/${VERSION}/${tarball}"
  local sums_url="${host}/download/${VERSION}/SHA256SUMS"
  local tmp; tmp=$(mktemp -d)
  say "downloading ${label} ${tarball}"
  curl -fsSL --output "$tmp/$tarball" "$url" >&2
  curl -fsSL --output "$tmp/SHA256SUMS" "$sums_url" >&2
  ( cd "$tmp" && grep "$tarball" SHA256SUMS | shasum -a 256 -c - >&2 ) \
    || die "${label}: checksum verification FAILED"
  tar -xzf "$tmp/$tarball" -C "$tmp" >&2
  printf '%s\n' "$tmp"
}

mkdir -p "$BIN_DIR" "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR"

FERROSA_TARBALL="ferrosa-${VERSION}-${TARGET}.tar.gz"
MEMORY_TARBALL="ferrosa-memory-${VERSION}-${TARGET}.tar.gz"

# ferrosa binary
F_TMP=$(install_tarball "ferrosa" "$FERROSA_RELEASE_HOST" "$FERROSA_TARBALL")
cp "$F_TMP/ferrosa"     "$BIN_DIR/"
cp "$F_TMP/ferrosa-ctl" "$BIN_DIR/"
chmod +x "$BIN_DIR/ferrosa" "$BIN_DIR/ferrosa-ctl"
if [ ! -f "$CONFIG_DIR/ferrosa.toml" ]; then
  cp "$F_TMP/config/ferrosa.example.toml" "$CONFIG_DIR/ferrosa.toml"
fi
# Keep the DB service templates before the extracted release is removed. The
# quick-start start path matches install.sh: launchd on macOS, systemd --user on
# Linux, and a foreground command when neither can run the service.
if [ -f "$F_TMP/launchd/com.ferrosadb.ferrosa.plist" ]; then
  mkdir -p "$INSTALL_ROOT/share/ferrosa/launchd"
  cp "$F_TMP/launchd/com.ferrosadb.ferrosa.plist" \
     "$INSTALL_ROOT/share/ferrosa/launchd/com.ferrosadb.ferrosa.plist"
  DB_LAUNCHD_TEMPLATE="$INSTALL_ROOT/share/ferrosa/launchd/com.ferrosadb.ferrosa.plist"
fi
if [ -f "$F_TMP/systemd/ferrosa.service" ]; then
  mkdir -p "$INSTALL_ROOT/share/ferrosa/systemd"
  cp "$F_TMP/systemd/ferrosa.service" \
     "$INSTALL_ROOT/share/ferrosa/systemd/ferrosa.service"
  DB_SYSTEMD_TEMPLATE="$INSTALL_ROOT/share/ferrosa/systemd/ferrosa.service"
fi
rm -rf "$F_TMP"

# ferrosa-memory binary
M_TMP=$(install_tarball "ferrosa-memory" "$MEMORY_RELEASE_HOST" "$MEMORY_TARBALL")
cp "$M_TMP/ferrosa-memory-mcp" "$BIN_DIR/"
chmod +x "$BIN_DIR/ferrosa-memory-mcp"
if [ ! -f "$CONFIG_DIR/ferrosa-memory.toml" ]; then
  cp "$M_TMP/config/ferrosa-memory.example.toml" "$CONFIG_DIR/ferrosa-memory.toml"
fi
# The release tarball ships a LaunchAgent plist template (with __BINARY_PATH__ /
# __REPO_ROOT__ / __CONFIG_PATH__ / __HOME__ placeholders). Stash it to a stable
# location now, before $M_TMP is removed, so the server-start stage can render it.
if [ -f "$M_TMP/launchd/com.ferrosa-memory.mcp.plist" ]; then
  mkdir -p "$INSTALL_ROOT/share/ferrosa-memory/launchd"
  cp "$M_TMP/launchd/com.ferrosa-memory.mcp.plist" \
     "$INSTALL_ROOT/share/ferrosa-memory/launchd/com.ferrosa-memory.mcp.plist"
  LAUNCHD_TEMPLATE="$INSTALL_ROOT/share/ferrosa-memory/launchd/com.ferrosa-memory.mcp.plist"
fi
rm -rf "$M_TMP"

# ── Stage 2: optional source clone ──────────────────────────────────────────
clone_or_update() {
  local url="$1" dir="$2"
  if [ -d "$dir/.git" ]; then
    say "updating $dir"
    git -C "$dir" fetch --all --prune
  else
    say "cloning $url -> $dir"
    git clone "$url" "$dir"
  fi
}

do_clone() {
  mkdir -p "$FERROSA_SUITE_DIR"
  clone_or_update "https://github.com/${FERROSA_REPO}.git" "$FERROSA_SUITE_DIR/ferrosa"
  clone_or_update "https://github.com/${MEMORY_REPO}.git" "$FERROSA_SUITE_DIR/ferrosa-memory"
}

case "$WANT_CLONE" in
  yes) do_clone ;;
  no)  : ;;
  "")  prompt_yes "Clone or update source repos at $FERROSA_SUITE_DIR?" && do_clone ;;
esac

# ── Stage 3: ONBOARDING.md ──────────────────────────────────────────────────
ONBOARDING_DIR="$FERROSA_SUITE_DIR/ferrosa-memory"
ONBOARDING_PATH="$ONBOARDING_DIR/ONBOARDING.md"
mkdir -p "$ONBOARDING_DIR"
if [ ! -f "$ONBOARDING_PATH" ]; then
  say "downloading ONBOARDING.md from $ONBOARDING_URL"
  curl -fsSL "$ONBOARDING_URL" -o "$ONBOARDING_PATH" \
    || say "failed to fetch ONBOARDING.md (continuing; you can re-download later)"
fi

# ── Stage 3b: install LLM-harness hooks ─────────────────────────────────────
# The hooks (session-start / recall / turn-finalization) are what make the
# memory server actually engage the LLM — without them onboarding is incomplete.
# They are installed by ferrosa-memory's self-contained hook installer
# (scripts/install-agent-hooks.py + scripts/hooks/ferrosa-memory-turn-hook.py,
# both stdlib-only Python). With a source checkout we run it in-tree; with
# --no-clone we fetch those two files PINNED to $VERSION into a stable location
# (the generated wrappers bake in the hook path, so it must persist) and run
# them against the installed MCP endpoint.
HOOK_SRC_DIR="${INSTALL_ROOT}/share/ferrosa-memory"
RAW_BASE="https://raw.githubusercontent.com/${MEMORY_REPO}/${VERSION}"

fetch_hook_installer() {
  # Pinned to the release tag so hooks match the installed binary; fails loud
  # (no silent fallback to main) if the tag lacks the files.
  mkdir -p "$HOOK_SRC_DIR/scripts/hooks"
  curl -fsSL "$RAW_BASE/scripts/install-agent-hooks.py" \
       -o "$HOOK_SRC_DIR/scripts/install-agent-hooks.py" || return 1
  curl -fsSL "$RAW_BASE/scripts/hooks/ferrosa-memory-turn-hook.py" \
       -o "$HOOK_SRC_DIR/scripts/hooks/ferrosa-memory-turn-hook.py" || return 1
}

install_hooks() {
  if ! command -v python3 >/dev/null 2>&1; then
    say "python3 not found — cannot install harness hooks automatically."
    say "Install python3, then run:"
    say "  curl -fsSL $RAW_BASE/scripts/install-agent-hooks.py | \\"
    say "    python3 - --harness $HARNESS --mcp-url $MCP_URL"
    return 1
  fi
  local root
  if [ -f "$FERROSA_SUITE_DIR/ferrosa-memory/scripts/install-agent-hooks.py" ]; then
    root="$FERROSA_SUITE_DIR/ferrosa-memory"
    say "installing harness hooks from source checkout (harness=$HARNESS)"
  else
    say "fetching harness hook installer @ $VERSION (no source checkout)"
    if ! fetch_hook_installer; then
      say "could not fetch the hook installer at $VERSION."
      say "Fix: clone the repo and run ./setup.sh, or fetch manually from"
      say "  $RAW_BASE/scripts/install-agent-hooks.py"
      return 1
    fi
    root="$HOOK_SRC_DIR"
  fi
  # No --skip-auth-check: it post-dates older release installers, and the
  # installer already warns-and-continues when the server is unreachable (and
  # correctly refuses if the server is up but credentials are inconsistent).
  if ( cd "$root" && python3 scripts/install-agent-hooks.py \
         --harness "$HARNESS" --mcp-url "$MCP_URL" ); then
    say "harness hooks installed (harness=$HARNESS, mcp=$MCP_URL)"
  else
    say "hook installer reported an error (see output above)"
    return 1
  fi
}

if [ "$WANT_HOOKS" = "no" ]; then
  say "skipping harness hook installation (--no-hooks)"
else
  install_hooks || say "WARNING: harness hooks were NOT installed (see above). The memory \
server will run, but your LLM will not auto-recall/ingest until hooks are installed."
fi

# ── Stage 4: optional Nomic embedding model ─────────────────────────────────
pull_nomic() {
  if command -v ollama >/dev/null 2>&1; then
    say "pulling $NOMIC_MODEL via ollama"
    ollama pull "$NOMIC_MODEL"
  else
    say "ollama not found — skipping. Install ollama and run: ollama pull $NOMIC_MODEL"
  fi
}

case "$WANT_NOMIC" in
  yes) pull_nomic ;;
  no)  : ;;
  "")  if command -v ollama >/dev/null 2>&1; then
         prompt_yes "Pull Nomic embedding model ($NOMIC_MODEL) for semantic search?" \
           && pull_nomic
       else
         say "ollama not found — skipping embedding model. Semantic/vector search will be degraded."
       fi ;;
esac

# ── Stage 4a: configure, start, and verify the local Ferrosa DB ─────────────
# The hosted quick start installs the DB artifact itself, so it must also apply
# the canonical install.sh lifecycle before it starts or advertises MCP.
db_reachable() {
  (: > "/dev/tcp/$DB_HOST/$DB_PORT") 2>/dev/null
}

wait_for_db() {
  for _ in $(seq 1 30); do
    db_reachable && return 0
    sleep 1
  done
  return 1
}

manual_db_action_required() {
  DB_OUTCOME="manual_action_required"
  write_install_outcome
  say "manual_action_required: local Ferrosa is configured but not running"
  say "Start the database, then re-run this installer:"
  say "  FERROSA_CONFIG=\"$CONFIG_DIR/ferrosa.toml\" \"$BIN_DIR/ferrosa\""
  exit 1
}

start_db_macos() {
  local domain plist
  command -v launchctl >/dev/null 2>&1 || return 1
  [ -n "$DB_LAUNCHD_TEMPLATE" ] && [ -f "$DB_LAUNCHD_TEMPLATE" ] || return 1

  domain="gui/$(id -u)"
  plist="$HOME/Library/LaunchAgents/com.ferrosadb.ferrosa.plist"
  mkdir -p "$(dirname "$plist")"
  sed "s|__HOME__|$HOME|g" "$DB_LAUNCHD_TEMPLATE" > "$plist"
  launchctl bootout "${domain}/com.ferrosadb.ferrosa" 2>/dev/null || true
  if launchctl bootstrap "$domain" "$plist" 2>/dev/null; then
    launchctl enable "${domain}/com.ferrosadb.ferrosa" 2>/dev/null || true
    launchctl kickstart -k "${domain}/com.ferrosadb.ferrosa" 2>/dev/null || true
  else
    launchctl unload "$plist" 2>/dev/null || true
    launchctl load "$plist" 2>/dev/null || return 1
  fi
  say "launchd: ferrosa.service loaded; waiting for database readiness"
}

start_db_linux() {
  local unit="$HOME/.config/systemd/user/ferrosa.service"
  command -v systemctl >/dev/null 2>&1 || return 1
  [ -n "$DB_SYSTEMD_TEMPLATE" ] && [ -f "$DB_SYSTEMD_TEMPLATE" ] || return 1

  mkdir -p "$(dirname "$unit")"
  cp "$DB_SYSTEMD_TEMPLATE" "$unit"
  systemctl --user daemon-reload || return 1
  systemctl --user enable --now ferrosa.service || return 1
  if command -v loginctl >/dev/null 2>&1; then
    loginctl enable-linger "$USER" 2>/dev/null \
      && say "systemd: lingering enabled (boot-time start without login)"
  fi
  say "systemd: ferrosa.service enabled; waiting for database readiness"
}

ensure_local_db_ready() {
  if db_reachable; then
    say "database ready on $DB_HOST:$DB_PORT"
    return 0
  fi

  case "$(uname -s)" in
    Darwin) start_db_macos || manual_db_action_required ;;
    Linux)  start_db_linux || manual_db_action_required ;;
    *)      manual_db_action_required ;;
  esac

  if wait_for_db; then
    say "database ready on $DB_HOST:$DB_PORT"
  else
    manual_db_action_required
  fi
}

ensure_local_db_ready
DB_OUTCOME="ready"

# ── Stage 4b: start the MCP server (macOS LaunchAgent) ──────────────────────
# A fresh install lays down a binary + config but no running process, so nothing
# is listening on :18765. The database readiness gate above must pass before
# this service is started or described as available.
LAUNCH_AGENT_LABEL="com.ferrosa-memory.mcp"
LAUNCH_AGENT_PLIST="${HOME}/Library/LaunchAgents/${LAUNCH_AGENT_LABEL}.plist"
LAUNCH_AGENT_LOG="/tmp/ferrosa-memory-mcp.log"
STARTED="no"   # no|yes|failed|manual — for the final summary

render_launch_agent_plist() {
  local out="$1"
  if [ -n "$LAUNCHD_TEMPLATE" ] && [ -f "$LAUNCHD_TEMPLATE" ]; then
    # Prefer the tarball-shipped template; sed-replace its placeholders.
    say "rendering LaunchAgent from tarball template"
    sed -e "s|__BINARY_PATH__|${BIN_DIR}/ferrosa-memory-mcp|g" \
        -e "s|__CONFIG_PATH__|${CONFIG_DIR}/ferrosa-memory.toml|g" \
        -e "s|__REPO_ROOT__|${INSTALL_ROOT}|g" \
        -e "s|__HOME__|${HOME}|g" \
        "$LAUNCHD_TEMPLATE" > "$out"
  else
    # No template in the tarball — write a self-contained plist inline.
    say "writing LaunchAgent plist inline"
    cat > "$out" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LAUNCH_AGENT_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${BIN_DIR}/ferrosa-memory-mcp</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${INSTALL_ROOT}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>FERROSA_MEMORY_CONFIG</key>
    <string>${CONFIG_DIR}/ferrosa-memory.toml</string>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>5</integer>
  <key>StandardOutPath</key>
  <string>${LAUNCH_AGENT_LOG}</string>
  <key>StandardErrorPath</key>
  <string>${LAUNCH_AGENT_LOG}</string>
</dict>
</plist>
PLIST
  fi
}

start_server() {
  local domain; domain="gui/$(id -u)"
  mkdir -p "$(dirname "$LAUNCH_AGENT_PLIST")"
  render_launch_agent_plist "$LAUNCH_AGENT_PLIST"
  # Idempotent: drop any existing instance, then load fresh. Don't error if it
  # was never loaded. Prefer modern bootout/bootstrap; fall back to unload/load
  # on older launchd that lacks them.
  launchctl bootout "${domain}/${LAUNCH_AGENT_LABEL}" 2>/dev/null || true
  if launchctl bootstrap "${domain}" "$LAUNCH_AGENT_PLIST" 2>/dev/null; then
    launchctl enable "${domain}/${LAUNCH_AGENT_LABEL}" 2>/dev/null || true
    launchctl kickstart -k "${domain}/${LAUNCH_AGENT_LABEL}" 2>/dev/null || true
  else
    launchctl unload "$LAUNCH_AGENT_PLIST" 2>/dev/null || true
    if ! launchctl load "$LAUNCH_AGENT_PLIST" 2>/dev/null; then
      say "could not load the LaunchAgent automatically."
      say "Start it manually: launchctl load $LAUNCH_AGENT_PLIST"
      return 1
    fi
  fi
  say "MCP server started via LaunchAgent ($LAUNCH_AGENT_PLIST)"
  say "  log: $LAUNCH_AGENT_LOG"
  say "  database: ready on $DB_HOST:$DB_PORT"
}

if [ "$WANT_START" = "no" ]; then
  MCP_OUTCOME="skipped"
  say "skipping MCP server start (--no-start). Start it later with:"
  say "  FERROSA_MEMORY_CONFIG=$CONFIG_DIR/ferrosa-memory.toml $BIN_DIR/ferrosa-memory-mcp"
elif [ "$(uname -s)" = "Darwin" ]; then
  if start_server; then
    STARTED="yes"
    MCP_OUTCOME="started"
  else
    STARTED="failed"
    MCP_OUTCOME="failed"
  fi
else
  # Auto-start uses a macOS LaunchAgent; on other OSes, tell the user how to run
  # it manually rather than failing.
  STARTED="manual"
  MCP_OUTCOME="manual"
  say "auto-start is macOS-only on this installer — start the server manually:"
  say "  FERROSA_MEMORY_CONFIG=$CONFIG_DIR/ferrosa-memory.toml $BIN_DIR/ferrosa-memory-mcp"
fi
write_install_outcome

# ── Stage 5: hand off to LLM harness ────────────────────────────────────────
case "$STARTED" in
  yes)    SERVER_LINE="server:   started via LaunchAgent after DB readiness verification
  log:      $LAUNCH_AGENT_LOG" ;;
  failed) SERVER_LINE="server:   LaunchAgent install FAILED — start manually: FERROSA_MEMORY_CONFIG=$CONFIG_DIR/ferrosa-memory.toml $BIN_DIR/ferrosa-memory-mcp" ;;
  manual) SERVER_LINE="server:   not started (macOS-only auto-start) — run: FERROSA_MEMORY_CONFIG=$CONFIG_DIR/ferrosa-memory.toml $BIN_DIR/ferrosa-memory-mcp" ;;
  *)      SERVER_LINE="server:   not started (--no-start) — run: FERROSA_MEMORY_CONFIG=$CONFIG_DIR/ferrosa-memory.toml $BIN_DIR/ferrosa-memory-mcp" ;;
esac

cat <<EOF >&2

ferrosa-memory $VERSION installed.

  binaries: $BIN_DIR
  config:   $CONFIG_DIR/ferrosa-memory.toml
  hooks:    ~/.config/ferrosa-memory/hooks (harness=$HARNESS; unless --no-hooks)
  outcome:  $INSTALL_OUTCOME_PATH
  onboard:  $ONBOARDING_PATH
  $SERVER_LINE

EOF

case "$WANT_HERMES" in
  yes) command -v hermes >/dev/null 2>&1 && exec hermes "onboard me using $ONBOARDING_PATH" ;;
  no)  : ;;
  "")  if command -v hermes >/dev/null 2>&1 \
         && prompt_yes "Launch Hermes with the onboard-me prompt now?"; then
         exec hermes "onboard me using $ONBOARDING_PATH"
       fi ;;
esac

cat <<EOF >&2
Next: run your preferred LLM harness with the onboard-me prompt.

Hermes:
  hermes "onboard me using $ONBOARDING_PATH"

Claude Code / Codex / another harness — paste at the prompt:
  onboard me using $ONBOARDING_PATH

The onboarding prompt walks through skills, hooks, credentials, and ports.

Database readiness was verified on $DB_HOST:$DB_PORT before MCP handling. The
MCP endpoint is at $MCP_URL when the selected MCP transport serves HTTP.
EOF
