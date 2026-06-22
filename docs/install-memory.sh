#!/usr/bin/env bash
# ferrosa-memory installer — fetches a release tarball, installs to ~/.ferrosa/,
# offers system-service registration. Assumes Ferrosa is already running at
# localhost:9042 (install via https://ferrosadb.com/install.sh first).
#
# SOURCE OF TRUTH: this file (ferrosadb/ferrosa-memory : docs/install-memory.sh).
# It is mirrored into ferrosadb/ferrosa docs/install-memory.sh, which is what
# GitHub Pages serves at https://ferrosadb.com/install-memory.sh. Edit it HERE;
# the ferrosa copy is a published mirror.
#
# It is idempotent: re-running upgrades an existing install in place. When the
# resolved version already matches what's installed it does nothing (use
# --force to reinstall).
#
# Channels:
#   stable  (default) — the latest release a maintainer has promoted (GitHub
#                       "latest"). Resolves via /releases/latest.
#   nightly           — the newest published release, including the prereleases
#                       cut automatically each night. Resolves via /releases.
#
# Usage:
#   curl -fsSL https://ferrosadb.com/install-memory.sh | bash
#   curl -fsSL https://ferrosadb.com/install-memory.sh | bash -s -- --channel nightly
#   curl -fsSL https://ferrosadb.com/install-memory.sh | bash -s -- --version v0.16.0 --no-service
set -euo pipefail

REPO="ferrosadb/ferrosa-memory"
RELEASE_HOST="https://github.com/${REPO}/releases"
INSTALL_ROOT="${HOME}/.ferrosa"
BIN_DIR="${INSTALL_ROOT}/bin"
CONFIG_DIR="${INSTALL_ROOT}/config"
DATA_DIR="${INSTALL_ROOT}/data"
LOG_DIR="${INSTALL_ROOT}/logs"
RUN_DIR="${INSTALL_ROOT}/run"
# Separate stamp from ferrosa's own .version so the two installers don't clobber
# each other's idempotency state.
VERSION_STAMP="${INSTALL_ROOT}/.memory-version"

VERSION=""
CHANNEL="stable"   # stable|nightly
FORCE="no"
WANT_SERVICE=""    # ask|yes|no
# Install from a local release tarball instead of downloading from GitHub.
# Used by the install smoke test (and devs) to exercise THIS script against a
# just-built binary before it is published. Honors the
# `FERROSA_MEMORY_INSTALL_TARBALL` env var or the `--tarball <path>` flag;
# requires `--version <label>`.
LOCAL_TARBALL="${FERROSA_MEMORY_INSTALL_TARBALL:-}"

# ---------- arg parsing ----------
while [ $# -gt 0 ]; do
  case "$1" in
    --version)      VERSION="$2"; shift 2 ;;
    --channel)      CHANNEL="$2"; shift 2 ;;
    --tarball)      LOCAL_TARBALL="$2"; shift 2 ;;
    --force)        FORCE="yes"; shift ;;
    --no-service)   WANT_SERVICE="no"; shift ;;
    --service)      WANT_SERVICE="yes"; shift ;;
    -h|--help)
      cat <<EOF
ferrosa-memory installer
  --version <tag>            install a specific tag (overrides --channel)
  --channel stable|nightly   release channel (default: stable)
  --force                    reinstall even if already up to date
  --service / --no-service   enable or skip system-service install
EOF
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

case "$CHANNEL" in
  stable|nightly) ;;
  *) echo "error: --channel must be 'stable' or 'nightly'" >&2; exit 2 ;;
esac

say() { printf ':: %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

# ---------- platform detect ----------
detect_target() {
  local os arch
  os=$(uname -s); arch=$(uname -m)
  case "$os/$arch" in
    Darwin/arm64)              echo "aarch64-apple-darwin" ;;
    Darwin/x86_64)
      die "Intel macOS is not supported. Please build from source: https://github.com/ferrosadb/ferrosa-memory#building" ;;
    Linux/x86_64)              echo "x86_64-unknown-linux-musl" ;;
    Linux/aarch64|Linux/arm64) echo "aarch64-unknown-linux-musl" ;;
    *) die "unsupported platform: $os/$arch" ;;
  esac
}
TARGET=$(detect_target)

# ---------- resolve the tag to install ----------
# stable  -> /releases/latest (only non-prerelease, maintainer-promoted)
# nightly -> /releases (newest published, includes nightly prereleases)
resolve_channel_tag() {
  case "$CHANNEL" in
    stable)
      curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
        | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1 ;;
    nightly)
      curl -fsSL "https://api.github.com/repos/${REPO}/releases?per_page=1" \
        | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1 ;;
  esac
}

if [ -z "$VERSION" ]; then
  [ -z "$LOCAL_TARBALL" ] || die "--tarball requires an explicit --version <label>"
  VERSION=$(resolve_channel_tag) || true
  [ -n "$VERSION" ] || die "no ${CHANNEL} release found at ${RELEASE_HOST}"
  say "resolved ${CHANNEL} channel to ${VERSION}"
fi

# ---------- idempotency: compare against what's installed ----------
read_installed_version() {
  # Stamp-only: ferrosa-memory-mcp is a stdio MCP server with no --version flag
  # (running it just starts the server), so there is no safe way to probe an
  # installed version from the binary. Installs predating the stamp are treated
  # as fresh and simply reinstalled.
  [ -f "$VERSION_STAMP" ] && cat "$VERSION_STAMP"
}
INSTALLED_VERSION="$(read_installed_version || true)"
IS_UPGRADE="no"; [ -n "$INSTALLED_VERSION" ] && IS_UPGRADE="yes"

if [ "$INSTALLED_VERSION" = "$VERSION" ] && [ "$FORCE" = "no" ]; then
  say "ferrosa-memory ${VERSION} is already installed (up to date); use --force to reinstall"
  exit 0
fi

if [ "$IS_UPGRADE" = "yes" ]; then
  say "upgrading ferrosa-memory ${INSTALLED_VERSION} -> ${VERSION}"
fi

TARBALL="ferrosa-memory-${VERSION}-${TARGET}.tar.gz"
URL="${RELEASE_HOST}/download/${VERSION}/${TARBALL}"
SUMS_URL="${RELEASE_HOST}/download/${VERSION}/SHA256SUMS"

# ---------- obtain the tarball (download, or use a local build) + verify ----------
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
if [ -n "$LOCAL_TARBALL" ]; then
  [ -f "$LOCAL_TARBALL" ] || die "local tarball not found: $LOCAL_TARBALL"
  say "installing from local tarball $LOCAL_TARBALL (skipping download + checksum)"
  cp "$LOCAL_TARBALL" "$TMP/$TARBALL"
else
  say "downloading $TARBALL"
  curl -fsSL --output "$TMP/$TARBALL" "$URL"
  curl -fsSL --output "$TMP/SHA256SUMS" "$SUMS_URL"

  say "verifying SHA256"
  ( cd "$TMP" && grep "$TARBALL" SHA256SUMS | shasum -a 256 -c - ) \
    || die "checksum verification FAILED"
fi

# ---------- install layout ----------
say "installing to $INSTALL_ROOT"
mkdir -p "$BIN_DIR" "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR" "$RUN_DIR"
tar -xzf "$TMP/$TARBALL" -C "$TMP"

cp "$TMP/ferrosa-memory-mcp" "$BIN_DIR/"
chmod +x "$BIN_DIR/ferrosa-memory-mcp"

# The native setup CLI (`ferrosa-memory`) ships in releases built after v0.16.x.
# Install it when the tarball contains it; older tarballs (e.g. stable v0.16.0)
# bundle only the MCP server, so guard the copy to stay compatible across tags.
if [ -f "$TMP/ferrosa-memory" ]; then
  cp "$TMP/ferrosa-memory" "$BIN_DIR/"
  chmod +x "$BIN_DIR/ferrosa-memory"
  HAS_SETUP_CLI="yes"
else
  HAS_SETUP_CLI="no"
fi

# Record the installed version so the next run is idempotent.
printf '%s\n' "$VERSION" > "$VERSION_STAMP"

if [ ! -f "$CONFIG_DIR/ferrosa-memory.toml" ]; then
  cp "$TMP/config/ferrosa-memory.example.toml" "$CONFIG_DIR/ferrosa-memory.toml"
  say "wrote default config to $CONFIG_DIR/ferrosa-memory.toml"
else
  say "kept existing $CONFIG_DIR/ferrosa-memory.toml"
fi

# ---------- service registration ----------
prompt_yes() {
  local q="$1" a
  read -r -p "$q [y/N] " a < /dev/tty
  case "${a:-N}" in y|Y|yes|Yes|YES) return 0 ;; *) return 1 ;; esac
}

register_macos() {
  local plist="$HOME/Library/LaunchAgents/com.ferrosa-memory.mcp.plist"
  mkdir -p "$(dirname "$plist")"
  sed -e "s|__BINARY_PATH__|$BIN_DIR/ferrosa-memory-mcp|g" \
      -e "s|__REPO_ROOT__|$INSTALL_ROOT|g" \
      -e "s|__CONFIG_PATH__|$CONFIG_DIR/ferrosa-memory.toml|g" \
      "$TMP/launchd/com.ferrosa-memory.mcp.plist" > "$plist"
  launchctl bootout "gui/$(id -u)" "$plist" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$plist"
  say "launchd: com.ferrosa-memory.mcp loaded; will start on every login"
}

register_linux() {
  local unit="$HOME/.config/systemd/user/ferrosa-memory.service"
  mkdir -p "$(dirname "$unit")"
  cp "$TMP/systemd/ferrosa-memory.service" "$unit"
  systemctl --user daemon-reload
  systemctl --user enable --now ferrosa-memory.service
  if command -v loginctl >/dev/null; then
    loginctl enable-linger "$USER" 2>/dev/null \
      && say "systemd: lingering enabled (boot-time start without login)"
  fi
  say "systemd: ferrosa-memory.service enabled and started"
}

do_service() {
  case "$(uname -s)" in
    Darwin) register_macos ;;
    Linux)  register_linux ;;
  esac
}

# On upgrade, restart an already-registered service so the new binary is the
# one actually running (the path is unchanged, so the old process keeps the
# old inode until restarted).
restart_service_if_present() {
  case "$(uname -s)" in
    Darwin)
      local plist="$HOME/Library/LaunchAgents/com.ferrosa-memory.mcp.plist"
      [ -f "$plist" ] || return 0
      launchctl kickstart -k "gui/$(id -u)/com.ferrosa-memory.mcp" 2>/dev/null \
        && say "restarted launchd service to apply the upgrade" || true ;;
    Linux)
      local unit="$HOME/.config/systemd/user/ferrosa-memory.service"
      [ -f "$unit" ] || return 0
      systemctl --user restart ferrosa-memory.service 2>/dev/null \
        && say "restarted systemd --user service to apply the upgrade" || true ;;
  esac
}

# Explicit flags always win. Otherwise: prompt on a fresh install, and on an
# upgrade quietly restart an existing service without re-prompting.
case "$WANT_SERVICE" in
  yes) do_service ;;
  no)  : ;;
  "")
    if [ "$IS_UPGRADE" = "yes" ]; then
      restart_service_if_present
    else
      prompt_yes "Register ferrosa-memory as a user service (autostart on login)?" \
        && do_service
    fi ;;
esac

# ---------- finish ----------
cat <<EOF >&2

ferrosa-memory $VERSION installed (${CHANNEL} channel).

EOF
if [ "$HAS_SETUP_CLI" = "yes" ]; then
  cat <<EOF >&2
  setup:  $BIN_DIR/ferrosa-memory

Run the native setup reconciler any time you want to change local choices:

  $BIN_DIR/ferrosa-memory setup

EOF
fi
cat <<EOF >&2
  binary: $BIN_DIR/ferrosa-memory-mcp
  config: $CONFIG_DIR/ferrosa-memory.toml

This MCP server connects to a running Ferrosa instance at localhost:9042
(default from https://ferrosadb.com/install.sh). Ensure Ferrosa is up:

  curl -fsSL https://ferrosadb.com/install.sh | bash

To register with Claude Code, add to your MCP config:

  {
    "mcpServers": {
      "ferrosa-memory": {
        "command": "$BIN_DIR/ferrosa-memory-mcp",
        "env": { "FERROSA_MEMORY_CONFIG": "$CONFIG_DIR/ferrosa-memory.toml" }
      }
    }
  }

Upgrade later by re-running this installer (idempotent):
  curl -fsSL https://ferrosadb.com/install-memory.sh | bash -s -- --channel ${CHANNEL}

Docs: https://github.com/ferrosadb/ferrosa-memory
EOF
