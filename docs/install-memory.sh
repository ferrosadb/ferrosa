#!/usr/bin/env bash
# ferrosa-memory installer — fetches a release tarball, installs to ~/.ferrosa/,
# offers system-service registration. Assumes Ferrosa is already running at
# localhost:9042 (install via https://ferrosadb.com/install.sh first).
#
# Usage:
#   curl -fsSL https://ferrosadb.com/install-memory.sh | bash
#   curl -fsSL https://ferrosadb.com/install-memory.sh | bash -s -- --version v0.12.0 --no-service
set -euo pipefail

REPO="ferrosadb/ferrosa-memory"
RELEASE_HOST="https://github.com/${REPO}/releases"
INSTALL_ROOT="${HOME}/.ferrosa"
BIN_DIR="${INSTALL_ROOT}/bin"
CONFIG_DIR="${INSTALL_ROOT}/config"
DATA_DIR="${INSTALL_ROOT}/data"
LOG_DIR="${INSTALL_ROOT}/logs"

VERSION=""
WANT_SERVICE=""    # ask|yes|no

while [ $# -gt 0 ]; do
  case "$1" in
    --version)      VERSION="$2"; shift 2 ;;
    --no-service)   WANT_SERVICE="no"; shift ;;
    --service)      WANT_SERVICE="yes"; shift ;;
    -h|--help)
      cat <<EOF
ferrosa-memory installer
  --version <tag>           install a specific tag (default: latest)
  --service / --no-service  enable or skip system-service install
EOF
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

say() { printf ':: %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

detect_target() {
  local os arch
  os=$(uname -s); arch=$(uname -m)
  case "$os/$arch" in
    Darwin/arm64)              echo "aarch64-apple-darwin" ;;
    Darwin/x86_64)
      die "Intel macOS is not supported in v0.x. Please build from source: https://github.com/ferrosadb/ferrosa-memory#building" ;;
    Linux/x86_64)              echo "x86_64-unknown-linux-musl" ;;
    Linux/aarch64|Linux/arm64) echo "aarch64-unknown-linux-musl" ;;
    *) die "unsupported platform: $os/$arch" ;;
  esac
}
TARGET=$(detect_target)

if [ -z "$VERSION" ]; then
  VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
              | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)
fi
[ -n "$VERSION" ] || die "no release found at https://github.com/${REPO}/releases"

TARBALL="ferrosa-memory-${VERSION}-${TARGET}.tar.gz"
URL="${RELEASE_HOST}/download/${VERSION}/${TARBALL}"
SUMS_URL="${RELEASE_HOST}/download/${VERSION}/SHA256SUMS"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
say "downloading $TARBALL"
curl -fsSL --output "$TMP/$TARBALL" "$URL"
curl -fsSL --output "$TMP/SHA256SUMS" "$SUMS_URL"

say "verifying SHA256"
( cd "$TMP" && grep "$TARBALL" SHA256SUMS | shasum -a 256 -c - ) \
  || die "checksum verification FAILED"

say "installing to $INSTALL_ROOT"
mkdir -p "$BIN_DIR" "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR"
tar -xzf "$TMP/$TARBALL" -C "$TMP"

cp "$TMP/ferrosa-memory-mcp" "$BIN_DIR/"
chmod +x "$BIN_DIR/ferrosa-memory-mcp"

if [ ! -f "$CONFIG_DIR/ferrosa-memory.toml" ]; then
  cp "$TMP/config/ferrosa-memory.example.toml" "$CONFIG_DIR/ferrosa-memory.toml"
  say "wrote default config to $CONFIG_DIR/ferrosa-memory.toml"
else
  say "kept existing $CONFIG_DIR/ferrosa-memory.toml"
fi

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

case "$WANT_SERVICE" in
  yes) do_service ;;
  no)  : ;;
  "")  prompt_yes "Register ferrosa-memory as a user service (autostart on login)?" \
         && do_service ;;
esac

cat <<EOF >&2

ferrosa-memory $VERSION installed.

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

Docs: https://github.com/ferrosadb/ferrosa-memory
EOF
