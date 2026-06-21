#!/usr/bin/env bash
# ferrosa fast setup — installs prebuilt binaries via the LATEST file.
#
# Reads https://ferrosadb.com/LATEST (a plain-text version tag like "v0.16.0"),
# downloads the matching release tarball from
# https://github.com/ferrosadb/ferrosa/releases, verifies SHA256, installs to
# ~/.ferrosa/, and optionally registers as a user service. No source clone,
# no compile.
#
# Usage:
#   curl -fsSL https://ferrosadb.com/setup.sh | bash
#   curl -fsSL https://ferrosadb.com/setup.sh | bash -s -- --version v0.16.0 --no-service
#
# Env overrides (mostly for testing):
#   FERROSA_LATEST_URL   — where to fetch the version pointer (default https://ferrosadb.com/LATEST)
#   FERROSA_RELEASE_HOST — release artifact root (default github.com release URL)
#   FERROSA_INSTALL_ROOT — install prefix (default $HOME/.ferrosa)
set -euo pipefail

REPO="ferrosadb/ferrosa"
LATEST_URL="${FERROSA_LATEST_URL:-https://ferrosadb.com/LATEST}"
RELEASE_HOST="${FERROSA_RELEASE_HOST:-https://github.com/${REPO}/releases}"
INSTALL_ROOT="${FERROSA_INSTALL_ROOT:-${HOME}/.ferrosa}"
BIN_DIR="${INSTALL_ROOT}/bin"
CONFIG_DIR="${INSTALL_ROOT}/config"
DATA_DIR="${INSTALL_ROOT}/data"
LOG_DIR="${INSTALL_ROOT}/logs"

VERSION=""
WANT_SERVICE=""    # ask|yes|no
WANT_PASSWORD=""   # ask|yes|no

while [ $# -gt 0 ]; do
  case "$1" in
    --version)      VERSION="$2"; shift 2 ;;
    --no-service)   WANT_SERVICE="no"; shift ;;
    --service)      WANT_SERVICE="yes"; shift ;;
    --no-password)  WANT_PASSWORD="no"; shift ;;
    --password)     WANT_PASSWORD="yes"; shift ;;
    -h|--help)
      cat <<EOF
ferrosa fast setup
  --version <tag>            install a specific tag (default: read $LATEST_URL)
  --service / --no-service   enable or skip system-service install
  --password / --no-password prompt for or skip admin-password setup
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
      die "Intel macOS is not supported in v0.x. Build from source: https://github.com/${REPO}#building" ;;
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

TARBALL="ferrosa-${VERSION}-${TARGET}.tar.gz"
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

cp "$TMP/ferrosa"     "$BIN_DIR/"
cp "$TMP/ferrosa-ctl" "$BIN_DIR/"
chmod +x "$BIN_DIR/ferrosa" "$BIN_DIR/ferrosa-ctl"

if [ ! -f "$CONFIG_DIR/ferrosa.toml" ]; then
  cp "$TMP/config/ferrosa.example.toml" "$CONFIG_DIR/ferrosa.toml"
  say "wrote default config to $CONFIG_DIR/ferrosa.toml"
else
  say "kept existing $CONFIG_DIR/ferrosa.toml"
fi

prompt_yes() {
  local q="$1" a
  if [ ! -t 0 ] && [ ! -r /dev/tty ]; then
    # Non-interactive (piped curl, no tty): default to NO for prompts.
    return 1
  fi
  read -r -p "$q [y/N] " a < /dev/tty
  case "${a:-N}" in y|Y|yes|Yes|YES) return 0 ;; *) return 1 ;; esac
}

register_macos() {
  local plist="$HOME/Library/LaunchAgents/com.ferrosadb.ferrosa.plist"
  mkdir -p "$(dirname "$plist")"
  sed "s|__HOME__|$HOME|g" "$TMP/launchd/com.ferrosadb.ferrosa.plist" > "$plist"
  launchctl bootout "gui/$(id -u)" "$plist" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$plist"
  say "launchd: com.ferrosadb.ferrosa loaded; will start now and on every login"
}

register_linux() {
  local unit="$HOME/.config/systemd/user/ferrosa.service"
  mkdir -p "$(dirname "$unit")"
  cp "$TMP/systemd/ferrosa.service" "$unit"
  systemctl --user daemon-reload
  systemctl --user enable --now ferrosa.service
  if command -v loginctl >/dev/null; then
    loginctl enable-linger "$USER" 2>/dev/null \
      && say "systemd: lingering enabled (boot-time start without login)"
  fi
  say "systemd: ferrosa.service enabled and started"
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
  "")  prompt_yes "Register ferrosa as a user service (autostart on login)?" \
         && do_service ;;
esac

wait_for_cql() {
  local host="${1:-127.0.0.1}" port="${2:-9042}"
  for _ in $(seq 1 30); do
    if (echo > "/dev/tcp/$host/$port") 2>/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

do_password() {
  if ! wait_for_cql 127.0.0.1 9042; then
    say "ferrosa not reachable on 127.0.0.1:9042 yet — skipping password setup"
    say "run later: $BIN_DIR/ferrosa-ctl auth set-password"
    return
  fi
  # Under `curl … | bash` this script's stdin is the pipe, not a terminal, so
  # ferrosa-ctl's masked prompt (rpassword reads stdin) can't disable echo —
  # the password echoes in cleartext and the read never completes (hang). Bind
  # the controlling terminal explicitly so the prompt + confirmation work.
  if [ ! -r /dev/tty ]; then
    say "no controlling terminal — skipping interactive password setup"
    say "run later from a terminal: $BIN_DIR/ferrosa-ctl auth set-password --user ferrosa_admin"
    return
  fi
  say "set ferrosa_admin password (current default: ferrosa_admin)"
  "$BIN_DIR/ferrosa-ctl" auth set-password --user ferrosa_admin < /dev/tty
}

case "$WANT_PASSWORD" in
  yes) do_password ;;
  no)  : ;;
  "")  prompt_yes "Set the ferrosa_admin password now?" && do_password ;;
esac

cat <<EOF >&2

ferrosa $VERSION installed.

  binaries: $BIN_DIR
  config:   $CONFIG_DIR/ferrosa.toml
  data:     $DATA_DIR
  logs:     $LOG_DIR

Add to your shell profile:
  export PATH="\$HOME/.ferrosa/bin:\$PATH"

If you didn't register a service, run manually:
  FERROSA_CONFIG="$CONFIG_DIR/ferrosa.toml" "$BIN_DIR/ferrosa"

For Ferrosa Memory + LLM onboarding, run:
  curl -fsSL https://ferrosadb.com/setup-memory.sh | bash

Docs: https://ferrosadb.com/database/getting-started.html
EOF
