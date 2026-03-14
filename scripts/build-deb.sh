#!/usr/bin/env bash
# Build ferrosa Debian package (.deb) for Linux amd64.
#
# Produces: dist/ferrosa_<version>_amd64.deb
#
# Contents:
#   /usr/bin/ferrosa           — database server
#   /usr/bin/ferrosa-ctl       — CLI admin tool
#   /usr/share/ferrosa/docs/   — marketing site (HTML, CSS, SVG)
#   /usr/share/ferrosa/tests/  — smoke tests
#   /etc/ferrosa/              — config directory (empty, for operator use)
#   /var/lib/ferrosa/          — default data directory
#
# Prerequisites:
#   - Rust toolchain (cargo, rustc)
#   - dpkg-deb (standard on Debian/Ubuntu, installable elsewhere)
#
# Usage:
#   ./scripts/build-deb.sh              # build for current architecture
#   ./scripts/build-deb.sh --target x86_64-unknown-linux-gnu  # cross-compile

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

# Parse arguments
TARGET=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --target) TARGET="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Extract version from Cargo.toml
VERSION=$(grep -m1 'version = ' Cargo.toml | sed 's/.*"\(.*\)".*/\1/')
ARCH="amd64"

if [[ -n "$TARGET" ]]; then
    case "$TARGET" in
        x86_64-*)  ARCH="amd64" ;;
        aarch64-*) ARCH="arm64" ;;
        *) echo "Unsupported target: $TARGET"; exit 1 ;;
    esac
fi

PKG_NAME="ferrosa"
PKG_DIR="dist/${PKG_NAME}_${VERSION}_${ARCH}"
DEB_FILE="dist/${PKG_NAME}_${VERSION}_${ARCH}.deb"

echo "=== Building ferrosa v${VERSION} (${ARCH}) ==="

# ── Step 1: Build release binaries ──────────────────────────────────
echo "Building release binaries..."
CARGO_BUILD_ARGS=(build --release -p ferrosa -p ferrosa-ctl)
if [[ -n "$TARGET" ]]; then
    CARGO_BUILD_ARGS+=(--target "$TARGET")
    BINARY_DIR="target/${TARGET}/release"
else
    BINARY_DIR="target/release"
fi

cargo "${CARGO_BUILD_ARGS[@]}"

# Verify binaries exist
for bin in ferrosa ferrosa-ctl; do
    if [[ ! -f "${BINARY_DIR}/${bin}" ]]; then
        echo "ERROR: Binary not found: ${BINARY_DIR}/${bin}"
        exit 1
    fi
done

echo "Binaries built successfully."

# ── Step 2: Assemble package directory ──────────────────────────────
echo "Assembling package directory..."
rm -rf "$PKG_DIR"

# Debian control metadata
mkdir -p "${PKG_DIR}/DEBIAN"
cat > "${PKG_DIR}/DEBIAN/control" <<CTRL
Package: ferrosa
Version: ${VERSION}
Section: database
Priority: optional
Architecture: ${ARCH}
Depends: ca-certificates
Maintainer: Ferrosa Team <ferrosa@ferrosadb.com>
Homepage: https://ferrosadb.com
Description: CQL-compatible distributed database with S3-backed storage
 Ferrosa is a Rust reimplementation of Apache Cassandra with S3-backed
 storage, pluggable secondary indexes (B-tree, hash, composite, phonetic,
 filtered, vector HNSW/IVFFlat), built-in graph queries, and pair-mode
 high availability with automatic failover.
CTRL

# Post-install: create ferrosa user and data directory
cat > "${PKG_DIR}/DEBIAN/postinst" <<'POSTINST'
#!/bin/sh
set -e

# Create ferrosa system user if it doesn't exist
if ! id -u ferrosa >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin ferrosa
fi

# Ensure data and config directories exist with correct ownership
mkdir -p /var/lib/ferrosa
chown ferrosa:ferrosa /var/lib/ferrosa

mkdir -p /var/log/ferrosa
chown ferrosa:ferrosa /var/log/ferrosa
POSTINST
chmod 755 "${PKG_DIR}/DEBIAN/postinst"

# Binaries
mkdir -p "${PKG_DIR}/usr/bin"
cp "${BINARY_DIR}/ferrosa" "${PKG_DIR}/usr/bin/"
cp "${BINARY_DIR}/ferrosa-ctl" "${PKG_DIR}/usr/bin/"
strip "${PKG_DIR}/usr/bin/ferrosa" 2>/dev/null || true
strip "${PKG_DIR}/usr/bin/ferrosa-ctl" 2>/dev/null || true

# Marketing docs
mkdir -p "${PKG_DIR}/usr/share/ferrosa/docs"
cp -r docs/*.html docs/*.svg docs/*.md "${PKG_DIR}/usr/share/ferrosa/docs/" 2>/dev/null || true
cp docs/CNAME "${PKG_DIR}/usr/share/ferrosa/docs/" 2>/dev/null || true
# Exclude any non-public content (specs, plans are NOT in docs/)

# Smoke tests
mkdir -p "${PKG_DIR}/usr/share/ferrosa/tests"
cp tests/docker-smoke.sh "${PKG_DIR}/usr/share/ferrosa/tests/" 2>/dev/null || true
cp tests/cqlsh_smoke_test.sh "${PKG_DIR}/usr/share/ferrosa/tests/" 2>/dev/null || true
cp docker-compose.yml "${PKG_DIR}/usr/share/ferrosa/tests/" 2>/dev/null || true
cp Dockerfile "${PKG_DIR}/usr/share/ferrosa/tests/" 2>/dev/null || true

# Systemd service file
mkdir -p "${PKG_DIR}/lib/systemd/system"
cat > "${PKG_DIR}/lib/systemd/system/ferrosa.service" <<SERVICE
[Unit]
Description=Ferrosa Database Server
After=network-online.target
Wants=network-online.target
Documentation=https://ferrosadb.com

[Service]
Type=simple
User=ferrosa
Group=ferrosa
ExecStart=/usr/bin/ferrosa
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536
LimitMEMLOCK=infinity

Environment=FERROSA_DATA_DIR=/var/lib/ferrosa
Environment=FERROSA_LOG_DIR=/var/log/ferrosa
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
SERVICE

# Config directory
mkdir -p "${PKG_DIR}/etc/ferrosa"

# Data directory (empty, created by postinst with correct ownership)
mkdir -p "${PKG_DIR}/var/lib/ferrosa"

# Copyright / license
mkdir -p "${PKG_DIR}/usr/share/doc/ferrosa"
if [[ -f LICENSE ]]; then
    cp LICENSE "${PKG_DIR}/usr/share/doc/ferrosa/copyright"
else
    echo "Copyright $(date +%Y) Ferrosa Team. Licensed under Apache-2.0." \
        > "${PKG_DIR}/usr/share/doc/ferrosa/copyright"
fi

# ── Step 3: Build .deb ──────────────────────────────────────────────
echo "Building Debian package..."
mkdir -p dist
dpkg-deb --build --root-owner-group "$PKG_DIR" "$DEB_FILE"

# ── Step 4: Verify ──────────────────────────────────────────────────
echo ""
echo "=== Package built successfully ==="
echo "  File: ${DEB_FILE}"
echo "  Size: $(du -h "$DEB_FILE" | cut -f1)"
echo ""
echo "Contents:"
dpkg-deb --contents "$DEB_FILE" | head -20
echo ""
echo "Install with:  sudo dpkg -i ${DEB_FILE}"
echo "Start with:    sudo systemctl start ferrosa"
echo "Status:        sudo systemctl status ferrosa"
echo "Logs:          journalctl -u ferrosa -f"

# Clean up staging directory
rm -rf "$PKG_DIR"
