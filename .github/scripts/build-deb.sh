#!/usr/bin/env bash
# Build the ferrosa .deb from glibc release binaries.
#
# Usage:
#   build-deb.sh <deb-arch> <release-bin-dir> <version> <max-glibc> <out-dir>
# Example:
#   build-deb.sh amd64 target/x86_64-unknown-linux-gnu/release 0.27.1 2.35 dist
#
# Produces <out-dir>/ferrosa_<version>_<deb-arch>.deb.
#
# The .deb carries the glibc build (15-20% faster than musl; a Debian/Ubuntu
# host always has glibc). Its control file declares `libc6 (>= X)`, X read from
# the packaged binaries, so apt refuses a host whose glibc is too old instead of
# the binary failing at start. <max-glibc> is the oldest glibc the package must
# install on (2.35 = Ubuntu 22.04): a binary that needs more fails the build
# here, loudly, rather than shipping a .deb 22.04 cannot install.
# tests/ci/test_build_deb.py exercises this script.
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "ERROR: usage: $0 <deb-arch> <release-bin-dir> <version> <max-glibc> <out-dir>" >&2
  exit 2
fi
ARCH="$1"
BIN_DIR="$2"
VERSION="$3"
MAX_GLIBC="$4"
OUT_DIR="$5"

for bin in ferrosa ferrosa-ctl; do
  [[ -x "${BIN_DIR}/${bin}" ]] || { echo "ERROR: missing ${BIN_DIR}/${bin}" >&2; exit 1; }
done

# The newest GLIBC_ symbol version either binary references.
required="$(objdump -T "${BIN_DIR}/ferrosa" "${BIN_DIR}/ferrosa-ctl" \
  | grep -oE 'GLIBC_[0-9]+\.[0-9]+' | sed 's/^GLIBC_//' | sort -uV | tail -n1)"
if [[ -z "$required" ]]; then
  echo "ERROR: could not read a GLIBC_ requirement from ${BIN_DIR}/ferrosa; is it a glibc build?" >&2
  exit 1
fi
if [[ "$(printf '%s\n%s\n' "$MAX_GLIBC" "$required" | sort -V | tail -n1)" != "$MAX_GLIBC" ]]; then
  echo "ERROR: the binaries need glibc ${required}, newer than ${MAX_GLIBC}, the oldest glibc this .deb must install on" >&2
  exit 1
fi
echo "ferrosa ${VERSION} (${ARCH}) needs glibc ${required}; Depends: libc6 (>= ${required})"

PKG_DIR="${OUT_DIR}/ferrosa_${VERSION}_${ARCH}"
DEB_FILE="${OUT_DIR}/ferrosa_${VERSION}_${ARCH}.deb"
rm -rf "$PKG_DIR"
mkdir -p "${PKG_DIR}/DEBIAN"

cat > "${PKG_DIR}/DEBIAN/control" <<CTRL
Package: ferrosa
Version: ${VERSION}
Section: database
Priority: optional
Architecture: ${ARCH}
Depends: ca-certificates, libc6 (>= ${required})
Maintainer: Ferrosa Team <ferrosa@ferrosadb.com>
Homepage: https://www.ferrosa.ai
Description: CQL-compatible distributed database with S3-backed storage
 Ferrosa is a Rust reimplementation of Apache Cassandra with S3-backed
 storage, pluggable secondary indexes, built-in graph queries, and
 pair-mode high availability with automatic failover.
CTRL

cat > "${PKG_DIR}/DEBIAN/postinst" <<'POSTINST'
#!/bin/sh
set -e
if ! id -u ferrosa >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin ferrosa
fi
mkdir -p /var/lib/ferrosa && chown ferrosa:ferrosa /var/lib/ferrosa
mkdir -p /var/log/ferrosa && chown ferrosa:ferrosa /var/log/ferrosa
POSTINST
chmod 755 "${PKG_DIR}/DEBIAN/postinst"

mkdir -p "${PKG_DIR}/usr/bin"
install -m 0755 "${BIN_DIR}/ferrosa" "${PKG_DIR}/usr/bin/ferrosa"
install -m 0755 "${BIN_DIR}/ferrosa-ctl" "${PKG_DIR}/usr/bin/ferrosa-ctl"

mkdir -p "${PKG_DIR}/lib/systemd/system"
if [[ -f systemd/ferrosa.service ]]; then
  cp systemd/ferrosa.service "${PKG_DIR}/lib/systemd/system/ferrosa.service"
else
  cat > "${PKG_DIR}/lib/systemd/system/ferrosa.service" <<SERVICE
[Unit]
Description=Ferrosa Database Server
After=network-online.target
Wants=network-online.target
Documentation=https://www.ferrosa.ai

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
fi

mkdir -p "${PKG_DIR}/etc/ferrosa"
mkdir -p "${PKG_DIR}/var/lib/ferrosa"
mkdir -p "${PKG_DIR}/usr/share/doc/ferrosa"
echo "Copyright 2026 Ferrosa Contributors. All rights reserved." \
  > "${PKG_DIR}/usr/share/doc/ferrosa/copyright"

dpkg-deb --build --root-owner-group "$PKG_DIR" "$DEB_FILE"
rm -rf "$PKG_DIR"
