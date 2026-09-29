#!/usr/bin/env bash
# Generate THROWAWAY test certificates for docker-compose.secure.yml.
#
# Writes a private CA and one certificate per compose node into a gitignored
# directory (default: .compose-tls/ at the repo root):
#
#   .compose-tls/ca.crt, ca.key          test CA (never trust it outside tests)
#   .compose-tls/nodeN/node.crt          leaf for nodeN, signed by the CA
#   .compose-tls/nodeN/node.key          its private key
#   .compose-tls/nodeN/ca.crt            copy of the CA (internode trust anchor)
#
# Each leaf carries the SANs a client can reach that node by:
#   DNS:nodeN, DNS:localhost, IP:127.0.0.1, IP:<subnet prefix>.10N
# The IP SAN matters: internode TLS verifies the peer's IP address (the dialer
# uses the resolved peer IP as the TLS server name), and the overlay pins each
# node to <prefix>.10N for exactly that reason.
#
# These are for local and CI smoke runs only. A real deployment uses
# certificates from its own PKI.
#
# Usage: scripts/gen-compose-test-certs.sh [--force]
#   FERROSA_COMPOSE_TLS_DIR      output directory (default: <repo>/.compose-tls)
#   FERROSA_SECURE_SUBNET_PREFIX first three octets of the overlay subnet
#                                (default: 172.28.77; must match the overlay)
#   FERROSA_COMPOSE_NODES        node count (default: 3)
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out_dir="${FERROSA_COMPOSE_TLS_DIR:-$repo_root/.compose-tls}"
prefix="${FERROSA_SECURE_SUBNET_PREFIX:-172.28.77}"
nodes="${FERROSA_COMPOSE_NODES:-3}"
force=0

for arg in "$@"; do
    case "$arg" in
        --force) force=1 ;;
        -h | --help)
            sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; /^set -euo/d'
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $arg (expected --force)" >&2
            exit 2
            ;;
    esac
done

command -v openssl >/dev/null || {
    echo "ERROR: openssl is required to generate the test certificates" >&2
    exit 1
}
if ! [[ "$prefix" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
    echo "ERROR: FERROSA_SECURE_SUBNET_PREFIX must be three octets (got '$prefix')" >&2
    exit 2
fi
if ! [[ "$nodes" =~ ^[1-9]$ ]]; then
    echo "ERROR: FERROSA_COMPOSE_NODES must be 1-9 (got '$nodes')" >&2
    exit 2
fi

if [[ -f "$out_dir/ca.crt" && $force -eq 0 ]]; then
    echo "test certificates already present in $out_dir (use --force to regenerate)"
    exit 0
fi

rm -rf "$out_dir"
mkdir -p "$out_dir"
umask 077

new_key() {
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null
}

# ── CA ─────────────────────────────────────────────────────────────────────
new_key "$out_dir/ca.key"
openssl req -x509 -new -key "$out_dir/ca.key" -sha256 -days 30 \
    -subj "/CN=ferrosa compose THROWAWAY test CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$out_dir/ca.crt"

# ── one leaf per node ──────────────────────────────────────────────────────
for n in $(seq 1 "$nodes"); do
    dir="$out_dir/node$n"
    mkdir -p "$dir"
    new_key "$dir/node.key"
    openssl req -new -key "$dir/node.key" -subj "/CN=node$n" -out "$dir/node.csr"
    cat > "$dir/ext.cnf" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:node$n,DNS:localhost,IP:127.0.0.1,IP:$prefix.10$n
EOF
    openssl x509 -req -in "$dir/node.csr" -CA "$out_dir/ca.crt" -CAkey "$out_dir/ca.key" \
        -CAcreateserial -days 30 -sha256 -extfile "$dir/ext.cnf" -out "$dir/node.crt" 2>/dev/null
    rm -f "$dir/node.csr" "$dir/ext.cnf"
    cp "$out_dir/ca.crt" "$dir/ca.crt"
    # The node process reads these through a bind mount, possibly as a
    # different uid (rootless podman maps ids). These are throwaway test keys,
    # so the leaf files are world-readable; the CA key stays 0600.
    chmod 0644 "$dir/node.crt" "$dir/ca.crt"
    chmod 0644 "$dir/node.key"
    chmod 0755 "$dir"
    openssl verify -CAfile "$out_dir/ca.crt" "$dir/node.crt" >/dev/null
done
rm -f "$out_dir/ca.srl"
chmod 0755 "$out_dir"
chmod 0644 "$out_dir/ca.crt"

echo "wrote THROWAWAY test CA + $nodes node certificates to $out_dir (subnet $prefix.0/24)"
