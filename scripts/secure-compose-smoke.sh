#!/usr/bin/env bash
# Prove the production Compose overlay starts and serves authenticated CQL
# and Arrow Flight over TLS (t_3422ae92, t_58db6320).
#
#   1. generate throwaway test certificates (scripts/gen-compose-test-certs.sh)
#   2. bring up docker-compose.yml + docker-compose.secure.yml (3 nodes,
#      FERROSA_MODE=production, TLS on every listener and internode)
#   3. wait until every node's /readyz answers over HTTPS (verified against the
#      test CA), and confirm plaintext HTTP to the same port is NOT served
#   4. run one authenticated CQL query over TLS (cqlsh --ssl, CA-validated),
#      and confirm a plaintext CQL client is refused
#   5. Arrow Flight on node1 (published 8815): scripts/flight-tls-probe.sh —
#      plaintext gRPC refused, authenticated Handshake + ListFlights over TLS
#   6. tear everything down (always, including volumes)
#
# Every node gets ONE node-wide certificate (FERROSA_TLS_*) in the overlay, so
# this also proves a single [tls] certificate covers every listener.
#
# Requires the node image to exist already (no build): every node uses
# ${FERROSA_SMOKE_IMAGE:-ferrosa-smoke:latest}. The image must contain curl
# for the healthcheck and a ferrosa binary built with the `flight` feature
# (a default-features binary never binds 8815 and step 5 fails, saying so).
# The host needs curl with HTTP/2 and python3 for the Flight probe.
#
# Environment:
#   FERROSA_CONTAINER_RUNTIME  docker (default) or podman
#   FERROSA_SEED_ADMIN_PASSWORD admin password to seed (default: random)
#   FERROSA_SECURE_READY_SECS  readiness deadline (default: 300)
#   FERROSA_SECURE_KEEP=1      leave the cluster running on exit (debugging)
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

runtime="${FERROSA_CONTAINER_RUNTIME:-docker}"
case "$runtime" in
    docker | podman) ;;
    *)
        echo "ERROR: FERROSA_CONTAINER_RUNTIME must be docker or podman (got '$runtime')" >&2
        exit 2
        ;;
esac
project="ferrosa-secure"
compose=("$runtime" compose -p "$project" -f docker-compose.yml -f docker-compose.secure.yml)
ready_secs="${FERROSA_SECURE_READY_SECS:-300}"
cqlsh_image="${FERROSA_CQLSH_IMAGE:-cassandra:5.0}"

if [[ -z "${FERROSA_SEED_ADMIN_PASSWORD:-}" ]]; then
    FERROSA_SEED_ADMIN_PASSWORD="$(openssl rand -hex 24)"
fi
export FERROSA_SEED_ADMIN_PASSWORD

log() { printf '[secure-compose] %s\n' "$*"; }

teardown() {
    local status=$?
    if [[ $status -ne 0 ]]; then
        log "FAILED (exit $status) — node logs follow"
        for node in node1 node2 node3; do
            echo "===== $node (last 120 lines, ANSI stripped) ====="
            "${compose[@]}" logs --no-color --tail 120 "$node" 2>&1 \
                | sed -E 's/\x1b\[[0-9;]*m//g' || true
        done
        "${compose[@]}" ps -a || true
    fi
    if [[ "${FERROSA_SECURE_KEEP:-0}" == "1" ]]; then
        log "FERROSA_SECURE_KEEP=1: leaving the cluster up (${compose[*]} down -v to remove)"
    else
        log "tearing down"
        "${compose[@]}" down -v --remove-orphans || log "WARNING: teardown reported an error"
    fi
    exit $status
}
trap teardown EXIT

log "generating throwaway test certificates"
scripts/gen-compose-test-certs.sh --force

log "starting the 3-node production overlay (${compose[*]})"
"${compose[@]}" up -d --no-build

# ── readiness over HTTPS ───────────────────────────────────────────────────
# node:host-port pairs (docker-compose.yml publishes 9090/9091/9092).
deadline=$((SECONDS + ready_secs))
for pair in node1:9090 node2:9091 node3:9092; do
    node="${pair%%:*}"
    port="${pair##*:}"
    until curl -sf --max-time 5 --cacert .compose-tls/ca.crt \
        "https://127.0.0.1:${port}/readyz" >/dev/null; do
        state="$("$runtime" inspect -f '{{.State.Status}}' "${project}-${node}-1" 2>/dev/null || echo missing)"
        if [[ "$state" == "exited" || "$state" == "missing" ]]; then
            log "ERROR: $node is $state before becoming ready"
            exit 1
        fi
        if ((SECONDS >= deadline)); then
            log "ERROR: $node did not report ready over HTTPS within ${ready_secs}s"
            exit 1
        fi
        sleep 3
    done
    log "$node ready over HTTPS (port $port, certificate verified against the test CA)"
    if curl -s --max-time 5 "http://127.0.0.1:${port}/readyz" 2>/dev/null | grep -q .; then
        log "ERROR: $node served /readyz over plaintext HTTP"
        exit 1
    fi
    log "$node refuses plaintext HTTP on the web port"
done

# ── one authenticated CQL query over TLS ───────────────────────────────────
# cqlsh runs on the compose network and connects to node1 by name; the node
# certificate carries DNS:node1 and SSL_VALIDATE checks it against the test CA.
network="${project}_default"
ca_mount=(-v "$repo_root/.compose-tls/ca.crt:/tls/ca.crt:ro")
query="SELECT cluster_name, release_version FROM system.local"
log "running an authenticated CQL query over TLS"
ok=0
for attempt in $(seq 1 40); do
    if out="$("$runtime" run --rm --network "$network" "${ca_mount[@]}" \
        -e SSL_CERTFILE=/tls/ca.crt -e SSL_VALIDATE=true \
        "$cqlsh_image" cqlsh --ssl -u ferrosa_admin -p "$FERROSA_SEED_ADMIN_PASSWORD" \
        node1 9042 -e "$query" 2>&1)"; then
        ok=1
        break
    fi
    log "attempt $attempt: cqlsh over TLS not yet successful: $(echo "$out" | tail -1)"
    sleep 5
done
if [[ $ok -ne 1 ]]; then
    log "ERROR: authenticated CQL over TLS never succeeded"
    echo "$out"
    exit 1
fi
echo "$out"
# system.local.release_version is "<cassandra version>-ferrosa" and the query
# returns exactly one row; anything else means cqlsh reached something else.
if ! grep -q -- "-ferrosa" <<<"$out" || ! grep -q "(1 rows)" <<<"$out"; then
    log "ERROR: the query ran but did not return ferrosa's system.local row"
    exit 1
fi
log "authenticated CQL over TLS succeeded"

if "$runtime" run --rm --network "$network" "$cqlsh_image" \
    cqlsh --connect-timeout 10 -u ferrosa_admin -p "$FERROSA_SEED_ADMIN_PASSWORD" \
    node1 9042 -e "$query" >/dev/null 2>&1; then
    log "ERROR: a plaintext CQL client was served on a TLS-required listener"
    exit 1
fi
log "plaintext CQL client refused"

# ── Arrow Flight over TLS ──────────────────────────────────────────────────
# node1 publishes 8815. The node certificate carries DNS:localhost and is
# verified against the test CA.
log "probing Arrow Flight on node1 (localhost:8815)"
flight_up=0
for attempt in $(seq 1 20); do
    if curl -s --max-time 5 --cacert .compose-tls/ca.crt -o /dev/null \
        "https://localhost:8815/" 2>/dev/null; then
        flight_up=1
        break
    fi
    log "attempt $attempt: Flight TLS port not answering yet"
    sleep 3
done
if [[ $flight_up -ne 1 ]]; then
    log "ERROR: nothing serves TLS on node1:8815 — is the image built with --features flight?"
    exit 1
fi
FLIGHT_PROBE_USER=ferrosa_admin FLIGHT_PROBE_PASSWORD="$FERROSA_SEED_ADMIN_PASSWORD" \
    scripts/flight-tls-probe.sh localhost:8815 .compose-tls/ca.crt

log "PASS: production overlay started with one node-wide certificate; HTTPS, CQL and Arrow Flight over TLS work"
