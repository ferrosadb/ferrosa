#!/usr/bin/env bash
# Prove an Arrow Flight endpoint serves gRPC over TLS only (t_58db6320).
#
#   scripts/flight-tls-probe.sh <host:port> <ca.crt>
#
# Checks, in order (any failure exits non-zero with the reason):
#   1. a plaintext HTTP/2 (h2c) gRPC request gets no gRPC response;
#   2. an unauthenticated ListFlights over TLS (CA-verified, ALPN h2) is
#      answered with grpc-status 16 (UNAUTHENTICATED) — the gRPC service is
#      really behind the TLS port;
#   3. Handshake over TLS with the user's credentials returns grpc-status 0 and
#      a bearer token;
#   4. ListFlights over TLS with that token returns grpc-status 0.
#
# Credentials come from the environment, never the command line:
#   FLIGHT_PROBE_USER       (default ferrosa_admin)
#   FLIGHT_PROBE_PASSWORD   required
# The bearer token is passed to curl through a 0600 header file, not argv.
#
# Requires curl with HTTP/2 and python3 (for gRPC/protobuf framing).
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <host:port> <ca.crt>" >&2
    exit 2
fi
target="$1"
ca="$2"
user="${FLIGHT_PROBE_USER:-ferrosa_admin}"
if [[ -z "${FLIGHT_PROBE_PASSWORD:-}" ]]; then
    echo "ERROR: FLIGHT_PROBE_PASSWORD is not set" >&2
    exit 2
fi
[[ -r "$ca" ]] || {
    echo "ERROR: CA file $ca is not readable" >&2
    exit 2
}

service="arrow.flight.protocol.FlightService"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
chmod 700 "$work"

log() { printf '[flight-probe] %s\n' "$*"; }

# gRPC frame: 1-byte compressed flag (0) + 4-byte big-endian length + message.
# HandshakeRequest { protocol_version = 1 (varint), payload = 2 (bytes) } with
# payload "user\0password". Criteria {} is the empty message.
FLIGHT_PROBE_USER="$user" python3 - "$work" <<'PY'
import os, struct, sys
work = sys.argv[1]
def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)
def frame(msg):
    return b"\x00" + struct.pack(">I", len(msg)) + msg
payload = os.environ["FLIGHT_PROBE_USER"].encode() + b"\x00" + os.environ["FLIGHT_PROBE_PASSWORD"].encode()
handshake = b"\x12" + varint(len(payload)) + payload
with open(os.path.join(work, "handshake.bin"), "wb") as f:
    f.write(frame(handshake))
with open(os.path.join(work, "empty.bin"), "wb") as f:
    f.write(frame(b""))
PY

# The last grpc-status in a curl -D dump. curl writes HTTP/2 trailers to -D
# after the response headers, so this is the trailer when there is one (or the
# header of a trailers-only response).
grpc_status() {
    tr -d '\r' <"$1" | awk -F': ' 'tolower($1) == "grpc-status" { print $2 }' | tail -1
}

# ── 1. plaintext must not be served ─────────────────────────────────────────
plain_rc=0
curl -sS --max-time 10 --http2-prior-knowledge -X POST \
    -H 'content-type: application/grpc' -H 'te: trailers' \
    --data-binary @"$work/empty.bin" -D "$work/plain.hdr" -o /dev/null \
    "http://${target}/${service}/ListActions" 2>"$work/plain.err" || plain_rc=$?
if [[ $plain_rc -eq 0 ]] || [[ -n "$(grpc_status "$work/plain.hdr")" ]]; then
    log "ERROR: a plaintext gRPC request to ${target} got a response (curl exit ${plain_rc})"
    exit 1
fi
log "plaintext gRPC refused (curl exit ${plain_rc}: $(tail -1 "$work/plain.err"))"

tls_call() { # <method> <body-file> <headers-out> [extra curl args...]
    local method="$1" body="$2" hdr="$3"
    shift 3
    curl -sS --max-time 15 --http2 --cacert "$ca" -X POST \
        -H 'content-type: application/grpc' -H 'te: trailers' "$@" \
        --data-binary @"$body" -D "$hdr" -o "$hdr.body" \
        "https://${target}/${service}/${method}"
}

# ── 2. unauthenticated call is answered by the gRPC service over TLS ────────
tls_call ListFlights "$work/empty.bin" "$work/unauth.hdr"
status="$(grpc_status "$work/unauth.hdr")"
if [[ "$status" != "16" ]]; then
    log "ERROR: unauthenticated ListFlights over TLS: grpc-status '${status}', expected 16"
    exit 1
fi
log "TLS gRPC reachable; unauthenticated ListFlights refused (grpc-status 16)"

# ── 3. Handshake over TLS → bearer token ────────────────────────────────────
tls_call Handshake "$work/handshake.bin" "$work/hs.hdr"
status="$(grpc_status "$work/hs.hdr")"
if [[ "$status" != "0" ]]; then
    log "ERROR: Handshake over TLS: grpc-status '${status}' ($(tr -d '\r' <"$work/hs.hdr" | grep -i '^grpc-message' || true))"
    exit 1
fi
python3 - "$work" <<'PY'
import os, struct, sys
work = sys.argv[1]
data = open(os.path.join(work, "hs.hdr.body"), "rb").read()
if len(data) < 5:
    sys.exit("Handshake response has no gRPC message")
(length,) = struct.unpack(">I", data[1:5])
msg = data[5:5 + length]
def varint(buf, i):
    shift = value = 0
    while True:
        b = buf[i]; i += 1
        value |= (b & 0x7F) << shift
        if not b & 0x80:
            return value, i
        shift += 7
i, token = 0, None
while i < len(msg):
    key, i = varint(msg, i)
    field, wire = key >> 3, key & 7
    if wire == 0:
        _, i = varint(msg, i)
    elif wire == 2:
        n, i = varint(msg, i)
        if field == 2:
            token = msg[i:i + n]
        i += n
    else:
        sys.exit(f"unexpected protobuf wire type {wire}")
if not token:
    sys.exit("Handshake response carried no token")
path = os.path.join(work, "auth.hdr")
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    f.write("authorization: Bearer " + token.decode() + "\n")
PY
log "Handshake over TLS issued a bearer token"

# ── 4. authenticated ListFlights over TLS ───────────────────────────────────
tls_call ListFlights "$work/empty.bin" "$work/list.hdr" -H @"$work/auth.hdr"
status="$(grpc_status "$work/list.hdr")"
if [[ "$status" != "0" ]]; then
    log "ERROR: authenticated ListFlights over TLS: grpc-status '${status}'"
    exit 1
fi
log "PASS: ${target} serves Arrow Flight over TLS only (Handshake + ListFlights OK, plaintext refused)"
