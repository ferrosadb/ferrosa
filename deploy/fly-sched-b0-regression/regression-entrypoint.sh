#!/bin/sh
# Entrypoint for the scheduler-B0 regression node image.
#
# Unlike the O_DIRECT baseline entrypoint, this does NOT establish a cgroup
# memory cap: the regression's pressure comes from the shared-CPU throttle (the
# VM size), not from bounded page cache. It only maps fly's private IP + any S3
# env into the FERROSA_* names ferrosa expects, then execs the target command.
set -eu

# Broadcast on the fly private IPv6 so peers learn a reachable address.
if [ -n "${FLY_PRIVATE_IP:-}" ]; then
  export FERROSA_INTERNODE_BROADCAST="${FERROSA_INTERNODE_BROADCAST:-[${FLY_PRIVATE_IP}]:17000}"
  export FERROSA_CQL_BROADCAST="${FERROSA_CQL_BROADCAST:-[${FLY_PRIVATE_IP}]:9042}"
fi

# Optional S3 mapping (mirrors the other fly deploy entrypoints). The regression
# does not require S3 — a node with local-only storage still exhibits the scan /
# heartbeat contention we are measuring — but honor it if provided.
[ -n "${AWS_ENDPOINT_URL_S3:-}" ]   && export FERROSA_S3_ENDPOINT="${FERROSA_S3_ENDPOINT:-${AWS_ENDPOINT_URL_S3}}"
[ -n "${BUCKET_NAME:-}" ]           && export FERROSA_S3_BUCKET="${FERROSA_S3_BUCKET:-${BUCKET_NAME}}"
[ -n "${AWS_REGION:-}" ]            && export FERROSA_S3_REGION="${FERROSA_S3_REGION:-${AWS_REGION}}"
[ -n "${AWS_ACCESS_KEY_ID:-}" ]     && export FERROSA_S3_ACCESS_KEY_ID="${FERROSA_S3_ACCESS_KEY_ID:-${AWS_ACCESS_KEY_ID}}"
[ -n "${AWS_SECRET_ACCESS_KEY:-}" ] && export FERROSA_S3_SECRET_ACCESS_KEY="${FERROSA_S3_SECRET_ACCESS_KEY:-${AWS_SECRET_ACCESS_KEY}}"

exec "$@"
