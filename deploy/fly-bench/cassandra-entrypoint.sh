#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${FLY_PRIVATE_IP:-}" ]]; then
  export CASSANDRA_LISTEN_ADDRESS="${CASSANDRA_LISTEN_ADDRESS:-${FLY_PRIVATE_IP}}"
  export CASSANDRA_BROADCAST_ADDRESS="${CASSANDRA_BROADCAST_ADDRESS:-${FLY_PRIVATE_IP}}"
  export CASSANDRA_RPC_ADDRESS="${CASSANDRA_RPC_ADDRESS:-0.0.0.0}"
  export CASSANDRA_BROADCAST_RPC_ADDRESS="${CASSANDRA_BROADCAST_RPC_ADDRESS:-${FLY_PRIVATE_IP}}"
fi

export MAX_HEAP_SIZE="${MAX_HEAP_SIZE:-4G}"
export HEAP_NEWSIZE="${HEAP_NEWSIZE:-800M}"
sed -i '/^-Djava\.net\.preferIPv4Stack=true$/d' /etc/cassandra/jvm-server.options
export JVM_EXTRA_OPTS="${JVM_EXTRA_OPTS:-} -Djava.net.preferIPv4Stack=false -Djava.net.preferIPv6Addresses=true"

exec /usr/local/bin/docker-entrypoint.sh cassandra -f
