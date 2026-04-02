#!/usr/bin/env bash
# Manage the Ferrosa test cluster.
#
# Usage:
#   ./scripts/cluster.sh up              # 3-node cluster (default)
#   ./scripts/cluster.sh up quint        # 5-node cluster
#   ./scripts/cluster.sh down            # tear down
#   ./scripts/cluster.sh status          # show container status
#   ./scripts/cluster.sh logs [node]     # tail logs (all nodes or specific)

set -euo pipefail

COMPOSE_FILE="tests/docker-compose.cluster.yml"
CMD="${1:-up}"
ARG="${2:-trio}"

case "$CMD" in
    down)
        echo "Tearing down cluster..."
        podman compose -f "$COMPOSE_FILE" --profile trio --profile quint down -v --remove-orphans
        # Clean up any leftover networks
        podman network prune -f 2>/dev/null || true
        ;;

    status)
        podman compose -f "$COMPOSE_FILE" ps
        ;;

    logs)
        if [ -n "${2:-}" ]; then
            podman compose -f "$COMPOSE_FILE" logs -f "$2"
        else
            podman compose -f "$COMPOSE_FILE" logs -f
        fi
        ;;

    up)
        PROFILE="$ARG"
        if [ "$PROFILE" != "trio" ] && [ "$PROFILE" != "quint" ]; then
            echo "Usage: $0 up [trio|quint]"
            exit 1
        fi

        echo "Starting Ferrosa ${PROFILE} cluster with RustFS..."
        podman compose -f "$COMPOSE_FILE" --profile "$PROFILE" up -d --build

        echo ""
        echo "Waiting for RustFS health..."
        until curl -sf http://127.0.0.1:9000/health > /dev/null 2>&1; do
            sleep 1
        done
        echo "RustFS ready."

        echo ""
        echo "Waiting for CQL on node1 (port 9042)..."
        until nc -z 127.0.0.1 9042 2>/dev/null; do
            sleep 1
        done
        echo "Node1 CQL ready."

        echo ""
        echo "Cluster endpoints:"
        echo "  CQL:     127.0.0.1:9042 (node1), :9043 (node2), :9044 (node3)"
        echo "  Web:     http://127.0.0.1:9090 (node1)"
        echo "  RustFS:  http://127.0.0.1:9000 (S3), http://127.0.0.1:9001 (console)"
        if [ "$PROFILE" = "quint" ]; then
            echo "           :9045 (node4), :9046 (node5)"
        fi
        echo ""
        echo "Load test:"
        echo "  ./target/debug/ferrosa-loadgen --node 127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044 --profile balanced --duration 240 --tui"
        echo ""
        echo "Tear down:"
        echo "  ./scripts/cluster.sh down"
        ;;

    *)
        echo "Usage: $0 {up|down|status|logs} [args]"
        exit 1
        ;;
esac
