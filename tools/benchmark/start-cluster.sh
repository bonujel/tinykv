#!/usr/bin/env bash
# Start a 3-node TinyKV cluster for benchmarking.
#
# Environment variables:
#   BINDIR  — directory containing tinykv-server and tinyscheduler-server binaries
#             (default: ../../bin)
#   DATADIR — root directory for data files (default: /tmp/tinykv-bench)
#
# Usage:
#   bash start-cluster.sh          # start cluster
#   bash start-cluster.sh stop     # stop cluster

set -euo pipefail

BINDIR="${BINDIR:-../../bin}"
DATADIR="${DATADIR:-/tmp/tinykv-bench}"

PD_PORT=2379
PD_PEER_PORT=2380
STORE_PORTS=(20160 20161 20162)

PD_ADDR="127.0.0.1:${PD_PORT}"
PD_PEER_ADDR="127.0.0.1:${PD_PEER_PORT}"

stop_cluster() {
    echo "Stopping cluster..."
    pkill -f "tinyscheduler-server" 2>/dev/null || true
    pkill -f "tinykv-server" 2>/dev/null || true
    echo "Cluster stopped."
}

if [[ "${1:-}" == "stop" ]]; then
    stop_cluster
    exit 0
fi

# Clean previous data
stop_cluster
rm -rf "${DATADIR}"

echo "=== Starting TinyKV 3-node cluster ==="
echo "  BINDIR  : ${BINDIR}"
echo "  DATADIR : ${DATADIR}"
echo ""

# Start scheduler (PD)
mkdir -p "${DATADIR}/pd"
mkdir -p "${DATADIR}/pd-logs"
echo "Starting scheduler on ${PD_ADDR}..."
"${BINDIR}/tinyscheduler-server" \
    --client-urls="http://${PD_ADDR}" \
    --peer-urls="http://${PD_PEER_ADDR}" \
    --data-dir="${DATADIR}/pd" \
    --log-file="${DATADIR}/pd-logs/pd.log" &
sleep 2

# Start 3 TinyKV stores
for i in "${!STORE_PORTS[@]}"; do
    port="${STORE_PORTS[$i]}"
    store_dir="${DATADIR}/store${i}"
    mkdir -p "${store_dir}"
    echo "Starting store ${i} on 127.0.0.1:${port}..."
    "${BINDIR}/tinykv-server" \
        --addr="127.0.0.1:${port}" \
        --scheduler="${PD_ADDR}" \
        --path="${store_dir}" &
    sleep 1
done

echo ""
echo "=== Cluster is running ==="
echo "  PD       : ${PD_ADDR}"
for i in "${!STORE_PORTS[@]}"; do
    echo "  Store ${i}  : 127.0.0.1:${STORE_PORTS[$i]}"
done
echo ""
echo "To stop:  bash $0 stop"
echo "Benchmark: ./benchmark -pd=${PD_ADDR} -topology"
