#!/usr/bin/env bash
set -euo pipefail

: "${TARGET_NAME:?TARGET_NAME is required, for example ferrosa-2g or cassandra-8g}"
: "${CONTACT_POINTS:?CONTACT_POINTS is required, comma-separated host:port list}"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
WORKLOAD="${WORKLOAD:-activities/baselines/cql_iot.yaml}"
SCENARIO="${SCENARIO:-default}"
LOCAL_DC="${LOCAL_DC:-datacenter1}"
THREADS="${THREADS:-256}"
WARMUP_CYCLES="${WARMUP_CYCLES:-5000000}"
MEASURE_CYCLES="${MEASURE_CYCLES:-50000000}"
REPEATS="${REPEATS:-5}"
RF="${RF:-3}"
READ_CL="${READ_CL:-LOCAL_QUORUM}"
WRITE_CL="${WRITE_CL:-LOCAL_QUORUM}"
NB_JAVA_MAX_HEAP="${NB_JAVA_MAX_HEAP:-24g}"
REQUEST_TIMEOUT_SECONDS="${REQUEST_TIMEOUT_SECONDS:-30}"
CQL_PROTOCOL_COMPRESSION="${CQL_PROTOCOL_COMPRESSION:-lz4}"
EXTRA_NB_ARGS="${EXTRA_NB_ARGS:-}"

if [[ -n "$NB_JAVA_MAX_HEAP" ]]; then
  export JDK_JAVA_OPTIONS="${JDK_JAVA_OPTIONS:-} -Xmx${NB_JAVA_MAX_HEAP}"
fi

OUT_DIR="/results/${RUN_ID}/${TARGET_NAME}"
mkdir -p "$OUT_DIR"

cat > "${OUT_DIR}/run.env" <<EOF
RUN_ID=${RUN_ID}
TARGET_NAME=${TARGET_NAME}
CONTACT_POINTS=${CONTACT_POINTS}
WORKLOAD=${WORKLOAD}
SCENARIO=${SCENARIO}
LOCAL_DC=${LOCAL_DC}
THREADS=${THREADS}
WARMUP_CYCLES=${WARMUP_CYCLES}
MEASURE_CYCLES=${MEASURE_CYCLES}
REPEATS=${REPEATS}
RF=${RF}
READ_CL=${READ_CL}
WRITE_CL=${WRITE_CL}
NB_JAVA_MAX_HEAP=${NB_JAVA_MAX_HEAP}
REQUEST_TIMEOUT_SECONDS=${REQUEST_TIMEOUT_SECONDS}
CQL_PROTOCOL_COMPRESSION=${CQL_PROTOCOL_COMPRESSION}
EXTRA_NB_ARGS=${EXTRA_NB_ARGS}
EOF

run_nb() {
  local repeat="$1"
  local logfile="$2"
  shift 2

  # Capture nosqlbench's own latency metrics, not just /usr/bin/time -v:
  #   --report-csv-to  → per-metric CSV incl. HdrHistogram percentiles
  #                      (result-success.csv: count,max,mean,...,p95,p98,p99,p999)
  #   --report-summary-to → the human-readable end-of-run metrics table
  # These land under OUT_DIR so they are included in the result tarball; the A/B
  # comparison parses result-success.csv for p95/p99/p100(=max).
  local csv_dir="${OUT_DIR}/metrics-${repeat}"
  /usr/bin/time -v \
    nb5 "$WORKLOAD" "$SCENARIO" \
      "hosts=${CONTACT_POINTS}" \
      "localdc=${LOCAL_DC}" \
      "threads=${THREADS}" \
      "rf=${RF}" \
      "read_cl=${READ_CL}" \
      "write_cl=${WRITE_CL}" \
      "driver.basic.request.timeout=${REQUEST_TIMEOUT_SECONDS} seconds" \
      "driver.advanced.protocol.compression=${CQL_PROTOCOL_COMPRESSION}" \
      "request_timeout_seconds=${REQUEST_TIMEOUT_SECONDS}.0" \
      "rampup-cycles=${WARMUP_CYCLES}" \
      "main-cycles=${MEASURE_CYCLES}" \
      --report-csv-to "${csv_dir}" \
      $EXTRA_NB_ARGS "$@" \
    >"$logfile" 2>&1
}

date -u +"%FT%TZ" > "${OUT_DIR}/started_at.txt"
for i in $(seq 1 "$REPEATS"); do
  run_nb "$i" "${OUT_DIR}/measure-${i}.log"
done

date -u +"%FT%TZ" > "${OUT_DIR}/finished_at.txt"
tar -C "/results/${RUN_ID}" -czf "/results/${RUN_ID}-${TARGET_NAME}.tgz" "$TARGET_NAME"
echo "/results/${RUN_ID}-${TARGET_NAME}.tgz"
