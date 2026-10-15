FROM rust:1.94 AS builder
WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends capnproto \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY ferrosa/Cargo.toml ferrosa/Cargo.toml
COPY ferrosa-common/Cargo.toml ferrosa-common/Cargo.toml
COPY ferrosa-sstable/Cargo.toml ferrosa-sstable/Cargo.toml
COPY ferrosa-storage/Cargo.toml ferrosa-storage/Cargo.toml
COPY ferrosa-schema/Cargo.toml ferrosa-schema/Cargo.toml
COPY ferrosa-cql/Cargo.toml ferrosa-cql/Cargo.toml
COPY ferrosa-index/Cargo.toml ferrosa-index/Cargo.toml
COPY ferrosa-net/Cargo.toml ferrosa-net/Cargo.toml
COPY ferrosa-cluster/Cargo.toml ferrosa-cluster/Cargo.toml
COPY ferrosa-graph/Cargo.toml ferrosa-graph/Cargo.toml
COPY ferrosa-udf/Cargo.toml ferrosa-udf/Cargo.toml
COPY ferrosa-worker/Cargo.toml ferrosa-worker/Cargo.toml
COPY ferrosa-sparql/Cargo.toml ferrosa-sparql/Cargo.toml
COPY ferrosa-ctl/Cargo.toml ferrosa-ctl/Cargo.toml
COPY ferrosa-jepsen/Cargo.toml ferrosa-jepsen/Cargo.toml
COPY ferrosa-loadgen/Cargo.toml ferrosa-loadgen/Cargo.toml
COPY ferrosa-index-builder/Cargo.toml ferrosa-index-builder/Cargo.toml
COPY ferrosa-sim/Cargo.toml ferrosa-sim/Cargo.toml
COPY ferrosa-cdc/Cargo.toml ferrosa-cdc/Cargo.toml
COPY ferrosa-flight/Cargo.toml ferrosa-flight/Cargo.toml
COPY ferrosa-jsonb/Cargo.toml ferrosa-jsonb/Cargo.toml
COPY ferrosa-postgres/Cargo.toml ferrosa-postgres/Cargo.toml
COPY ferrosa-row-bridge/Cargo.toml ferrosa-row-bridge/Cargo.toml
COPY ferrosa-sched/Cargo.toml ferrosa-sched/Cargo.toml
COPY ferrosa-session/Cargo.toml ferrosa-session/Cargo.toml
COPY ferrosa-sql/Cargo.toml ferrosa-sql/Cargo.toml
COPY ferrosa-view/Cargo.toml ferrosa-view/Cargo.toml
COPY xtask/Cargo.toml xtask/Cargo.toml

RUN for d in ferrosa ferrosa-common ferrosa-sstable ferrosa-storage ferrosa-schema \
            ferrosa-cql ferrosa-index ferrosa-net ferrosa-cluster ferrosa-graph \
            ferrosa-udf ferrosa-worker ferrosa-sparql ferrosa-ctl ferrosa-jepsen \
            ferrosa-loadgen ferrosa-index-builder ferrosa-sim \
            ferrosa-cdc ferrosa-flight ferrosa-jsonb ferrosa-postgres \
            ferrosa-row-bridge ferrosa-sched ferrosa-session ferrosa-sql \
            ferrosa-view; do \
      mkdir -p "$d/src" && echo "" > "$d/src/lib.rs"; \
    done && \
    mkdir -p xtask/src && echo 'fn main() {}' > xtask/src/main.rs && \
    echo 'fn main() {}' > ferrosa/src/main.rs && \
    echo 'fn main() {}' > ferrosa-ctl/src/main.rs && \
    echo 'fn main() {}' > ferrosa-jepsen/src/main.rs && \
    echo 'fn main() {}' > ferrosa-loadgen/src/main.rs && \
    echo 'fn main() {}' > ferrosa-index-builder/src/main.rs && \
    echo 'fn main() {}' > ferrosa-worker/src/main.rs

ENV CARGO_BUILD_JOBS=4
ENV RUSTFLAGS="-C force-frame-pointers=yes -C debuginfo=1"
# Dependency-cache layer. Same feature set as the real build below, or the cache
# would miss and be discarded. Best-effort by design (|| true).
RUN cargo build --release -p ferrosa --features ferrosa/full 2>&1 || true

COPY . .
RUN find . -name "lib.rs" -o -name "main.rs" | xargs touch
# Build ferrosa-loadgen too — its --scan-storm mode drives the concurrent
# full-table ALLOW FILTERING scans for the reads-under-load / ramp experiment.
#
# --features ferrosa/full (otel + flight + ferrosa-cql/asc-udf) because this is
# the set the published image is built with, and a bench binary that omits part
# of the shipped surface measures a product nobody runs. Arrow Flight in
# particular is gated at COMPILE time -- main.rs resolves FERROSA_FLIGHT_ENABLED
# only under #[cfg(feature = "flight")] and reports flight_enabled = None
# otherwise, so the env var is silently a no-op unless the feature is built in.
# Telemetry stays off at runtime regardless: it needs an explicit
# FERROSA_TELEMETRY_ENABLED=true, so compiling otel in has no side effect.
RUN cargo build --release -p ferrosa -p ferrosa-loadgen --features ferrosa/full

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gdb \
        linux-perf \
        procps \
        sysstat \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/ferrosa /usr/local/bin/
COPY --from=builder /build/target/release/ferrosa-loadgen /usr/local/bin/
COPY deploy/fly-bench/ferrosa-entrypoint.sh /usr/local/bin/
EXPOSE 9042 7000 9090
ENV FERROSA_DATA_DIR=/var/lib/ferrosa
ENTRYPOINT ["ferrosa-entrypoint.sh"]
CMD ["ferrosa"]
