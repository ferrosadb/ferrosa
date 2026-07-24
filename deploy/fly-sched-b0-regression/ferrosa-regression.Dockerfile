# Image for the scheduler-B0 no-step-down regression. Builds the ferrosa node
# binary AND the ferrosa-loadgen driver (the loadgen carries the --scan-storm
# mode on the B0 branch).
#
# Deliberately a plain `COPY . . && cargo build` — NO per-crate manifest cache
# layer — so the SAME Dockerfile builds both refs: the post-fix (B0) tree that
# has `ferrosa-sched`, and the pre-fix (origin/main) tree that does not. A
# per-manifest COPY would fail on the ref that lacks the crate. Each arm builds
# once, so the lost incremental-cache speed does not matter.
FROM rust:1.94 AS builder
WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends capnproto \
    && rm -rf /var/lib/apt/lists/*

COPY . .
ENV CARGO_BUILD_JOBS=4
ENV RUSTFLAGS="-C force-frame-pointers=yes -C debuginfo=1"
RUN cargo build --release -p ferrosa -p ferrosa-loadgen

FROM debian:trixie-slim
# gdb: capture an all-thread backtrace when the diag sampler detects a >1s pause.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl procps gdb \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/ferrosa /usr/local/bin/
COPY --from=builder /build/target/release/ferrosa-loadgen /usr/local/bin/
COPY deploy/fly-sched-b0-regression/regression-entrypoint.sh /usr/local/bin/
COPY deploy/fly-sched-b0-regression/scrape.sh /usr/local/bin/
COPY deploy/fly-sched-b0-regression/diag.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/regression-entrypoint.sh /usr/local/bin/scrape.sh /usr/local/bin/diag.sh
EXPOSE 9042 17000 9090
ENV FERROSA_DATA_DIR=/var/lib/ferrosa
ENTRYPOINT ["regression-entrypoint.sh"]
CMD ["ferrosa"]
