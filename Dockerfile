FROM rust:1.94 AS builder
WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends capnproto \
    && rm -rf /var/lib/apt/lists/*

# Layer 1: cache dependencies (only re-runs when Cargo.toml/lock change)
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

# Create stub lib.rs for each crate so cargo fetch + dep build works
RUN for d in ferrosa ferrosa-common ferrosa-sstable ferrosa-storage ferrosa-schema \
            ferrosa-cql ferrosa-index ferrosa-net ferrosa-cluster ferrosa-graph \
            ferrosa-udf ferrosa-worker ferrosa-sparql ferrosa-ctl ferrosa-jepsen \
            ferrosa-loadgen ferrosa-index-builder ferrosa-sim; do \
      mkdir -p "$d/src" && echo "" > "$d/src/lib.rs"; \
    done && \
    echo 'fn main() {}' > ferrosa/src/main.rs && \
    echo 'fn main() {}' > ferrosa-ctl/src/main.rs && \
    echo 'fn main() {}' > ferrosa-jepsen/src/main.rs && \
    echo 'fn main() {}' > ferrosa-loadgen/src/main.rs && \
    echo 'fn main() {}' > ferrosa-index-builder/src/main.rs && \
    echo 'fn main() {}' > ferrosa-worker/src/main.rs

# Limit parallelism aggressively: release rustc/LLVM builds are memory-heavy, and
# the local cluster image is commonly built inside an 8GB Podman VM. Exit 137
# during image build is a P0 because it blocks cluster validation; reliability
# matters more than build speed here.
ENV CARGO_BUILD_JOBS=1 \
    CARGO_INCREMENTAL=0

RUN cargo build --release -p ferrosa 2>&1 || true

# Layer 2: copy real source and build (only recompiles changed crates)
COPY . .
# Touch all lib.rs/main.rs so cargo sees them as newer than the stub artifacts
RUN find . -name "lib.rs" -o -name "main.rs" | xargs touch

# --------------------------------------------------------------------------
# Profile-guided optimization (opt-in).
#
#   docker build --build-arg PGO=1 ...
#
# The profile is collected here, in the builder, by running an instrumented
# build of the training workload (scripts/pgo-build.sh). That is only sound
# because the workload needs no database and no network — it drives the real
# StorageEngine in-process. Training the CQL/RPC/TLS paths is a different,
# release-time pass against a live cluster; see specs/pgo-release-build.md.
#
# Why PGO is off by default:
#   * it roughly doubles build time (instrumented build + optimized build);
#   * the workload costs real wall-clock in the builder;
#   * a profile trained on a hermetic workload is the wrong input for the
#     I/O-bound and network paths, so it should be a deliberate choice.
#
# Why the PGO path does NOT set LLVM_PROFILE_FILE: an instrumented process that
# is killed instead of exiting cleanly writes either nothing or a zero-count
# profraw. A zero-count profile is accepted by the compiler while optimizing
# nothing, so scripts/pgo-build.sh asserts the merged profile has non-zero
# counts and fails the build otherwise.
#
# With PGO=0 the command is the plain release build, exactly as before.
# --------------------------------------------------------------------------
ARG PGO=0
RUN if [ "$PGO" = "1" ]; then \
      bash scripts/pgo-build.sh --baseline; \
    else \
      cargo build --release -p ferrosa; \
    fi
# Fail here if the artifact is missing, rather than three layers later in COPY.
RUN test -x target/release/ferrosa

FROM downloads.ferrosa.ai/debian@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a
# gdb + procps available in the runtime image so crashes produce readable backtraces
# (paired with `[profile.release] debug = "line-tables-only"` in the workspace Cargo.toml).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        gdb \
        procps \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/ferrosa /usr/local/bin/
EXPOSE 9042 7000 9090
ENV FERROSA_DATA_DIR=/var/lib/ferrosa
CMD ["ferrosa"]
