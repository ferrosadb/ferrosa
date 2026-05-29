FROM rust:1.94-slim AS builder
WORKDIR /tests
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /tests
COPY --from=builder /tests/target/release/ferrosa-rust-driver-test .
CMD ["./ferrosa-rust-driver-test"]
