FROM rust:1.94 AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p ferrosa

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/ferrosa /usr/local/bin/
EXPOSE 9042 7000 9090
ENV FERROSA_DATA_DIR=/var/lib/ferrosa
CMD ["ferrosa"]
