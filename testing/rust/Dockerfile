FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock /build/
COPY proto/ /build/proto/
COPY service/ /build/service/
COPY client/rust/ /build/client/rust/
COPY testing/rust/ /build/testing/rust/

WORKDIR /build
RUN cargo build --release -p at-test-driver

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/at-test-driver /usr/local/bin/
CMD ["at-test-driver"]
