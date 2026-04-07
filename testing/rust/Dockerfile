FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY proto/ /build/proto/
COPY client/rust/ /build/client/rust/

WORKDIR /build
COPY testing/rust/Cargo.toml /build/testing/rust/Cargo.toml
COPY testing/rust/src/ /build/testing/rust/src/

WORKDIR /build/testing/rust
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /build/testing/rust/target/release/at-test-driver /usr/local/bin/
CMD ["at-test-driver"]
