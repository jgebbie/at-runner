# Retrieve pre-compiled binaries from the AT repository registry (or a locally-built image).
ARG AT_IMAGE=ghcr.io/jgebbie/at:latest
FROM ${AT_IMAGE} AS at-binaries

# Stage 2: Build Rust gRPC service
FROM rust:bookworm AS rust-builder
RUN apt-get update && apt-get install -y protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock /build/
COPY proto/ /build/proto/
COPY service/ /build/service/
COPY client/rust/ /build/client/rust/
COPY testing/rust/ /build/testing/rust/
WORKDIR /build
RUN cargo build --release -p at-runner

# Stage 3: Minimal runtime image
FROM debian:bookworm-slim
ARG GRPC_HEALTH_PROBE_VERSION=0.4.24
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates libgfortran5 \
    && ARCH="${TARGETARCH:-amd64}" \
    && wget -qO/usr/local/bin/grpc_health_probe \
    "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${ARCH}" \
    && chmod +x /usr/local/bin/grpc_health_probe \
    && apt-get purge -y wget \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /at/bin /workspace
COPY --from=at-binaries /at/bin/ /at/bin/
COPY --from=rust-builder /build/target/release/at-runner /usr/local/bin/
EXPOSE 50051
HEALTHCHECK --interval=5s --timeout=2s --retries=3 \
    CMD ["grpc_health_probe", "-addr=:50051"]
CMD ["at-runner", "--bin-dir", "/at/bin", "--workspace", "/workspace", "--port", "50051"]
