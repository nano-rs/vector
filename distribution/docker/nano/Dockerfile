# syntax=docker/dockerfile:1.7
#
# nano-rs/vector image: a drop-in replacement for `timberio/vector:<version>-debian`, built from
# this fork's source with the upstream release feature set plus the fork's extra components
# (they are part of `sources-logs`, so the release feature set already includes them).
#
# Build from the repository root (see REBASE.md, "Cutting an image"):
#
#   docker build -f distribution/docker/nano/Dockerfile -t nano-vector:0.59.0-nano.1-debian .
#
# Differences from the upstream image: the binary is built here instead of from the release
# .deb, and release LTO defaults to thin with 16 codegen units (override with LTO=fat
# CODEGEN_UNITS=1 for an upstream-identical profile).

ARG RUST_VERSION=1.96.1
ARG DEBIAN_DIGEST=sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

FROM docker.io/rust:${RUST_VERSION}-trixie AS builder

COPY scripts/environment/install-debian-build-deps.sh scripts/environment/install-protoc.sh /tmp/
RUN bash /tmp/install-debian-build-deps.sh && bash /tmp/install-protoc.sh /usr/local/bin

ARG FEATURES=target-x86_64-unknown-linux-gnu
ARG LTO=thin
ARG CODEGEN_UNITS=16
ARG CARGO_BUILD_JOBS=4

# Mirrors the [env] section of .cargo/config.toml, which .dockerignore keeps out of the context.
ENV JEMALLOC_SYS_WITH_LG_PAGE=16 \
    SOURCE_DATE_EPOCH=0 \
    CARGO_PROFILE_RELEASE_LTO=${LTO} \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=${CODEGEN_UNITS} \
    CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

WORKDIR /vector
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/vector/target \
    cargo build --release --locked --no-default-features --features "${FEATURES}" --bin vector \
    && install -m 0755 target/release/vector /usr/bin/vector \
    && mkdir -p /var/lib/vector /etc/vector

FROM docker.io/debian:trixie-slim@${DEBIAN_DIGEST}

LABEL org.opencontainers.image.url="https://vector.dev"
LABEL org.opencontainers.image.source="https://github.com/nano-rs/vector"
LABEL org.opencontainers.image.documentation="https://vector.dev/docs"
LABEL org.opencontainers.image.description="Vector, nano-rs fork (adds the azure_event_hubs source)"

# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata systemd libsasl2-2 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/bin/vector /usr/bin/vector
COPY --from=builder /var/lib/vector /var/lib/vector
COPY --from=builder /etc/vector /etc/vector

# Same minimal default as the upstream package's /etc/vector/vector.yaml: a demo source to console.
RUN cat > /etc/vector/vector.yaml <<'YAML'
sources:
  dummy_logs:
    type: demo_logs
    format: syslog
    interval: 1
sinks:
  print:
    type: console
    inputs: [dummy_logs]
    encoding:
      codec: json
YAML

# Smoke test
RUN ["vector", "--version"]
RUN vector list | grep -qx 'azure_event_hubs'

ENTRYPOINT ["/usr/bin/vector"]
