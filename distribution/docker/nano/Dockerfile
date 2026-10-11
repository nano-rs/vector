# syntax=docker/dockerfile:1.7
#
# nano-rs/vector image: a replacement for `timberio/vector:<version>-debian` built from this
# fork's source with ONLY the components nano ships or renders (distribution/docker/nano/FEATURES,
# derived by scan-components.py; the image's `vector list` equals COMPONENTS). Same base, layout,
# entrypoint and /etc/vector as upstream's image.
#
# Build from the repository root (see REBASE.md, "Cutting an image"):
#
#   docker build -f distribution/docker/nano/Dockerfile -t nano-vector:0.59.0-nano.1-debian .
#
# Differences from the upstream image: the binary is built here instead of from the release
# .deb, and release LTO defaults to thin with 16 codegen units (override with LTO=fat
# CODEGEN_UNITS=1 for an upstream-identical profile).
#
# Platforms: linux/amd64 and linux/arm64, built natively (no QEMU, no cross). FEATURES defaults
# to the FEATURES file (same set on both architectures); pass e.g.
# `--build-arg FEATURES=target-x86_64-unknown-linux-gnu` for upstream's full release set.

ARG RUST_VERSION=1.96.1
ARG DEBIAN_DIGEST=sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

FROM docker.io/rust:${RUST_VERSION}-trixie AS builder

# cmake builds librdkafka from source for the kafka source (`rdkafka?/cmake_build` in FEATURES);
# SASL PLAIN/SCRAM need only OpenSSL. libsasl2-dev is installed but GSSAPI is not enabled.
COPY scripts/environment/install-debian-build-deps.sh scripts/environment/install-protoc.sh /tmp/
RUN bash /tmp/install-debian-build-deps.sh && bash /tmp/install-protoc.sh /usr/local/bin

# Empty = the lines of distribution/docker/nano/FEATURES (comments stripped), comma-joined.
ARG FEATURES=
ARG TARGETARCH
ARG LTO=thin
ARG CODEGEN_UNITS=16
ARG CARGO_BUILD_JOBS=4
# Shown in `vector --version` after the target triple (upstream puts its git sha and date there).
# The version itself stays the plain crate version: see REBASE.md, "Invariants".
ARG VECTOR_BUILD_DESC=

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
    set -eu; \
    [ -n "${VECTOR_BUILD_DESC}" ] || unset VECTOR_BUILD_DESC; \
    case "${TARGETARCH:-amd64}" in \
      amd64) triple=x86_64-unknown-linux-gnu ;; \
      arm64) triple=aarch64-unknown-linux-gnu ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    if [ -z "${FEATURES}" ]; then \
      features="$(sed -e 's/#.*//' -e 's/[[:space:]]//g' -e '/^$/d' distribution/docker/nano/FEATURES | paste -sd, -)"; \
    else features="${FEATURES}"; fi; \
    [ -n "${features}" ] || { echo "empty feature list" >&2; exit 1; }; \
    echo "building vector for ${triple} with features: ${features}"; \
    cargo build --release --locked --no-default-features --features "${features}" --bin vector \
    && install -m 0755 target/release/vector /usr/bin/vector \
    && mkdir -p /var/lib/vector

FROM docker.io/debian:trixie-slim@${DEBIAN_DIGEST}

LABEL org.opencontainers.image.url="https://vector.dev"
LABEL org.opencontainers.image.source="https://github.com/nano-rs/vector"
LABEL org.opencontainers.image.documentation="https://vector.dev/docs"
LABEL org.opencontainers.image.description="Vector, nano-rs fork: nano's component set (see distribution/docker/nano/COMPONENTS)"

# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata systemd libsasl2-2 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/bin/vector /usr/bin/vector
COPY --from=builder /var/lib/vector /var/lib/vector

# /etc/vector matches the upstream image exactly: only the package's examples/, no default
# config. nano's entrypoints run `--config-dir /etc/vector`, so any file here would be loaded.
# (Dockerfile.dockerignore next to this file admits config/examples into the build context.)
COPY config/examples/*.yaml /etc/vector/examples/
RUN chmod 0755 /etc/vector /etc/vector/examples && chmod 0644 /etc/vector/examples/*.yaml

# Smoke test
RUN ["vector", "--version"]
RUN vector list | grep -qx -- '- azure_event_hubs' && vector list | grep -qx -- '- kafka'

ENTRYPOINT ["/usr/bin/vector"]
