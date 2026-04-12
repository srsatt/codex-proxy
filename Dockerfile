ARG RUST_VERSION=1.94
FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /src

COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl openssl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system codex-proxy \
    && useradd --system --gid codex-proxy --home-dir /app --create-home --shell /usr/sbin/nologin codex-proxy

WORKDIR /app

COPY --from=builder /src/target/release/codex-proxy /usr/local/bin/codex-proxy
COPY --chown=codex-proxy:codex-proxy config.docker.toml /app/config.toml

RUN mkdir -p /app/data \
    && chown -R codex-proxy:codex-proxy /app

USER codex-proxy:codex-proxy

EXPOSE 8317
HEALTHCHECK --interval=5s --timeout=3s --start-period=2s --retries=10 \
  CMD curl --fail --silent http://127.0.0.1:8317/healthz >/dev/null || exit 1

ENTRYPOINT ["codex-proxy"]
CMD ["start", "--bind", "0.0.0.0", "--config-path", "/app/config.toml", "--data-dir", "/app/data"]
