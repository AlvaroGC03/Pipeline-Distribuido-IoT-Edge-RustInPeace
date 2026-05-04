# ── Stage 1: Builder ──────────────────────────────────────────────────────
FROM rust:1.78-slim AS builder

WORKDIR /app

COPY rust/ ./rust/

WORKDIR /app/rust
RUN cargo build --release -p edge

# ── Stage 2: Runtime ──────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

COPY --from=builder /app/rust/target/release/edge /usr/local/bin/edge

ENV EDGE_ID=edge-01
ENV LISTEN_PORT=9000
ENV COORDINATOR_ADDR=10.10.10.1:9100
ENV THRESHOLD=35.0
ENV HEARTBEAT_INTERVAL_MS=3000

EXPOSE 9000

CMD ["/usr/local/bin/edge"]