# ── Stage 1: Builder ──────────────────────────────────────────────────────
FROM rust:1.78-slim AS builder

WORKDIR /app

COPY rust/ ./rust/

WORKDIR /app/rust
RUN cargo build --release -p coordinator

# ── Stage 2: Runtime ──────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

COPY --from=builder /app/rust/target/release/coordinator /usr/local/bin/coordinator

ENV TCP_PORT=9100
ENV HTTP_PORT=8080

EXPOSE 9100
EXPOSE 8080

CMD ["/usr/local/bin/coordinator"]