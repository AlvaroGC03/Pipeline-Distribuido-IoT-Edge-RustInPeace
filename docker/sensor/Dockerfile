# ── Stage 1: Builder ──────────────────────────────────────────────────────
FROM rust:1.78-slim AS builder

WORKDIR /app

# Copiar workspace completo
COPY rust/ ./rust/

# Compilar solo el binario sensor en modo release
WORKDIR /app/rust
RUN cargo build --release -p sensor

# ── Stage 2: Runtime ──────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# Copiar solo el binario compilado desde el builder
COPY --from=builder /app/rust/target/release/sensor /usr/local/bin/sensor

# Variables de entorno con valores por defecto para Docker
ENV SENSOR_ID=sensor-01
ENV EDGE_ADDR=edge:9000

CMD ["/usr/local/bin/sensor"]