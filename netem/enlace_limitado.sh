#!/usr/bin/env bash
# =============================================================================
# enlace_limitado.sh — Escenario 4: Enlace limitado
# =============================================================================
# Propósito: Simula un enlace de baja capacidad, típico de gateways IoT con
#            conectividad celular (2G/EDGE/NB-IoT) o radio LP-WAN.
#
# Parámetros tc netem aplicados:
#   rate 512kbit delay 50ms
#
#   - rate 512kbit → limita el throughput a 512 Kbps (≈ 64 KB/s)
#                    equivalente a un enlace 2G/EDGE real
#   - delay 50ms   → latencia base adicional (sin jitter en este escenario)
#
# Uso:
#   sudo ./enlace_limitado.sh
#   sudo ./enlace_limitado.sh --iface-docker br-XXXXXXXX
#
# Para revertir: sudo ./baseline.sh
# =============================================================================

set -euo pipefail

# ── Colores ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# ── Interfaces objetivo ───────────────────────────────────────────────────────
IFACE_DOCKER="${IFACE_DOCKER:-br-6e0e088d9cfe}"
IFACE_VPN="${IFACE_VPN:-wg0}"

# ── Parámetros netem de este escenario ───────────────────────────────────────
RATE="512kbit"
DELAY="50ms"
# Parámetros tbf (usados en el método alternativo)
TBF_BURST="32kbit"
TBF_LATENCY="400ms"

# ── Parseo de argumentos ──────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --iface-docker) IFACE_DOCKER="$2"; shift 2 ;;
    --iface-vpn)    IFACE_VPN="$2";    shift 2 ;;
    *) echo "Uso: $0 [--iface-docker IFACE] [--iface-vpn IFACE]"; exit 1 ;;
  esac
done

# ── Verificar privilegios ─────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
  echo -e "${RED}[ERROR]${NC} Este script requiere privilegios root."
  echo "       Ejecuta: sudo $0"
  exit 1
fi

# ── Función: aplicar netem con rate + delay ────────────────────────────────
apply_netem() {
  local iface="$1"
  local label="$2"

  echo -e "${CYAN}[NETEM]${NC} Aplicando en ${YELLOW}${iface}${NC} (${label})..."

  if ! ip link show "$iface" &>/dev/null; then
    echo -e "  ${YELLOW}[WARN]${NC} Interfaz ${iface} no encontrada — omitiendo."
    return 0
  fi

  # Limpiar regla previa
  tc qdisc del dev "$iface" root 2>/dev/null || true

  # Método principal: netem con rate nativo (kernel >= 3.3, Ubuntu 24.04 OK)
  if tc qdisc add dev "$iface" root netem \
       delay "$DELAY" rate "$RATE" 2>/dev/null; then
    echo -e "  ${GREEN}[OK]${NC} rate=${RATE} delay=${DELAY} (método netem directo)"

  else
    # Método alternativo: netem (delay) + tbf (rate limiting en cadena)
    echo -e "  ${YELLOW}[FALLBACK]${NC} netem rate no disponible — usando netem+tbf encadenado"

    # Paso 1: qdisc raíz con netem para el delay
    tc qdisc add dev "$iface" root handle 1: netem delay "$DELAY"

    # Paso 2: tbf como hijo del netem para el rate limiting
    # burst: tamaño del token bucket (afecta ráfagas cortas)
    # latency: tiempo máximo que un paquete puede esperar en la cola tbf
    tc qdisc add dev "$iface" parent 1:1 handle 10: tbf \
      rate "$RATE" burst "$TBF_BURST" latency "$TBF_LATENCY"

    echo -e "  ${GREEN}[OK]${NC} delay=${DELAY} via netem | rate=${RATE} via tbf"
  fi
}

# ── Ejecución ─────────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  ESCENARIO 4 — Enlace limitado                   ${NC}"
echo -e "${CYAN}  rate ${RATE}  delay ${DELAY}                       ${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo ""

apply_netem "$IFACE_DOCKER" "Plano 1: Docker IoT bridge (sensor→edge)"
echo ""
apply_netem "$IFACE_VPN"    "Plano 2: WireGuard VPN (edge→coordinator)"
echo ""

# ── Verificación ──────────────────────────────────────────────────────────────
echo -e "${CYAN}[VERIFICACIÓN]${NC} Reglas activas:"
echo ""
for iface in "$IFACE_DOCKER" "$IFACE_VPN"; do
  if ip link show "$iface" &>/dev/null; then
    echo -e "  ${YELLOW}${iface}${NC}:"
    tc qdisc show dev "$iface" | sed 's/^/    /'
  fi
done

echo ""
echo -e "${GREEN}[ESCENARIO ACTIVO]${NC} Enlace limitado a ${RATE} con delay ${DELAY} en ambos planos."
echo ""
echo -e "${CYAN}[REVERTIR]${NC} sudo ./baseline.sh"
echo -e "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
