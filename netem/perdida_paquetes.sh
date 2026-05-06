#!/usr/bin/env bash
# =============================================================================
# perdida_paquetes.sh — Escenario 3: Pérdida de paquetes
# =============================================================================
# Propósito: Simula condiciones de red con pérdida de paquetes, típico de
#            enlaces WiFi industriales, redes mesh o canales de radio.
#
# Parámetros tc netem aplicados:
#   loss 8%
#
#   - loss 8% → el kernel descarta aleatoriamente el 8% de los paquetes
#               en egress de la interfaz (distribución uniforme)
#
# Uso:
#   sudo ./perdida_paquetes.sh
#   sudo ./perdida_paquetes.sh --iface-docker br-XXXXXXXX
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
LOSS_PERCENT="8%"

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

# ── Función: aplicar netem con pérdida ───────────────────────────────────────
# tc qdisc add dev <iface> root netem loss X%
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

  # Aplicar pérdida de paquetes
  tc qdisc add dev "$iface" root netem loss "$LOSS_PERCENT"

  echo -e "  ${GREEN}[OK]${NC} loss=${LOSS_PERCENT} aplicado"
}

# ── Ejecución ─────────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  ESCENARIO 3 — Pérdida de paquetes               ${NC}"
echo -e "${CYAN}  loss ${LOSS_PERCENT}                                       ${NC}"
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
echo -e "${GREEN}[ESCENARIO ACTIVO]${NC} Pérdida de paquetes del ${LOSS_PERCENT} aplicada en ambos planos."
echo ""
echo -e "${CYAN}[REVERTIR]${NC} sudo ./baseline.sh"
echo -e "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
