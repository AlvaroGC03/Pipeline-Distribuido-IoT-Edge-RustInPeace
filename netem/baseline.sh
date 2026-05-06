#!/usr/bin/env bash
# =============================================================================
# baseline.sh — Restaurar red limpia (sin degradación)
# =============================================================================
# Propósito: Elimina TODAS las reglas tc activas en los dos planos de red del
#            proyecto y restaura el comportamiento de red por defecto.
#
# Uso:
#   sudo ./baseline.sh
#   sudo ./baseline.sh --iface-docker br-XXXXXXXX   # override manual
#
# Requisitos: iproute2 (tc), privilegios root/sudo
# =============================================================================

set -euo pipefail

# ── Colores para output ───────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ── Interfaces objetivo (editables si cambia el br-) ─────────────────────────
IFACE_DOCKER="${IFACE_DOCKER:-br-6e0e088d9cfe}"   # Plano 1: Docker bridge IoT
IFACE_VPN="${IFACE_VPN:-wg0}"                      # Plano 2: WireGuard

# ── Parseo de argumentos opcionales ──────────────────────────────────────────
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

# ── Función: eliminar qdisc raíz de una interfaz ─────────────────────────────
clear_iface() {
  local iface="$1"
  local label="$2"

  echo -e "${CYAN}[BASELINE]${NC} Limpiando interfaz ${YELLOW}${iface}${NC} (${label})..."

  # Verificar que la interfaz existe
  if ! ip link show "$iface" &>/dev/null; then
    echo -e "  ${YELLOW}[WARN]${NC} Interfaz ${iface} no encontrada — omitiendo."
    return 0
  fi

  # Eliminar el qdisc raíz (remueve TODAS las reglas netem/tbf asociadas)
  # El qdisc por defecto en Linux es 'pfifo_fast'; al borrar el actual se restaura.
  if tc qdisc del dev "$iface" root 2>/dev/null; then
    echo -e "  ${GREEN}[OK]${NC} qdisc eliminado en ${iface}."
  else
    # Si no hay qdisc personalizado, tc retorna error — es un estado limpio ya
    echo -e "  ${GREEN}[OK]${NC} No había reglas tc activas en ${iface} (ya limpia)."
  fi
}

# ── Ejecución ────────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}════════════════════════════════════════${NC}"
echo -e "${CYAN}  BASELINE — Restaurar red sin degradación${NC}"
echo -e "${CYAN}════════════════════════════════════════${NC}"
echo ""

clear_iface "$IFACE_DOCKER" "Plano 1: Docker IoT bridge (sensor→edge)"
echo ""
clear_iface "$IFACE_VPN"    "Plano 2: WireGuard VPN (edge→coordinator)"
echo ""

# ── Verificación final ────────────────────────────────────────────────────────
echo -e "${CYAN}[VERIFICACIÓN]${NC} Estado final de qdisc en ambas interfaces:"
echo ""
for iface in "$IFACE_DOCKER" "$IFACE_VPN"; do
  if ip link show "$iface" &>/dev/null; then
    echo -e "  ${YELLOW}${iface}${NC}:"
    tc qdisc show dev "$iface" | sed 's/^/    /'
  fi
done

echo ""
echo -e "${GREEN}[BASELINE ACTIVO]${NC} Red restaurada a condiciones sin degradación."
echo -e "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
