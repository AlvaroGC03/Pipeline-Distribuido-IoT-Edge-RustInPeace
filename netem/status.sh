#!/usr/bin/env bash
# =============================================================================
# status.sh — Estado actual de tc qdisc en ambas interfaces
# =============================================================================
# Propósito: Muestra de forma clara y legible el estado actual de las reglas
#            tc activas en los dos planos de red del proyecto.
#
# Útil para:
#   - Verificar que un escenario se aplicó correctamente
#   - Confirmar que baseline.sh limpió todo
#   - Diagnosticar problemas de red antes de correr iperf3
#   - Captura de evidencia para el reporte
#
# No requiere privilegios root (tc qdisc show es lectura).
#
# Uso:
#   ./status.sh
#   ./status.sh --iface-docker br-XXXXXXXX
# =============================================================================

set -euo pipefail

# ── Colores ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ── Interfaces objetivo ───────────────────────────────────────────────────────
IFACE_DOCKER="${IFACE_DOCKER:-br-6e0e088d9cfe}"
IFACE_VPN="${IFACE_VPN:-wg0}"

# ── Parseo de argumentos ──────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --iface-docker) IFACE_DOCKER="$2"; shift 2 ;;
    --iface-vpn)    IFACE_VPN="$2";    shift 2 ;;
    *) echo "Uso: $0 [--iface-docker IFACE] [--iface-vpn IFACE]"; exit 1 ;;
  esac
done

# ── Función: mostrar estado de una interfaz ───────────────────────────────────
show_iface_status() {
  local iface="$1"
  local label="$2"
  local subnet="$3"

  echo -e "${BOLD}${CYAN}┌─ ${label}${NC}"
  echo -e "${CYAN}│  Interfaz: ${YELLOW}${iface}${NC}  │  Red: ${subnet}"

  if ! ip link show "$iface" &>/dev/null; then
    echo -e "${CYAN}│${NC}  ${YELLOW}[WARN]${NC} Interfaz no encontrada (daemon detenido o interfaz inexistente)"
    echo -e "${CYAN}└────────────────────────────────────────────${NC}"
    echo ""
    return 0
  fi

  # Estado de la interfaz (UP/DOWN)
  local state
  state=$(ip link show "$iface" | grep -oE 'state [A-Z]+' | awk '{print $2}')
  echo -e "${CYAN}│${NC}  Estado físico: ${state}"

  # qdisc activo
  echo -e "${CYAN}│${NC}  tc qdisc:"
  local qdisc_output
  qdisc_output=$(tc qdisc show dev "$iface")

  # Interpretar el escenario activo
  if echo "$qdisc_output" | grep -q "netem"; then
    echo -e "${CYAN}│${NC}    ${YELLOW}[NETEM ACTIVO]${NC}"
    # Extraer parámetros relevantes
    echo "$qdisc_output" | sed 's/^/    /'

    # Detectar qué escenario está activo
    if echo "$qdisc_output" | grep -q "delay"; then
      local delay_val
      delay_val=$(echo "$qdisc_output" | grep -oE 'delay [0-9]+(\.[0-9]+)?[a-z]+' | head -1)
      echo -e "${CYAN}│${NC}    Parámetro detectado: ${delay_val}"
    fi
    if echo "$qdisc_output" | grep -q "loss"; then
      local loss_val
      loss_val=$(echo "$qdisc_output" | grep -oE 'loss [0-9]+%?' | head -1)
      echo -e "${CYAN}│${NC}    Parámetro detectado: ${loss_val}"
    fi
    if echo "$qdisc_output" | grep -q "rate"; then
      local rate_val
      rate_val=$(echo "$qdisc_output" | grep -oE 'rate [0-9]+[a-zA-Z]+' | head -1)
      echo -e "${CYAN}│${NC}    Parámetro detectado: ${rate_val}"
    fi
  elif echo "$qdisc_output" | grep -q "tbf"; then
    echo -e "${CYAN}│${NC}    ${YELLOW}[TBF ACTIVO — enlace limitado]${NC}"
    echo "$qdisc_output" | sed 's/^/    /'
  elif echo "$qdisc_output" | grep -q "pfifo_fast\|fq_codel\|noqueue"; then
    echo -e "${CYAN}│${NC}    ${GREEN}[LIMPIO — sin degradación (baseline)]${NC}"
    echo "$qdisc_output" | sed 's/^/    /'
  else
    echo "$qdisc_output" | sed 's/^/    /'
  fi

  echo -e "${CYAN}└────────────────────────────────────────────${NC}"
  echo ""
}

# ── Ejecución ─────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${CYAN}  STATUS — Estado de red del pipeline IoT/Edge       ${NC}"
echo -e "${BOLD}${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')                        ${NC}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════${NC}"
echo ""

show_iface_status "$IFACE_DOCKER" "Plano 1 — Docker IoT bridge (sensor→edge)" "172.19.0.0/16"
show_iface_status "$IFACE_VPN"    "Plano 2 — WireGuard VPN (edge→coordinator)" "10.10.10.0/24"

# ── Estado WireGuard ──────────────────────────────────────────────────────────
echo -e "${BOLD}${CYAN}┌─ WireGuard — Estado de peers${NC}"
if command -v wg &>/dev/null && sudo wg show &>/dev/null 2>&1; then
  sudo wg show | sed 's/^/│  /'
else
  echo -e "│  ${YELLOW}[INFO]${NC} wg show requiere sudo o WireGuard no está activo"
fi
echo -e "${CYAN}└────────────────────────────────────────────${NC}"
echo ""

# ── Estado de contenedores Docker ────────────────────────────────────────────
echo -e "${BOLD}${CYAN}┌─ Docker — Contenedores del pipeline${NC}"
if command -v docker &>/dev/null; then
  docker ps --format "│  {{.Names}}\t{{.Status}}\t{{.Image}}" \
    | grep -E "sensor|edge|coordinator" \
    | sed 's/^//' \
    || echo "│  (ningún contenedor del pipeline corriendo)"
fi
echo -e "${CYAN}└────────────────────────────────────────────${NC}"
echo ""

# ── Guía de referencia rápida ─────────────────────────────────────────────────
echo -e "${CYAN}[REFERENCIA RÁPIDA]${NC}"
echo "  sudo ./baseline.sh          → Sin degradación"
echo "  sudo ./latencia_iot.sh      → delay 80ms jitter 20ms"
echo "  sudo ./perdida_paquetes.sh  → loss 8%"
echo "  sudo ./enlace_limitado.sh   → rate 512kbit delay 50ms"
echo "  sudo ./falla_nodo.sh [edge] → Mata contenedor edge"
echo ""
