#!/usr/bin/env bash
# =============================================================================
# falla_nodo.sh — Escenario 5: Falla de nodo edge
# =============================================================================
# Propósito: Simula la caída abrupta de un contenedor edge durante ejecución
#            para verificar que el coordinator detecta la falla en < 10 segundos
#            (requisito del Documento B) y que el edge se reconecta automáticamente.
#
# Este escenario NO usa tc netem — en cambio simula una falla de nodo real:
#   docker stop <container>   → SIGTERM, graceful shutdown (hasta 10s)
#   docker kill <container>   → SIGKILL, falla abrupta inmediata ← usamos esto
#
# Flujo del escenario:
#   1. Verificar que el contenedor edge esté corriendo
#   2. Iniciar tail de logs del coordinator (en background) para capturar
#      el momento exacto de detección
#   3. Registrar timestamp de la muerte del contenedor
#   4. Ejecutar docker kill
#   5. Esperar y mostrar logs del coordinator durante la ventana de detección
#   6. Opcionalmente, reiniciar el edge y verificar reconexión automática
#
# Uso:
#   sudo ./falla_nodo.sh                        # usa contenedor 'edge' por defecto
#   sudo ./falla_nodo.sh edge-03                # nombre explícito del contenedor
#   sudo ./falla_nodo.sh edge-03 --no-restart   # no reiniciar tras la falla
#
# Para restaurar manualmente:
#   docker start <container_name>
# =============================================================================

set -euo pipefail

# ── Colores ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36b'
BOLD='\033[1m'
NC='\033[0m'

# ── Configuración ─────────────────────────────────────────────────────────────
EDGE_CONTAINER="${1:-edge}"           # Nombre del contenedor edge a matar
COORDINATOR_CONTAINER="coordinator"   # Para obtener logs post-falla
AUTO_RESTART=true                     # Reiniciar el edge automáticamente
WAIT_DETECTION=15                     # Segundos a esperar para capturar detección
WAIT_RECONNECT=20                     # Segundos a esperar reconexión tras restart

# ── Parseo de flags opcionales ────────────────────────────────────────────────
shift 1 2>/dev/null || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-restart) AUTO_RESTART=false; shift ;;
    --container)  EDGE_CONTAINER="$2"; shift 2 ;;
    *) echo "Uso: $0 [container_name] [--no-restart] [--container NAME]"; exit 1 ;;
  esac
done

# ── Verificar que docker está disponible ──────────────────────────────────────
if ! command -v docker &>/dev/null; then
  echo -e "${RED}[ERROR]${NC} Docker no encontrado en PATH."
  exit 1
fi

# ── Verificar que el contenedor edge existe y está corriendo ──────────────────
echo ""
echo -e "${CYAN}════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  ESCENARIO 5 — Falla de nodo edge                  ${NC}"
echo -e "${CYAN}  Contenedor objetivo: ${YELLOW}${EDGE_CONTAINER}${CYAN}               ${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════${NC}"
echo ""

CONTAINER_STATUS=$(docker inspect --format='{{.State.Status}}' "$EDGE_CONTAINER" 2>/dev/null || echo "not_found")

if [[ "$CONTAINER_STATUS" == "not_found" ]]; then
  echo -e "${RED}[ERROR]${NC} Contenedor '${EDGE_CONTAINER}' no encontrado."
  echo "  Contenedores disponibles:"
  docker ps --format "  - {{.Names}} ({{.Status}})"
  exit 1
fi

if [[ "$CONTAINER_STATUS" != "running" ]]; then
  echo -e "${RED}[ERROR]${NC} Contenedor '${EDGE_CONTAINER}' no está corriendo (estado: ${CONTAINER_STATUS})."
  exit 1
fi

echo -e "${GREEN}[OK]${NC} Contenedor '${EDGE_CONTAINER}' verificado — estado: running"
echo ""

# ── Mostrar estado actual del coordinator (últimas líneas antes de la falla) ──
echo -e "${CYAN}[PRE-FALLA]${NC} Últimos logs del coordinator (contexto):"
docker logs "$COORDINATOR_CONTAINER" --tail 5 2>/dev/null | sed 's/^/  /' \
  || echo "  (coordinator no accesible localmente — ver logs en EC2)"
echo ""

# ── Timestamp de inicio ───────────────────────────────────────────────────────
T_START=$(date +%s%3N)   # milisegundos
echo -e "${BOLD}${RED}[ACCIÓN]${NC} Matando contenedor '${EDGE_CONTAINER}' con SIGKILL..."
echo -e "  Timestamp: $(date '+%Y-%m-%d %H:%M:%S.%3N')"
echo ""

# ── Ejecutar la falla: SIGKILL al contenedor ──────────────────────────────────
docker kill "$EDGE_CONTAINER"

T_KILL=$(date +%s%3N)
echo -e "${RED}[FALLA SIMULADA]${NC} Contenedor detenido. t=0 ms"
echo ""

# ── Esperar y capturar la detección del coordinator ───────────────────────────
echo -e "${CYAN}[MONITOREO]${NC} Esperando detección del coordinator (máx ${WAIT_DETECTION}s)..."
echo -e "  (El coordinator debe detectar la falla en < 10s por ausencia de heartbeats)"
echo ""

# Tail de logs del coordinator durante la ventana de detección
echo -e "${YELLOW}--- LOGS DEL COORDINATOR (próximos ${WAIT_DETECTION}s) ---${NC}"
timeout "$WAIT_DETECTION" docker logs "$COORDINATOR_CONTAINER" --follow 2>/dev/null | \
  while IFS= read -r line; do
    T_NOW=$(date +%s%3N)
    ELAPSED=$(( T_NOW - T_KILL ))
    echo "  [+${ELAPSED}ms] $line"
  done || echo "  (timeout de ${WAIT_DETECTION}s alcanzado o coordinator en EC2)"
echo -e "${YELLOW}--- FIN DE VENTANA DE DETECCIÓN ---${NC}"
echo ""

# ── Reinicio automático (opcional) ───────────────────────────────────────────
if [[ "$AUTO_RESTART" == "true" ]]; then
  echo -e "${CYAN}[RECONEXIÓN]${NC} Reiniciando contenedor '${EDGE_CONTAINER}'..."
  docker start "$EDGE_CONTAINER"
  T_RESTART=$(date +%s%3N)
  echo -e "${GREEN}[OK]${NC} Contenedor reiniciado."
  echo ""
  echo -e "${CYAN}[MONITOREO]${NC} Verificando reconexión automática (${WAIT_RECONNECT}s)..."
  echo -e "  (El edge debe reconectarse con backoff exponencial sin intervención manual)"
  echo ""

  echo -e "${YELLOW}--- LOGS DEL COORDINATOR (reconexión) ---${NC}"
  timeout "$WAIT_RECONNECT" docker logs "$COORDINATOR_CONTAINER" --follow 2>/dev/null | \
    while IFS= read -r line; do
      T_NOW=$(date +%s%3N)
      ELAPSED=$(( T_NOW - T_RESTART ))
      echo "  [+${ELAPSED}ms] $line"
    done || echo "  (timeout de ${WAIT_RECONNECT}s alcanzado o coordinator en EC2)"
  echo -e "${YELLOW}--- FIN DE VENTANA DE RECONEXIÓN ---${NC}"
else
  echo -e "${YELLOW}[INFO]${NC} Reinicio automático deshabilitado (--no-restart)."
  echo "  Para reiniciar manualmente: docker start ${EDGE_CONTAINER}"
fi

echo ""
echo -e "Timestamp fin: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
