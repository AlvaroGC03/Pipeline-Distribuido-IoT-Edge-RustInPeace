# Guía de Pruebas iperf3 — Pipeline IoT/Edge IL355
# =============================================================================
# Este archivo documenta los comandos exactos para validar cada escenario
# tc netem con iperf3. Ejecutar ANTES de correr el sistema distribuido.
#
# Infraestructura:
#   Cliente (Peer 3 WSL2): 10.10.10.4
#   Servidor (Hub EC2):    10.10.10.1
#   Puerto iperf3:         5201
#
# Prerequisito: iperf3 instalado en ambos nodos
#   sudo apt install iperf3   (en WSL2 y en EC2)
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# INICIO DE CADA PRUEBA: Lanzar servidor en EC2
# ─────────────────────────────────────────────────────────────────────────────
# En la terminal del Hub EC2:
#   iperf3 -s -p 5201
# Dejar corriendo. Cada prueba de cliente se conecta a este servidor.


# ─────────────────────────────────────────────────────────────────────────────
# ESCENARIO 1 — BASELINE (sin degradación)
# ─────────────────────────────────────────────────────────────────────────────
# Activar:
#   sudo ./baseline.sh

# Verificar estado limpio:
#   ./status.sh

# Prueba TCP (throughput referencia):
iperf3 -c 10.10.10.1 -p 5201 -t 30 --get-server-output
# Resultado esperado: throughput máximo disponible (~varios Mbps sobre WireGuard)

# Prueba UDP (jitter de referencia):
iperf3 -c 10.10.10.1 -p 5201 -u -b 10M -t 30
# Resultado esperado: jitter < 5ms, pérdida 0%

# Registrar en tabla:
#   Throughput TCP baseline: _____ Mbps
#   Jitter UDP baseline:     _____ ms
#   RTT ping baseline:       _____ ms  (ping -c 20 10.10.10.1 | tail -1)


# ─────────────────────────────────────────────────────────────────────────────
# ESCENARIO 2 — LATENCIA IoT
# ─────────────────────────────────────────────────────────────────────────────
# Activar:
#   sudo ./latencia_iot.sh

# Verificar que netem está activo:
#   ./status.sh

# Prueba de latencia con ping (verifica el delay aplicado):
ping -c 20 10.10.10.1
# Resultado esperado: RTT ~160ms (80ms ida + 80ms vuelta) con jitter ±20ms

# Prueba TCP:
iperf3 -c 10.10.10.1 -p 5201 -t 30 --get-server-output

# Prueba UDP (jitter medible):
iperf3 -c 10.10.10.1 -p 5201 -u -b 10M -t 30
# Resultado esperado: jitter ~20ms, throughput reducido

# Registrar:
#   Throughput TCP latencia: _____ Mbps
#   Jitter UDP latencia:     _____ ms  (esperado: ~20ms)
#   RTT ping latencia:       _____ ms  (esperado: ~160ms)

# Limpiar:
#   sudo ./baseline.sh


# ─────────────────────────────────────────────────────────────────────────────
# ESCENARIO 3 — PÉRDIDA DE PAQUETES
# ─────────────────────────────────────────────────────────────────────────────
# Activar:
#   sudo ./perdida_paquetes.sh

# Verificar pérdida con ping (estadísticas al final):
ping -c 50 10.10.10.1
# Resultado esperado: ~8% de paquetes perdidos en la salida de estadísticas

# Prueba TCP (retransmisiones visibles en --get-server-output):
iperf3 -c 10.10.10.1 -p 5201 -t 30 --get-server-output
# Observar: "Retr" column con valor > 0

# Prueba UDP (pérdida real sin retransmisión):
iperf3 -c 10.10.10.1 -p 5201 -u -b 10M -t 30
# Resultado esperado: ~8% loss reportado por iperf3

# Registrar:
#   Throughput TCP pérdida:    _____ Mbps
#   Retransmisiones TCP:       _____
#   Pérdida UDP:               _____ %  (esperado: ~8%)
#   RTT ping (con pérdida):    _____ ms

# Limpiar:
#   sudo ./baseline.sh


# ─────────────────────────────────────────────────────────────────────────────
# ESCENARIO 4 — ENLACE LIMITADO
# ─────────────────────────────────────────────────────────────────────────────
# Activar:
#   sudo ./enlace_limitado.sh

# Prueba TCP (throughput debe quedar limitado a ~512 Kbps = 0.512 Mbps):
iperf3 -c 10.10.10.1 -p 5201 -t 30 --get-server-output
# Resultado esperado: throughput ~0.4-0.5 Mbps (limitado por tc)

# Prueba UDP con bitrate superior al límite (para observar el techo):
iperf3 -c 10.10.10.1 -p 5201 -u -b 2M -t 30
# Aunque pedimos 2Mbps, el throughput real debe ser ~512kbps
# La columna "Lost/Total" mostrará descarte activo

# Registrar:
#   Throughput TCP enlace limitado:  _____ Mbps  (esperado: ~0.5 Mbps)
#   Throughput UDP (bitrate pedido 2M): _____ Mbps
#   RTT ping enlace limitado:        _____ ms    (esperado: +50ms vs baseline)

# Limpiar:
#   sudo ./baseline.sh


# ─────────────────────────────────────────────────────────────────────────────
# ESCENARIO 5 — FALLA DE NODO EDGE
# ─────────────────────────────────────────────────────────────────────────────
# Prerequisito: Pipeline corriendo (sensor + edge + coordinator)
#
# 1. Verificar que el sistema está activo:
#    curl http://10.10.10.1:9100/status
#
# 2. En una terminal separada, seguir logs del coordinator en EC2:
#    ssh ubuntu@52.73.88.87 "docker logs coordinator -f"
#
# 3. Ejecutar la falla:
#    sudo ./falla_nodo.sh edge
#
# 4. En los logs del coordinator, buscar la línea de detección:
#    Ejemplo esperado:
#      [WARN] Edge 'edge-03' timeout — no heartbeat for 10001ms → marking DOWN
#
# 5. Calcular tiempo de detección:
#    t_detección = timestamp_log_detección - timestamp_docker_kill
#    Debe ser < 10000ms
#
# 6. Verificar reconexión automática en logs:
#    Ejemplo esperado:
#      [INFO] Edge 'edge-03' reconnected after 3200ms
#
# Registrar:
#   Tiempo de detección de caída:   _____ ms  (debe ser < 10000ms)
#   Tiempo de reconexión del edge:  _____ ms
#   ¿Backoff exponencial visible?:  Sí / No


# ─────────────────────────────────────────────────────────────────────────────
# TABLA COMPARATIVA — completar con resultados reales
# ─────────────────────────────────────────────────────────────────────────────
#
# | Escenario          | Throughput TCP | Jitter UDP | RTT ping | Pérdida UDP |
# |--------------------|----------------|------------|----------|-------------|
# | Baseline           |          Mbps  |       ms   |     ms   |      %      |
# | Latencia IoT       |          Mbps  |       ms   |     ms   |      %      |
# | Pérdida paquetes   |          Mbps  |       ms   |     ms   |      %      |
# | Enlace limitado    |          Mbps  |       ms   |     ms   |      %      |
# | Falla nodo edge    |       N/A       |     N/A    |    N/A   |     N/A     |
#   → Tiempo detección: ___ms  |  Tiempo reconexión: ___ms
