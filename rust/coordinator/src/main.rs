use axum::{routing::get, Json, Router};
use shared::{CoordStatus, EdgeReport, Heartbeat, Message};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tracing::{error, info, warn};

// ── Constantes ─────────────────────────────────────────────────────────────
// Tiempo máximo sin heartbeat antes de marcar un edge como caído
const HEARTBEAT_TIMEOUT_MS: u64 = 10_000;
// Intervalo de revisión del detector de caídas
const WATCHDOG_INTERVAL_MS: u64 = 2_000;
// Ventana de tiempo para métricas de latencia y anomalías
const METRICS_WINDOW_MS: u64 = 60_000;
// Capacidad del canal de mensajes entrantes
const INCOMING_CHANNEL_CAP: usize = 256;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

// ── Estado por edge ────────────────────────────────────────────────────────
#[derive(Debug)]
struct EdgeInfo {
    last_heartbeat_ms: u64,
    is_alive: bool,
    total_reports: u64,
}

// ── Estado global del coordinator ─────────────────────────────────────────
#[derive(Debug)]
struct CoordinatorState {
    start_time_ms: u64,
    // Mapa edge_id → estado del edge
    edges: HashMap<String, EdgeInfo>,
    // Total de lecturas procesadas desde el inicio
    total_readings: u64,
    // Timestamps de anomalías en los últimos 60s (para conteo exacto)
    anomaly_timestamps: VecDeque<u64>,
    // Ventana deslizante de latencias: (timestamp_ms, latency_ms)
    // Insertadas en orden de llegada, ordenadas solo al calcular percentiles
    latency_window: VecDeque<(u64, f64)>,
}

impl CoordinatorState {
    fn new() -> Self {
        Self {
            start_time_ms: now_ms(),
            edges: HashMap::new(),
            total_readings: 0,
            anomaly_timestamps: VecDeque::new(),
            latency_window: VecDeque::new(),
        }
    }

    /// Elimina entradas de latencia y anomalías fuera de la ventana de 60s.
    /// Llamar antes de cualquier cálculo de métricas.
    fn expire_old_entries(&mut self) {
        let cutoff = now_ms().saturating_sub(METRICS_WINDOW_MS);

        // VecDeque está en orden de inserción (cronológico)
        // Las entradas más antiguas están al frente
        while self
            .latency_window
            .front()
            .map_or(false, |&(ts, _)| ts < cutoff)
        {
            self.latency_window.pop_front();
        }

        while self
            .anomaly_timestamps
            .front()
            .map_or(false, |&ts| ts < cutoff)
        {
            self.anomaly_timestamps.pop_front();
        }
    }

    /// Calcula P50 y P99 sobre la ventana de latencias activa.
    /// Ordena una copia
    fn calculate_percentiles(&self) -> (f64, f64) {
        if self.latency_window.is_empty() {
            return (0.0, 0.0);
        }

        // Recolectar solo los valores de latencia (sin timestamps)
        let mut latencies: Vec<f64> =
            self.latency_window.iter().map(|&(_, lat)| lat).collect();

        // Ordenar solo aquí
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let len = latencies.len();

        // Índice P50: posición del 50% de los datos
        let p50_idx = (len as f64 * 0.50) as usize;
        // Índice P99: posición del 99% de los datos
        let p99_idx = ((len as f64 * 0.99) as usize).min(len - 1);

        (latencies[p50_idx], latencies[p99_idx])
    }

    /// Calcula throughput en msg/s sobre la ventana de 60s.
    fn throughput_msg_per_s(&self) -> f64 {
        let window_s = METRICS_WINDOW_MS as f64 / 1000.0;
        self.latency_window.len() as f64 / window_s
    }
}

// ── Tarea A: servidor TCP que escucha edges ────────────────────────────────
//
// Por cada edge que conecta, lanza una subtarea de lectura.
// Los mensajes deserializados se depositan en incoming_tx.
async fn task_edge_server(listen_port: u16, incoming_tx: mpsc::Sender<Message>) {
    let addr = format!("0.0.0.0:{}", listen_port);
    let listener = TcpListener::bind(&addr)
        .await
        .expect("No se pudo bindear el puerto de edges");

    info!("Escuchando edges en {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!("Edge conectado desde {}", peer_addr);
                let tx = incoming_tx.clone();
                tokio::spawn(handle_edge_connection(stream, tx));
            }
            Err(e) => {
                error!("Error aceptando conexión de edge: {}", e);
            }
        }
    }
}

/// Subtarea: lee Messages de un edge conectado.
async fn handle_edge_connection(stream: tokio::net::TcpStream, tx: mpsc::Sender<Message>) {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        match serde_json::from_str::<Message>(&line) {
            Ok(msg) => {
                if tx.try_send(msg).is_err() {
                    warn!("Canal incoming lleno — mensaje de edge descartado");
                }
            }
            Err(e) => {
                warn!("Mensaje malformado descartado: {}", e);
            }
        }
    }

    info!("Edge desconectado");
}

// ── Tarea C: procesador central de mensajes ────────────────────────────────
//
// Único escritor del CoordinatorState.
// Consume mensajes del canal incoming_rx y actualiza el estado.
async fn task_message_processor(
    mut incoming_rx: mpsc::Receiver<Message>,
    state: Arc<RwLock<CoordinatorState>>,
) {
    while let Some(msg) = incoming_rx.recv().await {
        match msg {
            Message::EdgeReport(report) => handle_edge_report(report, &state).await,
            Message::Heartbeat(hb) => handle_heartbeat(hb, &state).await,
        }
    }
}

async fn handle_edge_report(
    report: EdgeReport,
    state: &Arc<RwLock<CoordinatorState>>,
) {
    let mut s = state.write().await;
    let now = now_ms();

    // Actualizar o crear EdgeInfo para este edge
    let edge = s.edges.entry(report.edge_id.clone()).or_insert(EdgeInfo {
        last_heartbeat_ms: now,
        is_alive: true,
        total_reports: 0,
    });
    edge.total_reports += 1;

    s.total_readings += 1;

    // Registrar latencia E2E en la ventana deslizante
    s.latency_window.push_back((now, report.latency_ms as f64));

    // Registrar anomalía si fue detectada
    if report.anomaly_detected {
        s.anomaly_timestamps.push_back(now);
        warn!(
            edge_id = %report.edge_id,
            window_avg = format!("{:.2}", report.window_avg),
            "Anomalía global registrada"
        );
    }

    info!(
        edge_id = %report.edge_id,
        window_avg = format!("{:.2}", report.window_avg),
        total_readings = s.total_readings,
        "EdgeReport procesado"
    );
}

async fn handle_heartbeat(hb: Heartbeat, state: &Arc<RwLock<CoordinatorState>>) {
    let mut s = state.write().await;
    let now = now_ms();

    let edge = s.edges.entry(hb.node_id.clone()).or_insert(EdgeInfo {
        last_heartbeat_ms: now,
        is_alive: true,
        total_reports: 0,
    });

    // Si el edge estaba marcado como caído, registrar reconexión
    if !edge.is_alive {
        info!(node_id = %hb.node_id, "Edge reconectado");
    }

    edge.last_heartbeat_ms = now;
    edge.is_alive = true;
}

// ── Tarea B: detector de edges caídos (watchdog) ───────────────────────────
//
// Cada WATCHDOG_INTERVAL_MS revisa todos los edges conocidos.
// Si (ahora - last_heartbeat_ms) > HEARTBEAT_TIMEOUT_MS → marcar como caído.
async fn task_watchdog(state: Arc<RwLock<CoordinatorState>>) {
    let interval = Duration::from_millis(WATCHDOG_INTERVAL_MS);

    loop {
        sleep(interval).await;

        let mut s = state.write().await;
        let now = now_ms();

        for (edge_id, info) in s.edges.iter_mut() {
            let elapsed = now.saturating_sub(info.last_heartbeat_ms);

            if elapsed > HEARTBEAT_TIMEOUT_MS && info.is_alive {
                info.is_alive = false;
                warn!(
                    edge_id = %edge_id,
                    elapsed_ms = elapsed,
                    "Edge caído — sin heartbeat en {}ms", elapsed
                );
            }
        }
    }
}

// ── Tarea D: servidor HTTP axum ────────────────────────────────────────────
//
// GET /status → devuelve CoordStatus como JSON
// Solo lectura del estado — usa RwLock read() para no bloquear escrituras.
async fn task_http_server(http_port: u16, state: Arc<RwLock<CoordinatorState>>) {
    let app = Router::new().route(
        "/status",
        get(move || {
            let state = state.clone();
            async move {
                let mut s = state.write().await;
                s.expire_old_entries();

                let (p50, p99) = s.calculate_percentiles();
                let throughput = s.throughput_msg_per_s();
                let active_edges = s.edges.values().filter(|e| e.is_alive).count() as u32;
                let uptime_s = now_ms().saturating_sub(s.start_time_ms) / 1000;

                Json(CoordStatus {
                    active_edges,
                    total_readings: s.total_readings,
                    anomalies_last_min: s.anomaly_timestamps.len() as u32,
                    uptime_s,
                    throughput_msg_per_s: throughput,
                    latency_p50_ms: p50,
                    latency_p99_ms: p99,
                })
            }
        }),
    );

    let addr = format!("0.0.0.0:{}", http_port);
    info!("Servidor HTTP escuchando en {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("No se pudo bindear el puerto HTTP");

    axum::serve(listener, app)
        .await
        .expect("Error en servidor HTTP");
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("coordinator=info".parse().unwrap()),
        )
        .init();

    let tcp_port: u16 = env::var("TCP_PORT")
        .unwrap_or_else(|_| "9100".to_string())
        .parse()
        .unwrap_or(9100);

    let http_port: u16 = env::var("HTTP_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .unwrap_or(8080);

    info!(tcp_port, http_port, "Coordinator iniciando");

    let state = Arc::new(RwLock::new(CoordinatorState::new()));
    let (incoming_tx, incoming_rx) = mpsc::channel::<Message>(INCOMING_CHANNEL_CAP);

    tokio::join!(
        task_edge_server(tcp_port, incoming_tx),
        task_message_processor(incoming_rx, state.clone()),
        task_watchdog(state.clone()),
        task_http_server(http_port, state.clone()),
    );
}
