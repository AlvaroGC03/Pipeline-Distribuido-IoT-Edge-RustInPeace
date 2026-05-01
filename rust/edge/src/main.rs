use shared::{EdgeReport, Heartbeat, Message, SensorReading};
use std::collections::VecDeque;
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{error, info, warn};

// ── Constantes de backoff exponencial (mismos valores que sensor) ──────────
const BACKOFF_INITIAL_MS: u64 = 500;
const BACKOFF_MAX_MS: u64 = 30_000;
const BACKOFF_MULTIPLIER: u64 = 2;

// ── Parámetros del pipeline ────────────────────────────────────────────────
const WINDOW_SIZE: usize = 10;

// ── Capacidades de canales internos ───────────────────────────────────────
const SENSOR_CHANNEL_CAP: usize = 32;
const WRITER_CHANNEL_CAP: usize = 64;

/// Configuración leída desde variables de entorno.
struct Config {
    edge_id: String,
    listen_port: u16,
    coordinator_addr: String,
    threshold: f64,
    heartbeat_interval_ms: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            edge_id: env::var("EDGE_ID")
                .unwrap_or_else(|_| "edge-01".to_string()),
            listen_port: env::var("LISTEN_PORT")
                .unwrap_or_else(|_| "9000".to_string())
                .parse()
                .unwrap_or(9000),
            coordinator_addr: env::var("COORDINATOR_ADDR")
                .unwrap_or_else(|_| "10.10.10.1:9100".to_string()),
            threshold: env::var("THRESHOLD")
                .unwrap_or_else(|_| "35.0".to_string())
                .parse()
                .unwrap_or(35.0),
            heartbeat_interval_ms: env::var("HEARTBEAT_INTERVAL_MS")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .unwrap_or(3000),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

// ── Tarea A: servidor TCP que escucha sensores ─────────────────────────────
//
// Por cada sensor que conecta, lanza una subtarea de lectura independiente.
// Cada subtarea deserializa SensorReadings y las deposita en sensor_tx.
// Si la conexión del sensor se rompe, la subtarea termina silenciosamente.
async fn task_sensor_server(
    listen_port: u16,
    sensor_tx: mpsc::Sender<SensorReading>,
) {
    let addr = format!("0.0.0.0:{}", listen_port);
    let listener = TcpListener::bind(&addr)
        .await
        .expect("No se pudo bindear el puerto de sensores");

    info!("Escuchando sensores en {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!("Sensor conectado desde {}", peer_addr);
                let tx = sensor_tx.clone();
                // Cada sensor obtiene su propia tarea de lectura
                tokio::spawn(handle_sensor_connection(stream, tx));
            }
            Err(e) => {
                error!("Error aceptando conexión de sensor: {}", e);
            }
        }
    }
}

/// Subtarea - Lee SensorReadings de un sensor conectado.
async fn handle_sensor_connection(
    stream: TcpStream,
    sensor_tx: mpsc::Sender<SensorReading>,
) {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        match serde_json::from_str::<SensorReading>(&line) {
            Ok(reading) => {
                // try_send descarta el mensaje si el canal está lleno
                // en lugar de bloquear evita que un canal lento
                // bloquee la lectura de otros sensores
                if sensor_tx.try_send(reading).is_err() {
                    warn!("Canal de sensores lleno — lectura descartada");
                }
            }
            Err(e) => {
                warn!("Lectura malformada descartada: {}", e);
            }
        }
    }

    info!("Sensor desconectado");
}

// ── Tarea B: procesador de ventana deslizante ──────────────────────────────
//
// Mantiene una VecDeque con las últimas WINDOW_SIZE muestras.
// Por cada lectura recibida:
//   1. Agrega el valor a la ventana (descarta el más antiguo si está llena)
//   2. Calcula el promedio móvil
//   3. Detecta si el valor supera el umbral
//   4. Construye un EdgeReport y lo envía al writer
async fn task_window_processor(
    edge_id: String,
    threshold: f64,
    mut sensor_rx: mpsc::Receiver<SensorReading>,
    writer_tx: mpsc::Sender<Message>,
) {
    let mut window: VecDeque<f64> = VecDeque::with_capacity(WINDOW_SIZE);
    let mut sequence: u64 = 0;

    while let Some(reading) = sensor_rx.recv().await {
        // Actualizar ventana deslizante
        if window.len() == WINDOW_SIZE {
            window.pop_front();
        }
        window.push_back(reading.value);

        // Calcular promedio móvil
        let window_avg = window.iter().sum::<f64>() / window.len() as f64;

        // Detectar anomalía en la ventana completa
        let anomaly_detected = window.iter().any(|&v| v > threshold);

        // Calcular latencia sensor -> edge
        let latency_ms = now_ms().saturating_sub(reading.timestamp_ms);

        let report = EdgeReport {
            edge_id: edge_id.clone(),
            timestamp_ms: now_ms(),
            window_avg,
            anomaly_detected,
            sample_count: sequence + 1,
            latency_ms,
            sequence,
        };

        if anomaly_detected {
            warn!(
                edge_id = %edge_id,
                window_avg = format!("{:.2}", window_avg),
                "Anomalía detectada"
            );
        } else {
            info!(
                edge_id = %edge_id,
                window_avg = format!("{:.2}", window_avg),
                sequence,
                "EdgeReport generado"
            );
        }

        // Depositar en el canal del writer — descartar si está lleno
        if writer_tx.try_send(Message::EdgeReport(report)).is_err() {
            warn!("Canal writer lleno — EdgeReport descartado");
        }

        sequence += 1;
    }
}

// ── Tarea D: heartbeat periódico ───────────────────────────────────────────
//
// Cada heartbeat_interval_ms envía un Heartbeat al writer.
// El coordinator usa estos mensajes para detectar edges caídos
// si no recibe ninguno en más de 10 segundos.
async fn task_heartbeat(
    edge_id: String,
    heartbeat_interval_ms: u64,
    writer_tx: mpsc::Sender<Message>,
) {
    let interval = Duration::from_millis(heartbeat_interval_ms);

    loop {
        sleep(interval).await;

        let hb = Heartbeat {
            node_id: edge_id.clone(),
            role: "edge".to_string(),
            timestamp_ms: now_ms(),
        };

        if writer_tx.try_send(Message::Heartbeat(hb)).is_err() {
            warn!("Canal writer lleno — Heartbeat descartado");
        } else {
            info!(edge_id = %edge_id, "Heartbeat enviado");
        }
    }
}

// ── Tarea W: writer TCP hacia coordinator ──────────────────────────────────
//
// Es el único dueño del TcpStream hacia el coordinator.
// Consume mensajes del canal writer_rx y los serializa como NDJSON.
// Si la conexión se rompe, reconecta con backoff exponencial.
// Durante la reconexión, los mensajes en el canal esperan (hasta
// WRITER_CHANNEL_CAP mensajes) antes de ser descartados por los emisores.
async fn task_writer(
    coordinator_addr: String,
    mut writer_rx: mpsc::Receiver<Message>,
) {
    let mut delay_ms = BACKOFF_INITIAL_MS;

    loop {
        // ── Fase de conexión ──────────────────────────────────────────────
        let mut stream = loop {
            match TcpStream::connect(&coordinator_addr).await {
                Ok(s) => {
                    info!("Conectado al coordinator en {}", coordinator_addr);
                    delay_ms = BACKOFF_INITIAL_MS; // reset backoff al conectar
                    break s;
                }
                Err(e) => {
                    warn!(
                        "No se pudo conectar al coordinator ({}). Reintentando en {}ms...",
                        e, delay_ms
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * BACKOFF_MULTIPLIER).min(BACKOFF_MAX_MS);
                }
            }
        };

        // ── Fase de escritura ─────────────────────────────────────────────
        loop {
            match writer_rx.recv().await {
                Some(msg) => {
                    let mut line = match serde_json::to_string(&msg) {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Error serializando mensaje: {}", e);
                            continue;
                        }
                    };
                    line.push('\n');

                    if let Err(e) = stream.write_all(line.as_bytes()).await {
                        error!("Error escribiendo al coordinator ({}). Reconectando...", e);
                        break; // salir al loop externo para reconectar
                    }
                }
                None => {
                    // El canal se cerró o todos los emisores han caído
                    error!("Canal writer cerrado inesperadamente");
                    return;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("edge=info".parse().unwrap()),
        )
        .init();

    let config = Config::from_env();

    info!(
        edge_id = %config.edge_id,
        listen_port = config.listen_port,
        coordinator_addr = %config.coordinator_addr,
        threshold = config.threshold,
        "Edge node iniciando"
    );

    // ── Canales internos ──────────────────────────────────────────────────
    let (sensor_tx, sensor_rx) = mpsc::channel::<SensorReading>(SENSOR_CHANNEL_CAP);
    let (writer_tx, writer_rx) = mpsc::channel::<Message>(WRITER_CHANNEL_CAP);

    // ── Lanzar tareas ─────────────────────────────────────────────────────
    // Cada tarea recibe exactamente lo que necesita
    tokio::join!(
        task_sensor_server(config.listen_port, sensor_tx),
        task_window_processor(
            config.edge_id.clone(),
            config.threshold,
            sensor_rx,
            writer_tx.clone(),  // clone porque D también necesita writer_tx
        ),
        task_heartbeat(
            config.edge_id.clone(),
            config.heartbeat_interval_ms,
            writer_tx,          // moved
        ),
        task_writer(config.coordinator_addr, writer_rx),
    );
}
