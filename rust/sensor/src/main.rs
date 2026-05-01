use shared::SensorReading;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use rand_distr::{Normal, Distribution}; 
use std::env;

// ── Constantes de backoff exponencial (Sincronizadas con el Edge) ──────────
const BACKOFF_INITIAL_MS: u64 = 500;
const BACKOFF_MAX_MS: u64 = 30_000;
const BACKOFF_MULTIPLIER: u64 = 2;

// ── Configuración del Sensor ───────────────────────────────────────────────
const SAMPLING_INTERVAL_SECS: u64 = 2;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sensor_id = env::var("SENSOR_ID")
        .unwrap_or_else(|_| "SENSOR_TEMP_01".to_string());
    let target_addr = env::var("EDGE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    
    let mut sequence = 0;
    let mut retry_delay = Duration::from_millis(BACKOFF_INITIAL_MS);

    println!("Iniciando Sensor: {} - Destino: {}", sensor_id, target_addr);

    // ── LOOP EXTERIOR (Resiliencia) ────────────────────────────────────────
    loop {
        match TcpStream::connect(&target_addr).await {
            Ok(mut stream) => {
                println!("Conectado al Nodo Edge en {}", target_addr);
                retry_delay = Duration::from_millis(BACKOFF_INITIAL_MS); 

                let mut rng = rand::thread_rng();
                let normal = Normal::new(22.0, 2.0).unwrap(); 

                // ── LOOP INTERIOR (Envío de Datos) ─────────────────────────
                loop {
                    let raw_temp: f64 = normal.sample(&mut rng);
                    let temperature = raw_temp.clamp(10.0, 50.0);
                    
                    let reading = SensorReading {
                        sensor_id: sensor_id.clone(),
                        timestamp_ms: SystemTime::now()
                            .duration_since(UNIX_EPOCH)?
                            .as_millis() as u64,
                        value: temperature,
                        unit: "Celsius".to_string(),
                        sequence,
                    };

                    let mut json_string = serde_json::to_string(&reading)?;
                    json_string.push('\n'); 

                    if let Err(e) = stream.write_all(json_string.as_bytes()).await {
                        eprintln!("Error de red: {}. Intentando reconectar...", e);
                        break; 
                    }

                    println!("Enviado: {:.2}°C (Sec: {})", temperature, sequence);
                    sequence += 1;

                    tokio::time::sleep(Duration::from_secs(SAMPLING_INTERVAL_SECS)).await;
                }
            }
            // ── MANEJO DE FALLOS DE CONEXIÓN (Backoff Exponencial) ──────────
            Err(e) => {
                eprintln!(
                    "No se pudo conectar al Edge: {}. Reintentando en {:?}...", 
                    e, retry_delay
                );
                tokio::time::sleep(retry_delay).await;
                
                retry_delay = std::cmp::min(retry_delay * BACKOFF_MULTIPLIER as u32, Duration::from_millis(BACKOFF_MAX_MS));
            }
        }
    }
}
