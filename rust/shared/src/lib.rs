use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorReading {
    pub sensor_id: String,
    pub timestamp_ms: u64,
    pub value: f64,
    pub unit: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeReport {
    pub edge_id: String,
    pub timestamp_ms: u64,
    pub window_avg: f64,
    pub anomaly_detected: bool,
    pub sample_count: u64,
    pub latency_ms: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordStatus {
    pub active_edges: u32,
    pub total_readings: u64,
    pub anomalies_last_min: u32,
    pub uptime_s: u64,
    pub throughput_msg_per_s: f64,
    pub latency_p50_ms: f64,
    pub latency_p99_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub node_id: String,
    pub role: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum Message {
    EdgeReport(EdgeReport),
    Heartbeat(Heartbeat),
}
