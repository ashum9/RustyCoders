use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Debug)]
pub struct Metrics {
    pub total_connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub peak_active_connections: AtomicU64,
    pub bytes_rx: AtomicU64,
    pub bytes_tx: AtomicU64,
    pub connection_errors: AtomicU64,
    pub backend_connect_latency_us: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_active_connection(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        let current_active = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_active_connections.fetch_max(current_active, Ordering::Relaxed);
    }

    pub fn dec_active_connection(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn add_bytes(&self, rx: u64, tx: u64) {
        self.bytes_rx.fetch_add(rx, Ordering::Relaxed);
        self.bytes_tx.fetch_add(tx, Ordering::Relaxed);
    }

    pub fn inc_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_latency(&self, us: u64) {
        self.backend_connect_latency_us.fetch_add(us, Ordering::Relaxed);
    }
}
