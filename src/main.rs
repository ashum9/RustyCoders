mod listener;
mod metrics;
mod proxy;

use crate::metrics::Metrics;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    tracing::info!("gateway starting...");

    let bind_addr = "127.0.0.1:8080";
    let backend_addr = "127.0.0.1:9000";

    let metrics = Arc::new(Metrics::new());

    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let active = metrics_clone.active_connections.load(Ordering::Relaxed);
            let total = metrics_clone.total_connections.load(Ordering::Relaxed);
            let bytes = metrics_clone.bytes_forwarded.load(Ordering::Relaxed);
            tracing::info!(
                "metrics: active_connections={}, total_connections={}, total_bytes_forwarded={}",
                active, total, bytes
            );
        }
    });

    if let Err(e) = listener::run_server(bind_addr, backend_addr, metrics).await {
        tracing::error!("server error: {}", e);
    }

    Ok(())
}
