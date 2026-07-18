use traffic_gateway::config::GatewayConfig;
use traffic_gateway::listener;
use traffic_gateway::metrics::Metrics;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    tracing::info!("gateway starting...");

    let config_path = "gateway.toml";
    let config = GatewayConfig::load(config_path)?;
    
    tracing::info!("loaded config with {} listeners, max_connections: {}", config.listeners.len(), config.global.max_connections);
    for listener in &config.listeners {
        tracing::info!("  - [{}]: {} -> {}", listener.name, listener.listen, listener.backend);
    }

    let metrics = Arc::new(Metrics::new());
    let semaphore = Arc::new(Semaphore::new(config.global.max_connections));
    let cancel_token = CancellationToken::new();

    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let start_time = std::time::Instant::now();
        let mut last_tick = std::time::Instant::now();
        let mut last_total = 0;
        let mut last_rx = 0;
        let mut last_tx = 0;

        loop {
            interval.tick().await;
            let now = std::time::Instant::now();
            let elapsed_secs = (now - last_tick).as_secs().max(1);
            let uptime_secs = (now - start_time).as_secs();

            let active = metrics_clone.active_connections.load(Ordering::Relaxed);
            let total = metrics_clone.total_connections.load(Ordering::Relaxed);
            let peak = metrics_clone.peak_active_connections.load(Ordering::Relaxed);
            let errors = metrics_clone.connection_errors.load(Ordering::Relaxed);
            let bytes_rx = metrics_clone.bytes_rx.load(Ordering::Relaxed);
            let bytes_tx = metrics_clone.bytes_tx.load(Ordering::Relaxed);
            
            let total_latency_us = metrics_clone.backend_connect_latency_us.load(Ordering::Relaxed);
            let avg_latency = if total > 0 { total_latency_us / total } else { 0 };

            let cps = (total.saturating_sub(last_total)) / elapsed_secs;
            let rx_rate = (bytes_rx.saturating_sub(last_rx)) / elapsed_secs;
            let tx_rate = (bytes_tx.saturating_sub(last_tx)) / elapsed_secs;

            last_tick = now;
            last_total = total;
            last_rx = bytes_rx;
            last_tx = bytes_tx;

            tracing::info!(
                "metrics [UP {}s]: active={}, peak={}, total={}, CPS={}/s, rx_total={}B ({} B/s), tx_total={}B ({} B/s), latency={}µs, errors={}",
                uptime_secs, active, peak, total, cps, bytes_rx, rx_rate, bytes_tx, tx_rate, avg_latency, errors
            );
        }
    });

    let cancel_token_for_signal = cancel_token.clone();
    tokio::spawn(async move {
        if let Ok(_) = tokio::signal::ctrl_c().await {
            tracing::info!("ctrl-c received, shutting down gracefully...");
            cancel_token_for_signal.cancel();
        }
    });

    let mut listener_tasks = Vec::new();

    for listener_config in config.listeners {
        let metrics = metrics.clone();
        let semaphore = semaphore.clone();
        let cancel_token = cancel_token.clone();
        
        let task = tokio::spawn(async move {
            if let Err(e) = listener::run_server(listener_config, metrics, semaphore, cancel_token).await {
                tracing::error!("server error: {}", e);
            }
        });
        listener_tasks.push(task);
    }

    for task in listener_tasks {
        let _ = task.await;
    }

    tracing::info!("gateway shut down gracefully");

    Ok(())
}
