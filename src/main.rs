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
