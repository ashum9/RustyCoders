use traffic_gateway::backend::instance::BackendInstance;
use traffic_gateway::backend::pool::{BackendPool, LbStrategy};
use traffic_gateway::config::GatewayConfig;
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

    let metrics = Arc::new(Metrics::new());
    let semaphore = Arc::new(Semaphore::new(config.global.max_connections));
    let cancel_token = CancellationToken::new();

    let cancel_token_for_signal = cancel_token.clone();
    tokio::spawn(async move {
        if let Ok(_) = tokio::signal::ctrl_c().await {
            tracing::info!("ctrl-c received, shutting down gracefully...");
            cancel_token_for_signal.cancel();
        }
    });

    let mut all_pools = Vec::new();
    for listener in config.listeners {
        tracing::info!("  - [{}]: {} -> {:?}", listener.name, listener.listen, listener.backends);

        let mut instances = Vec::new();
        for addr_str in &listener.backends {
            if let Ok(addr) = addr_str.parse() {
                instances.push(Arc::new(BackendInstance::new(addr, listener.name.clone())));
            }
        }
        
        let strategy = LbStrategy::from_str(&listener.strategy).unwrap_or(LbStrategy::RoundRobin);
        let pool = Arc::new(BackendPool::new(listener.name.clone(), instances, strategy));
        all_pools.push(pool.clone());
        let connect_timeout_ms = listener.connect_timeout_ms;
        let listen_addr = listener.listen.clone();
        
        let health_config = listener.health_check.clone();

        let metrics_clone = metrics.clone();
        let semaphore_clone = semaphore.clone();
        let cancel_token_clone = cancel_token.clone();
        let cancel_token_health = cancel_token.clone();

        let healthy_threshold = listener.health_check.healthy_threshold;
        let unhealthy_threshold = listener.health_check.unhealthy_threshold;

        if health_config.enabled {
            traffic_gateway::backend::health::start_health_check_task(pool.clone(), health_config, cancel_token_health);
        }

        tokio::spawn(async move {
            if let Err(e) = traffic_gateway::listener::run_server(
                &listen_addr,
                pool,
                connect_timeout_ms,
                healthy_threshold,
                unhealthy_threshold,
                metrics_clone,
                semaphore_clone,
                cancel_token_clone,
            )
            .await
            {
                tracing::error!("listener {} failed: {}", listener.name, e);
            }
        });
    }

    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let start_time = std::time::Instant::now();
        let mut last_tick = std::time::Instant::now();
        let mut last_total = 0;

        loop {
            interval.tick().await;
            let now = std::time::Instant::now();
            let elapsed_secs = (now - last_tick).as_secs().max(1);
            let uptime_secs = (now - start_time).as_secs();

            let active = metrics_clone.active_connections.load(Ordering::Relaxed);
            let total = metrics_clone.total_connections.load(Ordering::Relaxed);
            let peak = metrics_clone.peak_active_connections.load(Ordering::Relaxed);
            let errors = metrics_clone.connection_errors.load(Ordering::Relaxed);

            let cps = (total.saturating_sub(last_total)) / elapsed_secs;

            last_tick = now;
            last_total = total;

            tracing::info!(
                "metrics [UP {}s]: active={}, peak={}, total={}, CPS={}/s, errors={}",
                uptime_secs, active, peak, total, cps, errors
            );

            for pool in &all_pools {
                let instances = pool.instances();
                let total_inst = instances.len();
                let healthy_inst = instances.iter().filter(|i| i.is_healthy()).count();
                
                let mut inst_status = String::new();
                for inst in instances {
                    let port = inst.address().port();
                    if inst.is_healthy() {
                        inst_status.push_str(&format!(" {}(H conns={} served={})", port, inst.active_connections(), inst.total_served()));
                    } else {
                        inst_status.push_str(&format!(" {}(DOWN)", port));
                    }
                }
                
                tracing::info!(
                    "pool[{}]: {}/{} healthy |{}",
                    pool.name(), healthy_inst, total_inst, inst_status
                );
            }
        }
    });

    cancel_token.cancelled().await;
    tracing::info!("gateway shut down gracefully");

    Ok(())
}
