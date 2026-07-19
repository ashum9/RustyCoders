use traffic_gateway::backend::instance::BackendInstance;
use traffic_gateway::backend::pool::{BackendPool, LbStrategy};
use traffic_gateway::config::GatewayConfig;
use traffic_gateway::metrics::Metrics;
use traffic_gateway::reload;

use arc_swap::ArcSwap;
use std::collections::HashMap;
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
    let start_time = std::time::Instant::now();

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

    // Pools map for hot-reload: listener_name -> Arc<ArcSwap<BackendPool>>
    let mut pools_map: HashMap<String, Arc<ArcSwap<BackendPool>>> = HashMap::new();
    let mut listener_addrs: HashMap<String, String> = HashMap::new();
    // Health check cancel tokens for restart on reload
    let mut health_tokens: HashMap<String, CancellationToken> = HashMap::new();
    let mut all_pools: Vec<Arc<ArcSwap<BackendPool>>> = Vec::new();

    for listener in config.listeners {
        tracing::info!("  - [{}]: {} -> {:?}", listener.name, listener.listen, listener.backends);

        let mut instances = Vec::new();
        for addr_str in &listener.backends {
            if let Ok(addr) = addr_str.parse() {
                instances.push(Arc::new(BackendInstance::new(addr, listener.name.clone())));
            }
        }
        
        let strategy = LbStrategy::from_str(&listener.strategy).unwrap_or(LbStrategy::RoundRobin);
        let pool = Arc::new(ArcSwap::from_pointee(BackendPool::new(listener.name.clone(), instances, strategy)));
        pools_map.insert(listener.name.clone(), pool.clone());
        listener_addrs.insert(listener.name.clone(), listener.listen.clone());
        all_pools.push(pool.clone());
        let connect_timeout_ms = listener.connect_timeout_ms;
        let listen_addr = listener.listen.clone();
        
        let health_config = listener.health_check.clone();

        let metrics_clone = metrics.clone();
        let semaphore_clone = semaphore.clone();
        let cancel_token_clone = cancel_token.clone();
        let cancel_token_health = cancel_token.child_token();

        let healthy_threshold = listener.health_check.healthy_threshold;
        let unhealthy_threshold = listener.health_check.unhealthy_threshold;

        if health_config.enabled {
            health_tokens.insert(listener.name.clone(), cancel_token_health.clone());
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

    // Admin dashboard server
    traffic_gateway::admin::start_admin_server(
        config.global.admin_listen.clone(),
        metrics.clone(),
        pools_map.clone(),
        listener_addrs,
        start_time,
        cancel_token.clone(),
    );

    // SIGHUP handler for config hot-reload
    let pools_for_reload = pools_map.clone();
    let health_tokens_reload = Arc::new(tokio::sync::Mutex::new(health_tokens));
    let cancel_token_for_reload = cancel_token.clone();
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sig = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to register SIGHUP handler: {}", e);
                return;
            }
        };
        loop {
            sig.recv().await;
            tracing::info!("SIGHUP received — reloading config...");
            let mut tokens = health_tokens_reload.lock().await;
            reload::reload_config(
                config_path,
                &pools_for_reload,
                &mut tokens,
                &cancel_token_for_reload,
            );
        }
    });

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
                let current_pool = pool.load();
                let instances = current_pool.instances();
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
                    current_pool.name(), healthy_inst, total_inst, inst_status
                );
            }
        }
    });

    cancel_token.cancelled().await;
    tracing::info!("gateway shut down gracefully");

    Ok(())
}
