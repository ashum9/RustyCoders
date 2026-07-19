use crate::backend::instance::BackendInstance;
use crate::backend::pool::{BackendPool, LbStrategy};
use crate::config::GatewayConfig;

use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Reload the gateway config from disk and atomically swap backend pools.
///
/// Safety guarantees:
/// - If the config file cannot be read, the old config is kept.
/// - If the config is invalid, the old config is kept.
/// - Existing connections are never dropped — they hold their own pool reference.
/// - New connections immediately use the new pool after swap.
/// - Health check tasks are restarted to probe the new backends.
pub fn reload_config(
    config_path: &str,
    pools: &HashMap<String, Arc<ArcSwap<BackendPool>>>,
    health_tokens: &mut HashMap<String, CancellationToken>,
    parent_cancel: &CancellationToken,
) {
    // Step 1: Read and validate config (reuse existing validation)
    let config = match GatewayConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("reload FAILED: {e} — keeping old config");
            return;
        }
    };

    // Step 2: For each listener in the new config, find and swap matching pool
    let mut reloaded = 0;
    for listener in &config.listeners {
        let pool_swap = match pools.get(&listener.name) {
            Some(p) => p,
            None => {
                tracing::warn!(
                    "reload: listener '{}' not found in running pools (adding new listeners requires restart)",
                    listener.name
                );
                continue;
            }
        };

        // Build new instances
        let mut instances = Vec::new();
        for addr_str in &listener.backends {
            match addr_str.parse() {
                Ok(addr) => {
                    instances.push(Arc::new(BackendInstance::new(addr, listener.name.clone())));
                }
                Err(e) => {
                    // Should not happen since validate() already checked, but be safe
                    tracing::error!(
                        "reload: invalid backend address '{}' for '{}': {} — skipping this listener",
                        addr_str, listener.name, e
                    );
                    continue;
                }
            }
        }

        let strategy = LbStrategy::from_str(&listener.strategy).unwrap_or(LbStrategy::RoundRobin);
        let new_pool = BackendPool::new(listener.name.clone(), instances, strategy.clone());
        let backend_count = new_pool.instances().len();

        // Atomic swap — in-flight connections keep their old pool reference
        pool_swap.store(Arc::new(new_pool));

        tracing::info!(
            "reloaded pool for '{}': {} backends, strategy={:?}",
            listener.name, backend_count, strategy
        );

        // Restart health check task: cancel old, start new
        if let Some(old_token) = health_tokens.remove(&listener.name) {
            old_token.cancel();
        }

        if listener.health_check.enabled {
            let new_token = parent_cancel.child_token();
            health_tokens.insert(listener.name.clone(), new_token.clone());
            crate::backend::health::start_health_check_task(
                pool_swap.clone(),
                listener.health_check.clone(),
                new_token,
            );
        }

        reloaded += 1;
    }

    if reloaded > 0 {
        tracing::info!("config reloaded successfully ({} listener(s) updated)", reloaded);
    } else {
        tracing::warn!("reload completed but no listeners were updated");
    }
}
