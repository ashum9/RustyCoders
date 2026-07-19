use crate::backend::pool::BackendPool;
use crate::config::HealthCheckConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{info, debug};

pub fn start_health_check_task(
    pool: Arc<BackendPool>,
    config: HealthCheckConfig,
    cancel_token: CancellationToken,
) {
    if !config.enabled {
        info!("[{}] health checks disabled", pool.name());
        return;
    }

    let interval_ms = config.interval_ms;
    let timeout_ms = config.timeout_ms;
    let healthy_threshold = config.healthy_threshold;
    let unhealthy_threshold = config.unhealthy_threshold;

    info!(
        "[{}] starting active health checks: interval={}ms, timeout={}ms",
        pool.name(), interval_ms, timeout_ms
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        
        // Note: Limitation - L4 TCP connect probe cannot detect "black-hole" backends
        // that accept connections but do not respond (e.g. frozen application threads).
        // For advanced protection, an L7 probe or byte-exchange probe would be required.

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut probe_handles = Vec::new();
                    
                    // Concurrently probe all instances so one slow backend doesn't delay others
                    for instance in pool.instances() {
                        let instance_clone = instance.clone();
                        let timeout_duration = Duration::from_millis(timeout_ms);
                        
                        let handle = tokio::spawn(async move {
                            match tokio::time::timeout(timeout_duration, TcpStream::connect(instance_clone.address())).await {
                                Ok(Ok(stream)) => {
                                    // Connection successful, backend is alive.
                                    // Drop the stream immediately to minimize load.
                                    drop(stream);
                                    instance_clone.record_success(healthy_threshold);
                                    debug!("health check success for {}", instance_clone.address());
                                }
                                Ok(Err(_e)) => {
                                    // Connection refused or other IO error
                                    instance_clone.record_failure(unhealthy_threshold);
                                    debug!("health check failed for {}", instance_clone.address());
                                }
                                Err(_) => {
                                    // Timeout reached
                                    instance_clone.record_failure(unhealthy_threshold);
                                    debug!("health check timeout for {}", instance_clone.address());
                                }
                            }
                        });
                        probe_handles.push(handle);
                    }
                    
                    // Wait for all probes in this round to finish
                    for handle in probe_handles {
                        let _ = handle.await;
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("[{}] health check task shutting down", pool.name());
                    break;
                }
            }
        }
    });
}
