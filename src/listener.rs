use crate::backend::pool::BackendPool;
use crate::backend::instance::ConnectionGuard;
use crate::metrics::Metrics;
use crate::proxy::connection::Connection;
use arc_swap::ArcSwap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub async fn run_server(
    listen_addr: &str,
    pool: Arc<ArcSwap<BackendPool>>,
    connect_timeout_ms: u64,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
    metrics: Arc<Metrics>,
    semaphore: Arc<Semaphore>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(listen_addr.parse()?)?;
    let listener = socket.listen(1024)?;
    let current_pool = pool.load();
    info!("[{}] listening on {} with {} backends", current_pool.name(), listen_addr, current_pool.instances().len());
    run_listener(listener, pool, connect_timeout_ms, healthy_threshold, unhealthy_threshold, metrics, semaphore, cancel_token).await
}

pub async fn run_listener(
    listener: TcpListener,
    pool: Arc<ArcSwap<BackendPool>>,
    connect_timeout_ms: u64,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
    metrics: Arc<Metrics>,
    semaphore: Arc<Semaphore>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("[{}] listener shutting down", pool.load().name());
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((client_stream, client_addr)) => {
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => {
                                info!("[{}] semaphore closed, shutting down", pool.load().name());
                                break;
                            }
                        };

                        let metrics_clone = metrics.clone();
                        let pool_clone = pool.clone();

                        tokio::spawn(async move {
                            let _permit = permit;
                            let timeout_duration = Duration::from_millis(connect_timeout_ms);
                            // Load the current pool snapshot for this connection
                            let current_pool = pool_clone.load();
                            let listener_name = current_pool.name().to_string();
                            let max_retries = current_pool.instances().len();
                            let mut tried = std::collections::HashSet::new();

                            // --- Retry Loop: try different backends until one succeeds ---
                            for attempt in 0..max_retries {
                                let instance = match current_pool.select() {
                                    Some(inst) => inst,
                                    None => {
                                        // No healthy backends left at all
                                        error!("[{}] no healthy backends available", listener_name);
                                        metrics_clone.inc_error();
                                        return;
                                    }
                                };

                                // Skip if we already tried this exact address
                                if !tried.insert(instance.address()) {
                                    continue;
                                }

                                // RAII guard: created inside the loop, dropped automatically on failure
                                let guard = ConnectionGuard::new(instance.clone());
                                let start = std::time::Instant::now();

                                match tokio::time::timeout(timeout_duration, TcpStream::connect(instance.address())).await {
                                    Ok(Ok(backend_stream)) => {
                                        // SUCCESS — record it, build the connection, and break out
                                        instance.record_success(healthy_threshold);
                                        metrics_clone.add_latency(start.elapsed().as_micros() as u64);
                                        let backend_addr = backend_stream.peer_addr().unwrap_or_else(|_| instance.address());
                                        let connection = Connection::new(
                                            client_stream,
                                            backend_stream,
                                            client_addr,
                                            backend_addr,
                                            listener_name,
                                            metrics_clone.clone(),
                                            guard,
                                        );

                                        if let Err(e) = connection.process().await {
                                            warn!(peer = %client_addr, error = %e, "connection ended with error");
                                            metrics_clone.inc_error();
                                        }
                                        return; // Done — successfully proxied
                                    }
                                    Ok(Err(e)) => {
                                        // Connection refused / error — mark failure and try next
                                        instance.record_failure(unhealthy_threshold);
                                        warn!(
                                            "[{}] failover: {} failed (attempt {}/{}): {}, trying next backend",
                                            listener_name, instance.address(), attempt + 1, max_retries, e
                                        );
                                        // guard is dropped here automatically (RAII), decrementing active count
                                    }
                                    Err(_) => {
                                        // Timeout — mark failure and try next
                                        instance.record_failure(unhealthy_threshold);
                                        warn!(
                                            "[{}] failover: {} timed out (attempt {}/{}), trying next backend",
                                            listener_name, instance.address(), attempt + 1, max_retries
                                        );
                                        // guard is dropped here automatically (RAII)
                                    }
                                }
                            }

                            // All retries exhausted — no backend could serve this client
                            error!(
                                "[{}] all {} backends failed for client {}, dropping connection",
                                listener_name, max_retries, client_addr
                            );
                            metrics_clone.inc_error();
                        });
                    }
                    Err(e) => {
                        error!("failed to accept client connection: {}", e);
                    }
                }
            }
        }
    }
    
    Ok(())
}
