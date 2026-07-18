use crate::backend::pool::BackendPool;
use crate::backend::instance::ConnectionGuard;
use crate::metrics::Metrics;
use crate::proxy::connection::Connection;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub async fn run_server(
    listen_addr: &str,
    pool: Arc<BackendPool>,
    connect_timeout_ms: u64,
    metrics: Arc<Metrics>,
    semaphore: Arc<Semaphore>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(listen_addr.parse()?)?;
    let listener = socket.listen(1024)?;
    info!("[{}] listening on {} with {} backends", pool.name(), listen_addr, pool.instances().len());
    run_listener(listener, pool, connect_timeout_ms, metrics, semaphore, cancel_token).await
}

pub async fn run_listener(
    listener: TcpListener,
    pool: Arc<BackendPool>,
    connect_timeout_ms: u64,
    metrics: Arc<Metrics>,
    semaphore: Arc<Semaphore>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("[{}] listener shutting down", pool.name());
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((client_stream, client_addr)) => {
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => {
                                info!("[{}] semaphore closed, shutting down", pool.name());
                                break;
                            }
                        };

                        let metrics_clone = metrics.clone();
                        let pool_clone = pool.clone();
                        let listener_name = pool.name().to_string();

                        tokio::spawn(async move {
                            let _permit = permit;
                            
                            let instance = match pool_clone.select() {
                                Some(inst) => inst,
                                None => {
                                    error!("[{}] no healthy backends available", pool_clone.name());
                                    metrics_clone.inc_error();
                                    return;
                                }
                            };

                            let guard = ConnectionGuard::new(instance.clone());
                            let timeout_duration = Duration::from_millis(connect_timeout_ms);
                            
                            let start = std::time::Instant::now();
                            match tokio::time::timeout(timeout_duration, TcpStream::connect(instance.address())).await {
                                Ok(Ok(backend_stream)) => {
                                    metrics_clone.add_latency(start.elapsed().as_micros() as u64);
                                    let connection = Connection::new(
                                        client_stream,
                                        backend_stream,
                                        client_addr,
                                        listener_name,
                                        metrics_clone.clone(),
                                        guard,
                                    );

                                    if let Err(e) = connection.process().await {
                                        warn!(peer = %client_addr, error = %e, "connection ended with error");
                                        metrics_clone.inc_error();
                                    }
                                }
                                Ok(Err(e)) => {
                                    error!("[{}] failed to connect to backend {}: {}", pool_clone.name(), instance.address(), e);
                                    metrics_clone.inc_error();
                                }
                                Err(_) => {
                                    error!("[{}] connection to backend {} timed out", pool_clone.name(), instance.address());
                                    metrics_clone.inc_error();
                                }
                            }
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
