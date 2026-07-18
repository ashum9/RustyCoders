use crate::config::ListenerConfig;
use crate::metrics::Metrics;
use crate::proxy::connection::Connection;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub async fn run_server(
    config: ListenerConfig,
    metrics: Arc<Metrics>,
    semaphore: Arc<Semaphore>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(config.listen.parse()?)?;
    let listener = socket.listen(1024)?;
    info!("[{}] listening on {} -> {}", config.name, config.listen, config.backend);
    run_listener(listener, config, metrics, semaphore, cancel_token).await
}

pub async fn run_listener(
    listener: TcpListener,
    config: ListenerConfig,
    metrics: Arc<Metrics>,
    semaphore: Arc<Semaphore>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("[{}] listener shutting down", config.name);
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((client_stream, client_addr)) => {
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => {
                                info!("[{}] semaphore closed, shutting down", config.name);
                                break;
                            }
                        };

                        let backend_addr_owned = config.backend.clone();
                        let metrics_clone = metrics.clone();
                        let listener_name = config.name.clone();

                        tokio::spawn(async move {
                            let _permit = permit;
                            
                            match TcpStream::connect(&backend_addr_owned).await {
                                Ok(backend_stream) => {
                                    let connection = Connection::new(
                                        client_stream,
                                        backend_stream,
                                        client_addr,
                                        listener_name,
                                        metrics_clone,
                                    );

                                    if let Err(e) = connection.process().await {
                                        warn!(peer = %client_addr, error = %e, "connection ended with error");
                                    }
                                }
                                Err(e) => {
                                    error!("failed to connect to backend {}: {}", backend_addr_owned, e);
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
