use crate::metrics::Metrics;
use crate::proxy::connection::Connection;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

pub async fn run_server(
    bind_addr: &str,
    backend_addr: &str,
    metrics: Arc<Metrics>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("listening on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((client_stream, client_addr)) => {
                let backend_addr_owned = backend_addr.to_string();
                let metrics_clone = metrics.clone();

                tokio::spawn(async move {
                    match TcpStream::connect(&backend_addr_owned).await {
                        Ok(backend_stream) => {
                            let connection = Connection::new(
                                client_stream,
                                backend_stream,
                                client_addr,
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
