use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::proxy;

pub async fn run_single_connection_listener(
    bind_addr: &str,
    backend_addr: &str,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("listening on {}", bind_addr);

    match listener.accept().await {
        Ok((client_stream, client_addr)) => {
            info!("accepted connection from client: {}", client_addr);

            match TcpStream::connect(backend_addr).await {
                Ok(backend_stream) => {
                    info!("connected to backend at {}", backend_addr);
                    proxy::forward(client_stream, backend_stream, client_addr).await;
                }
                Err(e) => {
                    error!("failed to connect to backend {}: {}", backend_addr, e);
                }
            }
        }
        Err(e) => {
            error!("failed to accept client connection: {}", e);
        }
    }

    Ok(())
}
