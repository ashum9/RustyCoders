use crate::metrics::Metrics;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{debug, error, info};

pub struct Connection {
    client: TcpStream,
    backend: TcpStream,
    client_addr: SocketAddr,
    listener_name: String,
    metrics: Arc<Metrics>,
}

impl Connection {
    pub fn new(
        client: TcpStream,
        backend: TcpStream,
        client_addr: SocketAddr,
        listener_name: String,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            client,
            backend,
            client_addr,
            listener_name,
            metrics,
        }
    }

    pub async fn process(self) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let client_addr = self.client_addr;
        let metrics = self.metrics.clone();

        metrics.inc_active_connection();

        let (mut cr, mut cw) = self.client.into_split();
        let (mut br, mut bw) = self.backend.into_split();

        let c2b = tokio::spawn(async move {
            let res = tokio::io::copy(&mut cr, &mut bw).await;
            let _ = bw.shutdown().await;
            res
        });

        let b2c = tokio::spawn(async move {
            let res = tokio::io::copy(&mut br, &mut cw).await;
            let _ = cw.shutdown().await;
            res
        });

        let (c2b_res, b2c_res) = tokio::join!(c2b, b2c);

        metrics.dec_active_connection();

        let client_to_backend_bytes = match c2b_res {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => {
                debug!("error copying from client to backend: {}", e);
                0
            }
            Err(e) => {
                error!("task c2b panicked or was cancelled: {}", e);
                0
            }
        };

        let backend_to_client_bytes = match b2c_res {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => {
                debug!("error copying from backend to client: {}", e);
                0
            }
            Err(e) => {
                error!("task b2c panicked or was cancelled: {}", e);
                0
            }
        };

        metrics.add_bytes(client_to_backend_bytes + backend_to_client_bytes);

        let duration = start_time.elapsed();

        info!(
            "[{}] connection closed. peer: {}, client->backend: {} bytes, backend->client: {} bytes, duration: {:?}",
            self.listener_name, client_addr, client_to_backend_bytes, backend_to_client_bytes, duration
        );

        Ok(())
    }
}
