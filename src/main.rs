mod listener;
mod proxy;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    tracing::info!("gateway starting...");

    let bind_addr = "127.0.0.1:8080";
    let backend_addr = "127.0.0.1:9000";

    listener::run_single_connection_listener(bind_addr, backend_addr).await?;

    Ok(())
}
