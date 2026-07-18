use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let name = args.iter().position(|a| a == "--name").and_then(|i| args.get(i + 1)).cloned().unwrap_or_else(|| "SERVICE-A".to_string());
    let port = args.iter().position(|a| a == "--port").and_then(|i| args.get(i + 1)).cloned().unwrap_or_else(|| "9001".to_string());

    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    println!("Identity Backend '{}' listening on {}", name, addr);

    loop {
        if let Ok((mut socket, _)) = listener.accept().await {
            let name_clone = name.clone();
            tokio::spawn(async move {
                let mut buf = [0; 1024];
                // Wait for the client to send something first
                if let Ok(n) = socket.read(&mut buf).await {
                    if n > 0 {
                        let _ = socket.write_all(name_clone.as_bytes()).await;
                    }
                }
            });
        }
    }
}
