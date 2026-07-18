use std::env;
use tokio::io;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let port = args.iter().position(|a| a == "--port")
        .and_then(|i| args.get(i + 1))
        .or_else(|| args.get(1)) // Fallback to the first argument passed
        .cloned()
        .unwrap_or_else(|| "9001".to_string());

    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    println!("Pure Echo Backend listening on {}", addr);

    loop {
        if let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = socket.split();
                let _ = io::copy(&mut r, &mut w).await;
            });
        }
    }
}
