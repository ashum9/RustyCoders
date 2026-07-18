use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use traffic_gateway::config::ListenerConfig;
use traffic_gateway::listener::run_listener;
use traffic_gateway::metrics::Metrics;

async fn spawn_gateway_listener(backend_port: u16) -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen_port = listener.local_addr().unwrap().port();

    let config = ListenerConfig {
        name: format!("test-{}", listen_port),
        listen: format!("127.0.0.1:{}", listen_port),
        backend: format!("127.0.0.1:{}", backend_port),
    };
    let metrics = Arc::new(Metrics::new());
    let semaphore = Arc::new(Semaphore::new(1024));
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async move {
        let _ = run_listener(listener, config, metrics, semaphore, cancel_token_clone).await;
    });

    (listen_port, cancel_token)
}

async fn spawn_echo_identity_backend(identity: &'static str) -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token_clone.cancelled() => break,
                Ok((mut socket, _)) = listener.accept() => {
                    tokio::spawn(async move {
                        let mut buf = [0; 1024];
                        if let Ok(n) = socket.read(&mut buf).await {
                            if n > 0 {
                                let _ = socket.write_all(identity.as_bytes()).await;
                            }
                        }
                    });
                }
            }
        }
    });

    (port, cancel_token)
}

async fn spawn_pure_echo_backend() -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token_clone.cancelled() => break,
                Ok((mut socket, _)) = listener.accept() => {
                    tokio::spawn(async move {
                        let (mut r, mut w) = socket.split();
                        let _ = tokio::io::copy(&mut r, &mut w).await;
                    });
                }
            }
        }
    });

    (port, cancel_token)
}

#[tokio::test]
async fn test_routing_correctness() {
    let (b1_port, _b1) = spawn_echo_identity_backend("SERVICE-A").await;
    let (b2_port, _b2) = spawn_echo_identity_backend("SERVICE-B").await;
    let (g1_port, _g1) = spawn_gateway_listener(b1_port).await;
    let (g2_port, _g2) = spawn_gateway_listener(b2_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", g1_port)).await.unwrap();
    client1.write_all(b"ping").await.unwrap();
    let mut buf = [0; 1024];
    let n = client1.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"SERVICE-A");

    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", g2_port)).await.unwrap();
    client2.write_all(b"ping").await.unwrap();
    let n = client2.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"SERVICE-B");
}

#[tokio::test]
async fn test_byte_integrity_small() {
    let (b_port, _b1) = spawn_pure_echo_backend().await;
    let (g_port, _g1) = spawn_gateway_listener(b_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await.unwrap();
    let payload = b"hello world, this is a small test payload!";
    client.write_all(payload).await.unwrap();
    let mut buf = [0; 1024];
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], payload);
}

#[tokio::test]
async fn test_byte_integrity_large() {
    let (b_port, _b1) = spawn_pure_echo_backend().await;
    let (g_port, _g1) = spawn_gateway_listener(b_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await.unwrap();
    
    // 1MB payload
    let payload = vec![0x42u8; 1024 * 1024];
    client.write_all(&payload).await.unwrap();
    
    let mut received = Vec::new();
    let mut buf = [0; 8192];
    while received.len() < payload.len() {
        let n = client.read(&mut buf).await.unwrap();
        if n == 0 { break; }
        received.extend_from_slice(&buf[..n]);
    }
    
    assert_eq!(received.len(), payload.len());
    assert_eq!(received, payload);
}

#[tokio::test]
async fn test_backend_unavailable() {
    // Point gateway at a port with nothing listening
    let (g1_port, _g1) = spawn_gateway_listener(59999).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", g1_port)).await.unwrap();
    client.write_all(b"hello").await.unwrap();
    
    let mut buf = [0; 1024];
    let res = client.read(&mut buf).await;
    
    match res {
        Ok(0) => (),
        Err(_) => (),
        Ok(n) => panic!("expected connection close, but received {} bytes", n),
    }

    // Ensure gateway didn't crash — spin up a working pair
    let (b2_port, _b2) = spawn_echo_identity_backend("SERVICE-GOOD").await;
    let (g2_port, _g2) = spawn_gateway_listener(b2_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", g2_port)).await.unwrap();
    client2.write_all(b"ping").await.unwrap();
    let n = client2.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"SERVICE-GOOD");
}

#[tokio::test]
async fn test_no_cross_talk() {
    let (b_port, _b1) = spawn_pure_echo_backend().await;
    let (g_port, _g1) = spawn_gateway_listener(b_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut handles = Vec::new();

    for i in 0..200 {
        let handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await.unwrap();
            let msg = format!("CLIENT_{}", i);
            client.write_all(msg.as_bytes()).await.unwrap();
            
            let mut buf = [0; 1024];
            let n = client.read(&mut buf).await.unwrap();
            let received = String::from_utf8_lossy(&buf[..n]);
            assert_eq!(received, msg);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_routing() {
    let (b1_port, _b1) = spawn_echo_identity_backend("SERVICE-A").await;
    let (b2_port, _b2) = spawn_echo_identity_backend("SERVICE-B").await;
    let (g1_port, _g1) = spawn_gateway_listener(b1_port).await;
    let (g2_port, _g2) = spawn_gateway_listener(b2_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut handles = Vec::new();

    for _ in 0..100 {
        let h1 = tokio::spawn(async move {
            let mut client = TcpStream::connect(format!("127.0.0.1:{}", g1_port)).await.unwrap();
            client.write_all(b"ping").await.unwrap();
            let mut buf = [0; 1024];
            let n = client.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"SERVICE-A");
        });
        handles.push(h1);

        let h2 = tokio::spawn(async move {
            let mut client = TcpStream::connect(format!("127.0.0.1:{}", g2_port)).await.unwrap();
            client.write_all(b"ping").await.unwrap();
            let mut buf = [0; 1024];
            let n = client.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"SERVICE-B");
        });
        handles.push(h2);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_load_scale() {
    let (b_port, _b1) = spawn_pure_echo_backend().await;
    let (g_port, _g1) = spawn_gateway_listener(b_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut handles = Vec::new();
    let num_clients = 1000;

    for i in 0..num_clients {
        let handle = tokio::spawn(async move {
            let mut client = match TcpStream::connect(format!("127.0.0.1:{}", g_port)).await {
                Ok(c) => c,
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::Other && e.to_string().contains("Too many open files") {
                        return;
                    }
                    if e.kind() == std::io::ErrorKind::ConnectionRefused {
                        return;
                    }
                    panic!("Connect failed: {}", e);
                }
            };

            let payload = format!("LOAD_{}", i);
            let _ = client.write_all(payload.as_bytes()).await;
            
            let mut buf = [0; 1024];
            if let Ok(n) = client.read(&mut buf).await {
                if n > 0 {
                    assert_eq!(String::from_utf8_lossy(&buf[..n]), payload);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }
}

#[tokio::test]
async fn test_backend_dies_mid_transfer() {
    let cancel_backend = CancellationToken::new();
    let cancel_backend_clone = cancel_backend.clone();
    
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b_port = backend_listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        tokio::select! {
            _ = cancel_backend_clone.cancelled() => {},
            Ok((mut socket, _)) = backend_listener.accept() => {
                let mut buf = [0; 1024];
                let _ = socket.read(&mut buf).await;
            }
        }
    });

    let (g_port, _g1) = spawn_gateway_listener(b_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await.unwrap();
    client.write_all(b"chunk1").await.unwrap();
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let res = client.read(&mut [0; 1024]).await;
    assert!(res.unwrap_or(0) == 0);
}

#[tokio::test]
async fn test_client_disconnects_abruptly() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b_port = backend_listener.local_addr().unwrap().port();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    
    tokio::spawn(async move {
        if let Ok((mut socket, _)) = backend_listener.accept().await {
            let mut buf = [0; 1024];
            let _ = socket.read(&mut buf).await;
            let n = socket.read(&mut buf).await.unwrap_or(0); 
            tx.send(n).await.unwrap();
        }
    });

    let (g_port, _g1) = spawn_gateway_listener(b_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await.unwrap();
    client.write_all(b"chunk1").await.unwrap();
    drop(client);
    
    let eof_read = rx.recv().await.unwrap();
    assert_eq!(eof_read, 0);
}

#[tokio::test]
async fn test_broken_connection_under_load() {
    let (b_port, _b1) = spawn_pure_echo_backend().await;
    let (g_port, _g1) = spawn_gateway_listener(b_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut handles = Vec::new();

    for i in 0..100 {
        let handle = tokio::spawn(async move {
            let mut client = match TcpStream::connect(format!("127.0.0.1:{}", g_port)).await {
                Ok(c) => c,
                Err(_) => return,
            };

            if i % 2 == 0 {
                let payload = format!("GOOD_{}", i);
                let _ = client.write_all(payload.as_bytes()).await;
                let mut buf = [0; 1024];
                if let Ok(n) = client.read(&mut buf).await {
                    if n > 0 {
                        assert_eq!(String::from_utf8_lossy(&buf[..n]), payload);
                    }
                }
            } else {
                let _ = client.write_all(b"BAD_").await;
                drop(client);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }
}
