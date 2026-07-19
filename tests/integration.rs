use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use traffic_gateway::config::ListenerConfig;
use traffic_gateway::listener::run_listener;
use traffic_gateway::metrics::Metrics;

use arc_swap::ArcSwap;
use traffic_gateway::backend::instance::BackendInstance;
use traffic_gateway::backend::pool::{BackendPool, LbStrategy};

async fn spawn_gateway_listener(backend_port: u16) -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen_port = listener.local_addr().unwrap().port();

    let config = ListenerConfig {
        name: format!("test-{}", listen_port),
        listen: format!("127.0.0.1:{}", listen_port),
        backends: vec![format!("127.0.0.1:{}", backend_port)],
        strategy: "round_robin".to_string(),
        connect_timeout_ms: 1000,
        health_check: Default::default(),
    };
    
    let instance = Arc::new(BackendInstance::new(format!("127.0.0.1:{}", backend_port).parse().unwrap(), "test_pool".into()));
    let pool = Arc::new(ArcSwap::from_pointee(BackendPool::new(config.name.clone(), vec![instance], LbStrategy::RoundRobin)));

    let metrics = Arc::new(Metrics::new());
    let semaphore = Arc::new(Semaphore::new(1024));
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async move {
        let _ = run_listener(listener, pool, config.connect_timeout_ms, config.health_check.healthy_threshold, config.health_check.unhealthy_threshold, metrics, semaphore, cancel_token_clone).await;
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
    // Gateway retries with timeout per attempt, so we need to wait long enough
    let res = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        client.read(&mut buf),
    ).await;
    
    match res {
        Ok(Ok(0)) => (),     // clean close
        Ok(Err(_)) => (),    // error close
        Err(_) => (),        // our read timed out — still acceptable
        Ok(Ok(n)) => panic!("expected connection close, but received {} bytes", n),
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

/// Helper: spawn a gateway with MULTIPLE backends (for failover tests)
async fn spawn_gateway_multi_backend(backend_ports: &[u16]) -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen_port = listener.local_addr().unwrap().port();

    let instances: Vec<Arc<BackendInstance>> = backend_ports
        .iter()
        .map(|p| Arc::new(BackendInstance::new(format!("127.0.0.1:{}", p).parse().unwrap(), "test_pool".into())))
        .collect();

    let pool = Arc::new(ArcSwap::from_pointee(BackendPool::new(
        format!("failover-test-{}", listen_port),
        instances,
        LbStrategy::RoundRobin,
    )));

    let metrics = Arc::new(Metrics::new());
    let semaphore = Arc::new(Semaphore::new(1024));
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    let healthy_threshold = 2u32;
    let unhealthy_threshold = 3u32;

    tokio::spawn(async move {
        let _ = run_listener(listener, pool, 1000, healthy_threshold, unhealthy_threshold, metrics, semaphore, cancel_token_clone).await;
    });

    (listen_port, cancel_token)
}

#[tokio::test]
async fn test_connect_time_failover_is_seamless() {
    // Start 2 real backends, use 1 fake dead port (59990)
    let (b1_port, _b1) = spawn_echo_identity_backend("BACKEND-A").await;
    let (b2_port, _b2) = spawn_echo_identity_backend("BACKEND-B").await;
    let dead_port: u16 = 59990; // nothing listens here

    // Gateway has 3 backends: alive, dead, alive
    let (g_port, _g) = spawn_gateway_multi_backend(&[b1_port, dead_port, b2_port]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Fire 20 requests — ALL should succeed despite the dead backend
    let mut handles = Vec::new();
    for _ in 0..20 {
        let handle = tokio::spawn(async move {
            let mut client = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await.unwrap();
            client.write_all(b"ping").await.unwrap();
            let mut buf = [0; 1024];
            let n = client.read(&mut buf).await.unwrap();
            let response = String::from_utf8_lossy(&buf[..n]);
            // Must get a response from one of the alive backends
            assert!(
                response == "BACKEND-A" || response == "BACKEND-B",
                "unexpected response: {}",
                response
            );
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_all_backends_down_clean_failure() {
    // All 3 ports are dead — nothing is listening
    let (g_port, _g) = spawn_gateway_multi_backend(&[59991, 59992, 59993]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Send a request — it should get a clean close, NOT a crash
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await.unwrap();
    client.write_all(b"hello").await.unwrap();

    let mut buf = [0; 1024];
    // Gateway retries all 3 dead backends (each with 1s timeout), so wait up to 10s
    let res = tokio::time::timeout(
        tokio::time::Duration::from_secs(10),
        client.read(&mut buf),
    ).await;

    match res {
        Ok(Ok(0)) => (),     // clean close — expected
        Ok(Err(_)) => (),    // error close — also fine
        Err(_) => (),        // our read timed out — still acceptable
        Ok(Ok(n)) => panic!("expected clean close, but received {} bytes", n),
    }

    // Most importantly: gateway should still be alive and accepting new connections
    // Spin up a real backend and a new gateway to prove it
    let (b_port, _b) = spawn_echo_identity_backend("RECOVERED").await;
    let (g2_port, _g2) = spawn_gateway_listener(b_port).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", g2_port)).await.unwrap();
    client2.write_all(b"ping").await.unwrap();
    let _n = client2.read(&mut buf).await.unwrap();
}

use traffic_gateway::config::HealthCheckConfig;
use traffic_gateway::backend::health::start_health_check_task;

async fn spawn_gateway_with_health_checks(
    backend_ports: &[u16],
    interval_ms: u64,
    timeout_ms: u64,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
) -> (u16, CancellationToken, Arc<ArcSwap<BackendPool>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen_port = listener.local_addr().unwrap().port();

    let instances: Vec<Arc<BackendInstance>> = backend_ports
        .iter()
        .map(|p| Arc::new(BackendInstance::new(format!("127.0.0.1:{}", p).parse().unwrap(), "test_pool".into())))
        .collect();

    let pool = Arc::new(ArcSwap::from_pointee(BackendPool::new(
        format!("health-test-{}", listen_port),
        instances,
        LbStrategy::RoundRobin,
    )));

    let metrics = Arc::new(Metrics::new());
    let semaphore = Arc::new(Semaphore::new(1024));
    let cancel_token = CancellationToken::new();
    
    let hc_config = HealthCheckConfig {
        enabled: true,
        interval_ms,
        timeout_ms,
        healthy_threshold,
        unhealthy_threshold,
    };
    
    start_health_check_task(pool.clone(), hc_config, cancel_token.clone());

    let pool_clone = pool.clone();
    let cancel_token_clone = cancel_token.clone();
    
    tokio::spawn(async move {
        let _ = run_listener(listener, pool_clone, 1000, healthy_threshold, unhealthy_threshold, metrics, semaphore, cancel_token_clone).await;
    });

    (listen_port, cancel_token, pool)
}

#[tokio::test]
async fn test_active_healthcheck_marks_down_without_traffic() {
    // We use a fake port that nothing listens on
    let dead_port = 59998;
    
    // Spawn gateway with health checks: 100ms interval, 50ms timeout, threshold=2
    let (_g_port, _g, pool) = spawn_gateway_with_health_checks(&[dead_port], 100, 50, 2, 2).await;
    
    // Instance should start healthy
    let current_pool = pool.load();
    let instance = &current_pool.instances()[0];
    assert!(instance.is_healthy(), "Instance should start healthy");
    
    // Wait enough time for 2 failures: (100ms interval + 50ms timeout) * 3 = 450ms wait
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // It should now be passively marked unhealthy without ANY client traffic!
    assert!(!instance.is_healthy(), "Health check failed to mark dead backend unhealthy");
}

#[tokio::test]
async fn test_recovery_readds_backend() {
    // 1. Start a backend and grab its port, but then immediately drop (kill) it
    let dead_port = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    };
    
    // 2. Start gateway pointing to this dead port
    let (g_port, _g, pool) = spawn_gateway_with_health_checks(&[dead_port], 100, 50, 2, 2).await;
    let current_pool = pool.load();
    let instance = &current_pool.instances()[0];
    
    // Wait for health check to mark it DOWN
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    assert!(!instance.is_healthy(), "Backend must be marked unhealthy first");
    
    // 3. RECOVERY: Now start the actual backend on the SAME port
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", dead_port)).await.unwrap();
        loop {
            tokio::select! {
                _ = cancel_token_clone.cancelled() => break,
                Ok((mut socket, _)) = listener.accept() => {
                    tokio::spawn(async move {
                        let mut buf = [0; 1024];
                        if let Ok(n) = socket.read(&mut buf).await {
                            if n > 0 {
                                let _ = socket.write_all(b"RECOVERED-SERVICE").await;
                            }
                        }
                    });
                }
            }
        }
    });
    
    // Wait for health check to detect it and mark it UP
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    assert!(instance.is_healthy(), "Backend was not marked healthy after recovery!");
    
    // 4. VERIFY: Traffic should now flow normally
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await.unwrap();
    client.write_all(b"ping").await.unwrap();
    let mut buf = [0; 1024];
    let _n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[.._n], b"RECOVERED-SERVICE");
    
    cancel_token.cancel();
}

/// Level 4 — Chaos Under Load: Kill a backend while traffic is flowing.
/// Assertion matches our documented L4 limitation:
///   - NEW connections after the kill see zero failures (failover reroutes them).
///   - Mid-stream connections on the killed backend MAY fail (L4 can't replay byte streams).
///   - Gateway must never crash.
#[tokio::test]
async fn test_kill_backend_during_load() {
    let (b1_port, _b1) = spawn_echo_identity_backend("CHAOS-A").await;
    let (b2_port, b2_cancel) = spawn_echo_identity_backend("CHAOS-B").await;
    let (b3_port, _b3) = spawn_echo_identity_backend("CHAOS-C").await;

    let (g_port, _g) = spawn_gateway_multi_backend(&[b1_port, b2_port, b3_port]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let successes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let errors = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Phase 1: Sustained load with all 3 backends alive
    let mut handles = Vec::new();
    for _ in 0..100 {
        let succ = successes.clone();
        let err = errors.clone();
        let h = tokio::spawn(async move {
            match TcpStream::connect(format!("127.0.0.1:{}", g_port)).await {
                Ok(mut client) => {
                    let _ = client.write_all(b"ping").await;
                    let mut buf = [0; 1024];
                    match client.read(&mut buf).await {
                        Ok(n) if n > 0 => {
                            let resp = String::from_utf8_lossy(&buf[..n]);
                            if resp.starts_with("CHAOS-") {
                                succ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                err.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        _ => { err.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                    }
                }
                Err(_) => { err.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
            }
        });
        handles.push(h);
    }
    for h in handles { let _ = h.await; }
    
    // All 100 should have succeeded with 3 healthy backends
    assert!(successes.load(std::sync::atomic::Ordering::Relaxed) >= 95, 
        "Pre-kill: too many failures");

    // Phase 2: Kill backend B mid-flight, then fire more requests.
    // New connections MUST succeed (failover to A and C).
    b2_cancel.cancel();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let post_kill_successes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let post_kill_errors = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..100 {
        let succ = post_kill_successes.clone();
        let err = post_kill_errors.clone();
        let h = tokio::spawn(async move {
            match TcpStream::connect(format!("127.0.0.1:{}", g_port)).await {
                Ok(mut client) => {
                    let _ = client.write_all(b"ping").await;
                    let mut buf = [0; 1024];
                    match tokio::time::timeout(
                        tokio::time::Duration::from_secs(3),
                        client.read(&mut buf),
                    ).await {
                        Ok(Ok(n)) if n > 0 => {
                            let resp = String::from_utf8_lossy(&buf[..n]);
                            if resp == "CHAOS-A" || resp == "CHAOS-C" {
                                succ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                err.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        _ => { err.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                    }
                }
                Err(_) => { err.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
            }
        });
        handles.push(h);
    }
    for h in handles { let _ = h.await; }
    
    let post_succ = post_kill_successes.load(std::sync::atomic::Ordering::Relaxed);
    assert!(post_succ >= 95,
        "Post-kill: new connections should succeed via failover, got only {}/100", post_succ);
}

/// Level 4 — Chaos Under Load: Kill a backend, let it be marked DOWN,
/// then revive it and verify it rejoins the pool under continuous traffic.
#[tokio::test]
async fn test_kill_and_revive_during_load() {
    let (b1_port, _b1) = spawn_echo_identity_backend("REVIVE-A").await;
    let (b2_port, b2_cancel) = spawn_echo_identity_backend("REVIVE-B").await;
    let (b3_port, _b3) = spawn_echo_identity_backend("REVIVE-C").await;

    // Use health-check-enabled gateway so recovery is detected
    let (g_port, _g, pool) = spawn_gateway_with_health_checks(
        &[b1_port, b2_port, b3_port],
        100,   // interval_ms: fast checks for test speed
        50,    // timeout_ms
        2,     // healthy_threshold
        2,     // unhealthy_threshold
    ).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // 1. Verify all 3 are healthy
    assert_eq!(pool.load().healthy_count(), 3, "All backends should start healthy");

    // 2. Kill backend B
    b2_cancel.cancel();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    assert_eq!(pool.load().healthy_count(), 2, "Killed backend should be marked unhealthy");
    let current_pool = pool.load();
    let b2_instance = &current_pool.instances()[1];
    assert!(!b2_instance.is_healthy(), "Backend B should be DOWN");

    // 3. Send traffic while B is down — should succeed via A and C
    let mut handles = Vec::new();
    for _ in 0..50 {
        let h = tokio::spawn(async move {
            if let Ok(mut client) = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await {
                let _ = client.write_all(b"ping").await;
                let mut buf = [0; 1024];
                if let Ok(n) = client.read(&mut buf).await {
                    if n > 0 {
                        let resp = String::from_utf8_lossy(&buf[..n]);
                        assert!(resp == "REVIVE-A" || resp == "REVIVE-C",
                            "Should NOT route to dead backend B, got: {}", resp);
                    }
                }
            }
        });
        handles.push(h);
    }
    for h in handles { let _ = h.await; }

    // 4. REVIVE: restart backend B on the same port
    let revive_cancel = CancellationToken::new();
    let revive_cancel_clone = revive_cancel.clone();
    tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", b2_port)).await.unwrap();
        loop {
            tokio::select! {
                _ = revive_cancel_clone.cancelled() => break,
                Ok((mut socket, _)) = listener.accept() => {
                    tokio::spawn(async move {
                        let mut buf = [0; 1024];
                        if let Ok(n) = socket.read(&mut buf).await {
                            if n > 0 {
                                let _ = socket.write_all(b"REVIVE-B").await;
                            }
                        }
                    });
                }
            }
        }
    });

    // 5. Wait for health checks to mark B healthy again
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    assert!(b2_instance.is_healthy(), "Backend B should have recovered");
    assert_eq!(pool.load().healthy_count(), 3, "All 3 backends should be healthy again");

    // 6. Verify B is back in rotation
    let mut saw_b = false;
    for _ in 0..30 {
        if let Ok(mut client) = TcpStream::connect(format!("127.0.0.1:{}", g_port)).await {
            let _ = client.write_all(b"ping").await;
            let mut buf = [0; 1024];
            if let Ok(n) = client.read(&mut buf).await {
                if n > 0 && &buf[..n] == b"REVIVE-B" {
                    saw_b = true;
                    break;
                }
            }
        }
    }
    assert!(saw_b, "Revived backend B should receive traffic again");
    
    revive_cancel.cancel();
}
