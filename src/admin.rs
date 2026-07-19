use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::backend::pool::BackendPool;
use crate::metrics::Metrics;

const DASHBOARD_HTML: &str = include_str!("dashboard.html");

/// Lightweight admin HTTP server — serves the dashboard and JSON stats.
/// Completely isolated from the data path.
pub fn start_admin_server(
    listen_addr: String,
    metrics: Arc<Metrics>,
    pools_map: HashMap<String, Arc<ArcSwap<BackendPool>>>,
    listener_addrs: HashMap<String, String>,
    start_time: Instant,
    cancel_token: CancellationToken,
) {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&listen_addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("admin server failed to bind on {}: {}", listen_addr, e);
                return;
            }
        };
        tracing::info!("admin dashboard live at http://{}", listen_addr);

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("admin server shutting down");
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((mut stream, _)) => {
                            let metrics = metrics.clone();
                            let pools_map = pools_map.clone();
                            let listener_addrs = listener_addrs.clone();
                            tokio::spawn(async move {
                                let mut buf = [0u8; 1024];
                                let n = match stream.read(&mut buf).await {
                                    Ok(n) if n > 0 => n,
                                    _ => return,
                                };
                                let request = String::from_utf8_lossy(&buf[..n]);
                                let first_line = request.lines().next().unwrap_or("");

                                let (status, content_type, body) = if first_line.starts_with("GET /api/stats") {
                                    let json = build_stats_json(&metrics, &pools_map, &listener_addrs, start_time);
                                    ("200 OK", "application/json", json)
                                } else if first_line.starts_with("GET / ") || first_line.starts_with("GET / HTTP") {
                                    ("200 OK", "text/html; charset=utf-8", DASHBOARD_HTML.to_string())
                                } else {
                                    ("404 Not Found", "text/plain", "not found".to_string())
                                };

                                let response = format!(
                                    "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{}",
                                    status,
                                    content_type,
                                    body.len(),
                                    body
                                );
                                let _ = stream.write_all(response.as_bytes()).await;
                            });
                        }
                        Err(e) => {
                            tracing::warn!("admin server accept error: {}", e);
                        }
                    }
                }
            }
        }
    });
}

fn build_stats_json(
    metrics: &Metrics,
    pools_map: &HashMap<String, Arc<ArcSwap<BackendPool>>>,
    listener_addrs: &HashMap<String, String>,
    start_time: Instant,
) -> String {
    let uptime = start_time.elapsed().as_secs();
    let active = metrics.active_connections.load(Ordering::Relaxed);
    let total = metrics.total_connections.load(Ordering::Relaxed);
    let peak = metrics.peak_active_connections.load(Ordering::Relaxed);
    let bytes_rx = metrics.bytes_rx.load(Ordering::Relaxed);
    let bytes_tx = metrics.bytes_tx.load(Ordering::Relaxed);
    let errors = metrics.connection_errors.load(Ordering::Relaxed);

    // Approximate CPS: total / uptime (simple but avoids extra state)
    let cps = if uptime > 0 { total / uptime } else { 0 };

    // Approximate throughput: bytes / uptime
    let throughput_rx = if uptime > 0 { bytes_rx / uptime } else { 0 };
    let throughput_tx = if uptime > 0 { bytes_tx / uptime } else { 0 };

    let mut pools_json = Vec::new();
    for (name, pool_swap) in pools_map {
        let pool = pool_swap.load();
        let listen = listener_addrs.get(name).cloned().unwrap_or_default();

        let mut backends_json = Vec::new();
        for inst in pool.instances() {
            backends_json.push(serde_json::json!({
                "address": inst.address().to_string(),
                "status": if inst.is_healthy() { "healthy" } else { "down" },
                "active_connections": inst.active_connections(),
                "total_served": inst.total_served(),
            }));
        }

        pools_json.push(serde_json::json!({
            "name": name,
            "listen": listen,
            "strategy": pool.strategy_name(),
            "healthy_count": pool.healthy_count(),
            "total_count": pool.instances().len(),
            "backends": backends_json,
        }));
    }

    let stats = serde_json::json!({
        "uptime_seconds": uptime,
        "active_connections": active,
        "total_connections": total,
        "peak_connections": peak,
        "connections_per_second": cps,
        "bytes_rx": bytes_rx,
        "bytes_tx": bytes_tx,
        "throughput_rx_per_sec": throughput_rx,
        "throughput_tx_per_sec": throughput_tx,
        "total_errors": errors,
        "pools": pools_json,
    });

    stats.to_string()
}
