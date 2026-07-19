use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ─── Configuration ───────────────────────────────────────────────────────────

struct Config {
    target: String,
    connections: usize,
    duration: Duration,
    payload_size: usize,
}

fn parse_config() -> Result<Config, String> {
    let args: Vec<String> = env::args().collect();

    // Help flag
    if args.iter().any(|a| a == "--help" || a == "-h") {
        return Err(usage());
    }

    let target = get_arg(&args, "--target")
        .unwrap_or_else(|| "127.0.0.1:8081".to_string());

    let connections: usize = get_arg(&args, "--connections")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let duration_secs: u64 = get_arg(&args, "--duration")
        .and_then(|s| s.trim_end_matches('s').parse().ok())
        .unwrap_or(10);

    let payload_size: usize = get_arg(&args, "--payload-size")
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    if connections == 0 {
        return Err("--connections must be > 0".to_string());
    }
    if duration_secs == 0 {
        return Err("--duration must be > 0".to_string());
    }
    if payload_size == 0 {
        return Err("--payload-size must be > 0".to_string());
    }

    Ok(Config {
        target,
        connections,
        duration: Duration::from_secs(duration_secs),
        payload_size,
    })
}

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn usage() -> String {
    format!(
        "Usage: loadtest [OPTIONS]\n\
         \n\
         Options:\n\
         \x20 --target <addr>        Gateway address (default: 127.0.0.1:8081)\n\
         \x20 --connections <N>      Concurrent persistent connections (default: 100)\n\
         \x20 --duration <secs>      Test duration in seconds (default: 10)\n\
         \x20 --payload-size <bytes> Bytes per request (default: 64)\n\
         \x20 --help                 Show this help"
    )
}

// ─── Per-Worker Results (no shared state in hot path) ────────────────────────

struct WorkerResult {
    latencies: Vec<u64>,  // microseconds per successful round-trip
    successes: u64,
    connect_errors: u64,
    io_errors: u64,
    mismatches: u64,
}

// ─── Worker Logic: persistent connection, loop send/recv ─────────────────────

async fn run_worker(
    worker_id: usize,
    target: String,
    payload_size: usize,
    test_start: Instant,
    duration: Duration,
    running: Arc<std::sync::atomic::AtomicBool>,
) -> WorkerResult {
    let mut result = WorkerResult {
        latencies: Vec::with_capacity(100_000),
        successes: 0,
        connect_errors: 0,
        io_errors: 0,
        mismatches: 0,
    };

    // ── STEP 1: Connect ONCE (outside the loop) ──
    let connect_timeout = Duration::from_secs(5);
    let mut stream = match tokio::time::timeout(connect_timeout, TcpStream::connect(&target)).await
    {
        Ok(Ok(s)) => s,
        Ok(Err(_)) | Err(_) => {
            result.connect_errors = 1;
            return result;
        }
    };

    // ── STEP 2: Build a per-worker unique payload ──
    // First 8 bytes = worker ID marker, rest = 0x42 padding.
    // Unique marker enables cross-talk detection under load.
    let payload = build_payload(worker_id, payload_size);
    let mut recv_buf = vec![0u8; payload_size];
    let deadline = test_start + duration;

    // ── STEP 3: Request loop on the SAME connection ──
    while running.load(std::sync::atomic::Ordering::Relaxed) && Instant::now() < deadline {
        // a. Start timer
        let req_start = Instant::now();

        // b. Write full payload
        if let Err(_) = stream.write_all(&payload).await {
            result.io_errors += 1;
            break; // connection broken, stop this worker
        }

        // c. Read back EXACTLY payload_size bytes
        if let Err(_) = stream.read_exact(&mut recv_buf).await {
            result.io_errors += 1;
            break; // connection broken, stop this worker
        }

        // d. Record latency (full round-trip: write + read)
        let latency_us = req_start.elapsed().as_micros() as u64;

        // e. Verify echo correctness
        if recv_buf != payload {
            result.mismatches += 1;
            // Don't break — keep going, but count it
        } else {
            result.successes += 1;
            result.latencies.push(latency_us);
        }
    }

    // ── STEP 5: Connection closes on drop (RAII) ──
    result
}

fn build_payload(worker_id: usize, size: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(size);
    // First 8 bytes: worker ID as big-endian u64 (unique marker for cross-talk detection)
    let id_bytes = (worker_id as u64).to_be_bytes();
    for &b in id_bytes.iter().take(size.min(8)) {
        payload.push(b);
    }
    // Remaining bytes: deterministic fill (0x42)
    while payload.len() < size {
        payload.push(0x42);
    }
    payload
}

// ─── Percentile computation ──────────────────────────────────────────────────

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn format_us(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.1}s", us as f64 / 1_000_000.0)
    } else if us >= 1_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{}µs", us)
    }
}

fn format_num(n: u64) -> String {
    // Simple thousands separator
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let config = match parse_config() {
        Ok(c) => c,
        Err(msg) => {
            eprintln!("{}", msg);
            std::process::exit(1);
        }
    };

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║     Traffic Gateway Load Test (Persistent Connections)  ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Target:       {:<40} ║", config.target);
    println!("║  Connections:  {:<40} ║", config.connections);
    println!("║  Duration:     {:<40} ║", format!("{}s", config.duration.as_secs()));
    println!("║  Payload:      {:<40} ║", format!("{} bytes", config.payload_size));
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();

    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let test_start = Instant::now();

    // Spawn all workers
    let mut handles = Vec::with_capacity(config.connections);
    for worker_id in 0..config.connections {
        let target = config.target.clone();
        let running = running.clone();
        let payload_size = config.payload_size;
        let duration = config.duration;

        handles.push(tokio::spawn(async move {
            run_worker(worker_id, target, payload_size, test_start, duration, running).await
        }));
    }

    // Progress ticker
    let running_progress = running.clone();
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        interval.tick().await; // skip first immediate tick

        while running_progress.load(std::sync::atomic::Ordering::Relaxed) {
            interval.tick().await;
            let elapsed = test_start.elapsed().as_secs();
            print!("\r  [{:>3}s] running...", elapsed);
            let _ = tokio::io::stdout().flush().await;
        }
    });

    // Duration timer — flip the stop flag
    let running_timer = running.clone();
    let duration = config.duration;
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        running_timer.store(false, std::sync::atomic::Ordering::Relaxed);
    });

    // Collect all worker results
    let mut all_latencies: Vec<u64> = Vec::new();
    let mut total_successes: u64 = 0;
    let mut total_connect_errors: u64 = 0;
    let mut total_io_errors: u64 = 0;
    let mut total_mismatches: u64 = 0;

    for handle in handles {
        if let Ok(result) = handle.await {
            all_latencies.extend_from_slice(&result.latencies);
            total_successes += result.successes;
            total_connect_errors += result.connect_errors;
            total_io_errors += result.io_errors;
            total_mismatches += result.mismatches;
        }
    }

    progress_handle.abort();
    println!(); // clear the progress line

    let actual_elapsed = test_start.elapsed();
    let elapsed_secs = actual_elapsed.as_secs_f64();

    // Check if we got anything at all
    if total_successes == 0 && total_connect_errors == config.connections as u64 {
        eprintln!();
        eprintln!("ERROR: Could not establish any connections to {}", config.target);
        eprintln!("       Is the gateway running? Check the address and try again.");
        std::process::exit(1);
    }

    // Compute stats
    let total_errors = total_connect_errors + total_io_errors + total_mismatches;
    let total_requests = total_successes + total_errors;
    let rps = if elapsed_secs > 0.0 { total_successes as f64 / elapsed_secs } else { 0.0 };
    // ×2 because each request sends payload AND receives it back through the gateway
    let bytes_total = total_successes * config.payload_size as u64 * 2;
    let mbps = if elapsed_secs > 0.0 { bytes_total as f64 / elapsed_secs / 1_048_576.0 } else { 0.0 };

    all_latencies.sort_unstable();
    let p50 = percentile(&all_latencies, 50.0);
    let p95 = percentile(&all_latencies, 95.0);
    let p99 = percentile(&all_latencies, 99.0);
    let max_lat = all_latencies.last().copied().unwrap_or(0);
    let min_lat = all_latencies.first().copied().unwrap_or(0);

    let error_rate = if total_requests > 0 {
        format!("{:.2}%", total_errors as f64 / total_requests as f64 * 100.0)
    } else {
        "N/A".to_string()
    };

    // Print results
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║          LOAD TEST RESULTS (Persistent Connections)     ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Target:           {:<36} ║", config.target);
    println!("║  Connections:      {:<36} ║", config.connections);
    println!("║  Duration:         {:<36} ║", format!("{:.2}s (actual)", elapsed_secs));
    println!("║  Payload size:     {:<36} ║", format!("{} bytes", config.payload_size));
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  THROUGHPUT                                             ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Total requests:   {:<36} ║", format_num(total_requests));
    println!("║  Successes:        {:<36} ║", format_num(total_successes));
    println!("║  Req/sec:          {:<36} ║", format!("{:.0}", rps));
    println!("║  Throughput:       {:<36} ║", format!("{:.2} MB/s", mbps));
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  LATENCY (per-request round-trip: send → recv → verify) ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  min:              {:<36} ║", format_us(min_lat));
    println!("║  p50:              {:<36} ║", format_us(p50));
    println!("║  p95:              {:<36} ║", format_us(p95));
    println!("║  p99:              {:<36} ║", format_us(p99));
    println!("║  max:              {:<36} ║", format_us(max_lat));
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  ERRORS                                                 ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Connect errors:   {:<36} ║", total_connect_errors);
    println!("║  IO errors:        {:<36} ║", total_io_errors);
    println!("║  Mismatches:       {:<36} ║", total_mismatches);
    println!("║  Error rate:       {:<36} ║", error_rate);
    println!("╚══════════════════════════════════════════════════════════╝");

    if total_mismatches > 0 {
        println!();
        println!("⚠️  WARNING: {} echo mismatches — data integrity issue!", total_mismatches);
    }
    if total_io_errors > 0 {
        println!();
        println!("ℹ️  {} IO errors (broken connections during test)", total_io_errors);
    }
}
