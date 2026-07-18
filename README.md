# RustyCoders

Code Repository for RustyCoders

## MVP Architecture

![MVP Architecture](assets/mvp.png)

## High Level Architecture

This project implements a **Layer-4 TCP Traffic Gateway** in **Rust + Tokio**, acting as an intermediary between clients and backend servers. The gateway accepts incoming TCP connections, selects a healthy backend using configurable load-balancing strategies, and forwards raw bytes bidirectionally without inspecting application-layer data.

![Architecture Diagram](assets/architecture.png)

### Key Components

- **TCP Listener** – Accepts incoming client connections and spawns an async task for each connection.
- **Load Balancer** – Selects a backend using **Round Robin** or **Least Connections**, considering only healthy servers.
- **Proxy Engine** – Establishes backend connections and performs raw-byte TCP forwarding with retry/failover support.
- **Health Checker** - Continuously monitors backend availability using active probes and passive failure detection.
- **Backend Pool** – Maintains backend metadata such as health status, active connections, and routing eligibility.
- **Metrics & Dashboard** – Provides observability through connection statistics, backend health, traffic metrics, and system events.
- **TOML Configuration** – Controls gateway behavior, backend definitions, thresholds, and load-balancing settings without code changes.

### Architectural Note: L4 Failover Limitation

> **Note:** Failover applies at **connection establishment time** only. If a selected backend refuses or times out during the initial TCP handshake, the gateway transparently retries a different healthy backend — the client never sees a failure.
>
> However, because this is an **L4 (TCP) proxy**, if a backend dies **mid-stream** (after bytes have already started flowing), the connection is closed cleanly. We cannot transparently replay a TCP byte stream to another backend because the protocol state (sequence numbers, application context) has already been established. This is a fundamental limitation of Layer-4 proxying — Layer-7 (HTTP) proxies can retry mid-stream because they understand request boundaries.
