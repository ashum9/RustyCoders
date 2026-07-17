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

