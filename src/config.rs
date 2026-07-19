use crate::backend::pool::LbStrategy;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct GatewayConfig {
    pub global: GlobalConfig,
    pub listeners: Vec<ListenerConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GlobalConfig {
    pub max_connections: usize,
    #[serde(default = "default_admin_listen")]
    pub admin_listen: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ListenerConfig {
    pub name: String,
    pub listen: String,
    #[serde(default)]
    pub backends: Vec<String>,
    #[serde(default = "default_strategy")]
    pub strategy: String,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default)]
    pub health_check: HealthCheckConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HealthCheckConfig {
    #[serde(default = "default_hc_enabled")]
    pub enabled: bool,
    #[serde(default = "default_hc_interval")]
    pub interval_ms: u64,
    #[serde(default = "default_hc_timeout")]
    pub timeout_ms: u64,
    #[serde(default = "default_hc_healthy_threshold")]
    pub healthy_threshold: u32,
    #[serde(default = "default_hc_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
}

fn default_strategy() -> String { "round_robin".to_string() }
fn default_connect_timeout() -> u64 { 1000 }
fn default_admin_listen() -> String { "127.0.0.1:9090".to_string() }
fn default_hc_enabled() -> bool { true }
fn default_hc_interval() -> u64 { 2000 }
fn default_hc_timeout() -> u64 { 500 }
fn default_hc_healthy_threshold() -> u32 { 2 }
fn default_hc_unhealthy_threshold() -> u32 { 3 }

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: default_hc_enabled(),
            interval_ms: default_hc_interval(),
            timeout_ms: default_hc_timeout(),
            healthy_threshold: default_hc_healthy_threshold(),
            unhealthy_threshold: default_hc_unhealthy_threshold(),
        }
    }
}

impl GatewayConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file at '{}'", path))?;
        
        let config: GatewayConfig = toml::from_str(&content)
            .with_context(|| "failed to parse TOML configuration")?;

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.listeners.is_empty() {
            anyhow::bail!("config must define at least one listener");
        }

        let mut seen_addresses = HashSet::new();
        for listener in &self.listeners {
            // Check if listen address is valid format
            if listener.listen.parse::<std::net::SocketAddr>().is_err() {
                anyhow::bail!("invalid listen address '{}' for listener '{}'", listener.listen, listener.name);
            }

            // Check for duplicate listen addresses
            if !seen_addresses.insert(&listener.listen) {
                anyhow::bail!("duplicate listen address '{}' found in config", listener.listen);
            }

            // Check backends
            if listener.backends.is_empty() {
                anyhow::bail!("listener '{}' must define at least one backend", listener.name);
            }

            for backend in &listener.backends {
                if backend.parse::<std::net::SocketAddr>().is_err() {
                    anyhow::bail!("invalid backend address '{}' for listener '{}'", backend, listener.name);
                }
            }
            
            // Check strategy
            if let Err(e) = LbStrategy::from_str(&listener.strategy) {
                anyhow::bail!("listener '{}' has invalid strategy: {}", listener.name, e);
            }
            
            // Check health thresholds
            if listener.health_check.interval_ms == 0 || listener.health_check.timeout_ms == 0 {
                anyhow::bail!("listener '{}' has invalid health check interval/timeout (must be > 0)", listener.name);
            }
            if listener.health_check.healthy_threshold == 0 || listener.health_check.unhealthy_threshold == 0 {
                anyhow::bail!("listener '{}' has invalid health check thresholds (must be > 0)", listener.name);
            }
        }

        Ok(())
    }
}
