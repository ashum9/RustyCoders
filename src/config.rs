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
}

#[derive(Debug, Deserialize, Clone)]
pub struct ListenerConfig {
    pub name: String,
    pub listen: String,
    pub backend: String,
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

            // Check if backend address is valid format
            if listener.backend.parse::<std::net::SocketAddr>().is_err() {
                anyhow::bail!("invalid backend address '{}' for listener '{}'", listener.backend, listener.name);
            }

            // Check for duplicate listen addresses
            if !seen_addresses.insert(&listener.listen) {
                anyhow::bail!("duplicate listen address '{}' found in config", listener.listen);
            }
        }

        Ok(())
    }
}
