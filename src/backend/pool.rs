use super::instance::BackendInstance;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LbStrategy {
    RoundRobin,
    LeastConnections,
}

impl LbStrategy {
    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "round_robin" => Ok(LbStrategy::RoundRobin),
            "least_connections" => Ok(LbStrategy::LeastConnections),
            other => anyhow::bail!(
                "unknown strategy '{}'. Valid options: 'round_robin', 'least_connections'",
                other
            ),
        }
    }
}

pub struct BackendPool {
    name: String,
    instances: Vec<Arc<BackendInstance>>,
    strategy: LbStrategy,
    round_robin_counter: AtomicUsize,
}

impl BackendPool {
    pub fn new(name: String, instances: Vec<Arc<BackendInstance>>, strategy: LbStrategy) -> Self {
        Self {
            name,
            instances,
            strategy,
            round_robin_counter: AtomicUsize::new(0),
        }
    }

    pub fn instances(&self) -> &[Arc<BackendInstance>] {
        &self.instances
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn healthy_count(&self) -> usize {
        self.instances.iter().filter(|i| i.is_healthy()).count()
    }

    pub fn select(&self) -> Option<Arc<BackendInstance>> {
        match self.strategy {
            LbStrategy::RoundRobin => self.select_round_robin(),
            LbStrategy::LeastConnections => self.select_least_connections(),
        }
    }

    fn select_round_robin(&self) -> Option<Arc<BackendInstance>> {
        if self.instances.is_empty() {
            return None;
        }

        let idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed);
        let len = self.instances.len();
        let start = idx % len;

        for offset in 0..len {
            let i = (start + offset) % len;
            if self.instances[i].is_healthy() {
                return Some(self.instances[i].clone());
            }
        }

        None
    }

    fn select_least_connections(&self) -> Option<Arc<BackendInstance>> {
        let mut best = None;
        let mut best_count = u64::MAX;

        for instance in &self.instances {
            if instance.is_healthy() {
                let count = instance.active_connections();
                if count < best_count {
                    best = Some(instance);
                    best_count = count;
                }
            }
        }

        best.map(|i| i.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::instance::ConnectionGuard;
    use std::net::SocketAddr;

    fn create_instances(count: usize) -> Vec<Arc<BackendInstance>> {
        (0..count)
            .map(|i| {
                let addr = format!("127.0.0.1:{}", 8000 + i).parse::<SocketAddr>().unwrap();
                Arc::new(BackendInstance::new(addr))
            })
            .collect()
    }

    #[test]
    fn test_round_robin_cycles_through_all() {
        let instances = create_instances(3);
        let pool = BackendPool::new("test".into(), instances.clone(), LbStrategy::RoundRobin);

        // 9 calls should cycle 1-2-3-1-2-3-1-2-3
        for i in 0..9 {
            let selected = pool.select().unwrap();
            let expected_addr = instances[i % 3].address();
            assert_eq!(selected.address(), expected_addr);
        }
    }

    #[test]
    fn test_round_robin_skips_unhealthy() {
        let instances = create_instances(3);
        instances[1].mark_unhealthy(); // instances[1] is 8001
        
        let pool = BackendPool::new("test".into(), instances.clone(), LbStrategy::RoundRobin);

        // idx=0 -> start=0 -> picks 0
        let selected1 = pool.select().unwrap();
        assert_eq!(selected1.address(), instances[0].address());

        // idx=1 -> start=1 -> skips 1, picks 2
        let selected2 = pool.select().unwrap();
        assert_eq!(selected2.address(), instances[2].address());

        // idx=2 -> start=2 -> picks 2
        let selected3 = pool.select().unwrap();
        assert_eq!(selected3.address(), instances[2].address());
        
        // idx=3 -> start=0 -> picks 0
        let selected4 = pool.select().unwrap();
        assert_eq!(selected4.address(), instances[0].address());
    }

    #[test]
    fn test_round_robin_all_unhealthy_returns_none() {
        let instances = create_instances(3);
        for instance in &instances {
            instance.mark_unhealthy();
        }
        let pool = BackendPool::new("test".into(), instances, LbStrategy::RoundRobin);
        assert!(pool.select().is_none());
    }

    #[test]
    fn test_least_conn_picks_lowest() {
        let instances = create_instances(3);
        
        // Use guards to increment active connections safely
        let mut guards = Vec::new();
        
        // Instance 0 gets 5 connections
        for _ in 0..5 { guards.push(ConnectionGuard::new(instances[0].clone())); }
        
        // Instance 1 gets 2 connections
        for _ in 0..2 { guards.push(ConnectionGuard::new(instances[1].clone())); }
        
        // Instance 2 gets 8 connections
        for _ in 0..8 { guards.push(ConnectionGuard::new(instances[2].clone())); }

        let pool = BackendPool::new("test".into(), instances.clone(), LbStrategy::LeastConnections);

        // Should pick instance 1 (has 2 connections)
        let selected = pool.select().unwrap();
        assert_eq!(selected.address(), instances[1].address());
    }

    #[test]
    fn test_least_conn_skips_unhealthy() {
        let instances = create_instances(3);
        
        // Instance 0 gets 5 connections
        let mut guards = Vec::new();
        for _ in 0..5 { guards.push(ConnectionGuard::new(instances[0].clone())); }
        
        // Instance 1 gets 2 connections, but is UNHEALTHY
        for _ in 0..2 { guards.push(ConnectionGuard::new(instances[1].clone())); }
        instances[1].mark_unhealthy();
        
        // Instance 2 gets 8 connections
        for _ in 0..8 { guards.push(ConnectionGuard::new(instances[2].clone())); }

        let pool = BackendPool::new("test".into(), instances.clone(), LbStrategy::LeastConnections);

        // Should skip instance 1 because it's unhealthy, and pick instance 0 (next lowest)
        let selected = pool.select().unwrap();
        assert_eq!(selected.address(), instances[0].address());
    }

    #[test]
    fn test_least_conn_all_unhealthy_returns_none() {
        let instances = create_instances(3);
        for instance in &instances {
            instance.mark_unhealthy();
        }
        let pool = BackendPool::new("test".into(), instances, LbStrategy::LeastConnections);
        assert!(pool.select().is_none());
    }

    #[test]
    fn test_healthy_count() {
        let instances = create_instances(3);
        let pool = BackendPool::new("test".into(), instances.clone(), LbStrategy::RoundRobin);
        
        assert_eq!(pool.healthy_count(), 3);
        
        instances[0].mark_unhealthy();
        assert_eq!(pool.healthy_count(), 2);
        
        instances[1].mark_unhealthy();
        assert_eq!(pool.healthy_count(), 1);
    }
}
