use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{info, warn};

pub struct BackendInstance {
    address: SocketAddr,
    healthy: AtomicBool,
    active_connections: AtomicU64,
    consecutive_failures: AtomicU32,
    consecutive_successes: AtomicU32,
    total_connections_served: AtomicU64,
}

impl BackendInstance {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            healthy: AtomicBool::new(true),
            active_connections: AtomicU64::new(0),
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            total_connections_served: AtomicU64::new(0),
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    pub fn total_served(&self) -> u64 {
        self.total_connections_served.load(Ordering::Relaxed)
    }

    pub fn mark_healthy(&self) {
        self.healthy.store(true, Ordering::SeqCst);
        info!("backend {} RECOVERED — marked HEALTHY", self.address);
    }

    pub fn mark_unhealthy(&self) {
        self.healthy.store(false, Ordering::SeqCst);
        warn!("backend {} marked UNHEALTHY", self.address);
    }

    pub fn record_failure(&self, unhealthy_threshold: u32) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        self.consecutive_successes.store(0, Ordering::Relaxed);
        
        if failures >= unhealthy_threshold && self.is_healthy() {
            self.mark_unhealthy();
        }
    }

    pub fn record_success(&self, healthy_threshold: u32) {
        let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;
        self.consecutive_failures.store(0, Ordering::Relaxed);
        
        if successes >= healthy_threshold && !self.is_healthy() {
            self.mark_healthy();
        }
    }

    pub fn inc_active(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections_served.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_active(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct ConnectionGuard {
    instance: Arc<BackendInstance>,
}

impl ConnectionGuard {
    pub fn new(instance: Arc<BackendInstance>) -> Self {
        instance.inc_active();
        Self { instance }
    }

    pub fn instance(&self) -> &BackendInstance {
        &self.instance
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.instance.dec_active();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_addr() -> SocketAddr {
        "127.0.0.1:8080".parse().unwrap()
    }

    #[test]
    fn test_instance_starts_healthy() {
        let instance = BackendInstance::new(dummy_addr());
        assert!(instance.is_healthy());
        assert_eq!(instance.active_connections(), 0);
        assert_eq!(instance.total_served(), 0);
    }

    #[test]
    fn test_mark_unhealthy_and_healthy() {
        let instance = BackendInstance::new(dummy_addr());
        
        instance.mark_unhealthy();
        assert!(!instance.is_healthy());
        
        instance.mark_healthy();
        assert!(instance.is_healthy());
    }

    #[test]
    fn test_record_failure_triggers_unhealthy() {
        let instance = BackendInstance::new(dummy_addr());
        
        instance.record_failure(3);
        assert!(instance.is_healthy()); // 1 failure
        
        instance.record_failure(3);
        assert!(instance.is_healthy()); // 2 failures
        
        instance.record_failure(3);
        assert!(!instance.is_healthy()); // 3 failures -> unhealthy
    }

    #[test]
    fn test_record_failure_below_threshold_stays_healthy() {
        let instance = BackendInstance::new(dummy_addr());
        instance.record_failure(3);
        instance.record_failure(3);
        assert!(instance.is_healthy());
    }

    #[test]
    fn test_record_success_triggers_recovery() {
        let instance = BackendInstance::new(dummy_addr());
        instance.mark_unhealthy();
        
        instance.record_success(2);
        assert!(!instance.is_healthy()); // 1 success
        
        instance.record_success(2);
        assert!(instance.is_healthy()); // 2 successes -> healthy
    }

    #[test]
    fn test_record_success_resets_failure_count() {
        let instance = BackendInstance::new(dummy_addr());
        
        instance.record_failure(5);
        instance.record_failure(5);
        assert_eq!(instance.consecutive_failures.load(Ordering::Relaxed), 2);
        
        instance.record_success(5);
        // Failures should be reset to 0, successes is 1
        assert_eq!(instance.consecutive_failures.load(Ordering::Relaxed), 0);
        assert_eq!(instance.consecutive_successes.load(Ordering::Relaxed), 1);
        
        instance.record_failure(5);
        instance.record_failure(5);
        // Only 2 consecutive failures now, not 4
        assert!(instance.is_healthy());
    }

    #[test]
    fn test_connection_guard_increments_and_decrements() {
        let instance = Arc::new(BackendInstance::new(dummy_addr()));
        assert_eq!(instance.active_connections(), 0);
        assert_eq!(instance.total_served(), 0);
        
        {
            let _guard = ConnectionGuard::new(instance.clone());
            assert_eq!(instance.active_connections(), 1);
            assert_eq!(instance.total_served(), 1);
        }
        
        assert_eq!(instance.active_connections(), 0);
        assert_eq!(instance.total_served(), 1);
    }

    #[test]
    fn test_connection_guard_multiple() {
        let instance = Arc::new(BackendInstance::new(dummy_addr()));
        
        let guard1 = ConnectionGuard::new(instance.clone());
        let guard2 = ConnectionGuard::new(instance.clone());
        let guard3 = ConnectionGuard::new(instance.clone());
        
        assert_eq!(instance.active_connections(), 3);
        assert_eq!(instance.total_served(), 3);
        
        drop(guard2);
        assert_eq!(instance.active_connections(), 2);
        
        drop(guard1);
        drop(guard3);
        assert_eq!(instance.active_connections(), 0);
        assert_eq!(instance.total_served(), 3);
    }
}
