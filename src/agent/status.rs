use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::protocol::AgentStatus;
use crate::cli::Interval;

/// What the agent knows about itself right now: the counters `agent status`
/// reports.
///
/// Shared state rather than something the actor is asked for, because the reader
/// runs outside it. `Ping` is answered by the connection handler on purpose —
/// `agent status` stays responsive while the actor is blocked on the BLE link,
/// which is precisely when its state is worth asking about — and anything that
/// had to reach the actor would time out exactly then.
pub struct AgentStats {
    started: Instant,
    interval: Interval,
    connected: AtomicBool,
    samples: AtomicU64,
    reconnects: AtomicU64,
    clients: AtomicUsize,
    /// When the most recent sample arrived, for the age `agent status` reports.
    /// A short-lived lock, never held across an await.
    last_sample: Mutex<Option<Instant>>,
}

impl AgentStats {
    pub fn new(interval: Interval) -> Self {
        Self {
            started: Instant::now(),
            interval,
            connected: AtomicBool::new(false),
            samples: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            clients: AtomicUsize::new(0),
            last_sample: Mutex::new(None),
        }
    }

    /// Record a measurement. Arriving at all is proof the link is up, so this
    /// is also what clears a `connected = false` left by an earlier failure.
    pub fn record_sample(&self) {
        self.samples.fetch_add(1, Ordering::Relaxed);
        self.connected.store(true, Ordering::Relaxed);
        if let Ok(mut last) = self.last_sample.lock() {
            *last = Some(Instant::now());
        }
    }

    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
    }

    pub fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
        self.connected.store(true, Ordering::Relaxed);
    }

    pub fn set_clients(&self, clients: usize) {
        self.clients.store(clients, Ordering::Relaxed);
    }

    /// How long ago the last measurement arrived, or `None` if none has. The
    /// device's own timestamp cannot answer this: it comes from an RTC that
    /// drifts, and that `--set-rtc` moves.
    fn sample_age(&self) -> Option<Duration> {
        let at = (*self.last_sample.lock().ok()?)?;
        Some(at.elapsed())
    }

    pub fn snapshot(&self) -> AgentStatus {
        AgentStatus {
            uptime_seconds: self.started.elapsed().as_secs(),
            interval_seconds: self.interval.as_secs_f64(),
            connected: self.connected.load(Ordering::Relaxed),
            samples: self.samples.load(Ordering::Relaxed),
            last_sample_age_seconds: self.sample_age().map(|age| age.as_secs_f64()),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            clients: self.clients.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(interval: &str) -> AgentStats {
        AgentStats::new(interval.parse().unwrap())
    }

    #[test]
    fn a_sample_is_reported() {
        let stats = stats("1s");
        assert_eq!(stats.snapshot().samples, 0);
        assert_eq!(stats.sample_age(), None);

        stats.record_sample();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.samples, 1);
        assert!(snapshot.connected);
        assert!(snapshot.last_sample_age_seconds.is_some());
    }

    /// A sample proves the link is up, so it clears a failure recorded earlier.
    #[test]
    fn a_sample_clears_a_dead_link() {
        let stats = stats("1s");
        stats.record_sample();
        stats.set_connected(false);
        assert!(!stats.snapshot().connected);

        stats.record_sample();
        assert!(stats.snapshot().connected);
    }

    #[test]
    fn counters_are_reported() {
        let stats = stats("500ms");
        stats.record_reconnect();
        stats.record_reconnect();
        stats.set_clients(3);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.reconnects, 2);
        assert_eq!(snapshot.clients, 3);
        assert_eq!(snapshot.interval_seconds, 0.5);
    }
}
