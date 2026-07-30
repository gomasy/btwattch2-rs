use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::protocol::AgentStatus;
use crate::cli::Interval;
use crate::connection::Measurement;

/// Never call a reading fresh for less than this, however short the interval:
/// one skipped sample on a busy link is not a dead device.
const MIN_FRESHNESS: Duration = Duration::from_secs(5);
/// How many intervals may pass before the last reading stops standing for the
/// device's present state.
const STALE_AFTER_INTERVALS: u32 = 3;

/// The most recent measurement, with the monotonic instant it arrived. The
/// device's own timestamp cannot answer "how long ago": it comes from an RTC
/// that drifts, and that `--set-rtc` moves.
struct Sample {
    measurement: Measurement,
    at: Instant,
}

/// The latest reading and whether it still describes the device. Taken as one
/// value so a scrape cannot read a measurement and a freshness verdict from
/// either side of an arriving sample.
pub struct Reading {
    pub measurement: Option<Measurement>,
    pub fresh: bool,
}

/// What the agent knows about itself right now: the counters `agent status`
/// reports and the latest measurement `/metrics` serves.
///
/// Shared state rather than something the actor is asked for, because both
/// readers run outside it. `Ping` is answered by the connection handler on
/// purpose — `agent status` stays responsive while the actor is blocked on the
/// BLE link, which is precisely when its state is worth asking about — and a
/// scrape must not queue behind a reconnect either. Anything that had to reach
/// the actor would time out in both cases.
pub struct AgentStats {
    started: Instant,
    interval: Interval,
    metrics_listen: Option<SocketAddr>,
    connected: AtomicBool,
    samples: AtomicU64,
    reconnects: AtomicU64,
    clients: AtomicUsize,
    /// The most recent sample, for the metrics endpoint and for the age
    /// `agent status` reports. A short-lived lock, never held across an await:
    /// the writer clones in, readers clone out.
    latest: Mutex<Option<Sample>>,
}

impl AgentStats {
    pub fn new(interval: Interval, metrics_listen: Option<SocketAddr>) -> Self {
        Self {
            started: Instant::now(),
            interval,
            metrics_listen,
            connected: AtomicBool::new(false),
            samples: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            clients: AtomicUsize::new(0),
            latest: Mutex::new(None),
        }
    }

    /// Record a measurement. Arriving at all is proof the link is up, so this
    /// is also what clears a `connected = false` left by an earlier failure.
    pub fn record_sample(&self, m: &Measurement) {
        self.samples.fetch_add(1, Ordering::Relaxed);
        self.connected.store(true, Ordering::Relaxed);
        if let Ok(mut latest) = self.latest.lock() {
            *latest = Some(Sample {
                measurement: m.clone(),
                at: Instant::now(),
            });
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

    /// The latest reading and whether it still describes the device: the link is
    /// up, and a sample arrived recently enough for the configured interval.
    pub fn reading(&self) -> Reading {
        let connected = self.connected.load(Ordering::Relaxed);
        let stale_after = self.stale_after();
        // One lock for both answers, so the verdict describes the measurement
        // returned beside it.
        let guard = self.latest.lock().ok();
        let sample = guard.as_ref().and_then(|latest| latest.as_ref());
        Reading {
            measurement: sample.map(|s| s.measurement.clone()),
            fresh: connected && sample.is_some_and(|s| s.at.elapsed() <= stale_after),
        }
    }

    /// How long ago the last measurement arrived, or `None` if none has.
    fn sample_age(&self) -> Option<Duration> {
        let at = self.latest.lock().ok()?.as_ref()?.at;
        Some(at.elapsed())
    }

    /// How old a reading may be before it stops standing for the device's
    /// present state.
    fn stale_after(&self) -> Duration {
        (self.interval.duration() * STALE_AFTER_INTERVALS).max(MIN_FRESHNESS)
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
            metrics_listen: self.metrics_listen.map(|addr| addr.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::testutil::measurement;

    fn stats(interval: &str) -> AgentStats {
        AgentStats::new(interval.parse().unwrap(), None)
    }

    #[test]
    fn a_fresh_sample_is_reported() {
        let stats = stats("1s");
        assert_eq!(stats.snapshot().samples, 0);
        assert_eq!(stats.sample_age(), None);
        // Nothing read yet is not "fresh", however new the agent is.
        assert!(!stats.reading().fresh);

        stats.record_sample(&measurement(100.0));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.samples, 1);
        assert!(snapshot.connected);
        assert!(snapshot.last_sample_age_seconds.is_some());
        let reading = stats.reading();
        assert!(reading.fresh);
        assert!(reading.measurement.is_some());
    }

    /// A sample proves the link is up, so it clears a failure recorded earlier.
    #[test]
    fn a_sample_clears_a_dead_link() {
        let stats = stats("1s");
        stats.record_sample(&measurement(100.0));
        stats.set_connected(false);
        let reading = stats.reading();
        assert!(!reading.fresh, "a dead link cannot be fresh");
        // The reading itself stays available, for `/metrics` to describe as down.
        assert!(reading.measurement.is_some());

        stats.record_sample(&measurement(100.0));
        assert!(stats.reading().fresh);
    }

    /// The freshness window follows the interval, but never drops below the
    /// floor — a 10 ms interval must not call a 30 ms gap a dead device.
    #[test]
    fn the_freshness_window_has_a_floor() {
        let stats = stats("10ms");
        stats.record_sample(&measurement(100.0));
        std::thread::sleep(Duration::from_millis(50));
        assert!(stats.reading().fresh);
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
        assert_eq!(snapshot.metrics_listen, None);
    }
}
