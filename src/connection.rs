use std::collections::HashMap;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use btleplug::api::{
    BDAddr, Central, CharPropFlags, Characteristic, Manager as _, Peripheral as _, ScanFilter,
    ValueNotification, WriteType,
};
use btleplug::platform::{Adapter, Manager, Peripheral};
use chrono::{DateTime, Local, NaiveDate};
use futures::{Stream, StreamExt};
use uuid::{Uuid, uuid};

use crate::cli::{ConnectionConfig, LogLevel, local_datetime};
use crate::payload::{self, FRAME_OVERHEAD, HEADER};

// TX/RX characteristics of the Nordic UART service the device exposes.
const C_TX: Uuid = uuid!("6e400002-b5a3-f393-e0a9-e50e24dcca9e");
pub(crate) const C_RX: Uuid = uuid!("6e400003-b5a3-f393-e0a9-e50e24dcca9e");

const SCAN_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_RETRIES: usize = 5;
const CONNECT_RETRIES: usize = 5;

/// Shortest frame that can carry a full measurement (through the 6-byte date
/// at offset 23). Also what distinguishes a measurement reply from the much
/// shorter status reply to a one-shot command.
pub(crate) const MEASUREMENT_FRAME_MIN_LEN: usize = 29;

/// Largest frame the device ever sends. Used only to bound the reassembly
/// buffer, so a desynchronized stream cannot grow it without limit.
const MAX_FRAME_LEN: usize = 256;

const VOLTAGE_SCALE: f64 = (1u64 << 24) as f64;
const AMPERE_SCALE: f64 = (1u64 << 30) as f64;
const WATTAGE_SCALE: f64 = (1u64 << 24) as f64;

pub(crate) type Notifications = Pin<Box<dyn Stream<Item = ValueNotification> + Send>>;

static INFO: AtomicBool = AtomicBool::new(false);

/// Set the verbosity of informational (`[INFO]`) output. Called once at
/// startup from the resolved CLI log level.
pub fn set_log_level(level: LogLevel) {
    INFO.store(level == LogLevel::Info, Ordering::Relaxed);
}

fn info_enabled() -> bool {
    INFO.load(Ordering::Relaxed)
}

// Print an informational message to stderr, only when --log-level allows it.
// Warnings and errors are printed unconditionally with plain `eprintln!`.
macro_rules! info {
    ($($arg:tt)*) => {
        if info_enabled() {
            eprintln!("[INFO] {}", format_args!($($arg)*));
        }
    };
}

#[derive(Debug)]
pub struct Measurement {
    pub voltage: f64,
    pub ampere: f64,
    pub wattage: f64,
    /// Power factor, derived as wattage / (voltage * ampere). The device does
    /// not report this directly; 0.0 when voltage or ampere is zero.
    pub power_factor: f64,
    pub timestamp: DateTime<Local>,
}

/// A device discovered during a scan.
#[derive(Debug, Clone)]
pub struct ScannedDevice {
    pub addr: BDAddr,
    pub name: Option<String>,
    pub rssi: Option<i16>,
}

pub(crate) struct FrameAssembler {
    buf: Vec<u8>,
}

impl FrameAssembler {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn feed(&mut self, data: &[u8]) -> Option<Vec<u8>> {
        if !self.buf.is_empty() && data.first() == Some(&HEADER) {
            self.buf.clear();
        }
        self.buf.extend_from_slice(data);

        if self.buf.len() >= FRAME_OVERHEAD {
            let payload_len = u16::from_be_bytes([self.buf[1], self.buf[2]]) as usize;
            // `>=` rather than `==`: a desynchronized stream can overshoot the
            // declared length, and waiting for an exact match would wedge the
            // assembler until the next header arrives.
            if self.buf.len() - FRAME_OVERHEAD >= payload_len {
                let mut frame = std::mem::take(&mut self.buf);
                frame.truncate(payload_len + FRAME_OVERHEAD);
                return Some(frame);
            }
        }

        // A bogus length field would otherwise let the buffer grow forever.
        if self.buf.len() > MAX_FRAME_LEN {
            eprintln!("[WARN] Discarding oversized frame buffer, resynchronizing");
            self.buf.clear();
        }
        None
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }
}

pub struct Connection {
    name: String,
    addr: BDAddr,
    interval: Duration,
    device: Peripheral,
    tx: Characteristic,
    rx: Characteristic,
}

impl Connection {
    pub async fn new(opts: &ConnectionConfig) -> Result<Self> {
        let manager = Manager::new().await?;
        let name = format!("hci{}", opts.index);
        let adapter = Self::find_adapter(&manager, &name).await?;
        let device = Self::find_device(&adapter, opts.addr).await?;

        let (tx, rx) = Self::establish(&device, opts.addr, &name).await?;

        Ok(Self {
            name,
            addr: opts.addr,
            interval: Duration::from_secs(opts.interval.max(1)),
            device,
            tx,
            rx,
        })
    }

    /// Scan for nearby Bluetooth devices for `duration` and return the unique
    /// ones seen, keyed by address. Useful for discovering the device's BD
    /// address before a first connection.
    pub async fn scan(index: usize, duration: Duration) -> Result<Vec<ScannedDevice>> {
        let manager = Manager::new().await?;
        let name = format!("hci{index}");
        let adapter = Self::find_adapter(&manager, &name).await?;

        adapter
            .start_scan(ScanFilter::default())
            .await
            .context("failed to start scanning (is Bluetooth powered on?)")?;

        let deadline = tokio::time::Instant::now() + duration;
        let mut found: HashMap<BDAddr, ScannedDevice> = HashMap::new();
        loop {
            for dev in adapter.peripherals().await? {
                let addr = dev.address();
                // Advertisements are fragmented: the name often arrives in a
                // later scan response, so keep polling properties (a D-Bus
                // round trip) until a device has one, then leave it alone.
                if found.get(&addr).is_some_and(|d| d.name.is_some()) {
                    continue;
                }
                let Some(props) = dev.properties().await? else {
                    continue;
                };
                found.insert(
                    addr,
                    ScannedDevice {
                        addr,
                        name: props.local_name,
                        rssi: props.rssi,
                    },
                );
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        adapter.stop_scan().await.ok();
        Ok(found.into_values().collect())
    }

    async fn find_adapter(manager: &Manager, name: &str) -> Result<Adapter> {
        for adapter in manager.adapters().await? {
            // On BlueZ this is "hciN (modalias)".
            match adapter.adapter_info().await {
                Ok(info) if info == name || info.starts_with(&format!("{name} ")) => {
                    return Ok(adapter);
                }
                _ => {}
            }
        }
        bail!("adapter {name} not found")
    }

    async fn find_device(adapter: &Adapter, addr: BDAddr) -> Result<Peripheral> {
        if let Some(device) = Self::lookup(adapter, addr).await? {
            return Ok(device);
        }

        info!("Scanning for {addr}...");
        adapter
            .start_scan(ScanFilter::default())
            .await
            .context("failed to start scanning (is Bluetooth powered on?)")?;

        let deadline = tokio::time::Instant::now() + SCAN_TIMEOUT;
        let device = loop {
            if let Some(device) = Self::lookup(adapter, addr).await? {
                break device;
            }
            if tokio::time::Instant::now() >= deadline {
                adapter.stop_scan().await.ok();
                bail!("device {addr} not found");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };

        adapter.stop_scan().await.ok();
        Ok(device)
    }

    async fn lookup(adapter: &Adapter, addr: BDAddr) -> Result<Option<Peripheral>> {
        Ok(adapter
            .peripherals()
            .await?
            .into_iter()
            .find(|device| device.address() == addr))
    }

    async fn establish(
        device: &Peripheral,
        addr: BDAddr,
        name: &str,
    ) -> Result<(Characteristic, Characteristic)> {
        for attempt in 1.. {
            info!("Connecting to {addr} via {name} (attempt {attempt})...");

            let result = async {
                device.connect().await?;
                device.discover_services().await
            }
            .await;

            match result {
                Ok(()) => break,
                Err(e) if attempt < CONNECT_RETRIES => {
                    info!("Connection failed: {e}, retrying...");
                    // On BlueZ, Connect() can succeed at the D-Bus level even
                    // when service discovery times out, leaving a half-open
                    // connection that makes every later attempt fail too.
                    // Drop it before retrying.
                    Self::drop_connection(device).await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    info!("Connection failed: {e}");
                    Self::drop_connection(device).await;
                    return Err(e)
                        .with_context(|| format!("connect failed after {attempt} attempts"));
                }
            }
        }

        let chars = device.characteristics();
        let find = |uuid: Uuid| {
            chars
                .iter()
                .find(|c| c.uuid == uuid)
                .cloned()
                .ok_or_else(|| anyhow!("characteristic {uuid} not found"))
        };
        let tx = find(C_TX)?;
        let rx = find(C_RX)?;

        info!("Connected to {addr} via {name}");
        Ok((tx, rx))
    }

    pub(crate) async fn connect(&mut self) -> Result<()> {
        let (tx, rx) = Self::establish(&self.device, self.addr, &self.name).await?;
        self.tx = tx;
        self.rx = rx;
        Ok(())
    }

    /// The RX notification stream ended: re-establish the link and return a
    /// fresh subscription. The "stream closed, reconnecting" warning is emitted
    /// here so it is logged exactly once, regardless of which caller drives it.
    pub(crate) async fn reconnect_stream(&mut self) -> Result<Notifications> {
        eprintln!("[WARN] Notification stream closed, reconnecting...");
        self.connect().await?;
        self.listen().await
    }

    pub async fn disconnect(&self) -> Result<()> {
        Self::drop_connection(&self.device).await;
        info!("Disconnected");
        Ok(())
    }

    /// Disconnect only if a link is actually established. Calling
    /// Disconnect() while a connect attempt is still in flight aborts it,
    /// which makes bluetoothd log "No matching connection for device".
    async fn drop_connection(device: &Peripheral) {
        if device.is_connected().await.unwrap_or(false) {
            device.disconnect().await.ok();
        }
    }

    pub(crate) fn interval(&self) -> Duration {
        self.interval
    }

    pub async fn set_rtc(&mut self, time: &DateTime<Local>) -> Result<()> {
        self.command(payload::rtc(time), "RTC set").await
    }

    pub async fn power(&mut self, on: bool) -> Result<()> {
        let (payload, action) = if on {
            (payload::on(), "Power on")
        } else {
            (payload::off(), "Power off")
        };
        self.command(payload, action).await
    }

    pub async fn blink_led(&mut self) -> Result<()> {
        self.command(payload::blink_led(), "Blink").await
    }

    /// Send a one-shot command and report the status byte of the reply.
    async fn command(&mut self, payload: Vec<u8>, action: &str) -> Result<()> {
        self.subscribe(payload, |frame| {
            match frame.get(4).copied() {
                Some(0x00) => info!("{action} succeeded"),
                Some(code) => eprintln!("[ERR] {action} failed, CODE: {code:#04x}"),
                None => eprintln!("[ERR] {action} failed, frame too short"),
            }
            ControlFlow::Break(())
        })
        .await
    }

    async fn subscribe<F>(&mut self, payload: Vec<u8>, mut on_frame: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> ControlFlow<()>,
    {
        let mut notifications = self.listen().await?;
        let mut assembler = FrameAssembler::new();
        let mut ticker = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if self.write(&payload).await? {
                        notifications = self.listen().await?;
                    }
                }
                event = notifications.next() => {
                    let Some(event) = event else {
                        notifications = self.reconnect_stream().await?;
                        assembler.clear();
                        continue;
                    };

                    if event.uuid != C_RX {
                        continue;
                    }

                    if let Some(frame) = assembler.feed(&event.value)
                        && on_frame(&frame).is_break()
                    {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn subscribe_measure<F>(&mut self, mut on_measure: F) -> Result<()>
    where
        F: FnMut(Measurement) -> ControlFlow<()>,
    {
        self.subscribe(payload::monitoring(), |frame| {
            match try_measurement(frame) {
                Some(m) => on_measure(m),
                None => ControlFlow::Continue(()),
            }
        })
        .await
    }

    pub(crate) async fn listen(&self) -> Result<Notifications> {
        self.device.subscribe(&self.rx).await?;
        Ok(self.device.notifications().await?)
    }

    /// Write `payload`, reconnecting on failure. Returns whether the
    /// connection was re-established along the way.
    pub(crate) async fn write(&mut self, payload: &[u8]) -> Result<bool> {
        let mut reconnected = false;

        for attempt in 1.. {
            // Re-read every attempt: a reconnect along the way replaces `tx`.
            let write_type = if self.tx.properties.contains(CharPropFlags::WRITE) {
                WriteType::WithResponse
            } else {
                WriteType::WithoutResponse
            };

            match self.device.write(&self.tx, payload, write_type).await {
                Ok(()) => return Ok(reconnected),
                Err(e) if attempt < WRITE_RETRIES => {
                    eprintln!("[WARN] Write failed: {e}");
                    match self.connect().await {
                        Ok(()) => reconnected = true,
                        Err(e) => {
                            eprintln!("[WARN] Reconnect failed: {e}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("write failed after {attempt} attempts"));
                }
            }
        }

        unreachable!()
    }
}

pub(crate) fn read_measure(frame: &[u8]) -> Result<Measurement> {
    if frame.len() < MEASUREMENT_FRAME_MIN_LEN {
        bail!("frame too short: {} bytes", frame.len());
    }

    // [sec, min, hour, day, mon, year - 1900]
    let date = &frame[23..29];
    let timestamp =
        NaiveDate::from_ymd_opt(1900 + date[5] as i32, date[4] as u32 + 1, date[3] as u32)
            .and_then(|d| d.and_hms_opt(date[2] as u32, date[1] as u32, date[0] as u32))
            .and_then(local_datetime)
            .ok_or_else(|| anyhow!("invalid timestamp in frame"))?;

    let voltage = u48_le(&frame[5..11]) as f64 / VOLTAGE_SCALE;
    let ampere = u48_le(&frame[11..17]) as f64 / AMPERE_SCALE;
    let wattage = u48_le(&frame[17..23]) as f64 / WATTAGE_SCALE;
    let power_factor = if voltage > 0.0 && ampere > 0.0 {
        wattage / (voltage * ampere)
    } else {
        0.0
    };

    Ok(Measurement {
        voltage,
        ampere,
        wattage,
        power_factor,
        timestamp,
    })
}

/// Parse a reassembled frame into a measurement. On failure it logs the single
/// "Failed to parse measurement" error and yields `None`, so callers can skip
/// the frame and keep streaming instead of duplicating that log line.
pub(crate) fn try_measurement(frame: &[u8]) -> Option<Measurement> {
    match read_measure(frame) {
        Ok(m) => Some(m),
        Err(e) => {
            eprintln!("[ERR] Failed to parse measurement: {e}");
            None
        }
    }
}

fn u48_le(payload: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes[..6].copy_from_slice(&payload[..6]);
    u64::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    #[test]
    fn u48_le_is_little_endian() {
        assert_eq!(u48_le(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00]), 1);
        assert_eq!(u48_le(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x01]), 1 << 40);
    }

    #[test]
    fn read_measure_parses_frame() {
        let mut frame = vec![0u8; 30];
        frame[0] = HEADER;
        // voltage = 100.5 * 2^24, little-endian
        frame[5..11].copy_from_slice(&(100 * (1u64 << 24) + (1 << 23)).to_le_bytes()[..6]);
        // ampere = 1.25 * 2^30
        frame[11..17].copy_from_slice(&((1u64 << 30) + (1 << 28)).to_le_bytes()[..6]);
        // wattage = 2^24
        frame[17..23].copy_from_slice(&(1u64 << 24).to_le_bytes()[..6]);
        // 2021-01-02 03:04:05 => [sec, min, hour, day, mon, year - 1900]
        frame[23..29].copy_from_slice(&[5, 4, 3, 2, 0, 121]);

        let m = read_measure(&frame).unwrap();
        assert_eq!(m.voltage, 100.5);
        assert_eq!(m.ampere, 1.25);
        assert_eq!(m.wattage, 1.0);
        assert!((m.power_factor - 1.0 / (100.5 * 1.25)).abs() < 1e-12);
        assert_eq!(
            (
                m.timestamp.year(),
                m.timestamp.month(),
                m.timestamp.day(),
                m.timestamp.hour(),
                m.timestamp.minute(),
                m.timestamp.second()
            ),
            (2021, 1, 2, 3, 4, 5)
        );
    }

    #[test]
    fn frame_assembler_reassembles_split_frame() {
        let mut a = FrameAssembler::new();
        assert_eq!(a.feed(&[HEADER, 0x00, 0x03, 0x01]), None);
        assert_eq!(
            a.feed(&[0x02, 0x03, 0xFF]),
            Some(vec![HEADER, 0x00, 0x03, 0x01, 0x02, 0x03, 0xFF])
        );
    }

    #[test]
    fn frame_assembler_restarts_on_new_header() {
        let mut a = FrameAssembler::new();
        assert_eq!(a.feed(&[HEADER, 0x00, 0x08, 0x01]), None);
        // A fresh header mid-frame abandons the partial one.
        assert_eq!(
            a.feed(&[HEADER, 0x00, 0x01, 0x08, 0xB3]),
            Some(vec![HEADER, 0x00, 0x01, 0x08, 0xB3])
        );
    }

    #[test]
    fn frame_assembler_discards_oversized_buffer() {
        let mut a = FrameAssembler::new();
        // Declares a payload far longer than any real frame.
        a.feed(&[HEADER, 0xFF, 0xFF]);
        for _ in 0..MAX_FRAME_LEN {
            a.feed(&[0x00]);
        }
        assert!(a.buf.len() <= MAX_FRAME_LEN);
    }

    #[test]
    fn read_measure_rejects_short_frame() {
        assert!(read_measure(&[0xAA, 0x00, 0x00, 0x00]).is_err());
    }
}
